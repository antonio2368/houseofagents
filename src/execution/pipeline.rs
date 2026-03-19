use crate::config::AppConfig;
use crate::error::AppError;
use crate::execution::{
    finish_live_log_forwarder, wait_for_cancel, ProgressEvent, PromptRuntimeContext,
};
use crate::output::OutputManager;
use crate::provider::{self, ProviderKind};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

const MAX_TERMINAL_OUTPUTS_BYTES: usize = 512 * 1024; // 512 KB cap

pub type BlockId = u32;
/// Sentinel value for wildcard data feeds: "all execution blocks".
pub const WILDCARD_BLOCK_ID: BlockId = 0;
type ProviderPool = HashMap<(String, String), Arc<Mutex<Box<dyn provider::Provider>>>>;
pub(crate) type PipelineAgentConfigs =
    HashMap<String, (ProviderKind, crate::config::ProviderConfig, bool)>;

#[derive(Debug, Clone)]
struct PipelineTaskMetadata {
    runtime_id: u32,
    source_block_id: BlockId,
    agent_name: String,
    label: String,
    iteration: u32,
    loop_pass: u32,
}

struct PipelineMessageContext<'a> {
    def: &'a PipelineDefinition,
    iteration: u32,
    block_outputs: &'a HashMap<u32, String>,
    previous_terminal_outputs: &'a str,
    output: &'a OutputManager,
    prompt_context: &'a PromptRuntimeContext,
    runtime_table: &'a RuntimeReplicaTable,
    block_to_loop: &'a HashMap<BlockId, (BlockId, BlockId)>,
    block_loop_pass: &'a HashMap<BlockId, u32>,
}

// ---------------------------------------------------------------------------
// Runtime Replica Table
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimePhase {
    Execution,
    Finalization,
}

pub(crate) struct RuntimeReplicaInfo {
    pub runtime_id: u32,
    pub source_block_id: BlockId,
    pub replica_index: u32,
    #[allow(dead_code)]
    pub phase: RuntimePhase,
    pub run_scope: Option<u32>,
    pub agent: String,
    pub display_label: String,
    pub session_key: String,
    pub filename_stem: String,
}

pub(crate) struct RuntimeReplicaTable {
    pub entries: Vec<RuntimeReplicaInfo>,
    pub logical_to_runtime: HashMap<BlockId, Vec<u32>>,
    pub keep_policy: HashMap<(String, String), bool>,
    pub keep_loop_policy: HashMap<(String, String), bool>,
}

pub(crate) fn build_runtime_table(def: &PipelineDefinition) -> RuntimeReplicaTable {
    let mut entries = Vec::new();
    let mut logical_to_runtime: HashMap<BlockId, Vec<u32>> = HashMap::new();
    let mut keep_policy: HashMap<(String, String), bool> = HashMap::new();
    let mut keep_loop_policy: HashMap<(String, String), bool> = HashMap::new();
    let mut next_id: u32 = 0;

    for block in &def.blocks {
        let base_session_key = block.effective_session_key();
        let block_name_key = if block.name.trim().is_empty() {
            format!("block{}", block.id)
        } else {
            format!(
                "{}_b{}",
                OutputManager::sanitize_session_name(&block.name),
                block.id
            )
        };

        let num_agents = block.agents.len();
        let num_replicas = block.replicas;
        let multi_agent = num_agents > 1;
        let multi_replica = num_replicas > 1;
        let blabel = block_label(block);

        let mut runtime_ids = Vec::new();
        for agent in &block.agents {
            let agent_file_key = OutputManager::sanitize_session_name(agent);
            let base_keep = def.keep_session_across_iterations(agent, &base_session_key);
            let base_keep_loops = def.keep_session_across_loop_passes(agent, &base_session_key);

            for ri in 0..num_replicas {
                let runtime_id = next_id;
                next_id += 1;

                let display_label = match (multi_agent, multi_replica) {
                    (false, false) => blabel.clone(),
                    (false, true) => format!("{} (r{})", blabel, ri + 1),
                    (true, false) => format!("{blabel} ({agent})"),
                    (true, true) => format!("{} ({} r{})", blabel, agent, ri + 1),
                };

                let session_key = if multi_replica {
                    format!("{}_r{}", base_session_key, ri + 1)
                } else {
                    base_session_key.clone()
                };

                let filename_stem = match (multi_agent, multi_replica) {
                    (false, false) => format!("{block_name_key}_{agent_file_key}"),
                    (false, true) => {
                        format!("{}_{}_r{}", block_name_key, agent_file_key, ri + 1)
                    }
                    (true, false) => format!("{block_name_key}_{agent_file_key}"),
                    (true, true) => {
                        format!("{}_{}_r{}", block_name_key, agent_file_key, ri + 1)
                    }
                };

                keep_policy.insert((agent.clone(), session_key.clone()), base_keep);
                keep_loop_policy.insert((agent.clone(), session_key.clone()), base_keep_loops);

                entries.push(RuntimeReplicaInfo {
                    runtime_id,
                    source_block_id: block.id,
                    replica_index: ri,
                    phase: RuntimePhase::Execution,
                    run_scope: None,
                    agent: agent.clone(),
                    display_label,
                    session_key,
                    filename_stem,
                });

                runtime_ids.push(runtime_id);
            }
        }
        logical_to_runtime.insert(block.id, runtime_ids);
    }

    RuntimeReplicaTable {
        entries,
        logical_to_runtime,
        keep_policy,
        keep_loop_policy,
    }
}

// ---------------------------------------------------------------------------
// Pipeline step labels (shared between TUI and headless)
// ---------------------------------------------------------------------------

/// Build step labels for pipeline progress tracking.
///
/// When `include_finalization` is true, finalization labels are appended
/// (appropriate for single-run where finalization is part of the run).
/// Batch callers should pass `false` because finalization is batch-scope,
/// not per-run, and is tracked separately via `BatchStageStarted/Finished`.
pub(crate) fn pipeline_step_labels(
    def: &PipelineDefinition,
    include_finalization: bool,
) -> Vec<String> {
    let rt = build_runtime_table(def);
    // Build set of block IDs participating in loops and their total passes
    let mut loop_passes: HashMap<BlockId, u32> = HashMap::new();
    let graph = RegularGraph::from_def(def);
    for lc in &def.loop_connections {
        if let Some(sub_dag) = compute_loop_sub_dag(&graph, lc.from, lc.to) {
            for &block_id in &sub_dag {
                loop_passes.insert(block_id, lc.count + 1);
            }
        }
    }
    let mut labels = Vec::new();
    for info in &rt.entries {
        let total_passes = loop_passes.get(&info.source_block_id).copied().unwrap_or(1);
        if total_passes > 1 {
            for pass in 0..total_passes {
                labels.push(format_block_step_label_with_pass(
                    info.runtime_id,
                    &info.display_label,
                    &info.agent,
                    pass,
                ));
            }
        } else {
            labels.push(format_block_step_label(
                info.runtime_id,
                &info.display_label,
                &info.agent,
            ));
        }
    }
    // Append finalization labels only for single-run progress
    if include_finalization && def.has_finalization() {
        let fin_scope = FinalizationRunScope::SingleRun {
            run_id: 1,
            run_dir: PathBuf::new(),
        };
        let fin_entries =
            build_finalization_runtime_entries(def, &fin_scope, rt.entries.len() as u32);
        for info in &fin_entries {
            labels.push(format_block_step_label(
                info.runtime_id,
                &info.display_label,
                &info.agent,
            ));
        }
    }
    labels
}

pub(crate) fn format_block_step_label(block_id: u32, label: &str, agent_name: &str) -> String {
    if label.trim().is_empty() {
        format!("Block {block_id} ({agent_name})")
    } else if label.contains(&format!("({agent_name})"))
        || label.contains(&format!("({agent_name} "))
    {
        // Label already includes agent name (multi-agent display labels from runtime table)
        label.to_string()
    } else {
        format!("{label} ({agent_name})")
    }
}

pub(crate) fn format_block_step_label_with_pass(
    block_id: u32,
    label: &str,
    agent_name: &str,
    pass: u32,
) -> String {
    let base = format_block_step_label(block_id, label, agent_name);
    format!("{base} [pass {pass}]")
}

fn replica_filename(info: &RuntimeReplicaInfo, iteration: u32) -> String {
    format!("{}_iter{}.md", info.filename_stem, iteration)
}

fn loop_replica_filename(info: &RuntimeReplicaInfo, iteration: u32, loop_pass: u32) -> String {
    if loop_pass == 0 {
        replica_filename(info, iteration)
    } else {
        format!(
            "{}_iter{}_loop{}.md",
            info.filename_stem, iteration, loop_pass
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineBlock {
    pub id: BlockId,
    #[serde(default)]
    pub name: String,
    pub agents: Vec<String>,
    #[serde(default)]
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<String>, // profile filenames without .md extension
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub position: (u16, u16), // grid coordinates (col, row)
    #[serde(default = "default_one", skip_serializing_if = "is_one")]
    pub replicas: u32,
}

/// Custom deserialize for backward compat: accepts both `agent` (legacy string)
/// and `agents` (new vec). On serialization, always writes `agents`.
impl<'de> Deserialize<'de> for PipelineBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            id: BlockId,
            #[serde(default)]
            name: String,
            #[serde(default)]
            agent: Option<String>,
            #[serde(default)]
            agents: Option<Vec<String>>,
            #[serde(default)]
            prompt: String,
            #[serde(default)]
            profiles: Vec<String>,
            #[serde(default)]
            session_id: Option<String>,
            position: (u16, u16),
            #[serde(default = "default_one")]
            replicas: u32,
        }
        let raw = Raw::deserialize(deserializer)?;
        let agents = match (raw.agents, raw.agent) {
            (Some(v), _) if !v.is_empty() => {
                if let Some(blank) = v.iter().find(|a| a.trim().is_empty()) {
                    return Err(serde::de::Error::custom(format!(
                        "block has a blank agent name '{blank}' in 'agents' list"
                    )));
                }
                v
            }
            (Some(_), _) => {
                // agents field explicitly present but empty — reject
                return Err(serde::de::Error::custom(
                    "block has an empty 'agents' list; at least one agent is required",
                ));
            }
            (_, Some(a)) if !a.trim().is_empty() => vec![a],
            (_, Some(_)) => {
                // agent field explicitly present but empty/whitespace — reject
                return Err(serde::de::Error::custom(
                    "block has a blank 'agent' field; a non-empty agent name is required",
                ));
            }
            // Neither field present (legacy/minimal TOML) — default to Claude
            (None, None) => vec!["Claude".to_string()],
        };
        // Validate profile names to prevent path traversal
        for p in &raw.profiles {
            if p.contains('/') || p.contains('\\') || p.contains("..") || p == "." || p.is_empty() {
                return Err(serde::de::Error::custom(format!(
                    "invalid profile name '{p}': must not contain path separators or '..'"
                )));
            }
        }
        Ok(PipelineBlock {
            id: raw.id,
            name: raw.name,
            agents,
            prompt: raw.prompt,
            profiles: raw.profiles,
            session_id: raw.session_id,
            position: raw.position,
            replicas: raw.replicas,
        })
    }
}

fn default_one() -> u32 {
    1
}

fn is_one(v: &u32) -> bool {
    *v == 1
}

impl PipelineBlock {
    /// Primary agent (first in the list). Used for backward-compat display contexts.
    pub fn primary_agent(&self) -> &str {
        self.agents.first().map(|s| s.as_str()).unwrap_or("Claude")
    }

    pub fn effective_session_key(&self) -> String {
        self.session_id
            .clone()
            .unwrap_or_else(|| format!("__block_{}", self.id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConnection {
    pub from: BlockId,
    pub to: BlockId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopConnection {
    pub from: BlockId,
    pub to: BlockId,
    pub count: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFeed {
    /// 0 = wildcard: all execution blocks
    pub from: BlockId,
    pub to: BlockId,
    pub collection: FeedCollection,
    pub granularity: FeedGranularity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FeedCollection {
    #[default]
    LastIteration,
    AllIterations,
}

impl FeedCollection {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeedCollection::LastIteration => "last_iteration",
            FeedCollection::AllIterations => "all_iterations",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FeedGranularity {
    #[default]
    PerRun,
    AllRuns,
}

impl FeedGranularity {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeedGranularity::PerRun => "per_run",
            FeedGranularity::AllRuns => "all_runs",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionConfig {
    pub agent: String,
    pub session_key: String,
    #[serde(default = "default_keep_across_iterations")]
    pub keep_across_iterations: bool,
    #[serde(default = "default_keep_across_loop_passes")]
    pub keep_across_loop_passes: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveSession {
    pub agent: String,
    pub session_key: String,
    pub display_label: String,
    pub block_ids: Vec<BlockId>,
    pub keep_across_iterations: bool,
    pub keep_across_loop_passes: bool,
    pub total_replicas: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDefinition {
    #[serde(default)]
    pub initial_prompt: String,
    #[serde(default = "default_iterations")]
    pub iterations: u32,
    #[serde(default)]
    pub blocks: Vec<PipelineBlock>,
    #[serde(default)]
    pub connections: Vec<PipelineConnection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub session_configs: Vec<SessionConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub loop_connections: Vec<LoopConnection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalization_blocks: Vec<PipelineBlock>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalization_connections: Vec<PipelineConnection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data_feeds: Vec<DataFeed>,
}

fn default_iterations() -> u32 {
    1
}

fn default_keep_across_iterations() -> bool {
    true
}

fn default_keep_across_loop_passes() -> bool {
    true
}

impl Default for PipelineDefinition {
    fn default() -> Self {
        Self {
            initial_prompt: String::new(),
            iterations: 1,
            blocks: Vec::new(),
            connections: Vec::new(),
            session_configs: Vec::new(),
            loop_connections: Vec::new(),
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        }
    }
}

impl PipelineDefinition {
    /// Collect unique agent names from all blocks (including finalization).
    pub fn all_agent_names(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut agents = Vec::new();
        for block in self.blocks.iter().chain(self.finalization_blocks.iter()) {
            for agent in &block.agents {
                if seen.insert(agent.clone()) {
                    agents.push(agent.clone());
                }
            }
        }
        agents
    }

    pub fn effective_sessions(&self) -> Vec<EffectiveSession> {
        let mut map: HashMap<(String, String), (String, Vec<BlockId>, u32)> = HashMap::new();
        for block in &self.blocks {
            for agent in &block.agents {
                let key = (agent.clone(), block.effective_session_key());
                let entry = map.entry(key).or_insert_with(|| {
                    let label = if block.session_id.is_some() {
                        block.session_id.clone().unwrap()
                    } else if !block.name.is_empty() {
                        block.name.clone()
                    } else {
                        format!("Block {}", block.id)
                    };
                    (label, Vec::new(), 0)
                });
                entry.1.push(block.id);
                entry.2 += block.replicas;
            }
        }
        let mut sessions: Vec<EffectiveSession> = map
            .into_iter()
            .map(
                |((agent, session_key), (display_label, block_ids, total_replicas))| {
                    let keep = self.keep_session_across_iterations(&agent, &session_key);
                    let keep_loops = self.keep_session_across_loop_passes(&agent, &session_key);
                    EffectiveSession {
                        agent,
                        session_key,
                        display_label,
                        block_ids,
                        keep_across_iterations: keep,
                        keep_across_loop_passes: keep_loops,
                        total_replicas,
                    }
                },
            )
            .collect();
        sessions.sort_by(|a, b| (&a.agent, &a.session_key).cmp(&(&b.agent, &b.session_key)));

        // Disambiguate rows where agent + display_label would look identical
        let mut label_counts: HashMap<(String, String), usize> = HashMap::new();
        for s in &sessions {
            *label_counts
                .entry((s.agent.clone(), s.display_label.clone()))
                .or_default() += 1;
        }
        for s in &mut sessions {
            if label_counts
                .get(&(s.agent.clone(), s.display_label.clone()))
                .copied()
                .unwrap_or(0)
                > 1
            {
                let ids: Vec<String> = s.block_ids.iter().map(|id| id.to_string()).collect();
                // Prefix with block IDs so truncation never hides the distinguishing part
                s.display_label = format!("#{}: {}", ids.join(","), s.display_label);
            }
        }

        sessions
    }

    pub fn keep_session_across_iterations(&self, agent: &str, session_key: &str) -> bool {
        self.session_configs
            .iter()
            .find(|c| c.agent == agent && c.session_key == session_key)
            .map(|c| c.keep_across_iterations)
            .unwrap_or(true)
    }

    pub fn set_keep_session_across_iterations(
        &mut self,
        agent: &str,
        session_key: &str,
        keep: bool,
    ) {
        if let Some(existing) = self
            .session_configs
            .iter_mut()
            .find(|c| c.agent == agent && c.session_key == session_key)
        {
            existing.keep_across_iterations = keep;
            // Remove entry entirely if both fields are back to default
            if keep && existing.keep_across_loop_passes {
                self.session_configs
                    .retain(|c| !(c.agent == agent && c.session_key == session_key));
            }
        } else if !keep {
            self.session_configs.push(SessionConfig {
                agent: agent.to_string(),
                session_key: session_key.to_string(),
                keep_across_iterations: false,
                keep_across_loop_passes: true,
            });
        }
    }

    pub fn keep_session_across_loop_passes(&self, agent: &str, session_key: &str) -> bool {
        self.session_configs
            .iter()
            .find(|c| c.agent == agent && c.session_key == session_key)
            .map(|c| c.keep_across_loop_passes)
            .unwrap_or(true)
    }

    pub fn set_keep_session_across_loop_passes(
        &mut self,
        agent: &str,
        session_key: &str,
        keep: bool,
    ) {
        if let Some(existing) = self
            .session_configs
            .iter_mut()
            .find(|c| c.agent == agent && c.session_key == session_key)
        {
            existing.keep_across_loop_passes = keep;
            // Remove entry entirely if both fields are back to default
            if keep && existing.keep_across_iterations {
                self.session_configs
                    .retain(|c| !(c.agent == agent && c.session_key == session_key));
            }
        } else if !keep {
            self.session_configs.push(SessionConfig {
                agent: agent.to_string(),
                session_key: session_key.to_string(),
                keep_across_iterations: true,
                keep_across_loop_passes: false,
            });
        }
    }

    pub fn has_finalization(&self) -> bool {
        !self.finalization_blocks.is_empty()
    }

    pub fn execution_block_ids(&self) -> HashSet<BlockId> {
        self.blocks.iter().map(|b| b.id).collect()
    }

    pub fn finalization_block_ids(&self) -> HashSet<BlockId> {
        self.finalization_blocks.iter().map(|b| b.id).collect()
    }

    pub fn is_finalization_block(&self, id: BlockId) -> bool {
        self.finalization_blocks.iter().any(|b| b.id == id)
    }

    /// A finalization block is per_run if:
    /// 1. It has a direct DataFeed with FeedGranularity::PerRun, OR
    /// 2. Any upstream finalization predecessor (via finalization_connections) is per_run
    pub fn is_per_run_finalization_block(&self, id: BlockId) -> bool {
        // Seed: blocks with direct PerRun feeds
        let mut per_run: HashSet<BlockId> = self
            .data_feeds
            .iter()
            .filter(|f| f.granularity == FeedGranularity::PerRun)
            .map(|f| f.to)
            .collect();
        // Propagate forward over finalization_connections
        let mut changed = true;
        while changed {
            changed = false;
            for conn in &self.finalization_connections {
                if per_run.contains(&conn.from) && per_run.insert(conn.to) {
                    changed = true;
                }
            }
        }
        per_run.contains(&id)
    }

    /// All blocks from both phases, for iteration patterns
    pub fn all_blocks(&self) -> impl Iterator<Item = &PipelineBlock> {
        self.blocks.iter().chain(self.finalization_blocks.iter())
    }

    pub fn normalize_session_configs(&mut self) {
        // Collect valid effective session keys
        let valid: HashSet<(String, String)> = self
            .blocks
            .iter()
            .chain(self.finalization_blocks.iter())
            .flat_map(|b| {
                let sk = b.effective_session_key();
                b.agents.iter().map(move |a| (a.clone(), sk.clone()))
            })
            .collect();

        // Drop stale rows and rows where both fields are at default (true)
        self.session_configs.retain(|c| {
            (!c.keep_across_iterations || !c.keep_across_loop_passes)
                && valid.contains(&(c.agent.clone(), c.session_key.clone()))
        });

        // Sort for stability
        self.session_configs
            .sort_by(|a, b| (&a.agent, &a.session_key).cmp(&(&b.agent, &b.session_key)));

        // Deduplicate
        self.session_configs
            .dedup_by(|a, b| a.agent == b.agent && a.session_key == b.session_key);
    }
}

// ---------------------------------------------------------------------------
// Graph utilities
// ---------------------------------------------------------------------------

/// Blocks with no incoming regular connections (DAG roots).
/// Loop connections are back-edges and excluded.
pub fn root_blocks(def: &PipelineDefinition) -> Vec<BlockId> {
    let has_incoming: HashSet<BlockId> = def.connections.iter().map(|c| c.to).collect();
    def.blocks
        .iter()
        .filter(|b| !has_incoming.contains(&b.id))
        .map(|b| b.id)
        .collect()
}

/// Blocks with no outgoing regular connections (DAG terminals).
/// Loop connections are back-edges and excluded.
pub fn terminal_blocks(def: &PipelineDefinition) -> Vec<BlockId> {
    let has_outgoing: HashSet<BlockId> = def.connections.iter().map(|c| c.from).collect();
    def.blocks
        .iter()
        .filter(|b| !has_outgoing.contains(&b.id))
        .map(|b| b.id)
        .collect()
}

/// Direct predecessors of a block via regular connections only.
/// Loop connections are back-edges and excluded.
/// Uses the precomputed reverse adjacency from `RegularGraph` when available;
/// falls back to linear scan for callers that don't have a graph handy.
pub fn upstream_of(def: &PipelineDefinition, id: BlockId) -> Vec<BlockId> {
    def.connections
        .iter()
        .filter(|c| c.to == id)
        .map(|c| c.from)
        .collect()
}

/// Kahn's algorithm: returns parallelizable layers or Err on cycle.
/// Only considers regular connections. Loop connections are back-edges.
pub fn topological_layers(def: &PipelineDefinition) -> Result<Vec<Vec<BlockId>>, CycleError> {
    let block_ids: HashSet<BlockId> = def.blocks.iter().map(|b| b.id).collect();
    let mut in_degree: HashMap<BlockId, usize> = block_ids.iter().map(|&id| (id, 0)).collect();
    let mut downstream: HashMap<BlockId, Vec<BlockId>> = HashMap::new();

    for conn in &def.connections {
        *in_degree.entry(conn.to).or_default() += 1;
        downstream.entry(conn.from).or_default().push(conn.to);
    }

    let mut queue: VecDeque<BlockId> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&id, _)| id)
        .collect();

    // Sort for determinism
    let mut sorted_queue: Vec<BlockId> = queue.drain(..).collect();
    sorted_queue.sort();
    queue.extend(sorted_queue);

    let mut layers: Vec<Vec<BlockId>> = Vec::new();
    let mut visited = 0usize;

    while !queue.is_empty() {
        let mut layer: Vec<BlockId> = queue.drain(..).collect();
        layer.sort();
        visited += layer.len();

        let mut next_queue = Vec::new();
        for &id in &layer {
            if let Some(children) = downstream.get(&id) {
                for &child in children {
                    let Some(deg) = in_degree.get_mut(&child) else {
                        continue;
                    };
                    *deg -= 1;
                    if *deg == 0 {
                        next_queue.push(child);
                    }
                }
            }
        }
        next_queue.sort();
        queue.extend(next_queue);
        layers.push(layer);
    }

    if visited != block_ids.len() {
        Err(CycleError)
    } else {
        Ok(layers)
    }
}

#[derive(Debug, Clone)]
pub struct CycleError;

impl std::fmt::Display for CycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pipeline contains a cycle")
    }
}

/// DFS reachability: would adding `from → to` create a cycle?
/// Accepts an explicit connection slice so it works for both execution and finalization connections.
pub fn would_create_cycle(connections: &[PipelineConnection], from: BlockId, to: BlockId) -> bool {
    if from == to {
        return true;
    }
    // BFS from `to` — if we reach `from`, adding from→to would create a cycle
    let downstream: HashMap<BlockId, Vec<BlockId>> = {
        let mut map: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
        for conn in connections {
            map.entry(conn.from).or_default().push(conn.to);
        }
        map
    };
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(to);
    visited.insert(to);
    while let Some(node) = queue.pop_front() {
        if let Some(children) = downstream.get(&node) {
            for &child in children {
                if child == from {
                    return true;
                }
                if visited.insert(child) {
                    queue.push_back(child);
                }
            }
        }
    }
    false
}

/// Remove loop connections whose sub-DAG is no longer valid after a graph edit.
/// Checks both ancestry validity and pairwise sub-DAG disjointness.
/// Returns descriptions of removed loops for user feedback.
pub fn prune_invalid_loops(def: &mut PipelineDefinition) -> Vec<String> {
    if def.loop_connections.is_empty() {
        return Vec::new();
    }
    let graph = RegularGraph::from_def(def);
    let mut removed = Vec::new();

    // Phase 1: remove loops with broken ancestry
    def.loop_connections.retain(|lc| {
        if compute_loop_sub_dag(&graph, lc.from, lc.to).is_some() {
            true
        } else {
            removed.push(format!(
                "Loop {}→{} removed (ancestry broken by edge change)",
                lc.from, lc.to
            ));
            false
        }
    });

    // Phase 2: remove loops whose sub-DAGs overlap with an earlier loop
    if def.loop_connections.len() > 1 {
        let mut kept_sub_dags: Vec<((BlockId, BlockId), HashSet<BlockId>)> = Vec::new();
        let mut overlap_indices: HashSet<usize> = HashSet::new();
        for (i, lc) in def.loop_connections.iter().enumerate() {
            if let Some(blocks) = compute_loop_sub_dag(&graph, lc.from, lc.to) {
                let overlaps = kept_sub_dags
                    .iter()
                    .any(|(_, existing)| blocks.iter().any(|b| existing.contains(b)));
                if overlaps {
                    removed.push(format!(
                        "Loop {}→{} removed (sub-DAG overlaps with another loop after edge change)",
                        lc.from, lc.to
                    ));
                    overlap_indices.insert(i);
                } else {
                    kept_sub_dags.push(((lc.from, lc.to), blocks));
                }
            }
        }
        if !overlap_indices.is_empty() {
            let mut idx = 0;
            def.loop_connections.retain(|_| {
                let keep = !overlap_indices.contains(&idx);
                idx += 1;
                keep
            });
        }
    }

    removed
}

// ---------------------------------------------------------------------------
// Loop-back analysis layer
// ---------------------------------------------------------------------------

/// Precomputed regular-graph adjacency (excludes loop connections).
pub(crate) struct RegularGraph {
    pub forward: HashMap<BlockId, Vec<BlockId>>,
    pub reverse: HashMap<BlockId, Vec<BlockId>>,
    pub replicas: HashMap<BlockId, u32>,
}

impl RegularGraph {
    pub fn from_def(def: &PipelineDefinition) -> Self {
        let mut forward: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
        let mut reverse: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
        let mut replicas: HashMap<BlockId, u32> = HashMap::new();
        for block in &def.blocks {
            replicas.insert(block.id, block.agents.len() as u32 * block.replicas);
        }
        for conn in &def.connections {
            forward.entry(conn.from).or_default().push(conn.to);
            reverse.entry(conn.to).or_default().push(conn.from);
        }
        RegularGraph {
            forward,
            reverse,
            replicas,
        }
    }
}

/// Sub-DAG for a single loop-back connection.
#[derive(Clone)]
pub(crate) struct LoopSubDag {
    pub blocks: HashSet<BlockId>,
    pub internal_in_degree: HashMap<BlockId, usize>,
    pub total_replicas: usize,
    pub deferred_external_edges: HashMap<BlockId, Vec<(BlockId, usize)>>,
}

/// Compute the set of blocks on all regular paths from `to` to `from`.
/// Returns `None` if `to` is not a regular-graph ancestor of `from`.
pub(crate) fn compute_loop_sub_dag(
    graph: &RegularGraph,
    from: BlockId,
    to: BlockId,
) -> Option<HashSet<BlockId>> {
    // BFS forward from `to`
    let mut forward_reachable = HashSet::new();
    {
        let mut queue = VecDeque::new();
        queue.push_back(to);
        forward_reachable.insert(to);
        while let Some(node) = queue.pop_front() {
            if let Some(children) = graph.forward.get(&node) {
                for &child in children {
                    if forward_reachable.insert(child) {
                        queue.push_back(child);
                    }
                }
            }
        }
    }
    // BFS backward from `from`
    let mut backward_reachable = HashSet::new();
    {
        let mut queue = VecDeque::new();
        queue.push_back(from);
        backward_reachable.insert(from);
        while let Some(node) = queue.pop_front() {
            if let Some(parents) = graph.reverse.get(&node) {
                for &parent in parents {
                    if backward_reachable.insert(parent) {
                        queue.push_back(parent);
                    }
                }
            }
        }
    }
    let intersection: HashSet<BlockId> = forward_reachable
        .intersection(&backward_reachable)
        .copied()
        .collect();
    if intersection.contains(&to) && intersection.contains(&from) {
        Some(intersection)
    } else {
        None
    }
}

/// Build a full `LoopSubDag` from a block set.
fn build_loop_sub_dag(graph: &RegularGraph, blocks: HashSet<BlockId>, to: BlockId) -> LoopSubDag {
    let mut internal_in_degree: HashMap<BlockId, usize> = blocks.iter().map(|&b| (b, 0)).collect();
    let mut deferred_external_edges: HashMap<BlockId, Vec<(BlockId, usize)>> = HashMap::new();
    let mut total_replicas: usize = 0;

    for &bid in &blocks {
        let r = graph.replicas.get(&bid).copied().unwrap_or(1) as usize;
        total_replicas += r;
        if let Some(children) = graph.forward.get(&bid) {
            for &child in children {
                if blocks.contains(&child) {
                    *internal_in_degree.entry(child).or_default() += r;
                } else {
                    deferred_external_edges
                        .entry(bid)
                        .or_default()
                        .push((child, r));
                }
            }
        }
    }
    // `to` always gets in-degree 0 (entry point)
    internal_in_degree.insert(to, 0);

    LoopSubDag {
        blocks,
        internal_in_degree,
        total_replicas,
        deferred_external_edges,
    }
}

/// Fully prepared loop data for validation, execution, and progress.
#[allow(dead_code)]
pub(crate) struct PreparedLoops {
    pub sub_dags: HashMap<(BlockId, BlockId), LoopSubDag>,
    pub block_to_loop: HashMap<BlockId, (BlockId, BlockId)>,
}

/// Prepare all loop sub-DAGs from a pipeline definition.
/// Returns `None` if any loop has invalid ancestry.
pub(crate) fn prepare_loops(def: &PipelineDefinition) -> Option<PreparedLoops> {
    let graph = RegularGraph::from_def(def);
    let mut sub_dags = HashMap::new();
    let mut block_to_loop = HashMap::new();

    for lc in &def.loop_connections {
        let blocks = compute_loop_sub_dag(&graph, lc.from, lc.to)?;
        let sub_dag = build_loop_sub_dag(&graph, blocks, lc.to);
        let key = (lc.from, lc.to);
        for &bid in &sub_dag.blocks {
            block_to_loop.insert(bid, key);
        }
        sub_dags.insert(key, sub_dag);
    }

    Some(PreparedLoops {
        sub_dags,
        block_to_loop,
    })
}

/// Compute total extra tasks from loop re-runs.
/// Returns: Σ sub_dag.total_replicas × lc.count
pub fn loop_extra_tasks(def: &PipelineDefinition) -> usize {
    if def.loop_connections.is_empty() {
        return 0;
    }
    let graph = RegularGraph::from_def(def);
    def.loop_connections
        .iter()
        .map(|lc| {
            let sub_dag_blocks = compute_loop_sub_dag(&graph, lc.from, lc.to);
            match sub_dag_blocks {
                Some(blocks) => {
                    let total_replicas: usize = blocks
                        .iter()
                        .map(|b| graph.replicas.get(b).copied().unwrap_or(1) as usize)
                        .sum();
                    total_replicas * lc.count as usize
                }
                None => 0,
            }
        })
        .sum()
}

// ---------------------------------------------------------------------------
// Auto-position
// ---------------------------------------------------------------------------

/// Scan grid left-to-right, top-to-bottom for first unoccupied slot.
/// Accepts an explicit block slice so it works for both execution and finalization blocks.
pub fn next_free_position(blocks: &[PipelineBlock]) -> (u16, u16) {
    if blocks.is_empty() {
        return (0, 0);
    }
    let occupied: HashSet<(u16, u16)> = blocks.iter().map(|b| b.position).collect();
    let max_row = blocks
        .iter()
        .map(|b| b.position.1)
        .max()
        .unwrap_or(0)
        .min(99);
    let max_col = blocks
        .iter()
        .map(|b| b.position.0)
        .max()
        .unwrap_or(0)
        .min(99);
    // Search within existing bounds + 1 row, capped to prevent hangs on absurd coordinates
    for row in 0..=max_row + 1 {
        for col in 0..=max_col + 1 {
            if !occupied.contains(&(col, row)) {
                return (col, row);
            }
        }
    }
    // Fallback: walk column 0 from row max_row+2 until we find an unoccupied cell
    let mut fallback_row = max_row + 2;
    while occupied.contains(&(0, fallback_row)) {
        fallback_row = fallback_row.saturating_add(1);
        if fallback_row == u16::MAX {
            break;
        }
    }
    (0, fallback_row)
}

// ---------------------------------------------------------------------------
// TOML save/load
// ---------------------------------------------------------------------------

pub fn pipelines_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("houseofagents")
        .join("pipelines")
}

pub fn ensure_pipelines_dir() -> io::Result<PathBuf> {
    let dir = pipelines_dir();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn save_pipeline(def: &PipelineDefinition, path: &Path) -> Result<(), AppError> {
    validate_pipeline(def)?;
    let mut normalized = def.clone();
    normalized.normalize_session_configs();
    let content = toml::to_string_pretty(&normalized)
        .map_err(|e| AppError::Config(format!("Failed to serialize pipeline: {e}")))?;
    std::fs::write(path, content)?;
    Ok(())
}

pub fn load_pipeline(path: &Path) -> Result<PipelineDefinition, AppError> {
    let content = std::fs::read_to_string(path)?;
    let mut def: PipelineDefinition = toml::from_str(&content)
        .map_err(|e| AppError::Config(format!("Failed to parse pipeline: {e}")))?;
    def.normalize_session_configs();
    migrate_loop_direction(&mut def);
    validate_pipeline(&def)?;
    Ok(def)
}

/// Migrate old-format loop connections where `from` was the upstream ancestor
/// and `to` was the downstream target. New convention: `from` is the downstream
/// feedback source, `to` is the upstream restart target. Swap if needed.
fn migrate_loop_direction(def: &mut PipelineDefinition) {
    if def.loop_connections.is_empty() {
        return;
    }
    let graph = RegularGraph::from_def(def);
    for lc in &mut def.loop_connections {
        // Already valid in new direction
        if compute_loop_sub_dag(&graph, lc.from, lc.to).is_some() {
            continue;
        }
        // Old format: from was ancestor, to was downstream — swap
        if compute_loop_sub_dag(&graph, lc.to, lc.from).is_some() {
            std::mem::swap(&mut lc.from, &mut lc.to);
        }
        // If neither works, let validate_pipeline catch it
    }
}

pub(crate) fn validate_replicas(def: &PipelineDefinition) -> Result<(), AppError> {
    for block in def.all_blocks() {
        if block.replicas < 1 {
            return Err(AppError::Config(format!(
                "Block '{}' has replicas < 1",
                block.name
            )));
        }
        if block.replicas > 32 {
            return Err(AppError::Config(format!(
                "Block '{}' has replicas > 32 (max allowed)",
                block.name
            )));
        }
        let total_tasks = block.agents.len() as u32 * block.replicas;
        if total_tasks > 32 {
            return Err(AppError::Config(format!(
                "Block '{}' has agents × replicas = {} (max 32 per block)",
                block.name, total_tasks
            )));
        }
    }
    // Session sharing restriction: blocks with replicas > 1 cannot share session_id
    let all_blocks: Vec<&PipelineBlock> = def.all_blocks().collect();
    for block in &all_blocks {
        if block.replicas > 1 {
            if let Some(ref sid) = block.session_id {
                for other in &all_blocks {
                    if other.id != block.id && other.session_id.as_deref() == Some(sid) {
                        return Err(AppError::Config(format!(
                            "Session '{}' is used by replicated block '{}' and cannot be shared",
                            sid, block.name
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_pipeline(def: &PipelineDefinition) -> Result<(), AppError> {
    // Rule 1: Block IDs unique across both blocks and finalization_blocks.
    // Block ID 0 is reserved as WILDCARD_BLOCK_ID for data feed sources.
    let mut seen = HashSet::new();
    for block in def.all_blocks() {
        if block.id == WILDCARD_BLOCK_ID {
            return Err(AppError::Config(format!(
                "Block ID {WILDCARD_BLOCK_ID} is reserved (wildcard sentinel); use a different ID"
            )));
        }
        if !seen.insert(block.id) {
            return Err(AppError::Config(format!(
                "Duplicate block ID: {}",
                block.id
            )));
        }
    }

    let exec_ids = def.execution_block_ids();
    let fin_ids = def.finalization_block_ids();

    // Rule 2: connections only reference execution blocks
    for conn in &def.connections {
        if !exec_ids.contains(&conn.from) {
            return Err(AppError::Config(format!(
                "Connection references non-existent block: {}",
                conn.from
            )));
        }
        if !exec_ids.contains(&conn.to) {
            return Err(AppError::Config(format!(
                "Connection references non-existent block: {}",
                conn.to
            )));
        }
        // Self-edges
        if conn.from == conn.to {
            return Err(AppError::Config(format!(
                "Self-edge on block {}",
                conn.from
            )));
        }
    }

    // Duplicate regular connections
    {
        let mut seen_conns = HashSet::new();
        for conn in &def.connections {
            if !seen_conns.insert((conn.from, conn.to)) {
                return Err(AppError::Config(format!(
                    "Duplicate connection from {} to {}",
                    conn.from, conn.to
                )));
            }
        }
    }

    // Rule 3: finalization_connections only reference finalization blocks
    for conn in &def.finalization_connections {
        if !fin_ids.contains(&conn.from) {
            return Err(AppError::Config(format!(
                "Finalization connection references non-finalization block: {}",
                conn.from
            )));
        }
        if !fin_ids.contains(&conn.to) {
            return Err(AppError::Config(format!(
                "Finalization connection references non-finalization block: {}",
                conn.to
            )));
        }
    }

    // Rule 7: No self-edges or duplicates in finalization_connections
    {
        let mut seen_fin_conns = HashSet::new();
        for conn in &def.finalization_connections {
            if conn.from == conn.to {
                return Err(AppError::Config(format!(
                    "Finalization self-edge on block {}",
                    conn.from
                )));
            }
            if !seen_fin_conns.insert((conn.from, conn.to)) {
                return Err(AppError::Config(format!(
                    "Duplicate finalization connection from {} to {}",
                    conn.from, conn.to
                )));
            }
        }
    }

    // Rule 4: loop_connections only reference execution blocks
    {
        let mut seen_loops = HashSet::new();
        let mut endpoint_set = HashSet::new();

        for lc in &def.loop_connections {
            // Self-edge
            if lc.from == lc.to {
                return Err(AppError::Config(format!(
                    "Loop self-edge on block {}",
                    lc.from
                )));
            }
            // Count bounds
            if lc.count < 1 || lc.count > 99 {
                return Err(AppError::Config(format!(
                    "Loop count must be 1-99, got {}",
                    lc.count
                )));
            }
            // Dangling refs — must be execution blocks
            if !exec_ids.contains(&lc.from) {
                return Err(AppError::Config(format!(
                    "Loop references non-execution block: {}",
                    lc.from
                )));
            }
            if !exec_ids.contains(&lc.to) {
                return Err(AppError::Config(format!(
                    "Loop references non-execution block: {}",
                    lc.to
                )));
            }
            // Endpoint exclusivity: no block may be an endpoint of multiple loops
            if !endpoint_set.insert(("endpoint", lc.from)) {
                return Err(AppError::Config(format!(
                    "Block {} is already a loop endpoint",
                    lc.from
                )));
            }
            if !endpoint_set.insert(("endpoint", lc.to)) {
                return Err(AppError::Config(format!(
                    "Block {} is already a loop endpoint",
                    lc.to
                )));
            }
            // No duplicate loops
            if !seen_loops.insert((lc.from, lc.to)) {
                return Err(AppError::Config(format!(
                    "Duplicate loop connection from {} to {}",
                    lc.from, lc.to
                )));
            }
        }

        // Ancestry check and sub-DAG overlap validation
        if !def.loop_connections.is_empty() {
            let graph = RegularGraph::from_def(def);
            let mut all_sub_dags: Vec<((BlockId, BlockId), HashSet<BlockId>)> = Vec::new();

            for lc in &def.loop_connections {
                match compute_loop_sub_dag(&graph, lc.from, lc.to) {
                    Some(blocks) => {
                        all_sub_dags.push(((lc.from, lc.to), blocks));
                    }
                    None => {
                        return Err(AppError::Config(format!(
                            "Loop target (to={}) is not a regular-graph ancestor of feedback source (from={}). \
                             Note: loop direction changed — 'from' is now the downstream feedback source, \
                             'to' is the upstream restart target.",
                            lc.to, lc.from
                        )));
                    }
                }
            }

            // Pairwise disjointness check
            for i in 0..all_sub_dags.len() {
                for j in (i + 1)..all_sub_dags.len() {
                    for bid in &all_sub_dags[i].1 {
                        if all_sub_dags[j].1.contains(bid) {
                            return Err(AppError::Config(format!(
                                "Overlapping loop sub-DAGs: block {bid} is in multiple loop regions"
                            )));
                        }
                    }
                }
            }
        }
    }

    // Rule 5 & 6: data_feeds validation
    for feed in &def.data_feeds {
        // Rule 5: from must be 0 (wildcard) or a valid execution block
        if feed.from != WILDCARD_BLOCK_ID && !exec_ids.contains(&feed.from) {
            return Err(AppError::Config(format!(
                "Data feed 'from' must be 0 (wildcard) or a valid execution block, got {}",
                feed.from
            )));
        }
        // Rule 6: to must be a finalization block
        if !fin_ids.contains(&feed.to) {
            return Err(AppError::Config(format!(
                "Data feed 'to' must be a finalization block, got {}",
                feed.to
            )));
        }
    }

    // Rule 8: No duplicate (from, to) feed pairs.
    // Each (from, to) pair allows exactly one feed; collection/granularity are set
    // via the feed edit popup. This keeps the TUI builder and validation in sync.
    {
        let mut seen_feeds = HashSet::new();
        for feed in &def.data_feeds {
            let key = (feed.from, feed.to);
            if !seen_feeds.insert(key) {
                return Err(AppError::Config(format!(
                    "Duplicate data feed ({} → {})",
                    feed.from, feed.to
                )));
            }
        }
    }

    // Rule 10: Wildcard feed (from=0) must not coexist with block-specific feeds
    // targeting the same finalization block.
    {
        let mut wildcard_targets: HashSet<BlockId> = HashSet::new();
        let mut specific_targets: HashSet<BlockId> = HashSet::new();
        for feed in &def.data_feeds {
            if feed.from == WILDCARD_BLOCK_ID {
                wildcard_targets.insert(feed.to);
            } else {
                specific_targets.insert(feed.to);
            }
        }
        for &target in &wildcard_targets {
            if specific_targets.contains(&target) {
                return Err(AppError::Config(format!(
                    "Wildcard feed conflicts with block-specific feed targeting finalization block {target}"
                )));
            }
        }
    }

    // Rule 11: No finalization connection from an all-runs block to a per-run block.
    // An all-runs block runs once after all runs complete, so it cannot feed into a
    // per-run block that runs per successful run.
    for conn in &def.finalization_connections {
        let from_is_per_run = def.is_per_run_finalization_block(conn.from);
        let to_is_per_run = def.is_per_run_finalization_block(conn.to);
        if !from_is_per_run && to_is_per_run {
            return Err(AppError::Config(format!(
                "Finalization connection from all-runs block {} to per-run block {} is unsatisfiable",
                conn.from, conn.to
            )));
        }
    }

    // Rule 9: finalization_connections must be acyclic
    if !def.finalization_connections.is_empty() {
        let fin_block_ids: HashSet<BlockId> = fin_ids.clone();
        let mut in_degree: HashMap<BlockId, usize> =
            fin_block_ids.iter().map(|&id| (id, 0)).collect();
        let mut downstream: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
        for conn in &def.finalization_connections {
            *in_degree.entry(conn.to).or_default() += 1;
            downstream.entry(conn.from).or_default().push(conn.to);
        }
        let mut queue: VecDeque<BlockId> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();
        let mut visited = 0usize;
        while let Some(node) = queue.pop_front() {
            visited += 1;
            if let Some(children) = downstream.get(&node) {
                for &child in children {
                    if let Some(deg) = in_degree.get_mut(&child) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(child);
                        }
                    }
                }
            }
        }
        if visited != fin_block_ids.len() {
            return Err(AppError::Config(
                "Finalization connections contain a cycle".to_string(),
            ));
        }
    }

    // Check for cycles (regular connections only, loop connections are back-edges)
    topological_layers(def)
        .map_err(|_| AppError::Config("Pipeline contains a cycle".to_string()))?;

    // Check for duplicate session config entries
    {
        let mut seen_configs = HashSet::new();
        for cfg in &def.session_configs {
            if !seen_configs.insert((&cfg.agent, &cfg.session_key)) {
                return Err(AppError::Config(format!(
                    "Duplicate session config for ({}, {})",
                    cfg.agent, cfg.session_key
                )));
            }
        }
    }

    // Rule 12: Finalization blocks follow same agent/replica rules as execution blocks
    for block in def.all_blocks() {
        if block.agents.is_empty() {
            return Err(AppError::Config(format!(
                "Block {} has no agents",
                block.id
            )));
        }
        let mut seen_agents = HashSet::new();
        for agent in &block.agents {
            if !seen_agents.insert(agent.as_str()) {
                return Err(AppError::Config(format!(
                    "Duplicate agent '{}' in block {}",
                    agent, block.id
                )));
            }
        }
    }

    // Replica validation
    validate_replicas(def)?;

    Ok(())
}

pub fn list_pipeline_files() -> io::Result<Vec<PathBuf>> {
    let dir = pipelines_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("toml") {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    files.sort();
    Ok(files)
}

pub fn profiles_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("houseofagents")
        .join("profiles")
}

pub fn list_profile_files() -> io::Result<Vec<PathBuf>> {
    let dir = profiles_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("md") && path.is_file() {
                // Skip files whose stem would be an invalid profile name
                let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                if is_valid_profile_name(stem) {
                    Some(path)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();
    files.sort();
    Ok(files)
}

// ---------------------------------------------------------------------------
// Execution engine
// ---------------------------------------------------------------------------

struct LoopRuntimeState {
    remaining: u32,
    current_pass: u32,
    prompt: String,
    sub_dag: LoopSubDag,
    block_completed_this_pass: HashMap<BlockId, u32>,
    extra_tasks_remaining: usize,
    abandoned: bool,
}

impl LoopRuntimeState {
    /// Number of tasks that will never run because the loop was abandoned.
    /// On pass > 0, current-pass incomplete replicas are excluded because
    /// they will be individually counted when processed via the skip path.
    fn abandoned_task_count(&self) -> usize {
        if self.current_pass == 0 {
            // Pass 0 is the initial run, not counted in extra_tasks_remaining
            self.extra_tasks_remaining
        } else {
            let completed_this_pass: u32 = self.block_completed_this_pass.values().sum();
            let current_pass_remaining = self
                .sub_dag
                .total_replicas
                .saturating_sub(completed_this_pass as usize);
            self.extra_tasks_remaining
                .saturating_sub(current_pass_remaining)
        }
    }

    /// Check if all replicas of a block have completed this pass.
    fn block_all_replicas_done(&self, block_id: BlockId, replicas: u32) -> bool {
        self.block_completed_this_pass
            .get(&block_id)
            .copied()
            .unwrap_or(0)
            >= replicas
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline(
    def: &PipelineDefinition,
    config: &AppConfig,
    agent_configs: PipelineAgentConfigs,
    client: reqwest::Client,
    cli_timeout_secs: u64,
    prompt_context: &PromptRuntimeContext,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    run_pipeline_with_provider_factory(
        def,
        config.pipeline_block_concurrency,
        agent_configs,
        prompt_context,
        output,
        progress_tx,
        cancel,
        |kind, cfg| {
            let mut dirs = vec![output.run_dir().display().to_string()];
            let pdir = profiles_dir();
            if pdir.is_dir() {
                dirs.push(pdir.display().to_string());
            }
            provider::create_provider(
                kind,
                cfg,
                client.clone(),
                config.default_max_tokens,
                config.max_history_messages,
                config.max_history_bytes,
                cli_timeout_secs,
                dirs,
            )
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_pipeline_with_provider_factory<F>(
    def: &PipelineDefinition,
    max_block_concurrency: u32,
    agent_configs: PipelineAgentConfigs,
    prompt_context: &PromptRuntimeContext,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
    provider_factory: F,
) -> Result<(), AppError>
where
    F: Fn(ProviderKind, &crate::config::ProviderConfig) -> Box<dyn provider::Provider>,
{
    let rt = build_runtime_table(def);
    if rt.entries.is_empty() {
        let _ = progress_tx.send(ProgressEvent::AllDone);
        return Ok(());
    }

    // Warn about missing profile files (soft — execution still proceeds)
    for block in &def.blocks {
        let missing = missing_profiles(&block.profiles);
        if !missing.is_empty() {
            let _ = output.append_error(&format!(
                "Warning: block {} has missing profile(s): {} — skipped at runtime",
                block.id,
                missing.join(", ")
            ));
        }
    }

    // Account for loop re-runs in total task count
    let loop_extra = loop_extra_tasks(def);
    let total_tasks = rt.entries.len() + loop_extra;

    let concurrency_sem = Arc::new(tokio::sync::Semaphore::new(if max_block_concurrency == 0 {
        tokio::sync::Semaphore::MAX_PERMITS
    } else {
        max_block_concurrency as usize
    }));

    // Build provider pool keyed by (agent, runtime_session_key)
    let mut provider_pool: ProviderPool = HashMap::new();
    for entry in &rt.entries {
        let pool_key = (entry.agent.clone(), entry.session_key.clone());
        if let std::collections::hash_map::Entry::Vacant(vacant) = provider_pool.entry(pool_key) {
            if let Some((kind, cfg, _use_cli)) = agent_configs.get(&entry.agent) {
                let p = provider_factory(*kind, cfg);
                vacant.insert(Arc::new(Mutex::new(p)));
            }
        }
    }

    // Build adjacency structures with replica-weighted in-degree (regular connections only)
    let graph = RegularGraph::from_def(def);
    let block_map: HashMap<BlockId, &PipelineBlock> =
        def.blocks.iter().map(|b| (b.id, b)).collect();
    let mut in_degree: HashMap<BlockId, usize> = def.blocks.iter().map(|b| (b.id, 0)).collect();
    let mut downstream: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
    for conn in &def.connections {
        let from_replicas = graph.replicas.get(&conn.from).copied().unwrap_or(1) as usize;
        *in_degree.entry(conn.to).or_default() += from_replicas;
        downstream.entry(conn.from).or_default().push(conn.to);
    }

    // Prepare loop data
    let prepared = prepare_loops(def);
    let block_to_loop: HashMap<BlockId, (BlockId, BlockId)> = prepared
        .as_ref()
        .map(|p| p.block_to_loop.clone())
        .unwrap_or_default();

    let terminals = terminal_blocks(def);
    let mut previous_terminal_outputs = String::new();

    for iteration in 1..=def.iterations {
        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        if progress_tx.is_closed() {
            break;
        }

        // Clear history for sessions configured to reset between iterations
        if iteration > 1 {
            for ((agent, session_key), provider_arc) in &provider_pool {
                let keep = rt
                    .keep_policy
                    .get(&(agent.clone(), session_key.clone()))
                    .copied()
                    .unwrap_or(true);
                if !keep {
                    let mut guard = provider_arc.lock().await;
                    guard.clear_history();
                }
            }
        }

        let mut current_in_degree = in_degree.clone();
        let mut failed_replicas: HashSet<u32> = HashSet::new();
        let mut failed_logical: HashSet<BlockId> = HashSet::new();
        let mut replica_outputs: HashMap<u32, String> = HashMap::new();
        let mut completed = 0usize;

        // Loop runtime state per iteration (clone from prepared sub_dags)
        let mut loop_state: HashMap<(BlockId, BlockId), LoopRuntimeState> =
            if let Some(ref p) = prepared {
                def.loop_connections
                    .iter()
                    .filter_map(|lc| {
                        let key = (lc.from, lc.to);
                        let sub_dag = p.sub_dags.get(&key)?.clone();
                        let extra = sub_dag.total_replicas * lc.count as usize;
                        Some((
                            key,
                            LoopRuntimeState {
                                remaining: lc.count,
                                current_pass: 0,
                                prompt: lc.prompt.clone(),
                                sub_dag,
                                block_completed_this_pass: HashMap::new(),
                                extra_tasks_remaining: extra,
                                abandoned: false,
                            },
                        ))
                    })
                    .collect()
            } else {
                HashMap::new()
            };
        // Track current loop pass per block for progress events
        let mut block_loop_pass: HashMap<BlockId, u32> = HashMap::new();

        // Seed ready queue with root blocks (logical IDs)
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<BlockId>();
        let mut roots: Vec<BlockId> = current_in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();
        roots.sort();
        for &id in &roots {
            let _ = ready_tx.send(id);
        }

        let mut tasks: tokio::task::JoinSet<(u32, Result<String, String>)> =
            tokio::task::JoinSet::new();
        let mut task_metadata: HashMap<tokio::task::Id, PipelineTaskMetadata> = HashMap::new();

        while completed < total_tasks {
            tokio::select! {
                Some(block_id) = ready_rx.recv() => {
                    if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }

                    let block = match block_map.get(&block_id).copied() {
                        Some(b) => b,
                        None => {
                            let replica_count = rt.logical_to_runtime.get(&block_id)
                                .map(|v| v.len()).unwrap_or(1);
                            completed += replica_count;
                            continue;
                        }
                    };
                    let replica_count = block.agents.len() * block.replicas as usize;

                    // Determine loop pass for this block
                    let current_loop_pass = if let Some(&key) = block_to_loop.get(&block_id) {
                        loop_state.get(&key).map(|ls| ls.current_pass).unwrap_or(0)
                    } else {
                        0
                    };
                    block_loop_pass.insert(block_id, current_loop_pass);

                    // Group-aware failure: skip only if ALL replicas of an upstream block failed
                    let failed_upstream: Vec<BlockId> = upstream_of(def, block_id)
                        .into_iter()
                        .filter(|u| failed_logical.contains(u))
                        .collect();

                    if !failed_upstream.is_empty() {
                        let reason = format!("all replicas of upstream Block {} failed", failed_upstream[0]);
                        if let Some(rids) = rt.logical_to_runtime.get(&block_id) {
                            for &rid in rids {
                                let info = &rt.entries[rid as usize];
                                let _ = progress_tx.send(ProgressEvent::BlockSkipped {
                                    block_id: rid,
                                    agent_name: info.agent.clone(),
                                    label: info.display_label.clone(),
                                    iteration,
                                    loop_pass: current_loop_pass,
                                    reason: reason.clone(),
                                });
                                failed_replicas.insert(rid);
                            }
                        }
                        failed_logical.insert(block_id);
                        completed += replica_count;
                        // For loop failure: abandon loop and apply deferred decrements
                        let in_loop = block_to_loop.get(&block_id).copied();
                        if let Some(key) = in_loop {
                            if let Some(ls) = loop_state.get_mut(&key) {
                                if !ls.abandoned {
                                    // First abandon: count remaining tasks, apply deferred edges
                                    completed += ls.abandoned_task_count();
                                    ls.extra_tasks_remaining = 0;
                                    ls.remaining = 0;
                                    ls.abandoned = true;
                                    // Mark all sub-DAG blocks as failed
                                    for &bid in &ls.sub_dag.blocks {
                                        failed_logical.insert(bid);
                                    }
                                    // Apply all deferred external edges (once)
                                    let all_deferred: Vec<(BlockId, usize)> = ls.sub_dag
                                        .deferred_external_edges
                                        .values()
                                        .flatten()
                                        .copied()
                                        .collect();
                                    for (child, weight) in all_deferred {
                                        if let Some(deg) = current_in_degree.get_mut(&child) {
                                            *deg = deg.saturating_sub(weight);
                                            if *deg == 0 {
                                                let _ = ready_tx.send(child);
                                            }
                                        }
                                    }
                                }
                                // Already abandoned: just count this block's replicas (done above)
                            }
                        }
                        // Propagate to downstream children
                        if let Some(children) = downstream.get(&block_id) {
                            for &child in children {
                                if let Some(key) = in_loop {
                                    if let Some(ls) = loop_state.get(&key) {
                                        if !ls.sub_dag.blocks.contains(&child) {
                                            // External child: skip — handled by deferred edges
                                            continue;
                                        }
                                    }
                                }
                                // Internal sub-DAG child or non-loop child: decrement
                                if let Some(deg) = current_in_degree.get_mut(&child) {
                                    *deg = deg.saturating_sub(replica_count);
                                    if *deg == 0 {
                                        let _ = ready_tx.send(child);
                                    }
                                }
                            }
                        }
                        continue;
                    }

                    // Check provider availability for all agents in this block
                    let rids = match rt.logical_to_runtime.get(&block_id) {
                        Some(rids) => rids,
                        None => { completed += replica_count; continue; }
                    };

                    let any_missing_agent = block.agents.iter().any(|a| !agent_configs.contains_key(a));
                    if any_missing_agent {
                        for &rid in rids {
                            let info = &rt.entries[rid as usize];
                            let _ = progress_tx.send(ProgressEvent::BlockError {
                                block_id: rid,
                                agent_name: info.agent.clone(),
                                label: info.display_label.clone(),
                                iteration,
                                loop_pass: current_loop_pass,
                                error: "No provider available".into(),
                                details: None,
                            });
                            failed_replicas.insert(rid);
                        }
                        failed_logical.insert(block_id);
                        completed += replica_count;
                        // Loop-aware abandon + downstream propagation
                        let in_loop = block_to_loop.get(&block_id).copied();
                        if let Some(key) = in_loop {
                            if let Some(ls) = loop_state.get_mut(&key) {
                                if !ls.abandoned {
                                    completed += ls.abandoned_task_count();
                                    ls.extra_tasks_remaining = 0;
                                    ls.remaining = 0;
                                    ls.abandoned = true;
                                    for &bid in &ls.sub_dag.blocks {
                                        failed_logical.insert(bid);
                                    }
                                    let all_deferred: Vec<(BlockId, usize)> = ls.sub_dag
                                        .deferred_external_edges
                                        .values()
                                        .flatten()
                                        .copied()
                                        .collect();
                                    for (child, weight) in all_deferred {
                                        if let Some(deg) = current_in_degree.get_mut(&child) {
                                            *deg = deg.saturating_sub(weight);
                                            if *deg == 0 {
                                                let _ = ready_tx.send(child);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if let Some(children) = downstream.get(&block_id) {
                            for &child in children {
                                if let Some(key) = in_loop {
                                    if let Some(ls) = loop_state.get(&key) {
                                        if !ls.sub_dag.blocks.contains(&child) {
                                            continue;
                                        }
                                    }
                                }
                                if let Some(deg) = current_in_degree.get_mut(&child) {
                                    *deg = deg.saturating_sub(replica_count);
                                    if *deg == 0 {
                                        let _ = ready_tx.send(child);
                                    }
                                }
                            }
                        }
                        continue;
                    }

                    // Spawn one task per replica (agents × replicas)
                    for &rid in rids {
                        let info = &rt.entries[rid as usize];

                        // Per-replica use_cli and message building (agents may differ)
                        let use_cli = agent_configs
                            .get(&info.agent)
                            .map(|(_, _, cli)| *cli)
                            .unwrap_or(false);

                        let message = if current_loop_pass > 0 {
                            if let Some(&(loop_from, loop_to)) = block_to_loop.get(&block_id) {
                                if block_id == loop_to {
                                    let ls = loop_state.get(&(loop_from, loop_to)).unwrap();
                                    let lc = def.loop_connections.iter()
                                        .find(|lc| lc.from == loop_from && lc.to == loop_to)
                                        .unwrap();
                                    build_loop_rerun_message_v2(
                                        block, use_cli, def,
                                        &ls.sub_dag.blocks, loop_from,
                                        current_loop_pass, lc.count + 1,
                                        &ls.prompt, &replica_outputs, &rt, output,
                                        iteration, &previous_terminal_outputs,
                                        prompt_context, &block_to_loop, &block_loop_pass,
                                    )
                                } else {
                                    build_pipeline_block_message(
                                        block, use_cli,
                                        &PipelineMessageContext {
                                            def, iteration,
                                            block_outputs: &replica_outputs,
                                            previous_terminal_outputs: &previous_terminal_outputs,
                                            output, prompt_context, runtime_table: &rt,
                                            block_to_loop: &block_to_loop,
                                            block_loop_pass: &block_loop_pass,
                                        },
                                    )
                                }
                            } else {
                                return Err(AppError::Pipeline("loop_pass > 0 but block not in a loop".into()))
                            }
                        } else {
                            build_pipeline_block_message(
                                block, use_cli,
                                &PipelineMessageContext {
                                    def, iteration,
                                    block_outputs: &replica_outputs,
                                    previous_terminal_outputs: &previous_terminal_outputs,
                                    output, prompt_context, runtime_table: &rt,
                                    block_to_loop: &block_to_loop,
                                    block_loop_pass: &block_loop_pass,
                                },
                            )
                        };

                        let pool_key = (info.agent.clone(), info.session_key.clone());
                        let provider_arc = match provider_pool.get(&pool_key) {
                            Some(p) => p.clone(),
                            None => {
                                let _ = progress_tx.send(ProgressEvent::BlockError {
                                    block_id: rid,
                                    agent_name: info.agent.clone(),
                                    label: info.display_label.clone(),
                                    iteration,
                                    loop_pass: current_loop_pass,
                                    error: "No provider available".into(),
                                    details: None,
                                });
                                failed_replicas.insert(rid);
                                completed += 1;
                                if let Some(children) = downstream.get(&block_id) {
                                    let in_loop = block_to_loop.get(&block_id).copied();
                                    for &child in children {
                                        // Skip external children if in a loop (deferred handles them)
                                        if let Some(key) = in_loop {
                                            if let Some(ls) = loop_state.get(&key) {
                                                if !ls.sub_dag.blocks.contains(&child) {
                                                    continue;
                                                }
                                            }
                                        }
                                        if let Some(deg) = current_in_degree.get_mut(&child) {
                                            debug_assert!(*deg > 0, "in-degree underflow for block {child}");
                                            *deg = deg.saturating_sub(1);
                                            if *deg == 0 {
                                                let _ = ready_tx.send(child);
                                            }
                                        }
                                    }
                                }
                                continue;
                            }
                        };

                        let _ = progress_tx.send(ProgressEvent::BlockStarted {
                            block_id: rid,
                            agent_name: info.agent.clone(),
                            label: info.display_label.clone(),
                            iteration,
                            loop_pass: current_loop_pass,
                        });

                        let ptx = progress_tx.clone();
                        let cancel_clone = cancel.clone();
                        let task_output = output.clone();
                        let task_filename = loop_replica_filename(info, iteration, current_loop_pass);
                        let task_agent_name = info.agent.clone();
                        let task_label = info.display_label.clone();
                        let message_clone = message;
                        let sem_clone = concurrency_sem.clone();
                        let task_loop_pass = current_loop_pass;
                        let task_handle = tasks.spawn(async move {
                            let _permit = sem_clone.acquire().await.expect("semaphore closed");
                            let mut guard = provider_arc.lock().await;

                            let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
                            guard.set_live_log_sender(Some(live_tx));

                            let bid = rid;
                            let an = task_agent_name.clone();
                            let it = iteration;
                            let lp = task_loop_pass;
                            let ptx2 = ptx.clone();
                            let live_forward = tokio::spawn(async move {
                                while let Some(line) = live_rx.recv().await {
                                    let _ = ptx2.send(ProgressEvent::BlockLog {
                                        block_id: bid,
                                        agent_name: an.clone(),
                                        iteration: it,
                                        loop_pass: lp,
                                        message: format!("CLI {line}"),
                                    });
                                }
                            });

                            let result = tokio::select! {
                                res = crate::execution::send_with_streaming(
                                    &mut **guard,
                                    &message_clone,
                                    &ptx,
                                    {
                                        let agent_name = task_agent_name.clone();
                                        let bid = rid;
                                        let it = iteration;
                                        let lp = task_loop_pass;
                                        move |chunk| ProgressEvent::BlockStreamChunk {
                                            block_id: bid,
                                            agent_name: agent_name.clone(),
                                            iteration: it,
                                            loop_pass: lp,
                                            chunk,
                                        }
                                    },
                                ) => Some(res),
                                _ = wait_for_cancel(&cancel_clone) => None
                            };

                            guard.set_live_log_sender(None);
                            let cancelled = result.is_none();
                            drop(guard);
                            finish_live_log_forwarder(live_forward, cancelled).await;

                            match result {
                                None => (rid, Err("Cancelled".to_string())),
                                Some(Ok(resp)) => {
                                    for log in &resp.debug_logs {
                                        let _ = ptx.send(ProgressEvent::BlockLog {
                                            block_id: rid,
                                            agent_name: task_agent_name.clone(),
                                            iteration,
                                            loop_pass: task_loop_pass,
                                            message: log.clone(),
                                        });
                                    }
                                    let path = task_output.run_dir().join(&task_filename);
                                    if let Err(e) = tokio::fs::write(&path, &resp.content).await {
                                        let error = format!("Failed to write output: {e}");
                                        let _ = task_output.append_error(&format!(
                                            "runtime {rid} {task_agent_name} iter{iteration}: {error}"
                                        ));
                                        let _ = ptx.send(ProgressEvent::BlockError {
                                            block_id: rid,
                                            agent_name: task_agent_name,
                                            label: task_label,
                                            iteration,
                                            loop_pass: task_loop_pass,
                                            error: error.clone(),
                                            details: Some(error.clone()),
                                        });
                                        (rid, Err(error))
                                    } else {
                                        let _ = ptx.send(ProgressEvent::BlockFinished {
                                            block_id: rid,
                                            agent_name: task_agent_name,
                                            label: task_label,
                                            iteration,
                                            loop_pass: task_loop_pass,
                                        });
                                        (rid, Ok(resp.content))
                                    }
                                }
                                Some(Err(e)) => {
                                    let error = e.to_string();
                                    let _ = task_output.append_error(&format!(
                                        "runtime {rid} {task_agent_name} iter{iteration}: {error}"
                                    ));
                                    let _ = ptx.send(ProgressEvent::BlockError {
                                        block_id: rid,
                                        agent_name: task_agent_name,
                                        label: task_label,
                                        iteration,
                                        loop_pass: task_loop_pass,
                                        error: error.clone(),
                                        details: Some(error.clone()),
                                    });
                                    (rid, Err(error))
                                }
                            }
                        });
                        task_metadata.insert(
                            task_handle.id(),
                            PipelineTaskMetadata {
                                runtime_id: rid,
                                source_block_id: block_id,
                                agent_name: info.agent.clone(),
                                label: info.display_label.clone(),
                                iteration,
                                loop_pass: current_loop_pass,
                            },
                        );
                    }
                }
                Some(result) = tasks.join_next() => {
                    completed += 1;

                    // Normalize panics into the same shape as normal results so
                    // both paths share the loop-aware downstream propagation.
                    let (source_id, this_loop_pass) = match &result {
                        Ok((runtime_id, _)) => {
                            let sid = rt.entries[*runtime_id as usize].source_block_id;
                            (Some(sid), block_loop_pass.get(&sid).copied().unwrap_or(0))
                        }
                        Err(join_error) => {
                            if let Some(metadata) = task_metadata.remove(&join_error.id()) {
                                let error = format!("Pipeline worker panicked: {join_error}");
                                let _ = progress_tx.send(ProgressEvent::BlockError {
                                    block_id: metadata.runtime_id,
                                    agent_name: metadata.agent_name.clone(),
                                    label: metadata.label.clone(),
                                    iteration: metadata.iteration,
                                    loop_pass: metadata.loop_pass,
                                    error: error.clone(),
                                    details: Some(error.clone()),
                                });
                                let _ = output.append_error(&format!(
                                    "runtime {} {} iter{}: {}",
                                    metadata.runtime_id,
                                    metadata.agent_name,
                                    metadata.iteration,
                                    error
                                ));
                                failed_replicas.insert(metadata.runtime_id);
                                // Remove stale output so a previous pass's content
                                // is not mistaken for current-pass feedback.
                                replica_outputs.remove(&metadata.runtime_id);
                                let src = metadata.source_block_id;
                                if let Some(rids) = rt.logical_to_runtime.get(&src) {
                                    if rids.iter().all(|r| failed_replicas.contains(r)) {
                                        failed_logical.insert(src);
                                    }
                                }
                                (Some(src), block_loop_pass.get(&src).copied().unwrap_or(0))
                            } else {
                                let _ = output.append_error(&format!(
                                    "pipeline panic could not be attributed to a block: {join_error}"
                                ));
                                (None, 0)
                            }
                        }
                    };

                    // Handle normal (non-panic) outcome recording
                    if let Ok((runtime_id, ref outcome)) = result {
                        let sid = rt.entries[runtime_id as usize].source_block_id;
                        match outcome {
                            Ok(content) => {
                                replica_outputs.insert(runtime_id, content.clone());
                            }
                            Err(_) => {
                                failed_replicas.insert(runtime_id);
                                // Remove stale output so a previous pass's content
                                // is not mistaken for current-pass feedback.
                                replica_outputs.remove(&runtime_id);
                                if let Some(rids) = rt.logical_to_runtime.get(&sid) {
                                    if rids.iter().all(|r| failed_replicas.contains(r)) {
                                        failed_logical.insert(sid);
                                    }
                                }
                            }
                        }
                    }

                    // --- Shared loop-aware downstream propagation ---
                    let Some(source_id) = source_id else { continue };

                    if let Some(&(loop_from, loop_to)) = block_to_loop.get(&source_id) {
                        let key = (loop_from, loop_to);
                        let source_replicas = graph.replicas.get(&source_id).copied().unwrap_or(1);

                        // Track per-block completion
                        if let Some(ls) = loop_state.get_mut(&key) {
                            *ls.block_completed_this_pass.entry(source_id).or_default() += 1;
                            // Decrement extra_tasks_remaining for re-run work
                            if this_loop_pass > 0 {
                                ls.extra_tasks_remaining = ls.extra_tasks_remaining.saturating_sub(1);
                            }
                        }

                        if source_id == loop_from {
                            // CASE A: `from` (feedback source) completed a replica
                            let all_from_done = loop_state.get(&key)
                                .map(|ls| ls.block_all_replicas_done(source_id, source_replicas))
                                .unwrap_or(true);

                            if all_from_done {
                                let loop_failed = failed_logical.contains(&loop_from)
                                    || loop_state.get(&key).map(|ls| {
                                        ls.sub_dag.blocks.iter().any(|b| failed_logical.contains(b))
                                    }).unwrap_or(false);

                                if loop_failed {
                                    // Abandon loop (only if not already abandoned)
                                    if let Some(ls) = loop_state.get_mut(&key) {
                                        if !ls.abandoned {
                                            completed += ls.abandoned_task_count();
                                            ls.extra_tasks_remaining = 0;
                                            ls.remaining = 0;
                                            ls.abandoned = true;
                                            // Apply all deferred external edges (once)
                                            let all_deferred: Vec<(BlockId, usize)> = ls.sub_dag
                                                .deferred_external_edges
                                                .values()
                                                .flatten()
                                                .copied()
                                                .collect();
                                            for (child, weight) in all_deferred {
                                                if let Some(deg) = current_in_degree.get_mut(&child) {
                                                    *deg = deg.saturating_sub(weight);
                                                    if *deg == 0 {
                                                        let _ = ready_tx.send(child);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else if let Some(ls) = loop_state.get_mut(&key) {
                                    if ls.remaining > 0 {
                                        // More loop passes — reset sub-DAG and queue `to`
                                        ls.remaining -= 1;
                                        ls.current_pass += 1;
                                        ls.block_completed_this_pass.clear();
                                        // Clear stale failure/output state for sub-DAG blocks
                                        // so that failures from pass N don't poison pass N+1.
                                        // Keep `from`'s outputs — needed as feedback for `to`.
                                        for &bid in &ls.sub_dag.blocks {
                                            failed_logical.remove(&bid);
                                            if let Some(rids) = rt.logical_to_runtime.get(&bid) {
                                                for &rid in rids {
                                                    failed_replicas.remove(&rid);
                                                    if bid != loop_from {
                                                        replica_outputs.remove(&rid);
                                                    }
                                                }
                                            }
                                        }
                                        // Clear provider history for sub-DAG sessions with keep_across_loop_passes=false
                                        {
                                            let mut cleared: HashSet<(String, String)> =
                                                HashSet::new();
                                            for &bid in &ls.sub_dag.blocks {
                                                if let Some(rids) =
                                                    rt.logical_to_runtime.get(&bid)
                                                {
                                                    for &rid in rids {
                                                        let info =
                                                            &rt.entries[rid as usize];
                                                        let pool_key = (
                                                            info.agent.clone(),
                                                            info.session_key.clone(),
                                                        );
                                                        if cleared.contains(&pool_key) {
                                                            continue;
                                                        }
                                                        let keep = rt
                                                            .keep_loop_policy
                                                            .get(&pool_key)
                                                            .copied()
                                                            .unwrap_or(true);
                                                        if !keep {
                                                            if let Some(p) =
                                                                provider_pool.get(&pool_key)
                                                            {
                                                                let mut guard =
                                                                    p.lock().await;
                                                                guard.clear_history();
                                                            }
                                                            cleared.insert(pool_key);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        // Reset all sub-DAG blocks' in-degrees
                                        for (&bid, &deg) in &ls.sub_dag.internal_in_degree {
                                            current_in_degree.insert(bid, deg);
                                        }
                                        // Queue `to` (in-degree 0)
                                        let _ = ready_tx.send(loop_to);
                                        // Do NOT propagate to from's external downstream
                                    } else {
                                        // Loop done — apply all deferred external edges
                                        let all_deferred: Vec<(BlockId, usize)> = ls.sub_dag
                                            .deferred_external_edges
                                            .values()
                                            .flatten()
                                            .copied()
                                            .collect();
                                        for (child, weight) in all_deferred {
                                            if let Some(deg) = current_in_degree.get_mut(&child) {
                                                *deg = deg.saturating_sub(weight);
                                                if *deg == 0 {
                                                    let _ = ready_tx.send(child);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // Individual from replicas don't propagate externally
                        } else {
                            // CASE B: `to` or intermediate sub-DAG block completed
                            // Decrement internal children normally, skip external
                            if let Some(children) = downstream.get(&source_id) {
                                for &child in children {
                                    let in_sub_dag = loop_state.get(&key)
                                        .map(|ls| ls.sub_dag.blocks.contains(&child))
                                        .unwrap_or(false);
                                    if in_sub_dag {
                                        // Internal child: decrement normally
                                        if let Some(deg) = current_in_degree.get_mut(&child) {
                                            debug_assert!(*deg > 0, "in-degree underflow for block {child}");
                                            *deg = deg.saturating_sub(1);
                                            if *deg == 0 {
                                                let _ = ready_tx.send(child);
                                            }
                                        }
                                    }
                                    // External children: deferred (handled at loop exit)
                                }
                            }
                        }
                    } else {
                        // CASE C: Block not in any loop — normal downstream propagation
                        if let Some(children) = downstream.get(&source_id) {
                            for &child in children {
                                if let Some(deg) = current_in_degree.get_mut(&child) {
                                    debug_assert!(*deg > 0, "in-degree underflow for block {child}");
                                    *deg = deg.saturating_sub(1);
                                    if *deg == 0 {
                                        let _ = ready_tx.send(child);
                                    }
                                }
                            }
                        }
                    }
                }
                else => break,
            }
        }

        let _ = progress_tx.send(ProgressEvent::IterationComplete { iteration });

        // Collect labeled terminal outputs for next iteration
        previous_terminal_outputs.clear();
        for &tid in &terminals {
            if let Some(rids) = rt.logical_to_runtime.get(&tid) {
                let needs_label = block_map
                    .get(&tid)
                    .map(|b| b.replicas > 1 || b.agents.len() > 1)
                    .unwrap_or(false);
                for &rid in rids {
                    if let Some(content) = replica_outputs.get(&rid) {
                        if !previous_terminal_outputs.is_empty() {
                            previous_terminal_outputs.push_str("\n\n---\n\n");
                        }
                        if needs_label {
                            let info = &rt.entries[rid as usize];
                            previous_terminal_outputs.push_str(&format!(
                                "--- Output from {} ---\n{}",
                                info.display_label, content
                            ));
                        } else {
                            previous_terminal_outputs.push_str(content);
                        }
                    }
                }
            }
        }

        // Free full LLM responses now that terminal outputs have been collected
        replica_outputs.clear();

        if previous_terminal_outputs.len() > MAX_TERMINAL_OUTPUTS_BYTES {
            // Floor to a char boundary to avoid panicking on multibyte UTF-8
            let mut end = MAX_TERMINAL_OUTPUTS_BYTES;
            while end > 0 && !previous_terminal_outputs.is_char_boundary(end) {
                end -= 1;
            }
            previous_terminal_outputs.truncate(end);
        }
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

fn block_label(block: &PipelineBlock) -> String {
    if block.name.trim().is_empty() {
        format!("Block {}", block.id)
    } else {
        block.name.clone()
    }
}

fn resolve_profile_instructions(profiles: &[String], use_cli: bool) -> String {
    resolve_profile_instructions_from_dir(&profiles_dir(), profiles, use_cli)
}

pub fn is_valid_profile_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
        && name != "."
}

fn resolve_profile_instructions_from_dir(dir: &Path, profiles: &[String], use_cli: bool) -> String {
    if profiles.is_empty() {
        return String::new();
    }
    if use_cli {
        let mut paths = Vec::new();
        for name in profiles {
            if !is_valid_profile_name(name) {
                continue;
            }
            let path = dir.join(format!("{name}.md"));
            if path.is_file() {
                paths.push(format!("- {}", path.display()));
            }
        }
        if paths.is_empty() {
            return String::new();
        }
        let mut out = String::from("Read these profile instruction files:\n");
        for p in &paths {
            out.push_str(p);
            out.push('\n');
        }
        out.push_str("Follow the instructions in each file.\n\n");
        out
    } else {
        let mut content = String::new();
        for name in profiles {
            if !is_valid_profile_name(name) {
                continue;
            }
            let path = dir.join(format!("{name}.md"));
            if let Ok(text) = std::fs::read_to_string(&path) {
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str(&format!("--- Profile: {name} ---\n{text}"));
            }
        }
        if !content.is_empty() {
            content.push_str("\n\n");
        }
        content
    }
}

/// Returns profile names that are configured on a block but missing from disk.
fn missing_profiles(profiles: &[String]) -> Vec<String> {
    if profiles.is_empty() {
        return Vec::new();
    }
    let dir = profiles_dir();
    profiles
        .iter()
        .filter(|p| is_valid_profile_name(p) && !dir.join(format!("{p}.md")).is_file())
        .cloned()
        .collect()
}

fn build_pipeline_block_message(
    block: &PipelineBlock,
    use_cli: bool,
    context: &PipelineMessageContext<'_>,
) -> String {
    let is_root = upstream_of(context.def, block.id).is_empty();
    let base_message = if is_root && context.iteration == 1 {
        if block.prompt.is_empty() {
            context.def.initial_prompt.clone()
        } else {
            format!("{}\n\n{}", block.prompt, context.def.initial_prompt)
        }
    } else if is_root {
        let base = if block.prompt.is_empty() {
            context.def.initial_prompt.clone()
        } else {
            block.prompt.clone()
        };
        format!(
            "{base}\n\n--- Previous iteration outputs ---\n{}",
            context.previous_terminal_outputs
        )
    } else {
        let upstream_ids = upstream_of(context.def, block.id);
        let prefix = if block.prompt.is_empty() {
            String::new()
        } else {
            format!("{}\n\n", block.prompt)
        };
        if use_cli {
            let mut file_refs = String::new();
            for uid in &upstream_ids {
                if let Some(rids) = context.runtime_table.logical_to_runtime.get(uid) {
                    for &rid in rids {
                        let info = &context.runtime_table.entries[rid as usize];
                        let filename = loop_aware_upstream_filename(
                            info,
                            context.iteration,
                            *uid,
                            context.block_to_loop,
                            context.block_loop_pass,
                        );
                        let path = context.output.run_dir().join(&filename);
                        if path.exists() {
                            file_refs.push_str(&format!("- {}\n", path.display()));
                        }
                    }
                }
            }
            format!(
                "{prefix}Read these upstream output files:\n{file_refs}\nRead each file before responding."
            )
        } else {
            let mut upstream_content = String::new();
            for uid in &upstream_ids {
                let upstream_block = context.def.blocks.iter().find(|b| b.id == *uid);
                if let Some(rids) = context.runtime_table.logical_to_runtime.get(uid) {
                    for &rid in rids {
                        if let Some(content) = context.block_outputs.get(&rid) {
                            if !upstream_content.is_empty() {
                                upstream_content.push_str("\n\n---\n\n");
                            }
                            let needs_label = upstream_block
                                .map(|b| b.replicas > 1 || b.agents.len() > 1)
                                .unwrap_or(false);
                            if needs_label {
                                let info = &context.runtime_table.entries[rid as usize];
                                upstream_content.push_str(&format!(
                                    "--- Output from {} ---\n{}",
                                    info.display_label, content
                                ));
                            } else {
                                upstream_content.push_str(content);
                            }
                        }
                    }
                }
            }
            format!("{prefix}--- Upstream outputs ---\n{upstream_content}")
        }
    };

    let profile_prefix = resolve_profile_instructions(&block.profiles, use_cli);
    let full_message = if profile_prefix.is_empty() {
        base_message
    } else {
        format!("{profile_prefix}{base_message}")
    };
    context
        .prompt_context
        .augment_prompt_for_agent(&full_message, use_cli)
}

/// Loop-aware filename resolver for CLI upstream references.
fn loop_aware_upstream_filename(
    info: &RuntimeReplicaInfo,
    iteration: u32,
    upstream_block_id: BlockId,
    block_to_loop: &HashMap<BlockId, (BlockId, BlockId)>,
    block_loop_pass: &HashMap<BlockId, u32>,
) -> String {
    if block_to_loop.contains_key(&upstream_block_id) {
        let pass = block_loop_pass
            .get(&upstream_block_id)
            .copied()
            .unwrap_or(0);
        if pass == 0 {
            replica_filename(info, iteration)
        } else {
            loop_replica_filename(info, iteration, pass)
        }
    } else {
        replica_filename(info, iteration)
    }
}

/// Message builder for `to` (restart target) block on re-run passes.
/// Includes external upstream context + loop header + loop prompt + block prompt + `from`'s feedback.
#[allow(clippy::too_many_arguments)]
fn build_loop_rerun_message_v2(
    block: &PipelineBlock,
    use_cli: bool,
    def: &PipelineDefinition,
    loop_sub_dag_blocks: &HashSet<BlockId>,
    from_block_id: BlockId,
    current_pass: u32,
    total_passes: u32,
    loop_prompt: &str,
    replica_outputs: &HashMap<u32, String>,
    runtime_table: &RuntimeReplicaTable,
    output: &OutputManager,
    iteration: u32,
    previous_terminal_outputs: &str,
    prompt_context: &PromptRuntimeContext,
    block_to_loop: &HashMap<BlockId, (BlockId, BlockId)>,
    block_loop_pass: &HashMap<BlockId, u32>,
) -> String {
    let mut message = String::new();

    // External upstream context (parents outside the loop sub-DAG)
    let external_parents: Vec<BlockId> = upstream_of(def, block.id)
        .into_iter()
        .filter(|uid| !loop_sub_dag_blocks.contains(uid))
        .collect();

    let is_root = upstream_of(def, block.id).is_empty();
    if is_root {
        // Root restart target: match build_pipeline_block_message root semantics.
        // On iteration 1: initial_prompt is always included.
        // On iteration > 1 with block prompt: only block.prompt + prev outputs
        //   (initial_prompt omitted — block.prompt is appended later).
        // On iteration > 1 without block prompt: initial_prompt + prev outputs.
        if (iteration == 1 || block.prompt.is_empty()) && !def.initial_prompt.is_empty() {
            message.push_str(&def.initial_prompt);
            message.push_str("\n\n");
        }
        if iteration > 1 && !previous_terminal_outputs.is_empty() {
            message.push_str("--- Previous iteration outputs ---\n");
            message.push_str(previous_terminal_outputs);
            message.push_str("\n\n");
        }
    } else if !external_parents.is_empty() {
        // Non-root with external parents: include their outputs
        if use_cli {
            let mut file_refs = String::new();
            for &uid in &external_parents {
                if let Some(rids) = runtime_table.logical_to_runtime.get(&uid) {
                    for &rid in rids {
                        let info = &runtime_table.entries[rid as usize];
                        let filename = loop_aware_upstream_filename(
                            info,
                            iteration,
                            uid,
                            block_to_loop,
                            block_loop_pass,
                        );
                        let path = output.run_dir().join(&filename);
                        if path.exists() {
                            file_refs.push_str(&format!("- {}\n", path.display()));
                        }
                    }
                }
            }
            if !file_refs.is_empty() {
                message.push_str("Read these upstream output files:\n");
                message.push_str(&file_refs);
                message.push('\n');
            }
        } else {
            let mut upstream_content = String::new();
            for &uid in &external_parents {
                if let Some(rids) = runtime_table.logical_to_runtime.get(&uid) {
                    for &rid in rids {
                        if let Some(content) = replica_outputs.get(&rid) {
                            if !upstream_content.is_empty() {
                                upstream_content.push_str("\n\n---\n\n");
                            }
                            let upstream_block = def.blocks.iter().find(|b| b.id == uid);
                            let needs_label = upstream_block
                                .map(|b| b.replicas > 1 || b.agents.len() > 1)
                                .unwrap_or(false);
                            if needs_label {
                                let info = &runtime_table.entries[rid as usize];
                                upstream_content.push_str(&format!(
                                    "--- Output from {} ---\n{}",
                                    info.display_label, content
                                ));
                            } else {
                                upstream_content.push_str(content);
                            }
                        }
                    }
                }
            }
            if !upstream_content.is_empty() {
                message.push_str("--- Upstream context ---\n");
                message.push_str(&upstream_content);
                message.push_str("\n\n");
            }
        }
    }

    // Loop iteration header
    message.push_str(&format!(
        "[Loop iteration {} of {}]\n\n",
        current_pass + 1,
        total_passes
    ));

    // Loop prompt if set
    if !loop_prompt.is_empty() {
        message.push_str(loop_prompt);
        message.push_str("\n\n");
    }

    // Block's own prompt
    if !block.prompt.is_empty() {
        message.push_str(&block.prompt);
        message.push_str("\n\n");
    }

    // `from`'s feedback (from previous pass)
    if let Some(from_rids) = runtime_table.logical_to_runtime.get(&from_block_id) {
        if use_cli {
            message.push_str("Read these feedback output files:\n");
            for &rid in from_rids {
                let info = &runtime_table.entries[rid as usize];
                if replica_outputs.contains_key(&rid) {
                    // `to` on pass P needs `from`'s output from pass P-1
                    let feedback_pass = current_pass.saturating_sub(1);
                    let filename = loop_replica_filename(info, iteration, feedback_pass);
                    let path = output.run_dir().join(&filename);
                    if path.exists() {
                        message.push_str(&format!("- {}\n", path.display()));
                    }
                }
            }
            message.push_str("\nRead each file before responding.");
        } else {
            message.push_str("--- Feedback from previous pass ---\n");
            for &rid in from_rids {
                if let Some(content) = replica_outputs.get(&rid) {
                    if from_rids.len() > 1 {
                        let info = &runtime_table.entries[rid as usize];
                        message.push_str(&format!(
                            "--- Output from {} ---\n{}",
                            info.display_label, content
                        ));
                    } else {
                        message.push_str(content);
                    }
                    message.push('\n');
                }
            }
        }
    }

    let profile_prefix = resolve_profile_instructions(&block.profiles, use_cli);
    let full_message = if profile_prefix.is_empty() {
        message
    } else {
        format!("{profile_prefix}{message}")
    };
    prompt_context.augment_prompt_for_agent(&full_message, use_cli)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
// Finalization
// ---------------------------------------------------------------------------

pub(crate) enum FinalizationRunScope {
    SingleRun {
        run_id: u32,
        run_dir: PathBuf,
    },
    Batch {
        successful_runs: Vec<(u32, PathBuf)>,
    },
}

/// Build runtime entries for finalization blocks.
/// For single-run or per-run within a batch, `run_scope_id` is `Some(run_id)`.
/// For all-runs batch blocks, `run_scope` is `None`.
pub(crate) fn build_finalization_runtime_entries(
    def: &PipelineDefinition,
    run_scope: &FinalizationRunScope,
    next_id_start: u32,
) -> Vec<RuntimeReplicaInfo> {
    let mut entries = Vec::new();
    let mut next_id = next_id_start;

    // Determine per-run block set
    let per_run_ids: HashSet<BlockId> = def
        .finalization_blocks
        .iter()
        .filter(|b| def.is_per_run_finalization_block(b.id))
        .map(|b| b.id)
        .collect();

    // Determine successful runs
    let runs: Vec<(u32, &PathBuf)> = match run_scope {
        FinalizationRunScope::SingleRun { run_id, run_dir } => vec![(*run_id, run_dir)],
        FinalizationRunScope::Batch { successful_runs } => {
            successful_runs.iter().map(|(id, p)| (*id, p)).collect()
        }
    };

    for block in &def.finalization_blocks {
        let is_per_run = per_run_ids.contains(&block.id);
        let blabel = block_label(block);
        let block_name_key = if block.name.trim().is_empty() {
            format!("block{}", block.id)
        } else {
            format!(
                "{}_b{}",
                OutputManager::sanitize_session_name(&block.name),
                block.id
            )
        };
        let num_agents = block.agents.len();
        let num_replicas = block.replicas;
        let multi_agent = num_agents > 1;
        let multi_replica = num_replicas > 1;

        // For per-run blocks, create entries per run; for all-runs blocks, one set
        let scope_list: Vec<Option<u32>> = if is_per_run {
            runs.iter().map(|(id, _)| Some(*id)).collect()
        } else {
            vec![None]
        };

        for scope_id in &scope_list {
            for agent in &block.agents {
                let agent_file_key = OutputManager::sanitize_session_name(agent);
                for ri in 0..num_replicas {
                    let runtime_id = next_id;
                    next_id += 1;

                    let scope_suffix = scope_id
                        .map(|id| format!(" [run {id}]"))
                        .unwrap_or_default();
                    let display_label = match (multi_agent, multi_replica) {
                        (false, false) => format!("Fin: {blabel}{scope_suffix}"),
                        (false, true) => {
                            format!("Fin: {} (r{}){}", blabel, ri + 1, scope_suffix)
                        }
                        (true, false) => {
                            format!("Fin: {blabel} ({agent}){scope_suffix}")
                        }
                        (true, true) => {
                            format!("Fin: {} ({} r{}){}", blabel, agent, ri + 1, scope_suffix)
                        }
                    };

                    let session_key = format!("__fin_block_{}", block.id);

                    // Add _run{id} suffix in batch mode so each run's output
                    // is distinguishable, even if only one run succeeded.
                    // Single-run mode omits the suffix since there's only one run.
                    let is_batch = matches!(run_scope, FinalizationRunScope::Batch { .. });
                    let run_suffix = if is_batch {
                        scope_id.map(|id| format!("_run{id}")).unwrap_or_default()
                    } else {
                        String::new()
                    };
                    let filename_stem = match (multi_agent, multi_replica) {
                        (false, false) => {
                            format!("{block_name_key}_{agent_file_key}{run_suffix}")
                        }
                        (false, true) => {
                            format!(
                                "{}_{}_r{}{}",
                                block_name_key,
                                agent_file_key,
                                ri + 1,
                                run_suffix
                            )
                        }
                        (true, false) => {
                            format!("{block_name_key}_{agent_file_key}{run_suffix}")
                        }
                        (true, true) => {
                            format!(
                                "{}_{}_r{}{}",
                                block_name_key,
                                agent_file_key,
                                ri + 1,
                                run_suffix
                            )
                        }
                    };

                    entries.push(RuntimeReplicaInfo {
                        runtime_id,
                        source_block_id: block.id,
                        replica_index: ri,
                        phase: RuntimePhase::Finalization,
                        run_scope: *scope_id,
                        agent: agent.clone(),
                        display_label,
                        session_key,
                        filename_stem,
                    });
                }
            }
        }
    }
    entries
}

/// Count finalization tasks for step accounting.
pub(crate) fn finalization_task_count(def: &PipelineDefinition) -> usize {
    if !def.has_finalization() {
        return 0;
    }
    def.finalization_blocks
        .iter()
        .map(|b| b.agents.len() * b.replicas.max(1) as usize)
        .sum()
}

/// Collect data from execution outputs for a finalization block's data feed.
fn collect_feed_data(
    feed: &DataFeed,
    def: &PipelineDefinition,
    exec_runtime_table: &RuntimeReplicaTable,
    run_scope: &FinalizationRunScope,
    run_id: Option<u32>,
) -> Result<String, AppError> {
    use crate::post_run;

    // Step 1: Determine source block IDs
    let exec_ids = def.execution_block_ids();
    let source_ids: Vec<BlockId> = if feed.from == WILDCARD_BLOCK_ID {
        let mut ids: Vec<BlockId> = exec_ids.into_iter().collect();
        ids.sort();
        ids
    } else {
        vec![feed.from]
    };

    // Step 2: Get filename stems from runtime table
    let filename_stems: Vec<&str> = exec_runtime_table
        .entries
        .iter()
        .filter(|e| source_ids.contains(&e.source_block_id))
        .map(|e| e.filename_stem.as_str())
        .collect();

    // Step 3: Determine run directories to scan
    let run_dirs: Vec<(u32, &Path)> = match (&feed.granularity, run_scope) {
        (
            FeedGranularity::PerRun,
            FinalizationRunScope::SingleRun {
                run_id: id,
                run_dir,
            },
        ) => {
            vec![(*id, run_dir.as_path())]
        }
        (FeedGranularity::PerRun, FinalizationRunScope::Batch { successful_runs }) => {
            if let Some(target_run) = run_id {
                successful_runs
                    .iter()
                    .filter(|(id, _)| *id == target_run)
                    .map(|(id, p)| (*id, p.as_path()))
                    .collect()
            } else {
                Vec::new()
            }
        }
        (FeedGranularity::AllRuns, FinalizationRunScope::Batch { successful_runs }) => {
            successful_runs
                .iter()
                .map(|(id, p)| (*id, p.as_path()))
                .collect()
        }
        (
            FeedGranularity::AllRuns,
            FinalizationRunScope::SingleRun {
                run_id: id,
                run_dir,
            },
        ) => {
            vec![(*id, run_dir.as_path())]
        }
    };

    let mut assembled = String::new();
    let budget = MAX_TERMINAL_OUTPUTS_BYTES;

    for (dir_run_id, dir_path) in &run_dirs {
        let entries = match std::fs::read_dir(dir_path) {
            Ok(e) => e,
            Err(_) => continue,
        };

        // Step 4: Collect matching files
        let mut matched_files: Vec<(String, PathBuf)> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.ends_with(".md") {
                continue;
            }
            // Check if this file matches any of the expected filename stems.
            // Require `_iter` after the stem to avoid prefix collisions (e.g.
            // stem "Builder_b1_Claude" must not match "Builder_b1_Claude_r1_iter1.md").
            let matches_stem = filename_stems
                .iter()
                .any(|stem| name.starts_with(stem) && name[stem.len()..].starts_with("_iter"));
            if !matches_stem {
                continue;
            }
            // Verify it has an iteration marker
            if post_run::parse_pipeline_iteration_filename(name).is_none() {
                continue;
            }
            matched_files.push((name.to_string(), path));
        }

        // Step 5: Apply collection filter
        match feed.collection {
            FeedCollection::LastIteration => {
                // Compute max iteration per filename stem so that each source
                // block contributes its own latest iteration.  A single global
                // max would drop sources that completed fewer iterations.
                let stem_matches = |name: &str, stem: &str| -> bool {
                    name.starts_with(stem) && name[stem.len()..].starts_with("_iter")
                };
                let mut max_per_stem: HashMap<&str, u32> = HashMap::new();
                for (name, _) in &matched_files {
                    if let Some(iter) = post_run::parse_pipeline_iteration_filename(name) {
                        for stem in &filename_stems {
                            if stem_matches(name, stem) {
                                let entry = max_per_stem.entry(*stem).or_insert(0);
                                if iter > *entry {
                                    *entry = iter;
                                }
                                break;
                            }
                        }
                    }
                }
                matched_files.retain(|(name, _)| {
                    if let Some(iter) = post_run::parse_pipeline_iteration_filename(name) {
                        filename_stems.iter().any(|stem| {
                            stem_matches(name, stem)
                                && max_per_stem.get(stem).copied() == Some(iter)
                        })
                    } else {
                        false
                    }
                });
            }
            FeedCollection::AllIterations => {
                // Keep all iterations
            }
        }

        // Step 6: Apply loop pass deduplication
        matched_files = post_run::keep_highest_loop_pass(matched_files);

        // Step 7: Sort for determinism
        matched_files.sort_by(|a, b| post_run::natural_cmp(&a.0, &b.0));

        // Step 8: Read contents and assemble
        for (name, path) in &matched_files {
            if assembled.len() >= budget {
                assembled.push_str("\n[... feed data truncated at 512 KB budget ...]\n");
                break;
            }
            if let Ok(content) = std::fs::read_to_string(path) {
                let run_label = if run_dirs.len() > 1 {
                    format!("(run {dir_run_id}) ")
                } else {
                    String::new()
                };
                assembled.push_str(&format!("### {run_label}{name}\n{content}\n\n"));
            }
        }
    }

    Ok(assembled)
}

/// Construct the message for a finalization block.
fn build_finalization_message(
    block: &PipelineBlock,
    feed_payloads: &[(String, String)],
    upstream_outputs: &[(String, String)],
) -> String {
    let mut message = String::new();

    // Block prompt
    if !block.prompt.is_empty() {
        message.push_str(&block.prompt);
    }

    // Feed inputs
    if !feed_payloads.is_empty() {
        if !message.is_empty() {
            message.push_str("\n\n");
        }
        message.push_str("--- Feed Inputs ---\n");
        for (label, content) in feed_payloads {
            message.push_str(&format!("### {label}\n{content}\n\n"));
        }
    }

    // Upstream finalization outputs (same budget as feed data)
    if !upstream_outputs.is_empty() {
        if !message.is_empty() {
            message.push_str("\n\n");
        }
        message.push_str("--- Upstream Finalization Outputs ---\n");
        let budget = MAX_TERMINAL_OUTPUTS_BYTES;
        let mut upstream_bytes = 0usize;
        for (label, content) in upstream_outputs {
            upstream_bytes += label.len() + content.len();
            if upstream_bytes > budget {
                message.push_str("\n[... upstream outputs truncated at 512 KB budget ...]\n");
                break;
            }
            message.push_str(&format!("### {label}\n{content}\n\n"));
        }
    }

    message
}

/// Write finalization metadata file.
fn write_finalization_toml(
    fin_dir: &Path,
    def: &PipelineDefinition,
    run_scope: &FinalizationRunScope,
    pipeline_source: Option<&str>,
) -> Result<(), AppError> {
    let successful_run_ids: Vec<u32> = match run_scope {
        FinalizationRunScope::SingleRun { run_id, .. } => vec![*run_id],
        FinalizationRunScope::Batch { successful_runs } => {
            successful_runs.iter().map(|(id, _)| *id).collect()
        }
    };

    #[derive(Serialize)]
    struct FinalizationMeta {
        finalization_blocks: usize,
        finalization_connections: usize,
        data_feeds: usize,
        successful_run_ids: Vec<u32>,
        pipeline_source: String,
    }

    let meta = FinalizationMeta {
        finalization_blocks: def.finalization_blocks.len(),
        finalization_connections: def.finalization_connections.len(),
        data_feeds: def.data_feeds.len(),
        successful_run_ids,
        pipeline_source: pipeline_source.unwrap_or("").to_string(),
    };

    let content = toml::to_string_pretty(&meta).map_err(|e| io::Error::other(e.to_string()))?;
    std::fs::write(fin_dir.join("finalization.toml"), content)?;
    Ok(())
}

/// Core finalization runner. Executes the finalization DAG after the execution phase completes.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_pipeline_finalization<F>(
    def: &PipelineDefinition,
    run_scope: FinalizationRunScope,
    exec_runtime_table: &RuntimeReplicaTable,
    agent_configs: PipelineAgentConfigs,
    output_root: &Path,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
    provider_factory: F,
) -> Result<(), AppError>
where
    F: Fn(ProviderKind, &crate::config::ProviderConfig) -> Box<dyn provider::Provider>,
{
    if def.finalization_blocks.is_empty() {
        return Ok(());
    }

    // Warn about missing profile files in finalization blocks
    for block in &def.finalization_blocks {
        let missing = missing_profiles(&block.profiles);
        if !missing.is_empty() {
            let msg = format!(
                "Warning: finalization block {} has missing profile(s): {} — skipped at runtime\n",
                block.id,
                missing.join(", ")
            );
            let err_path = output_root.join("_errors.log");
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&err_path)
                .and_then(|mut f| std::io::Write::write_all(&mut f, msg.as_bytes()));
        }
    }

    // Create finalization output directory
    let fin_dir = output_root.join("finalization");
    std::fs::create_dir_all(&fin_dir)?;

    // Classify blocks
    let per_run_ids: HashSet<BlockId> = def
        .finalization_blocks
        .iter()
        .filter(|b| def.is_per_run_finalization_block(b.id))
        .map(|b| b.id)
        .collect();

    let all_runs_ids: HashSet<BlockId> = def
        .finalization_blocks
        .iter()
        .filter(|b| !per_run_ids.contains(&b.id))
        .map(|b| b.id)
        .collect();

    // Build the finalization block map
    let fin_block_map: HashMap<BlockId, &PipelineBlock> =
        def.finalization_blocks.iter().map(|b| (b.id, b)).collect();

    // Build finalization adjacency (downstream)
    let mut fin_downstream: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
    for conn in &def.finalization_connections {
        fin_downstream.entry(conn.from).or_default().push(conn.to);
    }

    // Determine successful runs for scoping
    let successful_runs: Vec<(u32, PathBuf)> = match &run_scope {
        FinalizationRunScope::SingleRun { run_id, run_dir } => {
            vec![(*run_id, run_dir.clone())]
        }
        FinalizationRunScope::Batch { successful_runs } => successful_runs.clone(),
    };
    // Collect all finalization outputs keyed by (block_id, run_scope_id)
    type FinOutputMap = HashMap<(BlockId, Option<u32>), Vec<(String, String)>>;
    let mut fin_outputs: FinOutputMap = HashMap::new();
    let mut block_error_count: usize = 0;
    // Track blocks that fully failed (all replicas errored) so dependents can be skipped
    let mut failed_blocks: HashSet<(BlockId, Option<u32>)> = HashSet::new();

    // Build runtime entries for finalization
    let exec_entry_count = exec_runtime_table.entries.len() as u32;
    let fin_entries = build_finalization_runtime_entries(def, &run_scope, exec_entry_count);

    // Pipeline source for metadata.
    // For single-run, pipeline.toml lives in output_root (the run dir).
    // For batch, it lives inside each run subdir — check the first successful run.
    let pipeline_source = {
        let candidate = if output_root.join("pipeline.toml").exists() {
            true
        } else {
            successful_runs
                .first()
                .map(|(_, dir)| dir.join("pipeline.toml").exists())
                .unwrap_or(false)
        };
        candidate.then_some("pipeline.toml")
    };

    // -----------------------------------------------------------------------
    // PHASE 1: Per-run finalization
    // -----------------------------------------------------------------------
    if !per_run_ids.is_empty() {
        for &(run_id, ref _run_dir) in &successful_runs {
            if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            // Compute in-degree for per-run sub-DAG (within this run)
            let mut in_degree: HashMap<BlockId, usize> =
                per_run_ids.iter().map(|&id| (id, 0)).collect();
            for conn in &def.finalization_connections {
                if per_run_ids.contains(&conn.from) && per_run_ids.contains(&conn.to) {
                    let from_block = fin_block_map.get(&conn.from);
                    let from_replicas = from_block
                        .map(|b| b.agents.len() * b.replicas.max(1) as usize)
                        .unwrap_or(1);
                    *in_degree.entry(conn.to).or_default() += from_replicas;
                }
            }

            // Also account for data feed in-degree (one feed = one input, not replica-weighted)
            // Data feeds don't add to in-degree in the same way; they are always available at start

            // Execute per-run sub-DAG using topological order
            let mut ready: VecDeque<BlockId> = in_degree
                .iter()
                .filter(|(_, &deg)| deg == 0)
                .map(|(&id, _)| id)
                .collect();
            let mut sorted: Vec<BlockId> = ready.drain(..).collect();
            sorted.sort();
            ready.extend(sorted);

            while let Some(block_id) = ready.pop_front() {
                if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                let block = match fin_block_map.get(&block_id) {
                    Some(b) => *b,
                    None => continue,
                };

                // Skip if any upstream finalization block fully failed
                let upstream_failed = def
                    .finalization_connections
                    .iter()
                    .filter(|c| c.to == block_id && per_run_ids.contains(&c.from))
                    .any(|c| failed_blocks.contains(&(c.from, Some(run_id))));
                if upstream_failed {
                    failed_blocks.insert((block_id, Some(run_id)));
                    let replica_count = block.agents.len() * block.replicas.max(1) as usize;
                    // Emit BlockSkipped for each replica so the TUI marks rows as skipped
                    let reason = format!("upstream finalization block failed (run {run_id})");
                    for entry in fin_entries
                        .iter()
                        .filter(|e| e.source_block_id == block_id && e.run_scope == Some(run_id))
                    {
                        let _ = progress_tx.send(ProgressEvent::BlockSkipped {
                            block_id: entry.runtime_id,
                            agent_name: entry.agent.clone(),
                            label: entry.display_label.clone(),
                            iteration: 1,
                            loop_pass: 0,
                            reason: reason.clone(),
                        });
                    }
                    // Still decrement in-degree for children so the DAG walk continues
                    if let Some(children) = fin_downstream.get(&block_id) {
                        for &child in children {
                            if per_run_ids.contains(&child) {
                                if let Some(deg) = in_degree.get_mut(&child) {
                                    *deg = deg.saturating_sub(replica_count);
                                    if *deg == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                        }
                    }
                    continue;
                }

                // Find matching runtime entries for this block + run
                let block_entries: Vec<&RuntimeReplicaInfo> = fin_entries
                    .iter()
                    .filter(|e| e.source_block_id == block_id && e.run_scope == Some(run_id))
                    .collect();

                // Collect feed data for this block
                let mut feed_payloads: Vec<(String, String)> = Vec::new();
                for feed in &def.data_feeds {
                    if feed.to != block_id {
                        continue;
                    }
                    let label = if feed.from == WILDCARD_BLOCK_ID {
                        format!("All execution blocks ({})", feed.collection.as_str())
                    } else {
                        format!("Block {} ({})", feed.from, feed.collection.as_str())
                    };
                    let data =
                        collect_feed_data(feed, def, exec_runtime_table, &run_scope, Some(run_id))?;
                    if !data.is_empty() {
                        feed_payloads.push((label, data));
                    }
                }

                // Collect upstream finalization outputs
                let upstream_fin_ids: Vec<BlockId> = def
                    .finalization_connections
                    .iter()
                    .filter(|c| c.to == block_id)
                    .map(|c| c.from)
                    .collect();
                let mut upstream_outputs: Vec<(String, String)> = Vec::new();
                for uid in &upstream_fin_ids {
                    if let Some(outputs) = fin_outputs.get(&(*uid, Some(run_id))) {
                        for (label, content) in outputs {
                            upstream_outputs.push((label.clone(), content.clone()));
                        }
                    }
                }

                let message = build_finalization_message(block, &feed_payloads, &upstream_outputs);

                // Cache profile prefixes per block (once for CLI, once for API)
                let profile_prefix_cli = resolve_profile_instructions(&block.profiles, true);
                let profile_prefix_api = resolve_profile_instructions(&block.profiles, false);

                // Execute each replica
                let mut block_output_list: Vec<(String, String)> = Vec::new();
                for entry in &block_entries {
                    let _ = progress_tx.send(ProgressEvent::BlockStarted {
                        block_id: entry.runtime_id,
                        agent_name: entry.agent.clone(),
                        label: entry.display_label.clone(),
                        iteration: 1,
                        loop_pass: 0,
                    });

                    let (kind, cfg, use_cli) = match agent_configs.get(&entry.agent) {
                        Some(c) => c,
                        None => {
                            let _ = progress_tx.send(ProgressEvent::BlockError {
                                block_id: entry.runtime_id,
                                agent_name: entry.agent.clone(),
                                label: entry.display_label.clone(),
                                iteration: 1,
                                loop_pass: 0,
                                error: "No provider available".into(),
                                details: None,
                            });
                            block_error_count += 1;
                            continue;
                        }
                    };

                    let full_message = {
                        let prefix = if *use_cli {
                            &profile_prefix_cli
                        } else {
                            &profile_prefix_api
                        };
                        if prefix.is_empty() {
                            message.clone()
                        } else {
                            format!("{prefix}{message}")
                        }
                    };

                    let mut provider = provider_factory(*kind, cfg);
                    let rid = entry.runtime_id;
                    let agent_name = entry.agent.clone();
                    let label = entry.display_label.clone();

                    let result = crate::execution::run_with_cancellation(
                        &mut *provider,
                        &full_message,
                        &progress_tx,
                        &cancel,
                        {
                            let agent_name = agent_name.clone();
                            move |chunk| ProgressEvent::BlockStreamChunk {
                                block_id: rid,
                                agent_name: agent_name.clone(),
                                iteration: 1,
                                loop_pass: 0,
                                chunk,
                            }
                        },
                        {
                            let agent_name = agent_name.clone();
                            move |msg| ProgressEvent::BlockLog {
                                block_id: rid,
                                agent_name: agent_name.clone(),
                                iteration: 1,
                                loop_pass: 0,
                                message: msg,
                            }
                        },
                        ProgressEvent::BlockError {
                            block_id: rid,
                            agent_name: agent_name.clone(),
                            label: label.clone(),
                            iteration: 1,
                            loop_pass: 0,
                            error: "Cancelled".into(),
                            details: None,
                        },
                    )
                    .await;

                    match result {
                        Some(Ok(resp)) => {
                            // Write output file
                            let filename = format!("{}.md", entry.filename_stem);
                            let path = fin_dir.join(&filename);
                            if let Err(e) = tokio::fs::write(&path, &resp.content).await {
                                let _ = progress_tx.send(ProgressEvent::BlockError {
                                    block_id: rid,
                                    agent_name: agent_name.clone(),
                                    label: label.clone(),
                                    iteration: 1,
                                    loop_pass: 0,
                                    error: format!("Failed to write output: {e}"),
                                    details: None,
                                });
                                block_error_count += 1;
                            } else {
                                let _ = progress_tx.send(ProgressEvent::BlockFinished {
                                    block_id: rid,
                                    agent_name: agent_name.clone(),
                                    label,
                                    iteration: 1,
                                    loop_pass: 0,
                                });
                                block_output_list.push((entry.display_label.clone(), resp.content));
                            }
                        }
                        Some(Err(e)) => {
                            let _ = progress_tx.send(ProgressEvent::BlockError {
                                block_id: rid,
                                agent_name,
                                label,
                                iteration: 1,
                                loop_pass: 0,
                                error: e.to_string(),
                                details: Some(e.to_string()),
                            });
                            block_error_count += 1;
                        }
                        None => {
                            // Cancelled — already sent cancel event
                        }
                    }
                }

                // Track full failure: all replicas errored and no output produced
                if block_output_list.is_empty() && !block_entries.is_empty() {
                    failed_blocks.insert((block_id, Some(run_id)));
                }
                fin_outputs.insert((block_id, Some(run_id)), block_output_list);

                // Decrement in-degree for downstream per-run blocks
                let replica_count = block.agents.len() * block.replicas.max(1) as usize;
                if let Some(children) = fin_downstream.get(&block_id) {
                    for &child in children {
                        if per_run_ids.contains(&child) {
                            if let Some(deg) = in_degree.get_mut(&child) {
                                *deg = deg.saturating_sub(replica_count);
                                if *deg == 0 {
                                    ready.push_back(child);
                                }
                            }
                        }
                    }
                }
            }

            debug_assert!(
                cancel.load(std::sync::atomic::Ordering::Relaxed)
                    || in_degree.values().all(|&d| d == 0),
                "per-run finalization in-degree not fully drained"
            );
        }
    }

    // -----------------------------------------------------------------------
    // PHASE 2: All-runs finalization
    // -----------------------------------------------------------------------
    if !all_runs_ids.is_empty() {
        // Compute in-degree with weighted formula for tier-crossing edges
        let mut in_degree: HashMap<BlockId, usize> =
            all_runs_ids.iter().map(|&id| (id, 0)).collect();

        for conn in &def.finalization_connections {
            // Only count all-runs → all-runs edges for in-degree.
            // Per-run → all-runs edges are already satisfied (phase 1 completed);
            // those outputs are collected via upstream_outputs below.
            if !all_runs_ids.contains(&conn.to) || !all_runs_ids.contains(&conn.from) {
                continue;
            }
            let from_block = fin_block_map.get(&conn.from);
            let from_replicas = from_block
                .map(|b| b.agents.len() * b.replicas.max(1) as usize)
                .unwrap_or(1);

            *in_degree.entry(conn.to).or_default() += from_replicas;
        }

        let mut ready: VecDeque<BlockId> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();
        let mut sorted: Vec<BlockId> = ready.drain(..).collect();
        sorted.sort();
        ready.extend(sorted);

        while let Some(block_id) = ready.pop_front() {
            if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let block = match fin_block_map.get(&block_id) {
                Some(b) => *b,
                None => continue,
            };

            // Skip if any upstream all-runs finalization block fully failed
            let upstream_failed = def
                .finalization_connections
                .iter()
                .filter(|c| c.to == block_id && all_runs_ids.contains(&c.from))
                .any(|c| failed_blocks.contains(&(c.from, None)));
            if upstream_failed {
                failed_blocks.insert((block_id, None));
                let replica_count = block.agents.len() * block.replicas.max(1) as usize;
                // Emit BlockSkipped for each replica so the TUI marks rows as skipped
                let reason = "upstream finalization block failed".to_string();
                for entry in fin_entries
                    .iter()
                    .filter(|e| e.source_block_id == block_id && e.run_scope.is_none())
                {
                    let _ = progress_tx.send(ProgressEvent::BlockSkipped {
                        block_id: entry.runtime_id,
                        agent_name: entry.agent.clone(),
                        label: entry.display_label.clone(),
                        iteration: 1,
                        loop_pass: 0,
                        reason: reason.clone(),
                    });
                }
                if let Some(children) = fin_downstream.get(&block_id) {
                    for &child in children {
                        if all_runs_ids.contains(&child) {
                            if let Some(deg) = in_degree.get_mut(&child) {
                                *deg = deg.saturating_sub(replica_count);
                                if *deg == 0 {
                                    ready.push_back(child);
                                }
                            }
                        }
                    }
                }
                continue;
            }

            // Find matching runtime entries for this block (all-runs, no run_scope)
            let block_entries: Vec<&RuntimeReplicaInfo> = fin_entries
                .iter()
                .filter(|e| e.source_block_id == block_id && e.run_scope.is_none())
                .collect();

            // Collect feed data for this block (all-runs granularity)
            let mut feed_payloads: Vec<(String, String)> = Vec::new();
            for feed in &def.data_feeds {
                if feed.to != block_id {
                    continue;
                }
                let label = if feed.from == WILDCARD_BLOCK_ID {
                    format!("All execution blocks ({})", feed.collection.as_str())
                } else {
                    format!("Block {} ({})", feed.from, feed.collection.as_str())
                };
                let data = collect_feed_data(feed, def, exec_runtime_table, &run_scope, None)?;
                if !data.is_empty() {
                    feed_payloads.push((label, data));
                }
            }

            // Collect upstream finalization outputs (from per-run blocks across all runs, or all-runs blocks)
            let upstream_fin_ids: Vec<BlockId> = def
                .finalization_connections
                .iter()
                .filter(|c| c.to == block_id)
                .map(|c| c.from)
                .collect();

            let mut upstream_outputs: Vec<(String, String)> = Vec::new();
            for uid in &upstream_fin_ids {
                if per_run_ids.contains(uid) {
                    // Collect from all runs
                    for &(run_id, _) in &successful_runs {
                        if let Some(outputs) = fin_outputs.get(&(*uid, Some(run_id))) {
                            for (label, content) in outputs {
                                upstream_outputs.push((label.clone(), content.clone()));
                            }
                        }
                    }
                } else {
                    // All-runs upstream
                    if let Some(outputs) = fin_outputs.get(&(*uid, None)) {
                        for (label, content) in outputs {
                            upstream_outputs.push((label.clone(), content.clone()));
                        }
                    }
                }
            }

            let message = build_finalization_message(block, &feed_payloads, &upstream_outputs);

            // Cache profile prefixes per block (once for CLI, once for API)
            let profile_prefix_cli = resolve_profile_instructions(&block.profiles, true);
            let profile_prefix_api = resolve_profile_instructions(&block.profiles, false);

            // Execute each replica
            let mut block_output_list: Vec<(String, String)> = Vec::new();
            for entry in &block_entries {
                let _ = progress_tx.send(ProgressEvent::BlockStarted {
                    block_id: entry.runtime_id,
                    agent_name: entry.agent.clone(),
                    label: entry.display_label.clone(),
                    iteration: 1,
                    loop_pass: 0,
                });

                let (kind, cfg, use_cli) = match agent_configs.get(&entry.agent) {
                    Some(c) => c,
                    None => {
                        let _ = progress_tx.send(ProgressEvent::BlockError {
                            block_id: entry.runtime_id,
                            agent_name: entry.agent.clone(),
                            label: entry.display_label.clone(),
                            iteration: 1,
                            loop_pass: 0,
                            error: "No provider available".into(),
                            details: None,
                        });
                        block_error_count += 1;
                        continue;
                    }
                };

                let full_message = {
                    let prefix = if *use_cli {
                        &profile_prefix_cli
                    } else {
                        &profile_prefix_api
                    };
                    if prefix.is_empty() {
                        message.clone()
                    } else {
                        format!("{prefix}{message}")
                    }
                };

                let mut provider = provider_factory(*kind, cfg);
                let rid = entry.runtime_id;
                let agent_name = entry.agent.clone();
                let label = entry.display_label.clone();

                let result = crate::execution::run_with_cancellation(
                    &mut *provider,
                    &full_message,
                    &progress_tx,
                    &cancel,
                    {
                        let agent_name = agent_name.clone();
                        move |chunk| ProgressEvent::BlockStreamChunk {
                            block_id: rid,
                            agent_name: agent_name.clone(),
                            iteration: 1,
                            loop_pass: 0,
                            chunk,
                        }
                    },
                    {
                        let agent_name = agent_name.clone();
                        move |msg| ProgressEvent::BlockLog {
                            block_id: rid,
                            agent_name: agent_name.clone(),
                            iteration: 1,
                            loop_pass: 0,
                            message: msg,
                        }
                    },
                    ProgressEvent::BlockError {
                        block_id: rid,
                        agent_name: agent_name.clone(),
                        label: label.clone(),
                        iteration: 1,
                        loop_pass: 0,
                        error: "Cancelled".into(),
                        details: None,
                    },
                )
                .await;

                match result {
                    Some(Ok(resp)) => {
                        let filename = format!("{}.md", entry.filename_stem);
                        let path = fin_dir.join(&filename);
                        if let Err(e) = tokio::fs::write(&path, &resp.content).await {
                            let _ = progress_tx.send(ProgressEvent::BlockError {
                                block_id: rid,
                                agent_name: agent_name.clone(),
                                label: label.clone(),
                                iteration: 1,
                                loop_pass: 0,
                                error: format!("Failed to write output: {e}"),
                                details: None,
                            });
                            block_error_count += 1;
                        } else {
                            let _ = progress_tx.send(ProgressEvent::BlockFinished {
                                block_id: rid,
                                agent_name: agent_name.clone(),
                                label,
                                iteration: 1,
                                loop_pass: 0,
                            });
                            block_output_list.push((entry.display_label.clone(), resp.content));
                        }
                    }
                    Some(Err(e)) => {
                        let _ = progress_tx.send(ProgressEvent::BlockError {
                            block_id: rid,
                            agent_name,
                            label,
                            iteration: 1,
                            loop_pass: 0,
                            error: e.to_string(),
                            details: Some(e.to_string()),
                        });
                        block_error_count += 1;
                    }
                    None => {
                        // Cancelled
                    }
                }
            }

            if block_output_list.is_empty() && !block_entries.is_empty() {
                failed_blocks.insert((block_id, None));
            }
            fin_outputs.insert((block_id, None), block_output_list);

            // Decrement in-degree for downstream all-runs blocks
            let replica_count = block.agents.len() * block.replicas.max(1) as usize;
            if let Some(children) = fin_downstream.get(&block_id) {
                for &child in children {
                    if all_runs_ids.contains(&child) {
                        if let Some(deg) = in_degree.get_mut(&child) {
                            *deg = deg.saturating_sub(replica_count);
                            if *deg == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                }
            }
        }

        debug_assert!(
            cancel.load(std::sync::atomic::Ordering::Relaxed)
                || in_degree.values().all(|&d| d == 0),
            "all-runs finalization in-degree not fully drained"
        );
    }

    // Skip metadata write and error reporting on cancellation — the run is
    // incomplete and callers detect cancel via the shared flag.
    if cancel.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }

    // Write finalization.toml metadata
    write_finalization_toml(&fin_dir, def, &run_scope, pipeline_source)?;

    if block_error_count > 0 {
        return Err(AppError::Config(format!(
            "{block_error_count} finalization block(s) failed"
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProviderConfig;
    use crate::execution::test_utils::{
        collect_progress_events, MockProvider, PanicProvider, SuccessThenPanicProvider,
    };
    use crate::output::OutputManager;
    use crate::provider::ProviderKind;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;

    fn block(id: BlockId, col: u16, row: u16) -> PipelineBlock {
        PipelineBlock {
            id,
            name: format!("Block#{id}"),
            agents: vec!["Claude".into()],
            prompt: format!("block {id}"),
            profiles: vec![],
            session_id: None,
            position: (col, row),
            replicas: 1,
        }
    }

    fn conn(from: BlockId, to: BlockId) -> PipelineConnection {
        PipelineConnection { from, to }
    }

    fn def_with(
        blocks: Vec<PipelineBlock>,
        connections: Vec<PipelineConnection>,
    ) -> PipelineDefinition {
        PipelineDefinition {
            initial_prompt: "test".into(),
            iterations: 1,
            blocks,
            connections,
            session_configs: Vec::new(),
            loop_connections: Vec::new(),
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        }
    }

    // -- is_valid_profile_name --

    #[test]
    fn valid_profile_names() {
        assert!(is_valid_profile_name("reviewer"));
        assert!(is_valid_profile_name("code-review"));
        assert!(is_valid_profile_name("my_profile"));
    }

    #[test]
    fn invalid_profile_names_path_traversal() {
        assert!(!is_valid_profile_name(""));
        assert!(!is_valid_profile_name("."));
        assert!(!is_valid_profile_name(".."));
        assert!(!is_valid_profile_name("../../secret"));
        assert!(!is_valid_profile_name("foo/bar"));
        assert!(!is_valid_profile_name("foo\\bar"));
        assert!(!is_valid_profile_name("a..b"));
    }

    // -- resolve_profile_instructions_from_dir --

    #[test]
    fn resolve_profiles_empty_list() {
        let dir = tempfile::tempdir().unwrap();
        let result = resolve_profile_instructions_from_dir(dir.path(), &[], false);
        assert!(result.is_empty());
        let result = resolve_profile_instructions_from_dir(dir.path(), &[], true);
        assert!(result.is_empty());
    }

    #[test]
    fn resolve_profiles_api_mode() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("reviewer.md"), "Be a careful reviewer.").unwrap();
        std::fs::write(dir.path().join("writer.md"), "Write clearly.").unwrap();
        let result = resolve_profile_instructions_from_dir(
            dir.path(),
            &["reviewer".into(), "writer".into()],
            false,
        );
        assert!(result.contains("--- Profile: reviewer ---"));
        assert!(result.contains("Be a careful reviewer."));
        assert!(result.contains("--- Profile: writer ---"));
        assert!(result.contains("Write clearly."));
    }

    #[test]
    fn resolve_profiles_cli_mode() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("reviewer.md"), "Be a careful reviewer.").unwrap();
        let result = resolve_profile_instructions_from_dir(dir.path(), &["reviewer".into()], true);
        assert!(result.contains("Read these profile instruction files:"));
        assert!(result.contains("reviewer.md"));
        assert!(result.contains("Follow the instructions in each file."));
        // Content should NOT be inlined for CLI mode
        assert!(!result.contains("Be a careful reviewer."));
    }

    #[test]
    fn resolve_profiles_missing_file_skipped() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("exists.md"), "content").unwrap();
        let result = resolve_profile_instructions_from_dir(
            dir.path(),
            &["exists".into(), "missing".into()],
            false,
        );
        assert!(result.contains("--- Profile: exists ---"));
        assert!(!result.contains("missing"));
    }

    #[test]
    fn resolve_profiles_path_traversal_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let result =
            resolve_profile_instructions_from_dir(dir.path(), &["../../etc/passwd".into()], false);
        assert!(result.is_empty());
    }

    #[test]
    fn deserialize_block_rejects_path_traversal_profile() {
        let toml_str = r#"
            id = 1
            agents = ["Claude"]
            prompt = "test"
            profiles = ["../../secret"]
            position = [0, 0]
        "#;
        let result: Result<PipelineBlock, _> = toml::from_str(toml_str);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid profile name"));
    }

    // -- root_blocks / terminal_blocks --

    #[test]
    fn root_blocks_linear_chain() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        assert_eq!(root_blocks(&d), vec![1]);
    }

    #[test]
    fn terminal_blocks_linear_chain() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        assert_eq!(terminal_blocks(&d), vec![3]);
    }

    #[test]
    fn root_and_terminal_isolated_blocks() {
        let d = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let roots = root_blocks(&d);
        let terms = terminal_blocks(&d);
        assert!(roots.contains(&1) && roots.contains(&2));
        assert!(terms.contains(&1) && terms.contains(&2));
    }

    #[test]
    fn root_and_terminal_diamond() {
        // 1 → 2, 1 → 3, 2 → 4, 3 → 4
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 2), conn(1, 3), conn(2, 4), conn(3, 4)],
        );
        assert_eq!(root_blocks(&d), vec![1]);
        assert_eq!(terminal_blocks(&d), vec![4]);
    }

    // -- upstream_of --

    #[test]
    fn upstream_of_returns_direct_predecessors() {
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 4), conn(2, 4), conn(3, 4)],
        );
        let mut ups = upstream_of(&d, 4);
        ups.sort();
        assert_eq!(ups, vec![1, 2, 3]);
    }

    // -- topological_layers --

    #[test]
    fn topo_layers_linear_chain() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        let layers = topological_layers(&d).unwrap();
        assert_eq!(layers, vec![vec![1], vec![2], vec![3]]);
    }

    #[test]
    fn topo_layers_diamond() {
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 2), conn(1, 3), conn(2, 4), conn(3, 4)],
        );
        let layers = topological_layers(&d).unwrap();
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0], vec![1]);
        assert_eq!(layers[1], vec![2, 3]);
        assert_eq!(layers[2], vec![4]);
    }

    #[test]
    fn topo_layers_fan_out() {
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 1, 2),
            ],
            vec![conn(1, 2), conn(1, 3), conn(1, 4)],
        );
        let layers = topological_layers(&d).unwrap();
        assert_eq!(layers, vec![vec![1], vec![2, 3, 4]]);
    }

    #[test]
    fn topo_layers_isolated_blocks() {
        let d = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let layers = topological_layers(&d).unwrap();
        assert_eq!(layers, vec![vec![1, 2]]);
    }

    #[test]
    fn topo_layers_empty_graph() {
        let d = PipelineDefinition::default();
        let layers = topological_layers(&d).unwrap();
        assert!(layers.is_empty());
    }

    #[test]
    fn topo_layers_rejects_cycle_triangle() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3), conn(3, 1)],
        );
        assert!(topological_layers(&d).is_err());
    }

    #[test]
    fn topo_layers_rejects_self_edge() {
        let d = def_with(vec![block(1, 0, 0)], vec![conn(1, 1)]);
        assert!(topological_layers(&d).is_err());
    }

    // -- would_create_cycle --

    #[test]
    fn would_create_cycle_self_edge() {
        let d = def_with(vec![block(1, 0, 0)], vec![]);
        assert!(would_create_cycle(&d.connections, 1, 1));
    }

    #[test]
    fn would_create_cycle_back_edge() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        // Adding 3→1 would create 1→2→3→1
        assert!(would_create_cycle(&d.connections, 3, 1));
    }

    #[test]
    fn would_create_cycle_valid_forward_edge() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        // Adding 1→3 (skip-edge) is valid
        assert!(!would_create_cycle(&d.connections, 1, 3));
    }

    #[test]
    fn would_create_cycle_diamond_no_cycle() {
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 2), conn(1, 3), conn(2, 4)],
        );
        // Adding 3→4 is valid (diamond)
        assert!(!would_create_cycle(&d.connections, 3, 4));
    }

    // -- next_free_position --

    #[test]
    fn next_free_position_empty() {
        let blocks: Vec<PipelineBlock> = vec![];
        assert_eq!(next_free_position(&blocks), (0, 0));
    }

    #[test]
    fn next_free_position_fills_gaps() {
        let d = def_with(vec![block(1, 0, 0), block(2, 2, 0)], vec![]);
        // (1, 0) is the first gap
        assert_eq!(next_free_position(&d.blocks), (1, 0));
    }

    #[test]
    fn next_free_position_wraps_to_next_row() {
        // Fill entire row 0 cols 0..100? That's too many. Let's check a smaller scenario.
        let d = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        assert_eq!(next_free_position(&d.blocks), (2, 0));
    }

    // -- save/load roundtrip --

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![conn(1, 2)]);
        save_pipeline(&def, &path).unwrap();
        let loaded = load_pipeline(&path).unwrap();
        assert_eq!(loaded.blocks.len(), 2);
        assert_eq!(loaded.connections.len(), 1);
        assert_eq!(loaded.iterations, 1);
    }

    // -- load_pipeline validation --

    #[test]
    fn load_rejects_duplicate_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.toml");
        let content = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]

[[blocks]]
id = 1
agent = "Claude"
prompt = "b"
position = [1, 0]
"#;
        std::fs::write(&path, content).unwrap();
        let err = load_pipeline(&path).unwrap_err();
        assert!(err.to_string().contains("Duplicate block ID"));
    }

    #[test]
    fn load_rejects_dangling_connection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dangle.toml");
        let content = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]

[[connections]]
from = 1
to = 99
"#;
        std::fs::write(&path, content).unwrap();
        let err = load_pipeline(&path).unwrap_err();
        assert!(err.to_string().contains("non-existent block"));
    }

    #[test]
    fn load_rejects_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cycle.toml");
        let content = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]

[[blocks]]
id = 2
agent = "Claude"
prompt = "b"
position = [1, 0]

[[connections]]
from = 1
to = 2

[[connections]]
from = 2
to = 1
"#;
        std::fs::write(&path, content).unwrap();
        let err = load_pipeline(&path).unwrap_err();
        assert!(err.to_string().contains("cycle"));
    }

    #[test]
    fn load_rejects_self_edge() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("self.toml");
        let content = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]

[[connections]]
from = 1
to = 1
"#;
        std::fs::write(&path, content).unwrap();
        let err = load_pipeline(&path).unwrap_err();
        assert!(err.to_string().contains("Self-edge"));
    }

    #[test]
    fn default_pipeline_definition() {
        let d = PipelineDefinition::default();
        assert_eq!(d.iterations, 1);
        assert!(d.blocks.is_empty());
        assert!(d.connections.is_empty());
        assert!(d.initial_prompt.is_empty());
    }

    #[test]
    fn build_pipeline_block_message_only_adds_cli_prefix_for_cli_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("pipeline-msg")).unwrap();
        let def = PipelineDefinition {
            initial_prompt: "base prompt".into(),
            iterations: 1,
            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agents: vec!["Claude".into()],
                prompt: "block prompt".into(),
                profiles: vec![],
                session_id: None,
                position: (0, 0),
                replicas: 1,
            }],
            connections: vec![],
            session_configs: Vec::new(),
            loop_connections: Vec::new(),
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        };
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);

        let block_outputs = HashMap::new();
        let rt = build_runtime_table(&def);
        let btl = HashMap::new();
        let blp = HashMap::new();
        let message_context = PipelineMessageContext {
            def: &def,
            iteration: 1,
            block_outputs: &block_outputs,
            previous_terminal_outputs: "",
            output: &output,
            prompt_context: &context,
            runtime_table: &rt,
            block_to_loop: &btl,
            block_loop_pass: &blp,
        };
        let cli_message = build_pipeline_block_message(&def.blocks[0], true, &message_context);
        let api_message = build_pipeline_block_message(&def.blocks[0], false, &message_context);

        assert!(cli_message.contains("Working directory:"));
        assert!(cli_message.ends_with("block prompt\n\nbase prompt"));
        assert_eq!(api_message, "block prompt\n\nbase prompt");
    }

    #[tokio::test]
    async fn run_pipeline_panics_emit_block_error_and_append_error_log() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("pipeline-panic")).unwrap();
        let def = PipelineDefinition {
            initial_prompt: "base prompt".into(),
            iterations: 1,
            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agents: vec!["Claude".into()],
                prompt: String::new(),
                profiles: vec![],
                session_id: None,
                position: (0, 0),
                replicas: 1,
            }],
            connections: vec![],
            session_configs: Vec::new(),
            loop_connections: Vec::new(),
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        };
        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, rx) = mpsc::unbounded_channel();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            |_kind, _cfg| {
                Box::new(PanicProvider::new(
                    ProviderKind::Anthropic,
                    "pipeline panic",
                ))
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ProgressEvent::BlockError {
                    block_id,
                    agent_name,
                    error,
                    ..
                } if *block_id == 0 && agent_name == "Claude" && error.contains("panicked")
            )
        }));
        assert!(events
            .iter()
            .any(|event| matches!(event, ProgressEvent::AllDone)));

        let log = std::fs::read_to_string(output.run_dir().join("_errors.log")).expect("log");
        assert!(log.contains("runtime 0 Claude iter1"));
        assert!(log.contains("pipeline panic"));
    }

    #[tokio::test]
    async fn run_pipeline_provider_error_appends_error_log() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("pipeline-provider-error")).unwrap();
        let def = PipelineDefinition {
            initial_prompt: "base prompt".into(),
            iterations: 1,
            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agents: vec!["Claude".into()],
                prompt: String::new(),
                profiles: vec![],
                session_id: None,
                position: (0, 0),
                replicas: 1,
            }],
            connections: vec![],
            session_configs: Vec::new(),
            loop_connections: Vec::new(),
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        };
        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let received = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::unbounded_channel();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            |_kind, _cfg| {
                Box::new(MockProvider::err(
                    ProviderKind::Anthropic,
                    "provider failed",
                    received.clone(),
                ))
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ProgressEvent::BlockError {
                    block_id,
                    agent_name,
                    error,
                    ..
                } if *block_id == 0 && agent_name == "Claude" && error.contains("provider failed")
            )
        }));

        let log = std::fs::read_to_string(output.run_dir().join("_errors.log")).expect("log");
        assert!(log.contains("runtime 0 Claude iter1"));
        assert!(log.contains("provider failed"));
    }

    #[tokio::test]
    async fn pipeline_concurrency_limit_enforced() {
        use crate::provider::{CompletionResponse, Provider, SendFuture};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ConcurrencyTracker {
            kind: ProviderKind,
            active: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }

        impl Provider for ConcurrencyTracker {
            fn kind(&self) -> ProviderKind {
                self.kind
            }

            fn clear_history(&mut self) {}

            fn send(&mut self, _message: &str) -> SendFuture<'_> {
                let active = self.active.clone();
                let peak = self.peak.clone();
                Box::pin(async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(CompletionResponse {
                        content: "ok".to_string(),
                        debug_logs: Vec::new(),
                    })
                })
            }
        }

        let def = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![], // All independent roots
        );

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let agent_configs = {
            let mut m: PipelineAgentConfigs = HashMap::new();
            m.insert(
                "Claude".to_string(),
                (
                    ProviderKind::Anthropic,
                    ProviderConfig {
                        api_key: "k".to_string(),
                        model: "m".to_string(),
                        reasoning_effort: None,
                        thinking_effort: None,
                        use_cli: false,
                        cli_print_mode: true,
                        extra_cli_args: String::new(),
                    },
                    false,
                ),
            );
            m
        };

        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let dir = tempfile::tempdir().expect("tempdir");
        let output = OutputManager::new(dir.path(), Some("pipeline-concurrency")).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();

        let active_clone = active.clone();
        let peak_clone = peak.clone();

        run_pipeline_with_provider_factory(
            &def,
            1, // max_block_concurrency = 1
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| -> Box<dyn Provider> {
                Box::new(ConcurrencyTracker {
                    kind: ProviderKind::Anthropic,
                    active: active_clone.clone(),
                    peak: peak_clone.clone(),
                })
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);
        let finished_count = events
            .iter()
            .filter(|e| matches!(e, ProgressEvent::BlockFinished { .. }))
            .count();
        assert_eq!(finished_count, 3);
        assert_eq!(peak.load(Ordering::SeqCst), 1);
    }

    // -- effective_session_key --

    #[test]
    fn effective_session_key_returns_explicit_session_id() {
        let b = PipelineBlock {
            id: 1,
            name: "B".into(),
            agents: vec!["Claude".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: Some("shared".into()),
            position: (0, 0),
            replicas: 1,
        };
        assert_eq!(b.effective_session_key(), "shared");
    }

    #[test]
    fn effective_session_key_falls_back_to_block_id() {
        let b = block(5, 0, 0);
        assert_eq!(b.effective_session_key(), "__block_5");
    }

    // -- effective_sessions --

    #[test]
    fn effective_sessions_groups_shared_sessions() {
        let mut def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "A".into(),
                    agents: vec!["Claude".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: Some("shared".into()),
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "B".into(),
                    agents: vec!["Claude".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: Some("shared".into()),
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![conn(1, 2)],
        );
        def.session_configs = Vec::new();
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_key, "shared");
        assert!(sessions[0].block_ids.contains(&1));
        assert!(sessions[0].block_ids.contains(&2));
    }

    #[test]
    fn effective_sessions_separates_different_agents_same_session() {
        let mut def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "A".into(),
                    agents: vec!["Claude".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: Some("shared".into()),
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "B".into(),
                    agents: vec!["GPT".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: Some("shared".into()),
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![],
        );
        def.session_configs = Vec::new();
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn effective_sessions_isolated_blocks_get_separate_rows() {
        let def = def_with(vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)], vec![]);
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 3);
    }

    #[test]
    fn effective_sessions_sorted_by_agent_then_key() {
        let mut def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "Z".into(),
                    agents: vec!["GPT".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: None,
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "A".into(),
                    agents: vec!["Claude".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: None,
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![],
        );
        def.session_configs = Vec::new();
        let sessions = def.effective_sessions();
        assert_eq!(sessions[0].agent, "Claude");
        assert_eq!(sessions[1].agent, "GPT");
    }

    #[test]
    fn effective_sessions_disambiguates_identical_labels() {
        let def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "Worker".into(),
                    agents: vec!["Claude".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: None,
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "Worker".into(),
                    agents: vec!["Claude".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: None,
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![],
        );
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 2);
        // Both have same agent+name, so labels must be disambiguated with block IDs
        assert_ne!(sessions[0].display_label, sessions[1].display_label);
        assert!(sessions[0].display_label.contains("Worker"));
        assert!(sessions[1].display_label.contains("Worker"));
        // Each label is prefixed with #id so truncation never hides the distinguishing part
        assert!(
            sessions[0].display_label.starts_with('#'),
            "label should be prefixed with block ID: {}",
            sessions[0].display_label
        );
    }

    // -- keep_session_across_iterations --

    #[test]
    fn keep_session_defaults_to_true() {
        let def = def_with(vec![block(1, 0, 0)], vec![]);
        assert!(def.keep_session_across_iterations("Claude", "__block_1"));
    }

    #[test]
    fn set_keep_false_adds_explicit_entry() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.set_keep_session_across_iterations("Claude", "__block_1", false);
        assert!(!def.keep_session_across_iterations("Claude", "__block_1"));
        assert_eq!(def.session_configs.len(), 1);
    }

    #[test]
    fn toggle_back_to_true_removes_explicit_entry() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.set_keep_session_across_iterations("Claude", "__block_1", false);
        assert_eq!(def.session_configs.len(), 1);
        def.set_keep_session_across_iterations("Claude", "__block_1", true);
        assert!(def.session_configs.is_empty());
        assert!(def.keep_session_across_iterations("Claude", "__block_1"));
    }

    // -- normalize_session_configs --

    #[test]
    fn normalize_drops_stale_rows() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.session_configs.push(SessionConfig {
            agent: "Claude".into(),
            session_key: "nonexistent".into(),
            keep_across_iterations: false,
            keep_across_loop_passes: true,
        });
        def.normalize_session_configs();
        assert!(def.session_configs.is_empty());
    }

    #[test]
    fn normalize_drops_true_rows() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.session_configs.push(SessionConfig {
            agent: "Claude".into(),
            session_key: "__block_1".into(),
            keep_across_iterations: true,
            keep_across_loop_passes: true,
        });
        def.normalize_session_configs();
        assert!(def.session_configs.is_empty());
    }

    #[test]
    fn normalize_keeps_valid_false_rows() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.session_configs.push(SessionConfig {
            agent: "Claude".into(),
            session_key: "__block_1".into(),
            keep_across_iterations: false,
            keep_across_loop_passes: true,
        });
        def.normalize_session_configs();
        assert_eq!(def.session_configs.len(), 1);
    }

    // -- serde --

    #[test]
    fn old_toml_without_session_configs_loads_empty() {
        let toml_str = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
name = "B"
agent = "Claude"
prompt = ""
position = [0, 0]
"#;
        let def: PipelineDefinition = toml::from_str(toml_str).unwrap();
        assert!(def.session_configs.is_empty());
    }

    #[test]
    fn session_config_missing_keep_defaults_to_true() {
        let toml_str = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
name = "B"
agent = "Claude"
prompt = ""
position = [0, 0]

[[session_configs]]
agent = "Claude"
session_key = "__block_1"
"#;
        let def: PipelineDefinition = toml::from_str(toml_str).unwrap();
        assert_eq!(def.session_configs.len(), 1);
        assert!(def.session_configs[0].keep_across_iterations);
    }

    #[test]
    fn save_load_roundtrip_preserves_false_session_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_session.toml");
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.set_keep_session_across_iterations("Claude", "__block_1", false);
        save_pipeline(&def, &path).unwrap();
        let loaded = load_pipeline(&path).unwrap();
        assert!(!loaded.keep_session_across_iterations("Claude", "__block_1"));
    }

    #[test]
    fn load_deduplicates_session_configs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.toml");
        let toml_str = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
name = "B"
agent = "Claude"
prompt = ""
position = [0, 0]

[[session_configs]]
agent = "Claude"
session_key = "__block_1"
keep_across_iterations = false

[[session_configs]]
agent = "Claude"
session_key = "__block_1"
keep_across_iterations = false
"#;
        std::fs::write(&path, toml_str).unwrap();
        // Normalization deduplicates before validation
        let loaded = load_pipeline(&path).unwrap();
        assert_eq!(loaded.session_configs.len(), 1);
        assert!(!loaded.keep_session_across_iterations("Claude", "__block_1"));
    }

    // -- execution: clear_history --

    struct ClearCountProvider {
        kind: ProviderKind,
        responses:
            std::sync::Mutex<VecDeque<Result<crate::provider::CompletionResponse, AppError>>>,
        clear_count: Arc<AtomicUsize>,
    }

    impl crate::provider::Provider for ClearCountProvider {
        fn kind(&self) -> ProviderKind {
            self.kind
        }

        fn clear_history(&mut self) {
            self.clear_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        fn send(&mut self, _message: &str) -> crate::provider::SendFuture<'_> {
            Box::pin(async {
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| {
                        Ok(crate::provider::CompletionResponse {
                            content: "response".to_string(),
                            debug_logs: Vec::new(),
                        })
                    })
            })
        }
    }

    #[tokio::test]
    async fn iteration_clears_non_keep_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("clear-test")).unwrap();
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.iterations = 2;
        def.set_keep_session_across_iterations("Claude", "__block_1", false);

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        // Should have cleared once before iteration 2
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn single_iteration_never_clears() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("no-clear")).unwrap();
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.iterations = 1;
        def.set_keep_session_across_iterations("Claude", "__block_1", false);

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn all_keep_sessions_never_clear() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("keep-all")).unwrap();
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.iterations = 2;
        // Default is keep=true, so no explicit config needed

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn shared_session_clears_once_per_provider() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("shared-clear")).unwrap();
        let mut def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "A".into(),
                    agents: vec!["Claude".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: Some("shared".into()),
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "B".into(),
                    agents: vec!["Claude".into()],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: Some("shared".into()),
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![conn(1, 2)],
        );
        def.iterations = 2;
        def.set_keep_session_across_iterations("Claude", "shared", false);

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        // Two blocks share one provider, cleared once before iteration 2
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn loop_pass_clears_non_keep_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-clear")).unwrap();
        // A(1) → B(2) with loop from B back to A, 2 extra passes
        let mut def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 2)],
        );
        def.set_keep_session_across_loop_passes("Claude", "__block_1", false);
        def.set_keep_session_across_loop_passes("Claude", "__block_2", false);

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        // 2 pass-advances × 2 sessions = 4 clears
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn loop_pass_keeps_sessions_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-keep")).unwrap();
        // Same topology, default keep_across_loop_passes=true
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 2)],
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        // keep_session=true by default, no clears
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    // -- loop connection helpers --

    fn lconn(from: BlockId, to: BlockId, count: u32) -> LoopConnection {
        LoopConnection {
            from,
            to,
            count,
            prompt: String::new(),
        }
    }

    fn def_with_loops(
        blocks: Vec<PipelineBlock>,
        connections: Vec<PipelineConnection>,
        loops: Vec<LoopConnection>,
    ) -> PipelineDefinition {
        PipelineDefinition {
            initial_prompt: "test".into(),
            iterations: 1,
            blocks,
            connections,
            session_configs: Vec::new(),
            loop_connections: loops,
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        }
    }

    // -- loop validation tests --

    #[test]
    fn test_validate_rejects_loop_self_edge() {
        let def = def_with_loops(vec![block(1, 0, 0)], vec![], vec![lconn(1, 1, 2)]);
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("self-edge"),
            "expected 'self-edge', got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_loop_count_zero() {
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![],
            vec![LoopConnection {
                from: 1,
                to: 2,
                count: 0,
                prompt: String::new(),
            }],
        );
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("1-99"),
            "expected 'between 1 and 99' range msg, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_block_in_two_loops() {
        // Block 2 is `from` endpoint of two different loop connections
        // A(1)→B(2)→C(3), loop_back(B,A) and loop_back(B,C) — B is from in both
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
            vec![lconn(2, 1, 1), lconn(2, 3, 1)],
        );
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("already a loop endpoint"),
            "expected 'already a loop endpoint', got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_block_as_from_and_to_in_different_loops() {
        // A(1)→B(2)→C(3)→D(4), loop_back(B,A) has B as from,
        // loop_back(D,B) has B as to — B in two loops with different roles
        let def = def_with_loops(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 2, 0),
                block(4, 3, 0),
            ],
            vec![conn(1, 2), conn(2, 3), conn(3, 4)],
            vec![lconn(2, 1, 1), lconn(4, 2, 1)],
        );
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("already a loop endpoint"),
            "expected 'already a loop endpoint', got: {err}"
        );
    }

    #[test]
    fn test_validate_allows_regular_between_loop_endpoints() {
        // Regular A→B + loop_back(B, A) is valid under new semantics
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );
        assert!(
            validate_pipeline(&def).is_ok(),
            "regular connection between loop endpoints should be valid"
        );
    }

    #[test]
    fn test_validate_rejects_non_ancestor() {
        // No regular path from to→from, so ancestry check fails
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![],
            vec![lconn(2, 1, 1)],
        );
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("not a regular-graph ancestor"),
            "expected ancestry error, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_overlapping_sub_dags() {
        // A(1)→B(2)→C(3)→D(4), loop_back(C,A,1) and loop_back(D,B,1) — B is in both sub-DAGs
        let def = def_with_loops(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 2, 0),
                block(4, 3, 0),
            ],
            vec![conn(1, 2), conn(2, 3), conn(3, 4)],
            vec![lconn(3, 1, 1), lconn(4, 2, 1)],
        );
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("Overlapping loop sub-DAGs"),
            "expected overlap error, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_loop_dangling() {
        let def = def_with_loops(vec![block(1, 0, 0)], vec![], vec![lconn(1, 99, 1)]);
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("non-execution block"),
            "expected 'non-execution block' ref msg, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_duplicate_loop() {
        // Same (from, to) pair in two LoopConnections.
        // Endpoint exclusivity fires before duplicate-loop check.
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1), lconn(2, 1, 1)],
        );
        let err = validate_pipeline(&def).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Duplicate loop") || msg.contains("already a loop endpoint"),
            "expected duplicate/overlap rejection, got: {msg}"
        );
    }

    // -- loop migration / pruning tests --

    #[test]
    fn test_migrate_loop_direction_swaps_old_format() {
        // Old format: from=1 (ancestor), to=2 (downstream).
        // After migration: from=2, to=1 (new convention).
        let mut def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![LoopConnection {
                from: 1,
                to: 2,
                count: 1,
                prompt: String::new(),
            }],
        );
        migrate_loop_direction(&mut def);
        assert_eq!(def.loop_connections[0].from, 2);
        assert_eq!(def.loop_connections[0].to, 1);
    }

    #[test]
    fn test_migrate_loop_direction_keeps_new_format() {
        // Already new format: from=2 (downstream), to=1 (upstream).
        let mut def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );
        migrate_loop_direction(&mut def);
        assert_eq!(def.loop_connections[0].from, 2);
        assert_eq!(def.loop_connections[0].to, 1);
    }

    #[test]
    fn test_prune_invalid_loops_removes_broken() {
        // Chain: 1→2→3, loop from 3→1.
        // Remove edge 2→3 → loop 3→1 broken (1 no longer ancestor of 3).
        let mut def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
            vec![lconn(3, 1, 1)],
        );
        def.connections.retain(|c| !(c.from == 2 && c.to == 3));
        let warnings = prune_invalid_loops(&mut def);
        assert_eq!(warnings.len(), 1);
        assert!(def.loop_connections.is_empty());
    }

    #[test]
    fn test_prune_invalid_loops_keeps_valid() {
        let mut def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );
        let warnings = prune_invalid_loops(&mut def);
        assert!(warnings.is_empty());
        assert_eq!(def.loop_connections.len(), 1);
    }

    #[test]
    fn test_save_pipeline_rejects_invalid_loops() {
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![], // No regular edge — loop ancestry broken
            vec![lconn(2, 1, 1)],
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let err = save_pipeline(&def, &path).unwrap_err();
        assert!(err.to_string().contains("not a regular-graph ancestor"));
    }

    #[test]
    fn test_prune_invalid_loops_removes_overlap() {
        // Graph: 1→2→3, 2→4→5
        // Loop A: from=3, to=1 (sub-DAG {1,2,3})
        // Loop B: from=5, to=2 (sub-DAG {2,4,5})
        // Block 2 is in both sub-DAGs → overlap → loop B pruned.
        let mut def = def_with_loops(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 2, 0),
                block(4, 1, 1),
                block(5, 2, 1),
            ],
            vec![conn(1, 2), conn(2, 3), conn(2, 4), conn(4, 5)],
            vec![lconn(3, 1, 1), lconn(5, 2, 1)],
        );
        let warnings = prune_invalid_loops(&mut def);
        assert_eq!(warnings.len(), 1, "one overlapping loop should be removed");
        assert!(warnings[0].contains("overlaps"));
        assert_eq!(def.loop_connections.len(), 1);
        // The first loop (3→1) should survive
        assert_eq!(def.loop_connections[0].from, 3);
        assert_eq!(def.loop_connections[0].to, 1);
    }

    // -- loop graph utility tests (loop edges are back-edges, excluded from graph utilities) --

    #[test]
    fn test_loop_edges_not_in_upstream_of() {
        // Loop back(B,A) should NOT make A appear in upstream_of(B)
        // (only regular connections affect upstream_of)
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );
        let ups = upstream_of(&def, 1);
        // Block 1 has no regular incoming (only a loop back-edge from 2)
        assert!(
            ups.is_empty(),
            "upstream_of(A) should be empty — loop edge is not upstream"
        );
    }

    #[test]
    fn test_loop_edges_not_in_topo_layers() {
        // Loop back(B,A) should not prevent topological sort
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );
        let layers = topological_layers(&def).unwrap();
        assert_eq!(
            layers,
            vec![vec![1], vec![2]],
            "topo layers should follow regular connections only"
        );
    }

    #[test]
    fn test_loop_edges_not_in_cycle_detection() {
        // Loop back(B,A) should NOT cause would_create_cycle to detect a cycle
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );
        // Adding A→B regular edge would create a duplicate, but not a cycle
        // The cycle check should not consider the loop back-edge
        assert!(
            !would_create_cycle(&def.connections, 1, 2),
            "loop back-edge should not be considered by cycle detection"
        );
    }

    #[test]
    fn test_root_and_terminal_with_loop() {
        // A(1)→B(2), loop_back(B,A) — A is still root, B is still terminal
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );
        assert_eq!(
            root_blocks(&def),
            vec![1],
            "A should be root (loop back-edge excluded)"
        );
        assert_eq!(
            terminal_blocks(&def),
            vec![2],
            "B should be terminal (loop back-edge excluded)"
        );
    }

    // -- loop serialization test --

    #[test]
    fn test_loop_serde_roundtrip() {
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![],
            vec![LoopConnection {
                from: 1,
                to: 2,
                count: 3,
                prompt: "review again".into(),
            }],
        );
        let toml_str = toml::to_string(&def).expect("serialize");
        let loaded: PipelineDefinition = toml::from_str(&toml_str).expect("deserialize");
        assert_eq!(loaded.loop_connections.len(), 1);
        let lc = &loaded.loop_connections[0];
        assert_eq!(lc.from, 1);
        assert_eq!(lc.to, 2);
        assert_eq!(lc.count, 3);
        assert_eq!(lc.prompt, "review again");
    }

    // -- loop execution tests --

    #[tokio::test]
    async fn test_loop_exec_basic() {
        // A(1)→B(2) with loop_back(B, A, count=1).
        // Expected: pass 0 runs A then B, loop re-runs A then B (pass 1).
        // Total BlockFinished events: 4 (2 per block, 2 passes each).
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-exec-basic")).unwrap();
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let received = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::unbounded_channel();

        let recv_clone = received.clone();
        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| {
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "loop output",
                    recv_clone.clone(),
                ))
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);

        // Count BlockFinished events per runtime block_id
        // Runtime ID 0 = block 1 (A), Runtime ID 1 = block 2 (B)
        let a_finished = events
            .iter()
            .filter(|e| {
                matches!(
                    e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 0
                )
            })
            .count();
        let b_finished = events
            .iter()
            .filter(|e| {
                matches!(
                    e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 1
                )
            })
            .count();

        assert_eq!(a_finished, 2, "A should finish twice (pass 0 + pass 1)");
        assert_eq!(b_finished, 2, "B should finish twice (pass 0 + pass 1)");
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn test_loop_downstream_waits() {
        // A(1)→B(2)→C(3), loop_back(B, A, count=1).
        // C should only start after the loop completes.
        // B's external child (C) is deferred until loop exit.
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-downstream-waits")).unwrap();
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
            vec![lconn(2, 1, 1)],
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let received = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::unbounded_channel();

        let recv_clone = received.clone();
        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| {
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "output",
                    recv_clone.clone(),
                ))
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);

        // Runtime IDs: 0=A(block1), 1=B(block2), 2=C(block3)
        // Find index of C's BlockStarted
        let c_started_idx = events
            .iter()
            .position(|e| {
                matches!(
                    e, ProgressEvent::BlockStarted { block_id, .. } if *block_id == 2
                )
            })
            .expect("C should have started");

        // Find index of B's LAST BlockFinished (pass 1)
        let b_last_finished_idx = events
            .iter()
            .rposition(|e| {
                matches!(
                    e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 1
                )
            })
            .expect("B should have finished");

        assert!(
            c_started_idx > b_last_finished_idx,
            "C's BlockStarted (idx {c_started_idx}) should come after B's final BlockFinished (idx {b_last_finished_idx})"
        );
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn test_loop_mixed_panic_replica_completes() {
        // Regression: when one replica of the `from` (feedback source) block
        // panics while the other succeeds, the loop must still advance passes.
        //
        // Setup: A(1)→B(2, replicas=2), loop_back(B, A, count=1).
        // B is the feedback source (from). One replica panics each pass.
        // Expected: loop still completes 2 passes, A finishes twice.
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-mixed-panic")).unwrap();
        let mut b_block = block(2, 1, 0);
        b_block.replicas = 2;
        let def = def_with_loops(
            vec![block(1, 0, 0), b_block],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, rx) = mpsc::unbounded_channel();

        // Counter to alternate: odd calls get PanicProvider, even get MockProvider.
        let call_counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));
        let recv_clone = received.clone();
        let counter_clone = call_counter.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| {
                let n = counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n % 2 == 1 {
                    Box::new(PanicProvider::new(ProviderKind::Anthropic, "replica panic"))
                } else {
                    Box::new(MockProvider::ok(
                        ProviderKind::Anthropic,
                        "mixed output",
                        recv_clone.clone(),
                    ))
                }
            },
        )
        .await
        .expect("run should complete despite partial panics");

        let events = collect_progress_events(rx);

        // A gets runtime 0, B gets runtimes 1,2 (replicas=2)
        // A (runtime 0) should finish twice (pass 0 + pass 1)
        let a_finished = events
            .iter()
            .filter(|e| {
                matches!(
                    e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 0
                )
            })
            .count();
        assert_eq!(a_finished, 2, "A should finish twice (pass 0 + pass 1)");

        // At least one BlockError from the panicking replica
        assert!(
            events.iter().any(|e| matches!(
                e, ProgressEvent::BlockError { error, .. } if error.contains("panicked")
            )),
            "should have a panic error event"
        );

        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn test_loop_mixed_panic_on_to_side_completes() {
        // Regression: partial panic on the `to` (restart target) block.
        //
        // Setup: A(1, replicas=2)→B(2), loop_back(B, A, count=1).
        // A is the restart target with replicas=2. One replica panics.
        // Expected: loop still completes, B finishes twice.
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-to-panic")).unwrap();
        let mut a_block = block(1, 0, 0);
        a_block.replicas = 2;
        let def = def_with_loops(
            vec![a_block, block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 1)],
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, rx) = mpsc::unbounded_channel();

        // Factory calls: 0=A (even→ok), 1=B_r0 (odd→panic), 2=B_r1 (even→ok).
        let call_counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));
        let recv_clone = received.clone();
        let counter_clone = call_counter.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| {
                let n = counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n % 2 == 1 {
                    Box::new(PanicProvider::new(ProviderKind::Anthropic, "to-side panic"))
                } else {
                    Box::new(MockProvider::ok(
                        ProviderKind::Anthropic,
                        "to output",
                        recv_clone.clone(),
                    ))
                }
            },
        )
        .await
        .expect("run should complete despite partial to-side panics");

        let events = collect_progress_events(rx);

        // A gets runtimes 0,1 (replicas=2), B gets runtime 2
        // B (runtime 2, feedback source) should finish twice (pass 0 + pass 1)
        let b_finished = events
            .iter()
            .filter(|e| {
                matches!(
                    e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 2
                )
            })
            .count();
        assert_eq!(b_finished, 2, "B should finish twice (pass 0 + pass 1)");

        // At least one BlockError from the panicking A replica
        assert!(
            events.iter().any(|e| matches!(
                e, ProgressEvent::BlockError { error, .. } if error.contains("panicked")
            )),
            "should have a panic error event from A replica"
        );

        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[test]
    fn test_loop_filename_generation() {
        let info = RuntimeReplicaInfo {
            runtime_id: 0,
            source_block_id: 1,
            replica_index: 0,
            phase: RuntimePhase::Execution,
            run_scope: None,
            agent: "Claude".into(),
            display_label: "Block#1".into(),
            session_key: "__block_1".into(),
            filename_stem: "block1_claude".into(),
        };

        // pass=0 should produce same as replica_filename
        let pass0 = loop_replica_filename(&info, 1, 0);
        let normal = replica_filename(&info, 1);
        assert_eq!(pass0, normal, "pass 0 should match replica_filename");
        assert_eq!(pass0, "block1_claude_iter1.md");

        // pass=1 should include _loop1
        let pass1 = loop_replica_filename(&info, 1, 1);
        assert_eq!(pass1, "block1_claude_iter1_loop1.md");

        // pass=2 at iteration 3
        let pass2 = loop_replica_filename(&info, 3, 2);
        assert_eq!(pass2, "block1_claude_iter3_loop2.md");
    }

    // -- sub-DAG computation tests --

    #[test]
    fn test_compute_sub_dag_linear() {
        // A(1)→B(2)→C(3), sub_dag(C,A) = {A,B,C}
        let def = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        let graph = RegularGraph::from_def(&def);
        let sd = compute_loop_sub_dag(&graph, 3, 1).expect("should compute");
        assert_eq!(sd.len(), 3);
        assert!(sd.contains(&1) && sd.contains(&2) && sd.contains(&3));
    }

    #[test]
    fn test_compute_sub_dag_diamond() {
        // A(1)→{B(2),C(3)}→D(4), sub_dag(D,A) = {A,B,C,D}
        let def = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 2), conn(1, 3), conn(2, 4), conn(3, 4)],
        );
        let graph = RegularGraph::from_def(&def);
        let sd = compute_loop_sub_dag(&graph, 4, 1).expect("should compute");
        assert_eq!(sd.len(), 4);
    }

    #[test]
    fn test_compute_sub_dag_partial() {
        // A(1)→B(2)→C(3)→D(4), sub_dag(C,B) = {B,C}
        let def = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 2, 0),
                block(4, 3, 0),
            ],
            vec![conn(1, 2), conn(2, 3), conn(3, 4)],
        );
        let graph = RegularGraph::from_def(&def);
        let sd = compute_loop_sub_dag(&graph, 3, 2).expect("should compute");
        assert_eq!(sd.len(), 2);
        assert!(sd.contains(&2) && sd.contains(&3));
        assert!(!sd.contains(&1) && !sd.contains(&4));
    }

    #[test]
    fn test_compute_sub_dag_not_ancestor() {
        // A(1) and B(2) disconnected — no path
        let def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let graph = RegularGraph::from_def(&def);
        assert!(compute_loop_sub_dag(&graph, 2, 1).is_none());
    }

    #[test]
    fn test_compute_sub_dag_mixed_parents() {
        // X(5)→B(2), A(1)→B(2)→C(3), sub_dag(C,A) = {A,B,C}, X excluded
        let def = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 2, 0),
                block(5, 0, 1),
            ],
            vec![conn(1, 2), conn(5, 2), conn(2, 3)],
        );
        let graph = RegularGraph::from_def(&def);
        let sd = compute_loop_sub_dag(&graph, 3, 1).expect("should compute");
        assert_eq!(sd.len(), 3);
        assert!(sd.contains(&1) && sd.contains(&2) && sd.contains(&3));
        assert!(!sd.contains(&5));
    }

    // -- intermediate re-run execution test --

    #[tokio::test]
    async fn test_loop_exec_intermediate_reruns() {
        // A(1)→B(2)→C(3), loop_back(C, A, count=1).
        // All three blocks are in the sub-DAG. B should re-run on pass 1.
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-intermediate")).unwrap();
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
            vec![lconn(3, 1, 1)],
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let received = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::unbounded_channel();

        let recv_clone = received.clone();
        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| {
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "intermediate output",
                    recv_clone.clone(),
                ))
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);

        // Runtime IDs: 0=A, 1=B, 2=C
        // All three should finish twice (pass 0 + pass 1)
        for (rid, name) in [(0, "A"), (1, "B"), (2, "C")] {
            let count = events
                .iter()
                .filter(|e| {
                    matches!(
                        e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == rid
                    )
                })
                .count();
            assert_eq!(count, 2, "{name} (runtime {rid}) should finish twice");
        }
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn test_loop_exec_external_deferred() {
        // A(1)→B(2)→C(3), B(2)→E(4), loop_back(C, A, count=1).
        // E is B's external child — deferred until loop done.
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-external-deferred")).unwrap();
        let def = def_with_loops(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 2, 0),
                block(4, 2, 1),
            ],
            vec![conn(1, 2), conn(2, 3), conn(2, 4)],
            vec![lconn(3, 1, 1)],
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let received = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::unbounded_channel();

        let recv_clone = received.clone();
        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| {
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "output",
                    recv_clone.clone(),
                ))
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);

        // Runtime IDs: 0=A, 1=B, 2=C, 3=E
        // E (runtime 3) should start only after the loop completes
        let e_started_idx = events
            .iter()
            .position(|e| {
                matches!(
                    e, ProgressEvent::BlockStarted { block_id, .. } if *block_id == 3
                )
            })
            .expect("E should have started");

        // C (runtime 2, feedback source) last finished = loop completion
        let c_last_finished_idx = events
            .iter()
            .rposition(|e| {
                matches!(
                    e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 2
                )
            })
            .expect("C should have finished");

        assert!(
            e_started_idx > c_last_finished_idx,
            "E should start after loop completes (C's final finish)"
        );
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn test_loop_abort_with_external_downstream_completes() {
        // Regression: loop abort must not double-release deferred external edges.
        //
        // Setup: A(1)→B(2)→C(3), B(2)→E(4), loop_back(C, A, count=1).
        // A panics → B, C skipped → loop abandons → deferred releases E exactly once.
        // Must complete without hanging.
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("loop-abort-ext")).unwrap();
        let def = def_with_loops(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 2, 0),
                block(4, 2, 1),
            ],
            vec![conn(1, 2), conn(2, 3), conn(2, 4)],
            vec![lconn(3, 1, 1)],
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, rx) = mpsc::unbounded_channel();
        let received = Arc::new(Mutex::new(Vec::new()));
        let recv_clone = received.clone();

        // Counter: first pool entry (A) panics, rest succeed
        let call_counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = call_counter.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| {
                let n = counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n == 0 {
                    // A (first pool entry) panics
                    Box::new(PanicProvider::new(ProviderKind::Anthropic, "A fails"))
                } else {
                    Box::new(MockProvider::ok(
                        ProviderKind::Anthropic,
                        "output",
                        recv_clone.clone(),
                    ))
                }
            },
        )
        .await
        .expect("run should complete despite A panicking");

        let events = collect_progress_events(rx);

        // A panics
        assert!(
            events.iter().any(|e| matches!(
                e, ProgressEvent::BlockError { error, .. } if error.contains("panicked")
            )),
            "should have a panic error from A"
        );

        // E (runtime 3) should be skipped exactly once, not started or double-enqueued
        let e_skipped = events
            .iter()
            .filter(|e| {
                matches!(
                    e, ProgressEvent::BlockSkipped { block_id, .. } if *block_id == 3
                )
            })
            .count();
        assert_eq!(e_skipped, 1, "E should be skipped exactly once");

        // E should NOT have a BlockStarted event
        let e_started = events
            .iter()
            .filter(|e| {
                matches!(
                    e, ProgressEvent::BlockStarted { block_id, .. } if *block_id == 3
                )
            })
            .count();
        assert_eq!(
            e_started, 0,
            "E should not be started when loop is abandoned"
        );

        // Must reach AllDone (no hang from double-release)
        assert!(
            events.iter().any(|e| matches!(e, ProgressEvent::AllDone)),
            "pipeline should reach AllDone after loop abort"
        );
    }

    #[tokio::test]
    async fn test_panic_after_success_evicts_stale_output() {
        // Regression: a replica that succeeds on pass N and panics on pass N+1
        // must have its stale output removed from replica_outputs so that
        // subsequent loop-pass feedback doesn't include the stale content.
        //
        // Setup: A(1)→B(2, replicas=2), loop_back(B, A, count=2) → 3 passes.
        // B-r2 (runtime 2) succeeds on pass 0 with "STALE_MARKER", then panics
        // on passes 1+. B-r1 (runtime 1) always succeeds with "fresh-B1".
        //
        // The provider factory is called once per unique (agent, session_key)
        // during pool init. Each replica has a distinct session_key, so each
        // gets its own factory call. The returned provider is reused for all
        // send() calls across loop passes.
        //
        // On pass 2, A's re-run feedback is assembled from B's replica_outputs.
        // With the fix, B-r2's entry was removed when it panicked on pass 1,
        // so A's pass-2 prompt must NOT contain "STALE_MARKER".
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("panic-stale")).unwrap();
        let mut b_block = block(2, 1, 0);
        b_block.replicas = 2;
        let def = def_with_loops(
            vec![block(1, 0, 0), b_block],
            vec![conn(1, 2)],
            vec![lconn(2, 1, 2)], // count=2 → 3 passes
        );

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, rx) = mpsc::unbounded_channel();

        // Pool init iterates rt.entries in order:
        //   n=0 → A (runtime 0): MockProvider with 3 Ok responses, captures prompts
        //   n=1 → B-r1 (runtime 1): MockProvider with 3 Ok responses ("fresh-B1")
        //   n=2 → B-r2 (runtime 2): SuccessThenPanicProvider — succeeds once, panics after
        let received_a = Arc::new(Mutex::new(Vec::new()));
        let recv_clone = received_a.clone();
        let call_counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = call_counter.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| {
                use crate::execution::test_utils::ok_response;
                let n = counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                match n {
                    0 => {
                        // A: succeeds 3 times, captures received prompts
                        Box::new(MockProvider::with_responses(
                            ProviderKind::Anthropic,
                            vec![
                                ok_response("A-p0"),
                                ok_response("A-p1"),
                                ok_response("A-p2"),
                            ],
                            recv_clone.clone(),
                        ))
                    }
                    1 => {
                        // B-r1: always succeeds
                        Box::new(MockProvider::with_responses(
                            ProviderKind::Anthropic,
                            vec![
                                ok_response("fresh-B1"),
                                ok_response("fresh-B1"),
                                ok_response("fresh-B1"),
                            ],
                            Arc::new(Mutex::new(Vec::new())),
                        ))
                    }
                    2 => {
                        // B-r2: succeeds on first send (pass 0), panics on subsequent
                        Box::new(SuccessThenPanicProvider::new(
                            ProviderKind::Anthropic,
                            "STALE_MARKER",
                            "B-r2 panicked",
                        ))
                    }
                    _ => Box::new(MockProvider::ok(
                        ProviderKind::Anthropic,
                        "unexpected",
                        Arc::new(Mutex::new(Vec::new())),
                    )),
                }
            },
        )
        .await
        .expect("run should complete");

        let events = collect_progress_events(rx);

        // Pipeline should complete
        assert!(
            events.iter().any(|e| matches!(e, ProgressEvent::AllDone)),
            "pipeline should reach AllDone"
        );

        // B-r2 should have panic errors
        assert!(
            events.iter().any(|e| matches!(
                e, ProgressEvent::BlockError { error, .. } if error.contains("panicked")
            )),
            "B-r2 should emit a panic error"
        );

        // Key assertion: A's pass-2 prompt must NOT contain stale B-r2 output.
        // A is called 3 times (pass 0, 1, 2). The pass-2 prompt is the last.
        let prompts = received_a.lock().expect("lock");
        assert!(
            prompts.len() >= 3,
            "A should receive at least 3 prompts (passes 0,1,2), got {}",
            prompts.len()
        );
        let pass2_prompt = &prompts[2];
        assert!(
            !pass2_prompt.contains("STALE_MARKER"),
            "A's pass-2 feedback must not contain stale B-r2 pass-0 output.\nGot prompt:\n{pass2_prompt}"
        );
    }

    #[test]
    fn test_prune_invalid_loops_internal_block_deletion() {
        // Regression: deleting a block that is internal to a loop's sub-DAG
        // (not an endpoint) should invalidate the loop.
        //
        // Chain: 1→2→3, loop from 3→1.
        // Delete block 2 (internal node) → loop 3→1 should be pruned because
        // the path from 1 to 3 is broken.
        let mut def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
            vec![lconn(3, 1, 1)],
        );
        // Simulate block deletion: remove block 2 and its connections
        def.blocks.retain(|b| b.id != 2);
        def.connections.retain(|c| c.from != 2 && c.to != 2);
        def.loop_connections.retain(|lc| lc.from != 2 && lc.to != 2);
        // Now prune loops with broken sub-DAGs
        let warnings = prune_invalid_loops(&mut def);
        assert_eq!(
            warnings.len(),
            1,
            "loop with broken internal path should be pruned"
        );
        assert!(
            def.loop_connections.is_empty(),
            "loop 3→1 should be removed"
        );
    }

    // -- multi-agent runtime table tests --

    #[test]
    fn runtime_table_single_agent_single_replica() {
        let def = def_with(vec![block(1, 0, 0)], vec![]);
        let rt = build_runtime_table(&def);
        assert_eq!(rt.entries.len(), 1);
        assert_eq!(rt.entries[0].agent, "Claude");
        assert_eq!(rt.entries[0].display_label, "Block#1");
    }

    #[test]
    fn runtime_table_two_agents_single_replica() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].agents = vec!["Claude".into(), "GPT".into()];
        let rt = build_runtime_table(&def);
        assert_eq!(rt.entries.len(), 2);
        assert_eq!(rt.entries[0].agent, "Claude");
        assert!(rt.entries[0].display_label.contains("Claude"));
        assert_eq!(rt.entries[1].agent, "GPT");
        assert!(rt.entries[1].display_label.contains("GPT"));
    }

    #[test]
    fn runtime_table_single_agent_three_replicas() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].replicas = 3;
        let rt = build_runtime_table(&def);
        assert_eq!(rt.entries.len(), 3);
        for (i, e) in rt.entries.iter().enumerate() {
            assert_eq!(e.agent, "Claude");
            assert!(e.display_label.contains(&format!("r{}", i + 1)));
        }
    }

    #[test]
    fn runtime_table_two_agents_three_replicas() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].agents = vec!["Claude".into(), "GPT".into()];
        def.blocks[0].replicas = 3;
        let rt = build_runtime_table(&def);
        assert_eq!(rt.entries.len(), 6); // 2 agents × 3 replicas
                                         // First 3 entries: Claude r1, r2, r3
        assert_eq!(rt.entries[0].agent, "Claude");
        assert!(
            rt.entries[0].display_label.contains("Claude")
                && rt.entries[0].display_label.contains("r1")
        );
        assert_eq!(rt.entries[2].agent, "Claude");
        assert!(rt.entries[2].display_label.contains("r3"));
        // Next 3 entries: GPT r1, r2, r3
        assert_eq!(rt.entries[3].agent, "GPT");
        assert!(
            rt.entries[3].display_label.contains("GPT")
                && rt.entries[3].display_label.contains("r1")
        );
        assert_eq!(rt.entries[5].agent, "GPT");
        assert!(rt.entries[5].display_label.contains("r3"));
        // Session keys: 3 unique (per-replica), shared across agents (provider pool
        // disambiguates by (agent, session_key) pair)
        let keys: HashSet<String> = rt.entries.iter().map(|e| e.session_key.clone()).collect();
        assert_eq!(keys.len(), 3);
        // But (agent, session_key) pairs are unique
        let agent_keys: HashSet<(String, String)> = rt
            .entries
            .iter()
            .map(|e| (e.agent.clone(), e.session_key.clone()))
            .collect();
        assert_eq!(agent_keys.len(), 6);
        // Unique filename stems
        let stems: HashSet<String> = rt.entries.iter().map(|e| e.filename_stem.clone()).collect();
        assert_eq!(stems.len(), 6);
    }

    #[test]
    fn runtime_table_multi_agent_logical_mapping() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].agents = vec!["A".into(), "B".into()];
        def.blocks[0].replicas = 2;
        let rt = build_runtime_table(&def);
        let rids = &rt.logical_to_runtime[&1];
        assert_eq!(rids.len(), 4); // 2 agents × 2 replicas
    }

    // -- serde backward compat --

    #[test]
    fn serde_legacy_agent_field_deserialized_as_agents() {
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            agent = "OldAgent"
            position = [0, 0]
        "#;
        let def: PipelineDefinition = toml::from_str(toml_str).expect("should parse legacy agent");
        assert_eq!(def.blocks[0].agents, vec!["OldAgent".to_string()]);
    }

    #[test]
    fn serde_new_agents_field_deserialized() {
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            agents = ["Claude", "GPT"]
            position = [0, 0]
        "#;
        let def: PipelineDefinition = toml::from_str(toml_str).expect("should parse agents list");
        assert_eq!(
            def.blocks[0].agents,
            vec!["Claude".to_string(), "GPT".to_string()]
        );
    }

    #[test]
    fn serde_agents_takes_precedence_over_agent() {
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            agent = "Old"
            agents = ["New1", "New2"]
            position = [0, 0]
        "#;
        let def: PipelineDefinition = toml::from_str(toml_str).expect("should parse");
        assert_eq!(
            def.blocks[0].agents,
            vec!["New1".to_string(), "New2".to_string()]
        );
    }

    #[test]
    fn serde_omitted_agents_defaults_to_claude() {
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            position = [0, 0]
        "#;
        let def: PipelineDefinition = toml::from_str(toml_str).expect("should default to Claude");
        assert_eq!(def.blocks[0].agents, vec!["Claude".to_string()]);
    }

    #[test]
    fn serde_explicit_empty_agents_is_rejected() {
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            agents = []
            position = [0, 0]
        "#;
        let result: Result<PipelineDefinition, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn serde_explicit_empty_agent_string_is_rejected() {
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            agent = ""
            position = [0, 0]
        "#;
        let result: Result<PipelineDefinition, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn serde_whitespace_only_agent_string_is_rejected() {
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            agent = "   "
            position = [0, 0]
        "#;
        let result: Result<PipelineDefinition, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn serde_blank_agent_in_list_is_rejected() {
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            agents = ["Claude", ""]
            position = [0, 0]
        "#;
        let result: Result<PipelineDefinition, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn serde_serializes_as_agents_not_agent() {
        let def = def_with(vec![block(1, 0, 0)], vec![]);
        let toml_str = toml::to_string_pretty(&def).expect("should serialize");
        assert!(toml_str.contains("agents = "));
        assert!(!toml_str.contains("\nagent = "));
    }

    #[test]
    fn effective_sessions_multi_agent_block() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].agents = vec!["A".into(), "B".into()];
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 2);
        let agents: Vec<&str> = sessions.iter().map(|s| s.agent.as_str()).collect();
        assert!(agents.contains(&"A"));
        assert!(agents.contains(&"B"));
    }

    // -- validation: empty agents --

    #[test]
    fn validate_rejects_empty_agents() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].agents = vec![];
        assert!(validate_pipeline(&def).is_err());
    }

    // -- validation: agents × replicas cap --

    #[test]
    fn validate_rejects_agents_times_replicas_over_32() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].agents = vec!["A".into(), "B".into(), "C".into(), "D".into(), "E".into()];
        def.blocks[0].replicas = 7; // 5 × 7 = 35 > 32
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn validate_accepts_agents_times_replicas_at_32() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].agents = vec!["A".into(), "B".into(), "C".into(), "D".into()];
        def.blocks[0].replicas = 8; // 4 × 8 = 32
        assert!(validate_pipeline(&def).is_ok());
    }

    // -- RegularGraph uses agents × replicas --

    #[test]
    fn regular_graph_task_count_includes_agents() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.blocks[0].agents = vec!["A".into(), "B".into()];
        def.blocks[0].replicas = 3;
        let graph = RegularGraph::from_def(&def);
        assert_eq!(graph.replicas[&1], 6); // 2 agents × 3 replicas
    }

    // -- regression: next_free_position with far-out coordinates --

    #[test]
    fn next_free_position_capped_with_extreme_coordinates() {
        // A block at u16::MAX should not cause a hang; search is capped at 100×100
        let d = def_with(vec![block(1, 60000, 60000)], vec![]);
        let pos = next_free_position(&d.blocks);
        assert_ne!(pos, (60000, 60000));
        // The returned position must not collide with any existing block
        assert!(!d.blocks.iter().any(|b| b.position == pos));
    }

    #[test]
    fn next_free_position_fallback_avoids_occupied_out_of_range() {
        // Fallback row (0, 101) is already occupied — must skip past it
        let d = def_with(vec![block(1, 60000, 60000), block(2, 0, 101)], vec![]);
        let pos = next_free_position(&d.blocks);
        assert!(!d.blocks.iter().any(|b| b.position == pos));
    }

    // -- regression: UTF-8 safe truncation --

    #[test]
    fn truncate_at_char_boundary_does_not_panic() {
        // Simulate the pipeline truncation logic with multibyte content
        let mut buf = "á".repeat(300_000); // 2 bytes each = 600KB > 512KB cap
        let max = super::MAX_TERMINAL_OUTPUTS_BYTES;
        if buf.len() > max {
            let mut end = max;
            while end > 0 && !buf.is_char_boundary(end) {
                end -= 1;
            }
            buf.truncate(end);
        }
        assert!(buf.len() <= max);
        // Verify it's still valid UTF-8
        assert!(std::str::from_utf8(buf.as_bytes()).is_ok());
    }

    // -- Finalization DAG tests --

    fn fin_block(id: BlockId, col: u16, row: u16) -> PipelineBlock {
        PipelineBlock {
            id,
            name: format!("Fin#{id}"),
            agents: vec!["Claude".into()],
            prompt: format!("finalization {id}"),
            profiles: vec![],
            session_id: None,
            position: (col, row),
            replicas: 1,
        }
    }

    fn feed(from: BlockId, to: BlockId) -> DataFeed {
        DataFeed {
            from,
            to,
            collection: FeedCollection::LastIteration,
            granularity: FeedGranularity::PerRun,
        }
    }

    fn feed_all_runs(from: BlockId, to: BlockId) -> DataFeed {
        DataFeed {
            from,
            to,
            collection: FeedCollection::LastIteration,
            granularity: FeedGranularity::AllRuns,
        }
    }

    #[test]
    fn backward_compat_old_pipeline_deserializes() {
        // Old TOML with no finalization fields should still deserialize
        let toml_str = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]
"#;
        let def: PipelineDefinition = toml::from_str(toml_str).unwrap();
        assert!(!def.has_finalization());
        assert!(def.finalization_blocks.is_empty());
        assert!(def.finalization_connections.is_empty());
        assert!(def.data_feeds.is_empty());
    }

    #[test]
    fn finalization_toml_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fin_test.toml");
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        def.data_feeds = vec![feed(1, 10)];
        save_pipeline(&def, &path).unwrap();
        let loaded = load_pipeline(&path).unwrap();
        assert_eq!(loaded.finalization_blocks.len(), 1);
        assert_eq!(loaded.finalization_blocks[0].id, 10);
        assert_eq!(loaded.data_feeds.len(), 1);
        assert_eq!(loaded.data_feeds[0].from, 1);
        assert_eq!(loaded.data_feeds[0].to, 10);
    }

    #[test]
    fn validate_rejects_cross_phase_connections() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        def.data_feeds = vec![feed(1, 10)];
        // Add a regular connection from execution to finalization
        def.connections.push(conn(1, 10));
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("non-existent") || err.to_string().contains("block"),
            "expected block ref error, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_finalization_cycles() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0), fin_block(11, 1, 0)];
        def.finalization_connections = vec![conn(10, 11), conn(11, 10)];
        def.data_feeds = vec![feed(1, 10)];
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("cycle"),
            "expected cycle error, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_feeds_to_non_finalization_blocks() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        // Feed targeting execution block 2
        def.data_feeds = vec![feed(1, 2)];
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("finalization block"),
            "expected finalization block error, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_loop_endpoints_in_finalization() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0), fin_block(11, 1, 0)];
        def.data_feeds = vec![feed(1, 10)];
        // Loop referencing finalization block
        def.loop_connections.push(LoopConnection {
            from: 10,
            to: 11,
            count: 1,
            prompt: String::new(),
        });
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("non-execution block"),
            "expected non-execution block error, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_self_edge_finalization_connections() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        def.finalization_connections = vec![conn(10, 10)];
        def.data_feeds = vec![feed(1, 10)];
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("self-edge"),
            "expected self-edge error, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_duplicate_finalization_connections() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0), fin_block(11, 1, 0)];
        def.finalization_connections = vec![conn(10, 11), conn(10, 11)];
        def.data_feeds = vec![feed(1, 10)];
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string()
                .contains("Duplicate finalization connection"),
            "expected duplicate error, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_duplicate_feed_tuples() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        def.data_feeds = vec![feed(1, 10), feed(1, 10)];
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("Duplicate data feed"),
            "expected duplicate feed error, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_wildcard_conflict() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        // Wildcard feed + block-specific feed to same target
        def.data_feeds = vec![feed(WILDCARD_BLOCK_ID, 10), feed(1, 10)];
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("Wildcard feed conflicts"),
            "expected wildcard conflict error, got: {err}"
        );
    }

    #[test]
    fn is_per_run_propagates_transitively() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![
            fin_block(10, 0, 0),
            fin_block(11, 1, 0),
            fin_block(12, 2, 0),
        ];
        def.finalization_connections = vec![conn(10, 11), conn(11, 12)];
        def.data_feeds = vec![feed(1, 10)]; // PerRun feed to block 10
        assert!(def.is_per_run_finalization_block(10));
        assert!(def.is_per_run_finalization_block(11)); // propagated
        assert!(def.is_per_run_finalization_block(12)); // propagated transitively
    }

    #[test]
    fn is_per_run_all_runs_not_propagated() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0), fin_block(11, 1, 0)];
        def.finalization_connections = vec![conn(10, 11)];
        def.data_feeds = vec![feed_all_runs(1, 10)]; // AllRuns feed to block 10
        assert!(!def.is_per_run_finalization_block(10));
        assert!(!def.is_per_run_finalization_block(11));
    }

    #[test]
    fn validate_accepts_valid_finalization_dag() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![conn(1, 2)]);
        def.finalization_blocks = vec![fin_block(10, 0, 0), fin_block(11, 1, 0)];
        def.finalization_connections = vec![conn(10, 11)];
        def.data_feeds = vec![feed(WILDCARD_BLOCK_ID, 10)]; // wildcard
        assert!(validate_pipeline(&def).is_ok());
    }

    #[test]
    fn validate_rejects_duplicate_ids_across_phases() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        // Finalization block reuses execution block ID
        def.finalization_blocks = vec![fin_block(1, 0, 0)];
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            err.to_string().contains("Duplicate block ID"),
            "expected duplicate ID error, got: {err}"
        );
    }

    #[test]
    fn all_blocks_iterates_both_phases() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        let ids: Vec<BlockId> = def.all_blocks().map(|b| b.id).collect();
        assert_eq!(ids, vec![1, 2, 10]);
    }

    #[test]
    fn execution_and_finalization_block_id_sets() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        assert_eq!(def.execution_block_ids(), [1, 2].into_iter().collect());
        assert_eq!(def.finalization_block_ids(), [10].into_iter().collect());
        assert!(def.is_finalization_block(10));
        assert!(!def.is_finalization_block(1));
    }
}
