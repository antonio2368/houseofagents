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

const MAX_FEED_ASSEMBLY_BYTES: usize = 512 * 1024; // 512 KB cap

pub type BlockId = u32;
/// Sentinel value for wildcard data feeds: "all execution blocks".
pub const WILDCARD_BLOCK_ID: BlockId = 0;
type ProviderPool = HashMap<(String, String), Arc<Mutex<Box<dyn provider::Provider>>>>;
pub(crate) type PipelineAgentConfigs =
    HashMap<String, (ProviderKind, crate::config::ProviderConfig, bool)>;
pub(crate) type ProviderFactory = Arc<
    dyn Fn(ProviderKind, &crate::config::ProviderConfig) -> Box<dyn provider::Provider>
        + Send
        + Sync,
>;

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
    block_outputs: &'a HashMap<u32, String>,
    output: &'a OutputManager,
    prompt_context: &'a PromptRuntimeContext,
    runtime_table: &'a RuntimeReplicaTable,
    block_to_loop: &'a HashMap<BlockId, (BlockId, BlockId)>,
    block_loop_pass: &'a HashMap<BlockId, u32>,
    scatter_injection: Option<String>,
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
    pub keep_loop_policy: HashMap<(String, String), bool>,
}

pub(crate) fn build_runtime_table(def: &PipelineDefinition) -> RuntimeReplicaTable {
    let mut entries = Vec::new();
    let mut logical_to_runtime: HashMap<BlockId, Vec<u32>> = HashMap::new();
    let mut keep_loop_policy: HashMap<(String, String), bool> = HashMap::new();
    let mut next_id: u32 = 0;

    for block in &def.blocks {
        // Sub-pipeline blocks: one runtime entry per replica
        if block.is_sub_pipeline() {
            let blabel = block_label(block);
            let multi_replica = block.replicas > 1;
            let mut runtime_ids = Vec::new();
            for ri in 0..block.replicas {
                let display_label = if multi_replica {
                    format!("{} [sub-pipeline] (r{})", blabel, ri + 1)
                } else {
                    format!("{blabel} [sub-pipeline]")
                };
                let filename_stem = if multi_replica {
                    format!("sub_b{}_pipeline_r{}", block.id, ri + 1)
                } else {
                    format!("sub_b{}_pipeline", block.id)
                };
                let session_key = if multi_replica {
                    format!("__sub_{}_r{}", block.id, ri + 1)
                } else {
                    format!("__sub_{}", block.id)
                };
                entries.push(RuntimeReplicaInfo {
                    runtime_id: next_id,
                    source_block_id: block.id,
                    replica_index: ri,
                    phase: RuntimePhase::Execution,
                    run_scope: None,
                    agent: format!("[{}]", block.name),
                    display_label,
                    session_key,
                    filename_stem,
                });
                runtime_ids.push(next_id);
                next_id += 1;
            }
            logical_to_runtime
                .entry(block.id)
                .or_default()
                .extend(runtime_ids);
            continue;
        }

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

fn replica_filename(info: &RuntimeReplicaInfo) -> String {
    format!("{}.md", info.filename_stem)
}

fn loop_replica_filename(info: &RuntimeReplicaInfo, loop_pass: u32) -> String {
    if loop_pass == 0 {
        replica_filename(info)
    } else {
        format!("{}_loop{}.md", info.filename_stem, loop_pass)
    }
}

fn scatter_replica_filename(
    info: &RuntimeReplicaInfo,
    item_index: usize,
    loop_pass: u32,
) -> String {
    if loop_pass == 0 {
        format!("{}_item{}.md", info.filename_stem, item_index)
    } else {
        format!(
            "{}_item{}_loop{}.md",
            info.filename_stem, item_index, loop_pass
        )
    }
}

/// Parse an evaluator response: returns `true` if the first word is "BREAK"
/// (case-insensitive, ignoring trailing punctuation).
fn parse_break_decision(content: &str) -> bool {
    content
        .split_whitespace()
        .next()
        .map(|w| {
            w.trim_end_matches(|c: char| c.is_ascii_punctuation())
                .eq_ignore_ascii_case("BREAK")
        })
        .unwrap_or(false)
}

/// Evaluate whether a loop should break early by asking a dedicated agent.
/// Returns `true` if the agent decides to BREAK, `false` for CONTINUE or on error.
#[allow(clippy::too_many_arguments)]
async fn evaluate_loop_break(
    break_agent: &str,
    break_condition: &str,
    current_pass: u32,
    total_passes: u32,
    sub_dag_blocks: &HashSet<BlockId>,
    rt: &RuntimeReplicaTable,
    output: &crate::output::OutputManager,
    agent_configs: &PipelineAgentConfigs,
    provider_factory: &ProviderFactory,
    cancel: &Arc<AtomicBool>,
) -> bool {
    let (kind, cfg, _) = match agent_configs.get(break_agent) {
        Some(v) => v,
        None => return false,
    };

    // Silently clamp break_condition to 4KB to prevent it from blowing the budget.
    const MAX_CONDITION_LEN: usize = 4 * 1024;
    let clamped_condition: &str = if break_condition.len() > MAX_CONDITION_LEN {
        // Find a char boundary to avoid splitting a multi-byte char
        let end = break_condition
            .char_indices()
            .take_while(|(i, _)| *i < MAX_CONDITION_LEN)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        &break_condition[..end]
    } else {
        break_condition
    };

    // Budget: 64KB total, minus fixed prompt overhead and clamped condition length
    const TOTAL_BUDGET: usize = 64 * 1024;
    let fixed_overhead = 250 + clamped_condition.len(); // prompt template + condition
    let history_budget = TOTAL_BUDGET.saturating_sub(fixed_overhead);

    // Sort block IDs for deterministic iteration
    let mut sorted_blocks: Vec<BlockId> = sub_dag_blocks.iter().copied().collect();
    sorted_blocks.sort_unstable();

    // Collect pass history from disk (newest first for budget priority)
    let mut entries: Vec<String> = Vec::new();
    let mut used: usize = 0;
    for pass in (0..=current_pass).rev() {
        for &bid in &sorted_blocks {
            if let Some(rids) = rt.logical_to_runtime.get(&bid) {
                for &rid in rids {
                    let info = &rt.entries[rid as usize];
                    let fname = loop_replica_filename(info, pass);
                    let path = output.run_dir().join(&fname);
                    if let Ok(content) = tokio::fs::read_to_string(&path).await {
                        let entry =
                            format!("\n--- Block {} (pass {}) ---\n{}", bid, pass + 1, content);
                        if used + entry.len() > history_budget {
                            continue; // skip oversized entry, keep scanning
                        }
                        used += entry.len();
                        entries.push(entry);
                    }
                }
            }
        }
    }

    // Reverse to chronological order for the evaluator prompt
    entries.reverse();
    let history: String = entries.concat();

    let prompt = format!(
        "You are evaluating whether an iterative loop should continue or stop early.\n\
         This loop has completed pass {} of {} maximum.\n\n\
         Break condition: {}\n\n\
         === Pass history ===\n{}\n\n\
         Based on the break condition, should the loop continue iterating or stop?\n\
         Respond with exactly one word: CONTINUE or BREAK",
        current_pass + 1,
        total_passes,
        clamped_condition,
        history,
    );

    if cancel.load(std::sync::atomic::Ordering::Relaxed) {
        return false;
    }

    let mut provider = provider_factory(*kind, cfg);
    let result = tokio::select! {
        res = provider.send(&prompt) => res,
        _ = wait_for_cancel(cancel) => return false,
    };
    match result {
        Ok(resp) => parse_break_decision(&resp.content),
        // Evaluator is best-effort: on provider error, default to CONTINUE.
        // The caller emits a LoopBreakEval progress event for visibility.
        // We intentionally avoid append_error here because a non-empty
        // _errors.log marks the entire run as failed, which is wrong for
        // a non-fatal evaluator failure.
        Err(_) => false,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_pipeline: Option<PipelineDefinition>,
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
            #[serde(default)]
            sub_pipeline: Option<PipelineDefinition>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let agents = if raw.sub_pipeline.is_some() {
            // Sub-pipeline blocks must not have agents
            if raw.agents.as_ref().is_some_and(|v| !v.is_empty()) || raw.agent.is_some() {
                return Err(serde::de::Error::custom(
                    "sub-pipeline block must not have 'agents' or 'agent' fields",
                ));
            }
            vec![]
        } else {
            match (raw.agents, raw.agent) {
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
            }
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
            sub_pipeline: raw.sub_pipeline,
        })
    }
}

fn default_one() -> u32 {
    1
}

fn is_one(v: &u32) -> bool {
    *v == 1
}

fn is_false(v: &bool) -> bool {
    !*v
}

pub(crate) const DEFAULT_SCATTER_DELIMITER: &str = "===SCATTER_ITEM===";

fn default_scatter_delimiter() -> String {
    DEFAULT_SCATTER_DELIMITER.into()
}

fn is_default_scatter_delimiter(s: &str) -> bool {
    s == DEFAULT_SCATTER_DELIMITER || s.is_empty()
}

impl PipelineBlock {
    pub fn is_sub_pipeline(&self) -> bool {
        self.sub_pipeline.is_some()
    }

    /// Logical task count for scheduling: each sub-pipeline replica is one opaque unit.
    pub fn logical_task_count(&self) -> u32 {
        if self.is_sub_pipeline() {
            self.replicas
        } else {
            self.agents.len() as u32 * self.replicas
        }
    }

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
    #[serde(default, skip_serializing_if = "is_false")]
    pub scatter: bool,
    #[serde(
        default = "default_scatter_delimiter",
        skip_serializing_if = "is_default_scatter_delimiter"
    )]
    pub scatter_delimiter: String,
}

impl PipelineConnection {
    pub fn new(from: BlockId, to: BlockId) -> Self {
        Self {
            from,
            to,
            scatter: false,
            scatter_delimiter: DEFAULT_SCATTER_DELIMITER.into(),
        }
    }
}

pub(crate) struct ScatterItem {
    pub index: usize,
    pub content: String,
}

pub(crate) struct ScatterQueue {
    items: VecDeque<ScatterItem>,
    total_count: usize,
}

impl ScatterQueue {
    pub fn from_output(output: &str, delimiter: &str) -> Self {
        let delim = if delimiter.is_empty() {
            DEFAULT_SCATTER_DELIMITER
        } else {
            delimiter
        };
        let items: VecDeque<ScatterItem> = output
            .split(delim)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .enumerate()
            .map(|(i, s)| ScatterItem {
                index: i,
                content: s.to_string(),
            })
            .collect();
        let total_count = items.len();
        ScatterQueue { items, total_count }
    }
    pub fn pop(&mut self) -> Option<ScatterItem> {
        self.items.pop_front()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn total(&self) -> usize {
        self.total_count
    }
    #[allow(dead_code)]
    pub fn remaining(&self) -> usize {
        self.items.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopConnection {
    pub from: BlockId,
    pub to: BlockId,
    pub count: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prompt: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub break_condition: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub break_agent: String,
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
    #[serde(alias = "last_iteration")]
    LastPass,
    #[serde(alias = "all_iterations")]
    AllPasses,
}

impl FeedCollection {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeedCollection::LastPass => "last_pass",
            FeedCollection::AllPasses => "all_passes",
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
    #[serde(default = "default_keep_across_loop_passes")]
    pub keep_across_loop_passes: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveSession {
    pub agent: String,
    pub session_key: String,
    pub display_label: String,
    pub block_ids: Vec<BlockId>,
    pub keep_across_loop_passes: bool,
    pub total_replicas: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineDefinition {
    #[serde(default)]
    pub initial_prompt: String,
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

fn default_keep_across_loop_passes() -> bool {
    true
}

impl PipelineDefinition {
    /// Visit all agent references recursively (including sub-pipelines and break agents).
    pub fn visit_all_agent_refs(&self, visitor: &mut dyn FnMut(&str, BlockId)) {
        for block in self.blocks.iter().chain(self.finalization_blocks.iter()) {
            for agent in &block.agents {
                visitor(agent, block.id);
            }
            if let Some(ref sub) = block.sub_pipeline {
                sub.visit_all_agent_refs(visitor);
            }
        }
        for lc in &self.loop_connections {
            if !lc.break_agent.is_empty() {
                visitor(&lc.break_agent, lc.from);
            }
        }
    }

    /// Collect unique agent names from all blocks (including finalization and sub-pipelines).
    pub fn all_agent_names(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut agents = Vec::new();
        self.visit_all_agent_refs(&mut |agent, _block_id| {
            if seen.insert(agent.to_string()) {
                agents.push(agent.to_string());
            }
        });
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
                    let keep_loops = self.keep_session_across_loop_passes(&agent, &session_key);
                    EffectiveSession {
                        agent,
                        session_key,
                        display_label,
                        block_ids,
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
            // Remove entry entirely if field is back to default
            if keep {
                self.session_configs
                    .retain(|c| !(c.agent == agent && c.session_key == session_key));
            }
        } else if !keep {
            self.session_configs.push(SessionConfig {
                agent: agent.to_string(),
                session_key: session_key.to_string(),
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

        // Drop stale rows and rows where field is at default (true)
        self.session_configs.retain(|c| {
            !c.keep_across_loop_passes && valid.contains(&(c.agent.clone(), c.session_key.clone()))
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

/// Like `upstream_of`, but excludes scatter connections.
/// Use this for building prompt messages — scatter items are injected separately.
fn upstream_of_non_scatter(def: &PipelineDefinition, id: BlockId) -> Vec<BlockId> {
    def.connections
        .iter()
        .filter(|c| c.to == id && !c.scatter)
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
            replicas.insert(block.id, block.logical_task_count());
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

fn normalize_session_configs_recursive(def: &mut PipelineDefinition) {
    def.normalize_session_configs();
    for block in &mut def.blocks {
        if let Some(ref mut sub) = block.sub_pipeline {
            normalize_session_configs_recursive(sub);
        }
    }
}

fn migrate_loop_direction_recursive(def: &mut PipelineDefinition) {
    migrate_loop_direction(def);
    for block in &mut def.blocks {
        if let Some(ref mut sub) = block.sub_pipeline {
            migrate_loop_direction_recursive(sub);
        }
    }
}

pub fn save_pipeline(def: &PipelineDefinition, path: &Path) -> Result<(), AppError> {
    validate_pipeline(def)?;
    let mut normalized = def.clone();
    normalize_session_configs_recursive(&mut normalized);
    let content = toml::to_string_pretty(&normalized)
        .map_err(|e| AppError::Config(format!("Failed to serialize pipeline: {e}")))?;
    std::fs::write(path, content)?;
    Ok(())
}

pub fn load_pipeline(path: &Path) -> Result<PipelineDefinition, AppError> {
    let content = std::fs::read_to_string(path)?;
    let mut def: PipelineDefinition = toml::from_str(&content)
        .map_err(|e| AppError::Config(format!("Failed to parse pipeline: {e}")))?;
    normalize_session_configs_recursive(&mut def);
    migrate_loop_direction_recursive(&mut def);
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
        let total_tasks = block.logical_task_count();
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

    // Scatter validation rules
    {
        // Max one incoming scatter connection per block
        let mut scatter_targets: HashMap<BlockId, usize> = HashMap::new();
        for conn in &def.connections {
            if conn.scatter {
                *scatter_targets.entry(conn.to).or_default() += 1;
            }
        }
        for (&block_id, &count) in &scatter_targets {
            if count > 1 {
                return Err(AppError::Config(format!(
                    "Block {block_id} has {count} incoming scatter connections; only one is supported"
                )));
            }
        }

        // Scatter source must have logical_task_count() == 1
        for conn in &def.connections {
            if conn.scatter {
                if let Some(block) = def.blocks.iter().find(|b| b.id == conn.from) {
                    if block.logical_task_count() != 1 {
                        return Err(AppError::Config(format!(
                            "Scatter source block {} has logical_task_count {} (must be 1)",
                            conn.from,
                            block.logical_task_count()
                        )));
                    }
                }
            }
        }

        // Scatter target inside a loop must be the loop restart block (`to`)
        if def.connections.iter().any(|c| c.scatter) && !def.loop_connections.is_empty() {
            let graph = RegularGraph::from_def(def);
            for conn in &def.connections {
                if conn.scatter {
                    for lc in &def.loop_connections {
                        if let Some(sub_dag) = compute_loop_sub_dag(&graph, lc.from, lc.to) {
                            if sub_dag.contains(&conn.to) && conn.to != lc.to {
                                return Err(AppError::Config(format!(
                                    "Scatter target block {} is inside loop ({} → {}) but is not the loop restart block {}",
                                    conn.to, lc.from, lc.to, lc.to
                                )));
                            }
                        }
                    }
                }
            }
        }

        // Scatter forbidden on finalization connections
        for conn in &def.finalization_connections {
            if conn.scatter {
                return Err(AppError::Config(
                    "Scatter connections are not allowed in finalization phase".into(),
                ));
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
            // break_agent and break_condition must both be set or both empty
            if lc.break_agent.is_empty() != lc.break_condition.is_empty() {
                return Err(AppError::Config(format!(
                    "Loop {}→{}: break_agent and break_condition must both be set or both empty",
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
        if block.agents.is_empty() && !block.is_sub_pipeline() {
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

    // Rule 13: Sub-pipeline block validation
    // 13: reject sub_pipeline on finalization blocks
    for block in &def.finalization_blocks {
        if block.is_sub_pipeline() {
            return Err(AppError::Config(format!(
                "Finalization block {} cannot be a sub-pipeline",
                block.id
            )));
        }
    }
    for block in &def.blocks {
        if !block.is_sub_pipeline() {
            continue;
        }
        let sub_def = block.sub_pipeline.as_ref().unwrap();

        // 13: Strict field rejection — sub-pipeline blocks must not have agent/prompt/profile/session
        if !block.agents.is_empty() {
            return Err(AppError::Config(format!(
                "Sub-pipeline block '{}' must not have agents",
                block.name
            )));
        }
        if !block.prompt.is_empty() {
            return Err(AppError::Config(format!(
                "Sub-pipeline block '{}' must not have a prompt",
                block.name
            )));
        }
        if !block.profiles.is_empty() {
            return Err(AppError::Config(format!(
                "Sub-pipeline block '{}' must not have profiles",
                block.name
            )));
        }
        if block.session_id.is_some() {
            return Err(AppError::Config(format!(
                "Sub-pipeline block '{}' must not have a session_id",
                block.name
            )));
        }

        // 13a: No nested sub-pipelines
        for inner in sub_def
            .blocks
            .iter()
            .chain(sub_def.finalization_blocks.iter())
        {
            if inner.is_sub_pipeline() {
                return Err(AppError::Config(format!(
                    "Nested sub-pipeline in block '{}' — only one level of nesting is allowed",
                    block.name
                )));
            }
        }

        // 13b: Must have finalization blocks
        if sub_def.finalization_blocks.is_empty() {
            return Err(AppError::Config(format!(
                "Sub-pipeline '{}' has no finalization blocks — sub-pipelines require exactly one finalization output",
                block.name
            )));
        }

        // 13c: Exactly one terminal finalization leaf
        let fin_outgoing: HashSet<BlockId> = sub_def
            .finalization_connections
            .iter()
            .map(|c| c.from)
            .collect();
        let terminal_leaves: Vec<&PipelineBlock> = sub_def
            .finalization_blocks
            .iter()
            .filter(|b| !fin_outgoing.contains(&b.id))
            .collect();
        if terminal_leaves.is_empty() {
            return Err(AppError::Config(format!(
                "Sub-pipeline '{}' has no finalization output — sub-pipelines require exactly one finalization output",
                block.name
            )));
        }
        if terminal_leaves.len() > 1 {
            return Err(AppError::Config(format!(
                "Sub-pipeline '{}' has {} finalization outputs — sub-pipelines require exactly one",
                block.name,
                terminal_leaves.len()
            )));
        }

        let terminal = terminal_leaves[0];

        // 13d: Terminal finalization leaf replicas must be 1
        if terminal.replicas != 1 {
            return Err(AppError::Config(format!(
                "Sub-pipeline '{}' terminal finalization block must have replicas = 1",
                block.name
            )));
        }

        // 13g: Terminal finalization leaf must have exactly one agent
        if terminal.agents.len() != 1 {
            return Err(AppError::Config(format!(
                "Sub-pipeline '{}' terminal finalization block must have exactly one agent",
                block.name
            )));
        }

        // 13f: Recursive validation of sub-pipeline definition
        validate_pipeline(sub_def)?;
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
    total_passes: u32,
    prompt: String,
    break_condition: String,
    break_agent: String,
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
    let run_dir = output.run_dir().display().to_string();
    let default_max_tokens = config.default_max_tokens;
    let max_history_messages = config.max_history_messages;
    let max_history_bytes = config.max_history_bytes;
    let factory: ProviderFactory = Arc::new(move |kind, cfg| {
        let mut dirs = vec![run_dir.clone()];
        let pdir = profiles_dir();
        if pdir.is_dir() {
            dirs.push(pdir.display().to_string());
        }
        provider::create_provider(
            kind,
            cfg,
            client.clone(),
            default_max_tokens,
            max_history_messages,
            max_history_bytes,
            cli_timeout_secs,
            dirs,
        )
    });
    run_pipeline_with_provider_factory(
        def,
        config.pipeline_block_concurrency,
        agent_configs,
        prompt_context,
        output,
        progress_tx,
        cancel,
        factory,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_pipeline_with_provider_factory(
    def: &PipelineDefinition,
    max_block_concurrency: u32,
    agent_configs: PipelineAgentConfigs,
    prompt_context: &PromptRuntimeContext,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
    provider_factory: ProviderFactory,
) -> Result<(), AppError> {
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

    // Build provider pool keyed by (agent, runtime_session_key).
    // Sub-pipeline entries have synthetic agent names like "[Sub#N]" which
    // won't match any config — they are harmlessly skipped here.
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

    let iteration: u32 = 1;

    {
        let mut current_in_degree = in_degree.clone();
        let mut failed_replicas: HashSet<u32> = HashSet::new();
        let mut failed_logical: HashSet<BlockId> = HashSet::new();
        let mut replica_outputs: HashMap<u32, String> = HashMap::new();
        let mut completed = 0usize;

        // Loop runtime state (clone from prepared sub_dags)
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
                                total_passes: lc.count + 1,
                                prompt: lc.prompt.clone(),
                                break_condition: lc.break_condition.clone(),
                                break_agent: lc.break_agent.clone(),
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
        let mut scatter_queues: HashMap<(BlockId, BlockId), ScatterQueue> = HashMap::new();
        let mut scatter_done_replicas: HashMap<BlockId, HashSet<u32>> = HashMap::new();

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
                    let replica_count = block.logical_task_count() as usize;

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

                    // Sub-pipeline dispatch
                    if block.is_sub_pipeline() {
                        let parent_iteration: u32 = iteration;
                        let parent_loop_pass: u32 = *block_loop_pass.get(&block_id).unwrap_or(&0);
                        let parent_block_name = block.name.clone();
                        let upstream_context = assemble_raw_upstream_context(
                            block, def, &rt, &replica_outputs,
                        );

                        for &rid in rids {
                            let info = &rt.entries[rid as usize];
                            let parent_label = info.display_label.clone();
                            let replica_index = info.replica_index;

                            // --- Scatter: pop item from queue ---
                            let incoming_scatter: Option<&PipelineConnection> = def
                                .connections
                                .iter()
                                .find(|c| c.scatter && c.to == block_id);

                            let scatter_item: Option<ScatterItem> =
                                if let Some(sc) = incoming_scatter {
                                    scatter_queues
                                        .get_mut(&(sc.from, block_id))
                                        .and_then(|q| q.pop())
                                } else {
                                    None
                                };

                            let scatter_total = incoming_scatter
                                .and_then(|sc| scatter_queues.get(&(sc.from, block_id)))
                                .map(|q| q.total())
                                .unwrap_or(0);

                            // Queue exhausted → no-op task
                            if incoming_scatter.is_some() && scatter_item.is_none() {
                                let _ = progress_tx.send(ProgressEvent::BlockFinished {
                                    block_id: rid,
                                    agent_name: info.agent.clone(),
                                    label: info.display_label.clone(),
                                    iteration,
                                    loop_pass: current_loop_pass,
                                });
                                scatter_done_replicas.entry(block_id).or_default().insert(rid);
                                let task_handle = tasks.spawn(async move { (rid, Ok(String::new())) });
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
                                continue;
                            }

                            // Scatter log
                            if let Some(ref item) = scatter_item {
                                let _ = progress_tx.send(ProgressEvent::BlockLog {
                                    block_id: rid,
                                    agent_name: info.agent.clone(),
                                    iteration,
                                    loop_pass: current_loop_pass,
                                    message: format!(
                                        "[scatter] Processing item {}/{}",
                                        item.index + 1,
                                        scatter_total,
                                    ),
                                });
                            }

                            // Clone sub_def and inject upstream + scatter into initial_prompt
                            let sub_def = block.sub_pipeline.as_ref().unwrap().clone();
                            let mut sub_def_ctx = sub_def;
                            let mut final_prompt = sub_def_ctx.initial_prompt.clone();
                            if !upstream_context.is_empty() {
                                if final_prompt.is_empty() {
                                    final_prompt = upstream_context.clone();
                                } else {
                                    final_prompt = format!(
                                        "{final_prompt}\n\n--- Input from parent pipeline ---\n{upstream_context}",
                                    );
                                }
                            }
                            if let Some(ref item) = scatter_item {
                                let scatter_header = format!(
                                    "--- Scatter item {}/{} ---\n{}",
                                    item.index + 1,
                                    scatter_total,
                                    item.content,
                                );
                                if final_prompt.is_empty() {
                                    final_prompt = scatter_header;
                                } else {
                                    final_prompt =
                                        format!("{final_prompt}\n\n{scatter_header}");
                                }
                            }
                            sub_def_ctx.initial_prompt = final_prompt;

                            // Per-replica sub-run directory
                            let sub_run_dir = if block.replicas > 1 {
                                output.run_dir().join(format!(
                                    "sub_{}_r{}",
                                    block.id,
                                    replica_index + 1
                                ))
                            } else {
                                output.run_dir().join(format!("sub_{}", block.id))
                            };
                            let parent_run_dir = output.run_dir().to_path_buf();
                            let parent_filename = loop_replica_filename(
                                &rt.entries[rid as usize],
                                parent_loop_pass,
                            );
                            // Wrap the parent factory so CLI providers inside the
                            // sub-pipeline get --add-dir for the sub-run directory.
                            let parent_factory = provider_factory.clone();
                            let sub_dir_str = sub_run_dir.display().to_string();
                            let sub_factory: ProviderFactory = Arc::new(move |kind, cfg| {
                                let mut p = parent_factory(kind, cfg);
                                p.add_allowed_dir(sub_dir_str.clone());
                                p
                            });
                            let agent_configs_clone = agent_configs.clone();
                            let prompt_context_clone = prompt_context.clone();
                            let cancel_clone = cancel.clone();
                            let sem_clone = concurrency_sem.clone();
                            let max_conc = max_block_concurrency;

                            // Private child progress channel + forwarder
                            let (child_tx, mut child_rx) =
                                mpsc::unbounded_channel::<ProgressEvent>();
                            let parent_tx = progress_tx.clone();
                            let fwd_block_name = parent_block_name.clone();
                            let fwd_parent_label = parent_label.clone();
                            tokio::spawn(async move {
                                while let Some(event) = child_rx.recv().await {
                                    match &event {
                                        ProgressEvent::BlockStarted {
                                            block_id: inner_bid,
                                            label: inner_label,
                                            loop_pass: inner_lp,
                                            ..
                                        } => {
                                            let _ =
                                                parent_tx.send(ProgressEvent::SubBlockStarted {
                                                    parent_block_id: rid,
                                                    inner_block_id: *inner_bid,
                                                    inner_label: inner_label.clone(),
                                                    parent_label: fwd_parent_label.clone(),
                                                    iteration: parent_iteration,
                                                    loop_pass: parent_loop_pass,
                                                    inner_loop_pass: *inner_lp,
                                                });
                                        }
                                        ProgressEvent::BlockFinished {
                                            block_id: inner_bid,
                                            label: inner_label,
                                            loop_pass: inner_lp,
                                            ..
                                        } => {
                                            let _ =
                                                parent_tx.send(ProgressEvent::SubBlockFinished {
                                                    parent_block_id: rid,
                                                    inner_block_id: *inner_bid,
                                                    inner_label: inner_label.clone(),
                                                    parent_label: fwd_parent_label.clone(),
                                                    iteration: parent_iteration,
                                                    loop_pass: parent_loop_pass,
                                                    inner_loop_pass: *inner_lp,
                                                });
                                        }
                                        ProgressEvent::BlockError {
                                            block_id: inner_bid,
                                            label: inner_label,
                                            loop_pass: inner_lp,
                                            error,
                                            details,
                                            ..
                                        } => {
                                            let _ =
                                                parent_tx.send(ProgressEvent::SubBlockError {
                                                    parent_block_id: rid,
                                                    inner_block_id: *inner_bid,
                                                    inner_label: inner_label.clone(),
                                                    parent_label: fwd_parent_label.clone(),
                                                    iteration: parent_iteration,
                                                    loop_pass: parent_loop_pass,
                                                    inner_loop_pass: *inner_lp,
                                                    error: error.clone(),
                                                    details: details.clone(),
                                                    is_skip: false,
                                                });
                                        }
                                        ProgressEvent::BlockSkipped {
                                            block_id: inner_bid,
                                            label: inner_label,
                                            loop_pass: inner_lp,
                                            reason,
                                            ..
                                        } => {
                                            let _ =
                                                parent_tx.send(ProgressEvent::SubBlockError {
                                                    parent_block_id: rid,
                                                    inner_block_id: *inner_bid,
                                                    inner_label: inner_label.clone(),
                                                    parent_label: fwd_parent_label.clone(),
                                                    iteration: parent_iteration,
                                                    loop_pass: parent_loop_pass,
                                                    inner_loop_pass: *inner_lp,
                                                    error: format!("SKIPPED: {reason}"),
                                                    details: None,
                                                    is_skip: true,
                                                });
                                        }
                                        ProgressEvent::BlockLog {
                                            agent_name: inner_agent,
                                            message,
                                            ..
                                        } => {
                                            let _ = parent_tx.send(ProgressEvent::BlockLog {
                                                block_id: rid,
                                                agent_name: format!("[{fwd_block_name}]"),
                                                iteration: parent_iteration,
                                                loop_pass: parent_loop_pass,
                                                message: format!("[{inner_agent}] {message}"),
                                            });
                                        }
                                        ProgressEvent::AllDone => { /* Do not forward */ }
                                        _ => { /* Discard other inner events */ }
                                    }
                                }
                            });

                            // BlockStarted for the parent sub-pipeline row
                            let _ = progress_tx.send(ProgressEvent::BlockStarted {
                                block_id: rid,
                                agent_name: format!("[{}]", block.name),
                                label: parent_label.clone(),
                                iteration,
                                loop_pass: current_loop_pass,
                            });

                            let rt_handle = tokio::runtime::Handle::current();
                            let task_handle = tasks.spawn(async move {
                                let _permit =
                                    sem_clone.acquire().await.expect("semaphore closed");

                                let result = tokio::task::spawn_blocking(move || {
                                    rt_handle.block_on(async {
                                        if let Err(e) = std::fs::create_dir_all(&sub_run_dir) {
                                            return (rid, Err(e.to_string()));
                                        }
                                        let sub_output =
                                            match OutputManager::from_existing(sub_run_dir.clone())
                                            {
                                                Ok(o) => o,
                                                Err(e) => return (rid, Err(e.to_string())),
                                            };

                                        // Execution phase
                                        if let Err(e) = run_pipeline_with_provider_factory(
                                            &sub_def_ctx,
                                            max_conc,
                                            agent_configs_clone.clone(),
                                            &prompt_context_clone,
                                            &sub_output,
                                            child_tx.clone(),
                                            cancel_clone.clone(),
                                            sub_factory.clone(),
                                        )
                                        .await
                                        {
                                            return (rid, Err(e.to_string()));
                                        }

                                        // Finalization phase
                                        let exec_rt = build_runtime_table(&sub_def_ctx);
                                        let fin_scope = FinalizationRunScope::SingleRun {
                                            run_id: 1,
                                            run_dir: sub_run_dir.clone(),
                                        };
                                        if let Err(e) = run_pipeline_finalization(
                                            &sub_def_ctx,
                                            fin_scope,
                                            &exec_rt,
                                            agent_configs_clone,
                                            &sub_run_dir,
                                            child_tx,
                                            cancel_clone,
                                            sub_factory,
                                        )
                                        .await
                                        {
                                            return (rid, Err(e.to_string()));
                                        }

                                        match read_sub_pipeline_terminal_output(
                                            &sub_def_ctx,
                                            &sub_run_dir,
                                        ) {
                                            Ok(text) => {
                                                let parent_path =
                                                    parent_run_dir.join(&parent_filename);
                                                if let Err(e) =
                                                    std::fs::write(&parent_path, &text)
                                                {
                                                    return (
                                                        rid,
                                                        Err(format!(
                                                        "Failed to write sub-pipeline output: {e}"
                                                    )),
                                                    );
                                                }
                                                (rid, Ok(text))
                                            }
                                            Err(e) => (rid, Err(e.to_string())),
                                        }
                                    })
                                })
                                .await
                                .unwrap_or_else(|e| {
                                    (rid, Err(format!("sub-pipeline panicked: {e}")))
                                });

                                result
                            });

                            task_metadata.insert(
                                task_handle.id(),
                                PipelineTaskMetadata {
                                    runtime_id: rid,
                                    source_block_id: block_id,
                                    agent_name: format!("[{}]", block.name),
                                    label: parent_label,
                                    iteration,
                                    loop_pass: current_loop_pass,
                                },
                            );
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

                        // --- Scatter: pop item from queue ---
                        let incoming_scatter: Option<&PipelineConnection> = def
                            .connections
                            .iter()
                            .find(|c| c.scatter && c.to == block_id);

                        let scatter_item: Option<ScatterItem> =
                            if let Some(sc) = incoming_scatter {
                                scatter_queues
                                    .get_mut(&(sc.from, block_id))
                                    .and_then(|q| q.pop())
                            } else {
                                None
                            };

                        let scatter_total = incoming_scatter
                            .and_then(|sc| scatter_queues.get(&(sc.from, block_id)))
                            .map(|q| q.total())
                            .unwrap_or(0);

                        // Queue exhausted → no-op task
                        if incoming_scatter.is_some() && scatter_item.is_none() {
                            let _ = progress_tx.send(ProgressEvent::BlockFinished {
                                block_id: rid,
                                agent_name: info.agent.clone(),
                                label: info.display_label.clone(),
                                iteration,
                                loop_pass: current_loop_pass,
                            });
                            scatter_done_replicas.entry(block_id).or_default().insert(rid);

                            let task_handle = tasks.spawn(async move { (rid, Ok(String::new())) });
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
                            continue;
                        }

                        // CLI scatter: materialize item to disk
                        let scatter_cli_path: Option<std::path::PathBuf> =
                            if let Some(ref item) = scatter_item {
                                if use_cli {
                                    let scatter_dir = output.run_dir().join("_scatter");
                                    let sc = incoming_scatter.unwrap();
                                    let temp_name = format!(
                                        "from{}_to{}_rid{}_item{}_loop{}.md",
                                        sc.from, block_id, rid, item.index, current_loop_pass
                                    );
                                    match std::fs::create_dir_all(&scatter_dir).and_then(|_| {
                                        let path = scatter_dir.join(&temp_name);
                                        std::fs::write(&path, &item.content)?;
                                        Ok(path)
                                    }) {
                                        Ok(path) => Some(path),
                                        Err(e) => {
                                            let error_msg =
                                                format!("Failed to write scatter item: {e}");
                                            let _ = output.append_error(&format!(
                                                "runtime {rid}: {error_msg}"
                                            ));
                                            let _ =
                                                progress_tx.send(ProgressEvent::BlockError {
                                                    block_id: rid,
                                                    agent_name: info.agent.clone(),
                                                    label: info.display_label.clone(),
                                                    iteration,
                                                    loop_pass: current_loop_pass,
                                                    error: error_msg.clone(),
                                                    details: Some(error_msg.clone()),
                                                });
                                            let err = error_msg;
                                            let task_handle =
                                                tasks.spawn(async move { (rid, Err(err)) });
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
                                            continue;
                                        }
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                        // Scatter log
                        if let Some(ref item) = scatter_item {
                            let _ = progress_tx.send(ProgressEvent::BlockLog {
                                block_id: rid,
                                agent_name: info.agent.clone(),
                                iteration,
                                loop_pass: current_loop_pass,
                                message: format!(
                                    "[scatter] Processing item {}/{}",
                                    item.index + 1,
                                    scatter_total
                                ),
                            });
                        }

                        // Build scatter injection text (inserted before augment_prompt_for_agent)
                        let scatter_injection: Option<String> = scatter_item.as_ref().map(|item| {
                            if use_cli {
                                let path = scatter_cli_path.as_ref().expect("CLI scatter path must exist");
                                format!(
                                    "[Scatter work item {} of {scatter_total}]\nRead this file: {}",
                                    item.index + 1,
                                    path.display()
                                )
                            } else {
                                format!(
                                    "[Scatter work item {} of {scatter_total}]\n{}",
                                    item.index + 1,
                                    item.content
                                )
                            }
                        });

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
                                        prompt_context, &block_to_loop, &block_loop_pass,
                                        scatter_injection.as_deref(),
                                    )
                                } else {
                                    build_pipeline_block_message(
                                        block, use_cli,
                                        &PipelineMessageContext {
                                            def,
                                            block_outputs: &replica_outputs,
                                            output, prompt_context, runtime_table: &rt,
                                            block_to_loop: &block_to_loop,
                                            block_loop_pass: &block_loop_pass,
                                            scatter_injection,
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
                                    def,
                                    block_outputs: &replica_outputs,
                                    output, prompt_context, runtime_table: &rt,
                                    block_to_loop: &block_to_loop,
                                    block_loop_pass: &block_loop_pass,
                                    scatter_injection,
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
                        let task_filename = if let Some(ref item) = scatter_item {
                            scatter_replica_filename(info, item.index, current_loop_pass)
                        } else {
                            loop_replica_filename(info, current_loop_pass)
                        };
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
                                            "runtime {rid} {task_agent_name}: {error}"
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
                                        "runtime {rid} {task_agent_name}: {error}"
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
                                    "runtime {} {}: {}",
                                    metadata.runtime_id,
                                    metadata.agent_name,
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
                        let entry_agent = rt.entries[runtime_id as usize].agent.clone();
                        let entry_label = rt.entries[runtime_id as usize].display_label.clone();
                        let entry_lp = block_loop_pass.get(&sid).copied().unwrap_or(0);
                        match outcome {
                            Ok(content) => {
                                let is_scatter_done = scatter_done_replicas
                                    .get(&sid)
                                    .is_some_and(|s| s.contains(&runtime_id));
                                if !is_scatter_done {
                                    replica_outputs.insert(runtime_id, content.clone());

                                    // Create scatter queues for outgoing scatter connections
                                    for conn in &def.connections {
                                        if conn.scatter && conn.from == sid {
                                            if let Some(rids) = rt.logical_to_runtime.get(&sid) {
                                                if let Some(text) = replica_outputs.get(&rids[0]) {
                                                    let delim = if conn.scatter_delimiter.is_empty() {
                                                        DEFAULT_SCATTER_DELIMITER
                                                    } else {
                                                        &conn.scatter_delimiter
                                                    };
                                                    let queue = ScatterQueue::from_output(text, delim);
                                                    let _ = progress_tx.send(ProgressEvent::BlockLog {
                                                        block_id: runtime_id,
                                                        agent_name: entry_agent.clone(),
                                                        iteration,
                                                        loop_pass: entry_lp,
                                                        message: format!(
                                                            "[scatter] Queue created: {} items",
                                                            queue.total()
                                                        ),
                                                    });
                                                    scatter_queues.insert((conn.from, conn.to), queue);
                                                }
                                            }
                                        }
                                    }
                                }
                                // Emit BlockFinished for sub-pipeline blocks (normal
                                // tasks emit this inside the spawned task; sub-pipelines
                                // emit it here because the inner run does not know the
                                // parent runtime_id). Skip for scatter-exhausted no-ops
                                // which already emitted BlockFinished in the dispatch loop.
                                if block_map.get(&sid).is_some_and(|b| b.is_sub_pipeline())
                                    && !is_scatter_done
                                {
                                    let _ = progress_tx.send(ProgressEvent::BlockFinished {
                                        block_id: runtime_id,
                                        agent_name: entry_agent,
                                        label: entry_label,
                                        iteration,
                                        loop_pass: entry_lp,
                                    });
                                }
                            }
                            Err(error) => {
                                failed_replicas.insert(runtime_id);
                                // Remove stale output so a previous pass's content
                                // is not mistaken for current-pass feedback.
                                replica_outputs.remove(&runtime_id);
                                if let Some(rids) = rt.logical_to_runtime.get(&sid) {
                                    if rids.iter().all(|r| failed_replicas.contains(r)) {
                                        failed_logical.insert(sid);
                                    }
                                }
                                // Emit BlockError for sub-pipeline blocks (same
                                // rationale as BlockFinished above).
                                if block_map.get(&sid).is_some_and(|b| b.is_sub_pipeline()) {
                                    let _ = progress_tx.send(ProgressEvent::BlockError {
                                        block_id: runtime_id,
                                        agent_name: entry_agent,
                                        label: entry_label,
                                        iteration,
                                        loop_pass: entry_lp,
                                        error: error.clone(),
                                        details: Some(error.clone()),
                                    });
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
                                } else {
                                    // --- Early-break evaluation ---
                                    let should_break = if let Some(ls) = loop_state.get(&key) {
                                        if ls.remaining > 0
                                            && !ls.break_agent.is_empty()
                                            && !ls.break_condition.is_empty()
                                        {
                                            let agent = ls.break_agent.clone();
                                            let condition = ls.break_condition.clone();
                                            let pass = ls.current_pass;
                                            let blocks = ls.sub_dag.blocks.clone();
                                            let total = ls.total_passes;

                                            let result = evaluate_loop_break(
                                                &agent,
                                                &condition,
                                                pass,
                                                total,
                                                &blocks,
                                                &rt,
                                                output,
                                                &agent_configs,
                                                &provider_factory,
                                                &cancel,
                                            )
                                            .await;

                                            let decision =
                                                if result { "BREAK" } else { "CONTINUE" };
                                            let _ =
                                                progress_tx.send(ProgressEvent::LoopBreakEval {
                                                    from: loop_from,
                                                    to: loop_to,
                                                    iteration,
                                                    pass: pass + 1,
                                                    agent_name: agent,
                                                    decision: decision.into(),
                                                });
                                            result
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    };

                                    if should_break {
                                        if let Some(ls) = loop_state.get_mut(&key) {
                                            if !ls.abandoned {
                                                // Account for the passes that will never run,
                                                // mirroring the failure-abandon path.
                                                completed += ls.abandoned_task_count();
                                                ls.extra_tasks_remaining = 0;
                                                ls.remaining = 0;
                                                ls.abandoned = true;
                                            }
                                        }
                                    }

                                    // Scatter queue exhaustion → force loop termination
                                    let scatter_queue_empty = def.connections.iter()
                                        .find(|c| c.scatter && c.to == loop_to)
                                        .and_then(|sc| scatter_queues.get(&(sc.from, loop_to)))
                                        .map(|q| q.is_empty())
                                        .unwrap_or(false);

                                    if scatter_queue_empty && !should_break {
                                        if let Some(ls) = loop_state.get_mut(&key) {
                                            if !ls.abandoned {
                                                completed += ls.abandoned_task_count();
                                                ls.extra_tasks_remaining = 0;
                                                ls.remaining = 0;
                                                ls.abandoned = true;
                                            }
                                        }
                                    }

                                    if let Some(ls) = loop_state.get_mut(&key) {
                                        if ls.remaining > 0 {
                                            // More loop passes — reset sub-DAG and queue `to`
                                            ls.remaining -= 1;
                                            ls.current_pass += 1;
                                            ls.block_completed_this_pass.clear();
                                            scatter_done_replicas.remove(&loop_to);
                                            // Clear stale failure/output state for sub-DAG blocks
                                            // so that failures from pass N don't poison pass N+1.
                                            // Keep `from`'s outputs — needed as feedback for `to`.
                                            for &bid in &ls.sub_dag.blocks {
                                                failed_logical.remove(&bid);
                                                if let Some(rids) =
                                                    rt.logical_to_runtime.get(&bid)
                                                {
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
                                            let all_deferred: Vec<(BlockId, usize)> = ls
                                                .sub_dag
                                                .deferred_external_edges
                                                .values()
                                                .flatten()
                                                .copied()
                                                .collect();
                                            for (child, weight) in all_deferred {
                                                if let Some(deg) =
                                                    current_in_degree.get_mut(&child)
                                                {
                                                    *deg = deg.saturating_sub(weight);
                                                    if *deg == 0 {
                                                        let _ = ready_tx.send(child);
                                                    }
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

/// Read the terminal finalization leaf's output from a sub-pipeline run directory.
/// Returns the content of the single terminal block's output file.
fn read_sub_pipeline_terminal_output(
    def: &PipelineDefinition,
    output_root: &Path,
) -> Result<String, AppError> {
    // Find terminal finalization leaf (validated: exactly one)
    let outgoing: HashSet<BlockId> = def
        .finalization_connections
        .iter()
        .map(|c| c.from)
        .collect();
    let terminal_block = def
        .finalization_blocks
        .iter()
        .find(|b| !outgoing.contains(&b.id))
        .expect("validated: exactly one finalization leaf");

    // Derive filename from the same code path that writes it
    let exec_rt = build_runtime_table(def);
    let fin_scope = FinalizationRunScope::SingleRun {
        run_id: 1,
        run_dir: output_root.to_path_buf(),
    };
    let fin_entries =
        build_finalization_runtime_entries(def, &fin_scope, exec_rt.entries.len() as u32);
    let terminal_entry = fin_entries
        .iter()
        .find(|e| e.source_block_id == terminal_block.id)
        .expect("validated: terminal block has entry");

    let path = output_root
        .join("finalization")
        .join(format!("{}.md", terminal_entry.filename_stem));

    std::fs::read_to_string(&path).map_err(AppError::Io)
}

/// Assemble upstream context without augmentation (no cwd prefix, memory, or diagnostics).
/// Used for sub-pipeline dispatch to avoid double-augmentation.
fn assemble_raw_upstream_context(
    block: &PipelineBlock,
    def: &PipelineDefinition,
    runtime_table: &RuntimeReplicaTable,
    block_outputs: &HashMap<u32, String>,
) -> String {
    let upstream_ids = upstream_of_non_scatter(def, block.id);
    if upstream_ids.is_empty() {
        return def.initial_prompt.clone();
    }
    let mut upstream_content = String::new();
    for uid in &upstream_ids {
        if let Some(rids) = runtime_table.logical_to_runtime.get(uid) {
            for &rid in rids {
                if let Some(content) = block_outputs.get(&rid) {
                    if !upstream_content.is_empty() {
                        upstream_content.push_str("\n\n---\n\n");
                    }
                    let upstream_block = def.blocks.iter().find(|b| b.id == *uid);
                    let needs_label = upstream_block
                        .map(|b| b.logical_task_count() > 1)
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
    if upstream_content.is_empty() {
        String::new()
    } else {
        format!("--- Upstream outputs ---\n{upstream_content}")
    }
}

fn build_pipeline_block_message(
    block: &PipelineBlock,
    use_cli: bool,
    context: &PipelineMessageContext<'_>,
) -> String {
    let is_root = upstream_of_non_scatter(context.def, block.id).is_empty();
    let base_message = if is_root {
        if block.prompt.is_empty() {
            context.def.initial_prompt.clone()
        } else {
            format!("{}\n\n{}", block.prompt, context.def.initial_prompt)
        }
    } else {
        let upstream_ids = upstream_of_non_scatter(context.def, block.id);
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
                        let filenames = cli_upstream_filenames(
                            info,
                            *uid,
                            context.def,
                            context.block_to_loop,
                            context.block_loop_pass,
                            context.output.run_dir(),
                        );
                        for filename in filenames {
                            let path = context.output.run_dir().join(&filename);
                            if path.exists() {
                                file_refs.push_str(&format!("- {}\n", path.display()));
                            }
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
                                .map(|b| b.logical_task_count() > 1)
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
    let with_profiles = if profile_prefix.is_empty() {
        base_message
    } else {
        format!("{profile_prefix}{base_message}")
    };
    let full_message = if let Some(ref scatter_text) = context.scatter_injection {
        format!("{with_profiles}\n\n{scatter_text}")
    } else {
        with_profiles
    };
    context
        .prompt_context
        .augment_prompt_for_agent(&full_message, use_cli)
}

/// Loop-aware filename resolver for CLI upstream references.
fn loop_aware_upstream_filename(
    info: &RuntimeReplicaInfo,
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
            replica_filename(info)
        } else {
            loop_replica_filename(info, pass)
        }
    } else {
        replica_filename(info)
    }
}

/// Scatter-aware CLI filename resolver.
/// For scatter-target blocks, enumerates `_item{N}` files on disk because the
/// standard filename (`stem.md` / `stem_loop{P}.md`) does not exist — scatter
/// replicas write per-item files instead.
fn cli_upstream_filenames(
    info: &RuntimeReplicaInfo,
    upstream_block_id: BlockId,
    def: &PipelineDefinition,
    block_to_loop: &HashMap<BlockId, (BlockId, BlockId)>,
    block_loop_pass: &HashMap<BlockId, u32>,
    run_dir: &std::path::Path,
) -> Vec<String> {
    let is_scatter_target = def
        .connections
        .iter()
        .any(|c| c.scatter && c.to == upstream_block_id);

    if !is_scatter_target {
        return vec![loop_aware_upstream_filename(
            info,
            upstream_block_id,
            block_to_loop,
            block_loop_pass,
        )];
    }

    // Scatter target: glob for _item files matching this replica's stem.
    // Uses exact prefix match (stem + "_item") to avoid collisions between
    // replica suffixes like _r1 and _r10.
    let stem = &info.filename_stem;
    let item_prefix = format!("{stem}_item");
    let pass = block_loop_pass
        .get(&upstream_block_id)
        .copied()
        .unwrap_or(0);
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(run_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with(&item_prefix) || !name.ends_with(".md") {
                continue;
            }
            if pass > 0 {
                // On loop pass P, match only _item{N}_loop{P}.md
                if name.ends_with(&format!("_loop{pass}.md")) {
                    files.push(name);
                }
            } else {
                // Pass 0: match _item{N}.md (no _loop suffix).
                // Use suffix-aware check: reject only if the part before .md
                // ends with _loop{digits}, avoiding false negatives when the
                // block name itself contains "_loop" (e.g. "fix_loop").
                let without_ext = name.trim_end_matches(".md");
                let has_loop_suffix = without_ext.rfind("_loop").is_some_and(|i| {
                    let after = &without_ext[i + 5..];
                    !after.is_empty() && after.chars().all(|c| c.is_ascii_digit())
                });
                if !has_loop_suffix {
                    files.push(name);
                }
            }
        }
    }
    // Sort by item index numerically (item0, item1, ..., item10) not lexicographically
    files.sort_by(|a, b| {
        let idx = |s: &str| -> usize {
            s.rfind("_item")
                .and_then(|i| {
                    s[i + 5..]
                        .split(|c: char| !c.is_ascii_digit())
                        .next()
                        .and_then(|n| n.parse().ok())
                })
                .unwrap_or(0)
        };
        idx(a).cmp(&idx(b))
    });
    if files.is_empty() {
        // Fallback: no _item files found yet — use standard filename
        vec![loop_aware_upstream_filename(
            info,
            upstream_block_id,
            block_to_loop,
            block_loop_pass,
        )]
    } else {
        files
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
    prompt_context: &PromptRuntimeContext,
    block_to_loop: &HashMap<BlockId, (BlockId, BlockId)>,
    block_loop_pass: &HashMap<BlockId, u32>,
    scatter_injection: Option<&str>,
) -> String {
    let mut message = String::new();

    // External upstream context (parents outside the loop sub-DAG)
    let external_parents: Vec<BlockId> = upstream_of_non_scatter(def, block.id)
        .into_iter()
        .filter(|uid| !loop_sub_dag_blocks.contains(uid))
        .collect();

    let is_root = upstream_of_non_scatter(def, block.id).is_empty();
    if is_root {
        // Root restart target: include initial_prompt
        if !def.initial_prompt.is_empty() {
            message.push_str(&def.initial_prompt);
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
                        let filenames = cli_upstream_filenames(
                            info,
                            uid,
                            def,
                            block_to_loop,
                            block_loop_pass,
                            output.run_dir(),
                        );
                        for filename in filenames {
                            let path = output.run_dir().join(&filename);
                            if path.exists() {
                                file_refs.push_str(&format!("- {}\n", path.display()));
                            }
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
                                .map(|b| b.logical_task_count() > 1)
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

    // Loop pass header
    message.push_str(&format!(
        "[Loop pass {} of {}]\n\n",
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
                    let filename = loop_replica_filename(info, feedback_pass);
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
    let with_profiles = if profile_prefix.is_empty() {
        message
    } else {
        format!("{profile_prefix}{message}")
    };
    let full_message = if let Some(scatter_text) = scatter_injection {
        format!("{with_profiles}\n\n{scatter_text}")
    } else {
        with_profiles
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
        .map(|b| b.logical_task_count() as usize)
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
    let budget = MAX_FEED_ASSEMBLY_BYTES;

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
            let matches_stem = filename_stems.iter().any(|stem| {
                if !name.starts_with(stem) {
                    return false;
                }
                let remainder = &name[stem.len()..];
                remainder == ".md"
                    || (remainder.starts_with("_loop") && remainder.ends_with(".md"))
                    || (remainder.starts_with("_iter") && remainder.ends_with(".md"))
                    || (remainder.starts_with("_item") && remainder.ends_with(".md"))
            });
            if !matches_stem {
                continue;
            }
            if !post_run::is_pipeline_output_filename(name) {
                continue;
            }
            matched_files.push((name.to_string(), path));
        }

        // Step 5: Apply collection filter
        match feed.collection {
            FeedCollection::LastPass => {
                // Keep only the highest loop pass per filename stem
                matched_files = post_run::keep_highest_loop_pass(matched_files);
            }
            FeedCollection::AllPasses => {
                // Keep all loop pass files — no deduplication
            }
        }

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
        let budget = MAX_FEED_ASSEMBLY_BYTES;
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
pub(crate) async fn run_pipeline_finalization(
    def: &PipelineDefinition,
    run_scope: FinalizationRunScope,
    exec_runtime_table: &RuntimeReplicaTable,
    agent_configs: PipelineAgentConfigs,
    output_root: &Path,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
    provider_factory: ProviderFactory,
) -> Result<(), AppError> {
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
                        .map(|b| b.logical_task_count() as usize)
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
                    let replica_count = block.logical_task_count() as usize;
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
                let replica_count = block.logical_task_count() as usize;
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
                .map(|b| b.logical_task_count() as usize)
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
                let replica_count = block.logical_task_count() as usize;
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
            let replica_count = block.logical_task_count() as usize;
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
            sub_pipeline: None,
        }
    }

    fn conn(from: BlockId, to: BlockId) -> PipelineConnection {
        PipelineConnection::new(from, to)
    }

    fn def_with(
        blocks: Vec<PipelineBlock>,
        connections: Vec<PipelineConnection>,
    ) -> PipelineDefinition {
        PipelineDefinition {
            initial_prompt: "test".into(),

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
    }

    // -- load_pipeline validation --

    #[test]
    fn load_rejects_duplicate_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.toml");
        let content = r#"
initial_prompt = "test"

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

            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agents: vec!["Claude".into()],
                prompt: "block prompt".into(),
                profiles: vec![],
                session_id: None,
                position: (0, 0),
                replicas: 1,
                sub_pipeline: None,
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
            block_outputs: &block_outputs,
            output: &output,
            prompt_context: &context,
            runtime_table: &rt,
            block_to_loop: &btl,
            block_loop_pass: &blp,
            scatter_injection: None,
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

            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agents: vec!["Claude".into()],
                prompt: String::new(),
                profiles: vec![],
                session_id: None,
                position: (0, 0),
                replicas: 1,
                sub_pipeline: None,
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
            Arc::new(move |_kind, _cfg| {
                Box::new(PanicProvider::new(
                    ProviderKind::Anthropic,
                    "pipeline panic",
                ))
            }),
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
        assert!(log.contains("runtime 0 Claude:"));
        assert!(log.contains("pipeline panic"));
    }

    #[tokio::test]
    async fn run_pipeline_provider_error_appends_error_log() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("pipeline-provider-error")).unwrap();
        let def = PipelineDefinition {
            initial_prompt: "base prompt".into(),

            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agents: vec!["Claude".into()],
                prompt: String::new(),
                profiles: vec![],
                session_id: None,
                position: (0, 0),
                replicas: 1,
                sub_pipeline: None,
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
            Arc::new(move |_kind, _cfg| {
                Box::new(MockProvider::err(
                    ProviderKind::Anthropic,
                    "provider failed",
                    received.clone(),
                ))
            }),
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
        assert!(log.contains("runtime 0 Claude:"));
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
            Arc::new(move |_kind, _cfg| -> Box<dyn Provider> {
                Box::new(ConcurrencyTracker {
                    kind: ProviderKind::Anthropic,
                    active: active_clone.clone(),
                    peak: peak_clone.clone(),
                })
            }),
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
            sub_pipeline: None,
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
                    sub_pipeline: None,
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
                    sub_pipeline: None,
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
                    sub_pipeline: None,
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
                    sub_pipeline: None,
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
                    sub_pipeline: None,
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
                    sub_pipeline: None,
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
                    sub_pipeline: None,
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
                    sub_pipeline: None,
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

    // -- normalize_session_configs --

    #[test]
    fn normalize_drops_stale_rows() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.session_configs.push(SessionConfig {
            agent: "Claude".into(),
            session_key: "nonexistent".into(),
            keep_across_loop_passes: false,
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
            keep_across_loop_passes: false,
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
        assert!(def.session_configs[0].keep_across_loop_passes);
    }

    #[test]
    fn old_toml_keep_across_iterations_ignored_gracefully() {
        // Backward compat: old TOML files may contain keep_across_iterations.
        // Serde ignores unknown fields, so load_pipeline should succeed.
        // Use keep_across_loop_passes = false so normalize doesn't prune the row.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compat.toml");
        std::fs::write(
            &path,
            r#"
initial_prompt = "test"

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
keep_across_loop_passes = false
"#,
        )
        .unwrap();
        let def = load_pipeline(&path).unwrap();
        assert_eq!(def.session_configs.len(), 1);
        // keep_across_iterations is unknown and silently ignored;
        // keep_across_loop_passes = false survives normalization
        assert!(!def.session_configs[0].keep_across_loop_passes);
    }

    #[test]
    fn save_load_roundtrip_preserves_false_session_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_session.toml");
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.set_keep_session_across_loop_passes("Claude", "__block_1", false);
        save_pipeline(&def, &path).unwrap();
        let loaded = load_pipeline(&path).unwrap();
        assert!(!loaded.keep_session_across_loop_passes("Claude", "__block_1"));
    }

    #[test]
    fn load_deduplicates_session_configs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.toml");
        let toml_str = r#"
initial_prompt = "test"

[[blocks]]
id = 1
name = "B"
agent = "Claude"
prompt = ""
position = [0, 0]

[[session_configs]]
agent = "Claude"
session_key = "__block_1"
keep_across_loop_passes = false

[[session_configs]]
agent = "Claude"
session_key = "__block_1"
keep_across_loop_passes = false
"#;
        std::fs::write(&path, toml_str).unwrap();
        // Normalization deduplicates before validation
        let loaded = load_pipeline(&path).unwrap();
        assert_eq!(loaded.session_configs.len(), 1);
        assert!(!loaded.keep_session_across_loop_passes("Claude", "__block_1"));
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
            Arc::new(move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            }),
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
            Arc::new(move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            }),
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
            break_condition: String::new(),
            break_agent: String::new(),
        }
    }

    fn def_with_loops(
        blocks: Vec<PipelineBlock>,
        connections: Vec<PipelineConnection>,
        loops: Vec<LoopConnection>,
    ) -> PipelineDefinition {
        PipelineDefinition {
            initial_prompt: "test".into(),

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
                break_condition: String::new(),
                break_agent: String::new(),
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
                break_condition: String::new(),
                break_agent: String::new(),
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
                break_condition: String::new(),
                break_agent: String::new(),
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

    #[test]
    fn test_loop_serde_roundtrip_with_break_fields() {
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![],
            vec![LoopConnection {
                from: 1,
                to: 2,
                count: 2,
                prompt: "iterate".into(),
                break_condition: "Stop when stable".into(),
                break_agent: "Claude".into(),
            }],
        );
        let toml_str = toml::to_string(&def).expect("serialize");
        assert!(toml_str.contains("break_condition"));
        assert!(toml_str.contains("break_agent"));
        let loaded: PipelineDefinition = toml::from_str(&toml_str).expect("deserialize");
        assert_eq!(loaded.loop_connections.len(), 1);
        let lc = &loaded.loop_connections[0];
        assert_eq!(lc.break_condition, "Stop when stable");
        assert_eq!(lc.break_agent, "Claude");
    }

    #[test]
    fn test_validate_rejects_break_agent_without_condition() {
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![LoopConnection {
                from: 2,
                to: 1,
                count: 1,
                prompt: String::new(),
                break_condition: String::new(),
                break_agent: "Claude".into(),
            }],
        );
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            format!("{err}").contains("both be set or both empty"),
            "expected both-or-neither error, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_break_condition_without_agent() {
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
            vec![LoopConnection {
                from: 2,
                to: 1,
                count: 1,
                prompt: String::new(),
                break_condition: "Stop when done".into(),
                break_agent: String::new(),
            }],
        );
        let err = validate_pipeline(&def).unwrap_err();
        assert!(
            format!("{err}").contains("both be set or both empty"),
            "expected both-or-neither error, got: {err}"
        );
    }

    // -- break evaluator response parsing tests --
    // Tests exercise the shared `parse_break_decision` function directly.

    #[test]
    fn test_break_parsing_exact() {
        assert!(parse_break_decision("BREAK"));
        assert!(parse_break_decision("break"));
        assert!(parse_break_decision("Break"));
    }

    #[test]
    fn test_break_parsing_with_punctuation() {
        assert!(parse_break_decision("BREAK."));
        assert!(parse_break_decision("BREAK:"));
        assert!(parse_break_decision("BREAK!"));
        assert!(parse_break_decision("BREAK,"));
    }

    #[test]
    fn test_break_parsing_with_trailing_text() {
        assert!(parse_break_decision("BREAK the loop has converged"));
        assert!(parse_break_decision("BREAK\n\nThe output is stable."));
    }

    #[test]
    fn test_break_parsing_continue() {
        assert!(!parse_break_decision("CONTINUE"));
        assert!(!parse_break_decision("continue"));
        assert!(!parse_break_decision("CONTINUE."));
    }

    #[test]
    fn test_break_parsing_edge_cases() {
        assert!(!parse_break_decision(""));
        assert!(!parse_break_decision("   "));
        assert!(!parse_break_decision("BREAKING NEWS"));
        assert!(!parse_break_decision("Do not break yet"));
    }

    #[test]
    fn test_break_parsing_whitespace() {
        assert!(parse_break_decision("  BREAK  "));
        assert!(parse_break_decision("\n\nBREAK\n\n"));
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
            Arc::new(move |_kind, _cfg| {
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "loop output",
                    recv_clone.clone(),
                ))
            }),
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
            Arc::new(move |_kind, _cfg| {
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "output",
                    recv_clone.clone(),
                ))
            }),
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
            Arc::new(move |_kind, _cfg| {
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
            }),
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
            Arc::new(move |_kind, _cfg| {
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
            }),
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
        let pass0 = loop_replica_filename(&info, 0);
        let normal = replica_filename(&info);
        assert_eq!(pass0, normal, "pass 0 should match replica_filename");
        assert_eq!(pass0, "block1_claude.md");

        // pass=1 should include _loop1
        let pass1 = loop_replica_filename(&info, 1);
        assert_eq!(pass1, "block1_claude_loop1.md");

        // pass=2
        let pass2 = loop_replica_filename(&info, 2);
        assert_eq!(pass2, "block1_claude_loop2.md");
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
            Arc::new(move |_kind, _cfg| {
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "intermediate output",
                    recv_clone.clone(),
                ))
            }),
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
            Arc::new(move |_kind, _cfg| {
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "output",
                    recv_clone.clone(),
                ))
            }),
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
            Arc::new(move |_kind, _cfg| {
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
            }),
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
            Arc::new(move |_kind, _cfg| {
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
            }),
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
        let max = super::MAX_FEED_ASSEMBLY_BYTES;
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
            sub_pipeline: None,
        }
    }

    fn feed(from: BlockId, to: BlockId) -> DataFeed {
        DataFeed {
            from,
            to,
            collection: FeedCollection::LastPass,
            granularity: FeedGranularity::PerRun,
        }
    }

    fn feed_all_runs(from: BlockId, to: BlockId) -> DataFeed {
        DataFeed {
            from,
            to,
            collection: FeedCollection::LastPass,
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
            break_condition: String::new(),
            break_agent: String::new(),
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

    #[test]
    fn all_passes_skips_loop_dedup() {
        // Create a run directory with initial + loop pass files
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().join("run1");
        std::fs::create_dir_all(&run_dir).unwrap();
        // Build runtime table to discover the filename stem
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        let rt = build_runtime_table(&def);
        let stem = &rt.entries[0].filename_stem;
        // Initial pass (pass 0)
        std::fs::write(run_dir.join(format!("{stem}.md")), "initial output").unwrap();
        // Loop passes
        std::fs::write(run_dir.join(format!("{stem}_loop1.md")), "loop pass 1").unwrap();
        std::fs::write(run_dir.join(format!("{stem}_loop2.md")), "loop pass 2").unwrap();

        let scope = FinalizationRunScope::SingleRun {
            run_id: 1,
            run_dir: run_dir.clone(),
        };

        // AllPasses: all 3 files should be included
        let feed_all = DataFeed {
            from: 1,
            to: 10,
            collection: FeedCollection::AllPasses,
            granularity: FeedGranularity::PerRun,
        };
        let result = collect_feed_data(&feed_all, &def, &rt, &scope, Some(1)).unwrap();
        assert!(
            result.contains("initial output"),
            "AllPasses should include pass-0 output"
        );
        assert!(
            result.contains("loop pass 1"),
            "AllPasses should include loop pass 1"
        );
        assert!(
            result.contains("loop pass 2"),
            "AllPasses should include loop pass 2"
        );

        // LastPass: only the highest loop pass (loop2) should remain
        let feed_last = DataFeed {
            from: 1,
            to: 10,
            collection: FeedCollection::LastPass,
            granularity: FeedGranularity::PerRun,
        };
        let result_last = collect_feed_data(&feed_last, &def, &rt, &scope, Some(1)).unwrap();
        assert!(
            result_last.contains("loop pass 2"),
            "LastPass should include highest loop pass"
        );
        assert!(
            !result_last.contains("loop pass 1"),
            "LastPass should exclude lower loop passes"
        );
        assert!(
            !result_last.contains("initial output"),
            "LastPass should exclude pass-0 when loop variants exist"
        );
    }

    #[test]
    fn collect_feed_data_old_format_backward_compat() {
        // Old-format filenames ({stem}_iter1.md, {stem}_iter1_loop1.md) must still be collected
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().join("run1");
        std::fs::create_dir_all(&run_dir).unwrap();
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks = vec![fin_block(10, 0, 0)];
        let rt = build_runtime_table(&def);
        let stem = &rt.entries[0].filename_stem;
        // Write old-format files
        std::fs::write(run_dir.join(format!("{stem}_iter1.md")), "old output").unwrap();
        std::fs::write(run_dir.join(format!("{stem}_iter1_loop1.md")), "old loop 1").unwrap();

        let scope = FinalizationRunScope::SingleRun {
            run_id: 1,
            run_dir: run_dir.clone(),
        };
        let feed = DataFeed {
            from: 1,
            to: 10,
            collection: FeedCollection::AllPasses,
            granularity: FeedGranularity::PerRun,
        };
        let result = collect_feed_data(&feed, &def, &rt, &scope, Some(1)).unwrap();
        assert!(
            result.contains("old output"),
            "should collect old-format base file"
        );
        assert!(
            result.contains("old loop 1"),
            "should collect old-format loop file"
        );
    }

    #[tokio::test]
    async fn test_early_break_completes_and_emits_all_done() {
        // A(1)→B(2)→C(3) with loop_back(B, A, count=5) and break_agent="Claude".
        // The mock provider always returns "BREAK", so the evaluator breaks
        // after pass 0.  Downstream block C must still execute, and AllDone
        // must be emitted (proving completed reached total_tasks).
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("early-break")).unwrap();
        let def = def_with_loops(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
            vec![LoopConnection {
                from: 2,
                to: 1,
                count: 5,
                prompt: String::new(),
                break_condition: "always break".into(),
                break_agent: "Claude".into(),
            }],
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
            Arc::new(move |_kind, _cfg| {
                // All providers return "BREAK" — the evaluator will parse it
                // as a break decision, and the block agents produce it as content.
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "BREAK",
                    recv_clone.clone(),
                ))
            }),
        )
        .await
        .expect("pipeline should complete without error");

        let events = collect_progress_events(rx);

        // AllDone must be present — this is the regression guard against
        // the hang where completed never reached total_tasks.
        assert!(
            events.iter().any(|e| matches!(e, ProgressEvent::AllDone)),
            "AllDone event must be emitted after early break"
        );

        // A and B should each finish exactly once (pass 0 only).
        let a_finished = events
            .iter()
            .filter(
                |e| matches!(e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 0),
            )
            .count();
        let b_finished = events
            .iter()
            .filter(
                |e| matches!(e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 1),
            )
            .count();
        assert_eq!(a_finished, 1, "A should finish once (only pass 0)");
        assert_eq!(b_finished, 1, "B should finish once (only pass 0)");

        // Downstream C (outside the loop) must also finish.
        let c_finished = events
            .iter()
            .filter(
                |e| matches!(e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 2),
            )
            .count();
        assert_eq!(c_finished, 1, "downstream C should finish after loop exit");

        // A LoopBreakEval event should have been emitted with decision BREAK.
        let break_eval = events.iter().find(|e| {
            matches!(
                e,
                ProgressEvent::LoopBreakEval { decision, .. } if decision == "BREAK"
            )
        });
        assert!(
            break_eval.is_some(),
            "LoopBreakEval with BREAK decision must be emitted"
        );
    }

    // =========================================================================
    // Sub-pipeline tests
    // =========================================================================

    fn sub_pipeline_block(
        id: BlockId,
        col: u16,
        row: u16,
        sub_def: PipelineDefinition,
    ) -> PipelineBlock {
        PipelineBlock {
            id,
            name: format!("Sub#{id}"),
            agents: vec![],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (col, row),
            replicas: 1,
            sub_pipeline: Some(sub_def),
        }
    }

    fn minimal_sub_def() -> PipelineDefinition {
        PipelineDefinition {
            initial_prompt: "inner".into(),
            blocks: vec![block(100, 0, 0)],
            connections: vec![],
            session_configs: vec![],
            loop_connections: vec![],
            finalization_blocks: vec![PipelineBlock {
                id: 200,
                name: "Consolidate".into(),
                agents: vec!["Claude".into()],
                prompt: "summarize".into(),
                profiles: vec![],
                session_id: None,
                position: (0, 1),
                replicas: 1,
                sub_pipeline: None,
            }],
            finalization_connections: vec![],
            data_feeds: vec![DataFeed {
                from: WILDCARD_BLOCK_ID,
                to: 200,
                collection: FeedCollection::LastPass,
                granularity: FeedGranularity::PerRun,
            }],
        }
    }

    #[test]
    fn test_is_sub_pipeline_true_false() {
        let regular = block(1, 0, 0);
        assert!(!regular.is_sub_pipeline());

        let sub = sub_pipeline_block(2, 1, 0, minimal_sub_def());
        assert!(sub.is_sub_pipeline());
    }

    #[test]
    fn test_logical_task_count_regular_block() {
        let mut b = block(1, 0, 0);
        assert_eq!(b.logical_task_count(), 1);

        b.replicas = 3;
        assert_eq!(b.logical_task_count(), 3);

        b.agents = vec!["A".into(), "B".into()];
        b.replicas = 2;
        assert_eq!(b.logical_task_count(), 4);
    }

    #[test]
    fn test_logical_task_count_sub_pipeline_block() {
        let sub = sub_pipeline_block(1, 0, 0, minimal_sub_def());
        assert_eq!(sub.logical_task_count(), 1);
    }

    #[test]
    fn test_sub_pipeline_block_serde_roundtrip() {
        let sub_def = minimal_sub_def();
        let parent = PipelineDefinition {
            initial_prompt: "top".into(),
            blocks: vec![block(1, 0, 0), sub_pipeline_block(2, 1, 0, sub_def)],
            connections: vec![conn(1, 2)],
            session_configs: vec![],
            loop_connections: vec![],
            finalization_blocks: vec![],
            finalization_connections: vec![],
            data_feeds: vec![],
        };

        let toml_str = toml::to_string_pretty(&parent).unwrap();
        let parsed: PipelineDefinition = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.blocks.len(), 2);
        assert!(parsed.blocks[1].is_sub_pipeline());
        let inner = parsed.blocks[1].sub_pipeline.as_ref().unwrap();
        assert_eq!(inner.blocks.len(), 1);
        assert_eq!(inner.finalization_blocks.len(), 1);
    }

    #[test]
    fn test_validate_no_nested_sub_pipelines() {
        let mut inner_def = minimal_sub_def();
        // Add a sub-pipeline block INSIDE the sub-pipeline
        inner_def
            .blocks
            .push(sub_pipeline_block(101, 1, 0, minimal_sub_def()));

        let def = PipelineDefinition {
            initial_prompt: "top".into(),
            blocks: vec![sub_pipeline_block(1, 0, 0, inner_def)],
            connections: vec![],
            session_configs: vec![],
            loop_connections: vec![],
            finalization_blocks: vec![],
            finalization_connections: vec![],
            data_feeds: vec![],
        };
        let err = validate_pipeline(&def).unwrap_err();
        assert!(err.to_string().contains("Nested sub-pipeline"));
    }

    #[test]
    fn test_validate_sub_pipeline_must_have_finalization() {
        let bad_sub = PipelineDefinition {
            initial_prompt: "inner".into(),
            blocks: vec![block(100, 0, 0)],
            connections: vec![],
            session_configs: vec![],
            loop_connections: vec![],
            finalization_blocks: vec![],
            finalization_connections: vec![],
            data_feeds: vec![],
        };
        let def = def_with(vec![sub_pipeline_block(1, 0, 0, bad_sub)], vec![]);
        let err = validate_pipeline(&def).unwrap_err();
        assert!(err.to_string().contains("no finalization"));
    }

    #[test]
    fn test_validate_sub_pipeline_exactly_one_fin_leaf() {
        let mut sub_def = minimal_sub_def();
        // Add a second finalization leaf (not connected)
        sub_def.finalization_blocks.push(PipelineBlock {
            id: 201,
            name: "Second".into(),
            agents: vec!["Claude".into()],
            prompt: "extra".into(),
            profiles: vec![],
            session_id: None,
            position: (1, 1),
            replicas: 1,
            sub_pipeline: None,
        });
        sub_def.data_feeds.push(DataFeed {
            from: WILDCARD_BLOCK_ID,
            to: 201,
            collection: FeedCollection::LastPass,
            granularity: FeedGranularity::PerRun,
        });
        let def = def_with(vec![sub_pipeline_block(1, 0, 0, sub_def)], vec![]);
        let err = validate_pipeline(&def).unwrap_err();
        assert!(err.to_string().contains("2 finalization outputs"));
    }

    #[test]
    fn test_validate_sub_pipeline_terminal_leaf_replicas_1() {
        let mut sub_def = minimal_sub_def();
        sub_def.finalization_blocks[0].replicas = 2;
        let def = def_with(vec![sub_pipeline_block(1, 0, 0, sub_def)], vec![]);
        let err = validate_pipeline(&def).unwrap_err();
        assert!(err.to_string().contains("replicas = 1"));
    }

    #[test]
    fn test_validate_sub_pipeline_terminal_leaf_single_agent() {
        let mut sub_def = minimal_sub_def();
        sub_def.finalization_blocks[0].agents = vec!["A".into(), "B".into()];
        let def = def_with(vec![sub_pipeline_block(1, 0, 0, sub_def)], vec![]);
        let err = validate_pipeline(&def).unwrap_err();
        assert!(err.to_string().contains("exactly one agent"));
    }

    #[test]
    fn test_validate_parent_sub_block_multi_replicas() {
        let mut sub_block = sub_pipeline_block(1, 0, 0, minimal_sub_def());
        sub_block.replicas = 2;
        let def = def_with(vec![sub_block], vec![]);
        assert!(validate_pipeline(&def).is_ok());
    }

    #[test]
    fn test_validate_rejects_sub_pipeline_with_agents() {
        let mut sub_block = sub_pipeline_block(1, 0, 0, minimal_sub_def());
        sub_block.agents = vec!["Claude".into()];
        let def = def_with(vec![sub_block], vec![]);
        let err = validate_pipeline(&def).unwrap_err();
        assert!(err.to_string().contains("must not have agents"));
    }

    #[test]
    fn test_validate_rejects_sub_pipeline_on_finalization_block() {
        let def = PipelineDefinition {
            initial_prompt: "test".into(),
            blocks: vec![block(1, 0, 0)],
            connections: vec![],
            session_configs: vec![],
            loop_connections: vec![],
            finalization_blocks: vec![sub_pipeline_block(2, 0, 1, minimal_sub_def())],
            finalization_connections: vec![],
            data_feeds: vec![DataFeed {
                from: WILDCARD_BLOCK_ID,
                to: 2,
                collection: FeedCollection::LastPass,
                granularity: FeedGranularity::PerRun,
            }],
        };
        let err = validate_pipeline(&def).unwrap_err();
        assert!(err.to_string().contains("Finalization block"));
    }

    #[test]
    fn test_validate_accepts_mixed_blocks() {
        let def = PipelineDefinition {
            initial_prompt: "test".into(),
            blocks: vec![
                block(1, 0, 0),
                sub_pipeline_block(2, 1, 0, minimal_sub_def()),
                block(3, 2, 0),
            ],
            connections: vec![conn(1, 2), conn(2, 3)],
            session_configs: vec![],
            loop_connections: vec![],
            finalization_blocks: vec![],
            finalization_connections: vec![],
            data_feeds: vec![],
        };
        assert!(validate_pipeline(&def).is_ok());
    }

    #[test]
    fn test_all_agent_names_includes_sub_pipeline_agents() {
        let sub_def = PipelineDefinition {
            initial_prompt: "inner".into(),
            blocks: vec![PipelineBlock {
                id: 100,
                name: "InnerBlock".into(),
                agents: vec!["Gemini".into()],
                prompt: "do".into(),
                profiles: vec![],
                session_id: None,
                position: (0, 0),
                replicas: 1,
                sub_pipeline: None,
            }],
            connections: vec![],
            session_configs: vec![],
            loop_connections: vec![],
            finalization_blocks: vec![PipelineBlock {
                id: 200,
                name: "Consolidate".into(),
                agents: vec!["GPT".into()],
                prompt: "summarize".into(),
                profiles: vec![],
                session_id: None,
                position: (0, 1),
                replicas: 1,
                sub_pipeline: None,
            }],
            finalization_connections: vec![],
            data_feeds: vec![DataFeed {
                from: WILDCARD_BLOCK_ID,
                to: 200,
                collection: FeedCollection::LastPass,
                granularity: FeedGranularity::PerRun,
            }],
        };
        let def = PipelineDefinition {
            initial_prompt: "top".into(),
            blocks: vec![block(1, 0, 0), sub_pipeline_block(2, 1, 0, sub_def)],
            connections: vec![conn(1, 2)],
            session_configs: vec![],
            loop_connections: vec![],
            finalization_blocks: vec![],
            finalization_connections: vec![],
            data_feeds: vec![],
        };
        let names = def.all_agent_names();
        assert!(names.contains(&"Claude".to_string()));
        assert!(names.contains(&"Gemini".to_string()));
        assert!(names.contains(&"GPT".to_string()));
    }

    #[test]
    fn test_visit_all_agent_refs_includes_break_agents() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![conn(1, 2)]);
        def.loop_connections.push(LoopConnection {
            from: 2,
            to: 1,
            count: 2,
            prompt: "iterate".into(),
            break_agent: "Evaluator".into(),
            break_condition: "stop if done".into(),
        });
        let mut agents: Vec<String> = Vec::new();
        def.visit_all_agent_refs(&mut |name, _| {
            agents.push(name.to_string());
        });
        assert!(agents.contains(&"Evaluator".to_string()));
    }

    #[test]
    fn test_build_runtime_table_sub_pipeline_single_entry() {
        let def = def_with(
            vec![
                block(1, 0, 0),
                sub_pipeline_block(2, 1, 0, minimal_sub_def()),
            ],
            vec![conn(1, 2)],
        );
        let rt = build_runtime_table(&def);
        // Block 1 = 1 entry, Block 2 (sub-pipeline) = 1 entry
        assert_eq!(rt.entries.len(), 2);
        let sub_rids = rt.logical_to_runtime.get(&2).unwrap();
        assert_eq!(
            sub_rids.len(),
            1,
            "sub-pipeline should have exactly 1 runtime entry"
        );
        assert_eq!(rt.entries[sub_rids[0] as usize].agent, "[Sub#2]");
    }

    #[test]
    fn test_assemble_raw_upstream_context_no_augmentation() {
        let def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![conn(1, 2)]);
        let rt = build_runtime_table(&def);
        let mut outputs: HashMap<u32, String> = HashMap::new();
        let rid_1 = rt.logical_to_runtime.get(&1).unwrap()[0];
        outputs.insert(rid_1, "hello from block 1".into());

        let ctx = assemble_raw_upstream_context(&def.blocks[1], &def, &rt, &outputs);
        assert!(ctx.contains("hello from block 1"));
        // No cwd prefix, no memory, no diagnostics
        assert!(!ctx.contains("Working directory"));
    }

    #[test]
    fn test_read_sub_pipeline_terminal_output_correct_path() {
        let sub_def = minimal_sub_def();
        let dir = tempfile::tempdir().unwrap();
        let fin_dir = dir.path().join("finalization");
        std::fs::create_dir_all(&fin_dir).unwrap();

        // Figure out the expected filename
        let exec_rt = build_runtime_table(&sub_def);
        let fin_entries = build_finalization_runtime_entries(
            &sub_def,
            &FinalizationRunScope::SingleRun {
                run_id: 1,
                run_dir: dir.path().to_path_buf(),
            },
            exec_rt.entries.len() as u32,
        );
        let terminal_entry = fin_entries
            .iter()
            .find(|e| e.source_block_id == 200)
            .unwrap();
        let path = fin_dir.join(format!("{}.md", terminal_entry.filename_stem));
        std::fs::write(&path, "final output").unwrap();

        let result = read_sub_pipeline_terminal_output(&sub_def, dir.path());
        assert_eq!(result.unwrap(), "final output");
    }

    #[tokio::test]
    async fn test_sub_pipeline_e2e_with_mock_provider() {
        let sub_def = minimal_sub_def();
        let def = def_with(vec![sub_pipeline_block(1, 0, 0, sub_def)], vec![]);

        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("sub-e2e")).unwrap();

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let factory: ProviderFactory = Arc::new(move |_kind, _cfg| {
            Box::new(MockProvider::ok(
                ProviderKind::Anthropic,
                "mock output",
                received_clone.clone(),
            ))
        });

        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        let configs = HashMap::from([(
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

        let ctx = crate::execution::PromptRuntimeContext::new("test prompt", false);

        run_pipeline_with_provider_factory(&def, 4, configs, &ctx, &output, tx, cancel, factory)
            .await
            .unwrap();

        let events = collect_progress_events(rx);
        assert!(
            events.iter().any(|e| matches!(e, ProgressEvent::AllDone)),
            "AllDone event must be emitted"
        );
        // The mock should have been called at least once (for the inner execution block + finalization)
        let calls = received.lock().unwrap();
        assert!(
            !calls.is_empty(),
            "inner sub-pipeline blocks should invoke the provider"
        );

        // HIGH fix: BlockFinished must be emitted for the sub-pipeline parent row
        assert!(
            events.iter().any(
                |e| matches!(e, ProgressEvent::BlockFinished { block_id, .. } if *block_id == 0)
            ),
            "BlockFinished must be emitted for sub-pipeline (runtime_id 0): {:?}",
            events
                .iter()
                .filter(|e| matches!(
                    e,
                    ProgressEvent::BlockFinished { .. } | ProgressEvent::BlockError { .. }
                ))
                .collect::<Vec<_>>()
        );

        // HIGH fix: parent-level output file must be written for downstream/feed consumers
        let run_dir = output.run_dir();
        let expected_parent_file = run_dir.join("sub_b1_pipeline.md");
        assert!(
            expected_parent_file.exists(),
            "Parent-level output file {expected_parent_file:?} must be written",
        );
        let content = std::fs::read_to_string(&expected_parent_file).unwrap();
        assert!(
            !content.is_empty(),
            "Parent-level output file must not be empty"
        );
    }

    #[test]
    fn test_deser_rejects_agents_on_sub_pipeline_block() {
        let toml_str = r#"
            id = 1
            name = "bad"
            agents = ["Claude"]
            prompt = "x"
            profiles = []
            position = [0, 0]
            replicas = 1

            [sub_pipeline]
            initial_prompt = "inner"
            connections = []
            session_configs = []
            loop_connections = []
            finalization_connections = []
            data_feeds = []

            [[sub_pipeline.blocks]]
            id = 100
            name = "B"
            agents = ["Claude"]
            prompt = "p"
            profiles = []
            position = [0, 0]

            [[sub_pipeline.finalization_blocks]]
            id = 200
            name = "Fin"
            agents = ["Claude"]
            prompt = "fin"
            profiles = []
            position = [0, 1]
        "#;
        let result: Result<PipelineBlock, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "should reject agents on sub-pipeline block"
        );
        assert!(
            result.unwrap_err().to_string().contains("must not have"),
            "error should mention agents restriction"
        );
    }

    #[test]
    fn test_deser_allows_empty_agents_on_sub_pipeline_block() {
        let toml_str = r#"
            id = 1
            name = "ok"
            agents = []
            prompt = ""
            profiles = []
            position = [0, 0]
            replicas = 1

            [sub_pipeline]
            initial_prompt = "inner"
            connections = []
            session_configs = []
            loop_connections = []
            finalization_connections = []
            data_feeds = []

            [[sub_pipeline.blocks]]
            id = 100
            name = "B"
            agents = ["Claude"]
            prompt = "p"
            profiles = []
            position = [0, 0]

            [[sub_pipeline.finalization_blocks]]
            id = 200
            name = "Fin"
            agents = ["Claude"]
            prompt = "fin"
            profiles = []
            position = [0, 1]
        "#;
        let result: Result<PipelineBlock, _> = toml::from_str(toml_str);
        assert!(
            result.is_ok(),
            "empty agents on sub-pipeline should be allowed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_add_allowed_dir_on_provider_trait() {
        // Verify the default trait implementation is a no-op (doesn't panic)
        use crate::provider::Provider;
        let mut mock = MockProvider::ok(
            ProviderKind::Anthropic,
            "test",
            Arc::new(Mutex::new(Vec::new())),
        );
        mock.add_allowed_dir("/tmp/test".into());
        // Should not panic — default no-op
    }

    #[test]
    fn test_sub_pipeline_filename_stem_passes_output_contract() {
        let def = def_with(vec![sub_pipeline_block(1, 0, 0, minimal_sub_def())], vec![]);
        let rt = build_runtime_table(&def);
        let sub_rid = rt.logical_to_runtime.get(&1).unwrap()[0];
        let stem = &rt.entries[sub_rid as usize].filename_stem;
        let filename = format!("{stem}.md");
        assert!(
            crate::post_run::is_pipeline_output_filename(&filename),
            "Sub-pipeline filename '{filename}' must pass is_pipeline_output_filename",
        );
        // Loop variant too
        let loop_filename = format!("{stem}_loop3.md");
        assert!(
            crate::post_run::is_pipeline_output_filename(&loop_filename),
            "Sub-pipeline loop filename '{loop_filename}' must pass is_pipeline_output_filename",
        );
    }

    #[tokio::test]
    async fn test_sub_pipeline_in_loop_produces_loop_filenames() {
        // Build a parent pipeline: sub-pipeline block (1) -> regular block (2)
        // with a loop from block 2 back to block 1 (count=1 -> 2 total passes).
        let sub_def = minimal_sub_def();

        let def = PipelineDefinition {
            initial_prompt: "test".into(),
            blocks: vec![sub_pipeline_block(1, 0, 0, sub_def), block(2, 1, 0)],
            connections: vec![conn(1, 2)],
            session_configs: Vec::new(),
            loop_connections: vec![lconn(2, 1, 1)],
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        };

        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("sub-loop")).unwrap();

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let factory: ProviderFactory = Arc::new(move |_kind, _cfg| {
            Box::new(MockProvider::ok(
                ProviderKind::Anthropic,
                "mock output",
                received_clone.clone(),
            ))
        });

        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        let configs = HashMap::from([(
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

        let ctx = crate::execution::PromptRuntimeContext::new("test prompt", false);

        run_pipeline_with_provider_factory(&def, 4, configs, &ctx, &output, tx, cancel, factory)
            .await
            .unwrap();

        let events = collect_progress_events(rx);
        assert!(
            events.iter().any(|e| matches!(e, ProgressEvent::AllDone)),
            "AllDone event must be emitted"
        );

        let run_dir = output.run_dir();
        let pass0_file = run_dir.join("sub_b1_pipeline.md");
        let pass1_file = run_dir.join("sub_b1_pipeline_loop1.md");

        assert!(
            pass0_file.exists(),
            "Pass 0 output file {pass0_file:?} must exist in {run_dir:?}",
        );
        assert!(
            pass1_file.exists(),
            "Pass 1 (loop) output file {pass1_file:?} must exist in {run_dir:?}",
        );
    }

    #[tokio::test]
    async fn test_sub_pipeline_provider_error_propagates_block_error() {
        let sub_def = minimal_sub_def();
        let def = def_with(vec![sub_pipeline_block(1, 0, 0, sub_def)], vec![]);

        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("sub-err")).unwrap();

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let factory: ProviderFactory = Arc::new(move |_kind, _cfg| {
            Box::new(MockProvider::err(
                ProviderKind::Anthropic,
                "inner provider failed",
                received_clone.clone(),
            ))
        });

        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        let configs = HashMap::from([(
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

        let ctx = crate::execution::PromptRuntimeContext::new("test prompt", false);

        run_pipeline_with_provider_factory(&def, 4, configs, &ctx, &output, tx, cancel, factory)
            .await
            .unwrap();

        let events = collect_progress_events(rx);

        // Sub-pipeline block (logical id 1) gets runtime_id 0
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::BlockError { block_id, .. } if *block_id == 0
            )),
            "BlockError must be emitted for the sub-pipeline block (runtime_id 0): {events:?}"
        );

        assert!(
            events.iter().any(|e| matches!(e, ProgressEvent::AllDone)),
            "AllDone event must be emitted"
        );

        // The inner pipeline writes _errors.log inside the sub-run directory
        let sub_errors_log = output.run_dir().join("sub_1").join("_errors.log");
        assert!(
            sub_errors_log.exists(),
            "_errors.log must exist in sub-pipeline run dir {sub_errors_log:?}",
        );
        let log_content = std::fs::read_to_string(&sub_errors_log).unwrap();
        assert!(
            log_content.contains("inner provider failed"),
            "_errors.log must contain the error text, got: {log_content}"
        );
    }

    #[tokio::test]
    async fn test_sub_pipeline_cancel_stops_execution() {
        let sub_def = minimal_sub_def();
        let def = def_with(
            vec![sub_pipeline_block(1, 0, 0, sub_def), block(2, 1, 0)],
            vec![conn(1, 2)],
        );

        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("sub-cancel")).unwrap();

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let factory: ProviderFactory = Arc::new(move |_kind, _cfg| {
            Box::new(MockProvider::ok(
                ProviderKind::Anthropic,
                "should not run",
                received_clone.clone(),
            ))
        });

        let (tx, rx) = mpsc::unbounded_channel();
        // Pre-cancelled before execution starts
        let cancel = Arc::new(AtomicBool::new(true));

        let configs = HashMap::from([(
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

        let ctx = crate::execution::PromptRuntimeContext::new("test prompt", false);

        run_pipeline_with_provider_factory(&def, 4, configs, &ctx, &output, tx, cancel, factory)
            .await
            .unwrap();

        let events = collect_progress_events(rx);

        assert!(
            events.iter().any(|e| matches!(e, ProgressEvent::AllDone)),
            "AllDone event must be emitted even when cancelled"
        );

        let calls = received.lock().unwrap();
        assert!(
            calls.is_empty(),
            "Provider must never be called when cancelled, but got {0} calls",
            calls.len()
        );
    }

    #[tokio::test]
    async fn test_sub_pipeline_error_skips_downstream() {
        let sub_def = minimal_sub_def();
        let def = def_with(
            vec![sub_pipeline_block(1, 0, 0, sub_def), block(2, 1, 0)],
            vec![conn(1, 2)],
        );

        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("sub-skip")).unwrap();

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let factory: ProviderFactory = Arc::new(move |_kind, _cfg| {
            Box::new(MockProvider::err(
                ProviderKind::Anthropic,
                "sub-pipeline broke",
                received_clone.clone(),
            ))
        });

        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        let configs = HashMap::from([(
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

        let ctx = crate::execution::PromptRuntimeContext::new("test prompt", false);

        run_pipeline_with_provider_factory(&def, 4, configs, &ctx, &output, tx, cancel, factory)
            .await
            .unwrap();

        let events = collect_progress_events(rx);

        // Sub-pipeline block (logical id 1) gets runtime_id 0
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::BlockError { block_id, .. } if *block_id == 0
            )),
            "BlockError must be emitted for the sub-pipeline block (runtime_id 0): {events:?}"
        );

        // Block 2 (logical id 2) gets runtime_id 1 and should be skipped
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::BlockSkipped { block_id, .. } if *block_id == 1
            )),
            "BlockSkipped must be emitted for the downstream block (runtime_id 1): {events:?}"
        );

        assert!(
            events.iter().any(|e| matches!(e, ProgressEvent::AllDone)),
            "AllDone event must be emitted"
        );
    }

    // ── Scatter Queue Tests ──

    #[test]
    fn scatter_queue_basic_split() {
        let output = "item1\n===SCATTER_ITEM===\nitem2\n===SCATTER_ITEM===\nitem3";
        let mut q = ScatterQueue::from_output(output, "===SCATTER_ITEM===");
        assert_eq!(q.total(), 3);
        assert_eq!(q.remaining(), 3);

        let first = q.pop().unwrap();
        assert_eq!(first.index, 0);
        assert_eq!(first.content, "item1");

        let second = q.pop().unwrap();
        assert_eq!(second.index, 1);
        assert_eq!(second.content, "item2");

        let third = q.pop().unwrap();
        assert_eq!(third.index, 2);
        assert_eq!(third.content, "item3");

        assert!(q.pop().is_none());
        assert!(q.is_empty());
        assert_eq!(q.total(), 3); // total doesn't change
    }

    #[test]
    fn scatter_queue_empty_input() {
        let q = ScatterQueue::from_output("", "===SCATTER_ITEM===");
        assert_eq!(q.total(), 0);
        assert!(q.is_empty());
    }

    #[test]
    fn scatter_queue_single_item_no_delimiter() {
        let mut q = ScatterQueue::from_output("just one item", "===SCATTER_ITEM===");
        assert_eq!(q.total(), 1);
        let item = q.pop().unwrap();
        assert_eq!(item.index, 0);
        assert_eq!(item.content, "just one item");
        assert!(q.pop().is_none());
    }

    #[test]
    fn scatter_queue_trims_whitespace() {
        let output =
            "  a  \n===SCATTER_ITEM===\n  b  \n===SCATTER_ITEM===\n\n===SCATTER_ITEM===\n  c  ";
        let mut q = ScatterQueue::from_output(output, "===SCATTER_ITEM===");
        // Empty items after trimming are filtered out
        assert_eq!(q.total(), 3);
        assert_eq!(q.pop().unwrap().content, "a");
        assert_eq!(q.pop().unwrap().content, "b");
        assert_eq!(q.pop().unwrap().content, "c");
    }

    #[test]
    fn scatter_queue_custom_delimiter() {
        let output = "part1---part2---part3";
        let mut q = ScatterQueue::from_output(output, "---");
        assert_eq!(q.total(), 3);
        assert_eq!(q.pop().unwrap().content, "part1");
        assert_eq!(q.pop().unwrap().content, "part2");
        assert_eq!(q.pop().unwrap().content, "part3");
    }

    #[test]
    fn scatter_queue_fifo_ordering() {
        let output = "first\n===SCATTER_ITEM===\nsecond\n===SCATTER_ITEM===\nthird";
        let mut q = ScatterQueue::from_output(output, "===SCATTER_ITEM===");
        assert_eq!(q.pop().unwrap().index, 0);
        assert_eq!(q.pop().unwrap().index, 1);
        assert_eq!(q.pop().unwrap().index, 2);
        assert!(q.pop().is_none());
    }

    // ── Scatter Serialization Tests ──

    #[test]
    fn scatter_connection_serialization_roundtrip() {
        let mut def = def_with(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![{
                let mut c = PipelineConnection::new(1, 2);
                c.scatter = true;
                c.scatter_delimiter = "|||".into();
                c
            }],
        );
        def.connections.push(PipelineConnection::new(1, 2)); // non-scatter won't duplicate — just for serialization test
        def.connections.remove(1); // keep only scatter

        let toml_str = toml::to_string(&def).unwrap();
        assert!(toml_str.contains("scatter = true"));
        assert!(toml_str.contains("scatter_delimiter = \"|||\""));

        let round: PipelineDefinition = toml::from_str(&toml_str).unwrap();
        assert!(round.connections[0].scatter);
        assert_eq!(round.connections[0].scatter_delimiter, "|||");
    }

    #[test]
    fn scatter_false_omitted_from_toml() {
        let def = def_with(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![PipelineConnection::new(1, 2)],
        );
        let toml_str = toml::to_string(&def).unwrap();
        assert!(!toml_str.contains("scatter"));
    }

    #[test]
    fn scatter_backward_compat_old_toml() {
        // Old TOML without scatter fields should deserialize with scatter=false
        let toml_str = r#"
            initial_prompt = "test"
            [[blocks]]
            id = 1
            name = "A"
            agents = ["Claude"]
            position = [0, 0]
            [[blocks]]
            id = 2
            name = "B"
            agents = ["Claude"]
            position = [1, 0]
            [[connections]]
            from = 1
            to = 2
        "#;
        let def: PipelineDefinition = toml::from_str(toml_str).unwrap();
        assert!(!def.connections[0].scatter);
        // scatter_delimiter should be the default
        assert_eq!(def.connections[0].scatter_delimiter, "===SCATTER_ITEM===");
    }

    // ── Scatter Validation Tests ──

    #[test]
    fn scatter_validation_max_one_per_block() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)], vec![]);
        let mut c1 = PipelineConnection::new(1, 3);
        c1.scatter = true;
        let mut c2 = PipelineConnection::new(2, 3);
        c2.scatter = true;
        def.connections = vec![c1, c2];
        let result = validate_pipeline(&def);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("scatter connections"));
    }

    #[test]
    fn scatter_validation_source_must_be_single_task() {
        let mut def = def_with(
            vec![
                {
                    let mut b = block(1, 0, 0);
                    b.replicas = 2;
                    b
                },
                block(2, 1, 0),
            ],
            vec![],
        );
        let mut c = PipelineConnection::new(1, 2);
        c.scatter = true;
        def.connections = vec![c];
        let result = validate_pipeline(&def);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("logical_task_count"));
    }

    #[test]
    fn scatter_validation_allows_sub_pipeline_target() {
        let source = block(1, 0, 0);
        let mut target = sub_pipeline_block(2, 1, 0, minimal_sub_def());
        target.replicas = 2;
        let mut c = PipelineConnection::new(1, 2);
        c.scatter = true;
        let def = PipelineDefinition {
            initial_prompt: "test".into(),
            blocks: vec![source, target],
            connections: vec![c],
            session_configs: Vec::new(),
            loop_connections: Vec::new(),
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        };
        assert!(validate_pipeline(&def).is_ok());
    }

    #[test]
    fn scatter_validation_forbidden_on_finalization() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.finalization_blocks.push(block(10, 0, 1));
        def.finalization_blocks.push(block(11, 1, 1));
        let mut fc = PipelineConnection::new(10, 11);
        fc.scatter = true;
        def.finalization_connections = vec![fc];
        let result = validate_pipeline(&def);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("finalization"));
    }

    // ── Scatter File Naming Tests ──

    #[test]
    fn scatter_replica_filename_pass_0() {
        let info = RuntimeReplicaInfo {
            runtime_id: 0,
            source_block_id: 2,
            replica_index: 0,
            phase: RuntimePhase::Execution,
            run_scope: None,
            agent: "Claude".into(),
            session_key: "k".into(),
            display_label: "Fixer_b2_Claude_r0".into(),
            filename_stem: "Fixer_b2_Claude_r0".into(),
        };
        assert_eq!(
            scatter_replica_filename(&info, 3, 0),
            "Fixer_b2_Claude_r0_item3.md"
        );
    }

    #[test]
    fn scatter_replica_filename_pass_gt_0() {
        let info = RuntimeReplicaInfo {
            runtime_id: 1,
            source_block_id: 2,
            replica_index: 1,
            phase: RuntimePhase::Execution,
            run_scope: None,
            agent: "Claude".into(),
            session_key: "k".into(),
            display_label: "Fixer_b2_Claude_r1".into(),
            filename_stem: "Fixer_b2_Claude_r1".into(),
        };
        assert_eq!(
            scatter_replica_filename(&info, 5, 2),
            "Fixer_b2_Claude_r1_item5_loop2.md"
        );
    }

    #[test]
    fn pipeline_connection_new_defaults_scatter_false() {
        let c = PipelineConnection::new(1, 2);
        assert!(!c.scatter);
        assert_eq!(c.scatter_delimiter, DEFAULT_SCATTER_DELIMITER);
    }

    // ── H2 fix: upstream_of_non_scatter ──

    #[test]
    fn upstream_of_non_scatter_excludes_scatter_edges() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)], vec![]);
        // Regular edge 1→3, scatter edge 2→3
        def.connections.push(PipelineConnection::new(1, 3));
        let mut sc = PipelineConnection::new(2, 3);
        sc.scatter = true;
        def.connections.push(sc);

        // upstream_of includes both
        let all = upstream_of(&def, 3);
        assert_eq!(all.len(), 2);

        // upstream_of_non_scatter excludes scatter
        let non_scatter = upstream_of_non_scatter(&def, 3);
        assert_eq!(non_scatter.len(), 1);
        assert_eq!(non_scatter[0], 1);
    }

    #[test]
    fn upstream_of_non_scatter_root_when_only_scatter() {
        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let mut sc = PipelineConnection::new(1, 2);
        sc.scatter = true;
        def.connections.push(sc);

        // Block 2 has upstream via upstream_of (scatter edge)
        assert!(!upstream_of(&def, 2).is_empty());
        // But is considered root by non-scatter version
        assert!(upstream_of_non_scatter(&def, 2).is_empty());
    }

    // ── M3 fix: scatter target must be loop restart block ──

    #[test]
    fn scatter_validation_target_must_be_loop_restart_block() {
        // Setup: blocks 4→1→2→3, loop 3→2, scatter 4→2 (target is loop restart ✓)
        let mut def = def_with(
            vec![
                block(4, 0, 0),
                block(1, 1, 0),
                block(2, 2, 0),
                block(3, 3, 0),
            ],
            vec![
                PipelineConnection::new(4, 1),
                PipelineConnection::new(1, 2),
                PipelineConnection::new(2, 3),
            ],
        );
        def.loop_connections.push(LoopConnection {
            from: 3,
            to: 2,
            count: 2,
            prompt: String::new(),
            break_agent: String::new(),
            break_condition: String::new(),
        });
        let mut sc = PipelineConnection::new(4, 2);
        sc.scatter = true;
        def.connections.push(sc);

        // scatter target = loop_to (2) → valid
        if let Err(e) = validate_pipeline(&def) {
            panic!("expected valid but got: {e}");
        }

        // Now make scatter target = 3 (inside loop but NOT the restart block)
        def.connections.pop();
        let mut sc2 = PipelineConnection::new(4, 3);
        sc2.scatter = true;
        def.connections.push(sc2);
        let result = validate_pipeline(&def);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not the loop restart block"));
    }

    // ── L2 fix: feed collection matches _item filenames ──

    #[test]
    fn scatter_item_filename_matches_feed_pattern() {
        // The feed collection closure matches _item suffixes
        let stem = "Block1_b1_Claude_r0";
        let item_name = format!("{stem}_item3.md");
        let item_loop_name = format!("{stem}_item2_loop1.md");
        let base_name = format!("{stem}.md");
        let loop_name = format!("{stem}_loop2.md");

        let filename_stems = [stem.to_string()];
        let check = |name: &str| -> bool {
            filename_stems.iter().any(|s| {
                if !name.starts_with(s) {
                    return false;
                }
                let remainder = &name[s.len()..];
                remainder == ".md"
                    || (remainder.starts_with("_loop") && remainder.ends_with(".md"))
                    || (remainder.starts_with("_iter") && remainder.ends_with(".md"))
                    || (remainder.starts_with("_item") && remainder.ends_with(".md"))
            })
        };

        assert!(check(&base_name));
        assert!(check(&loop_name));
        assert!(check(&item_name));
        assert!(check(&item_loop_name));
        assert!(!check("unrelated.md"));
    }

    // ── cli_upstream_filenames: prefix collision and sort order ──

    #[test]
    fn cli_upstream_filenames_no_prefix_collision() {
        // Stems _r1 and _r10 must not cross-match
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path();

        // Create files for r1 (items 0,1) and r10 (items 0,1)
        std::fs::write(run_dir.join("B_b1_A_r1_item0.md"), "r1i0").unwrap();
        std::fs::write(run_dir.join("B_b1_A_r1_item1.md"), "r1i1").unwrap();
        std::fs::write(run_dir.join("B_b1_A_r10_item0.md"), "r10i0").unwrap();
        std::fs::write(run_dir.join("B_b1_A_r10_item1.md"), "r10i1").unwrap();

        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let mut sc = PipelineConnection::new(1, 2);
        sc.scatter = true;
        def.connections.push(sc);

        let btl = HashMap::new();
        let blp = HashMap::new();

        // Query for r1 stem — must NOT match r10 files
        let info_r1 = RuntimeReplicaInfo {
            runtime_id: 0,
            source_block_id: 2,
            replica_index: 1,
            phase: RuntimePhase::Execution,
            run_scope: None,
            agent: "A".into(),
            session_key: "k".into(),
            display_label: "B_b1_A_r1".into(),
            filename_stem: "B_b1_A_r1".into(),
        };
        let files_r1 = cli_upstream_filenames(&info_r1, 2, &def, &btl, &blp, run_dir);
        assert_eq!(
            files_r1.len(),
            2,
            "r1 should match 2 files, got: {files_r1:?}"
        );
        assert!(files_r1.iter().all(|f| f.contains("_r1_item")));
        assert!(!files_r1.iter().any(|f| f.contains("_r10")));

        // Query for r10 stem — must NOT match r1 files
        let info_r10 = RuntimeReplicaInfo {
            runtime_id: 1,
            source_block_id: 2,
            replica_index: 10,
            phase: RuntimePhase::Execution,
            run_scope: None,
            agent: "A".into(),
            session_key: "k".into(),
            display_label: "B_b1_A_r10".into(),
            filename_stem: "B_b1_A_r10".into(),
        };
        let files_r10 = cli_upstream_filenames(&info_r10, 2, &def, &btl, &blp, run_dir);
        assert_eq!(
            files_r10.len(),
            2,
            "r10 should match 2 files, got: {files_r10:?}"
        );
        assert!(files_r10.iter().all(|f| f.contains("_r10_item")));
    }

    #[test]
    fn cli_upstream_filenames_numeric_sort() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path();

        // Create 12 item files (out of order on disk)
        for i in [10, 2, 0, 11, 1, 5, 9, 3, 8, 4, 7, 6] {
            std::fs::write(run_dir.join(format!("B_b1_A_r0_item{i}.md")), "x").unwrap();
        }

        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let mut sc = PipelineConnection::new(1, 2);
        sc.scatter = true;
        def.connections.push(sc);

        let btl = HashMap::new();
        let blp = HashMap::new();

        let info = RuntimeReplicaInfo {
            runtime_id: 0,
            source_block_id: 2,
            replica_index: 0,
            phase: RuntimePhase::Execution,
            run_scope: None,
            agent: "A".into(),
            session_key: "k".into(),
            display_label: "B_b1_A_r0".into(),
            filename_stem: "B_b1_A_r0".into(),
        };
        let files = cli_upstream_filenames(&info, 2, &def, &btl, &blp, run_dir);
        assert_eq!(files.len(), 12);
        // Verify numeric order: item0, item1, ..., item11
        for (i, f) in files.iter().enumerate() {
            assert!(
                f.contains(&format!("_item{i}.")),
                "Expected item{i} at position {i}, got {f}"
            );
        }
    }

    #[test]
    fn cli_upstream_filenames_stem_with_loop_in_name() {
        // Block named "fix_loop" produces stem "fix_loop_b2_A_r0".
        // Pass-0 scatter files like "fix_loop_b2_A_r0_item0.md" contain "_loop"
        // in the stem but must NOT be rejected by the pass-0 filter.
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path();

        std::fs::write(run_dir.join("fix_loop_b2_A_r0_item0.md"), "x").unwrap();
        std::fs::write(run_dir.join("fix_loop_b2_A_r0_item1.md"), "x").unwrap();
        // Also create a real loop file that SHOULD be rejected on pass 0
        std::fs::write(run_dir.join("fix_loop_b2_A_r0_item0_loop1.md"), "x").unwrap();

        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let mut sc = PipelineConnection::new(1, 2);
        sc.scatter = true;
        def.connections.push(sc);

        let btl = HashMap::new();
        let blp = HashMap::new(); // pass 0

        let info = RuntimeReplicaInfo {
            runtime_id: 0,
            source_block_id: 2,
            replica_index: 0,
            phase: RuntimePhase::Execution,
            run_scope: None,
            agent: "A".into(),
            session_key: "k".into(),
            display_label: "fix_loop_b2_A_r0".into(),
            filename_stem: "fix_loop_b2_A_r0".into(),
        };
        let files = cli_upstream_filenames(&info, 2, &def, &btl, &blp, run_dir);
        assert_eq!(files.len(), 2, "Should find 2 pass-0 files, got: {files:?}");
        assert!(files.iter().all(|f| !f.contains("_loop1")));
    }

    #[test]
    fn cli_upstream_filenames_loop_pass_gt_0() {
        // On loop pass 2, only _item{N}_loop2.md files should be returned.
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path();

        // Pass 0 files (should be excluded)
        std::fs::write(run_dir.join("B_b2_A_r0_item0.md"), "x").unwrap();
        std::fs::write(run_dir.join("B_b2_A_r0_item1.md"), "x").unwrap();
        // Pass 1 files (should be excluded)
        std::fs::write(run_dir.join("B_b2_A_r0_item2_loop1.md"), "x").unwrap();
        // Pass 2 files (should be included)
        std::fs::write(run_dir.join("B_b2_A_r0_item3_loop2.md"), "x").unwrap();
        std::fs::write(run_dir.join("B_b2_A_r0_item4_loop2.md"), "x").unwrap();

        let mut def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let mut sc = PipelineConnection::new(1, 2);
        sc.scatter = true;
        def.connections.push(sc);

        let btl = HashMap::new();
        let mut blp = HashMap::new();
        blp.insert(2, 2u32); // upstream block 2 is on loop pass 2

        let info = RuntimeReplicaInfo {
            runtime_id: 0,
            source_block_id: 2,
            replica_index: 0,
            phase: RuntimePhase::Execution,
            run_scope: None,
            agent: "A".into(),
            session_key: "k".into(),
            display_label: "B_b2_A_r0".into(),
            filename_stem: "B_b2_A_r0".into(),
        };
        let files = cli_upstream_filenames(&info, 2, &def, &btl, &blp, run_dir);
        assert_eq!(files.len(), 2, "Should find 2 loop-2 files, got: {files:?}");
        // Verify numeric ordering: item3 before item4
        assert!(files[0].contains("_item3_loop2"));
        assert!(files[1].contains("_item4_loop2"));
    }

    // ── Multi-replica sub-pipeline tests ──

    #[test]
    fn logical_task_count_sub_pipeline_scales_with_replicas() {
        let mut sub = sub_pipeline_block(1, 0, 0, minimal_sub_def());
        assert_eq!(sub.logical_task_count(), 1);
        sub.replicas = 4;
        assert_eq!(sub.logical_task_count(), 4);
    }

    #[test]
    fn runtime_table_sub_pipeline_multi_replica() {
        let mut sub_block = sub_pipeline_block(2, 1, 0, minimal_sub_def());
        sub_block.replicas = 3;
        let def = def_with(vec![block(1, 0, 0), sub_block], vec![conn(1, 2)]);
        let rt = build_runtime_table(&def);
        assert_eq!(rt.entries.len(), 4); // 1 regular + 3 sub-pipeline
        let sub_rids = rt.logical_to_runtime.get(&2).unwrap();
        assert_eq!(sub_rids.len(), 3);
        for (i, &rid) in sub_rids.iter().enumerate() {
            let info = &rt.entries[rid as usize];
            assert_eq!(info.source_block_id, 2);
            assert_eq!(info.replica_index, i as u32);
            assert_eq!(info.filename_stem, format!("sub_b2_pipeline_r{}", i + 1));
        }
    }

    #[test]
    fn sub_pipeline_multi_replica_filename_passes_output_contract() {
        let mut sub_block = sub_pipeline_block(1, 0, 0, minimal_sub_def());
        sub_block.replicas = 2;
        let def = def_with(vec![sub_block], vec![]);
        let rt = build_runtime_table(&def);
        for rid in rt.logical_to_runtime.get(&1).unwrap() {
            let stem = &rt.entries[*rid as usize].filename_stem;
            let filename = format!("{stem}.md");
            assert!(
                crate::post_run::is_pipeline_output_filename(&filename),
                "Multi-replica sub-pipeline filename '{filename}' must pass is_pipeline_output_filename",
            );
            let loop_filename = format!("{stem}_loop3.md");
            assert!(
                crate::post_run::is_pipeline_output_filename(&loop_filename),
                "Loop variant '{loop_filename}' must pass is_pipeline_output_filename",
            );
        }
    }

    #[test]
    fn runtime_table_sub_pipeline_multi_replica_loop_filenames() {
        let mut sub = sub_pipeline_block(2, 0, 0, minimal_sub_def());
        sub.replicas = 2;
        let def = def_with(vec![block(1, 0, 0), sub], vec![conn(1, 2)]);
        let rt = build_runtime_table(&def);
        let rids = rt.logical_to_runtime.get(&2).unwrap();
        assert_eq!(
            loop_replica_filename(&rt.entries[rids[0] as usize], 0),
            "sub_b2_pipeline_r1.md"
        );
        assert_eq!(
            loop_replica_filename(&rt.entries[rids[1] as usize], 2),
            "sub_b2_pipeline_r2_loop2.md"
        );
    }

    #[test]
    fn scatter_source_multi_replica_sub_pipeline_rejected() {
        let mut source = sub_pipeline_block(1, 0, 0, minimal_sub_def());
        source.replicas = 2; // logical_task_count() == 2
        let target = block(2, 1, 0);
        let mut c = PipelineConnection::new(1, 2);
        c.scatter = true;
        let def = PipelineDefinition {
            initial_prompt: "test".into(),
            blocks: vec![source, target],
            connections: vec![c],
            session_configs: Vec::new(),
            loop_connections: Vec::new(),
            finalization_blocks: Vec::new(),
            finalization_connections: Vec::new(),
            data_feeds: Vec::new(),
        };
        let err = validate_pipeline(&def).unwrap_err();
        assert!(err.to_string().contains("logical_task_count"));
    }
}
