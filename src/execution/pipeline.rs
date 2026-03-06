use crate::config::AppConfig;
use crate::error::AppError;
use crate::execution::{wait_for_cancel, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::{self, ProviderKind};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub type BlockId = u32;
#[allow(dead_code)]
type ProviderPool = HashMap<(String, String), Arc<Mutex<Box<dyn provider::Provider>>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineBlock {
    pub id: BlockId,
    #[serde(default)]
    pub name: String,
    pub agent: String,
    #[serde(default)]
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub position: (u16, u16), // grid coordinates (col, row)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConnection {
    pub from: BlockId,
    pub to: BlockId,
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
}

fn default_iterations() -> u32 {
    1
}

impl Default for PipelineDefinition {
    fn default() -> Self {
        Self {
            initial_prompt: String::new(),
            iterations: 1,
            blocks: Vec::new(),
            connections: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Graph utilities
// ---------------------------------------------------------------------------

/// Blocks with no incoming connections (DAG roots).
#[allow(dead_code)]
pub fn root_blocks(def: &PipelineDefinition) -> Vec<BlockId> {
    let has_incoming: HashSet<BlockId> = def.connections.iter().map(|c| c.to).collect();
    def.blocks
        .iter()
        .filter(|b| !has_incoming.contains(&b.id))
        .map(|b| b.id)
        .collect()
}

/// Blocks with no outgoing connections (DAG terminals).
pub fn terminal_blocks(def: &PipelineDefinition) -> Vec<BlockId> {
    let has_outgoing: HashSet<BlockId> = def.connections.iter().map(|c| c.from).collect();
    def.blocks
        .iter()
        .filter(|b| !has_outgoing.contains(&b.id))
        .map(|b| b.id)
        .collect()
}

/// Direct predecessors of a block.
pub fn upstream_of(def: &PipelineDefinition, id: BlockId) -> Vec<BlockId> {
    def.connections
        .iter()
        .filter(|c| c.to == id)
        .map(|c| c.from)
        .collect()
}

/// Kahn's algorithm: returns parallelizable layers or Err on cycle.
pub fn topological_layers(
    def: &PipelineDefinition,
) -> Result<Vec<Vec<BlockId>>, CycleError> {
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
                    let deg = in_degree.get_mut(&child).unwrap();
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
/// Checks if `from` is reachable from `to` in the existing graph.
pub fn would_create_cycle(def: &PipelineDefinition, from: BlockId, to: BlockId) -> bool {
    if from == to {
        return true;
    }
    // BFS from `to` — if we reach `from`, adding from→to would create a cycle
    let downstream: HashMap<BlockId, Vec<BlockId>> = {
        let mut map: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
        for conn in &def.connections {
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

// ---------------------------------------------------------------------------
// Auto-position
// ---------------------------------------------------------------------------

/// Scan grid left-to-right, top-to-bottom for first unoccupied slot.
pub fn next_free_position(def: &PipelineDefinition) -> (u16, u16) {
    let occupied: HashSet<(u16, u16)> = def.blocks.iter().map(|b| b.position).collect();
    for row in 0u16..100 {
        for col in 0u16..100 {
            if !occupied.contains(&(col, row)) {
                return (col, row);
            }
        }
    }
    (0, 0)
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
    let content = toml::to_string_pretty(def)
        .map_err(|e| AppError::Config(format!("Failed to serialize pipeline: {e}")))?;
    std::fs::write(path, content)?;
    Ok(())
}

pub fn load_pipeline(path: &Path) -> Result<PipelineDefinition, AppError> {
    let content = std::fs::read_to_string(path)?;
    let def: PipelineDefinition = toml::from_str(&content)
        .map_err(|e| AppError::Config(format!("Failed to parse pipeline: {e}")))?;
    validate_pipeline(&def)?;
    Ok(def)
}

fn validate_pipeline(def: &PipelineDefinition) -> Result<(), AppError> {
    // Check duplicate block IDs
    let mut seen = HashSet::new();
    for block in &def.blocks {
        if !seen.insert(block.id) {
            return Err(AppError::Config(format!(
                "Duplicate block ID: {}",
                block.id
            )));
        }
    }

    // Check dangling connection references
    for conn in &def.connections {
        if !seen.contains(&conn.from) {
            return Err(AppError::Config(format!(
                "Connection references non-existent block: {}",
                conn.from
            )));
        }
        if !seen.contains(&conn.to) {
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

    // Check for cycles
    topological_layers(def)
        .map_err(|_| AppError::Config("Pipeline contains a cycle".to_string()))?;

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

// ---------------------------------------------------------------------------
// Execution engine
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline(
    def: &PipelineDefinition,
    config: &AppConfig,
    agent_configs: HashMap<String, (ProviderKind, crate::config::ProviderConfig, bool)>,
    client: reqwest::Client,
    cli_timeout_secs: u64,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let total_blocks = def.blocks.len();
    if total_blocks == 0 {
        let _ = progress_tx.send(ProgressEvent::AllDone);
        return Ok(());
    }

    // Build provider pool: key = (agent_name, session_key)
    let mut provider_pool: ProviderPool = HashMap::new();

    for block in &def.blocks {
        let session_key = block
            .session_id
            .clone()
            .unwrap_or_else(|| format!("__block_{}", block.id));
        let pool_key = (block.agent.clone(), session_key);
        if let std::collections::hash_map::Entry::Vacant(entry) = provider_pool.entry(pool_key) {
            if let Some((kind, cfg, _use_cli)) = agent_configs.get(&block.agent) {
                let p = provider::create_provider(
                    *kind,
                    cfg,
                    client.clone(),
                    config.default_max_tokens,
                    config.max_history_messages,
                    cli_timeout_secs,
                );
                entry.insert(Arc::new(Mutex::new(p)));
            }
        }
    }

    // Build adjacency structures
    let mut in_degree: HashMap<BlockId, usize> = def.blocks.iter().map(|b| (b.id, 0)).collect();
    let mut downstream: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
    for conn in &def.connections {
        *in_degree.entry(conn.to).or_default() += 1;
        downstream.entry(conn.from).or_default().push(conn.to);
    }

    let mut previous_terminal_outputs = String::new();

    for iteration in 1..=def.iterations {
        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        let mut current_in_degree = in_degree.clone();
        let mut failed_blocks: HashSet<BlockId> = HashSet::new();
        let mut block_outputs: HashMap<BlockId, String> = HashMap::new();
        let mut completed = 0usize;

        // Seed ready queue with root blocks
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

        let mut tasks: tokio::task::JoinSet<(BlockId, Result<String, String>)> =
            tokio::task::JoinSet::new();

        while completed < total_blocks {
            tokio::select! {
                Some(block_id) = ready_rx.recv() => {
                    if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }

                    let block = match def.blocks.iter().find(|b| b.id == block_id) {
                        Some(b) => b,
                        None => { completed += 1; continue; }
                    };
                    let agent_name = block.agent.clone();
                    let label = if block.name.trim().is_empty() {
                        format!("Block {} ({})", block_id, agent_name)
                    } else {
                        block.name.clone()
                    };

                    // Check for failed upstream
                    let failed_upstream: Vec<BlockId> = upstream_of(def, block_id)
                        .into_iter()
                        .filter(|u| failed_blocks.contains(u))
                        .collect();

                    if !failed_upstream.is_empty() {
                        let reason = format!("upstream Block {} failed", failed_upstream[0]);
                        let _ = progress_tx.send(ProgressEvent::BlockSkipped {
                            block_id,
                            agent_name,
                            label,
                            iteration,
                            reason,
                        });
                        failed_blocks.insert(block_id);
                        // Signal downstream
                        if let Some(children) = downstream.get(&block_id) {
                            for &child in children {
                                let deg = current_in_degree.get_mut(&child).unwrap();
                                *deg -= 1;
                                if *deg == 0 {
                                    let _ = ready_tx.send(child);
                                }
                            }
                        }
                        completed += 1;
                        continue;
                    }

                    // Build message
                    let is_root = upstream_of(def, block_id).is_empty();
                    let use_cli = agent_configs.get(&agent_name).map(|(_, _, cli)| *cli).unwrap_or(false);

                    let message = if is_root && iteration == 1 {
                        if block.prompt.is_empty() {
                            def.initial_prompt.clone()
                        } else {
                            format!("{}\n\n{}", block.prompt, def.initial_prompt)
                        }
                    } else if is_root {
                        let base = if block.prompt.is_empty() {
                            def.initial_prompt.clone()
                        } else {
                            block.prompt.clone()
                        };
                        format!("{base}\n\n--- Previous iteration outputs ---\n{previous_terminal_outputs}")
                    } else {
                        // Non-root: gather upstream outputs
                        let upstream_ids = upstream_of(def, block_id);
                        let mut upstream_content = String::new();
                        for uid in &upstream_ids {
                            if let Some(content) = block_outputs.get(uid) {
                                if !upstream_content.is_empty() {
                                    upstream_content.push_str("\n\n---\n\n");
                                }
                                upstream_content.push_str(content);
                            }
                        }
                        let prefix = if block.prompt.is_empty() {
                            String::new()
                        } else {
                            format!("{}\n\n", block.prompt)
                        };
                        if use_cli {
                            // For CLI, reference file paths
                            let mut file_refs = String::new();
                            for uid in &upstream_ids {
                                if let Some(ub) = def.blocks.iter().find(|b| b.id == *uid) {
                                    let ub_name_key = if ub.name.trim().is_empty() {
                                        format!("block{}", uid)
                                    } else {
                                        format!("{}_b{}", OutputManager::sanitize_session_name(&ub.name), uid)
                                    };
                                    let sanitized = OutputManager::sanitize_session_name(&ub.agent);
                                    let fname = format!("{}_{}_iter{}.md", ub_name_key, sanitized, iteration);
                                    let fpath = output.run_dir().join(&fname);
                                    if fpath.exists() {
                                        file_refs.push_str(&format!("- {}\n", fpath.display()));
                                    }
                                }
                            }
                            format!("{prefix}Read these upstream output files:\n{file_refs}\nRead each file before responding.")
                        } else {
                            format!("{prefix}--- Upstream outputs ---\n{upstream_content}")
                        }
                    };

                    // Get provider from pool
                    let session_key = block
                        .session_id
                        .clone()
                        .unwrap_or_else(|| format!("__block_{}", block.id));
                    let pool_key = (agent_name.clone(), session_key);
                    let provider_arc = match provider_pool.get(&pool_key) {
                        Some(p) => p.clone(),
                        None => {
                            let _ = progress_tx.send(ProgressEvent::BlockError {
                                block_id,
                                agent_name,
                                label: label.clone(),
                                iteration,
                                error: "No provider available".into(),
                                details: None,
                            });
                            failed_blocks.insert(block_id);
                            if let Some(children) = downstream.get(&block_id) {
                                for &child in children {
                                    let deg = current_in_degree.get_mut(&child).unwrap();
                                    *deg -= 1;
                                    if *deg == 0 {
                                        let _ = ready_tx.send(child);
                                    }
                                }
                            }
                            completed += 1;
                            continue;
                        }
                    };

                    let _ = progress_tx.send(ProgressEvent::BlockStarted {
                        block_id,
                        agent_name: agent_name.clone(),
                        label: label.clone(),
                        iteration,
                    });

                    let ptx = progress_tx.clone();
                    let cancel_clone = cancel.clone();
                    let output_run_dir = output.run_dir().to_path_buf();
                    let file_key = OutputManager::sanitize_session_name(&agent_name);
                    let block_name_key = if block.name.trim().is_empty() {
                        format!("block{}", block_id)
                    } else {
                        format!("{}_b{}", OutputManager::sanitize_session_name(&block.name), block_id)
                    };

                    tasks.spawn(async move {
                        let mut guard = provider_arc.lock().await;

                        // Wire live-log channel
                        let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
                        guard.set_live_log_sender(Some(live_tx));

                        let bid = block_id;
                        let an = agent_name.clone();
                        let it = iteration;
                        let ptx2 = ptx.clone();
                        let live_forward = tokio::spawn(async move {
                            while let Some(line) = live_rx.recv().await {
                                let _ = ptx2.send(ProgressEvent::BlockLog {
                                    block_id: bid,
                                    agent_name: an.clone(),
                                    iteration: it,
                                    message: format!("CLI {line}"),
                                });
                            }
                        });

                        let result = tokio::select! {
                            res = guard.send(&message) => Some(res),
                            _ = wait_for_cancel(&cancel_clone) => None
                        };

                        guard.set_live_log_sender(None);
                        drop(guard);
                        let _ = live_forward.await;

                        match result {
                            None => {
                                // Cancelled
                                (block_id, Err("Cancelled".to_string()))
                            }
                            Some(Ok(resp)) => {
                                // Forward debug logs
                                for log in &resp.debug_logs {
                                    let _ = ptx.send(ProgressEvent::BlockLog {
                                        block_id,
                                        agent_name: agent_name.clone(),
                                        iteration,
                                        message: log.clone(),
                                    });
                                }
                                // Write output
                                let filename = format!("{}_{}_iter{}.md", block_name_key, file_key, iteration);
                                let path = output_run_dir.join(&filename);
                                if let Err(e) = std::fs::write(&path, &resp.content) {
                                    let error = format!("Failed to write output: {e}");
                                    let _ = ptx.send(ProgressEvent::BlockError {
                                        block_id,
                                        agent_name,
                                        label,
                                        iteration,
                                        error: error.clone(),
                                        details: Some(error.clone()),
                                    });
                                    (block_id, Err(error))
                                } else {
                                    let _ = ptx.send(ProgressEvent::BlockFinished {
                                        block_id,
                                        agent_name,
                                        label,
                                        iteration,
                                    });
                                    (block_id, Ok(resp.content))
                                }
                            }
                            Some(Err(e)) => {
                                let _ = ptx.send(ProgressEvent::BlockError {
                                    block_id,
                                    agent_name,
                                    label,
                                    iteration,
                                    error: e.to_string(),
                                    details: Some(e.to_string()),
                                });
                                (block_id, Err(e.to_string()))
                            }
                        }
                    });
                }
                Some(result) = tasks.join_next() => {
                    completed += 1;
                    if let Ok((block_id, outcome)) = result {
                        match outcome {
                            Ok(content) => {
                                block_outputs.insert(block_id, content);
                            }
                            Err(_) => {
                                failed_blocks.insert(block_id);
                            }
                        }
                        // Signal downstream
                        if let Some(children) = downstream.get(&block_id) {
                            for &child in children {
                                let deg = current_in_degree.get_mut(&child).unwrap();
                                *deg -= 1;
                                if *deg == 0 {
                                    let _ = ready_tx.send(child);
                                }
                            }
                        }
                    }
                }
                else => break,
            }
        }

        let _ = progress_tx.send(ProgressEvent::IterationComplete { iteration });

        // Collect terminal block outputs for next iteration
        let terminals = terminal_blocks(def);
        previous_terminal_outputs.clear();
        for &tid in &terminals {
            if let Some(content) = block_outputs.get(&tid) {
                if !previous_terminal_outputs.is_empty() {
                    previous_terminal_outputs.push_str("\n\n---\n\n");
                }
                previous_terminal_outputs.push_str(content);
            }
        }
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn block(id: BlockId, col: u16, row: u16) -> PipelineBlock {
        PipelineBlock {
            id,
            name: format!("Block#{id}"),
            agent: "Claude".into(),
            prompt: format!("block {id}"),
            session_id: None,
            position: (col, row),
        }
    }

    fn conn(from: BlockId, to: BlockId) -> PipelineConnection {
        PipelineConnection { from, to }
    }

    fn def_with(blocks: Vec<PipelineBlock>, connections: Vec<PipelineConnection>) -> PipelineDefinition {
        PipelineDefinition {
            initial_prompt: "test".into(),
            iterations: 1,
            blocks,
            connections,
        }
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
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![],
        );
        let roots = root_blocks(&d);
        let terms = terminal_blocks(&d);
        assert!(roots.contains(&1) && roots.contains(&2));
        assert!(terms.contains(&1) && terms.contains(&2));
    }

    #[test]
    fn root_and_terminal_diamond() {
        // 1 → 2, 1 → 3, 2 → 4, 3 → 4
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 1, 1), block(4, 2, 0)],
            vec![conn(1, 2), conn(1, 3), conn(2, 4), conn(3, 4)],
        );
        assert_eq!(root_blocks(&d), vec![1]);
        assert_eq!(terminal_blocks(&d), vec![4]);
    }

    // -- upstream_of --

    #[test]
    fn upstream_of_returns_direct_predecessors() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 1, 1), block(4, 2, 0)],
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
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 1, 1), block(4, 2, 0)],
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
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 1, 1), block(4, 1, 2)],
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
        let d = def_with(
            vec![block(1, 0, 0)],
            vec![conn(1, 1)],
        );
        assert!(topological_layers(&d).is_err());
    }

    // -- would_create_cycle --

    #[test]
    fn would_create_cycle_self_edge() {
        let d = def_with(vec![block(1, 0, 0)], vec![]);
        assert!(would_create_cycle(&d, 1, 1));
    }

    #[test]
    fn would_create_cycle_back_edge() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        // Adding 3→1 would create 1→2→3→1
        assert!(would_create_cycle(&d, 3, 1));
    }

    #[test]
    fn would_create_cycle_valid_forward_edge() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        // Adding 1→3 (skip-edge) is valid
        assert!(!would_create_cycle(&d, 1, 3));
    }

    #[test]
    fn would_create_cycle_diamond_no_cycle() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 1, 1), block(4, 2, 0)],
            vec![conn(1, 2), conn(1, 3), conn(2, 4)],
        );
        // Adding 3→4 is valid (diamond)
        assert!(!would_create_cycle(&d, 3, 4));
    }

    // -- next_free_position --

    #[test]
    fn next_free_position_empty() {
        let d = PipelineDefinition::default();
        assert_eq!(next_free_position(&d), (0, 0));
    }

    #[test]
    fn next_free_position_fills_gaps() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 2, 0)],
            vec![],
        );
        // (1, 0) is the first gap
        assert_eq!(next_free_position(&d), (1, 0));
    }

    #[test]
    fn next_free_position_wraps_to_next_row() {
        // Fill entire row 0 cols 0..100? That's too many. Let's check a smaller scenario.
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![],
        );
        assert_eq!(next_free_position(&d), (2, 0));
    }

    // -- save/load roundtrip --

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let def = def_with(
            vec![block(1, 0, 0), block(2, 1, 0)],
            vec![conn(1, 2)],
        );
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
}
