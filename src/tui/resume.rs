use super::*;
use crate::post_run;

pub(super) fn find_last_complete_iteration_for_agents(
    run_dir: &std::path::Path,
    agents: &[String],
) -> Option<u32> {
    use std::collections::{HashMap, HashSet};

    if agents.is_empty() {
        return None;
    }

    let agent_keys: Vec<String> = agents
        .iter()
        .map(|n| OutputManager::sanitize_session_name(n))
        .collect();
    let mut iter_to_agents: HashMap<u32, HashSet<String>> = HashMap::new();
    let entries = std::fs::read_dir(run_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        for key in &agent_keys {
            if let Some(iter) = post_run::parse_agent_iteration_filename(&name, key) {
                iter_to_agents.entry(iter).or_default().insert(key.clone());
            }
        }
    }

    iter_to_agents
        .into_iter()
        .filter(|(_, present_agents)| present_agents.len() == agents.len())
        .map(|(iter, _)| iter)
        .max()
}

pub(super) fn find_latest_compatible_run(
    base_dir: &std::path::Path,
    mode: ExecutionMode,
    agents: &[String],
    keep_session: bool,
) -> Option<std::path::PathBuf> {
    let dirs = OutputManager::scan_run_dirs(base_dir).ok()?;

    for run_dir in dirs {
        if OutputManager::is_batch_root(&run_dir) {
            continue;
        }
        if !run_dir_matches_mode_and_agents(&run_dir, mode, agents, keep_session) {
            continue;
        }
        if find_last_complete_iteration_for_agents(&run_dir, agents).is_some() {
            return Some(run_dir);
        }
    }
    None
}

pub(super) fn session_matches_resume(
    session: &AgentSessionInfo,
    mode: ExecutionMode,
    agents: &[String],
    keep_session: bool,
) -> bool {
    if session.mode != mode {
        return false;
    }
    if session.keep_session != keep_session {
        return false;
    }

    match mode {
        ExecutionMode::Relay => session.agents == agents,
        ExecutionMode::Swarm => {
            if session.agents.len() != agents.len() {
                return false;
            }
            let stored: std::collections::HashSet<&String> = session.agents.iter().collect();
            let requested: std::collections::HashSet<&String> = agents.iter().collect();
            stored == requested
        }
        ExecutionMode::Pipeline => session.agents == agents,
    }
}

#[cfg(test)]
pub(super) fn validate_resume_run(
    run_dir: &std::path::Path,
    mode: ExecutionMode,
    agents: &[String],
    keep_session: bool,
) -> Result<(), String> {
    let session = OutputManager::read_agent_session_info(run_dir)
        .map_err(|e| format!("Failed to read session metadata: {e}"))?;
    if session_matches_resume(&session, mode, agents, keep_session) {
        Ok(())
    } else {
        Err(format!(
            "Previous run at {} does not exactly match the selected {} configuration",
            run_dir.display(),
            mode
        ))
    }
}

pub(super) fn run_dir_matches_mode_and_agents(
    run_dir: &std::path::Path,
    mode: ExecutionMode,
    agents: &[String],
    keep_session: bool,
) -> bool {
    OutputManager::read_agent_session_info(run_dir)
        .map(|session| session_matches_resume(&session, mode, agents, keep_session))
        .unwrap_or(false)
}
