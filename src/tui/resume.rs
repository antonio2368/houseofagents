use super::*;

pub(super) async fn find_last_iteration_async(
    run_dir: &std::path::Path,
    agent_keys: &[String],
) -> Option<u32> {
    let mut max_iter: Option<u32> = None;
    let mut entries = tokio::fs::read_dir(run_dir).await.ok()?;

    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let name = entry.file_name().to_string_lossy().to_string();
                if agent_keys.is_empty() {
                    if let Some(iter) = parse_pipeline_iteration_filename(&name) {
                        max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
                    }
                } else {
                    for key in agent_keys {
                        if let Some(iter) = parse_agent_iteration_filename(&name, key) {
                            max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    max_iter
}

pub(super) fn find_last_iteration(run_dir: &std::path::Path, agent_keys: &[String]) -> Option<u32> {
    let mut max_iter: Option<u32> = None;
    let entries = std::fs::read_dir(run_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if agent_keys.is_empty() {
            if let Some(iter) = parse_pipeline_iteration_filename(&name) {
                max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
            }
        } else {
            for key in agent_keys {
                if let Some(iter) = parse_agent_iteration_filename(&name, key) {
                    max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
                }
            }
        }
    }
    max_iter
}

pub(super) fn find_last_complete_iteration_for_agents(
    run_dir: &std::path::Path,
    agents: &[String],
) -> Option<u32> {
    use std::collections::{HashMap, HashSet};

    if agents.is_empty() {
        return None;
    }

    let agent_keys: Vec<String> = agents.iter().map(|n| App::agent_file_key(n)).collect();
    let mut iter_to_agents: HashMap<u32, HashSet<String>> = HashMap::new();
    let entries = std::fs::read_dir(run_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        for key in &agent_keys {
            if let Some(iter) = parse_agent_iteration_filename(&name, key) {
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

pub(super) fn parse_agent_iteration_filename(name: &str, agent_key: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let prefix = format!("{agent_key}_iter");
    if !name.starts_with(&prefix) {
        return None;
    }
    let iter_str = name.trim_end_matches(".md").strip_prefix(&prefix)?;
    iter_str.parse::<u32>().ok()
}

/// Parse iteration from a pipeline block output filename.
/// Matches both named blocks (`{name}_b{id}_{agent}_iter{n}.md`)
/// and unnamed blocks (`block{id}_{agent}_iter{n}.md`).
pub(super) fn parse_pipeline_iteration_filename(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let stem = name.trim_end_matches(".md");

    // Search right-to-left so block names containing "_b" (for example
    // "web_builder") still detect the actual "_b{id}_" marker.
    let mut search_end = stem.len();
    while let Some(rel) = stem[..search_end].rfind("_b") {
        let after_b = &stem[rel + 2..];
        if let Some(end_of_id) = after_b.find('_') {
            if after_b[..end_of_id].parse::<u32>().is_ok() {
                return parse_iteration_from_filename(name);
            }
        }
        search_end = rel;
    }

    if let Some(rest) = stem.strip_prefix("block") {
        if let Some(underscore) = rest.find('_') {
            if rest[..underscore].parse::<u32>().is_ok() {
                return parse_iteration_from_filename(name);
            }
        }
    }

    None
}

pub(super) fn parse_iteration_from_filename(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let stem = name.trim_end_matches(".md");
    let iter_pos = stem.rfind("_iter")?;
    let iter_str = &stem[iter_pos + 5..];
    iter_str.parse::<u32>().ok()
}
