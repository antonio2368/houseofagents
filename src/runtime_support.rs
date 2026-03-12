use crate::config::AgentConfig;
use crate::provider::{self, ProviderKind};
use std::collections::HashMap;

pub(crate) fn detect_cli(name: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    find_executable(name, std::env::split_paths(&paths))
}

pub(crate) fn find_executable(name: &str, dirs: impl Iterator<Item = std::path::PathBuf>) -> bool {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    dirs.into_iter().any(|dir| {
        let candidate = dir.join(name);
        candidate
            .metadata()
            .map(|m| {
                if !m.is_file() {
                    return false;
                }
                #[cfg(unix)]
                {
                    m.permissions().mode() & 0o111 != 0
                }
                #[cfg(not(unix))]
                {
                    true
                }
            })
            .unwrap_or(false)
    })
}

pub(crate) fn detect_cli_availability() -> HashMap<ProviderKind, bool> {
    let mut cli_available = HashMap::new();
    cli_available.insert(ProviderKind::Anthropic, detect_cli("claude"));
    cli_available.insert(ProviderKind::OpenAI, detect_cli("codex"));
    cli_available.insert(ProviderKind::Gemini, detect_cli("gemini"));
    cli_available
}

pub(crate) fn compute_session_overrides(
    agents: &[AgentConfig],
    cli_available: &HashMap<ProviderKind, bool>,
) -> HashMap<String, AgentConfig> {
    let mut session_overrides = HashMap::new();
    for agent in agents {
        let has_key = !agent.api_key.is_empty();
        let has_cli = cli_available.get(&agent.provider).copied().unwrap_or(false);
        if !has_key && has_cli {
            let mut override_agent = agent.clone();
            override_agent.use_cli = true;
            session_overrides.insert(agent.name.clone(), override_agent);
        }
    }
    session_overrides
}

pub(crate) fn resolve_agent_config<'a>(
    name: &str,
    session_overrides: &'a HashMap<String, AgentConfig>,
    agents: &'a [AgentConfig],
) -> Option<&'a AgentConfig> {
    session_overrides
        .get(name)
        .or_else(|| agents.iter().find(|a| a.name == name))
}

pub(crate) fn validate_agent_runtime(
    cli_available: &HashMap<ProviderKind, bool>,
    agent_label: &str,
    agent_config: &AgentConfig,
) -> Result<(), String> {
    if agent_config.use_cli
        && !cli_available
            .get(&agent_config.provider)
            .copied()
            .unwrap_or(false)
    {
        return Err(format!(
            "{agent_label}: {} CLI is not installed",
            agent_config.provider.display_name()
        ));
    }

    if !agent_config.use_cli && agent_config.api_key.trim().is_empty() {
        return Err(format!("{agent_label} API key is missing"));
    }

    provider::validate_effort_config(
        agent_config.provider,
        agent_config.use_cli,
        agent_config.reasoning_effort.as_deref(),
        agent_config.thinking_effort.as_deref(),
    )
    .map_err(|message| format!("{agent_label}: {message}"))
}

pub(crate) fn resolve_selected_agent_configs(
    agent_names: &[String],
    session_overrides: &HashMap<String, AgentConfig>,
    agents: &[AgentConfig],
    cli_available: &HashMap<ProviderKind, bool>,
) -> Result<Vec<AgentConfig>, String> {
    let mut resolved = Vec::with_capacity(agent_names.len());
    for name in agent_names {
        let agent_config = resolve_agent_config(name, session_overrides, agents)
            .cloned()
            .ok_or_else(|| format!("{name} is not configured"))?;
        validate_agent_runtime(cli_available, name, &agent_config)?;
        resolved.push(agent_config);
    }
    Ok(resolved)
}

pub(crate) fn effective_concurrency(runs: u32, concurrency: u32) -> u32 {
    match concurrency {
        0 => runs.max(1),
        value => value.min(runs).max(1),
    }
}

/// Build the pipeline agent config map from a pipeline definition.
///
/// Iterates pipeline blocks, deduplicates agents by name, and applies
/// session overrides before producing the map expected by `run_pipeline`.
pub(crate) fn build_pipeline_agent_configs(
    pipeline_def: &crate::execution::pipeline::PipelineDefinition,
    agents: &[AgentConfig],
    session_overrides: &HashMap<String, AgentConfig>,
) -> HashMap<String, (ProviderKind, crate::config::ProviderConfig, bool)> {
    let mut agent_configs = HashMap::new();
    for block in &pipeline_def.blocks {
        for agent_name in &block.agents {
            if agent_configs.contains_key(agent_name) {
                continue;
            }
            let agent_cfg = resolve_agent_config(agent_name, session_overrides, agents);
            if let Some(agent_cfg) = agent_cfg {
                agent_configs.insert(
                    agent_name.clone(),
                    (
                        agent_cfg.provider,
                        agent_cfg.to_provider_config(),
                        agent_cfg.use_cli,
                    ),
                );
            }
        }
    }
    agent_configs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_concurrency_zero_means_unlimited() {
        assert_eq!(effective_concurrency(5, 0), 5);
    }

    #[test]
    fn effective_concurrency_clamps_to_runs() {
        assert_eq!(effective_concurrency(3, 10), 3);
    }

    #[test]
    fn effective_concurrency_min_one() {
        assert_eq!(effective_concurrency(0, 0), 1);
    }

    #[cfg(unix)]
    #[test]
    fn find_executable_rejects_non_executable_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let bin_path = dir.path().join("fakecli");
        std::fs::write(&bin_path, "").unwrap();
        std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let result = find_executable("fakecli", std::iter::once(dir.path().to_path_buf()));
        assert!(!result, "non-executable file should not be detected as CLI");
    }

    #[cfg(unix)]
    #[test]
    fn find_executable_accepts_executable_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let bin_path = dir.path().join("fakecli2");
        std::fs::write(&bin_path, "").unwrap();
        std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let result = find_executable("fakecli2", std::iter::once(dir.path().to_path_buf()));
        assert!(result, "executable file should be detected as CLI");
    }

    #[test]
    fn compute_session_overrides_auto_switches_to_cli() {
        let agents = vec![AgentConfig {
            name: "Claude".into(),
            provider: ProviderKind::Anthropic,
            api_key: String::new(),
            model: String::new(),
            reasoning_effort: None,
            thinking_effort: None,
            use_cli: false,
            cli_print_mode: true,
            extra_cli_args: String::new(),
        }];
        let mut cli_available = HashMap::new();
        cli_available.insert(ProviderKind::Anthropic, true);
        let overrides = compute_session_overrides(&agents, &cli_available);
        assert!(overrides.get("Claude").unwrap().use_cli);
    }

    #[test]
    fn resolve_agent_config_prefers_override() {
        let agents = vec![AgentConfig {
            name: "Claude".into(),
            provider: ProviderKind::Anthropic,
            api_key: "key".into(),
            model: "original".into(),
            reasoning_effort: None,
            thinking_effort: None,
            use_cli: false,
            cli_print_mode: true,
            extra_cli_args: String::new(),
        }];
        let mut overrides = HashMap::new();
        let mut override_agent = agents[0].clone();
        override_agent.model = "overridden".into();
        overrides.insert("Claude".into(), override_agent);
        let config = resolve_agent_config("Claude", &overrides, &agents).unwrap();
        assert_eq!(config.model, "overridden");
    }

    #[test]
    fn validate_agent_runtime_rejects_missing_cli() {
        let cli_available = HashMap::new();
        let agent = AgentConfig {
            name: "Claude".into(),
            provider: ProviderKind::Anthropic,
            api_key: String::new(),
            model: String::new(),
            reasoning_effort: None,
            thinking_effort: None,
            use_cli: true,
            cli_print_mode: true,
            extra_cli_args: String::new(),
        };
        let err = validate_agent_runtime(&cli_available, "Claude", &agent).unwrap_err();
        assert!(err.contains("CLI is not installed"));
    }

    #[test]
    fn validate_agent_runtime_rejects_missing_api_key() {
        let cli_available = HashMap::new();
        let agent = AgentConfig {
            name: "Claude".into(),
            provider: ProviderKind::Anthropic,
            api_key: String::new(),
            model: String::new(),
            reasoning_effort: None,
            thinking_effort: None,
            use_cli: false,
            cli_print_mode: true,
            extra_cli_args: String::new(),
        };
        let err = validate_agent_runtime(&cli_available, "Claude", &agent).unwrap_err();
        assert!(err.contains("API key is missing"));
    }
}
