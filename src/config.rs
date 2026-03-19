use crate::error::AppError;
use crate::provider::ProviderKind;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub db_path: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default = "default_max_recall")]
    pub max_recall: usize,
    #[serde(default = "default_max_recall_bytes")]
    pub max_recall_bytes: usize,
    #[serde(default)]
    pub extraction_agent: String,
    #[serde(default)]
    pub disable_extraction: bool,
    #[serde(default = "default_observation_ttl_days")]
    pub observation_ttl_days: u32,
    #[serde(default = "default_summary_ttl_days")]
    pub summary_ttl_days: u32,
    #[serde(default = "default_stale_permanent_days")]
    pub stale_permanent_days: u32,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            db_path: String::new(),
            project_id: String::new(),
            max_recall: default_max_recall(),
            max_recall_bytes: default_max_recall_bytes(),
            extraction_agent: String::new(),
            disable_extraction: false,
            observation_ttl_days: default_observation_ttl_days(),
            summary_ttl_days: default_summary_ttl_days(),
            stale_permanent_days: default_stale_permanent_days(),
        }
    }
}

fn default_max_recall() -> usize {
    20
}

fn default_max_recall_bytes() -> usize {
    16384
}

fn default_observation_ttl_days() -> u32 {
    120
}

fn default_summary_ttl_days() -> u32 {
    180
}

fn default_stale_permanent_days() -> u32 {
    365
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub output_dir: String,
    #[serde(default = "default_max_tokens")]
    pub default_max_tokens: u32,
    #[serde(default = "default_max_history_messages")]
    pub max_history_messages: usize,
    #[serde(default = "default_http_timeout_seconds")]
    pub http_timeout_seconds: u64,
    #[serde(default = "default_model_fetch_timeout_seconds")]
    pub model_fetch_timeout_seconds: u64,
    #[serde(default = "default_cli_timeout_seconds")]
    pub cli_timeout_seconds: u64,
    #[serde(default = "default_max_history_bytes")]
    pub max_history_bytes: usize,
    /// Max concurrent pipeline blocks (0 = unlimited). Controls how many blocks execute in parallel.
    #[serde(default)]
    pub pipeline_block_concurrency: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic_provider: Option<String>,
    #[serde(default)]
    pub memory: MemoryConfig,
    #[serde(default)]
    pub agents: Vec<AgentConfig>,
    /// Legacy field — kept only for deserialization/migration. Not serialized.
    #[serde(default, skip_serializing)]
    pub providers: HashMap<String, ProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub name: String,
    pub provider: ProviderKind,
    pub api_key: String,
    pub model: String,
    /// OpenAI reasoning effort: "low", "medium", "high", "xhigh" (for o-series / gpt-5 models)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Thinking effort: "low", "medium", "high", "max" (max for Claude Opus 4.6 only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_effort: Option<String>,
    /// Use CLI tool instead of API for this provider
    #[serde(default)]
    pub use_cli: bool,
    /// Use print mode (-p) instead of agent mode for Anthropic CLI
    #[serde(default = "default_true")]
    pub cli_print_mode: bool,
    /// Extra CLI arguments parsed with shell-style quoting at runtime
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub extra_cli_args: String,
}

impl AgentConfig {
    /// Create an AgentConfig from a ProviderConfig (used for legacy migration)
    pub fn from_provider_config(name: String, provider: ProviderKind, pc: &ProviderConfig) -> Self {
        Self {
            name,
            provider,
            api_key: pc.api_key.clone(),
            model: pc.model.clone(),
            reasoning_effort: pc.reasoning_effort.clone(),
            thinking_effort: pc.thinking_effort.clone(),
            use_cli: pc.use_cli,
            cli_print_mode: pc.cli_print_mode,
            extra_cli_args: pc.extra_cli_args.clone(),
        }
    }

    /// Convert to ProviderConfig (for diagnostics interop and create_provider)
    pub fn to_provider_config(&self) -> ProviderConfig {
        ProviderConfig {
            api_key: self.api_key.clone(),
            model: self.model.clone(),
            reasoning_effort: self.reasoning_effort.clone(),
            thinking_effort: self.thinking_effort.clone(),
            use_cli: self.use_cli,
            cli_print_mode: self.cli_print_mode,
            extra_cli_args: self.extra_cli_args.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub api_key: String,
    pub model: String,
    /// OpenAI reasoning effort: "low", "medium", "high", "xhigh" (for o-series / gpt-5 models)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Thinking effort: "low", "medium", "high", "max" (max for Claude Opus 4.6 only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_effort: Option<String>,
    /// Use CLI tool instead of API for this provider
    #[serde(default)]
    pub use_cli: bool,
    /// Use print mode (-p) instead of agent mode for Anthropic CLI
    #[serde(default = "default_true")]
    pub cli_print_mode: bool,
    /// Extra CLI arguments parsed with shell-style quoting at runtime
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub extra_cli_args: String,
}

fn default_max_tokens() -> u32 {
    4096
}

fn default_max_history_messages() -> usize {
    50
}

fn default_http_timeout_seconds() -> u64 {
    120
}

fn default_model_fetch_timeout_seconds() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

fn default_cli_timeout_seconds() -> u64 {
    600
}

fn default_max_history_bytes() -> usize {
    102400
}

impl AppConfig {
    pub fn load() -> Result<Self, AppError> {
        Self::load_with_override(None)
    }

    pub fn load_with_override(path_override: Option<&str>) -> Result<Self, AppError> {
        let path = match path_override {
            Some(path) => PathBuf::from(path),
            None => Self::config_path()?,
        };

        if !path.exists() {
            return Err(AppError::Config(format!(
                "Config file not found at {}. Initialize one with `houseofagents --init-config`.",
                path.display()
            )));
        }
        let content = std::fs::read_to_string(&path)
            .map_err(|e| AppError::Config(format!("Failed to read config: {e}")))?;
        let mut config: AppConfig = toml::from_str(&content)
            .map_err(|e| AppError::Config(format!("Failed to parse config: {e}")))?;

        // Legacy migration: convert [providers.*] to [[agents]]
        if config.agents.is_empty() && !config.providers.is_empty() {
            config.migrate_providers_to_agents();
        }

        // Legacy migration: if diagnostic_provider contains a provider key like "openai",
        // try to find a matching agent by provider kind and replace with that agent's name.
        // Also normalize case to match the actual agent name.
        if let Some(ref dp) = config.diagnostic_provider {
            if ProviderKind::from_selector(dp).is_some()
                && !config
                    .agents
                    .iter()
                    .any(|a| a.name.eq_ignore_ascii_case(dp))
            {
                let kind = ProviderKind::from_selector(dp).unwrap();
                if let Some(agent) = config.agents.iter().find(|a| a.provider == kind) {
                    config.diagnostic_provider = Some(agent.name.clone());
                }
            } else if let Some(agent) = config
                .agents
                .iter()
                .find(|a| a.name.eq_ignore_ascii_case(dp))
            {
                // Normalize case to match the actual agent name
                if agent.name != *dp {
                    config.diagnostic_provider = Some(agent.name.clone());
                }
            }
        }

        config.validate_agents()?;
        Ok(config)
    }

    /// Migrate legacy `[providers.*]` HashMap into `[[agents]]` Vec.
    fn migrate_providers_to_agents(&mut self) {
        // Use a fixed order for deterministic migration
        let order = [
            ProviderKind::Anthropic,
            ProviderKind::OpenAI,
            ProviderKind::Gemini,
        ];
        for kind in &order {
            let key = kind.config_key();
            if let Some(pc) = self.providers.get(key) {
                self.agents.push(AgentConfig::from_provider_config(
                    kind.display_name().to_string(),
                    *kind,
                    pc,
                ));
            }
        }
        self.providers.clear();
    }

    /// Validate agent configs: unique names, non-empty, valid sanitized names
    fn validate_agents(&self) -> Result<(), AppError> {
        let mut seen_names = std::collections::HashSet::new();
        let mut seen_sanitized = std::collections::HashSet::new();

        for agent in &self.agents {
            if agent.name.trim().is_empty() {
                return Err(AppError::Config("Agent name cannot be empty".into()));
            }

            let lower = agent.name.to_lowercase();
            if !seen_names.insert(lower) {
                return Err(AppError::Config(format!(
                    "Duplicate agent name (case-insensitive): '{}'",
                    agent.name
                )));
            }

            let sanitized =
                crate::output::OutputManager::sanitize_session_name(&agent.name).to_lowercase();
            if !seen_sanitized.insert(sanitized.clone()) {
                return Err(AppError::Config(format!(
                    "Agents '{}' would produce duplicate filenames after sanitization",
                    agent.name
                )));
            }
        }
        Ok(())
    }

    pub fn config_path() -> Result<PathBuf, AppError> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| AppError::Config("Cannot determine config directory".into()))?;
        Ok(config_dir.join("houseofagents").join("config.toml"))
    }

    pub fn save(&self) -> Result<(), AppError> {
        self.save_with_override(None)
    }

    pub fn save_with_override(&self, path_override: Option<&str>) -> Result<(), AppError> {
        let path = match path_override {
            Some(path) => PathBuf::from(path),
            None => Self::config_path()?,
        };

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| AppError::Config(format!("Failed to create config dir: {e}")))?;
        }
        let content = toml::to_string_pretty(self)
            .map_err(|e| AppError::Config(format!("Failed to serialize config: {e}")))?;
        std::fs::write(&path, content)
            .map_err(|e| AppError::Config(format!("Failed to write config: {e}")))?;
        Ok(())
    }

    pub fn write_template_with_override(
        path_override: Option<&str>,
        force: bool,
    ) -> Result<PathBuf, AppError> {
        let path = match path_override {
            Some(path) => PathBuf::from(path),
            None => Self::config_path()?,
        };

        if path.exists() && !force {
            return Err(AppError::Config(format!(
                "Config already exists at {}. Use --force to overwrite.",
                path.display()
            )));
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| AppError::Config(format!("Failed to create config dir: {e}")))?;
        }

        const TEMPLATE: &str = r#"# House of Agents config
output_dir = "~/houseofagents-output"
default_max_tokens = 4096
# For values 4+, providers keep the first exchange plus the newest messages.
# For values 1-3, providers keep only the most recent messages up to the cap.
max_history_messages = 50
http_timeout_seconds = 120
model_fetch_timeout_seconds = 30
cli_timeout_seconds = 600
# Maximum total bytes of conversation history kept per provider session (default 100KB).
# Applied after message-count pruning as a second pass.
max_history_bytes = 102400
# Max concurrent pipeline blocks (0 = unlimited)
# pipeline_block_concurrency = 0

# Optional: set the diagnostic agent (agent name, e.g. "Claude")
# diagnostic_provider = "Claude"

# Cross-run memory system (SQLite+FTS5)
# [memory]
# enabled = true
# db_path = ""                  # empty = {output_dir}/memory.db
# project_id = ""               # empty = auto-detect from git remote / cwd
# max_recall = 20               # max memories injected per run
# max_recall_bytes = 16384      # max total bytes of recalled memory context
# extraction_agent = ""         # empty = first participating agent, then first configured
                                # Tip: stronger models produce higher-quality memories
# disable_extraction = false    # set true to skip post-run extraction
# observation_ttl_days = 120
# summary_ttl_days = 180
# stale_permanent_days = 365   # archive permanent memories after N days (0=disabled)

# Named agents — you can have multiple agents per provider
[[agents]]
name = "OpenAI"
provider = "openai"
api_key = ""
model = "gpt-5.3-codex"
reasoning_effort = "high"
use_cli = true
extra_cli_args = ""

[[agents]]
name = "Claude"
provider = "anthropic"
api_key = ""
model = "claude-opus-4-6"
thinking_effort = "high"
use_cli = true
cli_print_mode = true
extra_cli_args = ""

[[agents]]
name = "Gemini"
provider = "gemini"
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "medium"
use_cli = true
extra_cli_args = ""
"#;

        std::fs::write(&path, TEMPLATE)
            .map_err(|e| AppError::Config(format!("Failed to write config template: {e}")))?;
        Ok(path)
    }

    pub fn resolved_output_dir(&self) -> PathBuf {
        if self.output_dir.starts_with("~/") {
            if let Some(home) = dirs::home_dir() {
                home.join(&self.output_dir[2..])
            } else {
                PathBuf::from(&self.output_dir)
            }
        } else {
            PathBuf::from(&self.output_dir)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_config() -> AppConfig {
        AppConfig {
            output_dir: "/tmp/hoa".to_string(),
            default_max_tokens: 4096,
            max_history_messages: 50,
            http_timeout_seconds: 120,
            model_fetch_timeout_seconds: 30,
            cli_timeout_seconds: 600,
            max_history_bytes: 102400,
            pipeline_block_concurrency: 0,
            diagnostic_provider: None,
            memory: MemoryConfig::default(),
            agents: Vec::new(),
            providers: HashMap::new(),
        }
    }

    fn sample_agent(name: &str, provider: ProviderKind) -> AgentConfig {
        AgentConfig {
            name: name.to_string(),
            provider,
            api_key: String::new(),
            model: String::new(),
            reasoning_effort: None,
            thinking_effort: None,
            use_cli: false,
            cli_print_mode: true,
            extra_cli_args: String::new(),
        }
    }

    #[test]
    fn load_with_override_missing_file_returns_error() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let err = AppConfig::load_with_override(path.to_str()).expect_err("expected missing error");
        assert!(err.to_string().contains("Config file not found"));
    }

    #[test]
    fn load_with_override_invalid_toml_returns_error() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "not = [valid").expect("write");
        let err = AppConfig::load_with_override(path.to_str()).expect_err("expected parse failure");
        assert!(err.to_string().contains("Failed to parse config"));
    }

    #[test]
    fn load_with_override_valid_round_trip() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let mut cfg = sample_config();
        cfg.agents
            .push(sample_agent("Claude", ProviderKind::Anthropic));
        cfg.save_with_override(path.to_str()).expect("save");
        let loaded = AppConfig::load_with_override(path.to_str()).expect("load");
        assert_eq!(loaded.output_dir, cfg.output_dir);
        assert_eq!(loaded.default_max_tokens, cfg.default_max_tokens);
        assert_eq!(loaded.max_history_messages, cfg.max_history_messages);
        assert_eq!(loaded.http_timeout_seconds, cfg.http_timeout_seconds);
        assert_eq!(
            loaded.model_fetch_timeout_seconds,
            cfg.model_fetch_timeout_seconds
        );
        assert_eq!(loaded.cli_timeout_seconds, cfg.cli_timeout_seconds);
        assert_eq!(loaded.diagnostic_provider, cfg.diagnostic_provider);
        assert_eq!(loaded.agents.len(), 1);
        assert_eq!(loaded.agents[0].name, "Claude");
    }

    #[test]
    fn save_with_override_creates_parent_dirs() {
        let dir = tempdir().expect("tempdir");
        let nested = dir.path().join("a/b/c/config.toml");
        sample_config()
            .save_with_override(nested.to_str())
            .expect("save");
        assert!(nested.exists());
    }

    #[test]
    fn write_template_with_override_creates_file() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let written = AppConfig::write_template_with_override(path.to_str(), false).expect("write");
        assert_eq!(written, path);
        let body = std::fs::read_to_string(&written).expect("read");
        assert!(body.contains("output_dir"));
        assert!(body.contains("[[agents]]"));
        assert!(!body.contains("[diagnostics."));
        assert!(body.contains("# diagnostic_provider = \"Claude\""));
    }

    #[test]
    fn write_template_with_override_rejects_existing_without_force() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "existing").expect("write");
        let err = AppConfig::write_template_with_override(path.to_str(), false)
            .expect_err("should reject");
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn write_template_with_override_overwrites_with_force() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "existing").expect("write");
        AppConfig::write_template_with_override(path.to_str(), true).expect("write");
        let body = std::fs::read_to_string(path).expect("read");
        assert!(body.contains("House of Agents config"));
    }

    #[test]
    fn resolved_output_dir_plain_path() {
        let cfg = sample_config();
        assert_eq!(cfg.resolved_output_dir(), PathBuf::from("/tmp/hoa"));
    }

    #[test]
    fn resolved_output_dir_home_expansion() {
        let mut cfg = sample_config();
        cfg.output_dir = "~/hoa-out".to_string();
        let expected = dirs::home_dir()
            .map(|h| h.join("hoa-out"))
            .unwrap_or_else(|| PathBuf::from("~/hoa-out"));
        assert_eq!(cfg.resolved_output_dir(), expected);
    }

    #[test]
    fn legacy_providers_migration() {
        let body = r#"
output_dir = "/tmp/hoa"

[providers.openai]
api_key = "ok"
model = "gpt-5"

[providers.anthropic]
api_key = "ak"
model = "claude"
"#;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, body).expect("write");
        let cfg = AppConfig::load_with_override(path.to_str()).expect("load");
        assert!(cfg.providers.is_empty());
        assert_eq!(cfg.agents.len(), 2);
        // Should be in fixed order: Anthropic, OpenAI, Gemini
        assert_eq!(cfg.agents[0].name, "Claude");
        assert_eq!(cfg.agents[0].provider, ProviderKind::Anthropic);
        assert_eq!(cfg.agents[0].api_key, "ak");
        assert_eq!(cfg.agents[1].name, "OpenAI");
        assert_eq!(cfg.agents[1].provider, ProviderKind::OpenAI);
        assert_eq!(cfg.agents[1].api_key, "ok");
    }

    #[test]
    fn deserialize_uses_timeout_defaults_when_missing() {
        let body = r#"
output_dir = "/tmp/hoa"

[[agents]]
name = "OpenAI"
provider = "openai"
api_key = ""
model = "gpt-5"
"#;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, body).expect("write");
        let cfg = AppConfig::load_with_override(path.to_str()).expect("parse");
        assert_eq!(cfg.http_timeout_seconds, 120);
        assert_eq!(cfg.model_fetch_timeout_seconds, 30);
        assert_eq!(cfg.cli_timeout_seconds, 600);
        assert_eq!(cfg.default_max_tokens, 4096);
        assert_eq!(cfg.max_history_messages, 50);
        assert_eq!(cfg.diagnostic_provider, None);
    }

    #[test]
    fn validate_agents_rejects_duplicate_names() {
        let mut cfg = sample_config();
        cfg.agents
            .push(sample_agent("Claude", ProviderKind::Anthropic));
        cfg.agents
            .push(sample_agent("claude", ProviderKind::Anthropic));
        let err = cfg.validate_agents().expect_err("should reject");
        assert!(err.to_string().contains("Duplicate agent name"));
    }

    #[test]
    fn validate_agents_rejects_empty_name() {
        let mut cfg = sample_config();
        cfg.agents.push(sample_agent("", ProviderKind::Anthropic));
        let err = cfg.validate_agents().expect_err("should reject");
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn validate_agents_rejects_sanitized_collisions() {
        let mut cfg = sample_config();
        cfg.agents
            .push(sample_agent("Claude/1", ProviderKind::Anthropic));
        cfg.agents
            .push(sample_agent("Claude 1", ProviderKind::Anthropic));
        let err = cfg.validate_agents().expect_err("should reject");
        assert!(err.to_string().contains("duplicate filenames"));
    }

    #[test]
    fn agent_config_to_provider_config_round_trip() {
        let pc = ProviderConfig {
            api_key: "key".to_string(),
            model: "m".to_string(),
            reasoning_effort: Some("high".to_string()),
            thinking_effort: None,
            use_cli: true,
            cli_print_mode: true,
            extra_cli_args: "--x".to_string(),
        };
        let ac = AgentConfig::from_provider_config("Test".to_string(), ProviderKind::OpenAI, &pc);
        let pc2 = ac.to_provider_config();
        assert_eq!(pc2.api_key, "key");
        assert_eq!(pc2.model, "m");
        assert_eq!(pc2.reasoning_effort, Some("high".to_string()));
        assert!(pc2.use_cli);
        assert_eq!(pc2.extra_cli_args, "--x");
    }
}
