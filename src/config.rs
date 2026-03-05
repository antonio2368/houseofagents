use crate::error::AppError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic_provider: Option<String>,
    #[serde(default)]
    pub providers: HashMap<String, ProviderConfig>,
    #[serde(default)]
    pub diagnostics: HashMap<String, ProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub api_key: String,
    pub model: String,
    /// OpenAI reasoning effort: "low", "medium", "high" (for o-series models)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Thinking effort: "low", "medium", "high" (for Claude / Gemini models)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_effort: Option<String>,
    /// Use CLI tool instead of API for this provider
    #[serde(default)]
    pub use_cli: bool,
    /// Extra CLI argument appended as a single raw argument to provider CLI calls (no splitting)
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

fn default_cli_timeout_seconds() -> u64 {
    300
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
        let config: AppConfig = toml::from_str(&content)
            .map_err(|e| AppError::Config(format!("Failed to parse config: {e}")))?;
        Ok(config)
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
max_history_messages = 50
http_timeout_seconds = 120
model_fetch_timeout_seconds = 30
cli_timeout_seconds = 300

# Optional: set one diagnostics provider ("anthropic", "openai", "gemini")
# diagnostic_provider = "openai"

[providers.anthropic]
api_key = ""
model = "claude-sonnet-4-5"
thinking_effort = "medium"
use_cli = false
extra_cli_args = ""

[providers.openai]
api_key = ""
model = "gpt-5"
reasoning_effort = "medium"
use_cli = false
extra_cli_args = ""

[providers.gemini]
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "medium"
use_cli = false
extra_cli_args = ""

# Diagnostics are configured separately from run providers.
[diagnostics.anthropic]
api_key = ""
model = "claude-sonnet-4-5"
thinking_effort = "low"
use_cli = false
extra_cli_args = ""

[diagnostics.openai]
api_key = ""
model = "gpt-5-mini"
reasoning_effort = "low"
use_cli = false
extra_cli_args = ""

[diagnostics.gemini]
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "low"
use_cli = false
extra_cli_args = ""
"#;

        std::fs::write(&path, TEMPLATE)
            .map_err(|e| AppError::Config(format!("Failed to write config template: {e}")))?;
        Ok(path)
    }

    pub fn resolved_output_dir(&self) -> PathBuf {
        let expanded = if self.output_dir.starts_with("~/") {
            if let Some(home) = dirs::home_dir() {
                home.join(&self.output_dir[2..])
            } else {
                PathBuf::from(&self.output_dir)
            }
        } else {
            PathBuf::from(&self.output_dir)
        };
        expanded
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
            cli_timeout_seconds: 300,
            diagnostic_provider: Some("openai".to_string()),
            providers: HashMap::new(),
            diagnostics: HashMap::new(),
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
        let cfg = sample_config();
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
        assert!(body.contains("[providers.openai]"));
        assert!(body.contains("[diagnostics.gemini]"));
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
    fn deserialize_uses_timeout_defaults_when_missing() {
        let body = r#"
output_dir = "/tmp/hoa"

[providers.openai]
api_key = ""
model = "gpt-5"
"#;
        let cfg: AppConfig = toml::from_str(body).expect("parse");
        assert_eq!(cfg.http_timeout_seconds, 120);
        assert_eq!(cfg.model_fetch_timeout_seconds, 30);
        assert_eq!(cfg.cli_timeout_seconds, 300);
        assert_eq!(cfg.default_max_tokens, 4096);
        assert_eq!(cfg.max_history_messages, 50);
    }
}
