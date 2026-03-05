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
}

fn default_max_tokens() -> u32 {
    4096
}

fn default_max_history_messages() -> usize {
    50
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

# Optional: set one diagnostics provider ("anthropic", "openai", "gemini")
# diagnostic_provider = "openai"

[providers.anthropic]
api_key = ""
model = "claude-sonnet-4-5"
thinking_effort = "medium"
use_cli = false

[providers.openai]
api_key = ""
model = "gpt-5"
reasoning_effort = "medium"
use_cli = false

[providers.gemini]
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "medium"
use_cli = false

# Diagnostics are configured separately from run providers.
[diagnostics.anthropic]
api_key = ""
model = "claude-sonnet-4-5"
thinking_effort = "low"
use_cli = false

[diagnostics.openai]
api_key = ""
model = "gpt-5-mini"
reasoning_effort = "low"
use_cli = false

[diagnostics.gemini]
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "low"
use_cli = false
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
