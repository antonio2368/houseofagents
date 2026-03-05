pub mod anthropic;
pub mod cli;
pub mod gemini;
pub mod openai;

use crate::config::ProviderConfig;
use crate::error::AppError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProviderKind {
    Anthropic,
    OpenAI,
    Gemini,
}

impl ProviderKind {
    pub fn display_name(&self) -> &'static str {
        match self {
            ProviderKind::Anthropic => "Claude",
            ProviderKind::OpenAI => "Codex",
            ProviderKind::Gemini => "Gemini",
        }
    }

    pub fn config_key(&self) -> &'static str {
        match self {
            ProviderKind::Anthropic => "anthropic",
            ProviderKind::OpenAI => "openai",
            ProviderKind::Gemini => "gemini",
        }
    }

    pub fn all() -> &'static [ProviderKind] {
        &[
            ProviderKind::Anthropic,
            ProviderKind::OpenAI,
            ProviderKind::Gemini,
        ]
    }
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

pub struct CompletionResponse {
    pub content: String,
    pub debug_logs: Vec<String>,
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn kind(&self) -> ProviderKind;
    fn set_live_log_sender(&mut self, _tx: Option<mpsc::UnboundedSender<String>>) {}
    async fn send(&mut self, message: &str) -> Result<CompletionResponse, AppError>;
}

/// Prune message history keeping the first message (initial prompt) and most recent messages.
pub fn prune_history(history: &mut Vec<Message>, max_messages: usize) {
    if max_messages < 4 || history.len() <= max_messages {
        return;
    }
    // Keep first 2 messages (initial user prompt + first response) and last (max-2) messages
    let keep_start = 2;
    let keep_end = max_messages - keep_start;
    let drain_end = history.len() - keep_end;
    if drain_end > keep_start {
        history.drain(keep_start..drain_end);
    }
}

pub fn create_provider(
    kind: ProviderKind,
    config: &ProviderConfig,
    client: reqwest::Client,
    max_tokens: u32,
    max_history_messages: usize,
    cli_timeout_seconds: u64,
) -> Box<dyn Provider> {
    if config.use_cli {
        return Box::new(cli::CliProvider::new(
            kind,
            config.model.clone(),
            config.reasoning_effort.clone(),
            config.thinking_effort.clone(),
            config.extra_cli_args.clone(),
            vec![],
            cli_timeout_seconds,
            max_history_messages,
        ));
    }
    match kind {
        ProviderKind::Anthropic => Box::new(anthropic::AnthropicProvider::new(
            config.api_key.clone(),
            config.model.clone(),
            client,
            max_tokens,
            max_history_messages,
            config.thinking_effort.clone(),
        )),
        ProviderKind::OpenAI => Box::new(openai::OpenAIProvider::new(
            config.api_key.clone(),
            config.model.clone(),
            client,
            max_tokens,
            max_history_messages,
            config.reasoning_effort.clone(),
        )),
        ProviderKind::Gemini => Box::new(gemini::GeminiProvider::new(
            config.api_key.clone(),
            config.model.clone(),
            client,
            max_tokens,
            max_history_messages,
            config.thinking_effort.clone(),
        )),
    }
}

pub fn effort_to_budget(effort: &str) -> u32 {
    match effort {
        "low" => 4096,
        "medium" => 8192,
        _ => 16384,
    }
}

pub async fn list_models(
    kind: ProviderKind,
    api_key: &str,
    client: &reqwest::Client,
) -> Result<Vec<String>, String> {
    if api_key.is_empty() {
        return Err(
            "Add API key to fetch model list. You can still type any model manually.".into(),
        );
    }

    match kind {
        ProviderKind::Anthropic => anthropic::list_models(api_key, client).await,
        ProviderKind::OpenAI => openai::list_models(api_key, client).await,
        ProviderKind::Gemini => gemini::list_models(api_key, client).await,
    }
}
