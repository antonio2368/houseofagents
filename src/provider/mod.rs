pub mod anthropic;
pub mod cli;
pub mod gemini;
pub mod openai;
pub(crate) mod sse;

use crate::config::{AgentConfig, ProviderConfig};
use crate::error::AppError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;

pub type SendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CompletionResponse, AppError>> + Send + 'a>>;

// ---------------------------------------------------------------------------
// HttpProviderBase — shared fields + helpers for API-backed providers
// ---------------------------------------------------------------------------

pub(crate) struct HttpProviderBase {
    pub api_key: String,
    pub model: String,
    pub client: reqwest::Client,
    pub max_tokens: u32,
    pub max_history_messages: usize,
    pub max_history_bytes: usize,
    pub effort: Option<String>,
    pub history: Vec<Message>,
}

impl HttpProviderBase {
    /// Push the user message and prune history by count, then by byte budget.
    pub fn prepare_send(&mut self, message: &str) {
        self.history.push(Message {
            role: Role::User,
            content: message.to_string(),
        });
        prune_history(&mut self.history, self.max_history_messages);
        prune_history_bytes(&mut self.history, self.max_history_bytes);
    }

    /// Format history as `{"role": "user"|"assistant", "content": ...}` (Anthropic + OpenAI).
    pub fn format_messages_standard(&self) -> Vec<serde_json::Value> {
        self.history
            .iter()
            .map(|m| {
                serde_json::json!({
                    "role": match m.role { Role::User => "user", Role::Assistant => "assistant" },
                    "content": m.content,
                })
            })
            .collect()
    }

    /// Send a built request, check status, parse JSON, return body or provider error.
    pub async fn execute_request(
        &self,
        request: reqwest::Request,
        provider_name: &'static str,
    ) -> Result<serde_json::Value, AppError> {
        let resp = self.client.execute(request).await?;
        let status = resp.status();
        let resp_text = resp.text().await?;

        if !status.is_success() {
            return Err(AppError::Provider {
                provider: provider_name.into(),
                message: format!("{status}: {resp_text}"),
            });
        }

        serde_json::from_str(&resp_text).map_err(|e| AppError::Provider {
            provider: provider_name.into(),
            message: format!("Failed to parse response: {e}"),
        })
    }

    /// Send a built request and return a byte stream for SSE processing.
    /// On non-success status, returns an error with the response body.
    pub async fn execute_streaming_request(
        &self,
        request: reqwest::Request,
        provider_name: &'static str,
    ) -> Result<impl futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>>, AppError>
    {
        let resp = self.client.execute(request).await?;
        let status = resp.status();
        if !status.is_success() {
            let resp_text = resp.text().await?;
            return Err(AppError::Provider {
                provider: provider_name.into(),
                message: format!("{status}: {resp_text}"),
            });
        }
        Ok(resp.bytes_stream())
    }

    pub fn clear_history(&mut self) {
        self.history.clear();
    }

    /// Push assistant message and return `CompletionResponse`.
    pub fn finish_send(&mut self, content: String) -> CompletionResponse {
        self.history.push(Message {
            role: Role::Assistant,
            content,
        });
        CompletionResponse {
            content: self.history.last().unwrap().content.clone(),
            debug_logs: Vec::new(),
        }
    }

    /// Shared HTTP logic for model-listing endpoints (static method).
    pub async fn fetch_models(
        client: &reqwest::Client,
        request: reqwest::Request,
    ) -> Result<serde_json::Value, String> {
        let resp = client.execute(request).await.map_err(|e| e.to_string())?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.map_err(|e| e.to_string())?;
            return Err(format!("{status}: {text}"));
        }
        resp.json().await.map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    Anthropic,
    OpenAI,
    Gemini,
}

impl ProviderKind {
    pub fn display_name(&self) -> &'static str {
        match self {
            ProviderKind::Anthropic => "Claude",
            ProviderKind::OpenAI => "OpenAI",
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

    pub fn from_selector(raw: &str) -> Option<ProviderKind> {
        let selector = raw.trim().to_lowercase();
        Self::all().iter().copied().find(|kind| {
            kind.config_key() == selector || kind.display_name().to_lowercase() == selector
        })
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

pub trait Provider: Send {
    fn kind(&self) -> ProviderKind;
    fn set_live_log_sender(&mut self, _tx: Option<mpsc::UnboundedSender<String>>) {}
    fn send(&mut self, message: &str) -> SendFuture<'_>;
    fn clear_history(&mut self);
    fn supports_streaming(&self) -> bool {
        false
    }
    fn send_streaming(
        &mut self,
        message: &str,
        _chunk_tx: mpsc::Sender<String>,
    ) -> SendFuture<'_> {
        self.send(message)
    }
}

/// Prune message history by byte budget — remove oldest messages until total fits.
/// Always keeps at least one message to avoid emptying the history.
pub fn prune_history_bytes(history: &mut Vec<Message>, max_bytes: usize) {
    if max_bytes == 0 {
        return;
    }
    let total: usize = history.iter().map(|m| m.content.len()).sum();
    if total <= max_bytes {
        return;
    }
    let mut current = total;
    while current > max_bytes && history.len() > 1 {
        current -= history[0].content.len();
        history.remove(0);
    }
}

/// Prune message history, preserving the first exchange only when the cap is large enough.
pub fn prune_history(history: &mut Vec<Message>, max_messages: usize) {
    if history.len() <= max_messages {
        return;
    }
    if max_messages < 4 {
        if max_messages == 0 {
            history.clear();
        } else {
            let drain_end = history.len().saturating_sub(max_messages);
            history.drain(..drain_end);
        }
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
    max_history_bytes: usize,
    cli_timeout_seconds: u64,
) -> Box<dyn Provider> {
    if config.use_cli {
        return Box::new(cli::CliProvider::new(
            kind,
            config.model.clone(),
            config.reasoning_effort.clone(),
            config.thinking_effort.clone(),
            config.extra_cli_args.clone(),
            config.cli_print_mode,
            vec![],
            cli_timeout_seconds,
            max_history_messages,
            max_history_bytes,
        ));
    }
    let effort = match kind {
        ProviderKind::Anthropic | ProviderKind::Gemini => config.thinking_effort.clone(),
        ProviderKind::OpenAI => config.reasoning_effort.clone(),
    };
    let base = HttpProviderBase {
        api_key: config.api_key.clone(),
        model: config.model.clone(),
        client,
        max_tokens,
        max_history_messages,
        max_history_bytes,
        effort,
        history: Vec::new(),
    };
    match kind {
        ProviderKind::Anthropic => Box::new(anthropic::AnthropicProvider::new(base)),
        ProviderKind::OpenAI => Box::new(openai::OpenAIProvider::new(base)),
        ProviderKind::Gemini => Box::new(gemini::GeminiProvider::new(base)),
    }
}

#[allow(dead_code)]
pub fn create_provider_from_agent(
    agent: &AgentConfig,
    client: reqwest::Client,
    max_tokens: u32,
    max_history_messages: usize,
    max_history_bytes: usize,
    cli_timeout_seconds: u64,
) -> Box<dyn Provider> {
    create_provider(
        agent.provider,
        &agent.to_provider_config(),
        client,
        max_tokens,
        max_history_messages,
        max_history_bytes,
        cli_timeout_seconds,
    )
}

pub fn validate_effort_config(
    kind: ProviderKind,
    use_cli: bool,
    reasoning_effort: Option<&str>,
    thinking_effort: Option<&str>,
) -> Result<(), String> {
    let _ = reasoning_effort;
    match kind {
        ProviderKind::Anthropic => {
            if thinking_effort == Some("max") && !use_cli {
                return Err("\"max\" thinking effort requires CLI mode for Anthropic".into());
            }
        }
        ProviderKind::OpenAI | ProviderKind::Gemini => {}
    }
    Ok(())
}

pub fn effort_to_budget(effort: &str) -> Result<u32, String> {
    match effort {
        "low" => Ok(4096),
        "medium" => Ok(8192),
        "high" => Ok(16384),
        other => Err(format!(
            "unsupported thinking effort \"{other}\" for API mode"
        )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProviderConfig;

    fn cfg(use_cli: bool) -> ProviderConfig {
        ProviderConfig {
            api_key: "k".to_string(),
            model: "m".to_string(),
            reasoning_effort: Some("medium".to_string()),
            thinking_effort: Some("low".to_string()),
            use_cli,
            cli_print_mode: true,
            extra_cli_args: "--x".to_string(),
        }
    }

    #[test]
    fn provider_kind_display_names() {
        assert_eq!(ProviderKind::Anthropic.display_name(), "Claude");
        assert_eq!(ProviderKind::OpenAI.display_name(), "OpenAI");
        assert_eq!(ProviderKind::Gemini.display_name(), "Gemini");
    }

    #[test]
    fn provider_kind_config_keys() {
        assert_eq!(ProviderKind::Anthropic.config_key(), "anthropic");
        assert_eq!(ProviderKind::OpenAI.config_key(), "openai");
        assert_eq!(ProviderKind::Gemini.config_key(), "gemini");
    }

    #[test]
    fn provider_kind_all_has_three_unique() {
        let all = ProviderKind::all();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&ProviderKind::Anthropic));
        assert!(all.contains(&ProviderKind::OpenAI));
        assert!(all.contains(&ProviderKind::Gemini));
    }

    #[test]
    fn provider_kind_display_trait() {
        assert_eq!(ProviderKind::Anthropic.to_string(), "Claude");
        assert_eq!(ProviderKind::OpenAI.to_string(), "OpenAI");
    }

    #[test]
    fn provider_kind_from_selector_accepts_key_and_display_name() {
        assert_eq!(
            ProviderKind::from_selector("anthropic"),
            Some(ProviderKind::Anthropic)
        );
        assert_eq!(
            ProviderKind::from_selector(" OpenAI "),
            Some(ProviderKind::OpenAI)
        );
    }

    #[test]
    fn provider_kind_from_selector_unknown_returns_none() {
        assert_eq!(ProviderKind::from_selector("unknown"), None);
    }

    #[test]
    fn effort_to_budget_low_medium_high() {
        assert_eq!(effort_to_budget("low").unwrap(), 4096);
        assert_eq!(effort_to_budget("medium").unwrap(), 8192);
        assert_eq!(effort_to_budget("high").unwrap(), 16384);
    }

    #[test]
    fn effort_to_budget_rejects_unknown_values() {
        assert!(effort_to_budget("max").is_err());
        assert!(effort_to_budget("unexpected").is_err());
    }

    #[test]
    fn validate_effort_config_rejects_anthropic_max_in_api_mode() {
        let err = validate_effort_config(ProviderKind::Anthropic, false, None, Some("max"))
            .expect_err("should reject api mode");
        assert!(err.contains("requires CLI mode"));
    }

    #[test]
    fn validate_effort_config_allows_anthropic_max_in_cli_mode() {
        validate_effort_config(ProviderKind::Anthropic, true, None, Some("max"))
            .expect("cli mode should be allowed through to the provider");
    }

    #[test]
    fn prune_history_small_limit_keeps_most_recent_messages() {
        let mut history = vec![
            Message {
                role: Role::User,
                content: "u1".to_string(),
            },
            Message {
                role: Role::Assistant,
                content: "a1".to_string(),
            },
            Message {
                role: Role::User,
                content: "u2".to_string(),
            },
            Message {
                role: Role::Assistant,
                content: "a2".to_string(),
            },
            Message {
                role: Role::User,
                content: "u3".to_string(),
            },
        ];
        prune_history(&mut history, 3);
        let contents: Vec<String> = history.into_iter().map(|m| m.content).collect();
        assert_eq!(contents, vec!["u2", "a2", "u3"]);
    }

    #[test]
    fn prune_history_noop_when_len_within_max() {
        let mut history = vec![
            Message {
                role: Role::User,
                content: "u1".to_string(),
            },
            Message {
                role: Role::Assistant,
                content: "a1".to_string(),
            },
            Message {
                role: Role::User,
                content: "u2".to_string(),
            },
        ];
        prune_history(&mut history, 10);
        assert_eq!(history.len(), 3);
    }

    #[test]
    fn prune_history_keeps_first_two_and_last_n() {
        let mut history: Vec<Message> = (0..10)
            .map(|i| Message {
                role: if i % 2 == 0 {
                    Role::User
                } else {
                    Role::Assistant
                },
                content: format!("m{i}"),
            })
            .collect();

        prune_history(&mut history, 6);
        let contents: Vec<String> = history.into_iter().map(|m| m.content).collect();
        assert_eq!(contents, vec!["m0", "m1", "m6", "m7", "m8", "m9"]);
    }

    #[test]
    fn prune_history_limit_one_keeps_last_message() {
        let mut history: Vec<Message> = (0..5)
            .map(|i| Message {
                role: if i % 2 == 0 {
                    Role::User
                } else {
                    Role::Assistant
                },
                content: format!("m{i}"),
            })
            .collect();

        prune_history(&mut history, 1);
        let contents: Vec<String> = history.into_iter().map(|m| m.content).collect();
        assert_eq!(contents, vec!["m4"]);
    }

    #[test]
    fn prune_history_limit_two_keeps_last_two_messages() {
        let mut history: Vec<Message> = (0..5)
            .map(|i| Message {
                role: if i % 2 == 0 {
                    Role::User
                } else {
                    Role::Assistant
                },
                content: format!("m{i}"),
            })
            .collect();

        prune_history(&mut history, 2);
        let contents: Vec<String> = history.into_iter().map(|m| m.content).collect();
        assert_eq!(contents, vec!["m3", "m4"]);
    }

    #[test]
    fn prune_history_limit_three_keeps_last_three_messages() {
        let mut history: Vec<Message> = (0..6)
            .map(|i| Message {
                role: if i % 2 == 0 {
                    Role::User
                } else {
                    Role::Assistant
                },
                content: format!("m{i}"),
            })
            .collect();

        prune_history(&mut history, 3);
        let contents: Vec<String> = history.into_iter().map(|m| m.content).collect();
        assert_eq!(contents, vec!["m3", "m4", "m5"]);
    }

    #[test]
    fn create_provider_cli_returns_expected_kind() {
        let client = reqwest::Client::new();
        for kind in ProviderKind::all() {
            let p = create_provider(*kind, &cfg(true), client.clone(), 100, 20, 102400, 5);
            assert_eq!(p.kind(), *kind);
        }
    }

    #[test]
    fn create_provider_api_returns_expected_kind() {
        let client = reqwest::Client::new();
        for kind in ProviderKind::all() {
            let p = create_provider(*kind, &cfg(false), client.clone(), 100, 20, 102400, 5);
            assert_eq!(p.kind(), *kind);
        }
    }

    #[tokio::test]
    async fn list_models_empty_key_returns_user_friendly_error() {
        let client = reqwest::Client::new();
        let err = list_models(ProviderKind::OpenAI, "", &client)
            .await
            .expect_err("should reject empty key");
        assert!(err.contains("Add API key"));
    }

    #[test]
    fn role_serialization_is_lowercase() {
        let user = serde_json::to_string(&Role::User).expect("serialize");
        let assistant = serde_json::to_string(&Role::Assistant).expect("serialize");
        assert_eq!(user, "\"user\"");
        assert_eq!(assistant, "\"assistant\"");
    }

    #[test]
    fn provider_kind_serialization_is_lowercase() {
        #[derive(serde::Serialize)]
        struct W {
            kind: ProviderKind,
        }
        let s = toml::to_string(&W {
            kind: ProviderKind::Anthropic,
        })
        .expect("serialize");
        assert!(s.contains("anthropic"));
        let s = toml::to_string(&W {
            kind: ProviderKind::OpenAI,
        })
        .expect("serialize");
        assert!(s.contains("openai"));
        let s = toml::to_string(&W {
            kind: ProviderKind::Gemini,
        })
        .expect("serialize");
        assert!(s.contains("gemini"));
    }

    #[test]
    fn provider_kind_deserialization_from_lowercase() {
        #[derive(serde::Deserialize)]
        struct W {
            kind: ProviderKind,
        }
        let w: W = toml::from_str("kind = \"anthropic\"").expect("deserialize");
        assert_eq!(w.kind, ProviderKind::Anthropic);
        let w: W = toml::from_str("kind = \"openai\"").expect("deserialize");
        assert_eq!(w.kind, ProviderKind::OpenAI);
    }

    #[test]
    fn prune_history_bytes_removes_oldest_when_over_budget() {
        let mut history = vec![
            Message {
                role: Role::User,
                content: "a".repeat(50),
            },
            Message {
                role: Role::Assistant,
                content: "b".repeat(50),
            },
            Message {
                role: Role::User,
                content: "c".repeat(50),
            },
        ];
        // Total = 150 bytes, budget = 100
        prune_history_bytes(&mut history, 100);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].content, "b".repeat(50));
        assert_eq!(history[1].content, "c".repeat(50));
    }

    #[test]
    fn prune_history_bytes_noop_when_under_budget() {
        let mut history = vec![
            Message {
                role: Role::User,
                content: "short".to_string(),
            },
            Message {
                role: Role::Assistant,
                content: "reply".to_string(),
            },
        ];
        prune_history_bytes(&mut history, 1000);
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn prune_history_bytes_keeps_at_least_one_message() {
        let mut history = vec![Message {
            role: Role::User,
            content: "x".repeat(200),
        }];
        prune_history_bytes(&mut history, 50);
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn prune_history_bytes_zero_budget_is_noop() {
        let mut history = vec![Message {
            role: Role::User,
            content: "hello".to_string(),
        }];
        prune_history_bytes(&mut history, 0);
        assert_eq!(history.len(), 1);
    }
}
