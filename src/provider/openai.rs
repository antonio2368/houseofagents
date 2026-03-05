use super::{prune_history, CompletionResponse, Message, Provider, ProviderKind, Role};
use crate::error::AppError;
use async_trait::async_trait;

pub struct OpenAIProvider {
    api_key: String,
    model: String,
    client: reqwest::Client,
    max_tokens: u32,
    max_history_messages: usize,
    reasoning_effort: Option<String>,
    history: Vec<Message>,
}

impl OpenAIProvider {
    pub fn new(
        api_key: String,
        model: String,
        client: reqwest::Client,
        max_tokens: u32,
        max_history_messages: usize,
        reasoning_effort: Option<String>,
    ) -> Self {
        Self {
            api_key,
            model,
            client,
            max_tokens,
            max_history_messages,
            reasoning_effort,
            history: Vec::new(),
        }
    }
}

#[async_trait]
impl Provider for OpenAIProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::OpenAI
    }

    async fn send(&mut self, message: &str) -> Result<CompletionResponse, AppError> {
        self.history.push(Message {
            role: Role::User,
            content: message.to_string(),
        });

        prune_history(&mut self.history, self.max_history_messages);

        let messages: Vec<serde_json::Value> = self
            .history
            .iter()
            .map(|m| {
                serde_json::json!({
                    "role": match m.role { Role::User => "user", Role::Assistant => "assistant" },
                    "content": m.content,
                })
            })
            .collect();

        let body = if let Some(ref effort) = self.reasoning_effort {
            serde_json::json!({
                "model": self.model,
                "max_completion_tokens": self.max_tokens,
                "reasoning_effort": effort,
                "messages": messages,
            })
        } else {
            serde_json::json!({
                "model": self.model,
                "max_tokens": self.max_tokens,
                "messages": messages,
            })
        };

        let resp = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let resp_text = resp.text().await?;

        if !status.is_success() {
            return Err(AppError::Provider {
                provider: "OpenAI".into(),
                message: format!("{status}: {resp_text}"),
            });
        }

        let resp_body: serde_json::Value =
            serde_json::from_str(&resp_text).map_err(|e| AppError::Provider {
                provider: "OpenAI".into(),
                message: format!("Failed to parse response: {e}"),
            })?;

        let content = resp_body["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .to_string();

        self.history.push(Message {
            role: Role::Assistant,
            content,
        });

        let content = self.history.last().unwrap().content.clone();
        Ok(CompletionResponse {
            content,
            debug_logs: Vec::new(),
        })
    }
}

pub async fn list_models(api_key: &str, client: &reqwest::Client) -> Result<Vec<String>, String> {
    let resp = client
        .get("https://api.openai.com/v1/models")
        .header("Authorization", format!("Bearer {}", api_key))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();

    if !status.is_success() {
        let text = resp.text().await.map_err(|e| e.to_string())?;
        return Err(format!("{status}: {text}"));
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;

    let mut entries: Vec<(String, i64)> = body["data"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let id = m["id"].as_str()?.to_string();
                    let created = m["created"].as_i64().unwrap_or(0);
                    Some((id, created))
                })
                .collect()
        })
        .unwrap_or_default();

    entries.sort_by(|a, b| b.1.cmp(&a.1));
    Ok(entries.into_iter().map(|(id, _)| id).collect())
}
