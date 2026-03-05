use super::{
    effort_to_budget, prune_history, CompletionResponse, Message, Provider, ProviderKind, Role,
};
use crate::error::AppError;
use async_trait::async_trait;

pub struct GeminiProvider {
    api_key: String,
    model: String,
    client: reqwest::Client,
    max_tokens: u32,
    max_history_messages: usize,
    thinking_effort: Option<String>,
    history: Vec<Message>,
}

impl GeminiProvider {
    pub fn new(
        api_key: String,
        model: String,
        client: reqwest::Client,
        max_tokens: u32,
        max_history_messages: usize,
        thinking_effort: Option<String>,
    ) -> Self {
        Self {
            api_key,
            model,
            client,
            max_tokens,
            max_history_messages,
            thinking_effort,
            history: Vec::new(),
        }
    }
}

#[async_trait]
impl Provider for GeminiProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Gemini
    }

    async fn send(&mut self, message: &str) -> Result<CompletionResponse, AppError> {
        self.history.push(Message {
            role: Role::User,
            content: message.to_string(),
        });

        prune_history(&mut self.history, self.max_history_messages);

        let contents: Vec<serde_json::Value> = self
            .history
            .iter()
            .map(|m| {
                serde_json::json!({
                    "role": match m.role { Role::User => "user", Role::Assistant => "model" },
                    "parts": [{ "text": m.content }],
                })
            })
            .collect();

        let mut gen_config = serde_json::json!({
            "maxOutputTokens": self.max_tokens,
        });
        if let Some(ref effort) = self.thinking_effort {
            let budget = effort_to_budget(effort);
            gen_config["thinkingConfig"] = serde_json::json!({
                "thinkingBudget": budget,
            });
        }

        let body = serde_json::json!({
            "contents": contents,
            "generationConfig": gen_config,
        });

        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            self.model, self.api_key
        );

        let resp = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let resp_text = resp.text().await?;

        if !status.is_success() {
            return Err(AppError::Provider {
                provider: "Gemini".into(),
                message: format!("{status}: {resp_text}"),
            });
        }

        let resp_body: serde_json::Value =
            serde_json::from_str(&resp_text).map_err(|e| AppError::Provider {
                provider: "Gemini".into(),
                message: format!("Failed to parse response: {e}"),
            })?;

        let content = resp_body["candidates"][0]["content"]["parts"][0]["text"]
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
    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models?key={}&pageSize=1000",
        api_key
    );

    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let status = resp.status();

    if !status.is_success() {
        let text = resp.text().await.map_err(|e| e.to_string())?;
        return Err(format!("{status}: {text}"));
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;

    let mut models: Vec<String> = body["models"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    m["name"]
                        .as_str()
                        .map(|n| n.strip_prefix("models/").unwrap_or(n).to_string())
                })
                .collect()
        })
        .unwrap_or_default();

    // Gemini API doesn't expose created timestamps; reverse to show newest first
    models.reverse();
    Ok(models)
}
