use super::{
    effort_to_budget, prune_history, CompletionResponse, Message, Provider, ProviderKind, Role,
};
use crate::error::AppError;
use async_trait::async_trait;

const GEMINI_API_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";

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

fn build_generate_content_request(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    body: &serde_json::Value,
) -> Result<reqwest::Request, AppError> {
    Ok(client
        .post(format!("{base_url}/models/{model}:generateContent"))
        .header("content-type", "application/json")
        .header("x-goog-api-key", api_key)
        .json(body)
        .build()?)
}

fn build_list_models_request(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
) -> Result<reqwest::Request, String> {
    client
        .get(format!("{base_url}/models?pageSize=1000"))
        .header("x-goog-api-key", api_key)
        .build()
        .map_err(|e| e.to_string())
}

fn extract_content(resp_body: &serde_json::Value) -> Result<String, AppError> {
    let candidates = resp_body
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| AppError::Provider {
            provider: "Gemini".into(),
            message: "Missing `candidates` array in response".into(),
        })?;

    for candidate in candidates {
        let parts = candidate
            .get("content")
            .and_then(|content| content.get("parts"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| AppError::Provider {
                provider: "Gemini".into(),
                message: "Missing `content.parts` in response candidate".into(),
            })?;

        let joined = parts
            .iter()
            .filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>()
            .concat();

        if !joined.is_empty() {
            return Ok(joined);
        }
    }

    Err(AppError::Provider {
        provider: "Gemini".into(),
        message: "No text content found in response".into(),
    })
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
            let budget = effort_to_budget(effort).map_err(|e| AppError::Provider {
                provider: "Gemini".into(),
                message: e,
            })?;
            gen_config["thinkingConfig"] = serde_json::json!({
                "thinkingBudget": budget,
            });
        }

        let body = serde_json::json!({
            "contents": contents,
            "generationConfig": gen_config,
        });

        let request = build_generate_content_request(
            &self.client,
            GEMINI_API_BASE_URL,
            &self.api_key,
            &self.model,
            &body,
        )?;

        let resp = self.client.execute(request).await?;

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

        let content = extract_content(&resp_body)?;

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
    let request = build_list_models_request(client, GEMINI_API_BASE_URL, api_key)?;
    let resp = client.execute(request).await.map_err(|e| e.to_string())?;
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

#[cfg(test)]
mod tests {
    use super::{build_generate_content_request, build_list_models_request, extract_content};
    use serde_json::json;

    #[test]
    fn extract_content_reads_text_parts() {
        let body = json!({
            "candidates": [
                {
                    "content": {
                        "parts": [
                            { "text": "answer" }
                        ]
                    }
                }
            ]
        });
        let content = extract_content(&body).expect("extract");
        assert_eq!(content, "answer");
    }

    #[test]
    fn extract_content_concatenates_multiple_parts() {
        let body = json!({
            "candidates": [
                {
                    "content": {
                        "parts": [
                            { "text": "a " },
                            { "text": "b" }
                        ]
                    }
                }
            ]
        });
        let content = extract_content(&body).expect("extract");
        assert_eq!(content, "a b");
    }

    #[test]
    fn extract_content_errors_when_candidates_missing() {
        let body = json!({});
        let err = extract_content(&body).expect_err("expected error");
        assert!(err.to_string().contains("Missing `candidates` array"));
    }

    #[test]
    fn extract_content_errors_when_no_text() {
        let body = json!({
            "candidates": [
                {
                    "content": {
                        "parts": [
                            { "type": "inline_data" }
                        ]
                    }
                }
            ]
        });
        let err = extract_content(&body).expect_err("expected error");
        assert!(err
            .to_string()
            .contains("No text content found in response"));
    }

    #[test]
    fn build_generate_content_request_uses_header_auth() {
        let client = reqwest::Client::new();
        let body = json!({ "contents": [] });
        let request = build_generate_content_request(
            &client,
            "https://example.com/v1beta",
            "secret",
            "gemini-2.5-pro",
            &body,
        )
        .expect("request");

        assert_eq!(
            request.url().as_str(),
            "https://example.com/v1beta/models/gemini-2.5-pro:generateContent"
        );
        assert_eq!(
            request
                .headers()
                .get("x-goog-api-key")
                .and_then(|value| value.to_str().ok()),
            Some("secret")
        );
        assert!(request.url().query().is_none());
    }

    #[test]
    fn build_list_models_request_uses_header_auth() {
        let client = reqwest::Client::new();
        let request = build_list_models_request(&client, "https://example.com/v1beta", "secret")
            .expect("request");

        assert_eq!(
            request.url().as_str(),
            "https://example.com/v1beta/models?pageSize=1000"
        );
        assert_eq!(
            request
                .headers()
                .get("x-goog-api-key")
                .and_then(|value| value.to_str().ok()),
            Some("secret")
        );
        assert!(request.url().query().is_some());
        assert!(!request.url().as_str().contains("key=secret"));
    }
}
