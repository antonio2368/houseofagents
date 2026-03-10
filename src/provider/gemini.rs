use super::{effort_to_budget, HttpProviderBase, Provider, ProviderKind, Role, SendFuture};
use crate::error::AppError;
use futures_util::StreamExt;
use tokio::sync::mpsc;

const GEMINI_API_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";

pub struct GeminiProvider {
    base: HttpProviderBase,
}

impl GeminiProvider {
    pub fn new(base: HttpProviderBase) -> Self {
        Self { base }
    }

    fn format_messages_gemini(&self) -> Vec<serde_json::Value> {
        self.base
            .history
            .iter()
            .map(|m| {
                serde_json::json!({
                    "role": match m.role { Role::User => "user", Role::Assistant => "model" },
                    "parts": [{ "text": m.content }],
                })
            })
            .collect()
    }
}

fn extract_stream_text_parts(event_data: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(event_data) else {
        return Vec::new();
    };

    value["candidates"][0]["content"]["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| part["text"].as_str().map(str::to_string))
        .collect()
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

fn build_stream_generate_content_request(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    body: &serde_json::Value,
) -> Result<reqwest::Request, AppError> {
    Ok(client
        .post(format!(
            "{base_url}/models/{model}:streamGenerateContent?alt=sse"
        ))
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

impl Provider for GeminiProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Gemini
    }

    fn clear_history(&mut self) {
        self.base.clear_history();
    }

    fn send(&mut self, message: &str) -> SendFuture<'_> {
        let message = message.to_string();
        Box::pin(async move {
            self.base.prepare_send(&message);
            let contents = self.format_messages_gemini();

            let mut gen_config = serde_json::json!({
                "maxOutputTokens": self.base.max_tokens,
            });
            if let Some(ref effort) = self.base.effort {
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
                &self.base.client,
                GEMINI_API_BASE_URL,
                &self.base.api_key,
                &self.base.model,
                &body,
            )?;

            let resp_body = self.base.execute_request(request, "Gemini").await?;
            let content = extract_content(&resp_body)?;
            Ok(self.base.finish_send(content))
        })
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn send_streaming(
        &mut self,
        message: &str,
        chunk_tx: mpsc::Sender<String>,
    ) -> SendFuture<'_> {
        let message = message.to_string();
        Box::pin(async move {
            self.base.prepare_send(&message);
            let contents = self.format_messages_gemini();

            let mut gen_config = serde_json::json!({
                "maxOutputTokens": self.base.max_tokens,
            });
            if let Some(ref effort) = self.base.effort {
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

            let request = build_stream_generate_content_request(
                &self.base.client,
                GEMINI_API_BASE_URL,
                &self.base.api_key,
                &self.base.model,
                &body,
            )?;

            let mut stream = self
                .base
                .execute_streaming_request(request, "Gemini")
                .await?;
            let mut parser = crate::provider::sse::SseParser::new();
            let mut full_content = String::new();

            while let Some(chunk_result) = stream.next().await {
                let bytes = chunk_result.map_err(|e| AppError::Provider {
                    provider: "Gemini".into(),
                    message: format!("Stream error: {e}"),
                })?;
                parser.feed(&bytes);
                while let Some(event) = parser.next_event() {
                    for text in extract_stream_text_parts(&event.data) {
                        full_content.push_str(&text);
                        let _ = chunk_tx.send(text).await;
                    }
                }
            }

            Ok(self.base.finish_send(full_content))
        })
    }
}

pub async fn list_models(api_key: &str, client: &reqwest::Client) -> Result<Vec<String>, String> {
    let request = build_list_models_request(client, GEMINI_API_BASE_URL, api_key)?;
    let body = HttpProviderBase::fetch_models(client, request).await?;

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
    use super::{
        build_generate_content_request, build_list_models_request, extract_content,
        extract_stream_text_parts,
    };
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
    fn extract_stream_text_parts_reads_all_text_parts() {
        let data = r#"{"candidates":[{"content":{"parts":[{"text":"a "},{"text":"b"}]}}]}"#;
        assert_eq!(
            extract_stream_text_parts(data),
            vec!["a ".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn extract_stream_text_parts_ignores_payload_without_text_parts() {
        let data = r#"{"candidates":[{"content":{"parts":[{"type":"inline_data"}]}}]}"#;
        assert!(extract_stream_text_parts(data).is_empty());
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
