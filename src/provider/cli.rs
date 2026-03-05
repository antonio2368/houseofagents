use crate::error::AppError;
use crate::provider::{prune_history, CompletionResponse, Message, Provider, ProviderKind, Role};
use async_trait::async_trait;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use uuid::Uuid;

pub struct CliProvider {
    kind: ProviderKind,
    model: String,
    reasoning_effort: Option<String>,
    thinking_effort: Option<String>,
    add_dirs: Vec<String>,
    session_id: Option<String>,
    session_started: bool,
    max_history_messages: usize,
    history: Vec<Message>,
}

impl CliProvider {
    pub fn new(
        kind: ProviderKind,
        model: String,
        reasoning_effort: Option<String>,
        thinking_effort: Option<String>,
        add_dirs: Vec<String>,
        max_history_messages: usize,
    ) -> Self {
        Self {
            kind,
            model,
            reasoning_effort,
            thinking_effort,
            add_dirs,
            session_id: match kind {
                ProviderKind::Anthropic => Some(Uuid::new_v4().to_string()),
                _ => None,
            },
            session_started: false,
            max_history_messages,
            history: Vec::new(),
        }
    }

    fn bin_name(&self) -> &'static str {
        match self.kind {
            ProviderKind::Anthropic => "claude",
            ProviderKind::OpenAI => "codex",
            ProviderKind::Gemini => "gemini",
        }
    }

    fn anthropic_effort(&self) -> Option<&str> {
        self.thinking_effort.as_deref()
    }

    fn build_prompt_from_history(&self) -> String {
        let mut prompt = String::new();
        for msg in &self.history {
            let role = match msg.role {
                Role::User => "User",
                Role::Assistant => "Assistant",
            };
            prompt.push_str(role);
            prompt.push_str(":\n");
            prompt.push_str(&msg.content);
            prompt.push_str("\n\n");
        }
        prompt
    }

    fn uses_native_session(&self) -> bool {
        match self.kind {
            ProviderKind::Anthropic => true,
            ProviderKind::OpenAI => self.session_id.is_some(),
            ProviderKind::Gemini => false,
        }
    }

    fn codex_temp_output_path() -> PathBuf {
        let suffix = Uuid::new_v4().to_string();
        std::env::temp_dir().join(format!("houseofagents-codex-last-{suffix}.txt"))
    }

    fn extract_session_id_from_jsonl(stdout: &str) -> Option<String> {
        for line in stdout.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Some(id) = Self::find_session_id(&value) {
                return Some(id);
            }
        }
        None
    }

    fn find_session_id(value: &Value) -> Option<String> {
        match value {
            Value::Object(map) => {
                for (k, v) in map {
                    let key = k.to_ascii_lowercase();
                    if (key == "session_id"
                        || key == "sessionid"
                        || key == "conversation_id"
                        || key == "conversationid"
                        || key == "thread_id"
                        || key == "threadid")
                        && v.as_str().and_then(|s| Uuid::parse_str(s).ok()).is_some()
                    {
                        return v.as_str().map(|s| s.to_string());
                    }
                    if let Some(found) = Self::find_session_id(v) {
                        return Some(found);
                    }
                }
                None
            }
            Value::Array(items) => {
                for item in items {
                    if let Some(found) = Self::find_session_id(item) {
                        return Some(found);
                    }
                }
                None
            }
            Value::String(s) => Uuid::parse_str(s).ok().map(|_| s.to_string()),
            _ => None,
        }
    }
}

#[async_trait]
impl Provider for CliProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    async fn send(&mut self, message: &str) -> Result<CompletionResponse, AppError> {
        self.history.push(Message {
            role: Role::User,
            content: message.to_string(),
        });
        prune_history(&mut self.history, self.max_history_messages);
        let prompt = if self.uses_native_session() {
            message.to_string()
        } else {
            self.build_prompt_from_history()
        };

        let bin = self.bin_name();
        let mut codex_output_path: Option<PathBuf> = None;
        let mut args: Vec<String> = match self.kind {
            ProviderKind::Anthropic => {
                let mut args = vec![
                    "-p".to_string(),
                    "--output-format".to_string(),
                    "text".to_string(),
                ];
                for dir in &self.add_dirs {
                    args.push("--add-dir".to_string());
                    args.push(dir.clone());
                }
                if let Some(ref session_id) = self.session_id {
                    if self.session_started {
                        args.push("--resume".to_string());
                    } else {
                        args.push("--session-id".to_string());
                    }
                    args.push(session_id.clone());
                }
                if let Some(effort) = self.anthropic_effort() {
                    args.push("--effort".to_string());
                    args.push(effort.to_string());
                }
                args
            }
            ProviderKind::OpenAI => {
                let mut args = if self.session_id.is_some() {
                    vec!["exec".to_string(), "resume".to_string()]
                } else {
                    vec!["exec".to_string()]
                };
                if let Some(effort) = self.reasoning_effort.as_deref() {
                    args.push("-c".to_string());
                    args.push(format!("model_reasoning_effort=\"{effort}\""));
                }
                if !self.model.is_empty() {
                    args.push("--model".to_string());
                    args.push(self.model.clone());
                }
                let out_path = Self::codex_temp_output_path();
                args.push("--json".to_string());
                args.push("-o".to_string());
                args.push(out_path.display().to_string());
                codex_output_path = Some(out_path);
                if let Some(ref session_id) = self.session_id {
                    args.push(session_id.clone());
                }
                args.push("-".to_string());
                args
            }
            ProviderKind::Gemini => vec![
                "--prompt".to_string(),
                "".to_string(),
                "--output-format".to_string(),
                "text".to_string(),
            ],
        };

        if self.kind != ProviderKind::OpenAI && !self.model.is_empty() {
            args.push("--model".to_string());
            args.push(self.model.clone());
        }

        let mut child = Command::new(bin)
            .args(&args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| AppError::Provider {
                provider: bin.to_string(),
                message: format!("Failed to spawn: {e}"),
            })?;

        // Write prompt via stdin
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(prompt.as_bytes())
                .await
                .map_err(|e| AppError::Provider {
                    provider: bin.to_string(),
                    message: format!("Failed to write stdin: {e}"),
                })?;
            // Drop stdin to close it, signaling EOF
        }

        // 5-minute timeout
        let output =
            match tokio::time::timeout(Duration::from_secs(300), child.wait_with_output()).await {
                Ok(Ok(output)) => output,
                Ok(Err(e)) => {
                    return Err(AppError::Provider {
                        provider: bin.to_string(),
                        message: e.to_string(),
                    })
                }
                Err(_) => {
                    return Err(AppError::Provider {
                        provider: bin.to_string(),
                        message: "Timed out after 5 minutes".into(),
                    })
                }
            };

        if !output.status.success() {
            if let Some(path) = codex_output_path {
                let _ = std::fs::remove_file(path);
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(AppError::Provider {
                provider: bin.to_string(),
                message: format!("exit {}: {stderr}", output.status),
            });
        }

        let stdout_text: String = String::from_utf8_lossy(&output.stdout).into();
        if self.kind == ProviderKind::OpenAI {
            if let Some(session_id) = Self::extract_session_id_from_jsonl(&stdout_text) {
                self.session_id = Some(session_id);
            }
        } else if self.kind == ProviderKind::Anthropic {
            self.session_started = true;
        }

        let content = if self.kind == ProviderKind::OpenAI {
            if let Some(path) = codex_output_path.as_ref() {
                let text = std::fs::read_to_string(path).unwrap_or_else(|_| stdout_text.clone());
                let _ = std::fs::remove_file(path);
                text
            } else {
                stdout_text
            }
        } else {
            stdout_text
        };
        self.history.push(Message {
            role: Role::Assistant,
            content: content.clone(),
        });
        prune_history(&mut self.history, self.max_history_messages);

        Ok(CompletionResponse { content })
    }
}
