use crate::error::AppError;
use crate::provider::{prune_history, CompletionResponse, Message, Provider, ProviderKind, Role};
use async_trait::async_trait;
use serde_json::Value;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct CliProvider {
    kind: ProviderKind,
    model: String,
    reasoning_effort: Option<String>,
    thinking_effort: Option<String>,
    extra_cli_args: String,
    cli_print_mode: bool,
    add_dirs: Vec<String>,
    session_id: Option<String>,
    session_started: bool,
    timeout_seconds: u64,
    max_history_messages: usize,
    history: Vec<Message>,
    live_log_tx: Option<mpsc::UnboundedSender<String>>,
}

impl CliProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: ProviderKind,
        model: String,
        reasoning_effort: Option<String>,
        thinking_effort: Option<String>,
        extra_cli_args: String,
        cli_print_mode: bool,
        add_dirs: Vec<String>,
        timeout_seconds: u64,
        max_history_messages: usize,
    ) -> Self {
        Self {
            kind,
            model,
            reasoning_effort,
            thinking_effort,
            extra_cli_args,
            cli_print_mode,
            add_dirs,
            session_id: match kind {
                ProviderKind::Anthropic => Some(Uuid::new_v4().to_string()),
                _ => None,
            },
            session_started: false,
            timeout_seconds: timeout_seconds.max(1),
            max_history_messages,
            history: Vec::new(),
            live_log_tx: None,
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

    fn extra_cli_arg(&self) -> Option<String> {
        let trimmed = self.extra_cli_args.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
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
            _ => None,
        }
    }

    fn clip(s: &str, max_chars: usize) -> String {
        let clipped: String = s.chars().take(max_chars).collect();
        if s.chars().count() > max_chars {
            format!("{clipped}...")
        } else {
            clipped
        }
    }

    fn short_session(id: &str) -> String {
        if id.len() <= 12 {
            id.to_string()
        } else {
            format!("{}...{}", &id[..6], &id[id.len() - 4..])
        }
    }

    fn append_stderr_debug(debug_logs: &mut Vec<String>, stderr_text: &str) {
        let lines: Vec<String> = stderr_text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| Self::clip(line, 160))
            .collect();
        if lines.is_empty() {
            return;
        }
        debug_logs.push(format!("stderr lines: {}", lines.len()));
        let tail_start = lines.len().saturating_sub(8);
        for line in lines.iter().skip(tail_start) {
            debug_logs.push(format!("stderr> {line}"));
        }
    }

    fn push_command_debug(&self, debug_logs: &mut Vec<String>, bin: &str, args: &[String]) {
        debug_logs.push(format!(
            "cli start: {bin} (timeout={}s, model={})",
            self.timeout_seconds,
            if self.model.is_empty() {
                "(default)"
            } else {
                &self.model
            }
        ));
        if let Some(session_id) = self.session_id.as_deref() {
            debug_logs.push(format!(
                "session: {} ({})",
                Self::short_session(session_id),
                if self.session_started {
                    "resuming"
                } else {
                    "initial"
                }
            ));
        } else {
            debug_logs.push("session: none".into());
        }
        let preview = args
            .iter()
            .take(18)
            .map(|a| Self::clip(a, 48))
            .collect::<Vec<_>>()
            .join(" ");
        let suffix = if args.len() > 18 {
            format!(" ... (+{} more)", args.len() - 18)
        } else {
            String::new()
        };
        debug_logs.push(format!("args: {preview}{suffix}"));
    }

    fn provider_error_with_debug(
        provider: &str,
        message: String,
        debug_logs: &[String],
    ) -> AppError {
        if debug_logs.is_empty() {
            return AppError::Provider {
                provider: provider.to_string(),
                message,
            };
        }
        let mut merged = message;
        merged.push_str("\nCLI logs:");
        for line in debug_logs {
            merged.push_str("\n- ");
            merged.push_str(line);
        }
        AppError::Provider {
            provider: provider.to_string(),
            message: merged,
        }
    }

    fn emit_live_log(&self, message: String) {
        if let Some(tx) = self.live_log_tx.as_ref() {
            let _ = tx.send(message);
        }
    }
}

#[async_trait]
impl Provider for CliProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn set_live_log_sender(&mut self, tx: Option<mpsc::UnboundedSender<String>>) {
        self.live_log_tx = tx;
    }

    async fn send(&mut self, message: &str) -> Result<CompletionResponse, AppError> {
        let started_at = Instant::now();
        let mut debug_logs: Vec<String> = Vec::new();
        self.history.push(Message {
            role: Role::User,
            content: message.to_string(),
        });
        prune_history(&mut self.history, self.max_history_messages);
        let mut prompt = if self.uses_native_session() {
            message.to_string()
        } else {
            self.build_prompt_from_history()
        };

        if self.kind == ProviderKind::Anthropic && self.cli_print_mode && !self.session_started {
            prompt = format!(
                "IMPORTANT: Do NOT write any files. Return everything in your output.\n\n{prompt}"
            );
        }

        let bin = self.bin_name();
        let mut codex_output_path: Option<PathBuf> = None;
        let mut args: Vec<String> = match self.kind {
            ProviderKind::Anthropic => {
                let mut args = Vec::new();
                if self.cli_print_mode {
                    args.push("-p".to_string());
                }
                args.push("--output-format".to_string());
                args.push("text".to_string());
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
        if let Some(extra_arg) = self.extra_cli_arg() {
            args.push(extra_arg);
        }
        self.push_command_debug(&mut debug_logs, bin, &args);
        self.emit_live_log(format!("start {} (timeout {}s)", bin, self.timeout_seconds));

        let mut child = Command::new(bin)
            .args(&args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                Self::provider_error_with_debug(bin, format!("Failed to spawn: {e}"), &debug_logs)
            })?;

        let stdout = child.stdout.take().ok_or_else(|| {
            Self::provider_error_with_debug(
                bin,
                "Failed to capture stdout from CLI process".into(),
                &debug_logs,
            )
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            Self::provider_error_with_debug(
                bin,
                "Failed to capture stderr from CLI process".into(),
                &debug_logs,
            )
        })?;

        let stdout_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let mut out = Vec::new();
            reader.read_to_end(&mut out).await?;
            Ok::<Vec<u8>, std::io::Error>(out)
        });
        let live_log_tx = self.live_log_tx.clone();
        let stderr_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut line = String::new();
            let mut full = String::new();
            loop {
                line.clear();
                let read = reader.read_line(&mut line).await?;
                if read == 0 {
                    break;
                }
                full.push_str(&line);
                let trimmed = line.trim_end_matches(['\r', '\n']).trim();
                if !trimmed.is_empty() {
                    if let Some(tx) = live_log_tx.as_ref() {
                        let _ = tx.send(format!("stderr> {}", CliProvider::clip(trimmed, 180)));
                    }
                }
            }
            Ok::<String, std::io::Error>(full)
        });

        // Write prompt via stdin
        if let Some(mut stdin) = child.stdin.take() {
            debug_logs.push(format!("stdin chars: {}", prompt.chars().count()));
            stdin.write_all(prompt.as_bytes()).await.map_err(|e| {
                Self::provider_error_with_debug(
                    bin,
                    format!("Failed to write stdin: {e}"),
                    &debug_logs,
                )
            })?;
            // Drop stdin to close it, signaling EOF
        }

        let status =
            match tokio::time::timeout(Duration::from_secs(self.timeout_seconds), child.wait())
                .await
            {
                Ok(Ok(status)) => status,
                Ok(Err(e)) => {
                    if let Some(path) = codex_output_path.as_ref() {
                        let _ = fs::remove_file(path).await;
                    }
                    debug_logs.push(format!(
                        "cli wait failed after {} ms",
                        started_at.elapsed().as_millis()
                    ));
                    self.emit_live_log("wait failed".into());
                    stdout_task.abort();
                    stderr_task.abort();
                    return Err(Self::provider_error_with_debug(
                        bin,
                        e.to_string(),
                        &debug_logs,
                    ));
                }
                Err(_) => {
                    if let Some(path) = codex_output_path.as_ref() {
                        let _ = fs::remove_file(path).await;
                    }
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    debug_logs.push(format!(
                        "cli timeout after {} seconds",
                        self.timeout_seconds
                    ));
                    self.emit_live_log(format!("timeout after {}s", self.timeout_seconds));
                    stdout_task.abort();
                    stderr_task.abort();
                    return Err(Self::provider_error_with_debug(
                        bin,
                        format!("Timed out after {} seconds", self.timeout_seconds),
                        &debug_logs,
                    ));
                }
            };

        let stdout_bytes = match stdout_task.await {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(e)) => {
                return Err(Self::provider_error_with_debug(
                    bin,
                    format!("Failed reading stdout: {e}"),
                    &debug_logs,
                ))
            }
            Err(e) => {
                return Err(Self::provider_error_with_debug(
                    bin,
                    format!("stdout task failed: {e}"),
                    &debug_logs,
                ))
            }
        };
        let stderr_text = match stderr_task.await {
            Ok(Ok(text)) => text,
            Ok(Err(e)) => {
                return Err(Self::provider_error_with_debug(
                    bin,
                    format!("Failed reading stderr: {e}"),
                    &debug_logs,
                ))
            }
            Err(e) => {
                return Err(Self::provider_error_with_debug(
                    bin,
                    format!("stderr task failed: {e}"),
                    &debug_logs,
                ))
            }
        };

        debug_logs.push(format!(
            "exit: {} (elapsed {} ms, stdout {} bytes, stderr {} bytes)",
            status,
            started_at.elapsed().as_millis(),
            stdout_bytes.len(),
            stderr_text.len(),
        ));
        self.emit_live_log(format!("exit {}", status));
        Self::append_stderr_debug(&mut debug_logs, &stderr_text);

        if !status.success() {
            if let Some(path) = codex_output_path.as_ref() {
                let _ = fs::remove_file(path).await;
            }
            return Err(Self::provider_error_with_debug(
                bin,
                format!("exit {}: {}", status, stderr_text),
                &debug_logs,
            ));
        }

        let stdout_text: String = String::from_utf8_lossy(&stdout_bytes).into();
        if self.kind == ProviderKind::OpenAI {
            if let Some(session_id) = Self::extract_session_id_from_jsonl(&stdout_text) {
                debug_logs.push(format!(
                    "session detected: {}",
                    Self::short_session(&session_id)
                ));
                self.session_id = Some(session_id);
                self.session_started = true;
            }
        } else if self.kind == ProviderKind::Anthropic {
            self.session_started = true;
            if let Some(session_id) = self.session_id.as_deref() {
                debug_logs.push(format!(
                    "session active: {}",
                    Self::short_session(session_id)
                ));
            }
        }

        let content = if self.kind == ProviderKind::OpenAI {
            if let Some(path) = codex_output_path.as_ref() {
                let text = fs::read_to_string(path)
                    .await
                    .unwrap_or_else(|_| stdout_text.clone());
                let _ = fs::remove_file(path).await;
                debug_logs.push(format!("codex output chars: {}", text.chars().count()));
                text
            } else {
                stdout_text
            }
        } else {
            stdout_text
        };
        self.history.push(Message {
            role: Role::Assistant,
            content,
        });
        prune_history(&mut self.history, self.max_history_messages);

        let content = self.history.last().unwrap().content.clone();
        Ok(CompletionResponse {
            content,
            debug_logs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CliProvider;
    use crate::provider::ProviderKind;
    use serde_json::json;

    fn provider_with_extra(extra: &str) -> CliProvider {
        CliProvider::new(
            ProviderKind::OpenAI,
            String::new(),
            None,
            None,
            extra.to_string(),
            true,
            Vec::new(),
            30,
            50,
        )
    }

    #[test]
    fn extra_cli_arg_none_when_blank() {
        let provider = provider_with_extra("   ");
        assert_eq!(provider.extra_cli_arg(), None);
    }

    #[test]
    fn extra_cli_arg_uses_single_trimmed_argument_without_splitting() {
        let provider = provider_with_extra("  --config key=value with spaces  ");
        assert_eq!(
            provider.extra_cli_arg(),
            Some("--config key=value with spaces".to_string())
        );
    }

    #[test]
    fn extra_cli_arg_preserves_quote_characters() {
        let provider = provider_with_extra("--opt \"quoted value\"");
        assert_eq!(
            provider.extra_cli_arg(),
            Some("--opt \"quoted value\"".to_string())
        );
    }

    #[test]
    fn find_session_id_matches_expected_keys() {
        let value = json!({
            "meta": {
                "session_id": "123e4567-e89b-12d3-a456-426614174000"
            }
        });
        let found = CliProvider::find_session_id(&value);
        assert_eq!(
            found.as_deref(),
            Some("123e4567-e89b-12d3-a456-426614174000")
        );
    }

    #[test]
    fn find_session_id_does_not_match_unrelated_uuid_strings() {
        let value = json!({
            "path": "123e4567-e89b-12d3-a456-426614174000",
            "data": ["123e4567-e89b-12d3-a456-426614174001"]
        });
        let found = CliProvider::find_session_id(&value);
        assert_eq!(found, None);
    }

    #[test]
    fn extract_session_id_from_jsonl_ignores_invalid_lines() {
        let stdout = r#"
not json
{"event":"noop"}
{"meta":{"thread_id":"123e4567-e89b-12d3-a456-426614174000"}}
"#;
        let found = CliProvider::extract_session_id_from_jsonl(stdout);
        assert_eq!(
            found.as_deref(),
            Some("123e4567-e89b-12d3-a456-426614174000")
        );
    }

    #[test]
    fn short_session_formats_long_id() {
        let short = CliProvider::short_session("1234567890abcdef");
        assert_eq!(short, "123456...cdef");
    }

    #[test]
    fn push_command_debug_openai_marks_resuming_after_session_started() {
        let mut provider = CliProvider::new(
            ProviderKind::OpenAI,
            String::new(),
            None,
            None,
            String::new(),
            false,
            Vec::new(),
            30,
            50,
        );
        provider.session_id = Some("123e4567-e89b-12d3-a456-426614174000".to_string());
        provider.session_started = true;

        let mut logs = Vec::new();
        provider.push_command_debug(
            &mut logs,
            "codex",
            &["exec".to_string(), "resume".to_string()],
        );

        assert!(logs.iter().any(|line| line.contains("session:")));
        assert!(logs.iter().any(|line| line.contains("(resuming)")));
        assert!(!logs.iter().any(|line| line.contains("(initial)")));
    }
}
