pub mod multi;
pub mod pipeline;
pub mod relay;
pub mod swarm;
#[cfg(test)]
pub(crate) mod test_utils;

use crate::provider::ProviderKind;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const DIAGNOSTIC_SUFFIX: &str =
    "Write any encountered issues (for example permission, tool, or environment issues) to an explicit \"Errors\" section of your report.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptRuntimeContext {
    raw_prompt: String,
    diagnostics_suffix: Option<&'static str>,
    cli_working_directory_prefix: Option<String>,
}

impl PromptRuntimeContext {
    pub fn new(raw_prompt: impl Into<String>, diagnostics_enabled: bool) -> Self {
        let cli_working_directory_prefix = std::env::current_dir()
            .ok()
            .map(|cwd| cwd.display().to_string())
            .filter(|cwd| !cwd.is_empty())
            .map(|cwd| {
                format!(
                    "Working directory: {cwd}\nYou have access to the data and files in this directory for context."
                )
            });

        Self {
            raw_prompt: raw_prompt.into(),
            diagnostics_suffix: diagnostics_enabled.then_some(DIAGNOSTIC_SUFFIX),
            cli_working_directory_prefix,
        }
    }

    pub fn raw_prompt(&self) -> &str {
        &self.raw_prompt
    }

    pub fn initial_prompt_for_agent(&self, is_cli: bool) -> String {
        self.augment_prompt_for_agent(&self.raw_prompt, is_cli)
    }

    pub fn augment_prompt_for_agent(&self, base_prompt: &str, is_cli: bool) -> String {
        let mut prompt = String::new();

        if is_cli {
            if let Some(prefix) = &self.cli_working_directory_prefix {
                prompt.push_str(prefix);
                if !base_prompt.is_empty() || self.diagnostics_suffix.is_some() {
                    prompt.push_str("\n\n");
                }
            }
        }

        prompt.push_str(base_prompt);

        if let Some(suffix) = self.diagnostics_suffix {
            if !prompt.is_empty() {
                prompt.push_str("\n\n");
            }
            prompt.push_str(suffix);
        }

        prompt
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    Relay,
    Swarm,
    Pipeline,
}

impl ExecutionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionMode::Relay => "relay",
            ExecutionMode::Swarm => "swarm",
            ExecutionMode::Pipeline => "pipeline",
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            ExecutionMode::Relay => "Sequential cooperative - agents build on each other",
            ExecutionMode::Swarm => "Parallel cooperative - agents share results between rounds",
            ExecutionMode::Pipeline => {
                "Custom pipeline \u{2014} build arbitrary DAGs of agent blocks"
            }
        }
    }

    pub fn all() -> &'static [ExecutionMode] {
        &[
            ExecutionMode::Relay,
            ExecutionMode::Swarm,
            ExecutionMode::Pipeline,
        ]
    }
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ExecutionMode::Relay => "Relay",
                ExecutionMode::Swarm => "Swarm",
                ExecutionMode::Pipeline => "Pipeline",
            }
        )
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ProgressEvent {
    AgentStarted {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
    },
    AgentLog {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
        message: String,
    },
    AgentFinished {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
    },
    AgentError {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
        error: String,
        /// Full error body/details for display
        details: Option<String>,
    },
    IterationComplete {
        iteration: u32,
    },
    BlockStarted {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
    },
    BlockLog {
        block_id: u32,
        agent_name: String,
        iteration: u32,
        message: String,
    },
    BlockFinished {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
    },
    BlockError {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
        error: String,
        details: Option<String>,
    },
    BlockSkipped {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
        reason: String,
    },
    AgentStreamChunk {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
        chunk: String,
    },
    BlockStreamChunk {
        block_id: u32,
        agent_name: String,
        iteration: u32,
        chunk: String,
    },
    AllDone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone)]
pub enum BatchProgressEvent {
    RunQueued {
        run_id: u32,
    },
    RunStarted {
        run_id: u32,
    },
    RunEvent {
        run_id: u32,
        event: ProgressEvent,
    },
    RunFinished {
        run_id: u32,
        outcome: RunOutcome,
        error: Option<String>,
    },
    AllRunsDone,
}

pub async fn wait_for_cancel(cancel: &Arc<AtomicBool>) {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

pub(crate) async fn send_with_streaming(
    provider: &mut dyn crate::provider::Provider,
    message: &str,
    progress_tx: &mpsc::UnboundedSender<ProgressEvent>,
    make_chunk_event: impl Fn(String) -> ProgressEvent + Send + 'static,
) -> Result<crate::provider::CompletionResponse, crate::error::AppError> {
    if !provider.supports_streaming() {
        return provider.send(message).await;
    }
    let (chunk_tx, mut chunk_rx) = mpsc::channel::<String>(64);
    let ptx = progress_tx.clone();
    let fwd = tokio::spawn(async move {
        while let Some(chunk) = chunk_rx.recv().await {
            let _ = ptx.send(make_chunk_event(chunk));
        }
    });
    let result = provider.send_streaming(message, chunk_tx).await;
    let _ = fwd.await;
    result
}

pub(crate) async fn finish_live_log_forwarder(task: JoinHandle<()>, cancelled: bool) {
    if cancelled {
        task.abort();
    }
    let _ = task.await;
}

pub fn truncate_chars(s: &str, max_chars: usize) -> String {
    let mut iter = s.chars();
    let mut out = String::new();
    for _ in 0..max_chars {
        match iter.next() {
            Some(ch) => out.push(ch),
            None => return out,
        }
    }
    if iter.next().is_some() {
        format!("{out}...")
    } else {
        out
    }
}

impl ProgressEvent {
    #[allow(dead_code)]
    pub fn is_stream_chunk(&self) -> bool {
        matches!(
            self,
            ProgressEvent::AgentStreamChunk { .. } | ProgressEvent::BlockStreamChunk { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn execution_mode_as_str_values() {
        assert_eq!(ExecutionMode::Relay.as_str(), "relay");
        assert_eq!(ExecutionMode::Swarm.as_str(), "swarm");
    }

    #[test]
    fn execution_mode_display_values() {
        assert_eq!(ExecutionMode::Relay.to_string(), "Relay");
        assert_eq!(ExecutionMode::Swarm.to_string(), "Swarm");
    }

    #[test]
    fn execution_mode_description_not_empty() {
        for mode in ExecutionMode::all() {
            assert!(!mode.description().trim().is_empty());
        }
    }

    #[test]
    fn truncate_chars_no_truncation() {
        assert_eq!(truncate_chars("hello", 5), "hello");
    }

    #[test]
    fn truncate_chars_with_truncation_ascii() {
        assert_eq!(truncate_chars("hello world", 5), "hello...");
    }

    #[test]
    fn truncate_chars_with_multibyte() {
        assert_eq!(truncate_chars("héllö", 3), "hél...");
    }

    #[test]
    fn truncate_chars_zero_limit() {
        assert_eq!(truncate_chars("abc", 0), "...");
    }

    #[test]
    fn prompt_runtime_context_keeps_raw_prompt_unmodified() {
        let context = PromptRuntimeContext::new("raw prompt", true);
        assert_eq!(context.raw_prompt(), "raw prompt");
    }

    #[test]
    fn prompt_runtime_context_only_adds_cli_prefix_for_cli_agents() {
        let context = PromptRuntimeContext::new("base prompt", false);
        let cli_prompt = context.initial_prompt_for_agent(true);
        let api_prompt = context.initial_prompt_for_agent(false);

        assert!(cli_prompt.contains("Working directory:"));
        assert!(cli_prompt.ends_with("base prompt"));
        assert_eq!(api_prompt, "base prompt");
    }

    #[test]
    fn prompt_runtime_context_appends_diagnostics_suffix() {
        let context = PromptRuntimeContext::new("base prompt", true);
        let prompt = context.initial_prompt_for_agent(false);

        assert!(prompt.starts_with("base prompt"));
        assert!(prompt.contains("explicit \"Errors\" section"));
    }

    #[tokio::test]
    async fn wait_for_cancel_returns_when_flag_set() {
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_setter = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            cancel_setter.store(true, Ordering::Relaxed);
        });
        tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            wait_for_cancel(&cancel),
        )
        .await
        .expect("wait_for_cancel should complete");
    }

    #[tokio::test]
    async fn finish_live_log_forwarder_aborts_on_cancel() {
        let task = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            finish_live_log_forwarder(task, true),
        )
        .await
        .expect("cancelled forwarder should not hang");
    }

    #[tokio::test]
    async fn finish_live_log_forwarder_drains_completed_task() {
        let task = tokio::spawn(async {});
        finish_live_log_forwarder(task, false).await;
    }
}
