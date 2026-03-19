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
use unicode_width::UnicodeWidthChar;

const DIAGNOSTIC_SUFFIX: &str =
    "Write any encountered issues (for example permission, tool, or environment issues) to an explicit \"Errors\" section of your report.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptRuntimeContext {
    raw_prompt: String,
    diagnostics_suffix: Option<&'static str>,
    cli_working_directory_prefix: Option<String>,
    memory_context: Option<String>,
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
            memory_context: None,
        }
    }

    pub fn raw_prompt(&self) -> &str {
        &self.raw_prompt
    }

    pub fn set_memory_context(&mut self, context: String) {
        if !context.is_empty() {
            self.memory_context = Some(context);
        }
    }

    pub fn memory_context(&self) -> Option<&str> {
        self.memory_context.as_deref()
    }

    pub fn initial_prompt_for_agent(&self, is_cli: bool) -> String {
        self.augment_prompt_for_agent(&self.raw_prompt, is_cli)
    }

    pub fn augment_prompt_for_agent(&self, base_prompt: &str, is_cli: bool) -> String {
        let mut prompt = String::new();

        if is_cli {
            if let Some(prefix) = &self.cli_working_directory_prefix {
                prompt.push_str(prefix);
                if !base_prompt.is_empty()
                    || self.memory_context.is_some()
                    || self.diagnostics_suffix.is_some()
                {
                    prompt.push_str("\n\n");
                }
            }
        }

        // Memory context between cwd prefix and base prompt
        if let Some(ref mem) = self.memory_context {
            prompt.push_str(mem);
            if !base_prompt.is_empty() || self.diagnostics_suffix.is_some() {
                prompt.push_str("\n\n");
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
#[allow(dead_code)] // Fields within variants are constructed but not all are pattern-matched
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
        loop_pass: u32,
    },
    BlockLog {
        block_id: u32,
        agent_name: String,
        iteration: u32,
        loop_pass: u32,
        message: String,
    },
    BlockFinished {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
        loop_pass: u32,
    },
    BlockError {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
        loop_pass: u32,
        error: String,
        details: Option<String>,
    },
    BlockSkipped {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
        loop_pass: u32,
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
        loop_pass: u32,
        chunk: String,
    },
    LoopBreakEval {
        from: u32,
        to: u32,
        iteration: u32,
        pass: u32,
        agent_name: String,
        decision: String,
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
    BatchStageStarted {
        label: String,
    },
    BatchStageFinished {
        label: String,
        error: Option<String>,
    },
    AllRunsDone,
}

/// Waits until the cancellation flag is set.
/// Uses a short polling interval (10ms) as a simple alternative to a Notify channel.
pub async fn wait_for_cancel(cancel: &Arc<AtomicBool>) {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
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
    if let Err(e) = fwd.await {
        eprintln!("live log forwarder panicked: {e}");
    }
    result
}

pub(crate) async fn finish_live_log_forwarder(task: JoinHandle<()>, cancelled: bool) {
    if cancelled {
        task.abort();
    }
    let _ = task.await;
}

/// Runs a provider request with live-log forwarding and cancellation support.
///
/// Returns `Some(result)` on completion, or `None` if cancelled.
/// On cancellation, sends `cancel_event` to the progress channel.
pub(crate) async fn run_with_cancellation(
    provider: &mut dyn crate::provider::Provider,
    message: &str,
    progress_tx: &mpsc::UnboundedSender<ProgressEvent>,
    cancel: &Arc<AtomicBool>,
    make_chunk_event: impl Fn(String) -> ProgressEvent + Send + 'static,
    make_log_event: impl Fn(String) -> ProgressEvent + Send + 'static,
    cancel_event: ProgressEvent,
) -> Option<Result<crate::provider::CompletionResponse, crate::error::AppError>> {
    let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
    provider.set_live_log_sender(Some(live_tx));
    let ptx = progress_tx.clone();
    let live_forward = tokio::spawn(async move {
        while let Some(line) = live_rx.recv().await {
            let _ = ptx.send(make_log_event(format!("CLI {line}")));
        }
    });
    let result = tokio::select! {
        res = send_with_streaming(provider, message, progress_tx, make_chunk_event) => Some(res),
        _ = wait_for_cancel(cancel) => {
            let _ = progress_tx.send(cancel_event);
            None
        }
    };
    provider.set_live_log_sender(None);
    let cancelled = result.is_none();
    finish_live_log_forwarder(live_forward, cancelled).await;
    result
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

/// Fit a string to exactly `width` terminal columns: truncate with "..." if
/// wider, pad with trailing spaces if narrower.
///
/// Uses unicode display width so CJK and other wide glyphs are measured
/// correctly. Only appends "..." when the string actually exceeds `width`,
/// so borderline-length names are never truncated needlessly.
///
/// Requires `width >= 3` so "..." can fit. For `width < 3`, returns spaces.
pub fn fit_display_width(s: &str, width: usize) -> String {
    if width < 3 {
        return " ".repeat(width);
    }
    let str_width: usize = s
        .chars()
        .map(|c| UnicodeWidthChar::width(c).unwrap_or(0))
        .sum();
    if str_width <= width {
        let pad = width - str_width;
        let mut out = s.to_string();
        for _ in 0..pad {
            out.push(' ');
        }
        return out;
    }
    // Truncate: fit chars into (width - 3) display columns, then append "..."
    let target = width - 3; // safe: width >= 3
    let mut out = String::new();
    let mut used = 0usize;
    for ch in s.chars() {
        let w = UnicodeWidthChar::width(ch).unwrap_or(0);
        if used + w > target {
            break;
        }
        out.push(ch);
        used += w;
    }
    out.push_str("...");
    // Pad remainder (e.g. wide char skipped left a gap before "...")
    let total = used + 3;
    for _ in total..width {
        out.push(' ');
    }
    out
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
    fn fit_display_width_pads_short_string() {
        assert_eq!(fit_display_width("hello", 10), "hello     ");
    }

    #[test]
    fn fit_display_width_exact_fit_no_pad() {
        assert_eq!(fit_display_width("abcdefghijklmno", 15), "abcdefghijklmno");
    }

    #[test]
    fn fit_display_width_truncates_ascii() {
        // 16 chars in 15-wide column → 12 chars + "..." = 15 cols
        assert_eq!(fit_display_width("abcdefghijklmnop", 15), "abcdefghijkl...");
    }

    #[test]
    fn fit_display_width_cjk_truncate_and_pad() {
        // "你好世界" = 8 cols, in 7-wide: target=4, "你好"(4)+"..."(3) = 7
        assert_eq!(fit_display_width("你好世界", 7), "你好...");
    }

    #[test]
    fn fit_display_width_cjk_fits_with_pad() {
        // "你好世界" = 8 cols, in 10-wide: pad 2 spaces
        assert_eq!(fit_display_width("你好世界", 10), "你好世界  ");
    }

    #[test]
    fn fit_display_width_cjk_gap_after_truncation() {
        // "你好世x" = 2+2+2+1 = 7 cols. In 6-wide column:
        // target = 3 cols. "你"(2) fits, "好"(2+2=4) > 3, stop.
        // "你"(2) + "..."(3) = 5, pad 1 space → 6 cols total
        assert_eq!(fit_display_width("你好世x", 6), "你... ");
    }

    #[test]
    fn fit_display_width_width_below_three() {
        assert_eq!(fit_display_width("hello", 2), "  ");
        assert_eq!(fit_display_width("hello", 0), "");
    }

    #[test]
    fn fit_display_width_borderline_no_truncation() {
        // 13-char agent name in 15-wide column: show fully, pad 2
        assert_eq!(fit_display_width("thirteen_char", 15), "thirteen_char  ");
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

    #[test]
    fn prompt_runtime_context_memory_ordering() {
        let mut context = PromptRuntimeContext::new("base prompt", true);
        context.set_memory_context("<project_memory>recalled</project_memory>".to_string());

        // API agent: [memory] → [base] → [diagnostics]
        let api_prompt = context.augment_prompt_for_agent("base prompt", false);
        let mem_pos = api_prompt.find("<project_memory>").unwrap();
        let base_pos = api_prompt.find("base prompt").unwrap();
        let diag_pos = api_prompt.find("Errors").unwrap();
        assert!(mem_pos < base_pos, "memory should come before base prompt");
        assert!(
            base_pos < diag_pos,
            "base prompt should come before diagnostics"
        );

        // CLI agent: [cwd] → [memory] → [base] → [diagnostics]
        let cli_prompt = context.augment_prompt_for_agent("base prompt", true);
        let cwd_pos = cli_prompt.find("Working directory").unwrap();
        let mem_pos = cli_prompt.find("<project_memory>").unwrap();
        let base_pos = cli_prompt.find("base prompt").unwrap();
        assert!(cwd_pos < mem_pos, "cwd should come before memory");
        assert!(mem_pos < base_pos, "memory should come before base prompt");
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
