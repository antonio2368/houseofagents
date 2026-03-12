use crate::config::{AgentConfig, AppConfig};
use crate::execution::multi::run_multi;
use crate::execution::pipeline as pipeline_mod;
use crate::execution::relay::run_relay;
use crate::execution::swarm::run_swarm;
use crate::execution::{
    BatchProgressEvent, ExecutionMode, ProgressEvent, PromptRuntimeContext, RunOutcome,
};
use crate::output::OutputManager;
use crate::provider;
use crate::{post_run, runtime_support as rs};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputFormat {
    Text,
    Json,
}

pub(crate) struct HeadlessArgs {
    pub prompt: Option<String>,
    pub mode: ExecutionMode,
    pub agents: Vec<String>,
    pub relay_order: Vec<String>,
    pub iterations: Option<u32>,
    pub runs: u32,
    pub concurrency: u32,
    pub session_name: Option<String>,
    pub forward_prompt: bool,
    pub keep_session: bool,
    pub pipeline_path: Option<PathBuf>,
    pub consolidate_agent: Option<String>,
    pub consolidation_prompt: String,
    pub output_format: OutputFormat,
    pub quiet: bool,
}

// ---------------------------------------------------------------------------
// Exit codes
// ---------------------------------------------------------------------------

const EXIT_OK: i32 = 0;
const EXIT_VALIDATION: i32 = 1;
const EXIT_EXECUTION: i32 = 2;
const EXIT_CANCELLED: i32 = 130;

// ---------------------------------------------------------------------------
// Typed headless error — distinguishes input-validation failures (exit 1)
// from runtime/execution failures (exit 2).
// ---------------------------------------------------------------------------

enum HeadlessError {
    /// Bad input: malformed pipeline, missing agents, invalid config.
    Validation(String),
    /// Runtime failure: HTTP errors, provider errors, panics.
    Execution(String),
}

impl HeadlessError {
    fn message(&self) -> &str {
        match self {
            HeadlessError::Validation(s) | HeadlessError::Execution(s) => s,
        }
    }

    fn exit_code(&self) -> i32 {
        match self {
            HeadlessError::Validation(_) => EXIT_VALIDATION,
            HeadlessError::Execution(_) => EXIT_EXECUTION,
        }
    }
}

impl From<String> for HeadlessError {
    fn from(s: String) -> Self {
        HeadlessError::Execution(s)
    }
}

// ---------------------------------------------------------------------------
// Diagnostics error ledger limit (same as TUI)
// ---------------------------------------------------------------------------

const ERROR_LEDGER_LIMIT: usize = 200;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check whether the run directory contains a non-empty `_errors.log`, which
/// indicates logged errors that may not have been captured by the progress
/// channel (e.g. snapshot write failures appended directly to the error log).
///
/// Fails closed: if `_errors.log` exists but cannot be stat'd (permissions,
/// transient I/O), we assume errors rather than silently reporting success.
fn has_nonempty_error_log(run_dir: &str) -> bool {
    match std::path::Path::new(run_dir)
        .join("_errors.log")
        .metadata()
    {
        Ok(m) => m.len() > 0,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true, // Can't verify — assume errors for safety
    }
}

// ---------------------------------------------------------------------------
// Progress logger
// ---------------------------------------------------------------------------

struct ProgressLogger {
    format: OutputFormat,
    quiet: bool,
    total_iterations: u32,
    total_runs: u32,
    /// Keyed by (run_id, agent_name) to avoid cross-run collisions.
    agent_started: HashMap<(u32, String), Instant>,
    /// Keyed by (run_id, block_id, agent_name) to avoid cross-run collisions.
    block_started: HashMap<(u32, u32, String), Instant>,
    run_started: HashMap<u32, Instant>,
    error_ledger: Vec<String>,
}

impl ProgressLogger {
    fn new(format: OutputFormat, quiet: bool, total_iterations: u32, total_runs: u32) -> Self {
        Self {
            format,
            quiet,
            total_iterations,
            total_runs,
            agent_started: HashMap::new(),
            block_started: HashMap::new(),
            run_started: HashMap::new(),
            error_ledger: Vec::new(),
        }
    }

    fn log_progress(&mut self, event: &ProgressEvent) {
        match self.format {
            OutputFormat::Text => self.log_text(event, None),
            OutputFormat::Json => self.log_json(event, None),
        }
    }

    fn log_batch_progress(&mut self, event: &BatchProgressEvent) {
        match self.format {
            OutputFormat::Text => self.log_batch_text(event),
            OutputFormat::Json => self.log_batch_json(event),
        }
    }

    fn log_text(&mut self, event: &ProgressEvent, run_id: Option<u32>) {
        if self.quiet {
            self.accumulate_errors(event, run_id);
            return;
        }
        let now = chrono::Local::now().format("%H:%M:%S");
        let prefix = run_id
            .map(|id| format!("[Run {id}/{}] ", self.total_runs))
            .unwrap_or_default();
        let rid = run_id.unwrap_or(0);
        match event {
            ProgressEvent::AgentStarted {
                agent, iteration, ..
            } => {
                self.agent_started
                    .insert((rid, agent.clone()), Instant::now());
                eprintln!(
                    "[{now}] {prefix}{agent} started (iteration {iteration}/{})",
                    self.total_iterations
                );
            }
            ProgressEvent::AgentLog { agent, message, .. } => {
                eprintln!("[{now}] {prefix}{agent}: {message}");
            }
            ProgressEvent::AgentFinished {
                agent, iteration, ..
            } => {
                let elapsed = self
                    .agent_started
                    .remove(&(rid, agent.clone()))
                    .map(|s| format!("{:.1}s", s.elapsed().as_secs_f64()))
                    .unwrap_or_default();
                eprintln!(
                    "[{now}] {prefix}{agent} finished (iteration {iteration}/{}, {elapsed})",
                    self.total_iterations
                );
            }
            ProgressEvent::AgentError {
                agent,
                iteration,
                error,
                details,
                ..
            } => {
                self.agent_started.remove(&(rid, agent.clone()));
                let detail = details.as_deref().unwrap_or(error);
                eprintln!("[{now}] {prefix}{agent} ERROR (iteration {iteration}): {detail}");
            }
            ProgressEvent::IterationComplete { iteration } => {
                eprintln!("[{now}] {prefix}iteration {iteration} complete");
            }
            ProgressEvent::BlockStarted {
                label,
                agent_name,
                iteration,
                loop_pass,
                ..
            } => {
                self.block_started.insert(
                    (rid, event_block_id(event), agent_name.clone()),
                    Instant::now(),
                );
                eprintln!(
                    "[{now}] {prefix}[{label}] {agent_name} started (iteration {iteration}, pass {loop_pass})"
                );
            }
            ProgressEvent::BlockLog {
                agent_name,
                message,
                ..
            } => {
                eprintln!("[{now}] {prefix}{agent_name}: {message}");
            }
            ProgressEvent::BlockFinished {
                block_id,
                label,
                agent_name,
                ..
            } => {
                let elapsed = self
                    .block_started
                    .remove(&(rid, *block_id, agent_name.clone()))
                    .map(|s| format!("{:.1}s", s.elapsed().as_secs_f64()))
                    .unwrap_or_default();
                eprintln!("[{now}] {prefix}[{label}] {agent_name} finished ({elapsed})");
            }
            ProgressEvent::BlockError {
                block_id,
                label,
                agent_name,
                error,
                details,
                ..
            } => {
                self.block_started
                    .remove(&(rid, *block_id, agent_name.clone()));
                let detail = details.as_deref().unwrap_or(error);
                eprintln!("[{now}] {prefix}[{label}] {agent_name} ERROR: {detail}");
            }
            ProgressEvent::BlockSkipped {
                label,
                agent_name,
                reason,
                ..
            } => {
                eprintln!("[{now}] {prefix}[{label}] {agent_name} skipped: {reason}");
            }
            ProgressEvent::AgentStreamChunk { .. } | ProgressEvent::BlockStreamChunk { .. } => {
                // Suppress stream chunks in text mode
            }
            ProgressEvent::AllDone => {}
        }
        self.accumulate_errors(event, run_id);
    }

    fn log_batch_text(&mut self, event: &BatchProgressEvent) {
        let now = chrono::Local::now().format("%H:%M:%S");
        match event {
            BatchProgressEvent::RunQueued { run_id } => {
                if !self.quiet {
                    eprintln!("[{now}] [Run {run_id}/{}] queued", self.total_runs);
                }
            }
            BatchProgressEvent::RunStarted { run_id } => {
                self.run_started.insert(*run_id, Instant::now());
                if !self.quiet {
                    eprintln!("[{now}] [Run {run_id}/{}] started", self.total_runs);
                }
            }
            BatchProgressEvent::RunEvent { run_id, event } => {
                self.log_text(event, Some(*run_id));
            }
            BatchProgressEvent::RunFinished {
                run_id,
                outcome,
                error,
            } => {
                let elapsed = self
                    .run_started
                    .remove(run_id)
                    .map(|s| format!(" ({:.1}s)", s.elapsed().as_secs_f64()))
                    .unwrap_or_default();
                let status = match outcome {
                    RunOutcome::Done => "ok",
                    RunOutcome::Failed => "FAILED",
                    RunOutcome::Cancelled => "cancelled",
                };
                if !self.quiet {
                    if let Some(err) = error {
                        eprintln!(
                            "[{now}] [Run {run_id}/{}] finished: {status}{elapsed} - {err}",
                            self.total_runs
                        );
                    } else {
                        eprintln!(
                            "[{now}] [Run {run_id}/{}] finished: {status}{elapsed}",
                            self.total_runs
                        );
                    }
                }
            }
            BatchProgressEvent::AllRunsDone => {}
        }
        self.accumulate_batch_finish_errors(event);
    }

    fn log_json(&mut self, event: &ProgressEvent, run_id: Option<u32>) {
        if !self.quiet {
            let obj = progress_event_to_json(event, run_id);
            if let Some(obj) = obj {
                eprintln!("{}", serde_json::to_string(&obj).unwrap_or_default());
            }
        }
        self.accumulate_errors(event, run_id);
    }

    fn log_batch_json(&mut self, event: &BatchProgressEvent) {
        if !self.quiet {
            match event {
                BatchProgressEvent::RunQueued { run_id } => {
                    let obj = serde_json::json!({"event": "run_queued", "run_id": run_id});
                    eprintln!("{}", serde_json::to_string(&obj).unwrap_or_default());
                }
                BatchProgressEvent::RunStarted { run_id } => {
                    let obj = serde_json::json!({"event": "run_started", "run_id": run_id});
                    eprintln!("{}", serde_json::to_string(&obj).unwrap_or_default());
                }
                BatchProgressEvent::RunEvent { run_id, event } => {
                    let obj = progress_event_to_json(event, Some(*run_id));
                    if let Some(obj) = obj {
                        eprintln!("{}", serde_json::to_string(&obj).unwrap_or_default());
                    }
                }
                BatchProgressEvent::RunFinished {
                    run_id,
                    outcome,
                    error,
                } => {
                    let status = match outcome {
                        RunOutcome::Done => "ok",
                        RunOutcome::Failed => "failed",
                        RunOutcome::Cancelled => "cancelled",
                    };
                    let obj = serde_json::json!({
                        "event": "run_finished",
                        "run_id": run_id,
                        "status": status,
                        "error": error,
                    });
                    eprintln!("{}", serde_json::to_string(&obj).unwrap_or_default());
                }
                BatchProgressEvent::AllRunsDone => {
                    let obj = serde_json::json!({"event": "all_runs_done"});
                    eprintln!("{}", serde_json::to_string(&obj).unwrap_or_default());
                }
            }
        }
        // Track state and errors regardless of quiet
        match event {
            BatchProgressEvent::RunStarted { run_id } => {
                self.run_started.insert(*run_id, Instant::now());
            }
            BatchProgressEvent::RunFinished { run_id, .. } => {
                // Remove start-time entry so memory scales with concurrency,
                // not total runs. (Text mode removes in its own branch above;
                // JSON mode needs this path.)
                self.run_started.remove(run_id);
            }
            BatchProgressEvent::RunEvent { run_id, event } => {
                self.accumulate_errors(event, Some(*run_id));
            }
            _ => {}
        }
        self.accumulate_batch_finish_errors(event);
    }

    fn accumulate_batch_finish_errors(&mut self, event: &BatchProgressEvent) {
        if let BatchProgressEvent::RunFinished {
            run_id,
            outcome: RunOutcome::Failed,
            error: Some(err),
        } = event
        {
            self.push_error(format!("[run {run_id}] {err}"));
        }
    }

    fn accumulate_errors(&mut self, event: &ProgressEvent, run_id: Option<u32>) {
        let run_tag = run_id.map(|id| format!("run {id} ")).unwrap_or_default();
        match event {
            ProgressEvent::AgentError {
                agent,
                iteration,
                error,
                details,
                ..
            } => {
                let detail = details.as_deref().unwrap_or(error);
                self.push_error(format!("[{run_tag}{agent} iter {iteration}] {detail}"));
            }
            ProgressEvent::BlockError {
                label,
                agent_name,
                iteration,
                error,
                details,
                ..
            } => {
                let detail = details.as_deref().unwrap_or(error);
                self.push_error(format!(
                    "[{run_tag}{label} {agent_name} iter {iteration}] {detail}"
                ));
            }
            _ => {}
        }
    }

    fn push_error(&mut self, entry: String) {
        if self.error_ledger.len() < ERROR_LEDGER_LIMIT {
            self.error_ledger.push(entry);
        }
    }

    fn has_errors(&self) -> bool {
        !self.error_ledger.is_empty()
    }

    fn drain_errors(self) -> Vec<String> {
        self.error_ledger
    }
}

fn event_block_id(event: &ProgressEvent) -> u32 {
    match event {
        ProgressEvent::BlockStarted { block_id, .. }
        | ProgressEvent::BlockFinished { block_id, .. }
        | ProgressEvent::BlockError { block_id, .. }
        | ProgressEvent::BlockSkipped { block_id, .. } => *block_id,
        _ => 0,
    }
}

fn progress_event_to_json(event: &ProgressEvent, run_id: Option<u32>) -> Option<serde_json::Value> {
    let mut obj = match event {
        ProgressEvent::AgentStarted {
            agent,
            kind,
            iteration,
        } => serde_json::json!({
            "event": "agent_started",
            "agent": agent,
            "provider": kind.config_key(),
            "iteration": iteration,
        }),
        ProgressEvent::AgentLog {
            agent,
            iteration,
            message,
            ..
        } => serde_json::json!({
            "event": "agent_log",
            "agent": agent,
            "iteration": iteration,
            "message": message,
        }),
        ProgressEvent::AgentFinished {
            agent,
            kind,
            iteration,
        } => serde_json::json!({
            "event": "agent_finished",
            "agent": agent,
            "provider": kind.config_key(),
            "iteration": iteration,
        }),
        ProgressEvent::AgentError {
            agent,
            iteration,
            error,
            details,
            ..
        } => serde_json::json!({
            "event": "agent_error",
            "agent": agent,
            "iteration": iteration,
            "error": error,
            "details": details,
        }),
        ProgressEvent::IterationComplete { iteration } => serde_json::json!({
            "event": "iteration_complete",
            "iteration": iteration,
        }),
        ProgressEvent::BlockStarted {
            block_id,
            agent_name,
            label,
            iteration,
            loop_pass,
        } => serde_json::json!({
            "event": "block_started",
            "block_id": block_id,
            "agent": agent_name,
            "label": label,
            "iteration": iteration,
            "loop_pass": loop_pass,
        }),
        ProgressEvent::BlockLog {
            block_id,
            agent_name,
            iteration,
            loop_pass,
            message,
        } => serde_json::json!({
            "event": "block_log",
            "block_id": block_id,
            "agent": agent_name,
            "iteration": iteration,
            "loop_pass": loop_pass,
            "message": message,
        }),
        ProgressEvent::BlockFinished {
            block_id,
            agent_name,
            label,
            iteration,
            loop_pass,
        } => serde_json::json!({
            "event": "block_finished",
            "block_id": block_id,
            "agent": agent_name,
            "label": label,
            "iteration": iteration,
            "loop_pass": loop_pass,
        }),
        ProgressEvent::BlockError {
            block_id,
            agent_name,
            label,
            iteration,
            loop_pass,
            error,
            details,
        } => serde_json::json!({
            "event": "block_error",
            "block_id": block_id,
            "agent": agent_name,
            "label": label,
            "iteration": iteration,
            "loop_pass": loop_pass,
            "error": error,
            "details": details,
        }),
        ProgressEvent::BlockSkipped {
            block_id,
            agent_name,
            label,
            iteration,
            loop_pass,
            reason,
        } => serde_json::json!({
            "event": "block_skipped",
            "block_id": block_id,
            "agent": agent_name,
            "label": label,
            "iteration": iteration,
            "loop_pass": loop_pass,
            "reason": reason,
        }),
        ProgressEvent::AgentStreamChunk {
            agent,
            iteration,
            chunk,
            ..
        } => serde_json::json!({
            "event": "agent_stream_chunk",
            "agent": agent,
            "iteration": iteration,
            "chunk": chunk,
        }),
        ProgressEvent::BlockStreamChunk {
            block_id,
            agent_name,
            iteration,
            loop_pass,
            chunk,
        } => serde_json::json!({
            "event": "block_stream_chunk",
            "block_id": block_id,
            "agent": agent_name,
            "iteration": iteration,
            "loop_pass": loop_pass,
            "chunk": chunk,
        }),
        ProgressEvent::AllDone => serde_json::json!({
            "event": "all_done",
        }),
    };
    if let Some(rid) = run_id {
        if let Some(map) = obj.as_object_mut() {
            map.insert("run_id".into(), serde_json::json!(rid));
        }
    }
    Some(obj)
}

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

fn install_signal_handler(cancel: Arc<AtomicBool>, quiet: bool) {
    tokio::spawn(async move {
        let mut first = true;
        loop {
            if tokio::signal::ctrl_c().await.is_ok() {
                if first {
                    cancel.store(true, Ordering::Relaxed);
                    if !quiet {
                        eprintln!("\nCancelling... (press Ctrl+C again to force exit)");
                    }
                    first = false;
                } else {
                    std::process::exit(EXIT_CANCELLED);
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn validate_args(args: &HeadlessArgs, config: &AppConfig) -> Result<(), String> {
    let is_pipeline = args.pipeline_path.is_some();

    if !is_pipeline {
        // Prompt-based runs
        if args.mode == ExecutionMode::Pipeline {
            return Err("--mode pipeline requires --pipeline".into());
        }
        let prompt = args.prompt.as_deref().unwrap_or("");
        if prompt.trim().is_empty() {
            return Err("Prompt is required (--prompt or --prompt-file)".into());
        }
        if args.agents.is_empty() {
            return Err("--agents is required for relay/swarm runs".into());
        }
        {
            let mut seen = std::collections::HashSet::new();
            for name in &args.agents {
                if !seen.insert(name) {
                    return Err(format!("--agents contains duplicate: '{name}'"));
                }
            }
        }
    } else {
        // Pipeline mode
        if args.mode == ExecutionMode::Relay {
            return Err("--mode relay is not valid with --pipeline".into());
        }
        if !args.agents.is_empty() {
            return Err("--agents is not valid with --pipeline".into());
        }
        if !args.relay_order.is_empty() {
            return Err("--order is not valid with --pipeline".into());
        }
        if args.forward_prompt {
            return Err("--forward-prompt is not valid with --pipeline".into());
        }
    }

    // Relay-specific validations
    if !is_pipeline {
        if !args.relay_order.is_empty() && args.mode != ExecutionMode::Relay {
            return Err("--order is only valid in relay mode".into());
        }
        if args.forward_prompt && args.mode != ExecutionMode::Relay {
            return Err("--forward-prompt is only valid in relay mode".into());
        }
        if !args.relay_order.is_empty() {
            // Validate order matches agents
            let mut order_set: Vec<String> = args.relay_order.clone();
            let mut agent_set: Vec<String> = args.agents.clone();
            order_set.sort();
            agent_set.sort();
            let order_dedup = {
                let mut s = order_set.clone();
                s.dedup();
                s
            };
            if order_dedup.len() != args.relay_order.len() {
                return Err("--order contains duplicates".into());
            }
            if order_set != agent_set {
                return Err("--order must contain exactly the same agents as --agents".into());
            }
        }
    }

    // Iterations
    if let Some(iters) = args.iterations {
        if iters < 1 {
            return Err("--iterations must be >= 1".into());
        }
    }
    if args.runs < 1 {
        return Err("--runs must be >= 1".into());
    }

    // Consolidation validation
    if !args.consolidation_prompt.is_empty() && args.consolidate_agent.is_none() {
        return Err("--consolidation-prompt requires --consolidate".into());
    }
    if args.consolidate_agent.is_some()
        && !is_pipeline
        && args.mode == ExecutionMode::Relay
        && args.runs <= 1
    {
        return Err(
            "--consolidate is not supported for single relay runs (relay produces a single output chain)".into(),
        );
    }

    // Validate agent availability
    let cli_available = rs::detect_cli_availability();
    let session_overrides = rs::compute_session_overrides(&config.agents, &cli_available);

    if !is_pipeline {
        for name in &args.agents {
            let agent_config = rs::resolve_agent_config(name, &session_overrides, &config.agents)
                .ok_or_else(|| format!("Agent '{name}' is not configured"))?;
            rs::validate_agent_runtime(&cli_available, name, agent_config)?;
        }
    }

    // Validate consolidation agent
    if let Some(ref agent_name) = args.consolidate_agent {
        let agent_config = rs::resolve_agent_config(agent_name, &session_overrides, &config.agents)
            .ok_or_else(|| format!("Consolidation agent '{agent_name}' is not configured"))?;
        rs::validate_agent_runtime(&cli_available, agent_name, agent_config)?;
    }

    // Diagnostic agent is validated lazily at post-run time — a broken or
    // missing diagnostics config must not block execution.

    Ok(())
}

// ---------------------------------------------------------------------------
// Pre-run info banner
// ---------------------------------------------------------------------------

fn print_run_info(args: &HeadlessArgs, effective_iterations: u32) {
    if args.quiet {
        return;
    }

    let is_pipeline = args.pipeline_path.is_some();
    let mode_str = if is_pipeline {
        "pipeline"
    } else {
        args.mode.as_str()
    };

    match args.output_format {
        OutputFormat::Text => {
            eprintln!("--- House of Agents ---");
            eprintln!("Mode:       {mode_str}");
            if is_pipeline {
                if let Some(ref path) = args.pipeline_path {
                    eprintln!("Pipeline:   {}", path.display());
                }
            } else {
                eprintln!("Agents:     {}", args.agents.join(", "));
                if args.mode == ExecutionMode::Relay && !args.relay_order.is_empty() {
                    eprintln!("Order:      {}", args.relay_order.join(" → "));
                }
            }
            eprintln!("Iterations: {effective_iterations}");
            if args.runs > 1 {
                eprintln!("Runs:       {}", args.runs);
                if args.concurrency > 0 {
                    eprintln!("Concurrency: {}", args.concurrency);
                }
            }
            if let Some(ref name) = args.session_name {
                eprintln!("Session:    {name}");
            }
            if let Some(ref agent) = args.consolidate_agent {
                eprintln!("Consolidate: {agent}");
            }
            if let Some(ref prompt) = args.prompt {
                let truncated = if prompt.len() > 120 {
                    let mut end = 120;
                    while end > 0 && !prompt.is_char_boundary(end) {
                        end -= 1;
                    }
                    format!("{}...", &prompt[..end])
                } else {
                    prompt.clone()
                };
                eprintln!("Prompt:     {truncated}");
            }
            eprintln!("---");
        }
        OutputFormat::Json => {
            let obj = serde_json::json!({
                "event": "run_info",
                "mode": mode_str,
                "agents": if is_pipeline { vec![] } else { args.agents.clone() },
                "iterations": effective_iterations,
                "runs": args.runs,
                "concurrency": args.concurrency,
                "session_name": args.session_name,
                "pipeline": args.pipeline_path.as_ref().map(|p| p.display().to_string()),
                "consolidate": args.consolidate_agent,
                "forward_prompt": args.forward_prompt,
            });
            eprintln!("{}", serde_json::to_string(&obj).unwrap_or_default());
        }
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub(crate) async fn run(args: HeadlessArgs, config: AppConfig) -> i32 {
    if let Err(e) = validate_args(&args, &config) {
        match args.output_format {
            OutputFormat::Json => {
                let obj = serde_json::json!({"event": "result", "status": "error", "error": e});
                println!("{}", serde_json::to_string(&obj).unwrap_or_default());
            }
            OutputFormat::Text => {
                eprintln!("Error: {e}");
            }
        }
        return EXIT_VALIDATION;
    }

    // For pipeline runs, load and prepare the definition once up front so
    // the single/batch execution paths don't need to re-parse the TOML.
    let pipeline_def = if let Some(ref path) = args.pipeline_path {
        let mut def = match pipeline_mod::load_pipeline(path) {
            Ok(d) => d,
            Err(e) => {
                let msg = format!("Failed to load pipeline: {e}");
                match args.output_format {
                    OutputFormat::Json => {
                        let obj = serde_json::json!({"event": "result", "status": "error", "error": msg});
                        println!("{}", serde_json::to_string(&obj).unwrap_or_default());
                    }
                    OutputFormat::Text => eprintln!("Error: {msg}"),
                }
                return EXIT_VALIDATION;
            }
        };
        if let Some(ref prompt) = args.prompt {
            def.initial_prompt = prompt.clone();
        }
        if let Some(iters) = args.iterations {
            def.iterations = iters;
        }
        def.normalize_session_configs();
        if let Err(e) = pipeline_mod::validate_pipeline(&def) {
            let msg = e.to_string();
            match args.output_format {
                OutputFormat::Json => {
                    let obj = serde_json::json!({"event": "result", "status": "error", "error": msg});
                    println!("{}", serde_json::to_string(&obj).unwrap_or_default());
                }
                OutputFormat::Text => eprintln!("Error: {msg}"),
            }
            return EXIT_VALIDATION;
        }
        Some(def)
    } else {
        None
    };

    let effective_iterations = pipeline_def
        .as_ref()
        .map(|d| d.iterations)
        .unwrap_or_else(|| args.iterations.unwrap_or(1));
    print_run_info(&args, effective_iterations);

    let cancel = Arc::new(AtomicBool::new(false));
    install_signal_handler(cancel.clone(), args.quiet);

    let is_batch = args.runs > 1;

    let result = if let Some(pdef) = pipeline_def {
        if is_batch {
            run_batch_pipeline(&args, &config, pdef, cancel.clone()).await
        } else {
            run_single_pipeline(&args, &config, pdef, cancel.clone()).await
        }
    } else if is_batch {
        run_batch_standard(&args, &config, cancel.clone()).await
    } else {
        run_single_standard(&args, &config, cancel.clone()).await
    };

    match result {
        Ok(summary) => {
            let cancelled = cancel.load(Ordering::Relaxed);
            if cancelled {
                emit_final_result(&args, "cancelled", summary.run_dir.as_deref(), None, None);
                EXIT_CANCELLED
            } else {
                // Run post-run steps even on partial failure — successful
                // runs in a batch still benefit from consolidation/diagnostics.
                let (post_run_partial, post_run_err_msg) =
                    run_post_steps(&args, &config, &summary, cancel.clone()).await;

                if let Some(ref e) = post_run_err_msg {
                    if !args.quiet {
                        eprintln!("Post-run step failed: {e}");
                    }
                }

                // Re-check cancel after post-run steps — Ctrl+C during
                // consolidation/diagnostics should still exit 130.
                if cancel.load(Ordering::Relaxed) {
                    emit_final_result(&args, "cancelled", summary.run_dir.as_deref(), None, None);
                    return EXIT_CANCELLED;
                }

                let extra = (post_run_partial.consolidation, post_run_partial.diagnostics);

                if summary.failed || post_run_err_msg.is_some() {
                    // Combine execution + post-run errors for the final payload.
                    let error_msg = match (&summary.error, &post_run_err_msg) {
                        (Some(exec), Some(post)) => format!("{exec}; {post}"),
                        (Some(exec), None) => exec.clone(),
                        (None, Some(post)) => post.clone(),
                        (None, None) => "Execution failed".to_string(),
                    };
                    emit_final_result(
                        &args,
                        "error",
                        summary.run_dir.as_deref(),
                        Some(&error_msg),
                        Some(FinalResultExtra {
                            consolidation: extra.0.as_deref(),
                            diagnostics: extra.1.as_deref(),
                        }),
                    );
                    EXIT_EXECUTION
                } else {
                    emit_final_result(
                        &args,
                        "ok",
                        summary.run_dir.as_deref(),
                        None,
                        Some(FinalResultExtra {
                            consolidation: extra.0.as_deref(),
                            diagnostics: extra.1.as_deref(),
                        }),
                    );
                    EXIT_OK
                }
            }
        }
        Err(e) => {
            if cancel.load(Ordering::Relaxed) {
                emit_final_result(&args, "cancelled", None, None, None);
                EXIT_CANCELLED
            } else {
                let msg = e.message();
                if matches!(args.output_format, OutputFormat::Text) {
                    eprintln!("Error: {msg}");
                }
                emit_final_result(&args, "error", None, Some(msg), None);
                e.exit_code()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Run summaries
// ---------------------------------------------------------------------------

struct RunSummary {
    run_dir: Option<String>,
    mode: ExecutionMode,
    agents: Vec<String>,
    runs: u32,
    failed: bool,
    error: Option<String>,
    successful_runs: Vec<u32>,
    #[allow(dead_code)]
    failed_runs: Vec<u32>,
    base_errors: Vec<String>,
}

struct PostRunResult {
    consolidation: Option<String>,
    diagnostics: Option<String>,
}

struct FinalResultExtra<'a> {
    consolidation: Option<&'a str>,
    diagnostics: Option<&'a str>,
}

// ---------------------------------------------------------------------------
// Single relay/swarm run
// ---------------------------------------------------------------------------

async fn run_single_standard(
    args: &HeadlessArgs,
    config: &AppConfig,
    cancel: Arc<AtomicBool>,
) -> Result<RunSummary, HeadlessError> {
    let cli_available = rs::detect_cli_availability();
    let session_overrides = rs::compute_session_overrides(&config.agents, &cli_available);
    let prompt = args.prompt.as_deref().unwrap_or("").to_string();
    let iterations = args.iterations.unwrap_or(1);
    let agent_names = if args.mode == ExecutionMode::Relay && !args.relay_order.is_empty() {
        args.relay_order.clone()
    } else {
        args.agents.clone()
    };

    let resolved = rs::resolve_selected_agent_configs(
        &agent_names,
        &session_overrides,
        &config.agents,
        &cli_available,
    )?;

    let http_timeout_secs = config.http_timeout_seconds.max(1);
    let cli_timeout_secs = config.cli_timeout_seconds.max(1);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(http_timeout_secs))
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {e}"))?;

    let use_cli_by_agent: HashMap<String, bool> = resolved
        .iter()
        .map(|a| (a.name.clone(), a.use_cli))
        .collect();

    let providers: Vec<(String, Box<dyn provider::Provider>)> = resolved
        .iter()
        .map(|a| {
            let p = provider::create_provider(
                a.provider,
                &a.to_provider_config(),
                client.clone(),
                config.default_max_tokens,
                config.max_history_messages,
                config.max_history_bytes,
                cli_timeout_secs,
            );
            (a.name.clone(), p)
        })
        .collect();

    let prompt_context =
        PromptRuntimeContext::new(prompt.clone(), config.diagnostic_provider.is_some());
    let output_dir = config.resolved_output_dir();
    let output = OutputManager::new(&output_dir, args.session_name.as_deref())
        .map_err(|e| format!("Failed to create output directory: {e}"))?;
    let run_dir = output.run_dir().display().to_string();
    emit_run_dir_early(args, &run_dir);

    output
        .write_prompt(prompt_context.raw_prompt())
        .map_err(|e| format!("Failed to write prompt: {e}"))?;
    let agent_info: Vec<(String, String)> = resolved
        .iter()
        .map(|a| (a.name.clone(), a.provider.config_key().to_string()))
        .collect();
    let run_models: Vec<(String, String)> = resolved
        .iter()
        .map(|a| {
            (
                a.name.clone(),
                if a.model.is_empty() {
                    "(default)".to_string()
                } else {
                    a.model.clone()
                },
            )
        })
        .collect();
    output
        .write_session_info(
            &args.mode,
            &agent_info,
            iterations,
            args.session_name.as_deref(),
            &run_models,
            args.keep_session,
        )
        .map_err(|e| format!("Failed to write session metadata: {e}"))?;

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let mut logger = ProgressLogger::new(args.output_format, args.quiet, iterations, 1);
    let task_cancel = cancel.clone();

    let handle = tokio::spawn({
        let prompt_context = prompt_context.clone();
        let mode = args.mode;
        let forward_prompt = args.forward_prompt;
        let keep_session = args.keep_session;
        async move {
            let result = match mode {
                ExecutionMode::Relay => {
                    run_relay(
                        &prompt_context,
                        providers,
                        iterations,
                        1,
                        None,
                        forward_prompt,
                        keep_session,
                        use_cli_by_agent,
                        &output,
                        progress_tx.clone(),
                        task_cancel,
                    )
                    .await
                }
                ExecutionMode::Swarm => {
                    run_swarm(
                        &prompt_context,
                        providers,
                        iterations,
                        1,
                        HashMap::new(),
                        keep_session,
                        use_cli_by_agent,
                        &output,
                        progress_tx.clone(),
                        task_cancel,
                    )
                    .await
                }
                _ => unreachable!(),
            };
            // Executors send AllDone on Ok(()); only send on Err.
            if let Err(e) = &result {
                let _ = output.append_error(&format!("Execution failed: {e}"));
                let _ = progress_tx.send(ProgressEvent::AllDone);
            }
            result
        }
    });

    // Drain progress
    while let Some(event) = progress_rx.recv().await {
        let is_done = matches!(event, ProgressEvent::AllDone);
        logger.log_progress(&event);
        if is_done {
            break;
        }
    }

    let task_result = handle.await.map_err(|e| format!("Task panicked: {e}"))?;
    let has_agent_errors = logger.has_errors();
    let has_error_log = has_nonempty_error_log(&run_dir);
    let failed = task_result.is_err() || has_agent_errors || has_error_log;
    let error = task_result
        .err()
        .map(|e| e.to_string())
        .or_else(|| {
            (has_agent_errors || has_error_log)
                .then(|| "One or more agents reported errors".to_string())
        });

    Ok(RunSummary {
        run_dir: Some(run_dir),
        mode: args.mode,
        agents: agent_names,
        runs: 1,
        failed,
        error,
        successful_runs: if failed { vec![] } else { vec![1] },
        failed_runs: if failed { vec![1] } else { vec![] },
        base_errors: logger.drain_errors(),
    })
}

// ---------------------------------------------------------------------------
// Single pipeline run
// ---------------------------------------------------------------------------

async fn run_single_pipeline(
    args: &HeadlessArgs,
    config: &AppConfig,
    pipeline_def: pipeline_mod::PipelineDefinition,
    cancel: Arc<AtomicBool>,
) -> Result<RunSummary, HeadlessError> {
    let cli_available = rs::detect_cli_availability();
    let session_overrides = rs::compute_session_overrides(&config.agents, &cli_available);

    // Validate all referenced agents
    for block in &pipeline_def.blocks {
        for agent_name in &block.agents {
            let agent_config =
                rs::resolve_agent_config(agent_name, &session_overrides, &config.agents)
                    .ok_or_else(|| {
                        HeadlessError::Validation(format!(
                            "Pipeline agent '{agent_name}' is not configured"
                        ))
                    })?;
            rs::validate_agent_runtime(&cli_available, agent_name, agent_config)
                .map_err(HeadlessError::Validation)?;
        }
    }

    let agent_configs =
        rs::build_pipeline_agent_configs(&pipeline_def, &config.agents, &session_overrides);

    let http_timeout_secs = config.http_timeout_seconds.max(1);
    let cli_timeout_secs = config.cli_timeout_seconds.max(1);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(http_timeout_secs))
        .build()
        .map_err(|e| HeadlessError::Execution(format!("Failed to create HTTP client: {e}")))?;

    let prompt_context = PromptRuntimeContext::new(
        pipeline_def.initial_prompt.clone(),
        config.diagnostic_provider.is_some(),
    );

    let rt = pipeline_mod::build_runtime_table(&pipeline_def);
    let loop_extra = pipeline_mod::loop_extra_tasks(&pipeline_def);

    let output_dir = config.resolved_output_dir();
    let output = OutputManager::new(&output_dir, args.session_name.as_deref())
        .map_err(|e| HeadlessError::Execution(format!("Failed to create output directory: {e}")))?;
    let run_dir = output.run_dir().display().to_string();
    emit_run_dir_early(args, &run_dir);

    output
        .write_prompt(prompt_context.raw_prompt())
        .map_err(|e| HeadlessError::Execution(format!("Failed to write prompt: {e}")))?;
    output
        .write_pipeline_session_info(
            pipeline_def.blocks.len(),
            pipeline_def.connections.len(),
            pipeline_def.loop_connections.len(),
            pipeline_def.iterations,
            rt.entries.len() + loop_extra,
            args.pipeline_path
                .as_deref()
                .map(|p| p.to_string_lossy().to_string())
                .as_deref(),
        )
        .map_err(|e| HeadlessError::Execution(format!("Failed to write pipeline session metadata: {e}")))?;

    // Write pipeline.toml snapshot
    match toml::to_string_pretty(&pipeline_def) {
        Ok(toml_str) => {
            if let Err(e) = std::fs::write(output.run_dir().join("pipeline.toml"), &toml_str) {
                let _ = output.append_error(&format!("Failed to write pipeline.toml: {e}"));
                return Err(HeadlessError::Execution(format!("Failed to write pipeline.toml: {e}")));
            }
        }
        Err(e) => {
            let _ = output.append_error(&format!("Failed to serialize pipeline.toml: {e}"));
            return Err(HeadlessError::Execution(format!("Failed to serialize pipeline.toml: {e}")));
        }
    }

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let mut logger =
        ProgressLogger::new(args.output_format, args.quiet, pipeline_def.iterations, 1);
    let task_cancel = cancel.clone();

    let handle = tokio::spawn({
        let pipeline_def = pipeline_def.clone();
        let config = config.clone();
        let prompt_context = prompt_context.clone();
        async move {
            let result = pipeline_mod::run_pipeline(
                &pipeline_def,
                &config,
                agent_configs,
                client,
                cli_timeout_secs,
                &prompt_context,
                &output,
                progress_tx.clone(),
                task_cancel,
            )
            .await;
            if let Err(e) = &result {
                let _ = progress_tx.send(ProgressEvent::AllDone);
                let _ = output.append_error(&format!("Pipeline failed: {e}"));
            }
            result
        }
    });

    while let Some(event) = progress_rx.recv().await {
        let is_done = matches!(event, ProgressEvent::AllDone);
        logger.log_progress(&event);
        if is_done {
            break;
        }
    }

    let task_result = handle.await.map_err(|e| format!("Task panicked: {e}"))?;
    let has_agent_errors = logger.has_errors();
    let has_error_log = has_nonempty_error_log(&run_dir);
    let failed = task_result.is_err() || has_agent_errors || has_error_log;
    let error = task_result
        .err()
        .map(|e| e.to_string())
        .or_else(|| {
            (has_agent_errors || has_error_log)
                .then(|| "One or more agents reported errors".to_string())
        });

    Ok(RunSummary {
        run_dir: Some(run_dir),
        mode: ExecutionMode::Pipeline,
        agents: vec![],
        runs: 1,
        failed,
        error,
        successful_runs: if failed { vec![] } else { vec![1] },
        failed_runs: if failed { vec![1] } else { vec![] },
        base_errors: logger.drain_errors(),
    })
}

// ---------------------------------------------------------------------------
// Batch relay/swarm run
// ---------------------------------------------------------------------------

async fn run_batch_standard(
    args: &HeadlessArgs,
    config: &AppConfig,
    cancel: Arc<AtomicBool>,
) -> Result<RunSummary, HeadlessError> {
    let cli_available = rs::detect_cli_availability();
    let session_overrides = rs::compute_session_overrides(&config.agents, &cli_available);
    let prompt = args.prompt.as_deref().unwrap_or("").to_string();
    let iterations = args.iterations.unwrap_or(1);
    let agent_names = if args.mode == ExecutionMode::Relay && !args.relay_order.is_empty() {
        args.relay_order.clone()
    } else {
        args.agents.clone()
    };

    let resolved = rs::resolve_selected_agent_configs(
        &agent_names,
        &session_overrides,
        &config.agents,
        &cli_available,
    )?;

    let concurrency = rs::effective_concurrency(args.runs, args.concurrency);

    let prompt_context =
        PromptRuntimeContext::new(prompt.clone(), config.diagnostic_provider.is_some());
    let output_dir = config.resolved_output_dir();
    let batch_root = OutputManager::new_batch_parent(&output_dir, args.session_name.as_deref())
        .map_err(|e| format!("Failed to create batch directory: {e}"))?;

    let step_labels: Vec<String> = agent_names.clone();
    batch_root
        .write_batch_info(args.runs, concurrency, &args.mode, &step_labels, iterations)
        .map_err(|e| format!("Failed to write batch metadata: {e}"))?;

    let (batch_tx, mut batch_rx) = mpsc::unbounded_channel();
    let mut logger = ProgressLogger::new(args.output_format, args.quiet, iterations, args.runs);

    let batch_dir = batch_root.run_dir().display().to_string();
    emit_run_dir_early(args, &batch_dir);
    let task_cancel = cancel.clone();

    let config_clone = config.clone();
    let mode = args.mode;
    let forward_prompt = args.forward_prompt;
    let keep_session = args.keep_session;
    let session_name: Option<Arc<str>> = args
        .session_name
        .as_deref()
        .map(Arc::from);
    let runs = args.runs;
    let http_timeout_secs = config.http_timeout_seconds.max(1);
    let cli_timeout_secs = config.cli_timeout_seconds.max(1);
    let resolved = Arc::new(resolved);
    let prompt_context = Arc::new(prompt_context);
    let batch_root_dir = batch_root.run_dir().to_path_buf();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(http_timeout_secs))
        .build()
        .map_err(|e| format!("HTTP client error: {e}"))?;

    let handle = tokio::spawn(async move {
        run_multi(
            runs,
            concurrency,
            batch_tx,
            task_cancel,
            move |run_id, progress_tx, run_cancel| {
                let client = client.clone();
                let config = config_clone.clone();
                let resolved = resolved.clone();
                let prompt_context = prompt_context.clone();
                let batch_root_dir = batch_root_dir.clone();
                let session_name = session_name.clone();
                async move {
                    let parent_output = match OutputManager::from_existing(batch_root_dir.clone()) {
                        Ok(o) => o,
                        Err(e) => {
                            let _ = progress_tx.send(ProgressEvent::AllDone);
                            return (RunOutcome::Failed, Some(format!("Output error: {e}")));
                        }
                    };
                    let output = match parent_output.new_run_subdir(run_id) {
                        Ok(o) => o,
                        Err(e) => {
                            let _ = progress_tx.send(ProgressEvent::AllDone);
                            return (RunOutcome::Failed, Some(format!("Output error: {e}")));
                        }
                    };

                    if let Err(e) = output.write_prompt(prompt_context.raw_prompt()) {
                        let _ = progress_tx.send(ProgressEvent::AllDone);
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write prompt: {e}")),
                        );
                    }
                    let agent_info: Vec<(String, String)> = resolved
                        .iter()
                        .map(|a| (a.name.clone(), a.provider.config_key().to_string()))
                        .collect();
                    let run_models: Vec<(String, String)> = resolved
                        .iter()
                        .map(|a| {
                            (
                                a.name.clone(),
                                if a.model.is_empty() {
                                    "(default)".to_string()
                                } else {
                                    a.model.clone()
                                },
                            )
                        })
                        .collect();
                    if let Err(e) = output.write_session_info(
                        &mode,
                        &agent_info,
                        iterations,
                        session_name.as_deref(),
                        &run_models,
                        keep_session,
                    ) {
                        let _ = progress_tx.send(ProgressEvent::AllDone);
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write session metadata: {e}")),
                        );
                    }

                    let use_cli_by_agent: HashMap<String, bool> = resolved
                        .iter()
                        .map(|a| (a.name.clone(), a.use_cli))
                        .collect();

                    let providers: Vec<(String, Box<dyn provider::Provider>)> = resolved
                        .iter()
                        .map(|a| {
                            let p = provider::create_provider(
                                a.provider,
                                &a.to_provider_config(),
                                client.clone(),
                                config.default_max_tokens,
                                config.max_history_messages,
                                config.max_history_bytes,
                                cli_timeout_secs,
                            );
                            (a.name.clone(), p)
                        })
                        .collect();

                    let cancel_flag = run_cancel.clone();
                    let result = match mode {
                        ExecutionMode::Relay => {
                            run_relay(
                                &prompt_context,
                                providers,
                                iterations,
                                1,
                                None,
                                forward_prompt,
                                keep_session,
                                use_cli_by_agent,
                                &output,
                                progress_tx.clone(),
                                run_cancel,
                            )
                            .await
                        }
                        ExecutionMode::Swarm => {
                            run_swarm(
                                &prompt_context,
                                providers,
                                iterations,
                                1,
                                HashMap::new(),
                                keep_session,
                                use_cli_by_agent,
                                &output,
                                progress_tx.clone(),
                                run_cancel,
                            )
                            .await
                        }
                        _ => unreachable!(),
                    };

                    if cancel_flag.load(Ordering::Relaxed) {
                        return (RunOutcome::Cancelled, None);
                    }

                    // Executors send AllDone on Ok(()); only send on Err.
                    match result {
                        Ok(()) => {
                            let has_errors = match output
                                .run_dir()
                                .join("_errors.log")
                                .metadata()
                            {
                                Ok(m) => m.len() > 0,
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                                Err(_) => true, // Can't verify — assume errors for safety
                            };
                            if has_errors {
                                (
                                    RunOutcome::Failed,
                                    Some("One or more agents reported errors".to_string()),
                                )
                            } else {
                                (RunOutcome::Done, None)
                            }
                        }
                        Err(e) => {
                            let _ = output.append_error(&format!("Run failed: {e}"));
                            let _ = progress_tx.send(ProgressEvent::AllDone);
                            (RunOutcome::Failed, Some(e.to_string()))
                        }
                    }
                }
            },
        )
        .await;
    });

    // Drain batch progress
    let mut successful_runs = Vec::new();
    let mut failed_runs = Vec::new();
    while let Some(event) = batch_rx.recv().await {
        let is_done = matches!(event, BatchProgressEvent::AllRunsDone);
        if let BatchProgressEvent::RunFinished {
            run_id, outcome, ..
        } = &event
        {
            match outcome {
                RunOutcome::Done => successful_runs.push(*run_id),
                RunOutcome::Failed | RunOutcome::Cancelled => failed_runs.push(*run_id),
            }
        }
        logger.log_batch_progress(&event);
        if is_done {
            break;
        }
    }

    if let Err(e) = handle.await {
        return Err(format!("Batch task panicked: {e}").into());
    }
    // Sort for deterministic consolidation order (completion order is arbitrary).
    successful_runs.sort_unstable();
    failed_runs.sort_unstable();
    let failed = !failed_runs.is_empty();

    Ok(RunSummary {
        run_dir: Some(batch_dir),
        mode: args.mode,
        agents: agent_names,
        runs: args.runs,
        failed,
        error: if failed {
            Some(format!(
                "{} of {} runs failed",
                failed_runs.len(),
                args.runs
            ))
        } else {
            None
        },
        successful_runs,
        failed_runs,
        base_errors: logger.drain_errors(),
    })
}

// ---------------------------------------------------------------------------
// Batch pipeline run
// ---------------------------------------------------------------------------

async fn run_batch_pipeline(
    args: &HeadlessArgs,
    config: &AppConfig,
    pipeline_def: pipeline_mod::PipelineDefinition,
    cancel: Arc<AtomicBool>,
) -> Result<RunSummary, HeadlessError> {
    let cli_available = rs::detect_cli_availability();
    let session_overrides = rs::compute_session_overrides(&config.agents, &cli_available);

    for block in &pipeline_def.blocks {
        for agent_name in &block.agents {
            let agent_config =
                rs::resolve_agent_config(agent_name, &session_overrides, &config.agents)
                    .ok_or_else(|| {
                        HeadlessError::Validation(format!(
                            "Pipeline agent '{agent_name}' is not configured"
                        ))
                    })?;
            rs::validate_agent_runtime(&cli_available, agent_name, agent_config)
                .map_err(HeadlessError::Validation)?;
        }
    }

    let concurrency = rs::effective_concurrency(args.runs, args.concurrency);
    let step_labels = pipeline_mod::pipeline_step_labels(&pipeline_def);

    let output_dir = config.resolved_output_dir();
    let batch_root = OutputManager::new_batch_parent(&output_dir, args.session_name.as_deref())
        .map_err(|e| format!("Failed to create batch directory: {e}"))?;

    batch_root
        .write_batch_info(
            args.runs,
            concurrency,
            &ExecutionMode::Pipeline,
            &step_labels,
            pipeline_def.iterations,
        )
        .map_err(|e| format!("Failed to write batch metadata: {e}"))?;

    let (batch_tx, mut batch_rx) = mpsc::unbounded_channel();
    let mut logger = ProgressLogger::new(
        args.output_format,
        args.quiet,
        pipeline_def.iterations,
        args.runs,
    );

    let batch_dir = batch_root.run_dir().display().to_string();
    emit_run_dir_early(args, &batch_dir);
    let task_cancel = cancel.clone();

    let config_clone = config.clone();
    let agent_configs = Arc::new(rs::build_pipeline_agent_configs(
        &pipeline_def,
        &config.agents,
        &session_overrides,
    ));
    // Pre-compute immutable pipeline metadata once instead of per-run.
    let rt = pipeline_mod::build_runtime_table(&pipeline_def);
    let loop_extra = pipeline_mod::loop_extra_tasks(&pipeline_def);
    let total_tasks = rt.entries.len() + loop_extra;
    let pipeline_toml_str = Arc::new(
        toml::to_string_pretty(&pipeline_def)
            .map_err(|e| format!("Failed to serialize pipeline: {e}"))?,
    );
    let pipeline_def = Arc::new(pipeline_def);
    let batch_root_dir = batch_root.run_dir().to_path_buf();
    let pipeline_path_clone = args.pipeline_path.clone();
    let runs = args.runs;
    let http_timeout_secs = config.http_timeout_seconds.max(1);
    let cli_timeout_secs = config.cli_timeout_seconds.max(1);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(http_timeout_secs))
        .build()
        .map_err(|e| format!("HTTP client error: {e}"))?;

    let handle = tokio::spawn(async move {
        run_multi(
            runs,
            concurrency,
            batch_tx,
            task_cancel,
            move |run_id, progress_tx, run_cancel| {
                let client = client.clone();
                let config = config_clone.clone();
                let pipeline_def = pipeline_def.clone();
                let agent_configs = agent_configs.clone();
                let batch_root_dir = batch_root_dir.clone();
                let pipeline_path = pipeline_path_clone.clone();
                let pipeline_toml_str = pipeline_toml_str.clone();
                async move {
                    let parent_output = match OutputManager::from_existing(batch_root_dir.clone()) {
                        Ok(o) => o,
                        Err(e) => {
                            let _ = progress_tx.send(ProgressEvent::AllDone);
                            return (RunOutcome::Failed, Some(format!("Output error: {e}")));
                        }
                    };
                    let output = match parent_output.new_run_subdir(run_id) {
                        Ok(o) => o,
                        Err(e) => {
                            let _ = progress_tx.send(ProgressEvent::AllDone);
                            return (RunOutcome::Failed, Some(format!("Output error: {e}")));
                        }
                    };

                    let prompt_context = PromptRuntimeContext::new(
                        pipeline_def.initial_prompt.clone(),
                        config.diagnostic_provider.is_some(),
                    );

                    if let Err(e) = output.write_prompt(prompt_context.raw_prompt()) {
                        let _ = progress_tx.send(ProgressEvent::AllDone);
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write prompt: {e}")),
                        );
                    }
                    if let Err(e) = output.write_pipeline_session_info(
                        pipeline_def.blocks.len(),
                        pipeline_def.connections.len(),
                        pipeline_def.loop_connections.len(),
                        pipeline_def.iterations,
                        total_tasks,
                        pipeline_path
                            .as_deref()
                            .map(|p| p.to_string_lossy().to_string())
                            .as_deref(),
                    ) {
                        let _ = progress_tx.send(ProgressEvent::AllDone);
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write pipeline session metadata: {e}")),
                        );
                    }

                    if let Err(e) =
                        std::fs::write(output.run_dir().join("pipeline.toml"), &*pipeline_toml_str)
                    {
                        let _ = output
                            .append_error(&format!("Failed to write pipeline.toml: {e}"));
                        let _ = progress_tx.send(ProgressEvent::AllDone);
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write pipeline.toml: {e}")),
                        );
                    }

                    let result = pipeline_mod::run_pipeline(
                        &pipeline_def,
                        &config,
                        (*agent_configs).clone(),
                        client,
                        cli_timeout_secs,
                        &prompt_context,
                        &output,
                        progress_tx.clone(),
                        run_cancel.clone(),
                    )
                    .await;

                    if run_cancel.load(Ordering::Relaxed) {
                        return (RunOutcome::Cancelled, None);
                    }

                    // Executors send AllDone on Ok(()); only send on Err.
                    match result {
                        Ok(()) => {
                            let has_errors = match output
                                .run_dir()
                                .join("_errors.log")
                                .metadata()
                            {
                                Ok(m) => m.len() > 0,
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                                Err(_) => true, // Can't verify — assume errors for safety
                            };
                            if has_errors {
                                (
                                    RunOutcome::Failed,
                                    Some("One or more agents reported errors".to_string()),
                                )
                            } else {
                                (RunOutcome::Done, None)
                            }
                        }
                        Err(e) => {
                            let _ = output.append_error(&format!("Pipeline failed: {e}"));
                            let _ = progress_tx.send(ProgressEvent::AllDone);
                            (RunOutcome::Failed, Some(e.to_string()))
                        }
                    }
                }
            },
        )
        .await;
    });

    let mut successful_runs = Vec::new();
    let mut failed_runs = Vec::new();
    while let Some(event) = batch_rx.recv().await {
        let is_done = matches!(event, BatchProgressEvent::AllRunsDone);
        if let BatchProgressEvent::RunFinished {
            run_id, outcome, ..
        } = &event
        {
            match outcome {
                RunOutcome::Done => successful_runs.push(*run_id),
                RunOutcome::Failed | RunOutcome::Cancelled => failed_runs.push(*run_id),
            }
        }
        logger.log_batch_progress(&event);
        if is_done {
            break;
        }
    }

    if let Err(e) = handle.await {
        return Err(format!("Batch pipeline task panicked: {e}").into());
    }
    // Sort for deterministic consolidation order (completion order is arbitrary).
    successful_runs.sort_unstable();
    failed_runs.sort_unstable();
    let failed = !failed_runs.is_empty();

    Ok(RunSummary {
        run_dir: Some(batch_dir),
        mode: ExecutionMode::Pipeline,
        agents: vec![],
        runs: args.runs,
        failed,
        error: if failed {
            Some(format!(
                "{} of {} runs failed",
                failed_runs.len(),
                args.runs
            ))
        } else {
            None
        },
        successful_runs,
        failed_runs,
        base_errors: logger.drain_errors(),
    })
}

// ---------------------------------------------------------------------------
// Post-run steps (consolidation + diagnostics)
// ---------------------------------------------------------------------------

/// Returns `(partial_results, optional_error)`. Partial results are always
/// populated with whatever succeeded before a failure, so callers can
/// include consolidation/diagnostics paths in the final payload even when
/// a later step errors out.
async fn run_post_steps(
    args: &HeadlessArgs,
    config: &AppConfig,
    summary: &RunSummary,
    cancel: Arc<AtomicBool>,
) -> (PostRunResult, Option<String>) {
    let mut result = PostRunResult {
        consolidation: None,
        diagnostics: None,
    };

    let run_dir = match summary.run_dir.as_ref() {
        Some(d) => PathBuf::from(d),
        None => return (result, None),
    };

    if cancel.load(Ordering::Relaxed) {
        return (result, None);
    }

    // Consolidation — for single runs, `run_consolidation_steps` discovers
    // outputs and skips if there aren't enough. This matches TUI behavior
    // where partial failures (e.g. 2 of 3 swarm agents) still consolidate.
    let mut consolidation_error = None;
    if let Some(ref agent_name) = args.consolidate_agent {
        if let Err(e) =
            run_consolidation_steps(&run_dir, args, config, summary, agent_name, &mut result).await
        {
            consolidation_error = Some(e);
        }
    }

    // Diagnostics — always run even if consolidation failed (matches TUI
    // behavior where diagnostics run regardless of consolidation outcome).
    if cancel.load(Ordering::Relaxed) {
        return (result, consolidation_error);
    }

    // Diagnostics are best-effort — a broken or missing diagnostics config
    // must not turn an otherwise successful run into a failure (matches TUI
    // behavior where diagnostics errors are logged, not fatal).
    if let Some(ref diag_name) = config.diagnostic_provider {
        if let Err(e) = run_diagnostics_step(
            &run_dir,
            config,
            summary,
            diag_name,
            args.quiet,
            &mut result,
        )
        .await
        {
            if !args.quiet {
                eprintln!("Diagnostics failed (non-blocking): {e}");
            }
        }
    }

    (result, consolidation_error)
}

/// Consolidation sub-step — extracted so `run_post_steps` can capture partial
/// results before returning an error.
async fn run_consolidation_steps(
    run_dir: &std::path::Path,
    args: &HeadlessArgs,
    config: &AppConfig,
    summary: &RunSummary,
    agent_name: &str,
    result: &mut PostRunResult,
) -> Result<(), String> {
    let cli_available = rs::detect_cli_availability();
    let session_overrides = rs::compute_session_overrides(&config.agents, &cli_available);
    let agent_config = rs::resolve_agent_config(agent_name, &session_overrides, &config.agents)
        .cloned()
        .ok_or_else(|| format!("Consolidation agent '{agent_name}' not configured"))?;

    let is_batch = summary.runs > 1;
    let is_single_relay = !is_batch && summary.mode == ExecutionMode::Relay;

    if is_single_relay {
        // Unreachable in practice — validation rejects this combination.
        return Err("Consolidation is not supported for single relay runs".into());
    } else if is_batch {
        if !summary.successful_runs.is_empty() {
            // Per-run consolidation
            let per_run_msg = run_consolidation(
                run_dir,
                crate::app::ConsolidationTarget::PerRun,
                summary,
                &agent_config,
                config,
                &args.consolidation_prompt,
            )
            .await
            .map_err(|e| format!("Per-run consolidation failed: {e}"))?;
            if !args.quiet {
                eprintln!("Per-run consolidation: {per_run_msg}");
            }
            // Record per-run result so it survives if cross-run fails.
            result.consolidation = Some(per_run_msg);

            // Cross-run consolidation
            let msg = run_consolidation(
                run_dir,
                crate::app::ConsolidationTarget::AcrossRuns,
                summary,
                &agent_config,
                config,
                &args.consolidation_prompt,
            )
            .await
            .map_err(|e| format!("Cross-run consolidation failed: {e}"))?;
            result.consolidation = Some(msg);
        }
    } else {
        // Single swarm/pipeline
        let outputs = post_run::discover_final_outputs(run_dir, summary.mode, &summary.agents);
        if outputs.len() > 1 {
            let msg = run_consolidation(
                run_dir,
                crate::app::ConsolidationTarget::Single,
                summary,
                &agent_config,
                config,
                &args.consolidation_prompt,
            )
            .await
            .map_err(|e| format!("Consolidation failed: {e}"))?;
            result.consolidation = Some(msg);
        } else if !args.quiet {
            eprintln!("Skipping consolidation (not enough outputs)");
        }
    }
    Ok(())
}

/// Diagnostics sub-step — extracted so `run_post_steps` can capture partial
/// results before returning an error.
async fn run_diagnostics_step(
    run_dir: &std::path::Path,
    config: &AppConfig,
    summary: &RunSummary,
    diag_name: &str,
    quiet: bool,
    result: &mut PostRunResult,
) -> Result<(), String> {
    let cli_available = rs::detect_cli_availability();
    let session_overrides = rs::compute_session_overrides(&config.agents, &cli_available);
    let diag_config = rs::resolve_agent_config(diag_name, &session_overrides, &config.agents)
        .cloned()
        .ok_or_else(|| format!("Diagnostic agent '{diag_name}' not configured"))?;
    rs::validate_agent_runtime(&cli_available, diag_name, &diag_config)?;

    let pconfig = diag_config.to_provider_config();
    let base_errors = summary.base_errors.clone();
    let run_dir_owned = run_dir.to_path_buf();
    let use_cli = pconfig.use_cli;

    let prompt_result = tokio::task::spawn_blocking(move || {
        let report_files = post_run::collect_report_files(&run_dir_owned);
        let app_errors = post_run::collect_application_errors(&base_errors, &run_dir_owned);
        post_run::build_diagnostic_prompt(&report_files, &app_errors, use_cli)
    })
    .await
    .map_err(|e| format!("Diagnostic preparation failed: {e}"))?;

    let prompt = prompt_result?;

    let http_timeout_secs = config.http_timeout_seconds.max(1);
    let cli_timeout_secs = config.cli_timeout_seconds.max(1);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(http_timeout_secs))
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {e}"))?;

    let mut diag_provider = provider::create_provider(
        diag_config.provider,
        &pconfig,
        client,
        config.default_max_tokens,
        config.max_history_messages,
        config.max_history_bytes,
        cli_timeout_secs,
    );

    if !quiet {
        eprintln!("Running diagnostics...");
    }

    let response = diag_provider
        .send(&prompt)
        .await
        .map_err(|e| e.to_string())?;
    let output_path = run_dir.join("errors.md");
    tokio::fs::write(&output_path, &response.content)
        .await
        .map_err(|e| format!("Failed to write errors.md: {e}"))?;

    result.diagnostics = Some(output_path.display().to_string());
    Ok(())
}

async fn run_consolidation(
    run_dir: &std::path::Path,
    target: crate::app::ConsolidationTarget,
    summary: &RunSummary,
    agent_config: &AgentConfig,
    config: &AppConfig,
    additional: &str,
) -> Result<String, String> {
    let provider_kind = agent_config.provider;
    let provider_config = agent_config.to_provider_config();
    let http_timeout_secs = config.http_timeout_seconds.max(1);
    let cli_timeout_secs = config.cli_timeout_seconds.max(1);
    let default_max_tokens = config.default_max_tokens;
    let max_history_messages = config.max_history_messages;
    let max_history_bytes = config.max_history_bytes;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(http_timeout_secs))
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {e}"))?;

    post_run::run_consolidation_with_provider_factory(
        post_run::ConsolidationRequest {
            run_dir: run_dir.to_path_buf(),
            target,
            mode: summary.mode,
            selected_agents: summary.agents.clone(),
            successful_runs: summary.successful_runs.clone(),
            batch_stage1_done: matches!(target, crate::app::ConsolidationTarget::AcrossRuns),
            additional: additional.to_string(),
            agent_name: agent_config.name.clone(),
            agent_use_cli: agent_config.use_cli,
        },
        move || {
            provider::create_provider(
                provider_kind,
                &provider_config,
                client.clone(),
                default_max_tokens,
                max_history_messages,
                max_history_bytes,
                cli_timeout_secs,
            )
        },
    )
    .await
}

// ---------------------------------------------------------------------------
// Final result emission
// ---------------------------------------------------------------------------

fn emit_run_dir_early(args: &HeadlessArgs, run_dir: &str) {
    if args.quiet {
        return;
    }
    match args.output_format {
        OutputFormat::Text => {
            eprintln!("Output: {run_dir}");
        }
        OutputFormat::Json => {
            let obj = serde_json::json!({"event": "run_dir", "path": run_dir});
            eprintln!("{}", serde_json::to_string(&obj).unwrap_or_default());
        }
    }
}

fn emit_final_result(
    args: &HeadlessArgs,
    status: &str,
    run_dir: Option<&str>,
    error: Option<&str>,
    extra: Option<FinalResultExtra<'_>>,
) {
    match args.output_format {
        OutputFormat::Text => {
            if status == "ok" {
                if let Some(dir) = run_dir {
                    println!("{dir}");
                }
            } else if status == "cancelled" {
                if let Some(dir) = run_dir {
                    eprintln!("Cancelled. Output at: {dir}");
                }
            } else if let Some(err) = error {
                if let Some(dir) = run_dir {
                    eprintln!("Failed: {err}\nOutput at: {dir}");
                }
            }
        }
        OutputFormat::Json => {
            let mode = if args.pipeline_path.is_some() {
                "pipeline"
            } else {
                args.mode.as_str()
            };
            let consolidation = extra.as_ref().and_then(|e| e.consolidation);
            let diagnostics = extra.as_ref().and_then(|e| e.diagnostics);
            let obj = serde_json::json!({
                "event": "result",
                "status": status,
                "mode": mode,
                "run_dir": run_dir,
                "runs": args.runs,
                "consolidation": consolidation,
                "diagnostics": diagnostics,
                "error": error,
            });
            println!("{}", serde_json::to_string_pretty(&obj).unwrap_or_default());
        }
    }
}
