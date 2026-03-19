use super::execution::*;
use super::input::*;
use super::results::*;
use super::resume::*;
use super::setup_analysis::*;
use super::text_edit::*;
use super::*;
use crate::config::AppConfig;
use crate::error::AppError;
use crate::execution::ProgressEvent;
use crate::post_run::{
    build_diagnostic_prompt, build_file_consolidation_prompt, collect_application_errors,
    collect_report_files, discover_final_outputs, discover_final_outputs_async,
    find_last_iteration, find_last_iteration_async, is_pipeline_output_filename,
    parse_agent_iteration_filename, parse_iteration_from_filename,
    run_consolidation_with_provider_factory, ConsolidationRequest,
    POST_RUN_SYNTHESIS_MAX_INPUT_BYTES,
};
use crate::provider::{CompletionResponse, Provider};
use crossterm::event::KeyEvent;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use tokio::sync::mpsc;

fn test_config() -> AppConfig {
    AppConfig {
        output_dir: "/tmp".to_string(),
        default_max_tokens: 4096,
        max_history_messages: 50,
        http_timeout_seconds: 120,
        model_fetch_timeout_seconds: 30,
        cli_timeout_seconds: 600,
        max_history_bytes: 102400,
        pipeline_block_concurrency: 0,
        diagnostic_provider: None,
        memory: crate::config::MemoryConfig::default(),
        agents: Vec::new(),
        providers: HashMap::new(),
    }
}

fn test_app() -> App {
    App::new(test_config())
}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn write_session_toml(run_dir: &Path, mode: &str, agents: &[&str]) {
    write_session_toml_with_keep(run_dir, mode, agents, None);
}

fn write_session_toml_with_keep(
    run_dir: &Path,
    mode: &str,
    agents: &[&str],
    keep_session: Option<bool>,
) {
    let agent_values = agents
        .iter()
        .map(|a| format!("\"{a}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let keep_line = match keep_session {
        Some(v) => format!("keep_session = {v}\n"),
        None => String::new(),
    };
    fs::write(
        run_dir.join("session.toml"),
        format!("mode = \"{mode}\"\nagents = [{agent_values}]\n{keep_line}"),
    )
    .unwrap();
}

fn write_agent_iter(run_dir: &Path, agent_key: &str, iter: u32) {
    fs::write(
        run_dir.join(format!("{agent_key}_iter{iter}.md")),
        format!("{agent_key} {iter}"),
    )
    .unwrap();
}

fn test_agent(
    name: &str,
    provider: ProviderKind,
    model: &str,
    use_cli: bool,
    thinking_effort: Option<&str>,
) -> AgentConfig {
    AgentConfig {
        name: name.to_string(),
        provider,
        api_key: if use_cli {
            String::new()
        } else {
            "k".to_string()
        },
        model: model.to_string(),
        reasoning_effort: None,
        thinking_effort: thinking_effort.map(str::to_string),
        use_cli,
        cli_print_mode: true,
        extra_cli_args: String::new(),
    }
}

struct HistoryEchoProvider {
    kind: ProviderKind,
    calls: usize,
}

impl Provider for HistoryEchoProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn clear_history(&mut self) {}

    fn send(&mut self, _message: &str) -> crate::provider::SendFuture<'_> {
        Box::pin(async move {
            self.calls += 1;
            Ok(CompletionResponse {
                content: format!("call {}", self.calls),
                debug_logs: Vec::new(),
            })
        })
    }
}

#[test]
fn parse_agent_iteration_valid() {
    assert_eq!(
        parse_agent_iteration_filename("anthropic_iter3.md", "anthropic"),
        Some(3)
    );
}

#[test]
fn parse_agent_iteration_wrong_agent() {
    assert_eq!(
        parse_agent_iteration_filename("openai_iter3.md", "anthropic"),
        None
    );
}

#[test]
fn parse_agent_iteration_not_md() {
    assert_eq!(
        parse_agent_iteration_filename("anthropic_iter3.txt", "anthropic"),
        None
    );
}

#[test]
fn parse_agent_iteration_no_number() {
    assert_eq!(
        parse_agent_iteration_filename("anthropic_iter.md", "anthropic"),
        None
    );
}

#[test]
fn validate_agent_runtime_rejects_anthropic_max_in_api_mode() {
    let app = test_app();
    let agent = test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "claude-opus-4-6",
        false,
        Some("max"),
    );

    let err = validate_agent_runtime(&app, &agent.name, &agent).expect_err("should reject");
    assert!(err.contains("\"max\" thinking effort requires CLI mode"));
}

#[test]
fn validate_agent_runtime_allows_anthropic_max_in_cli_mode() {
    let mut app = test_app();
    app.cli_available.insert(ProviderKind::Anthropic, true);
    let agent = test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "claude-sonnet-4-5",
        true,
        Some("max"),
    );

    validate_agent_runtime(&app, &agent.name, &agent)
        .expect("cli mode should be allowed through to the provider");
}

#[test]
fn handle_execution_task_result_uses_selected_provider_kind() {
    let (tx, mut rx) = mpsc::unbounded_channel();

    handle_execution_task_result(
        &tx,
        Err(AppError::Config("boom".to_string())),
        "OpenAI".to_string(),
        ProviderKind::OpenAI,
    );

    match rx.try_recv().expect("agent error event") {
        ProgressEvent::AgentError {
            agent, kind, error, ..
        } => {
            assert_eq!(agent, "OpenAI");
            assert_eq!(kind, ProviderKind::OpenAI);
            assert!(error.contains("Config error: boom"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
    assert!(matches!(
        rx.try_recv().expect("all done"),
        ProgressEvent::AllDone
    ));
}

#[test]
fn handle_key_esc_cancels_active_run() {
    let mut app = test_app();
    app.screen = Screen::Running;
    app.running.is_running = true;

    handle_key(&mut app, key(KeyCode::Esc));

    assert!(app
        .running
        .cancel_flag
        .load(std::sync::atomic::Ordering::Relaxed));
}

#[test]
fn parse_iteration_from_filename_valid() {
    assert_eq!(parse_iteration_from_filename("openai_iter5.md"), Some(5));
}

#[test]
fn parse_iteration_from_filename_consolidated() {
    assert_eq!(
        parse_iteration_from_filename("consolidated_anthropic.md"),
        None
    );
}

#[test]
fn parse_iteration_from_filename_non_md() {
    assert_eq!(parse_iteration_from_filename("session.toml"), None);
}

#[test]
fn parse_iteration_from_filename_accepts_any_iter_suffix() {
    // Low-level parser matches any {name}_iter{N}.md pattern;
    // find_last_iteration filters by known agent keys.
    assert_eq!(parse_iteration_from_filename("notes_iter42.md"), Some(42));
}

#[test]
fn parse_block_iteration_named_block() {
    // Legacy: {name}_b{id}_{agent}_iter{n}.md
    assert!(is_pipeline_output_filename("Analyzer_b1_Claude_iter2.md"));
    // New: {name}_b{id}_{agent}.md
    assert!(is_pipeline_output_filename("Analyzer_b1_Claude.md"));
}

#[test]
fn parse_pipeline_iteration_unnamed_block() {
    // Legacy: block{id}_{agent}_iter{n}.md
    assert!(is_pipeline_output_filename("block1_openai_iter5.md"));
    // New: block{id}_{agent}.md
    assert!(is_pipeline_output_filename("block1_claude.md"));
}

#[test]
fn parse_block_iteration_different_agent() {
    assert!(is_pipeline_output_filename("Reviewer_b3_Gemini_iter5.md"));
}

#[test]
fn parse_block_iteration_not_md() {
    assert!(!is_pipeline_output_filename("Analyzer_b1_Claude_iter2.txt"));
    assert!(!is_pipeline_output_filename("Analyzer_b1_Claude.txt"));
}

#[test]
fn parse_block_iteration_no_block_id_marker() {
    assert!(!is_pipeline_output_filename("Claude_iter2.md"));
    assert!(!is_pipeline_output_filename("Claude.md"));
}

#[test]
fn parse_block_iteration_non_numeric_block_id() {
    assert!(!is_pipeline_output_filename("Analyzer_bx_Claude_iter2.md"));
    assert!(!is_pipeline_output_filename("Analyzer_bx_Claude.md"));
}

#[test]
fn parse_block_iteration_name_contains_b() {
    // Block name "web_builder" contains "_b" — parser must skip it and find _b7_
    assert!(is_pipeline_output_filename(
        "web_builder_b7_Claude_iter1.md"
    ));
    assert!(is_pipeline_output_filename("web_builder_b7_Claude.md"));
}

#[test]
fn is_pipeline_output_filename_loop_variants() {
    // New loop format
    assert!(is_pipeline_output_filename("block1_claude_loop2.md"));
    assert!(is_pipeline_output_filename("Analyzer_b1_Claude_loop1.md"));
    // Legacy iter+loop format
    assert!(is_pipeline_output_filename("block1_claude_iter1_loop2.md"));
    assert!(is_pipeline_output_filename(
        "Analyzer_b1_Claude_iter1_loop1.md"
    ));
}

#[test]
fn parse_iteration_from_filename_matches_block_files() {
    // Named block format
    assert_eq!(
        parse_iteration_from_filename("Reviewer_b2_Gemini_iter4.md"),
        Some(4)
    );
    // Unnamed block fallback format
    assert_eq!(
        parse_iteration_from_filename("block2_gemini_iter4.md"),
        Some(4)
    );
}

#[test]
fn find_last_iteration_includes_block_files() {
    let dir = tempdir().unwrap();
    write_agent_iter(dir.path(), "anthropic", 1);
    fs::write(dir.path().join("Analyzer_b1_Claude.md"), "test").unwrap();
    fs::write(dir.path().join("Reviewer_b2_Gemini.md"), "test").unwrap();
    assert_eq!(find_last_iteration(dir.path(), &[]), Some(1));
}

#[tokio::test]
async fn find_last_iteration_async_matches_sync_for_pipeline_files() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude.md"), "test").unwrap();
    fs::write(dir.path().join("Reviewer_b2_Gemini.md"), "test").unwrap();

    let sync = find_last_iteration(dir.path(), &[]);
    let async_found = find_last_iteration_async(dir.path(), &[]).await;
    assert_eq!(async_found, sync);
    assert_eq!(sync, Some(1));
}

// --- Backward-compat integration tests for old _iter{N} filenames ---

#[test]
fn find_last_iteration_old_format_pipeline_files() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude_iter1.md"), "old").unwrap();
    fs::write(dir.path().join("Reviewer_b2_Gemini_iter1.md"), "old").unwrap();
    assert_eq!(find_last_iteration(dir.path(), &[]), Some(1));
}

#[tokio::test]
async fn find_last_iteration_async_old_format_pipeline_files() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude_iter2.md"), "old").unwrap();
    let sync = find_last_iteration(dir.path(), &[]);
    let async_found = find_last_iteration_async(dir.path(), &[]).await;
    assert_eq!(sync, Some(1));
    assert_eq!(async_found, Some(1));
}

#[test]
fn discover_final_outputs_old_format_pipeline_files() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude_iter1.md"), "old output").unwrap();
    fs::write(
        dir.path().join("Reviewer_b2_Gemini_iter1.md"),
        "old output 2",
    )
    .unwrap();
    fs::write(dir.path().join("session.toml"), "metadata").unwrap();

    let files = discover_final_outputs(dir.path(), ExecutionMode::Pipeline, &[]);
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"Analyzer_b1_Claude_iter1.md"),
        "should discover old-format named block file"
    );
    assert!(
        names.contains(&"Reviewer_b2_Gemini_iter1.md"),
        "should discover old-format named block file"
    );
    assert_eq!(files.len(), 2, "should not include non-pipeline files");
}

#[tokio::test]
async fn discover_final_outputs_async_old_format_pipeline_files() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("block1_claude_iter1.md"), "old output").unwrap();
    fs::write(
        dir.path().join("block1_claude_iter1_loop2.md"),
        "old loop output",
    )
    .unwrap();
    fs::write(dir.path().join("session.toml"), "metadata").unwrap();

    let files = discover_final_outputs_async(dir.path(), ExecutionMode::Pipeline, &[]).await;
    // keep_highest_loop_pass should keep only the loop2 variant
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"block1_claude_iter1_loop2.md"),
        "should discover old-format loop file"
    );
    assert!(
        !names.contains(&"block1_claude_iter1.md"),
        "loop dedup should exclude base when loop variant exists"
    );
}

#[tokio::test]
async fn discover_final_outputs_async_new_format_loop_dedup() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude.md"), "base output").unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude_loop1.md"), "loop 1").unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude_loop2.md"), "loop 2").unwrap();

    let files = discover_final_outputs_async(dir.path(), ExecutionMode::Pipeline, &[]).await;
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"Analyzer_b1_Claude_loop2.md"),
        "should keep highest loop pass"
    );
    assert!(
        !names.contains(&"Analyzer_b1_Claude.md"),
        "loop dedup should exclude base when loop variants exist"
    );
    assert!(
        !names.contains(&"Analyzer_b1_Claude_loop1.md"),
        "loop dedup should exclude lower loop passes"
    );
    assert_eq!(files.len(), 1, "only the highest loop pass should remain");
}

#[test]
fn discover_final_outputs_mixed_old_and_new_format() {
    let dir = tempdir().unwrap();
    // Old format
    fs::write(dir.path().join("Analyzer_b1_Claude_iter1.md"), "old").unwrap();
    // New format
    fs::write(dir.path().join("Reviewer_b2_Gemini.md"), "new").unwrap();

    let files = discover_final_outputs(dir.path(), ExecutionMode::Pipeline, &[]);
    assert_eq!(
        files.len(),
        2,
        "should discover both old and new format files"
    );
}

#[test]
fn discover_final_outputs_old_format_loop_dedup_sync() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("block1_claude_iter1.md"), "old output").unwrap();
    fs::write(
        dir.path().join("block1_claude_iter1_loop2.md"),
        "old loop output",
    )
    .unwrap();

    let files = discover_final_outputs(dir.path(), ExecutionMode::Pipeline, &[]);
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"block1_claude_iter1_loop2.md"),
        "should keep highest loop pass"
    );
    assert!(
        !names.contains(&"block1_claude_iter1.md"),
        "loop dedup should exclude base when loop variant exists"
    );
}

#[test]
fn discover_final_outputs_new_format_loop_dedup() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude.md"), "base output").unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude_loop1.md"), "loop 1").unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude_loop2.md"), "loop 2").unwrap();

    let files = discover_final_outputs(dir.path(), ExecutionMode::Pipeline, &[]);
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"Analyzer_b1_Claude_loop2.md"),
        "should keep highest loop pass"
    );
    assert!(
        !names.contains(&"Analyzer_b1_Claude.md"),
        "loop dedup should exclude base when loop variants exist"
    );
    assert!(
        !names.contains(&"Analyzer_b1_Claude_loop1.md"),
        "loop dedup should exclude lower loop passes"
    );
    assert_eq!(files.len(), 1, "only the highest loop pass should remain");
}

#[test]
fn discover_final_outputs_ignores_non_pipeline_files() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude.md"), "output").unwrap();
    // These don't match is_pipeline_output_filename (no _b{id}_ or block{id}_ pattern)
    fs::write(dir.path().join("consolidated_claude.md"), "consolidated").unwrap();
    fs::write(dir.path().join("prompt.md"), "prompt").unwrap();
    fs::write(dir.path().join("session.toml"), "metadata").unwrap();

    let files = discover_final_outputs(dir.path(), ExecutionMode::Pipeline, &[]);
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"Analyzer_b1_Claude.md"));
    assert!(!names.contains(&"consolidated_claude.md"));
    assert!(!names.contains(&"prompt.md"));
    assert_eq!(files.len(), 1);
}

#[test]
fn find_last_iteration_multiple_files() {
    let dir = tempdir().unwrap();
    write_agent_iter(dir.path(), "Claude", 1);
    write_agent_iter(dir.path(), "OpenAI", 3);
    write_agent_iter(dir.path(), "Gemini", 2);

    let keys: Vec<String> = ["Claude", "OpenAI", "Gemini"]
        .iter()
        .map(|n| App::agent_file_key(n))
        .collect();
    assert_eq!(find_last_iteration(dir.path(), &keys), Some(3));
}

#[tokio::test]
async fn find_last_iteration_async_matches_sync_for_agent_files() {
    let dir = tempdir().unwrap();
    write_agent_iter(dir.path(), "Claude", 1);
    write_agent_iter(dir.path(), "OpenAI", 4);
    write_agent_iter(dir.path(), "Gemini", 2);

    let keys: Vec<String> = ["Claude", "OpenAI", "Gemini"]
        .iter()
        .map(|n| App::agent_file_key(n))
        .collect();
    let sync = find_last_iteration(dir.path(), &keys);
    let async_found = find_last_iteration_async(dir.path(), &keys).await;
    assert_eq!(async_found, sync);
}

#[test]
fn find_last_iteration_ignores_unknown_files() {
    let dir = tempdir().unwrap();
    write_agent_iter(dir.path(), "Claude", 2);
    // Stray file that looks like an iteration but isn't a known agent
    std::fs::write(dir.path().join("notes_iter42.md"), "stray").unwrap();

    let keys = vec![App::agent_file_key("Claude")];
    assert_eq!(find_last_iteration(dir.path(), &keys), Some(2));
}

#[test]
fn run_dir_matches_exact() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

    assert!(run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_wrong_mode() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "swarm", &["anthropic", "openai"]);

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_relay_requires_exact_agent_order() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_relay_rejects_different_agent_order() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["openai".to_string(), "anthropic".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_swarm_accepts_different_agent_order_for_same_set() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "swarm", &["anthropic", "openai"]);

    assert!(run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Swarm,
        &["openai".to_string(), "anthropic".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_missing_agent() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "relay", &["anthropic"]);

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "gemini".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_no_session_toml() {
    let dir = tempdir().unwrap();
    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_mode_and_agents_missing_mode_field() {
    let dir = tempdir().unwrap();
    fs::write(
        dir.path().join("session.toml"),
        "agents = [\"anthropic\", \"openai\"]\n",
    )
    .unwrap();

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_mode_and_agents_missing_agents_field() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("session.toml"), "mode = \"relay\"\n").unwrap();

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()],
        true,
    ));
}

#[test]
fn find_latest_compatible_run_ordering() {
    let dir = tempdir().unwrap();

    let run1 = dir.path().join("20260101_000000");
    fs::create_dir_all(&run1).unwrap();
    write_session_toml(&run1, "relay", &["anthropic", "openai"]);
    write_agent_iter(&run1, "anthropic", 1);
    write_agent_iter(&run1, "openai", 1);

    let run2 = dir.path().join("20260201_000000");
    fs::create_dir_all(&run2).unwrap();
    write_session_toml(&run2, "relay", &["anthropic", "openai"]);
    write_agent_iter(&run2, "anthropic", 1);
    write_agent_iter(&run2, "openai", 1);

    let run3 = dir.path().join("20260301_000000");
    fs::create_dir_all(&run3).unwrap();
    write_session_toml(&run3, "swarm", &["anthropic", "openai"]);
    write_agent_iter(&run3, "anthropic", 1);
    write_agent_iter(&run3, "openai", 1);

    // Add a grouped dir from April with matching mode+agents — should win
    let run4 = dir.path().join("2026-04-01/10-00-00");
    fs::create_dir_all(&run4).unwrap();
    write_session_toml(&run4, "relay", &["anthropic", "openai"]);
    write_agent_iter(&run4, "anthropic", 1);
    write_agent_iter(&run4, "openai", 1);

    assert_eq!(
        find_latest_compatible_run(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()],
            true,
        ),
        Some(run4)
    );
}

#[test]
fn find_latest_compatible_run_none() {
    let dir = tempdir().unwrap();
    let run = dir.path().join("20260101_000000");
    fs::create_dir_all(&run).unwrap();
    write_session_toml(&run, "relay", &["anthropic", "openai"]);
    write_agent_iter(&run, "anthropic", 1);

    assert_eq!(
        find_latest_compatible_run(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()],
            true,
        ),
        None
    );
}

#[test]
fn find_latest_compatible_run_ignores_invalid_session_toml() {
    let dir = tempdir().unwrap();

    let good = dir.path().join("20260101_000000");
    fs::create_dir_all(&good).unwrap();
    write_session_toml(&good, "relay", &["anthropic", "openai"]);
    write_agent_iter(&good, "anthropic", 1);
    write_agent_iter(&good, "openai", 1);

    let bad = dir.path().join("20260201_000000");
    fs::create_dir_all(&bad).unwrap();
    fs::write(bad.join("session.toml"), "mode = ").unwrap();
    write_agent_iter(&bad, "anthropic", 1);
    write_agent_iter(&bad, "openai", 1);

    // Also add a grouped dir with bad TOML
    let bad_grouped = dir.path().join("2026-03-01/12-00-00");
    fs::create_dir_all(&bad_grouped).unwrap();
    fs::write(bad_grouped.join("session.toml"), "mode = ").unwrap();
    write_agent_iter(&bad_grouped, "anthropic", 1);
    write_agent_iter(&bad_grouped, "openai", 1);

    assert_eq!(
        find_latest_compatible_run(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()],
            true,
        ),
        Some(good)
    );
}

#[test]
fn validate_resume_run_rejects_named_session_with_wrong_relay_order() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

    let err = validate_resume_run(
        dir.path(),
        ExecutionMode::Relay,
        &["openai".to_string(), "anthropic".to_string()],
        true,
    )
    .expect_err("should reject");
    assert!(err.contains("does not exactly match"));
}

#[test]
fn run_dir_rejects_keep_session_mismatch() {
    let dir = tempdir().unwrap();
    write_session_toml_with_keep(dir.path(), "relay", &["anthropic", "openai"], Some(false));

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()],
        true,
    ));
}

#[test]
fn run_dir_matches_explicit_keep_session_false() {
    let dir = tempdir().unwrap();
    write_session_toml_with_keep(dir.path(), "relay", &["anthropic", "openai"], Some(false));

    assert!(run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()],
        false,
    ));
}

#[test]
fn find_latest_compatible_run_respects_keep_session() {
    let dir = tempdir().unwrap();

    let run_keep = dir.path().join("20260101_000000");
    fs::create_dir_all(&run_keep).unwrap();
    write_session_toml_with_keep(&run_keep, "relay", &["anthropic", "openai"], Some(true));
    write_agent_iter(&run_keep, "anthropic", 1);
    write_agent_iter(&run_keep, "openai", 1);

    let run_no_keep = dir.path().join("20260201_000000");
    fs::create_dir_all(&run_no_keep).unwrap();
    write_session_toml_with_keep(&run_no_keep, "relay", &["anthropic", "openai"], Some(false));
    write_agent_iter(&run_no_keep, "anthropic", 1);
    write_agent_iter(&run_no_keep, "openai", 1);

    // Searching with keep_session=false should find run_no_keep (the newer one matches)
    assert_eq!(
        find_latest_compatible_run(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()],
            false,
        ),
        Some(run_no_keep)
    );

    // Searching with keep_session=true should find run_keep (skips the newer mismatched one)
    assert_eq!(
        find_latest_compatible_run(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()],
            true,
        ),
        Some(run_keep)
    );
}

#[test]
fn validate_resume_run_rejects_keep_session_mismatch() {
    let dir = tempdir().unwrap();
    write_session_toml_with_keep(dir.path(), "relay", &["anthropic", "openai"], Some(true));

    let err = validate_resume_run(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()],
        false,
    )
    .expect_err("should reject keep_session mismatch");
    assert!(err.contains("does not exactly match"));
}

#[test]
fn find_last_complete_iteration_for_agents_returns_last_complete() {
    let dir = tempdir().unwrap();
    write_agent_iter(dir.path(), "anthropic", 1);
    write_agent_iter(dir.path(), "openai", 1);
    write_agent_iter(dir.path(), "anthropic", 2);

    assert_eq!(
        find_last_complete_iteration_for_agents(
            dir.path(),
            &["anthropic".to_string(), "openai".to_string()]
        ),
        Some(1)
    );
}

#[test]
fn find_last_complete_iteration_for_agents_none_complete() {
    let dir = tempdir().unwrap();
    write_agent_iter(dir.path(), "anthropic", 1);
    write_agent_iter(dir.path(), "openai", 2);

    assert_eq!(
        find_last_complete_iteration_for_agents(
            dir.path(),
            &["anthropic".to_string(), "openai".to_string()]
        ),
        None
    );
}

#[test]
fn collect_report_files_excludes_prompt_and_errors() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("prompt.md"), "prompt").unwrap();
    fs::write(dir.path().join("errors.md"), "errors").unwrap();
    fs::write(dir.path().join("anthropic_iter1.md"), "report").unwrap();
    fs::write(dir.path().join("session.toml"), "mode = \"relay\"").unwrap();

    let files = collect_report_files(dir.path());
    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0].file_name().and_then(|n| n.to_str()),
        Some("anthropic_iter1.md")
    );
}

#[test]
fn collect_report_files_sorted() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("b.md"), "b").unwrap();
    fs::write(dir.path().join("a.md"), "a").unwrap();
    fs::write(dir.path().join("c.md"), "c").unwrap();

    let files = collect_report_files(dir.path());
    let names: Vec<String> = files
        .iter()
        .map(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    assert_eq!(names, vec!["a.md", "b.md", "c.md"]);
}

#[test]
fn collect_report_files_empty_dir() {
    let dir = tempdir().unwrap();
    assert!(collect_report_files(dir.path()).is_empty());
}

#[test]
fn collect_application_errors_deduplicates() {
    let dir = tempdir().unwrap();
    let mut app = test_app();
    app.record_progress(ProgressEvent::AgentError {
        agent: "Claude".to_string(),
        kind: ProviderKind::Anthropic,
        iteration: 1,
        error: "boom".to_string(),
        details: None,
    });
    app.record_progress(ProgressEvent::AgentError {
        agent: "Claude".to_string(),
        kind: ProviderKind::Anthropic,
        iteration: 1,
        error: "boom".to_string(),
        details: None,
    });
    fs::write(dir.path().join("_errors.log"), "[Claude iter 1] boom\n").unwrap();

    let base_errors = app.error_ledger().iter().cloned().collect::<Vec<_>>();
    let errors = collect_application_errors(&base_errors, dir.path());
    assert_eq!(errors, vec!["[Claude iter 1] boom".to_string()]);
}

#[test]
fn collect_application_errors_merges_log_file() {
    let dir = tempdir().unwrap();
    let mut app = test_app();
    app.record_progress(ProgressEvent::AgentError {
        agent: "OpenAI".to_string(),
        kind: ProviderKind::OpenAI,
        iteration: 2,
        error: "api failed".to_string(),
        details: Some("rate limited".to_string()),
    });
    fs::write(dir.path().join("_errors.log"), "tool timeout\n\n").unwrap();

    let base_errors = app.error_ledger().iter().cloned().collect::<Vec<_>>();
    let errors = collect_application_errors(&base_errors, dir.path());
    assert_eq!(errors.len(), 2);
    assert!(errors.iter().any(|e| e == "[OpenAI iter 2] rate limited"));
    assert!(errors.iter().any(|e| e == "tool timeout"));
}

#[test]
fn collect_application_errors_empty_sources() {
    let dir = tempdir().unwrap();
    let app = test_app();
    let base_errors = app.error_ledger().iter().cloned().collect::<Vec<_>>();
    assert!(collect_application_errors(&base_errors, dir.path()).is_empty());
}

#[test]
fn build_diagnostic_prompt_cli_mode() {
    let dir = tempdir().unwrap();
    let report = dir.path().join("anthropic_iter1.md");
    fs::write(&report, "hidden content").unwrap();

    let prompt = build_diagnostic_prompt(&[report], &[], true).expect("prompt");
    assert!(prompt.contains("Read each listed file from disk"));
    assert!(!prompt.contains("hidden content"));
}

#[test]
fn build_diagnostic_prompt_api_mode() {
    let dir = tempdir().unwrap();
    let report = dir.path().join("anthropic_iter1.md");
    fs::write(&report, "file body").unwrap();

    let prompt = build_diagnostic_prompt(&[report], &[], false).expect("prompt");
    assert!(prompt.contains("=== BEGIN anthropic_iter1.md ==="));
    assert!(prompt.contains("file body"));
    assert!(prompt.contains("=== END anthropic_iter1.md ==="));
}

#[test]
fn build_diagnostic_prompt_no_report_files() {
    let prompt = build_diagnostic_prompt(&[], &[], false).expect("prompt");
    assert!(prompt.contains("Reports to analyze:\n- none"));
    assert!(!prompt.contains("Report contents:"));
}

#[test]
fn build_diagnostic_prompt_unreadable_file_in_api_mode() {
    let missing = PathBuf::from("/tmp/does-not-exist-for-houseofagents-tests.md");
    let err = build_diagnostic_prompt(&[missing], &[], false).expect_err("should fail");
    assert!(err.contains("Failed to inspect"));
}

#[test]
fn build_diagnostic_prompt_rejects_oversized_inline_input() {
    let dir = tempdir().unwrap();
    let report = dir.path().join("anthropic_iter1.md");
    fs::write(
        &report,
        "x".repeat((POST_RUN_SYNTHESIS_MAX_INPUT_BYTES as usize) + 1),
    )
    .unwrap();

    let err = build_diagnostic_prompt(&[report], &[], false).expect_err("should reject");
    assert!(err.contains("too large"));
}

#[test]
fn build_diagnostic_prompt_cli_mode_does_not_budget_file_contents() {
    let dir = tempdir().unwrap();
    let report = dir.path().join("anthropic_iter1.md");
    fs::write(
        &report,
        "x".repeat((POST_RUN_SYNTHESIS_MAX_INPUT_BYTES as usize) + 1),
    )
    .unwrap();

    let prompt = build_diagnostic_prompt(&[report], &[], true).expect("prompt");
    assert!(prompt.contains("Read each listed file from disk"));
}

#[tokio::test]
async fn per_run_consolidation_recreates_provider_each_run() {
    let dir = tempdir().unwrap();
    let batch_root = OutputManager::new_batch_parent(dir.path(), Some("batch")).unwrap();
    let run1 = batch_root.new_run_subdir(1).unwrap();
    let run2 = batch_root.new_run_subdir(2).unwrap();

    for run_dir in [run1.run_dir(), run2.run_dir()] {
        fs::write(run_dir.join("Claude_iter1.md"), "claude").unwrap();
        fs::write(run_dir.join("OpenAI_iter1.md"), "openai").unwrap();
    }

    let result = run_consolidation_with_provider_factory(
        ConsolidationRequest {
            run_dir: batch_root.run_dir().clone(),
            target: ConsolidationTarget::PerRun,
            mode: ExecutionMode::Swarm,
            selected_agents: vec!["Claude".to_string(), "OpenAI".to_string()],
            successful_runs: vec![1, 2],
            batch_stage1_done: false,
            additional: String::new(),
            agent_name: "Claude".to_string(),
            agent_use_cli: false,
        },
        || {
            Box::new(HistoryEchoProvider {
                kind: ProviderKind::Anthropic,
                calls: 0,
            })
        },
    )
    .await
    .expect("consolidation");

    assert_eq!(result, "Per-run consolidation completed");
    assert_eq!(
        fs::read_to_string(run1.run_dir().join("consolidation.md")).unwrap(),
        "call 1"
    );
    assert_eq!(
        fs::read_to_string(run2.run_dir().join("consolidation.md")).unwrap(),
        "call 1"
    );
}

#[tokio::test]
async fn build_file_consolidation_prompt_rejects_oversized_inline_input() {
    let dir = tempdir().unwrap();
    let report = dir.path().join("anthropic_iter1.md");
    fs::write(
        &report,
        "x".repeat((POST_RUN_SYNTHESIS_MAX_INPUT_BYTES as usize) + 1),
    )
    .unwrap();

    let err = build_file_consolidation_prompt(&[("Claude".to_string(), report)], "", false)
        .await
        .expect_err("should reject");
    assert!(err.contains("too large"));
}

#[tokio::test]
async fn build_file_consolidation_prompt_cli_mode_does_not_budget_file_contents() {
    let dir = tempdir().unwrap();
    let report = dir.path().join("anthropic_iter1.md");
    fs::write(
        &report,
        "x".repeat((POST_RUN_SYNTHESIS_MAX_INPUT_BYTES as usize) + 1),
    )
    .unwrap();

    let prompt = build_file_consolidation_prompt(&[("Claude".to_string(), report)], "", true)
        .await
        .expect("prompt");
    assert!(prompt.contains("Files to read:"));
}

#[test]
fn prev_char_boundary_ascii() {
    assert_eq!(prev_char_boundary("hello", 3), 2);
}

#[test]
fn prev_char_boundary_multibyte() {
    assert_eq!(prev_char_boundary("héllo", 3), 1);
}

#[test]
fn prev_char_boundary_at_zero() {
    assert_eq!(prev_char_boundary("hello", 0), 0);
}

#[test]
fn next_char_boundary_ascii() {
    assert_eq!(next_char_boundary("hello", 2), 3);
}

#[test]
fn next_char_boundary_at_end() {
    assert_eq!(next_char_boundary("hello", 5), 5);
}

#[test]
fn next_char_boundary_multibyte() {
    assert_eq!(next_char_boundary("café", 3), 5);
}

#[test]
fn word_move_left_skips_whitespace_then_word() {
    let mut app = test_app();
    app.prompt.prompt_text = "hello world".to_string();
    app.prompt.prompt_cursor = app.prompt.prompt_text.len();

    move_cursor_word_left(&app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_cursor, 6);
}

#[test]
fn move_cursor_word_right_skips_whitespace_and_word() {
    let mut app = test_app();
    app.prompt.prompt_text = "hello   world  next".to_string();
    app.prompt.prompt_cursor = 5;

    move_cursor_word_right(&app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_cursor, 13);
}

#[test]
fn move_cursor_line_up_and_down_preserves_column() {
    let mut app = test_app();
    app.prompt.prompt_text = "abc\ndefg\nhi".to_string();
    let second_line_start = "abc\n".len();
    app.prompt.prompt_cursor = second_line_start + 2; // after 'e'

    move_cursor_line_up(&app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_cursor, 2);

    move_cursor_line_down(&app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_cursor, second_line_start + 2);
}

#[test]
fn move_cursor_line_up_and_down_clamp_to_line_length() {
    let mut app = test_app();
    app.prompt.prompt_text = "a\nlonger".to_string();
    app.prompt.prompt_cursor = app.prompt.prompt_text.len();

    move_cursor_line_up(&app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_cursor, 1);

    move_cursor_line_down(&app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_cursor, 3);
}

#[test]
fn move_cursor_line_up_down_noop_at_edges() {
    let mut app = test_app();
    app.prompt.prompt_text = "top\nbottom".to_string();

    app.prompt.prompt_cursor = 1;
    move_cursor_line_up(&app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_cursor, 1);

    app.prompt.prompt_cursor = app.prompt.prompt_text.len();
    move_cursor_line_down(&app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_cursor, app.prompt.prompt_text.len());
}

#[test]
fn delete_char_left_multibyte() {
    let mut app = test_app();
    app.prompt.prompt_text = "aé".to_string();
    app.prompt.prompt_cursor = app.prompt.prompt_text.len();

    delete_char_left(&mut app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_text, "a");
    assert_eq!(app.prompt.prompt_cursor, 1);
}

#[test]
fn delete_word_left_multibyte_boundary_safe() {
    let mut app = test_app();
    app.prompt.prompt_text = "hello café".to_string();
    app.prompt.prompt_cursor = app.prompt.prompt_text.len();

    delete_word_left(&mut app.prompt.prompt_text, &mut app.prompt.prompt_cursor);
    assert_eq!(app.prompt.prompt_text, "hello ");
    assert_eq!(app.prompt.prompt_cursor, 6);
}

#[test]
fn sync_iterations_buf_empty() {
    let mut app = test_app();
    app.prompt.iterations_buf.clear();
    sync_iterations_buf(&mut app);
    assert_eq!(app.prompt.iterations, 1);
    assert_eq!(app.prompt.iterations_buf, "1");
}

#[test]
fn sync_iterations_buf_valid() {
    let mut app = test_app();
    app.prompt.iterations_buf = "5".to_string();
    sync_iterations_buf(&mut app);
    assert_eq!(app.prompt.iterations, 5);
    assert_eq!(app.prompt.iterations_buf, "5");
}

#[test]
fn sync_iterations_buf_over_99() {
    let mut app = test_app();
    app.prompt.iterations_buf = "150".to_string();
    sync_iterations_buf(&mut app);
    assert_eq!(app.prompt.iterations, 99);
    assert_eq!(app.prompt.iterations_buf, "99");
}

#[test]
fn sync_iterations_buf_zero() {
    let mut app = test_app();
    app.prompt.iterations_buf = "0".to_string();
    sync_iterations_buf(&mut app);
    assert_eq!(app.prompt.iterations, 1);
    assert_eq!(app.prompt.iterations_buf, "1");
}

#[test]
fn sync_iterations_buf_non_numeric() {
    let mut app = test_app();
    app.prompt.iterations_buf = "abc".to_string();
    sync_iterations_buf(&mut app);
    assert_eq!(app.prompt.iterations, 1);
    assert_eq!(app.prompt.iterations_buf, "1");
}

#[test]
fn enforce_prompt_resume_constraints_disables_resume_in_multi_run_relay() {
    let mut app = test_app();
    app.selected_mode = ExecutionMode::Relay;
    app.prompt.runs = 3;
    app.prompt.resume_previous = true;

    enforce_prompt_resume_constraints(&mut app);

    assert!(!app.prompt.resume_previous);
}

#[test]
fn handle_prompt_resume_toggle_shows_error_when_disallowed() {
    let mut app = test_app();
    app.selected_mode = ExecutionMode::Swarm;
    app.prompt.prompt_focus = PromptFocus::Resume;
    app.prompt.runs = 2;
    app.prompt.resume_previous = false;

    handle_prompt_key(&mut app, key(KeyCode::Char(' ')));

    assert!(!app.prompt.resume_previous);
    assert_eq!(
        app.error_modal,
        Some("Resume is only supported for single-run execution".to_string())
    );
}

#[test]
fn batch_result_entry_helpers_respect_expansion_state() {
    let mut app = test_app();
    app.results.batch_result_runs = vec![
        BatchRunGroup {
            run_id: 1,
            files: vec![PathBuf::from("run1/a.md"), PathBuf::from("run1/b.md")],
        },
        BatchRunGroup {
            run_id: 2,
            files: vec![PathBuf::from("run2/c.md")],
        },
    ];
    app.results.batch_result_root_files = vec![PathBuf::from("root.md")];
    app.results.batch_result_expanded.insert(1);

    assert_eq!(batch_result_visible_len(&app), 5);
    assert!(matches!(
        batch_result_entry_at(&app, 0),
        Some(BatchResultEntry::RunHeader(1))
    ));
    assert!(matches!(
        batch_result_entry_at(&app, 1),
        Some(BatchResultEntry::File(path)) if path.ends_with("run1/a.md")
    ));
    assert!(matches!(
        batch_result_entry_at(&app, 2),
        Some(BatchResultEntry::File(path)) if path.ends_with("run1/b.md")
    ));
    assert!(matches!(
        batch_result_entry_at(&app, 3),
        Some(BatchResultEntry::RunHeader(2))
    ));
    assert!(matches!(
        batch_result_entry_at(&app, 4),
        Some(BatchResultEntry::File(path)) if path.ends_with("root.md")
    ));
    assert!(batch_result_entry_at(&app, 5).is_none());
}

#[test]
fn prompt_focus_cycle_swarm_tab_and_backtab() {
    let mut app = test_app();
    app.selected_mode = ExecutionMode::Swarm;
    app.prompt.prompt_focus = PromptFocus::Text;

    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::SessionName);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Iterations);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Runs);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Concurrency);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Resume);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::KeepSession);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Text);

    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::KeepSession);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Resume);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Concurrency);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Runs);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Iterations);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::SessionName);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Text);
}

#[test]
fn prompt_focus_cycle_relay_tab_and_backtab() {
    let mut app = test_app();
    app.selected_mode = ExecutionMode::Relay;
    app.prompt.prompt_focus = PromptFocus::Text;

    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::SessionName);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Iterations);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Runs);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Concurrency);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Resume);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::ForwardPrompt);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::KeepSession);
    handle_prompt_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Text);

    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::KeepSession);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::ForwardPrompt);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Resume);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Concurrency);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Runs);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Iterations);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::SessionName);
    handle_prompt_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Text);
}

#[test]
fn handle_paste_inserts_prompt_text_and_normalizes_newlines() {
    let mut app = test_app();
    app.screen = Screen::Prompt;
    app.prompt.prompt_focus = PromptFocus::Text;
    app.prompt.prompt_text = "ab".to_string();
    app.prompt.prompt_cursor = 1;

    handle_paste(&mut app, "X\r\nY\rZ");
    assert_eq!(app.prompt.prompt_text, "aX\nY\nZb");
    assert_eq!(app.prompt.prompt_cursor, "aX\nY\nZ".len());
}

#[test]
fn handle_paste_ignored_outside_prompt_text() {
    let mut app = test_app();
    app.screen = Screen::Prompt;
    app.prompt.prompt_focus = PromptFocus::SessionName;
    app.prompt.prompt_text = "base".to_string();
    app.prompt.prompt_cursor = 2;

    handle_paste(&mut app, "ZZZ");
    assert_eq!(app.prompt.prompt_text, "base");
    assert_eq!(app.prompt.prompt_cursor, 2);
}

#[test]
fn pipeline_move_selected_block_moves_into_empty_cell() {
    let mut app = test_app();
    app.pipeline.pipeline_def.blocks = vec![pipeline_mod::PipelineBlock {
        id: 1,
        name: "one".into(),
        agents: vec!["agent".into()],
        prompt: String::new(),
        profiles: vec![],
        session_id: None,
        position: (2, 2),
        replicas: 1,
    }];
    app.pipeline.pipeline_block_cursor = Some(1);

    pipeline_move_selected_block(&mut app, 1, 0);

    assert_eq!(app.pipeline.pipeline_def.blocks[0].position, (3, 2));
    assert_eq!(app.pipeline.pipeline_block_cursor, Some(1));
}

#[test]
fn pipeline_move_selected_block_swaps_when_target_occupied() {
    let mut app = test_app();
    app.pipeline.pipeline_def.blocks = vec![
        pipeline_mod::PipelineBlock {
            id: 1,
            name: "one".into(),
            agents: vec!["agent".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (2, 2),
            replicas: 1,
        },
        pipeline_mod::PipelineBlock {
            id: 2,
            name: "two".into(),
            agents: vec!["agent".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (3, 2),
            replicas: 1,
        },
    ];
    app.pipeline.pipeline_block_cursor = Some(1);

    pipeline_move_selected_block(&mut app, 1, 0);

    let b1 = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .find(|b| b.id == 1)
        .unwrap();
    let b2 = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .find(|b| b.id == 2)
        .unwrap();
    assert_eq!(b1.position, (3, 2));
    assert_eq!(b2.position, (2, 2));
}

#[test]
fn pipeline_builder_arrow_navigates_shift_arrow_moves_block() {
    let mut app = test_app();
    app.pipeline.pipeline_def.blocks = vec![
        pipeline_mod::PipelineBlock {
            id: 1,
            name: "one".into(),
            agents: vec!["agent".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (2, 2),
            replicas: 1,
        },
        pipeline_mod::PipelineBlock {
            id: 2,
            name: "two".into(),
            agents: vec!["agent".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (3, 2),
            replicas: 1,
        },
    ];
    app.pipeline.pipeline_block_cursor = Some(1);

    // Unmodified arrow navigates to the nearest block (does not move block 1)
    handle_pipeline_builder_key(&mut app, KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
    let after_nav = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .find(|b| b.id == 1)
        .unwrap();
    assert_eq!(
        after_nav.position,
        (2, 2),
        "unmodified arrow should not move the block"
    );
    assert_eq!(
        app.pipeline.pipeline_block_cursor,
        Some(2),
        "cursor should navigate to block 2"
    );

    // Shift+arrow moves the selected block
    handle_pipeline_builder_key(&mut app, KeyEvent::new(KeyCode::Right, KeyModifiers::SHIFT));
    let moved = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .find(|b| b.id == 2)
        .unwrap();
    assert_eq!(moved.position, (4, 2), "Shift+arrow should move the block");
    assert_eq!(
        app.pipeline.pipeline_block_cursor,
        Some(2),
        "cursor stays on the moved block"
    );
}

// --- New tests for grouped directory support in find_latest_compatible_run ---

#[test]
fn find_latest_compatible_run_grouped() {
    let dir = tempdir().unwrap();

    let run1 = dir.path().join("2026-01-01/10-00-00");
    fs::create_dir_all(&run1).unwrap();
    write_session_toml(&run1, "relay", &["anthropic", "openai"]);
    write_agent_iter(&run1, "anthropic", 1);
    write_agent_iter(&run1, "openai", 1);

    let run2 = dir.path().join("2026-02-01/11-00-00");
    fs::create_dir_all(&run2).unwrap();
    write_session_toml(&run2, "relay", &["anthropic", "openai"]);
    write_agent_iter(&run2, "anthropic", 1);
    write_agent_iter(&run2, "openai", 1);

    assert_eq!(
        find_latest_compatible_run(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()],
            true,
        ),
        Some(run2)
    );
}

#[test]
fn find_latest_compatible_run_mixed_grouped_wins() {
    let dir = tempdir().unwrap();

    let legacy = dir.path().join("20260101_000000");
    fs::create_dir_all(&legacy).unwrap();
    write_session_toml(&legacy, "relay", &["anthropic", "openai"]);
    write_agent_iter(&legacy, "anthropic", 1);
    write_agent_iter(&legacy, "openai", 1);

    let grouped = dir.path().join("2026-02-01/12-00-00");
    fs::create_dir_all(&grouped).unwrap();
    write_session_toml(&grouped, "relay", &["anthropic", "openai"]);
    write_agent_iter(&grouped, "anthropic", 1);
    write_agent_iter(&grouped, "openai", 1);

    assert_eq!(
        find_latest_compatible_run(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()],
            true,
        ),
        Some(grouped)
    );
}

#[test]
fn find_latest_compatible_run_skips_grouped_batch() {
    let dir = tempdir().unwrap();

    let batch = dir.path().join("2026-02-01/12-00-00");
    fs::create_dir_all(&batch).unwrap();
    write_session_toml(&batch, "relay", &["anthropic", "openai"]);
    write_agent_iter(&batch, "anthropic", 1);
    write_agent_iter(&batch, "openai", 1);
    fs::write(batch.join("batch.toml"), "runs = 2").unwrap();

    let good = dir.path().join("2026-01-01/10-00-00");
    fs::create_dir_all(&good).unwrap();
    write_session_toml(&good, "relay", &["anthropic", "openai"]);
    write_agent_iter(&good, "anthropic", 1);
    write_agent_iter(&good, "openai", 1);

    assert_eq!(
        find_latest_compatible_run(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()],
            true,
        ),
        Some(good)
    );
}

// -- Session config popup tests --

fn pipeline_app_with_block() -> App {
    use crate::execution::pipeline::PipelineBlock;
    let mut app = test_app();
    app.screen = Screen::Pipeline;
    app.pipeline.pipeline_focus = PipelineFocus::Builder;
    app.pipeline.pipeline_def.blocks.push(PipelineBlock {
        id: 1,
        name: "B1".into(),
        agents: vec!["Claude".into()],
        prompt: String::new(),
        profiles: vec![],
        session_id: None,
        position: (0, 0),
        replicas: 1,
    });
    app.pipeline.pipeline_block_cursor = Some(1);
    app.pipeline.pipeline_next_id = 2;
    app
}

#[test]
fn session_config_popup_opens_with_s_in_builder() {
    let mut app = pipeline_app_with_block();
    assert!(!app.pipeline.pipeline_show_session_config);
    handle_key(&mut app, key(KeyCode::Char('s')));
    assert!(app.pipeline.pipeline_show_session_config);
}

#[test]
fn session_config_popup_does_not_open_outside_builder() {
    let mut app = pipeline_app_with_block();
    app.pipeline.pipeline_focus = PipelineFocus::InitialPrompt;
    handle_key(&mut app, key(KeyCode::Char('s')));
    assert!(!app.pipeline.pipeline_show_session_config);
}

#[test]
fn session_config_popup_closes_with_esc() {
    let mut app = pipeline_app_with_block();
    handle_key(&mut app, key(KeyCode::Char('s')));
    assert!(app.pipeline.pipeline_show_session_config);
    handle_key(&mut app, key(KeyCode::Esc));
    assert!(!app.pipeline.pipeline_show_session_config);
}

#[test]
fn session_config_popup_toggle_sets_false() {
    let mut app = pipeline_app_with_block();
    handle_key(&mut app, key(KeyCode::Char('s')));
    // Default is true (keep_across_loop_passes), toggle to false
    handle_key(&mut app, key(KeyCode::Char(' ')));
    let sessions = app.pipeline.pipeline_def.effective_sessions();
    assert!(!sessions.is_empty());
    let first = &sessions[0];
    assert!(!app
        .pipeline
        .pipeline_def
        .keep_session_across_loop_passes(&first.agent, &first.session_key));
}

#[test]
fn session_config_popup_toggle_back_to_true() {
    let mut app = pipeline_app_with_block();
    handle_key(&mut app, key(KeyCode::Char('s')));
    // Toggle off then back on
    handle_key(&mut app, key(KeyCode::Char(' ')));
    handle_key(&mut app, key(KeyCode::Char(' ')));
    let sessions = app.pipeline.pipeline_def.effective_sessions();
    let first = &sessions[0];
    assert!(app
        .pipeline
        .pipeline_def
        .keep_session_across_loop_passes(&first.agent, &first.session_key));
}

#[test]
fn session_config_cursor_clamps() {
    let mut app = pipeline_app_with_block();
    handle_key(&mut app, key(KeyCode::Char('s')));
    // Only 1 session — cursor should clamp to 0
    handle_key(&mut app, key(KeyCode::Char('j')));
    assert_eq!(app.pipeline.pipeline_session_config_cursor, 0);
    handle_key(&mut app, key(KeyCode::Char('k')));
    assert_eq!(app.pipeline.pipeline_session_config_cursor, 0);
}

#[test]
fn session_config_paste_ignored() {
    let mut app = pipeline_app_with_block();
    handle_key(&mut app, key(KeyCode::Char('s')));
    assert!(app.pipeline.pipeline_show_session_config);
    // Paste via handle_paste should be swallowed inside session config popup
    handle_paste(&mut app, "some pasted text");
    // Popup still open, no state changed, no crash
    assert!(app.pipeline.pipeline_show_session_config);
    assert!(app.pipeline.pipeline_def.blocks[0].prompt.is_empty());
}

#[test]
fn session_config_normalizes_on_block_delete() {
    let mut app = pipeline_app_with_block();
    // Set keep=false for block 1's session
    app.pipeline
        .pipeline_def
        .set_keep_session_across_loop_passes("Claude", "__block_1", false);
    assert_eq!(app.pipeline.pipeline_def.session_configs.len(), 1);
    // Delete the block via 'd' key
    handle_key(&mut app, key(KeyCode::Char('d')));
    // Session config should be cleaned up immediately
    assert!(app.pipeline.pipeline_def.session_configs.is_empty());
}

#[test]
fn session_config_normalizes_on_edit_confirm() {
    let mut app = pipeline_app_with_block();
    // Set keep=false for block 1's session
    app.pipeline
        .pipeline_def
        .set_keep_session_across_loop_passes("Claude", "__block_1", false);
    assert_eq!(app.pipeline.pipeline_def.session_configs.len(), 1);
    // Open edit dialog
    handle_key(&mut app, key(KeyCode::Char('e')));
    assert!(app.pipeline.pipeline_show_edit);
    // Change the session_id field (which changes the session_key)
    app.pipeline.pipeline_edit_session_buf = "shared".to_string();
    // Confirm edit via Enter (on Name field by default)
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(!app.pipeline.pipeline_show_edit);
    // Old __block_1 config should be cleaned up since the session key changed
    assert!(app.pipeline.pipeline_def.session_configs.is_empty());
}

// ---------------------------------------------------------------------------
// Help popup tests
// ---------------------------------------------------------------------------

#[test]
fn help_opens_on_home() {
    let mut app = test_app();
    app.screen = Screen::Home;
    handle_key(&mut app, key(KeyCode::Char('?')));
    assert!(app.help_popup.active);
    assert_eq!(app.help_popup.tab_count, 1);
}

#[test]
fn help_opens_on_prompt_non_text() {
    let mut app = test_app();
    app.screen = Screen::Prompt;
    app.prompt.prompt_focus = PromptFocus::Iterations;
    handle_key(&mut app, key(KeyCode::Char('?')));
    assert!(app.help_popup.active);
    assert_eq!(app.help_popup.tab_count, 1);
}

#[test]
fn help_blocked_on_prompt_session_name() {
    let mut app = test_app();
    app.screen = Screen::Prompt;
    app.prompt.prompt_focus = PromptFocus::SessionName;
    handle_key(&mut app, key(KeyCode::Char('?')));
    assert!(!app.help_popup.active);
    assert!(app.prompt.session_name.contains('?'));
}

#[test]
fn help_blocked_on_prompt_text() {
    let mut app = test_app();
    app.screen = Screen::Prompt;
    app.prompt.prompt_focus = PromptFocus::Text;
    handle_key(&mut app, key(KeyCode::Char('?')));
    assert!(!app.help_popup.active);
    assert!(app.prompt.prompt_text.contains('?'));
}

#[test]
fn help_opens_on_order() {
    let mut app = test_app();
    app.screen = Screen::Order;
    app.selected_agents = vec!["A".into(), "B".into()];
    handle_key(&mut app, key(KeyCode::Char('?')));
    assert!(app.help_popup.active);
    assert_eq!(app.help_popup.tab_count, 1);
}

#[test]
fn help_opens_on_pipeline() {
    let mut app = test_app();
    app.screen = Screen::Pipeline;
    app.pipeline.pipeline_focus = PipelineFocus::Builder;
    handle_key(&mut app, key(KeyCode::Char('?')));
    assert!(app.help_popup.active);
    assert_eq!(
        app.help_popup.tab_count,
        crate::screen::help::PIPELINE_TAB_COUNT
    );
}

#[test]
fn help_blocked_on_pipeline_initial_prompt() {
    let mut app = test_app();
    app.screen = Screen::Pipeline;
    app.pipeline.pipeline_focus = PipelineFocus::InitialPrompt;
    handle_key(&mut app, key(KeyCode::Char('?')));
    assert!(!app.help_popup.active);
    assert!(app.pipeline.pipeline_def.initial_prompt.contains('?'));
}

#[test]
fn help_blocked_on_pipeline_session_name() {
    let mut app = test_app();
    app.screen = Screen::Pipeline;
    app.pipeline.pipeline_focus = PipelineFocus::SessionName;
    handle_key(&mut app, key(KeyCode::Char('?')));
    assert!(!app.help_popup.active);
    assert!(app.pipeline.pipeline_session_name.contains('?'));
}

#[test]
fn help_tab_wraps_forward() {
    let mut app = test_app();
    app.help_popup.open(6);
    app.help_popup.tab = 5;
    app.help_popup.scroll = 10;
    handle_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.help_popup.tab, 0);
    assert_eq!(app.help_popup.scroll, 0);
}

#[test]
fn help_tab_wraps_backward() {
    let mut app = test_app();
    app.help_popup.open(6);
    app.help_popup.tab = 0;
    handle_key(
        &mut app,
        KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT),
    );
    assert_eq!(app.help_popup.tab, 5);
    assert_eq!(app.help_popup.scroll, 0);
}

#[test]
fn help_tab_noop_single() {
    let mut app = test_app();
    app.help_popup.open(1);
    handle_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.help_popup.tab, 0);
}

#[test]
fn help_scroll_resets_on_tab_change() {
    let mut app = test_app();
    app.help_popup.open(6);
    app.help_popup.scroll = 20;
    app.help_popup.tab = 2;
    handle_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.help_popup.tab, 3);
    assert_eq!(app.help_popup.scroll, 0);
}

#[test]
fn error_dismisses_before_help() {
    let mut app = test_app();
    app.error_modal = Some("test error".into());
    app.help_popup.open(1);
    handle_key(&mut app, key(KeyCode::Char('a')));
    assert!(app.error_modal.is_none());
    assert!(app.help_popup.active);
}

#[test]
fn info_modal_dismisses_with_any_key() {
    let mut app = test_app();
    app.info_modal = Some("Config saved to disk".into());
    handle_key(&mut app, key(KeyCode::Char('a')));
    assert!(app.info_modal.is_none());
}

#[test]
fn info_modal_dismisses_before_help() {
    let mut app = test_app();
    app.info_modal = Some("Config saved to disk".into());
    app.help_popup.open(1);
    handle_key(&mut app, key(KeyCode::Char('a')));
    assert!(app.info_modal.is_none());
    assert!(app.help_popup.active);
}

#[test]
fn config_save_success_sets_info_modal() {
    let mut app = test_app();
    app.session_http_timeout_seconds = Some(99);
    app.error_modal = Some("stale error".into());
    // Simulate: the async task merged overrides into a config clone and saved it
    let mut saved = app.config.clone();
    saved.http_timeout_seconds = 99;
    handle_config_save_result(&mut app, Ok(saved));
    assert_eq!(app.info_modal.as_deref(), Some("Config saved to disk"));
    assert!(app.error_modal.is_none());
    assert!(app.session_http_timeout_seconds.is_none());
    assert_eq!(app.config.http_timeout_seconds, 99);
    assert!(!app.edit_popup.config_save_in_progress);
}

#[test]
fn config_save_failure_sets_error_modal() {
    let mut app = test_app();
    app.info_modal = Some("stale info".into());
    handle_config_save_result(&mut app, Err("disk full".into()));
    assert_eq!(
        app.error_modal.as_deref(),
        Some("Failed to save config: disk full")
    );
    assert!(app.info_modal.is_none());
}

#[test]
fn config_save_success_merges_memory_overrides() {
    let mut app = test_app();
    app.session_memory_max_recall = Some(42);
    app.session_memory_disable_extraction = Some(true);
    // Simulate: the async task merged overrides into a config clone and saved it
    let mut saved = app.config.clone();
    saved.memory.max_recall = 42;
    saved.memory.disable_extraction = true;
    handle_config_save_result(&mut app, Ok(saved));
    // Config replaced with the saved version
    assert_eq!(app.config.memory.max_recall, 42);
    assert!(app.config.memory.disable_extraction);
    // Session overrides cleared
    assert!(app.session_memory_max_recall.is_none());
    assert!(app.session_memory_disable_extraction.is_none());
}

#[test]
fn config_save_failure_preserves_config() {
    let mut app = test_app();
    let original_max_recall = app.config.memory.max_recall;
    let original_ttl = app.config.memory.observation_ttl_days;
    app.session_memory_max_recall = Some(99);
    app.session_memory_observation_ttl_days = Some(7);
    handle_config_save_result(&mut app, Err("write error".into()));
    // Config unchanged
    assert_eq!(app.config.memory.max_recall, original_max_recall);
    assert_eq!(app.config.memory.observation_ttl_days, original_ttl);
    // Session overrides still intact
    assert_eq!(app.session_memory_max_recall, Some(99));
    assert_eq!(app.session_memory_observation_ttl_days, Some(7));
}

#[test]
fn popup_locked_during_in_flight_save() {
    let mut app = test_app();
    app.config.agents.push(test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "m",
        false,
        None,
    ));
    app.edit_popup.visible = true;
    app.edit_popup.config_save_in_progress = true;
    app.edit_popup.section = EditPopupSection::Memory;
    app.edit_popup.memory_cursor = 0;
    // Space should be blocked (no toggle)
    handle_edit_popup_key(&mut app, key(KeyCode::Char(' ')));
    assert!(app.session_memory_enabled.is_none());
    // 's' should be blocked (no double-save)
    handle_edit_popup_key(&mut app, key(KeyCode::Char('s')));
    // Esc still works to close
    handle_edit_popup_key(&mut app, key(KeyCode::Esc));
    assert!(!app.edit_popup.visible);
}

#[test]
fn remove_agent_clears_extraction_agent() {
    let mut app = test_app();
    app.config.agents.push(test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "m",
        false,
        None,
    ));
    app.config
        .agents
        .push(test_agent("GPT", ProviderKind::OpenAI, "m", false, None));
    app.config.memory.extraction_agent = "Claude".to_string();
    app.session_memory_extraction_agent = Some("Claude".to_string());
    app.edit_popup.cursor = 0; // points at "Claude"
    remove_agent(&mut app);
    assert!(app.config.memory.extraction_agent.is_empty());
    assert!(app.session_memory_extraction_agent.is_none());
}

#[test]
fn rename_agent_updates_extraction_agent() {
    let mut app = test_app();
    app.config.agents.push(test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "m",
        false,
        None,
    ));
    app.config.memory.extraction_agent = "Claude".to_string();
    app.session_memory_extraction_agent = Some("Claude".to_string());
    app.edit_popup.cursor = 0;
    app.edit_popup.edit_buffer = "Claude-v2".to_string();
    app.edit_popup.editing = true;
    app.edit_popup.field = EditField::AgentName;
    commit_agent_rename(&mut app);
    assert_eq!(app.config.memory.extraction_agent, "Claude-v2");
    assert_eq!(
        app.session_memory_extraction_agent.as_deref(),
        Some("Claude-v2")
    );
}

#[test]
fn dismiss_clears_both_modals() {
    let mut app = test_app();
    app.error_modal = Some("error".into());
    app.info_modal = Some("info".into());
    handle_key(&mut app, key(KeyCode::Char('x')));
    assert!(app.error_modal.is_none());
    assert!(app.info_modal.is_none());
}

#[test]
fn pipeline_step_labels_expands_replicas() {
    use crate::execution::pipeline::{PipelineBlock, PipelineDefinition};
    let def = PipelineDefinition {
        initial_prompt: "go".into(),

        blocks: vec![
            PipelineBlock {
                id: 1,
                name: "Writer".into(),
                agents: vec!["Claude".into()],
                prompt: String::new(),
                profiles: vec![],
                session_id: None,
                position: (0, 0),
                replicas: 3,
            },
            PipelineBlock {
                id: 2,
                name: "Reviewer".into(),
                agents: vec!["GPT".into()],
                prompt: String::new(),
                profiles: vec![],
                session_id: None,
                position: (1, 0),
                replicas: 1,
            },
        ],
        connections: vec![],
        session_configs: vec![],
        loop_connections: vec![],
        finalization_blocks: Vec::new(),
        finalization_connections: Vec::new(),
        data_feeds: Vec::new(),
    };
    let labels = pipeline_step_labels(&def, true);
    assert_eq!(labels.len(), 4); // 3 replicas + 1
    assert_eq!(labels[0], "Writer (r1) (Claude)");
    assert_eq!(labels[1], "Writer (r2) (Claude)");
    assert_eq!(labels[2], "Writer (r3) (Claude)");
    assert_eq!(labels[3], "Reviewer (GPT)");
}

#[test]
fn pipeline_step_labels_unnamed_blocks_no_agent_duplication() {
    use crate::execution::pipeline::{PipelineBlock, PipelineDefinition};
    let def = PipelineDefinition {
        initial_prompt: "go".into(),

        blocks: vec![PipelineBlock {
            id: 5,
            name: String::new(),
            agents: vec!["Claude".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (0, 0),
            replicas: 1,
        }],
        connections: vec![],
        session_configs: vec![],
        loop_connections: vec![],
        finalization_blocks: Vec::new(),
        finalization_connections: Vec::new(),
        data_feeds: Vec::new(),
    };
    let labels = pipeline_step_labels(&def, true);
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0], "Block 5 (Claude)");
}

#[test]
fn pipeline_step_labels_multi_agent_no_duplication() {
    use crate::execution::pipeline::{PipelineBlock, PipelineDefinition};
    let def = PipelineDefinition {
        initial_prompt: "go".into(),

        blocks: vec![PipelineBlock {
            id: 1,
            name: "Writer".into(),
            agents: vec!["Claude".into(), "GPT".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (0, 0),
            replicas: 1,
        }],
        connections: vec![],
        session_configs: vec![],
        loop_connections: vec![],
        finalization_blocks: Vec::new(),
        finalization_connections: Vec::new(),
        data_feeds: Vec::new(),
    };
    let labels = pipeline_step_labels(&def, true);
    assert_eq!(labels.len(), 2);
    // Agent name should appear exactly once, not doubled
    assert_eq!(labels[0], "Writer (Claude)");
    assert_eq!(labels[1], "Writer (GPT)");
}

// ---------------------------------------------------------------------------
// Loop connection tests
// ---------------------------------------------------------------------------

fn pipeline_app_with_two_blocks() -> App {
    use crate::execution::pipeline::PipelineBlock;
    let mut app = test_app();
    app.screen = Screen::Pipeline;
    app.pipeline.pipeline_focus = PipelineFocus::Builder;
    app.pipeline.pipeline_def.blocks.push(PipelineBlock {
        id: 1,
        name: "A".into(),
        agents: vec!["Claude".into()],
        prompt: String::new(),
        profiles: vec![],
        session_id: None,
        position: (0, 0),
        replicas: 1,
    });
    app.pipeline.pipeline_def.blocks.push(PipelineBlock {
        id: 2,
        name: "B".into(),
        agents: vec!["GPT".into()],
        prompt: String::new(),
        profiles: vec![],
        session_id: None,
        position: (1, 0),
        replicas: 1,
    });
    app.pipeline.pipeline_block_cursor = Some(1);
    app.pipeline.pipeline_next_id = 3;
    app
}

#[test]
fn test_o_enters_loop_connect_mode() {
    let mut app = pipeline_app_with_two_blocks();
    assert!(app.pipeline.pipeline_loop_connecting_from.is_none());
    handle_key(&mut app, key(KeyCode::Char('o')));
    assert_eq!(app.pipeline.pipeline_loop_connecting_from, Some(1));
}

#[test]
fn test_o_opens_edit_on_existing_loop() {
    use crate::execution::pipeline::LoopConnection;
    let mut app = pipeline_app_with_two_blocks();
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 3,
            prompt: "review again".into(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('o')));
    assert!(app.pipeline.pipeline_show_loop_edit);
    assert_eq!(app.pipeline.pipeline_loop_edit_count_buf, "3");
    assert_eq!(app.pipeline.pipeline_loop_edit_prompt_buf, "review again");
    assert_eq!(app.pipeline.pipeline_loop_edit_target, Some((1, 2)));
}

#[test]
fn test_loop_connect_rejects_self_edge() {
    let mut app = pipeline_app_with_two_blocks();
    // Enter loop connect mode from block 1
    app.pipeline.pipeline_loop_connecting_from = Some(1);
    // Cursor is already on block 1
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(app.error_modal.is_some());
    assert!(app.pipeline.pipeline_def.loop_connections.is_empty());
}

#[test]
fn test_loop_connect_creates_loop() {
    use crate::execution::pipeline::PipelineConnection;
    let mut app = pipeline_app_with_two_blocks();
    // Regular connection 1→2 so ancestry check passes
    app.pipeline
        .pipeline_def
        .connections
        .push(PipelineConnection { from: 1, to: 2 });
    // Enter loop connect mode from block 2 (downstream feedback source)
    app.pipeline.pipeline_loop_connecting_from = Some(2);
    // Cursor on block 1 (upstream restart target)
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Enter));
    assert_eq!(app.pipeline.pipeline_def.loop_connections.len(), 1);
    let lc = &app.pipeline.pipeline_def.loop_connections[0];
    assert_eq!(lc.from, 2);
    assert_eq!(lc.to, 1);
    assert_eq!(lc.count, 1);
    assert!(app.pipeline.pipeline_loop_connecting_from.is_none());
}

#[test]
fn test_delete_block_cleans_loops() {
    use crate::execution::pipeline::LoopConnection;
    let mut app = pipeline_app_with_two_blocks();
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 2,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    // Delete block 1
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('d')));
    assert!(app.pipeline.pipeline_def.loop_connections.is_empty());
}

#[test]
fn test_x_includes_loop_connections() {
    use crate::execution::pipeline::LoopConnection;
    let mut app = pipeline_app_with_two_blocks();
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 1,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('x')));
    assert!(app.pipeline.pipeline_removing_conn);
}

#[test]
fn test_regular_connect_allows_loop_pair() {
    use crate::execution::pipeline::LoopConnection;
    let mut app = pipeline_app_with_two_blocks();
    // Add a regular connection first (needed for loop ancestry)
    app.pipeline
        .pipeline_def
        .connections
        .push(crate::execution::pipeline::PipelineConnection { from: 1, to: 2 });
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 2,
            to: 1,
            count: 1,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    // Adding another regular connection between loop endpoint blocks is allowed
    // (as long as it doesn't create a cycle — but same-direction won't)
    // Remove the existing regular connection first, then re-add via UI
    app.pipeline.pipeline_def.connections.clear();
    app.pipeline.pipeline_connecting_from = Some(1);
    app.pipeline.pipeline_block_cursor = Some(2);
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(app.error_modal.is_none());
    assert_eq!(app.pipeline.pipeline_def.connections.len(), 1);
}

#[test]
fn test_loop_edit_saves_on_enter() {
    use crate::execution::pipeline::LoopConnection;
    let mut app = pipeline_app_with_two_blocks();
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 1,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    // Open loop edit popup via 'o' on block 1
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('o')));
    assert!(app.pipeline.pipeline_show_loop_edit);
    // Modify count buffer
    app.pipeline.pipeline_loop_edit_count_buf = "5".to_string();
    // Press Enter on Count field to save
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(!app.pipeline.pipeline_show_loop_edit);
    assert_eq!(app.pipeline.pipeline_def.loop_connections[0].count, 5);
}

#[test]
fn test_loop_edit_esc_discards() {
    use crate::execution::pipeline::LoopConnection;
    let mut app = pipeline_app_with_two_blocks();
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 3,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    // Open loop edit popup via 'o' on block 1
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('o')));
    assert!(app.pipeline.pipeline_show_loop_edit);
    // Modify count buffer but discard via Esc
    app.pipeline.pipeline_loop_edit_count_buf = "99".to_string();
    handle_key(&mut app, key(KeyCode::Esc));
    assert!(!app.pipeline.pipeline_show_loop_edit);
    // Original count should be unchanged
    assert_eq!(app.pipeline.pipeline_def.loop_connections[0].count, 3);
}

#[test]
fn terminal_sweep_uses_effective_status_not_outcome() {
    // Regression: when run_pipeline returns Ok (RunOutcome::Done) but
    // BlockError events already marked state.status = Failed, leftover
    // Queued steps must become Error, not Done.
    use crate::app::{RunState, RunStatus, RunStepStatus};
    use crate::execution::{BatchProgressEvent, RunOutcome};

    let mut app = test_app();
    app.running.is_running = true;

    let labels: Vec<String> = vec![
        "A (Claude)".into(),
        "B (GPT)".into(),
        "B [pass 1] (GPT)".into(),
    ];
    let mut run = RunState::new(1, &labels);
    // Simulate: A finished OK, B finished with error (setting run status),
    // B [pass 1] never ran (still Queued).
    run.status = RunStatus::Failed;
    run.steps[0].status = RunStepStatus::Done;
    run.steps[1].status = RunStepStatus::Error;
    // steps[2] stays Queued — the abandoned loop pass

    app.running.multi_run_states.push(run);

    // Deliver RunFinished with outcome=Done (as run_pipeline returns Ok)
    super::execution::handle_batch_progress(
        &mut app,
        BatchProgressEvent::RunFinished {
            run_id: 1,
            outcome: RunOutcome::Done,
            error: None,
        },
    );

    let steps = &app.running.multi_run_states[0].steps;
    assert_eq!(steps[0].status, RunStepStatus::Done, "A completed OK");
    assert_eq!(steps[1].status, RunStepStatus::Error, "B failed");
    assert_eq!(
        steps[2].status,
        RunStepStatus::Error,
        "abandoned loop pass should be Error, not Done, because run has failures"
    );
}

#[test]
fn terminal_sweep_cancelled_marks_leftover_as_error() {
    // When a run is cancelled, all leftover Queued/Pending steps must become
    // Error (not Done), regardless of any earlier successful steps.
    use crate::app::{RunState, RunStatus, RunStepStatus};
    use crate::execution::{BatchProgressEvent, RunOutcome};

    let mut app = test_app();
    app.running.is_running = true;

    let labels: Vec<String> = vec!["A (Claude)".into(), "B (GPT)".into(), "C (Claude)".into()];
    let mut run = RunState::new(1, &labels);
    // A completed, B was in-flight, C never started.
    run.status = RunStatus::Running;
    run.steps[0].status = RunStepStatus::Done;
    run.steps[1].status = RunStepStatus::Pending;
    // steps[2] stays Queued

    app.running.multi_run_states.push(run);

    super::execution::handle_batch_progress(
        &mut app,
        BatchProgressEvent::RunFinished {
            run_id: 1,
            outcome: RunOutcome::Cancelled,
            error: None,
        },
    );

    let steps = &app.running.multi_run_states[0].steps;
    assert_eq!(
        steps[0].status,
        RunStepStatus::Done,
        "A completed before cancel"
    );
    assert_eq!(
        steps[1].status,
        RunStepStatus::Error,
        "in-flight B should become Error on cancel"
    );
    assert_eq!(
        steps[2].status,
        RunStepStatus::Error,
        "queued C should become Error on cancel"
    );
}

// ---------------------------------------------------------------------------
// Setup Analysis Tests
// ---------------------------------------------------------------------------

fn setup_analysis_app_with_diag() -> App {
    let agents = vec![
        test_agent(
            "Claude",
            ProviderKind::Anthropic,
            "claude-opus-4-6",
            false,
            None,
        ),
        test_agent("OpenAI", ProviderKind::OpenAI, "gpt-4o", false, None),
    ];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.selected_agents = vec!["Claude".into(), "OpenAI".into()];
    app.selected_mode = ExecutionMode::Relay;
    app.prompt.prompt_text = "Analyze this codebase".into();
    app
}

#[test]
fn setup_analysis_prompt_relay_from_prompt_screen() {
    let mut app = setup_analysis_app_with_diag();
    app.screen = Screen::Prompt;
    let prompt = build_setup_analysis_prompt(&app);
    assert!(prompt.contains("Mode: Relay"));
    assert!(prompt.contains("Claude"));
    assert!(prompt.contains("OpenAI"));
    assert!(prompt.contains("Forward Prompt"));
    assert!(prompt.contains("Keep Session"));
    assert!(prompt.contains("Iterations:"));
    assert!(prompt.contains("order can still be changed"));
}

#[test]
fn setup_analysis_prompt_relay_from_order_screen() {
    let mut app = setup_analysis_app_with_diag();
    app.screen = Screen::Order;
    app.order_cursor = 1;
    let prompt = build_setup_analysis_prompt(&app);
    assert!(prompt.contains("Mode: Relay"));
    assert!(prompt.contains("Effective execution order"));
    assert!(prompt.contains("Pressing Enter will start execution"));
    assert!(!prompt.contains("order can still be changed"));
}

#[test]
fn setup_analysis_prompt_swarm() {
    let mut app = setup_analysis_app_with_diag();
    app.selected_mode = ExecutionMode::Swarm;
    app.screen = Screen::Prompt;
    let prompt = build_setup_analysis_prompt(&app);
    assert!(prompt.contains("Mode: Swarm"));
    assert!(prompt.contains("Claude"));
    assert!(prompt.contains("OpenAI"));
    assert!(prompt.contains("Iterations (rounds):"));
}

#[test]
fn setup_analysis_prompt_pipeline() {
    let mut app = setup_analysis_app_with_diag();
    app.selected_mode = ExecutionMode::Pipeline;
    app.screen = Screen::Pipeline;

    use crate::execution::pipeline::{PipelineBlock, PipelineConnection};
    app.pipeline.pipeline_def.initial_prompt = "Research topic".into();
    app.pipeline.pipeline_def.blocks = vec![
        PipelineBlock {
            id: 1,
            name: "Research".into(),
            agents: vec!["Claude".into()],
            prompt: "Do research".into(),
            profiles: vec![],
            session_id: None,
            position: (0, 0),
            replicas: 1,
        },
        PipelineBlock {
            id: 2,
            name: "Analyze".into(),
            agents: vec!["OpenAI".into()],
            prompt: "Analyze results".into(),
            profiles: vec![],
            session_id: Some("shared-1".into()),
            position: (1, 0),
            replicas: 1,
        },
    ];
    app.pipeline.pipeline_def.connections = vec![PipelineConnection { from: 1, to: 2 }];

    let prompt = build_setup_analysis_prompt(&app);
    assert!(prompt.contains("Mode: Pipeline"));
    assert!(prompt.contains("Research"));
    assert!(prompt.contains("Analyze"));
    assert!(prompt.contains("1 \"Research\" -> 2 \"Analyze\""));
    assert!(prompt.contains("Root blocks"));
    assert!(prompt.contains("Terminal blocks"));
    assert!(prompt.contains("Layer 1"));
    assert!(prompt.contains("Layer 2"));
    assert!(prompt.contains("session: shared-1"));
}

#[test]
fn setup_analysis_prompt_pipeline_with_replicas() {
    let mut app = setup_analysis_app_with_diag();
    app.selected_mode = ExecutionMode::Pipeline;
    app.screen = Screen::Pipeline;

    use crate::execution::pipeline::PipelineBlock;
    app.pipeline.pipeline_def.initial_prompt = "test".into();
    app.pipeline.pipeline_def.blocks = vec![PipelineBlock {
        id: 1,
        name: "Worker".into(),
        agents: vec!["Claude".into()],
        prompt: "work".into(),
        profiles: vec![],
        session_id: None,
        position: (0, 0),
        replicas: 3,
    }];

    let prompt = build_setup_analysis_prompt(&app);
    assert!(prompt.contains("replicas: 3"));
    assert!(prompt.contains("Runtime blocks"));
}

#[test]
fn setup_analysis_prompt_multi_run() {
    let mut app = setup_analysis_app_with_diag();
    app.screen = Screen::Prompt;
    app.prompt.runs = 5;
    app.prompt.concurrency = 2;
    let prompt = build_setup_analysis_prompt(&app);
    assert!(prompt.contains("Multi-run: 5 independent runs"));
    assert!(prompt.contains("concurrency 2"));
}

// --- Popup key handling tests ---

#[test]
fn setup_analysis_scroll_j_k() {
    let mut app = test_app();
    app.setup_analysis.active = true;
    app.setup_analysis.content = "line1\nline2\nline3".into();

    handle_key(&mut app, key(KeyCode::Char('j')));
    assert_eq!(app.setup_analysis.scroll, 1);

    handle_key(&mut app, key(KeyCode::Char('k')));
    assert_eq!(app.setup_analysis.scroll, 0);
}

#[test]
fn setup_analysis_esc_closes() {
    let mut app = test_app();
    app.setup_analysis.active = true;
    app.setup_analysis.content = "test".into();

    handle_key(&mut app, key(KeyCode::Esc));
    assert!(!app.setup_analysis.active);
}

#[test]
fn setup_analysis_q_closes() {
    let mut app = test_app();
    app.setup_analysis.active = true;
    app.setup_analysis.content = "test".into();

    handle_key(&mut app, key(KeyCode::Char('q')));
    assert!(!app.setup_analysis.active);
}

#[test]
fn setup_analysis_loading_blocks_scroll() {
    let mut app = test_app();
    app.setup_analysis.active = true;
    app.setup_analysis.loading = true;

    handle_key(&mut app, key(KeyCode::Char('j')));
    assert_eq!(app.setup_analysis.scroll, 0);
}

#[test]
fn setup_analysis_home_end() {
    let mut app = test_app();
    app.setup_analysis.active = true;
    app.setup_analysis.content = "test".into();
    app.setup_analysis.scroll = 5;

    handle_key(&mut app, key(KeyCode::Home));
    assert_eq!(app.setup_analysis.scroll, 0);

    handle_key(&mut app, key(KeyCode::End));
    assert_eq!(app.setup_analysis.scroll, u16::MAX);
}

#[test]
fn setup_analysis_paste_blocked() {
    let mut app = test_app();
    app.setup_analysis.active = true;
    app.setup_analysis.content = "test".into();
    app.screen = Screen::Prompt;
    app.prompt.prompt_focus = PromptFocus::Text;

    let original = app.prompt.prompt_text.clone();
    handle_paste(&mut app, "injected");
    assert_eq!(app.prompt.prompt_text, original);
}

// --- Error path tests ---

#[test]
fn setup_analysis_no_diagnostic_provider() {
    let mut app = test_app();
    app.screen = Screen::Prompt;
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(!app.setup_analysis.loading);
    assert!(app
        .setup_analysis
        .content
        .contains("No diagnostic_provider"));
}

#[test]
fn setup_analysis_missing_agent() {
    let mut config = test_config();
    config.diagnostic_provider = Some("NonExistent".into());
    let mut app = App::new(config);
    app.screen = Screen::Prompt;
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(app.setup_analysis.content.contains("NonExistent"));
    assert!(app.setup_analysis.content.contains("not configured"));
}

#[test]
fn setup_analysis_empty_prompt_without_resume() {
    let agents = vec![test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "claude-opus-4-6",
        false,
        None,
    )];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.screen = Screen::Prompt;
    app.prompt.prompt_text.clear();
    app.prompt.resume_previous = false;
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(app.setup_analysis.content.contains("Enter a prompt first"));
}

#[test]
fn setup_analysis_relay_agent_invalid_runtime() {
    // CLI-mode agent with CLI not installed should be caught
    let agents = vec![
        test_agent(
            "Claude",
            ProviderKind::Anthropic,
            "claude-opus-4-6",
            false,
            None,
        ),
        test_agent("CliAgent", ProviderKind::OpenAI, "gpt-4", true, None),
    ];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.cli_available.insert(ProviderKind::OpenAI, false);
    app.selected_agents = vec!["Claude".into(), "CliAgent".into()];
    app.selected_mode = ExecutionMode::Relay;
    app.screen = Screen::Prompt;
    app.prompt.prompt_text = "test".into();
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(app.setup_analysis.content.contains("CLI is not installed"));
}

#[test]
fn setup_analysis_pipeline_agent_invalid_runtime() {
    // Pipeline block with CLI-mode agent that has no CLI installed
    let agents = vec![
        test_agent(
            "Claude",
            ProviderKind::Anthropic,
            "claude-opus-4-6",
            false,
            None,
        ),
        test_agent("CliAgent", ProviderKind::OpenAI, "gpt-4", true, None),
    ];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.cli_available.insert(ProviderKind::OpenAI, false);
    app.screen = Screen::Pipeline;

    use crate::execution::pipeline::PipelineBlock;
    app.pipeline.pipeline_def.initial_prompt = "test".into();
    app.pipeline.pipeline_def.blocks = vec![PipelineBlock {
        id: 1,
        name: "A".into(),
        agents: vec!["CliAgent".into()],
        prompt: "test".into(),
        profiles: vec![],
        session_id: None,
        position: (0, 0),
        replicas: 1,
    }];
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(app.setup_analysis.content.contains("CLI is not installed"));
}

#[test]
fn setup_analysis_empty_pipeline() {
    let agents = vec![test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "claude-opus-4-6",
        false,
        None,
    )];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.screen = Screen::Pipeline;
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(app.setup_analysis.content.contains("no blocks"));
}

#[test]
fn setup_analysis_invalid_pipeline() {
    let agents = vec![test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "claude-opus-4-6",
        false,
        None,
    )];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.screen = Screen::Pipeline;

    use crate::execution::pipeline::{PipelineBlock, PipelineConnection};
    app.pipeline.pipeline_def.initial_prompt = "test".into();
    app.pipeline.pipeline_def.blocks = vec![PipelineBlock {
        id: 1,
        name: "A".into(),
        agents: vec!["Claude".into()],
        prompt: "test".into(),
        profiles: vec![],
        session_id: None,
        position: (0, 0),
        replicas: 1,
    }];
    // Self-edge
    app.pipeline.pipeline_def.connections = vec![PipelineConnection { from: 1, to: 1 }];
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(app.setup_analysis.content.contains("validation failed"));
}

#[test]
fn setup_analysis_empty_pipeline_initial_prompt() {
    let agents = vec![test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "claude-opus-4-6",
        false,
        None,
    )];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.screen = Screen::Pipeline;

    use crate::execution::pipeline::PipelineBlock;
    app.pipeline.pipeline_def.blocks = vec![PipelineBlock {
        id: 1,
        name: "A".into(),
        agents: vec!["Claude".into()],
        prompt: "test".into(),
        profiles: vec![],
        session_id: None,
        position: (0, 0),
        replicas: 1,
    }];
    app.pipeline.pipeline_def.initial_prompt.clear();
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(app.setup_analysis.content.contains("initial prompt"));
}

#[test]
fn setup_analysis_pipeline_unavailable_agent() {
    let agents = vec![test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "claude-opus-4-6",
        false,
        None,
    )];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.screen = Screen::Pipeline;

    use crate::execution::pipeline::PipelineBlock;
    app.pipeline.pipeline_def.initial_prompt = "test".into();
    app.pipeline.pipeline_def.blocks = vec![PipelineBlock {
        id: 1,
        name: "A".into(),
        agents: vec!["MissingAgent".into()],
        prompt: "test".into(),
        profiles: vec![],
        session_id: None,
        position: (0, 0),
        replicas: 1,
    }];
    start_setup_analysis(&mut app);
    assert!(app.setup_analysis.active);
    assert!(app.setup_analysis.content.contains("not found"));
}

// --- Input precedence tests ---

#[tokio::test]
async fn ctrl_e_from_prompt_text_focus_opens_popup() {
    let mut app = setup_analysis_app_with_diag();
    app.screen = Screen::Prompt;
    app.prompt.prompt_focus = PromptFocus::Text;

    let key = KeyEvent::new(KeyCode::Char('e'), KeyModifiers::CONTROL);
    handle_key(&mut app, key);
    // Should open popup (loading), not insert 'e' into text
    assert!(app.setup_analysis.active || !app.setup_analysis.content.is_empty());
    assert!(
        !app.prompt.prompt_text.contains('e') || app.prompt.prompt_text == "Analyze this codebase"
    );
}

#[test]
fn ctrl_e_from_pipeline_session_name_focus_opens_popup() {
    let agents = vec![test_agent(
        "Claude",
        ProviderKind::Anthropic,
        "claude-opus-4-6",
        false,
        None,
    )];
    let mut config = test_config();
    config.agents = agents;
    config.diagnostic_provider = Some("Claude".into());
    let mut app = App::new(config);
    app.screen = Screen::Pipeline;
    app.pipeline.pipeline_focus = PipelineFocus::SessionName;

    let key = KeyEvent::new(KeyCode::Char('e'), KeyModifiers::CONTROL);
    handle_key(&mut app, key);
    // Should open popup (error about empty pipeline), not insert 'e' into session name
    assert!(app.setup_analysis.active);
    assert!(app.pipeline.pipeline_session_name.is_empty());
}

// --- Result handler tests ---

#[test]
fn setup_analysis_result_ok() {
    let mut app = test_app();
    app.setup_analysis.open_loading();
    handle_setup_analysis_result(&mut app, Ok("Analysis done".into()));
    assert!(!app.setup_analysis.loading);
    assert_eq!(app.setup_analysis.content, "Analysis done");
}

#[test]
fn setup_analysis_result_err() {
    let mut app = test_app();
    app.setup_analysis.open_loading();
    handle_setup_analysis_result(&mut app, Err("timeout".into()));
    assert!(!app.setup_analysis.loading);
    assert!(app.setup_analysis.content.contains("Analysis failed"));
    assert!(app.setup_analysis.content.contains("timeout"));
}

#[test]
fn setup_analysis_result_after_close_discards() {
    let mut app = test_app();
    app.setup_analysis.open_loading();
    app.setup_analysis.close(); // User pressed Esc
    handle_setup_analysis_result(&mut app, Ok("Late result".into()));
    assert!(!app.setup_analysis.active);
    assert!(app.setup_analysis.content.is_empty());
}

#[test]
fn test_delete_internal_block_prunes_loop() {
    // Regression: deleting a block that is internal to a loop's sub-DAG
    // (not an endpoint) must prune the loop through the UI 'd' key handler.
    //
    // Chain: 1→2→3, loop 3→1.
    // Delete block 2 via 'd' key → loop 3→1 should be removed.
    use crate::execution::pipeline::{LoopConnection, PipelineBlock, PipelineConnection};
    let mut app = test_app();
    app.screen = Screen::Pipeline;
    app.pipeline.pipeline_focus = PipelineFocus::Builder;
    app.pipeline.pipeline_def.blocks = vec![
        PipelineBlock {
            id: 1,
            name: "A".into(),
            agents: vec!["Claude".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (0, 0),
            replicas: 1,
        },
        PipelineBlock {
            id: 2,
            name: "B".into(),
            agents: vec!["Claude".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (1, 0),
            replicas: 1,
        },
        PipelineBlock {
            id: 3,
            name: "C".into(),
            agents: vec!["Claude".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (2, 0),
            replicas: 1,
        },
    ];
    app.pipeline.pipeline_def.connections = vec![
        PipelineConnection { from: 1, to: 2 },
        PipelineConnection { from: 2, to: 3 },
    ];
    app.pipeline.pipeline_def.loop_connections = vec![LoopConnection {
        from: 3,
        to: 1,
        count: 1,
        prompt: String::new(),
        break_condition: String::new(),
        break_agent: String::new(),
    }];

    // Select block 2 (internal to the loop sub-DAG) and press 'd'
    app.pipeline.pipeline_block_cursor = Some(2);
    handle_key(&mut app, key(KeyCode::Char('d')));

    // Block 2 should be deleted
    assert!(!app.pipeline.pipeline_def.blocks.iter().any(|b| b.id == 2));
    // Connections involving block 2 should be removed
    assert!(app.pipeline.pipeline_def.connections.is_empty());
    // Loop 3→1 should be pruned because the sub-DAG path is broken
    assert!(
        app.pipeline.pipeline_def.loop_connections.is_empty(),
        "loop 3→1 should be pruned when internal block 2 is deleted"
    );
    // User should see a warning about the pruned loop
    assert!(
        app.error_modal.is_some(),
        "user should be warned about pruned loop"
    );
}

// ---------------------------------------------------------------------------
// Profile order preservation tests
// ---------------------------------------------------------------------------

#[test]
fn edit_dialog_save_preserves_profile_order() {
    let mut app = pipeline_app_with_block();
    // Set up block with specific profile order
    app.pipeline.pipeline_def.blocks[0].profiles =
        vec!["security".into(), "reviewer".into(), "writer".into()];

    // Open edit dialog
    handle_key(&mut app, key(KeyCode::Char('e')));
    assert!(app.pipeline.pipeline_show_edit);

    // Simulate the dialog state as if on-disk profiles are alphabetical
    app.pipeline.pipeline_edit_profile_list =
        vec!["reviewer".into(), "security".into(), "writer".into()];
    app.pipeline.pipeline_edit_profile_selection = vec![true, true, true]; // all selected
    app.pipeline.pipeline_edit_profile_orphaned = vec![];
    // original_order was captured on dialog open
    assert_eq!(
        app.pipeline.pipeline_edit_profile_original_order,
        vec!["security", "reviewer", "writer"]
    );

    // Save without changes (Enter on Name field)
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(!app.pipeline.pipeline_show_edit);

    // Order must be preserved — NOT rewritten to alphabetical
    assert_eq!(
        app.pipeline.pipeline_def.blocks[0].profiles,
        vec!["security", "reviewer", "writer"]
    );
}

#[test]
fn edit_dialog_save_removes_deselected_preserves_order() {
    let mut app = pipeline_app_with_block();
    app.pipeline.pipeline_def.blocks[0].profiles =
        vec!["security".into(), "reviewer".into(), "writer".into()];

    handle_key(&mut app, key(KeyCode::Char('e')));

    // Simulate: reviewer deselected, others kept
    app.pipeline.pipeline_edit_profile_list =
        vec!["reviewer".into(), "security".into(), "writer".into()];
    app.pipeline.pipeline_edit_profile_selection = vec![false, true, true];
    app.pipeline.pipeline_edit_profile_orphaned = vec![];

    handle_key(&mut app, key(KeyCode::Enter));

    // "security" should still come before "writer"
    assert_eq!(
        app.pipeline.pipeline_def.blocks[0].profiles,
        vec!["security", "writer"]
    );
}

#[test]
fn edit_dialog_save_appends_new_profiles_after_existing() {
    let mut app = pipeline_app_with_block();
    app.pipeline.pipeline_def.blocks[0].profiles = vec!["security".into(), "writer".into()];

    handle_key(&mut app, key(KeyCode::Char('e')));

    // Simulate: "formatter" newly selected, existing kept
    app.pipeline.pipeline_edit_profile_list =
        vec!["formatter".into(), "security".into(), "writer".into()];
    app.pipeline.pipeline_edit_profile_selection = vec![true, true, true];
    app.pipeline.pipeline_edit_profile_orphaned = vec![];

    handle_key(&mut app, key(KeyCode::Enter));

    // Original order preserved, new profile appended at end
    assert_eq!(
        app.pipeline.pipeline_def.blocks[0].profiles,
        vec!["security", "writer", "formatter"]
    );
}

// ---------------------------------------------------------------------------
// Pipeline Load Dialog — behavioral tests
// ---------------------------------------------------------------------------

/// Set up an app with the Load dialog open and a fake file list.
fn pipeline_app_with_load_dialog(files: Vec<&str>) -> App {
    let mut app = pipeline_app_with_block();
    let file_list: Vec<String> = files.into_iter().map(String::from).collect();
    let filtered: Vec<usize> = (0..file_list.len()).collect();
    app.pipeline.pipeline_file_dialog = Some(PipelineDialogMode::Load);
    app.pipeline.pipeline_file_list = file_list;
    app.pipeline.pipeline_file_filtered = filtered;
    app.pipeline.pipeline_file_cursor = 0;
    app.pipeline.pipeline_file_search.clear();
    app.pipeline.pipeline_file_search_focus = true;
    app
}

#[test]
fn load_dialog_typing_filters_list() {
    let mut app = pipeline_app_with_load_dialog(vec![
        "review.toml",
        "build.toml",
        "review-v2.toml",
        "deploy.toml",
    ]);

    // Type "rev" — should match "review.toml" and "review-v2.toml"
    handle_key(&mut app, key(KeyCode::Char('r')));
    handle_key(&mut app, key(KeyCode::Char('e')));
    handle_key(&mut app, key(KeyCode::Char('v')));

    assert_eq!(app.pipeline.pipeline_file_search, "rev");
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 2);
    // Both matches should point to the correct original indices
    assert_eq!(app.pipeline.pipeline_file_filtered, vec![0, 2]);
    assert_eq!(app.pipeline.pipeline_file_cursor, 0);
}

#[test]
fn load_dialog_filter_is_case_insensitive() {
    let mut app = pipeline_app_with_load_dialog(vec!["MyPipeline.toml", "other.toml"]);

    handle_key(&mut app, key(KeyCode::Char('m')));
    handle_key(&mut app, key(KeyCode::Char('y')));

    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 1);
    assert_eq!(app.pipeline.pipeline_file_filtered[0], 0);
}

#[test]
fn load_dialog_backspace_widens_filter() {
    let mut app = pipeline_app_with_load_dialog(vec!["abc.toml", "abd.toml", "xyz.toml"]);

    // Type "abc" — 1 match
    for c in ['a', 'b', 'c'] {
        handle_key(&mut app, key(KeyCode::Char(c)));
    }
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 1);

    // Backspace to "ab" — 2 matches
    handle_key(&mut app, key(KeyCode::Backspace));
    assert_eq!(app.pipeline.pipeline_file_search, "ab");
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 2);
}

#[test]
fn load_dialog_tab_toggles_focus() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml", "b.toml"]);

    // Starts in search focus
    assert!(app.pipeline.pipeline_file_search_focus);

    // Tab → list focus
    handle_key(&mut app, key(KeyCode::Tab));
    assert!(!app.pipeline.pipeline_file_search_focus);

    // Tab → back to search focus
    handle_key(&mut app, key(KeyCode::Tab));
    assert!(app.pipeline.pipeline_file_search_focus);
}

#[test]
fn load_dialog_tab_stays_in_search_when_filtered_empty() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml"]);

    // Type something that matches nothing
    handle_key(&mut app, key(KeyCode::Char('z')));
    handle_key(&mut app, key(KeyCode::Char('z')));
    assert!(app.pipeline.pipeline_file_filtered.is_empty());
    assert!(app.pipeline.pipeline_file_search_focus);

    // Tab should NOT toggle to list — nothing to navigate
    handle_key(&mut app, key(KeyCode::Tab));
    assert!(app.pipeline.pipeline_file_search_focus);
}

#[test]
fn load_dialog_esc_clears_search_first_then_closes() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml", "b.toml"]);

    // Type a search query
    handle_key(&mut app, key(KeyCode::Char('a')));
    assert_eq!(app.pipeline.pipeline_file_search, "a");
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 1);

    // First Esc: clears search, restores full list
    handle_key(&mut app, key(KeyCode::Esc));
    assert!(app.pipeline.pipeline_file_search.is_empty());
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 2);
    assert!(app.pipeline.pipeline_file_dialog.is_some());

    // Second Esc: closes dialog
    handle_key(&mut app, key(KeyCode::Esc));
    assert!(app.pipeline.pipeline_file_dialog.is_none());
}

#[test]
fn load_dialog_esc_from_list_focus_closes_immediately() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml", "b.toml"]);

    // Tab to list focus
    handle_key(&mut app, key(KeyCode::Tab));
    assert!(!app.pipeline.pipeline_file_search_focus);

    // Esc from list focus closes dialog immediately
    handle_key(&mut app, key(KeyCode::Esc));
    assert!(app.pipeline.pipeline_file_dialog.is_none());
}

#[test]
fn load_dialog_enter_with_single_match_triggers_load_attempt() {
    let mut app = pipeline_app_with_load_dialog(vec!["target.toml", "other.toml"]);

    // Filter to exactly one match
    handle_key(&mut app, key(KeyCode::Char('t')));
    handle_key(&mut app, key(KeyCode::Char('a')));
    handle_key(&mut app, key(KeyCode::Char('r')));
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 1);

    // Enter from search focus — tries to load (will fail since file doesn't exist,
    // but that shows the error modal which proves the load path was taken)
    handle_key(&mut app, key(KeyCode::Enter));
    // Load attempt was made — either dialog closed (success) or error modal appeared
    assert!(
        app.pipeline.pipeline_file_dialog.is_none() || app.error_modal.is_some(),
        "Enter with single match should attempt to load"
    );
}

#[test]
fn load_dialog_enter_with_multiple_matches_switches_to_list() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml", "ab.toml", "c.toml"]);

    // Filter to 2 matches
    handle_key(&mut app, key(KeyCode::Char('a')));
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 2);
    assert!(app.pipeline.pipeline_file_search_focus);

    // Enter with multiple matches → switch to list focus
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(!app.pipeline.pipeline_file_search_focus);
    assert!(app.pipeline.pipeline_file_dialog.is_some());
}

#[test]
fn load_dialog_enter_with_zero_matches_stays_in_search() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml"]);

    handle_key(&mut app, key(KeyCode::Char('z')));
    assert!(app.pipeline.pipeline_file_filtered.is_empty());

    // Enter with no matches — should stay in search, dialog stays open
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(app.pipeline.pipeline_file_search_focus);
    assert!(app.pipeline.pipeline_file_dialog.is_some());
}

#[test]
fn load_dialog_arrow_keys_navigate_list() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml", "b.toml", "c.toml"]);

    // Switch to list focus
    handle_key(&mut app, key(KeyCode::Tab));
    assert_eq!(app.pipeline.pipeline_file_cursor, 0);

    // Down
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_file_cursor, 1);

    // Down again
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_file_cursor, 2);

    // Down at bottom — stays at 2
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_file_cursor, 2);

    // Up
    handle_key(&mut app, key(KeyCode::Up));
    assert_eq!(app.pipeline.pipeline_file_cursor, 1);

    // Up to top
    handle_key(&mut app, key(KeyCode::Up));
    assert_eq!(app.pipeline.pipeline_file_cursor, 0);

    // Up at top — stays at 0
    handle_key(&mut app, key(KeyCode::Up));
    assert_eq!(app.pipeline.pipeline_file_cursor, 0);
}

#[test]
fn load_dialog_jk_navigate_list() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml", "b.toml", "c.toml"]);

    handle_key(&mut app, key(KeyCode::Tab));

    handle_key(&mut app, key(KeyCode::Char('j')));
    assert_eq!(app.pipeline.pipeline_file_cursor, 1);

    handle_key(&mut app, key(KeyCode::Char('k')));
    assert_eq!(app.pipeline.pipeline_file_cursor, 0);
}

#[test]
fn load_dialog_char_in_list_switches_to_search() {
    let mut app = pipeline_app_with_load_dialog(vec!["a.toml", "b.toml"]);

    // Switch to list focus
    handle_key(&mut app, key(KeyCode::Tab));
    assert!(!app.pipeline.pipeline_file_search_focus);

    // Type a char while in list focus — should switch to search and append char
    handle_key(&mut app, key(KeyCode::Char('x')));
    assert!(app.pipeline.pipeline_file_search_focus);
    assert_eq!(app.pipeline.pipeline_file_search, "x");
}

#[test]
fn load_dialog_paste_routes_to_search() {
    let mut app = pipeline_app_with_load_dialog(vec!["review.toml", "build.toml"]);

    // Paste text — should go to search field and filter
    handle_pipeline_paste(&mut app, "rev");
    assert!(app.pipeline.pipeline_file_search_focus);
    assert_eq!(app.pipeline.pipeline_file_search, "rev");
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 1);
}

#[test]
fn load_dialog_filter_resets_cursor_to_zero() {
    let mut app = pipeline_app_with_load_dialog(vec!["aa.toml", "ab.toml", "ac.toml"]);

    // Navigate to cursor=2 in list
    handle_key(&mut app, key(KeyCode::Tab));
    handle_key(&mut app, key(KeyCode::Down));
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_file_cursor, 2);

    // Type a char (switches to search) — cursor should reset to 0
    handle_key(&mut app, key(KeyCode::Char('b')));
    assert_eq!(app.pipeline.pipeline_file_cursor, 0);
}

#[test]
fn load_dialog_cursor_clamped_after_filter_narrows() {
    let mut app = pipeline_app_with_load_dialog(vec!["alpha.toml", "beta.toml", "gamma.toml"]);

    // Navigate down in list
    handle_key(&mut app, key(KeyCode::Tab));
    handle_key(&mut app, key(KeyCode::Down));
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_file_cursor, 2);

    // Now type to filter — cursor resets to 0 via recompute
    handle_key(&mut app, key(KeyCode::Char('a')));
    assert_eq!(app.pipeline.pipeline_file_cursor, 0);
    // All three filenames contain "a" (alpha, beta, gamma)
    assert_eq!(app.pipeline.pipeline_file_filtered.len(), 3);
}

#[test]
fn load_dialog_scroll_offset_tracks_cursor() {
    const FILE_NAMES: [&str; 30] = [
        "file_00.toml",
        "file_01.toml",
        "file_02.toml",
        "file_03.toml",
        "file_04.toml",
        "file_05.toml",
        "file_06.toml",
        "file_07.toml",
        "file_08.toml",
        "file_09.toml",
        "file_10.toml",
        "file_11.toml",
        "file_12.toml",
        "file_13.toml",
        "file_14.toml",
        "file_15.toml",
        "file_16.toml",
        "file_17.toml",
        "file_18.toml",
        "file_19.toml",
        "file_20.toml",
        "file_21.toml",
        "file_22.toml",
        "file_23.toml",
        "file_24.toml",
        "file_25.toml",
        "file_26.toml",
        "file_27.toml",
        "file_28.toml",
        "file_29.toml",
    ];
    let mut app = pipeline_app_with_load_dialog(FILE_NAMES.to_vec());

    // Default visible rows from Cell is 6.
    let visible = app.pipeline.pipeline_file_visible.get();
    assert_eq!(visible, 6);

    // Switch to list, navigate down past visible area
    handle_key(&mut app, key(KeyCode::Tab));
    for _ in 0..25 {
        handle_key(&mut app, key(KeyCode::Down));
    }
    assert_eq!(app.pipeline.pipeline_file_cursor, 25);
    // Exact expected scroll: cursor + 1 - visible = 25 + 1 - 6 = 20
    assert_eq!(app.pipeline.pipeline_file_scroll, 25 + 1 - visible);
}

#[test]
fn load_dialog_viewport_shrink_corrects_scroll() {
    const FILE_NAMES: [&str; 20] = [
        "file_00.toml",
        "file_01.toml",
        "file_02.toml",
        "file_03.toml",
        "file_04.toml",
        "file_05.toml",
        "file_06.toml",
        "file_07.toml",
        "file_08.toml",
        "file_09.toml",
        "file_10.toml",
        "file_11.toml",
        "file_12.toml",
        "file_13.toml",
        "file_14.toml",
        "file_15.toml",
        "file_16.toml",
        "file_17.toml",
        "file_18.toml",
        "file_19.toml",
    ];
    let mut app = pipeline_app_with_load_dialog(FILE_NAMES.to_vec());

    // Simulate a large viewport: 10 visible rows.
    app.pipeline.pipeline_file_visible.set(10);

    // Navigate to cursor=9 — still visible (rows 0..9), scroll stays 0.
    handle_key(&mut app, key(KeyCode::Tab));
    for _ in 0..9 {
        handle_key(&mut app, key(KeyCode::Down));
    }
    assert_eq!(app.pipeline.pipeline_file_cursor, 9);
    assert_eq!(app.pipeline.pipeline_file_scroll, 0);

    // Simulate terminal shrink: viewport drops to 4 rows.
    // This is what `terminal.draw()` would set after a resize.
    app.pipeline.pipeline_file_visible.set(4);

    // The post-draw scroll sync (adjust_file_dialog_scroll) fires:
    adjust_file_dialog_scroll(&mut app);

    // Scroll must advance so cursor=9 is in the 4-row viewport.
    // Expected: 9 + 1 - 4 = 6
    assert_eq!(app.pipeline.pipeline_file_scroll, 9 + 1 - 4);
}

// --- Feed list picker tests ---

fn pipeline_app_with_fin_and_feeds(n: usize) -> App {
    use crate::execution::pipeline::{DataFeed, FeedCollection, FeedGranularity, PipelineBlock};
    let mut app = test_app();
    app.screen = Screen::Pipeline;
    app.pipeline.pipeline_focus = PipelineFocus::Builder;

    let fin_id = (n + 1) as u32;

    for i in 1..=n {
        app.pipeline.pipeline_def.blocks.push(PipelineBlock {
            id: i as u32,
            name: format!("Exec{i}"),
            agents: vec!["Claude".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: ((i - 1) as u16, 0),
            replicas: 1,
        });
        app.pipeline.pipeline_def.data_feeds.push(DataFeed {
            from: i as u32,
            to: fin_id,
            collection: FeedCollection::LastPass,
            granularity: FeedGranularity::PerRun,
        });
    }

    app.pipeline
        .pipeline_def
        .finalization_blocks
        .push(PipelineBlock {
            id: fin_id,
            name: "Fin".into(),
            agents: vec!["Claude".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (0, 1),
            replicas: 1,
        });

    app.pipeline.pipeline_block_cursor = Some(fin_id);
    app.pipeline.pipeline_next_id = fin_id + 1;
    app
}

#[test]
fn f_on_fin_no_feeds_shows_error() {
    let mut app = pipeline_app_with_fin_and_feeds(0);
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert!(app.error_modal.is_some());
    assert!(!app.pipeline.pipeline_show_feed_list);
    assert!(!app.pipeline.pipeline_show_feed_edit);
}

#[test]
fn f_on_fin_one_feed_opens_edit_directly() {
    let mut app = pipeline_app_with_fin_and_feeds(1);
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert!(app.pipeline.pipeline_show_feed_edit);
    assert!(!app.pipeline.pipeline_show_feed_list);
    assert_eq!(app.pipeline.pipeline_feed_edit_target, Some((1, 2)));
}

#[test]
fn f_on_fin_two_feeds_opens_list() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert!(app.pipeline.pipeline_show_feed_list);
    assert_eq!(app.pipeline.pipeline_feed_list_target, Some(3));
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 0);
    assert!(!app.pipeline.pipeline_show_feed_edit);
}

#[test]
fn feed_list_nav_wraps_down() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 0);
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 1);
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 0);
}

#[test]
fn feed_list_nav_wraps_up() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 0);
    handle_key(&mut app, key(KeyCode::Up));
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 1);
}

#[test]
fn feed_list_enter_opens_edit_keeps_list() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    handle_key(&mut app, key(KeyCode::Char('f')));
    handle_key(&mut app, key(KeyCode::Down)); // cursor at 1
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(app.pipeline.pipeline_show_feed_edit);
    assert_eq!(app.pipeline.pipeline_feed_edit_target, Some((2, 3)));
    assert!(app.pipeline.pipeline_show_feed_list);
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 1);
}

#[test]
fn esc_from_edit_returns_to_list() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    handle_key(&mut app, key(KeyCode::Char('f')));
    handle_key(&mut app, key(KeyCode::Enter)); // open edit
    assert!(app.pipeline.pipeline_show_feed_edit);
    assert!(app.pipeline.pipeline_show_feed_list);
    handle_key(&mut app, key(KeyCode::Esc)); // close edit
    assert!(!app.pipeline.pipeline_show_feed_edit);
    assert!(app.pipeline.pipeline_feed_edit_target.is_none());
    assert!(app.pipeline.pipeline_show_feed_list);
}

#[test]
fn esc_from_list_closes_all() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert!(app.pipeline.pipeline_show_feed_list);
    handle_key(&mut app, key(KeyCode::Esc));
    assert!(!app.pipeline.pipeline_show_feed_list);
    assert!(app.pipeline.pipeline_feed_list_target.is_none());
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 0);
}

#[test]
fn feed_list_f_deletes_and_clamps() {
    let mut app = pipeline_app_with_fin_and_feeds(3);
    handle_key(&mut app, key(KeyCode::Char('f')));
    // Move cursor to last item (index 2)
    handle_key(&mut app, key(KeyCode::Down));
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 2);
    handle_key(&mut app, key(KeyCode::Char('F')));
    let remaining = app
        .pipeline
        .pipeline_def
        .data_feeds
        .iter()
        .filter(|f| f.to == 4)
        .count();
    assert_eq!(remaining, 2);
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 1);
    assert!(app.pipeline.pipeline_show_feed_list);
}

#[test]
fn feed_list_f_deletes_until_empty_closes() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    handle_key(&mut app, key(KeyCode::Char('f')));
    // Delete first feed
    handle_key(&mut app, key(KeyCode::Char('F')));
    let remaining = app
        .pipeline
        .pipeline_def
        .data_feeds
        .iter()
        .filter(|f| f.to == 3)
        .count();
    assert_eq!(remaining, 1);
    assert!(app.pipeline.pipeline_show_feed_list);
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 0);
    // Delete second feed
    handle_key(&mut app, key(KeyCode::Char('F')));
    let remaining = app
        .pipeline
        .pipeline_def
        .data_feeds
        .iter()
        .filter(|f| f.to == 3)
        .count();
    assert_eq!(remaining, 0);
    assert!(!app.pipeline.pipeline_show_feed_list);
    assert!(app.pipeline.pipeline_feed_list_target.is_none());
    assert_eq!(app.pipeline.pipeline_feed_list_cursor, 0);
}

#[test]
fn feed_list_suppresses_canvas_cursor() {
    let app = pipeline_app_with_fin_and_feeds(2);
    // When feed list is not shown, no overlay from it
    let has_overlay_without = app.pipeline.pipeline_show_feed_list;
    assert!(!has_overlay_without);

    let mut app2 = pipeline_app_with_fin_and_feeds(2);
    app2.pipeline.pipeline_show_feed_list = true;
    assert!(app2.pipeline.pipeline_show_feed_list);
}

#[test]
fn paste_is_noop_when_feed_list_open() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    // Open the feed list via 'f' on builder focus (default from helper)
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert!(app.pipeline.pipeline_show_feed_list);
    // Now switch underlying focus to InitialPrompt to verify paste doesn't leak through
    app.pipeline.pipeline_focus = PipelineFocus::InitialPrompt;
    app.pipeline.pipeline_def.initial_prompt = String::new();
    handle_pipeline_paste(&mut app, "should not appear");
    assert!(app.pipeline.pipeline_def.initial_prompt.is_empty());
}

#[test]
fn paste_is_noop_when_feed_edit_open() {
    let mut app = pipeline_app_with_fin_and_feeds(1);
    // Open feed edit via 'f' on builder focus (single feed -> direct edit)
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert!(app.pipeline.pipeline_show_feed_edit);
    app.pipeline.pipeline_focus = PipelineFocus::InitialPrompt;
    app.pipeline.pipeline_def.initial_prompt = String::new();
    handle_pipeline_paste(&mut app, "should not appear");
    assert!(app.pipeline.pipeline_def.initial_prompt.is_empty());
}

#[test]
fn feed_list_f_deletes_exact_feed_by_index() {
    use crate::execution::pipeline::{DataFeed, FeedCollection, FeedGranularity};
    // Create a setup with two feeds that have the same (from, to) but different settings
    // to verify index-based deletion removes exactly one
    let mut app = pipeline_app_with_fin_and_feeds(2);
    let fin_id = 3;
    // Add a duplicate-source feed (same from=1, to=3 but different collection)
    app.pipeline.pipeline_def.data_feeds.push(DataFeed {
        from: 1,
        to: fin_id,
        collection: FeedCollection::AllPasses,
        granularity: FeedGranularity::AllRuns,
    });
    // Now there are 3 feeds to fin_id: from=1, from=2, from=1(dup)
    handle_key(&mut app, key(KeyCode::Char('f')));
    assert!(app.pipeline.pipeline_show_feed_list);
    // Cursor at 0 → first feed (from=1, LastPass)
    handle_key(&mut app, key(KeyCode::Char('F')));
    // Should delete exactly 1 feed, leaving 2
    let remaining: Vec<_> = app
        .pipeline
        .pipeline_def
        .data_feeds
        .iter()
        .filter(|f| f.to == fin_id)
        .collect();
    assert_eq!(remaining.len(), 2);
    // The duplicate (from=1, AllPasses) should still be present
    assert!(remaining
        .iter()
        .any(|f| f.from == 1 && f.collection == FeedCollection::AllPasses));
}

#[test]
fn canvas_f_on_multi_feed_fin_opens_list() {
    let mut app = pipeline_app_with_fin_and_feeds(2);
    let feed_count_before = app.pipeline.pipeline_def.data_feeds.len();
    handle_key(&mut app, key(KeyCode::Char('F')));
    // Should open feed list instead of deleting
    assert!(app.pipeline.pipeline_show_feed_list);
    assert_eq!(app.pipeline.pipeline_feed_list_target, Some(3));
    // No feed was deleted
    assert_eq!(
        app.pipeline.pipeline_def.data_feeds.len(),
        feed_count_before
    );
}

#[test]
fn canvas_f_on_single_feed_fin_deletes_directly() {
    let mut app = pipeline_app_with_fin_and_feeds(1);
    assert_eq!(app.pipeline.pipeline_def.data_feeds.len(), 1);
    handle_key(&mut app, key(KeyCode::Char('F')));
    // Should delete directly, not open list
    assert!(!app.pipeline.pipeline_show_feed_list);
    assert_eq!(app.pipeline.pipeline_def.data_feeds.len(), 0);
}

#[test]
fn canvas_f_on_exec_with_multi_feeds_shows_error() {
    use crate::execution::pipeline::{DataFeed, FeedCollection, FeedGranularity, PipelineBlock};
    let mut app = pipeline_app_with_fin_and_feeds(1);
    // Add a second finalization block and a second feed from the same exec block
    app.pipeline
        .pipeline_def
        .finalization_blocks
        .push(PipelineBlock {
            id: 10,
            name: "Fin2".into(),
            agents: vec!["Claude".into()],
            prompt: String::new(),
            profiles: vec![],
            session_id: None,
            position: (1, 1),
            replicas: 1,
        });
    app.pipeline.pipeline_def.data_feeds.push(DataFeed {
        from: 1,
        to: 10,
        collection: FeedCollection::LastPass,
        granularity: FeedGranularity::PerRun,
    });
    // Cursor on execution block 1, which now has 2 outgoing feeds
    app.pipeline.pipeline_block_cursor = Some(1);
    let feed_count = app.pipeline.pipeline_def.data_feeds.len();
    handle_key(&mut app, key(KeyCode::Char('F')));
    // Should show error, not delete
    assert!(app.error_modal.is_some());
    assert!(!app.pipeline.pipeline_show_feed_list);
    assert_eq!(app.pipeline.pipeline_def.data_feeds.len(), feed_count);
}

// ---------------------------------------------------------------------------
// Loop edit: BackTab field cycling
// ---------------------------------------------------------------------------

#[test]
fn loop_edit_backtab_cycles_fields_backward() {
    use crate::app::PipelineLoopEditField;
    use crate::execution::pipeline::LoopConnection;

    let mut app = pipeline_app_with_two_blocks();
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 1,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('o')));
    assert!(app.pipeline.pipeline_show_loop_edit);
    assert_eq!(
        app.pipeline.pipeline_loop_edit_field,
        PipelineLoopEditField::Count
    );

    // BackTab from Count → BreakCondition
    handle_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(
        app.pipeline.pipeline_loop_edit_field,
        PipelineLoopEditField::BreakCondition
    );
    // BackTab from BreakCondition → BreakAgent
    handle_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(
        app.pipeline.pipeline_loop_edit_field,
        PipelineLoopEditField::BreakAgent
    );
    // BackTab from BreakAgent → Prompt
    handle_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(
        app.pipeline.pipeline_loop_edit_field,
        PipelineLoopEditField::Prompt
    );
    // BackTab from Prompt → Count (full cycle)
    handle_key(&mut app, key(KeyCode::BackTab));
    assert_eq!(
        app.pipeline.pipeline_loop_edit_field,
        PipelineLoopEditField::Count
    );
}

// ---------------------------------------------------------------------------
// Loop edit: count field fresh-replacement behavior
// ---------------------------------------------------------------------------

#[test]
fn loop_edit_count_fresh_replaces_on_first_digit() {
    use crate::execution::pipeline::LoopConnection;

    let mut app = pipeline_app_with_two_blocks();
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 1,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('o')));
    assert!(app.pipeline.pipeline_show_loop_edit);
    assert_eq!(app.pipeline.pipeline_loop_edit_count_buf, "1");
    assert!(app.pipeline.pipeline_loop_edit_count_fresh);

    // Typing '5' should replace '1' with '5', not produce '15'
    handle_key(&mut app, key(KeyCode::Char('5')));
    assert_eq!(app.pipeline.pipeline_loop_edit_count_buf, "5");
    assert!(!app.pipeline.pipeline_loop_edit_count_fresh);

    // Subsequent digits should append normally
    handle_key(&mut app, key(KeyCode::Char('3')));
    assert_eq!(app.pipeline.pipeline_loop_edit_count_buf, "53");
}

#[test]
fn loop_edit_count_empty_defaults_to_1_on_save() {
    use crate::execution::pipeline::LoopConnection;

    let mut app = pipeline_app_with_two_blocks();
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 5,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('o')));

    // Clear via backspace (fresh flag is consumed by backspace)
    handle_key(&mut app, key(KeyCode::Backspace));
    assert!(app.pipeline.pipeline_loop_edit_count_buf.is_empty());

    // Save with empty buffer — should default to 1
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(!app.pipeline.pipeline_show_loop_edit);
    assert_eq!(app.pipeline.pipeline_def.loop_connections[0].count, 1);
}

// ---------------------------------------------------------------------------
// Loop edit: break agent cursor vs selected semantics
// ---------------------------------------------------------------------------

#[test]
fn loop_edit_break_agent_cursor_does_not_change_selection() {
    use crate::app::PipelineLoopEditField;
    use crate::execution::pipeline::LoopConnection;

    let mut app = pipeline_app_with_two_blocks();
    app.config.agents.push(test_agent(
        "TestAgent",
        ProviderKind::Anthropic,
        "test",
        false,
        None,
    ));
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 1,
            prompt: String::new(),
            break_condition: String::new(),
            break_agent: String::new(),
        });
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('o')));

    // Navigate to BreakAgent field
    handle_key(&mut app, key(KeyCode::Tab));
    handle_key(&mut app, key(KeyCode::Tab));
    assert_eq!(
        app.pipeline.pipeline_loop_edit_field,
        PipelineLoopEditField::BreakAgent
    );

    // Cursor at 0 ((none)), selected at 0
    assert_eq!(app.pipeline.pipeline_loop_edit_break_agent_idx, 0);
    assert_eq!(app.pipeline.pipeline_loop_edit_break_agent_selected, 0);

    // Move cursor down — selection must not change
    handle_key(&mut app, key(KeyCode::Down));
    assert_eq!(app.pipeline.pipeline_loop_edit_break_agent_idx, 1);
    assert_eq!(app.pipeline.pipeline_loop_edit_break_agent_selected, 0);

    // Confirm with Space — now selection changes
    handle_key(&mut app, key(KeyCode::Char(' ')));
    assert_eq!(app.pipeline.pipeline_loop_edit_break_agent_selected, 1);
}

#[test]
fn loop_edit_saves_confirmed_selection_not_cursor() {
    use crate::app::PipelineLoopEditField;
    use crate::execution::pipeline::LoopConnection;

    let mut app = pipeline_app_with_two_blocks();
    app.config.agents.push(test_agent(
        "BreakBot",
        ProviderKind::Anthropic,
        "test",
        false,
        None,
    ));
    app.pipeline
        .pipeline_def
        .loop_connections
        .push(LoopConnection {
            from: 1,
            to: 2,
            count: 1,
            prompt: String::new(),
            break_condition: "stop when done".into(),
            break_agent: "BreakBot".into(),
        });
    app.pipeline.pipeline_block_cursor = Some(1);
    handle_key(&mut app, key(KeyCode::Char('o')));

    // Verify it loaded with BreakBot selected (index 1)
    assert_eq!(app.pipeline.pipeline_loop_edit_break_agent_selected, 1);

    // Navigate to BreakAgent field and move cursor to (none)
    handle_key(&mut app, key(KeyCode::Tab));
    handle_key(&mut app, key(KeyCode::Tab));
    assert_eq!(
        app.pipeline.pipeline_loop_edit_field,
        PipelineLoopEditField::BreakAgent
    );
    handle_key(&mut app, key(KeyCode::Up));
    assert_eq!(app.pipeline.pipeline_loop_edit_break_agent_idx, 0);
    // Selection still at BreakBot
    assert_eq!(app.pipeline.pipeline_loop_edit_break_agent_selected, 1);

    // Tab away and save — should keep the confirmed BreakBot, not cursor (none)
    handle_key(&mut app, key(KeyCode::BackTab));
    handle_key(&mut app, key(KeyCode::BackTab));
    // Now on Count — Enter saves
    handle_key(&mut app, key(KeyCode::Enter));
    assert!(!app.pipeline.pipeline_show_loop_edit);
    assert_eq!(
        app.pipeline.pipeline_def.loop_connections[0].break_agent,
        "BreakBot"
    );
}

// ---------------------------------------------------------------------------
// Progress normalization: cross-mode state leak regression test
// ---------------------------------------------------------------------------

#[test]
fn progress_normalization_does_not_leak_across_modes() {
    // Regression: after a pipeline run, expected_total_steps was not reset.
    // A subsequent relay/swarm run would inherit the stale pipeline total,
    // causing the AllDone normalization to produce impossible counts (e.g., 14/3).
    let mut app = test_app();

    // Simulate a completed pipeline run with expected_total_steps = 14
    app.running.expected_total_steps = 14;
    app.running.completed_steps = 14;
    app.running.is_running = true;
    handle_progress(&mut app, ProgressEvent::AllDone);
    assert!(!app.running.is_running);

    // Now start a fresh relay run — reset_running_state calls clear_activity
    app.reset_running_state();
    assert_eq!(
        app.running.expected_total_steps, 0,
        "expected_total_steps must be reset between runs"
    );
    assert_eq!(
        app.running.completed_steps, 0,
        "completed_steps must be reset between runs"
    );

    // Simulate relay completing with 3 steps
    app.running.is_running = true;
    app.record_progress(ProgressEvent::AgentStarted {
        agent: "Claude".into(),
        kind: ProviderKind::Anthropic,
        iteration: 1,
    });
    app.record_progress(ProgressEvent::AgentFinished {
        agent: "Claude".into(),
        kind: ProviderKind::Anthropic,
        iteration: 1,
    });
    handle_progress(&mut app, ProgressEvent::AllDone);
    // completed_steps should reflect relay reality (1), not stale pipeline total
    assert_eq!(
        app.completed_steps(),
        1,
        "completed_steps must not be inflated by stale expected_total_steps"
    );
}
