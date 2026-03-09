use super::consolidation::*;
use super::diagnostics::*;
use super::execution::*;
use super::input::*;
use super::results::*;
use super::resume::*;
use super::text_edit::*;
use super::*;
use crate::config::AppConfig;
use crate::error::AppError;
use crate::execution::ProgressEvent;
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
    let agent_values = agents
        .iter()
        .map(|a| format!("\"{a}\""))
        .collect::<Vec<_>>()
        .join(", ");
    fs::write(
        run_dir.join("session.toml"),
        format!("mode = \"{mode}\"\nagents = [{agent_values}]\n"),
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
    // {name}_b{id}_{agent}_iter{n}.md
    assert_eq!(
        parse_pipeline_iteration_filename("Analyzer_b1_Claude_iter2.md"),
        Some(2)
    );
}

#[test]
fn parse_pipeline_iteration_unnamed_block() {
    // unnamed blocks use block{id}_{agent}_iter{n}.md pattern
    assert_eq!(
        parse_pipeline_iteration_filename("block1_openai_iter5.md"),
        Some(5)
    );
}

#[test]
fn parse_block_iteration_different_agent() {
    assert_eq!(
        parse_pipeline_iteration_filename("Reviewer_b3_Gemini_iter5.md"),
        Some(5)
    );
}

#[test]
fn parse_block_iteration_not_md() {
    assert_eq!(
        parse_pipeline_iteration_filename("Analyzer_b1_Claude_iter2.txt"),
        None
    );
}

#[test]
fn parse_block_iteration_no_block_id_marker() {
    assert_eq!(parse_pipeline_iteration_filename("Claude_iter2.md"), None);
}

#[test]
fn parse_block_iteration_non_numeric_block_id() {
    assert_eq!(
        parse_pipeline_iteration_filename("Analyzer_bx_Claude_iter2.md"),
        None
    );
}

#[test]
fn parse_block_iteration_name_contains_b() {
    // Block name "web_builder" contains "_b" — parser must skip it and find _b7_
    assert_eq!(
        parse_pipeline_iteration_filename("web_builder_b7_Claude_iter1.md"),
        Some(1)
    );
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
    fs::write(dir.path().join("Analyzer_b1_Claude_iter3.md"), "test").unwrap();
    fs::write(dir.path().join("Reviewer_b2_Gemini_iter3.md"), "test").unwrap();
    assert_eq!(find_last_iteration(dir.path(), &[]), Some(3));
}

#[tokio::test]
async fn find_last_iteration_async_matches_sync_for_pipeline_files() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("Analyzer_b1_Claude_iter2.md"), "test").unwrap();
    fs::write(dir.path().join("Reviewer_b2_Gemini_iter5.md"), "test").unwrap();

    let sync = find_last_iteration(dir.path(), &[]);
    let async_found = find_last_iteration_async(dir.path(), &[]).await;
    assert_eq!(async_found, sync);
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
        &["anthropic".to_string(), "openai".to_string()]
    ));
}

#[test]
fn run_dir_matches_wrong_mode() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "swarm", &["anthropic", "openai"]);

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()]
    ));
}

#[test]
fn run_dir_matches_relay_requires_exact_agent_order() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string()]
    ));
}

#[test]
fn run_dir_matches_relay_rejects_different_agent_order() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["openai".to_string(), "anthropic".to_string()]
    ));
}

#[test]
fn run_dir_matches_swarm_accepts_different_agent_order_for_same_set() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "swarm", &["anthropic", "openai"]);

    assert!(run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Swarm,
        &["openai".to_string(), "anthropic".to_string()]
    ));
}

#[test]
fn run_dir_matches_missing_agent() {
    let dir = tempdir().unwrap();
    write_session_toml(dir.path(), "relay", &["anthropic"]);

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "gemini".to_string()]
    ));
}

#[test]
fn run_dir_matches_no_session_toml() {
    let dir = tempdir().unwrap();
    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string()]
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
        &["anthropic".to_string(), "openai".to_string()]
    ));
}

#[test]
fn run_dir_matches_mode_and_agents_missing_agents_field() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("session.toml"), "mode = \"relay\"\n").unwrap();

    assert!(!run_dir_matches_mode_and_agents(
        dir.path(),
        ExecutionMode::Relay,
        &["anthropic".to_string(), "openai".to_string()]
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
            &["anthropic".to_string(), "openai".to_string()]
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
            &["anthropic".to_string(), "openai".to_string()]
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
            &["anthropic".to_string(), "openai".to_string()]
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
    )
    .expect_err("should reject");
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
        ConsolidationRequest::new(
            batch_root.run_dir().clone(),
            ConsolidationTarget::PerRun,
            ExecutionMode::Solo,
            vec!["Claude".to_string(), "OpenAI".to_string()],
            vec![1, 2],
            false,
            String::new(),
            "Claude".to_string(),
            false,
        ),
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
fn prompt_focus_cycle_solo_tab_and_backtab() {
    let mut app = test_app();
    app.selected_mode = ExecutionMode::Solo;
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
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Text);

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
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Text);

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
    assert_eq!(app.prompt.prompt_focus, PromptFocus::Text);

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
        agent: "agent".into(),
        prompt: String::new(),
        session_id: None,
        position: (2, 2),
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
            agent: "agent".into(),
            prompt: String::new(),
            session_id: None,
            position: (2, 2),
        },
        pipeline_mod::PipelineBlock {
            id: 2,
            name: "two".into(),
            agent: "agent".into(),
            prompt: String::new(),
            session_id: None,
            position: (3, 2),
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
            agent: "agent".into(),
            prompt: String::new(),
            session_id: None,
            position: (2, 2),
        },
        pipeline_mod::PipelineBlock {
            id: 2,
            name: "two".into(),
            agent: "agent".into(),
            prompt: String::new(),
            session_id: None,
            position: (3, 2),
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
            &["anthropic".to_string(), "openai".to_string()]
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
            &["anthropic".to_string(), "openai".to_string()]
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
            &["anthropic".to_string(), "openai".to_string()]
        ),
        Some(good)
    );
}
