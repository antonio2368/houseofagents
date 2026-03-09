use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent, PromptRuntimeContext};
use crate::output::OutputManager;
use crate::provider::Provider;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub async fn run_solo(
    prompt_context: &PromptRuntimeContext,
    agents: Vec<(String, Box<dyn Provider>)>,
    use_cli_by_agent: HashMap<String, bool>,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let mut handles: Vec<(String, crate::provider::ProviderKind, JoinHandle<()>)> = Vec::new();

    for (name, provider) in agents {
        let prompt = prompt_context
            .initial_prompt_for_agent(use_cli_by_agent.get(&name).copied().unwrap_or(false));
        let tx = progress_tx.clone();
        let cancel = cancel.clone();
        let run_dir = output.run_dir().clone();
        let provider_kind = provider.kind();
        let agent_name = name.clone();

        let handle =
            tokio::spawn(
                async move { solo_agent(name, provider, &prompt, &run_dir, tx, cancel).await },
            );
        handles.push((agent_name, provider_kind, handle));
    }

    for (agent_name, kind, handle) in handles {
        if let Err(join_error) = handle.await {
            let error = format!("Solo worker panicked: {join_error}");
            let _ = progress_tx.send(ProgressEvent::AgentError {
                agent: agent_name.clone(),
                kind,
                iteration: 1,
                error: error.clone(),
                details: Some(error.clone()),
            });
            let _ = output.append_error(&format!("{agent_name} iter1: {error}"));
        }
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

async fn solo_agent(
    name: String,
    mut provider: Box<dyn Provider>,
    prompt: &str,
    run_dir: &std::path::Path,
    tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) {
    let kind = provider.kind();

    if cancel.load(Ordering::Relaxed) {
        return;
    }

    let _ = tx.send(ProgressEvent::AgentStarted {
        agent: name.clone(),
        kind,
        iteration: 1,
    });
    let _ = tx.send(ProgressEvent::AgentLog {
        agent: name.clone(),
        kind,
        iteration: 1,
        message: "Sending request...".into(),
    });

    let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
    provider.set_live_log_sender(Some(live_tx));
    let live_name = name.clone();
    let live_progress_tx = tx.clone();
    let live_forward = tokio::spawn(async move {
        while let Some(line) = live_rx.recv().await {
            let _ = live_progress_tx.send(ProgressEvent::AgentLog {
                agent: live_name.clone(),
                kind,
                iteration: 1,
                message: format!("CLI {line}"),
            });
        }
    });

    let result = tokio::select! {
        res = provider.send(prompt) => Some(res),
        _ = wait_for_cancel(&cancel) => {
            let _ = tx.send(ProgressEvent::AgentLog {
                agent: name.clone(), kind, iteration: 1, message: "Cancelled".into(),
            });
            None
        }
    };
    provider.set_live_log_sender(None);
    let _ = live_forward.await;

    let Some(result) = result else {
        return;
    };

    match result {
        Ok(resp) => {
            for log in &resp.debug_logs {
                let _ = tx.send(ProgressEvent::AgentLog {
                    agent: name.clone(),
                    kind,
                    iteration: 1,
                    message: format!("CLI {log}"),
                });
            }
            let preview = resp.content.lines().take(3).collect::<Vec<_>>().join(" | ");
            let _ = tx.send(ProgressEvent::AgentLog {
                agent: name.clone(),
                kind,
                iteration: 1,
                message: format!(
                    "Response received ({} chars): {}...",
                    resp.content.len(),
                    truncate_chars(&preview, 80)
                ),
            });
            let sanitized = OutputManager::sanitize_session_name(&name);
            let filename = format!("{sanitized}_iter1.md");
            let path = run_dir.join(&filename);
            if let Err(e) = tokio::fs::write(&path, &resp.content).await {
                let err = format!("Failed to write output file {}: {e}", path.display());
                let _ = tx.send(ProgressEvent::AgentError {
                    agent: name.clone(),
                    kind,
                    iteration: 1,
                    error: err.clone(),
                    details: Some(err),
                });
                return;
            }
            let _ = tx.send(ProgressEvent::AgentFinished {
                agent: name,
                kind,
                iteration: 1,
            });
        }
        Err(e) => {
            let err_str = e.to_string();
            let _ = tx.send(ProgressEvent::AgentError {
                agent: name,
                kind,
                iteration: 1,
                error: err_str.clone(),
                details: Some(err_str),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::test_utils::{collect_progress_events, MockProvider, PanicProvider};
    use crate::provider::ProviderKind;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn named(
        name: &str,
        _kind: ProviderKind,
        provider: Box<dyn Provider>,
    ) -> (String, Box<dyn Provider>) {
        (name.to_string(), provider)
    }

    fn context(prompt: &str) -> PromptRuntimeContext {
        PromptRuntimeContext::new(prompt, false)
    }

    #[tokio::test]
    async fn run_solo_success_writes_outputs_and_all_done() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), Some("solo")).expect("out");
        let received = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![
            named(
                "Claude",
                ProviderKind::Anthropic,
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "a1",
                    received.clone(),
                )),
            ),
            named(
                "OpenAI",
                ProviderKind::OpenAI,
                Box::new(MockProvider::ok(
                    ProviderKind::OpenAI,
                    "o1",
                    received.clone(),
                )),
            ),
        ];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_solo(&context("prompt"), agents, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        assert_eq!(received.lock().expect("lock").len(), 2);
        assert!(out.run_dir().join("Claude_iter1.md").exists());
        assert!(out.run_dir().join("OpenAI_iter1.md").exists());

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
        assert!(events.iter().any(|e| matches!(
            e,
            ProgressEvent::AgentStarted {
                kind: ProviderKind::Anthropic,
                ..
            }
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            ProgressEvent::AgentFinished {
                kind: ProviderKind::OpenAI,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn run_solo_only_cli_agents_receive_working_directory_prefix() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), Some("solo-cli-prefix")).expect("out");
        let recv_api = Arc::new(Mutex::new(Vec::new()));
        let recv_cli = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![
            named(
                "Claude",
                ProviderKind::Anthropic,
                Box::new(MockProvider::ok(
                    ProviderKind::Anthropic,
                    "api",
                    recv_api.clone(),
                )),
            ),
            named(
                "OpenAI",
                ProviderKind::OpenAI,
                Box::new(MockProvider::ok(
                    ProviderKind::OpenAI,
                    "cli",
                    recv_cli.clone(),
                )),
            ),
        ];
        let mut use_cli = HashMap::new();
        use_cli.insert("OpenAI".to_string(), true);
        let (tx, _rx) = mpsc::unbounded_channel();

        run_solo(
            &context("prompt"),
            agents,
            use_cli,
            &out,
            tx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("run");

        let api_prompt = recv_api.lock().expect("lock")[0].clone();
        let cli_prompt = recv_cli.lock().expect("lock")[0].clone();
        assert_eq!(api_prompt, "prompt");
        assert!(cli_prompt.contains("Working directory:"));
        assert!(cli_prompt.ends_with("prompt"));
    }

    #[tokio::test]
    async fn run_solo_provider_error_emits_agent_error() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let received = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named(
            "Gemini",
            ProviderKind::Gemini,
            Box::new(MockProvider::err(ProviderKind::Gemini, "boom", received)),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_solo(&context("prompt"), agents, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|e| {
            matches!(
                e,
                ProgressEvent::AgentError {
                    kind: ProviderKind::Gemini,
                    ..
                }
            )
        }));
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn run_solo_write_failure_emits_agent_error() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        std::fs::create_dir_all(out.run_dir().join("Claude_iter1.md")).expect("mkdir");
        let received = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(MockProvider::ok(
                ProviderKind::Anthropic,
                "content",
                received,
            )),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_solo(&context("prompt"), agents, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|e| {
            matches!(
                e,
                ProgressEvent::AgentError { kind: ProviderKind::Anthropic, error, .. }
                if error.contains("Failed to write output file")
            )
        }));
        assert!(!events.iter().any(|e| {
            matches!(
                e,
                ProgressEvent::AgentFinished {
                    kind: ProviderKind::Anthropic,
                    ..
                }
            )
        }));
    }

    #[tokio::test]
    async fn run_solo_cancelled_before_start_skips_agent_work() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let received = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(MockProvider::ok(
                ProviderKind::Anthropic,
                "unused",
                received.clone(),
            )),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(true));

        run_solo(&context("prompt"), agents, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        assert!(received.lock().expect("lock").is_empty());
        assert!(!out.run_dir().join("Claude_iter1.md").exists());
        let events = collect_progress_events(rx);
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn run_solo_emits_live_cli_log_lines() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let received = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(MockProvider::ok(ProviderKind::Anthropic, "ok", received)),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_solo(&context("prompt"), agents, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|e| {
            matches!(
                e,
                ProgressEvent::AgentLog { message, .. } if message.contains("CLI live")
            )
        }));
    }

    #[tokio::test]
    async fn run_solo_panics_emit_agent_error_and_append_error_log() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), Some("solo-panic")).expect("out");
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(PanicProvider::new(ProviderKind::Anthropic, "solo panic")),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_solo(&context("prompt"), agents, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ProgressEvent::AgentError {
                    agent,
                    kind: ProviderKind::Anthropic,
                    error,
                    ..
                } if agent == "Claude" && error.contains("panicked")
            )
        }));
        assert!(events
            .iter()
            .any(|event| matches!(event, ProgressEvent::AllDone)));

        let log = std::fs::read_to_string(out.run_dir().join("_errors.log")).expect("log");
        assert!(log.contains("Claude iter1"));
        assert!(log.contains("solo panic"));
    }
}
