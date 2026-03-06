use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::Provider;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub async fn run_solo(
    prompt: &str,
    agents: Vec<(String, Box<dyn Provider>)>,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let mut handles = Vec::new();

    for (name, provider) in agents {
        let prompt = prompt.to_string();
        let tx = progress_tx.clone();
        let cancel = cancel.clone();
        let run_dir = output.run_dir().clone();

        handles.push(tokio::spawn(async move {
            solo_agent(name, provider, &prompt, &run_dir, tx, cancel).await
        }));
    }

    for handle in handles {
        let _ = handle.await;
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
    use crate::execution::test_utils::{collect_progress_events, MockProvider};
    use crate::provider::ProviderKind;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn named(
        name: &str,
        _kind: ProviderKind,
        provider: Box<dyn Provider>,
    ) -> (String, Box<dyn Provider>) {
        (name.to_string(), provider)
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
                "Codex",
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

        run_solo("prompt", agents, &out, tx, cancel)
            .await
            .expect("run");

        assert_eq!(received.lock().expect("lock").len(), 2);
        assert!(out.run_dir().join("Claude_iter1.md").exists());
        assert!(out.run_dir().join("Codex_iter1.md").exists());

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

        run_solo("prompt", agents, &out, tx, cancel)
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

        run_solo("prompt", agents, &out, tx, cancel)
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

        run_solo("prompt", agents, &out, tx, cancel)
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
            Box::new(MockProvider::ok(
                ProviderKind::Anthropic,
                "ok",
                received,
            )),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_solo("prompt", agents, &out, tx, cancel)
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
}
