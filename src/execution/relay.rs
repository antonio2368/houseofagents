use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::{Provider, ProviderKind};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[allow(clippy::too_many_arguments)]
pub async fn run_relay(
    prompt: &str,
    mut providers: Vec<Box<dyn Provider>>,
    iterations: u32,
    start_iteration: u32,
    initial_last_output: Option<String>,
    use_cli_by_kind: HashMap<ProviderKind, bool>,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let has_initial_last_output = initial_last_output.is_some();
    let mut last_output = initial_last_output.unwrap_or_default();

    // Pre-compute agent kinds for building messages without borrowing providers
    let agent_kinds: Vec<ProviderKind> = providers.iter().map(|p| p.kind()).collect();
    let num_agents = providers.len();

    for offset in 0..iterations {
        let iteration = start_iteration + offset;
        for i in 0..num_agents {
            if cancel.load(Ordering::Relaxed) {
                let _ = progress_tx.send(ProgressEvent::AllDone);
                return Ok(());
            }

            let kind = agent_kinds[i];
            let _ = progress_tx.send(ProgressEvent::AgentStarted { kind, iteration });
            let _ = progress_tx.send(ProgressEvent::AgentLog {
                kind,
                iteration,
                message: "Sending request...".into(),
            });

            let message = if !has_initial_last_output && offset == 0 && i == 0 {
                prompt.to_string()
            } else {
                let prev_kind = if i == 0 {
                    agent_kinds[num_agents - 1]
                } else {
                    agent_kinds[i - 1]
                };
                let receiver_is_cli = use_cli_by_kind.get(&kind).copied().unwrap_or(false);
                if receiver_is_cli {
                    let prev_iteration = if i == 0 { iteration - 1 } else { iteration };
                    let prev_path = output.run_dir().join(format!(
                        "{}_iter{}.md",
                        prev_kind.config_key(),
                        prev_iteration
                    ));
                    format!(
                        "Read the previous agent output from file and build on it.\n\nPrevious agent: {}\nFile: {}\n\nUse that file as the source of truth and provide an improved response.",
                        prev_kind.display_name(),
                        prev_path.display()
                    )
                } else {
                    format!(
                        "Here is the output from {} (the previous agent):\n\n---\n{}\n---\n\nPlease build upon and improve this work.",
                        prev_kind.display_name(), last_output
                    )
                }
            };

            // Use select! so cancel aborts the in-flight request
            let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
            providers[i].set_live_log_sender(Some(live_tx));
            let live_progress_tx = progress_tx.clone();
            let live_forward = tokio::spawn(async move {
                while let Some(line) = live_rx.recv().await {
                    let _ = live_progress_tx.send(ProgressEvent::AgentLog {
                        kind,
                        iteration,
                        message: format!("CLI {line}"),
                    });
                }
            });
            let result = tokio::select! {
                res = providers[i].send(&message) => Some(res),
                _ = wait_for_cancel(&cancel) => {
                    let _ = progress_tx.send(ProgressEvent::AgentLog {
                        kind, iteration, message: "Cancelled".into(),
                    });
                    None
                }
            };
            providers[i].set_live_log_sender(None);
            let _ = live_forward.await;
            let Some(result) = result else {
                let _ = progress_tx.send(ProgressEvent::AllDone);
                return Ok(());
            };

            match result {
                Ok(resp) => {
                    for log in &resp.debug_logs {
                        let _ = progress_tx.send(ProgressEvent::AgentLog {
                            kind,
                            iteration,
                            message: format!("CLI {log}"),
                        });
                    }
                    let preview = resp.content.lines().take(3).collect::<Vec<_>>().join(" | ");
                    let _ = progress_tx.send(ProgressEvent::AgentLog {
                        kind,
                        iteration,
                        message: format!(
                            "Response received ({} chars): {}",
                            resp.content.len(),
                            truncate_chars(&preview, 80)
                        ),
                    });
                    if let Err(e) = output.write_agent_output(kind, iteration, &resp.content) {
                        let err_str =
                            format!("Failed to write output file for {kind} iter{iteration}: {e}");
                        if let Err(log_err) = output.append_error(&err_str) {
                            let _ = progress_tx.send(ProgressEvent::AgentLog {
                                kind,
                                iteration,
                                message: format!("Failed to append error log: {log_err}"),
                            });
                        }
                        let _ = progress_tx.send(ProgressEvent::AgentError {
                            kind,
                            iteration,
                            error: err_str.clone(),
                            details: Some(err_str),
                        });
                        let _ = progress_tx.send(ProgressEvent::AllDone);
                        return Ok(());
                    }
                    let _ = progress_tx.send(ProgressEvent::AgentFinished { kind, iteration });
                    last_output = resp.content;
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if let Err(log_err) =
                        output.append_error(&format!("{kind} iter{iteration}: {err_str}"))
                    {
                        let _ = progress_tx.send(ProgressEvent::AgentLog {
                            kind,
                            iteration,
                            message: format!("Failed to append error log: {log_err}"),
                        });
                    }
                    let _ = progress_tx.send(ProgressEvent::AgentError {
                        kind,
                        iteration,
                        error: err_str.clone(),
                        details: Some(err_str),
                    });
                    // Stop the entire relay on failure
                    let _ = progress_tx.send(ProgressEvent::AllDone);
                    return Ok(());
                }
            }
        }

        let _ = progress_tx.send(ProgressEvent::IterationComplete { iteration });
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::CompletionResponse;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    struct MockProvider {
        kind: ProviderKind,
        responses: VecDeque<Result<CompletionResponse, AppError>>,
        received: Arc<Mutex<Vec<String>>>,
        live_tx: Option<mpsc::UnboundedSender<String>>,
    }

    impl MockProvider {
        fn with_responses(
            kind: ProviderKind,
            responses: Vec<Result<CompletionResponse, AppError>>,
            received: Arc<Mutex<Vec<String>>>,
        ) -> Self {
            Self {
                kind,
                responses: VecDeque::from(responses),
                received,
                live_tx: None,
            }
        }
    }

    #[async_trait]
    impl Provider for MockProvider {
        fn kind(&self) -> ProviderKind {
            self.kind
        }

        fn set_live_log_sender(&mut self, tx: Option<mpsc::UnboundedSender<String>>) {
            self.live_tx = tx;
        }

        async fn send(&mut self, message: &str) -> Result<CompletionResponse, AppError> {
            self.received
                .lock()
                .expect("lock")
                .push(message.to_string());
            if let Some(tx) = self.live_tx.as_ref() {
                let _ = tx.send("relay-live".to_string());
            }
            self.responses.pop_front().unwrap_or_else(|| {
                Ok(CompletionResponse {
                    content: "default".to_string(),
                    debug_logs: Vec::new(),
                })
            })
        }
    }

    fn ok(content: &str) -> Result<CompletionResponse, AppError> {
        Ok(CompletionResponse {
            content: content.to_string(),
            debug_logs: vec!["dbg".to_string()],
        })
    }

    fn collect(mut rx: mpsc::UnboundedReceiver<ProgressEvent>) -> Vec<ProgressEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    #[tokio::test]
    async fn run_relay_one_iteration_passes_previous_output_to_next_agent() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), Some("relay")).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok("first output")],
                recv_a.clone(),
            )),
            Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok("second output")],
                recv_b.clone(),
            )),
        ];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            "initial prompt",
            providers,
            1,
            1,
            None,
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let a_msgs = recv_a.lock().expect("lock");
        let b_msgs = recv_b.lock().expect("lock");
        assert_eq!(a_msgs[0], "initial prompt");
        assert!(b_msgs[0].contains("first output"));
        assert!(out.run_dir().join("anthropic_iter1.md").exists());
        assert!(out.run_dir().join("openai_iter1.md").exists());
        let events = collect(rx);
        assert!(events
            .iter()
            .any(|e| matches!(e, ProgressEvent::IterationComplete { iteration: 1 })));
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn run_relay_uses_file_instruction_for_cli_receivers() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok("a out")],
                recv_a,
            )),
            Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok("b out")],
                recv_b.clone(),
            )),
        ];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut use_cli = HashMap::new();
        use_cli.insert(ProviderKind::OpenAI, true);

        run_relay("p", providers, 1, 1, None, use_cli, &out, tx, cancel)
            .await
            .expect("run");

        let b_msgs = recv_b.lock().expect("lock");
        assert!(b_msgs[0].contains("Read the previous agent output from file"));
        assert!(b_msgs[0].contains("anthropic_iter1.md"));
    }

    #[tokio::test]
    async fn run_relay_uses_initial_last_output_for_resumed_run() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![Box::new(MockProvider::with_responses(
            ProviderKind::Anthropic,
            vec![ok("new")],
            recv.clone(),
        ))];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            "ignored",
            providers,
            1,
            2,
            Some("resume seed".to_string()),
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let msg = recv.lock().expect("lock")[0].clone();
        assert!(msg.contains("resume seed"));
        assert!(out.run_dir().join("anthropic_iter2.md").exists());
    }

    #[tokio::test]
    async fn run_relay_starting_later_without_seed_uses_prompt_for_first_agent() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![Box::new(MockProvider::with_responses(
            ProviderKind::Anthropic,
            vec![ok("new")],
            recv.clone(),
        ))];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            "initial prompt",
            providers,
            1,
            3,
            None,
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let msg = recv.lock().expect("lock")[0].clone();
        assert_eq!(msg, "initial prompt");
        assert!(out.run_dir().join("anthropic_iter3.md").exists());
    }

    #[tokio::test]
    async fn run_relay_error_stops_and_writes_error_log() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![Box::new(MockProvider::with_responses(
            ProviderKind::Anthropic,
            vec![Err(AppError::Provider {
                provider: "mock".to_string(),
                message: "bad".to_string(),
            })],
            recv,
        ))];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay("p", providers, 2, 1, None, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        let log = std::fs::read_to_string(out.run_dir().join("_errors.log")).expect("log");
        assert!(log.contains("iter1"));
        let events = collect(rx);
        assert!(events
            .iter()
            .any(|e| matches!(e, ProgressEvent::AgentError { .. })));
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn run_relay_cancel_before_iteration_sends_all_done() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![Box::new(MockProvider::with_responses(
            ProviderKind::Anthropic,
            vec![ok("x")],
            recv.clone(),
        ))];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(true));

        run_relay("p", providers, 1, 1, None, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        assert!(recv.lock().expect("lock").is_empty());
        let events = collect(rx);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ProgressEvent::AllDone));
    }

    #[tokio::test]
    async fn run_relay_write_failure_emits_agent_error_and_stops() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        std::fs::create_dir_all(out.run_dir().join("anthropic_iter1.md")).expect("mkdir");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![Box::new(MockProvider::with_responses(
            ProviderKind::Anthropic,
            vec![ok("x")],
            recv,
        ))];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay("p", providers, 2, 1, None, HashMap::new(), &out, tx, cancel)
            .await
            .expect("run");

        let events = collect(rx);
        assert!(events.iter().any(|e| {
            matches!(
                e,
                ProgressEvent::AgentError { kind: ProviderKind::Anthropic, error, .. }
                if error.contains("Failed to write output file")
            )
        }));
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
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
}
