use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::{Provider, ProviderKind};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

type SwarmWorkerResult = (usize, Box<dyn Provider>, Option<(ProviderKind, String)>);

#[allow(clippy::too_many_arguments)]
pub async fn run_swarm(
    prompt: &str,
    mut providers: Vec<Box<dyn Provider>>,
    iterations: u32,
    start_iteration: u32,
    initial_last_round_outputs: HashMap<ProviderKind, String>,
    use_cli_by_kind: HashMap<ProviderKind, bool>,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let mut last_round_outputs = initial_last_round_outputs;

    for offset in 0..iterations {
        let iteration = start_iteration + offset;
        if cancel.load(Ordering::Relaxed) {
            let _ = progress_tx.send(ProgressEvent::AllDone);
            return Ok(());
        }

        // Build messages for this round
        let messages: Vec<String> = providers
            .iter()
            .map(|p| {
                if iteration == 1 {
                    prompt.to_string()
                } else {
                    let kind = p.kind();
                    if use_cli_by_kind.get(&kind).copied().unwrap_or(false) {
                        build_swarm_file_message(
                            &last_round_outputs,
                            output.run_dir(),
                            iteration - 1,
                        )
                    } else {
                        build_swarm_message(kind, &last_round_outputs)
                    }
                }
            })
            .collect();

        // Take ownership of providers for parallel execution
        let taken: Vec<Box<dyn Provider>> = std::mem::take(&mut providers);
        let mut spawn_handles: Vec<JoinHandle<SwarmWorkerResult>> = Vec::new();

        for (i, (mut provider, message)) in taken.into_iter().zip(messages.into_iter()).enumerate()
        {
            let kind = provider.kind();
            let _ = progress_tx.send(ProgressEvent::AgentStarted { kind, iteration });
            let _ = progress_tx.send(ProgressEvent::AgentLog {
                kind,
                iteration,
                message: "Sending request...".into(),
            });

            let tx = progress_tx.clone();
            let cancel_flag = cancel.clone();
            let run_dir = output.run_dir().clone();
            let iter = iteration;

            spawn_handles.push(tokio::spawn(async move {
                if cancel_flag.load(Ordering::Relaxed) {
                    return (i, provider, None);
                }

                let kind = provider.kind();
                let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
                provider.set_live_log_sender(Some(live_tx));
                let live_progress_tx = tx.clone();
                let live_forward = tokio::spawn(async move {
                    while let Some(line) = live_rx.recv().await {
                        let _ = live_progress_tx.send(ProgressEvent::AgentLog {
                            kind,
                            iteration: iter,
                            message: format!("CLI {line}"),
                        });
                    }
                });

                let result = tokio::select! {
                    res = provider.send(&message) => Some(res),
                    _ = wait_for_cancel(&cancel_flag) => {
                        let _ = tx.send(ProgressEvent::AgentLog {
                            kind, iteration: iter, message: "Cancelled".into(),
                        });
                        None
                    }
                };
                provider.set_live_log_sender(None);
                let _ = live_forward.await;
                let Some(result) = result else {
                    return (i, provider, None);
                };

                match result {
                    Ok(resp) => {
                        for log in &resp.debug_logs {
                            let _ = tx.send(ProgressEvent::AgentLog {
                                kind,
                                iteration: iter,
                                message: format!("CLI {log}"),
                            });
                        }
                        let preview = resp.content.lines().take(3).collect::<Vec<_>>().join(" | ");
                        let _ = tx.send(ProgressEvent::AgentLog {
                            kind,
                            iteration: iter,
                            message: format!(
                                "Response received ({} chars): {}",
                                resp.content.len(),
                                truncate_chars(&preview, 80)
                            ),
                        });
                        let filename = format!("{}_iter{}.md", kind.config_key(), iter);
                        let path = run_dir.join(&filename);
                        if let Err(e) = tokio::fs::write(&path, &resp.content).await {
                            let err =
                                format!("Failed to write output file {}: {e}", path.display());
                            let _ = tx.send(ProgressEvent::AgentError {
                                kind,
                                iteration: iter,
                                error: err.clone(),
                                details: Some(err),
                            });
                            return (i, provider, None);
                        }
                        let _ = tx.send(ProgressEvent::AgentFinished {
                            kind,
                            iteration: iter,
                        });
                        (i, provider, Some((kind, resp.content)))
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        let _ = tx.send(ProgressEvent::AgentError {
                            kind,
                            iteration: iter,
                            error: err_str.clone(),
                            details: Some(err_str),
                        });
                        (i, provider, None)
                    }
                }
            }));
        }

        // Collect results and restore providers
        let mut round_outputs: HashMap<ProviderKind, String> = HashMap::new();
        let mut restored: Vec<(usize, Box<dyn Provider>)> = Vec::new();
        for handle in spawn_handles {
            if let Ok((idx, provider, result)) = handle.await {
                if let Some((kind, content)) = result {
                    round_outputs.insert(kind, content);
                }
                restored.push((idx, provider));
            }
        }

        restored.sort_by_key(|(idx, _)| *idx);
        providers = restored.into_iter().map(|(_, p)| p).collect();

        last_round_outputs = round_outputs;

        let _ = progress_tx.send(ProgressEvent::IterationComplete { iteration });
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

fn build_swarm_message(_current: ProviderKind, outputs: &HashMap<ProviderKind, String>) -> String {
    let mut msg = String::from("Here are the outputs from all agents in the previous round:\n\n");
    for kind in ProviderKind::all() {
        if let Some(output) = outputs.get(kind) {
            msg.push_str(&format!(
                "=== {}'s output ===\n{}\n\n",
                kind.display_name(),
                output
            ));
        }
    }
    msg.push_str("Review all perspectives and provide your updated analysis.");
    msg
}

fn build_swarm_file_message(
    outputs: &HashMap<ProviderKind, String>,
    run_dir: &std::path::Path,
    prev_iteration: u32,
) -> String {
    let mut msg = String::from(
        "Read the previous round agent outputs from files and synthesize them into an updated analysis.\n\nFiles:\n",
    );
    for kind in ProviderKind::all() {
        if outputs.contains_key(kind) {
            let path = run_dir.join(format!("{}_iter{}.md", kind.config_key(), prev_iteration));
            msg.push_str(&format!("- {}: {}\n", kind.display_name(), path.display()));
        }
    }
    msg.push_str("\nUse the file contents as the source of truth.");
    msg
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
                let _ = tx.send("swarm-live".to_string());
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

    #[test]
    fn build_swarm_message_includes_all_available_outputs() {
        let mut outputs = HashMap::new();
        outputs.insert(ProviderKind::Anthropic, "a".to_string());
        outputs.insert(ProviderKind::OpenAI, "b".to_string());
        let msg = build_swarm_message(ProviderKind::Gemini, &outputs);
        assert!(msg.contains("Claude"));
        assert!(msg.contains("Codex"));
        assert!(msg.contains("a"));
        assert!(msg.contains("b"));
    }

    #[test]
    fn build_swarm_file_message_includes_expected_paths() {
        let dir = tempdir().expect("tempdir");
        let mut outputs = HashMap::new();
        outputs.insert(ProviderKind::Anthropic, "a".to_string());
        let msg = build_swarm_file_message(&outputs, dir.path(), 3);
        assert!(msg.contains("anthropic_iter3.md"));
        assert!(msg.contains("source of truth"));
    }

    #[tokio::test]
    async fn run_swarm_single_iteration_writes_outputs_and_events() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), Some("swarm")).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok("a1")],
                recv_a,
            )),
            Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok("o1")],
                recv_b,
            )),
        ];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_swarm(
            "prompt",
            providers,
            1,
            1,
            HashMap::new(),
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        assert!(out.run_dir().join("anthropic_iter1.md").exists());
        assert!(out.run_dir().join("openai_iter1.md").exists());
        let events = collect(rx);
        assert!(events
            .iter()
            .any(|e| matches!(e, ProgressEvent::IterationComplete { iteration: 1 })));
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn run_swarm_second_round_receives_prior_outputs() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok("a1"), ok("a2")],
                recv_a.clone(),
            )),
            Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok("o1"), ok("o2")],
                recv_b.clone(),
            )),
        ];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_swarm(
            "prompt",
            providers,
            2,
            1,
            HashMap::new(),
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let a_msgs = recv_a.lock().expect("lock");
        let b_msgs = recv_b.lock().expect("lock");
        assert_eq!(a_msgs[0], "prompt");
        assert_eq!(b_msgs[0], "prompt");
        assert!(a_msgs[1].contains("previous round"));
        assert!(b_msgs[1].contains("previous round"));
    }

    #[tokio::test]
    async fn run_swarm_cli_mode_uses_file_messages_in_followup_rounds() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok("a1"), ok("a2")],
                recv_a.clone(),
            )),
            Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok("o1"), ok("o2")],
                recv_b.clone(),
            )),
        ];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut use_cli = HashMap::new();
        use_cli.insert(ProviderKind::Anthropic, true);
        use_cli.insert(ProviderKind::OpenAI, true);

        run_swarm(
            "prompt",
            providers,
            2,
            1,
            HashMap::new(),
            use_cli,
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        assert!(recv_a.lock().expect("lock")[1].contains("Files:"));
        assert!(recv_b.lock().expect("lock")[1].contains("iter1.md"));
    }

    #[tokio::test]
    async fn run_swarm_handles_agent_error_without_failing_run() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let providers: Vec<Box<dyn Provider>> = vec![
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![Err(AppError::Provider {
                    provider: "mock".to_string(),
                    message: "bad".to_string(),
                })],
                recv_a,
            )),
            Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok("ok")],
                recv_b,
            )),
        ];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_swarm(
            "prompt",
            providers,
            1,
            1,
            HashMap::new(),
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let events = collect(rx);
        assert!(events
            .iter()
            .any(|e| matches!(e, ProgressEvent::AgentError { .. })));
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn run_swarm_cancel_before_start_sends_all_done() {
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

        run_swarm(
            "prompt",
            providers,
            1,
            1,
            HashMap::new(),
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        assert!(recv.lock().expect("lock").is_empty());
        let events = collect(rx);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ProgressEvent::AllDone));
    }

    #[tokio::test]
    async fn run_swarm_write_failure_emits_agent_error() {
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

        run_swarm(
            "prompt",
            providers,
            1,
            1,
            HashMap::new(),
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
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
