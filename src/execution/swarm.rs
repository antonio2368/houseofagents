use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::Provider;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

type SwarmWorkerResult = (usize, String, Box<dyn Provider>, Option<(String, String)>);

#[allow(clippy::too_many_arguments)]
pub async fn run_swarm(
    prompt: &str,
    mut agents: Vec<(String, Box<dyn Provider>)>,
    iterations: u32,
    start_iteration: u32,
    initial_last_round_outputs: HashMap<String, String>,
    use_cli_by_agent: HashMap<String, bool>,
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
        let messages: Vec<String> = agents
            .iter()
            .map(|(name, _p)| {
                if iteration == 1 {
                    prompt.to_string()
                } else if use_cli_by_agent.get(name).copied().unwrap_or(false) {
                    build_swarm_file_message(
                        &last_round_outputs,
                        output.run_dir(),
                        iteration - 1,
                    )
                } else {
                    build_swarm_message(&last_round_outputs)
                }
            })
            .collect();

        // Take ownership of agents for parallel execution
        let taken: Vec<(String, Box<dyn Provider>)> = std::mem::take(&mut agents);
        let mut spawn_handles: Vec<JoinHandle<SwarmWorkerResult>> = Vec::new();

        for (i, ((name, mut provider), message)) in
            taken.into_iter().zip(messages.into_iter()).enumerate()
        {
            let kind = provider.kind();
            let agent_name = name.clone();
            let _ = progress_tx.send(ProgressEvent::AgentStarted {
                agent: agent_name.clone(),
                kind,
                iteration,
            });
            let _ = progress_tx.send(ProgressEvent::AgentLog {
                agent: agent_name.clone(),
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
                    return (i, agent_name, provider, None);
                }

                let kind = provider.kind();
                let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
                provider.set_live_log_sender(Some(live_tx));
                let live_progress_tx = tx.clone();
                let live_agent = agent_name.clone();
                let live_forward = tokio::spawn(async move {
                    while let Some(line) = live_rx.recv().await {
                        let _ = live_progress_tx.send(ProgressEvent::AgentLog {
                            agent: live_agent.clone(),
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
                            agent: agent_name.clone(), kind, iteration: iter, message: "Cancelled".into(),
                        });
                        None
                    }
                };
                provider.set_live_log_sender(None);
                let _ = live_forward.await;
                let Some(result) = result else {
                    return (i, agent_name, provider, None);
                };

                match result {
                    Ok(resp) => {
                        for log in &resp.debug_logs {
                            let _ = tx.send(ProgressEvent::AgentLog {
                                agent: agent_name.clone(),
                                kind,
                                iteration: iter,
                                message: format!("CLI {log}"),
                            });
                        }
                        let preview = resp.content.lines().take(3).collect::<Vec<_>>().join(" | ");
                        let _ = tx.send(ProgressEvent::AgentLog {
                            agent: agent_name.clone(),
                            kind,
                            iteration: iter,
                            message: format!(
                                "Response received ({} chars): {}",
                                resp.content.len(),
                                truncate_chars(&preview, 80)
                            ),
                        });
                        let sanitized = OutputManager::sanitize_session_name(&agent_name);
                        let filename = format!("{}_iter{}.md", sanitized, iter);
                        let path = run_dir.join(&filename);
                        if let Err(e) = tokio::fs::write(&path, &resp.content).await {
                            let err =
                                format!("Failed to write output file {}: {e}", path.display());
                            let _ = tx.send(ProgressEvent::AgentError {
                                agent: agent_name.clone(),
                                kind,
                                iteration: iter,
                                error: err.clone(),
                                details: Some(err),
                            });
                            return (i, agent_name, provider, None);
                        }
                        let _ = tx.send(ProgressEvent::AgentFinished {
                            agent: agent_name.clone(),
                            kind,
                            iteration: iter,
                        });
                        let result_name = agent_name.clone();
                        (i, agent_name, provider, Some((result_name, resp.content)))
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        let _ = tx.send(ProgressEvent::AgentError {
                            agent: agent_name.clone(),
                            kind,
                            iteration: iter,
                            error: err_str.clone(),
                            details: Some(err_str),
                        });
                        (i, agent_name, provider, None)
                    }
                }
            }));
        }

        // Collect results and restore agents
        let mut round_outputs: HashMap<String, String> = HashMap::new();
        let mut restored: Vec<(usize, String, Box<dyn Provider>)> = Vec::new();
        for handle in spawn_handles {
            if let Ok((idx, name, provider, result)) = handle.await {
                if let Some((_agent_name, content)) = result {
                    round_outputs.insert(name.clone(), content);
                }
                restored.push((idx, name, provider));
            }
        }

        restored.sort_by_key(|(idx, _, _)| *idx);
        agents = restored.into_iter().map(|(_, n, p)| (n, p)).collect();

        last_round_outputs = round_outputs;

        let _ = progress_tx.send(ProgressEvent::IterationComplete { iteration });
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

fn build_swarm_message(outputs: &HashMap<String, String>) -> String {
    let mut msg = String::from("Here are the outputs from all agents in the previous round:\n\n");
    let mut keys: Vec<&String> = outputs.keys().collect();
    keys.sort();
    for name in keys {
        if let Some(output) = outputs.get(name) {
            msg.push_str(&format!(
                "=== {}'s output ===\n{}\n\n",
                name, output
            ));
        }
    }
    msg.push_str("Review all perspectives and provide your updated analysis.");
    msg
}

fn build_swarm_file_message(
    outputs: &HashMap<String, String>,
    run_dir: &std::path::Path,
    prev_iteration: u32,
) -> String {
    let mut msg = String::from(
        "Read the previous round agent outputs from files and synthesize them into an updated analysis.\n\nFiles:\n",
    );
    let mut keys: Vec<&String> = outputs.keys().collect();
    keys.sort();
    for name in keys {
        let sanitized = OutputManager::sanitize_session_name(name);
        let path = run_dir.join(format!("{}_iter{}.md", sanitized, prev_iteration));
        msg.push_str(&format!("- {}: {}\n", name, path.display()));
    }
    msg.push_str("\nUse the file contents as the source of truth.");
    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::test_utils::{collect_progress_events, ok_response, MockProvider};
    use crate::provider::ProviderKind;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn named(name: &str, _kind: ProviderKind, provider: Box<dyn crate::provider::Provider>) -> (String, Box<dyn crate::provider::Provider>) {
        (name.to_string(), provider)
    }

    #[test]
    fn build_swarm_message_includes_all_available_outputs() {
        let mut outputs = HashMap::new();
        outputs.insert("Claude".to_string(), "a".to_string());
        outputs.insert("Codex".to_string(), "b".to_string());
        let msg = build_swarm_message(&outputs);
        assert!(msg.contains("Claude"));
        assert!(msg.contains("Codex"));
        assert!(msg.contains("a"));
        assert!(msg.contains("b"));
    }

    #[test]
    fn build_swarm_file_message_includes_expected_paths() {
        let dir = tempdir().expect("tempdir");
        let mut outputs = HashMap::new();
        outputs.insert("Claude".to_string(), "a".to_string());
        let msg = build_swarm_file_message(&outputs, dir.path(), 3);
        assert!(msg.contains("Claude_iter3.md"));
        assert!(msg.contains("source of truth"));
    }

    #[tokio::test]
    async fn run_swarm_single_iteration_writes_outputs_and_events() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), Some("swarm")).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![
            named("Claude", ProviderKind::Anthropic, Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok_response("a1")],
                recv_a,
            ))),
            named("Codex", ProviderKind::OpenAI, Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok_response("o1")],
                recv_b,
            ))),
        ];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_swarm(
            "prompt",
            agents,
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

        assert!(out.run_dir().join("Claude_iter1.md").exists());
        assert!(out.run_dir().join("Codex_iter1.md").exists());
        let events = collect_progress_events(rx);
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
        let agents = vec![
            named("Claude", ProviderKind::Anthropic, Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok_response("a1"), ok_response("a2")],
                recv_a.clone(),
            ))),
            named("Codex", ProviderKind::OpenAI, Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok_response("o1"), ok_response("o2")],
                recv_b.clone(),
            ))),
        ];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_swarm(
            "prompt",
            agents,
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
        let agents = vec![
            named("Claude", ProviderKind::Anthropic, Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok_response("a1"), ok_response("a2")],
                recv_a.clone(),
            ))),
            named("Codex", ProviderKind::OpenAI, Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok_response("o1"), ok_response("o2")],
                recv_b.clone(),
            ))),
        ];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut use_cli = HashMap::new();
        use_cli.insert("Claude".to_string(), true);
        use_cli.insert("Codex".to_string(), true);

        run_swarm(
            "prompt",
            agents,
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
        let agents = vec![
            named("Claude", ProviderKind::Anthropic, Box::new(MockProvider::err(ProviderKind::Anthropic, "bad", recv_a))),
            named("Codex", ProviderKind::OpenAI, Box::new(MockProvider::with_responses(
                ProviderKind::OpenAI,
                vec![ok_response("ok")],
                recv_b,
            ))),
        ];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_swarm(
            "prompt",
            agents,
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

        let events = collect_progress_events(rx);
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
        let agents = vec![named("Claude", ProviderKind::Anthropic, Box::new(MockProvider::with_responses(
            ProviderKind::Anthropic,
            vec![ok_response("x")],
            recv.clone(),
        )))];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(true));

        run_swarm(
            "prompt",
            agents,
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
        let events = collect_progress_events(rx);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ProgressEvent::AllDone));
    }

    #[tokio::test]
    async fn run_swarm_write_failure_emits_agent_error() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        std::fs::create_dir_all(out.run_dir().join("Claude_iter1.md")).expect("mkdir");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named("Claude", ProviderKind::Anthropic, Box::new(MockProvider::with_responses(
            ProviderKind::Anthropic,
            vec![ok_response("x")],
            recv,
        )))];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_swarm(
            "prompt",
            agents,
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

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|e| {
            matches!(
                e,
                ProgressEvent::AgentError { error, .. }
                if error.contains("Failed to write output file")
            )
        }));
    }
}
