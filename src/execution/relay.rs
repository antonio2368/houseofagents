use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent, PromptRuntimeContext};
use crate::output::OutputManager;
use crate::provider::Provider;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[allow(clippy::too_many_arguments)]
pub async fn run_relay(
    prompt_context: &PromptRuntimeContext,
    mut agents: Vec<(String, Box<dyn Provider>)>,
    iterations: u32,
    start_iteration: u32,
    initial_last_output: Option<String>,
    forward_prompt: bool,
    use_cli_by_agent: HashMap<String, bool>,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let has_initial_last_output = initial_last_output.is_some();
    let mut last_output = initial_last_output.unwrap_or_default();

    // Pre-compute agent names/kinds for building messages without borrowing agents
    let agent_info: Vec<(String, crate::provider::ProviderKind)> = agents
        .iter()
        .map(|(name, p)| (name.clone(), p.kind()))
        .collect();
    let num_agents = agents.len();

    for offset in 0..iterations {
        let iteration = start_iteration + offset;
        for i in 0..num_agents {
            if cancel.load(Ordering::Relaxed) {
                let _ = progress_tx.send(ProgressEvent::AllDone);
                return Ok(());
            }

            let (ref name, ref kind) = agent_info[i];
            let _ = progress_tx.send(ProgressEvent::AgentStarted {
                agent: name.clone(),
                kind: *kind,
                iteration,
            });
            let _ = progress_tx.send(ProgressEvent::AgentLog {
                agent: name.clone(),
                kind: *kind,
                iteration,
                message: "Sending request...".into(),
            });

            let receiver_is_cli = use_cli_by_agent.get(name).copied().unwrap_or(false);
            let message = if !has_initial_last_output && offset == 0 && i == 0 {
                prompt_context.initial_prompt_for_agent(receiver_is_cli)
            } else {
                let (ref prev_name, ref _prev_kind) = if i == 0 {
                    &agent_info[num_agents - 1]
                } else {
                    &agent_info[i - 1]
                };
                let task_prefix = if forward_prompt {
                    format!("Original task: {}\n\n", prompt_context.raw_prompt())
                } else {
                    String::new()
                };
                let base_message = if receiver_is_cli {
                    let prev_iteration = if i == 0 { iteration - 1 } else { iteration };
                    let prev_file_key = OutputManager::sanitize_session_name(prev_name);
                    let prev_path = output
                        .run_dir()
                        .join(format!("{}_iter{}.md", prev_file_key, prev_iteration));
                    format!(
                        "{}Read the previous agent output from file and build on it.\n\nPrevious agent: {}\nFile: {}\n\nUse that file as the source of truth and provide an improved response.",
                        task_prefix,
                        prev_name,
                        prev_path.display()
                    )
                } else {
                    format!(
                        "{}Here is the output from {} (the previous agent):\n\n---\n{}\n---\n\nPlease build upon and improve this work.",
                        task_prefix, prev_name, last_output
                    )
                };
                prompt_context.augment_prompt_for_agent(&base_message, receiver_is_cli)
            };

            // Use select! so cancel aborts the in-flight request
            let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
            agents[i].1.set_live_log_sender(Some(live_tx));
            let live_progress_tx = progress_tx.clone();
            let live_name = name.clone();
            let live_kind = *kind;
            let live_forward = tokio::spawn(async move {
                while let Some(line) = live_rx.recv().await {
                    let _ = live_progress_tx.send(ProgressEvent::AgentLog {
                        agent: live_name.clone(),
                        kind: live_kind,
                        iteration,
                        message: format!("CLI {line}"),
                    });
                }
            });
            let result = tokio::select! {
                res = agents[i].1.send(&message) => Some(res),
                _ = wait_for_cancel(&cancel) => {
                    let _ = progress_tx.send(ProgressEvent::AgentLog {
                        agent: name.clone(), kind: *kind, iteration, message: "Cancelled".into(),
                    });
                    None
                }
            };
            agents[i].1.set_live_log_sender(None);
            let _ = live_forward.await;
            let Some(result) = result else {
                let _ = progress_tx.send(ProgressEvent::AllDone);
                return Ok(());
            };

            match result {
                Ok(resp) => {
                    for log in &resp.debug_logs {
                        let _ = progress_tx.send(ProgressEvent::AgentLog {
                            agent: name.clone(),
                            kind: *kind,
                            iteration,
                            message: format!("CLI {log}"),
                        });
                    }
                    let preview = resp.content.lines().take(3).collect::<Vec<_>>().join(" | ");
                    let _ = progress_tx.send(ProgressEvent::AgentLog {
                        agent: name.clone(),
                        kind: *kind,
                        iteration,
                        message: format!(
                            "Response received ({} chars): {}",
                            resp.content.len(),
                            truncate_chars(&preview, 80)
                        ),
                    });
                    if let Err(e) = output.write_agent_output(name, iteration, &resp.content) {
                        let err_str =
                            format!("Failed to write output file for {name} iter{iteration}: {e}");
                        if let Err(log_err) = output.append_error(&err_str) {
                            let _ = progress_tx.send(ProgressEvent::AgentLog {
                                agent: name.clone(),
                                kind: *kind,
                                iteration,
                                message: format!("Failed to append error log: {log_err}"),
                            });
                        }
                        let _ = progress_tx.send(ProgressEvent::AgentError {
                            agent: name.clone(),
                            kind: *kind,
                            iteration,
                            error: err_str.clone(),
                            details: Some(err_str),
                        });
                        let _ = progress_tx.send(ProgressEvent::AllDone);
                        return Ok(());
                    }
                    let _ = progress_tx.send(ProgressEvent::AgentFinished {
                        agent: name.clone(),
                        kind: *kind,
                        iteration,
                    });
                    last_output = resp.content;
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if let Err(log_err) =
                        output.append_error(&format!("{name} iter{iteration}: {err_str}"))
                    {
                        let _ = progress_tx.send(ProgressEvent::AgentLog {
                            agent: name.clone(),
                            kind: *kind,
                            iteration,
                            message: format!("Failed to append error log: {log_err}"),
                        });
                    }
                    let _ = progress_tx.send(ProgressEvent::AgentError {
                        agent: name.clone(),
                        kind: *kind,
                        iteration,
                        error: err_str.clone(),
                        details: Some(err_str),
                    });
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
    use crate::execution::test_utils::{collect_progress_events, ok_response, MockProvider};
    use crate::provider::ProviderKind;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn named(
        name: &str,
        _kind: ProviderKind,
        provider: Box<dyn crate::provider::Provider>,
    ) -> (String, Box<dyn crate::provider::Provider>) {
        (name.to_string(), provider)
    }

    fn context(prompt: &str) -> PromptRuntimeContext {
        PromptRuntimeContext::new(prompt, false)
    }

    #[tokio::test]
    async fn run_relay_one_iteration_passes_previous_output_to_next_agent() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), Some("relay")).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![
            named(
                "Claude",
                ProviderKind::Anthropic,
                Box::new(MockProvider::with_responses(
                    ProviderKind::Anthropic,
                    vec![ok_response("first output")],
                    recv_a.clone(),
                )),
            ),
            named(
                "OpenAI",
                ProviderKind::OpenAI,
                Box::new(MockProvider::with_responses(
                    ProviderKind::OpenAI,
                    vec![ok_response("second output")],
                    recv_b.clone(),
                )),
            ),
        ];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            &context("initial prompt"),
            agents,
            1,
            1,
            None,
            false,
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
        assert!(out.run_dir().join("Claude_iter1.md").exists());
        assert!(out.run_dir().join("OpenAI_iter1.md").exists());
        let events = collect_progress_events(rx);
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
        let agents = vec![
            named(
                "Claude",
                ProviderKind::Anthropic,
                Box::new(MockProvider::with_responses(
                    ProviderKind::Anthropic,
                    vec![ok_response("a out")],
                    recv_a,
                )),
            ),
            named(
                "OpenAI",
                ProviderKind::OpenAI,
                Box::new(MockProvider::with_responses(
                    ProviderKind::OpenAI,
                    vec![ok_response("b out")],
                    recv_b.clone(),
                )),
            ),
        ];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut use_cli = HashMap::new();
        use_cli.insert("OpenAI".to_string(), true);

        run_relay(
            &context("p"),
            agents,
            1,
            1,
            None,
            false,
            use_cli,
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let b_msgs = recv_b.lock().expect("lock");
        assert!(b_msgs[0].contains("Read the previous agent output from file"));
        assert!(b_msgs[0].contains("Claude_iter1.md"));
    }

    #[tokio::test]
    async fn run_relay_uses_initial_last_output_for_resumed_run() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok_response("new")],
                recv.clone(),
            )),
        )];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            &context("ignored"),
            agents,
            1,
            2,
            Some("resume seed".to_string()),
            false,
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let msg = recv.lock().expect("lock")[0].clone();
        assert!(msg.contains("resume seed"));
        assert!(out.run_dir().join("Claude_iter2.md").exists());
    }

    #[tokio::test]
    async fn run_relay_starting_later_without_seed_uses_prompt_for_first_agent() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok_response("new")],
                recv.clone(),
            )),
        )];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            &context("initial prompt"),
            agents,
            1,
            3,
            None,
            false,
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let msg = recv.lock().expect("lock")[0].clone();
        assert_eq!(msg, "initial prompt");
        assert!(out.run_dir().join("Claude_iter3.md").exists());
    }

    #[tokio::test]
    async fn run_relay_error_stops_and_writes_error_log() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(MockProvider::err(ProviderKind::Anthropic, "bad", recv)),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            &context("p"),
            agents,
            2,
            1,
            None,
            false,
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let log = std::fs::read_to_string(out.run_dir().join("_errors.log")).expect("log");
        assert!(log.contains("iter1"));
        let events = collect_progress_events(rx);
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
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok_response("x")],
                recv.clone(),
            )),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(true));

        run_relay(
            &context("p"),
            agents,
            1,
            1,
            None,
            false,
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
    async fn run_relay_write_failure_emits_agent_error_and_stops() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        std::fs::create_dir_all(out.run_dir().join("Claude_iter1.md")).expect("mkdir");
        let recv = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![named(
            "Claude",
            ProviderKind::Anthropic,
            Box::new(MockProvider::with_responses(
                ProviderKind::Anthropic,
                vec![ok_response("x")],
                recv,
            )),
        )];
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            &context("p"),
            agents,
            2,
            1,
            None,
            false,
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
        assert!(events.iter().any(|e| matches!(e, ProgressEvent::AllDone)));
    }

    #[tokio::test]
    async fn run_relay_forward_prompt_prepends_original_task() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![
            named(
                "Claude",
                ProviderKind::Anthropic,
                Box::new(MockProvider::with_responses(
                    ProviderKind::Anthropic,
                    vec![ok_response("first output")],
                    recv_a.clone(),
                )),
            ),
            named(
                "OpenAI",
                ProviderKind::OpenAI,
                Box::new(MockProvider::with_responses(
                    ProviderKind::OpenAI,
                    vec![ok_response("second output")],
                    recv_b.clone(),
                )),
            ),
        ];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_relay(
            &context("write a poem"),
            agents,
            1,
            1,
            None,
            true,
            HashMap::new(),
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let a_msgs = recv_a.lock().expect("lock");
        assert_eq!(a_msgs[0], "write a poem");
        let b_msgs = recv_b.lock().expect("lock");
        assert!(b_msgs[0].contains("Original task: write a poem"));
        assert!(b_msgs[0].contains("first output"));
        assert!(b_msgs[0].contains("Please build upon and improve this work."));
    }

    #[tokio::test]
    async fn run_relay_forward_prompt_for_cli_receiver_includes_original_task() {
        let dir = tempdir().expect("tempdir");
        let out = OutputManager::new(dir.path(), None).expect("out");
        let recv_a = Arc::new(Mutex::new(Vec::new()));
        let recv_b = Arc::new(Mutex::new(Vec::new()));
        let agents = vec![
            named(
                "Claude",
                ProviderKind::Anthropic,
                Box::new(MockProvider::with_responses(
                    ProviderKind::Anthropic,
                    vec![ok_response("first output")],
                    recv_a,
                )),
            ),
            named(
                "OpenAI",
                ProviderKind::OpenAI,
                Box::new(MockProvider::with_responses(
                    ProviderKind::OpenAI,
                    vec![ok_response("second output")],
                    recv_b.clone(),
                )),
            ),
        ];
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut use_cli = HashMap::new();
        use_cli.insert("OpenAI".to_string(), true);

        run_relay(
            &context("write a poem"),
            agents,
            1,
            1,
            None,
            true,
            use_cli,
            &out,
            tx,
            cancel,
        )
        .await
        .expect("run");

        let b_msgs = recv_b.lock().expect("lock");
        assert!(b_msgs[0].contains("Original task: write a poem"));
        assert!(b_msgs[0].contains("Read the previous agent output from file"));
        assert!(b_msgs[0].contains("Claude_iter1.md"));
    }
}
