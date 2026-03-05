use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::{Provider, ProviderKind};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

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

            let message = if iteration == 1 && i == 0 {
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
                    let _ = output.write_agent_output(kind, iteration, &resp.content);
                    let _ = progress_tx.send(ProgressEvent::AgentFinished { kind, iteration });
                    last_output = resp.content;
                }
                Err(e) => {
                    let err_str = e.to_string();
                    let _ = output.append_error(&format!("{kind} iter{iteration}: {err_str}"));
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
