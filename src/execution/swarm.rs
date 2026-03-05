use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::{Provider, ProviderKind};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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
        let taken: Vec<Box<dyn Provider>> = providers.drain(..).collect();
        let mut spawn_handles: Vec<
            JoinHandle<(usize, Box<dyn Provider>, Option<(ProviderKind, String)>)>,
        > = Vec::new();

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

                let result = tokio::select! {
                    res = provider.send(&message) => res,
                    _ = wait_for_cancel(&cancel_flag) => {
                        let _ = tx.send(ProgressEvent::AgentLog {
                            kind, iteration: iter, message: "Cancelled".into(),
                        });
                        return (i, provider, None);
                    }
                };

                match result {
                    Ok(resp) => {
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
                        let _ = std::fs::write(&path, &resp.content);
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

