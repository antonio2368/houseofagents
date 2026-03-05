use crate::error::AppError;
use crate::execution::{truncate_chars, wait_for_cancel, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::Provider;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub async fn run_solo(
    prompt: &str,
    mut providers: Vec<Box<dyn Provider>>,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let mut handles = Vec::new();

    for provider in providers.drain(..) {
        let prompt = prompt.to_string();
        let tx = progress_tx.clone();
        let cancel = cancel.clone();
        let run_dir = output.run_dir().clone();

        handles.push(tokio::spawn(async move {
            solo_agent(provider, &prompt, &run_dir, tx, cancel).await
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

async fn solo_agent(
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

    let _ = tx.send(ProgressEvent::AgentStarted { kind, iteration: 1 });
    let _ = tx.send(ProgressEvent::AgentLog {
        kind,
        iteration: 1,
        message: "Sending request...".into(),
    });

    let result = tokio::select! {
        res = provider.send(prompt) => res,
        _ = wait_for_cancel(&cancel) => {
            let _ = tx.send(ProgressEvent::AgentLog {
                kind, iteration: 1, message: "Cancelled".into(),
            });
            return;
        }
    };

    match result {
        Ok(resp) => {
            let preview = resp.content.lines().take(3).collect::<Vec<_>>().join(" | ");
            let _ = tx.send(ProgressEvent::AgentLog {
                kind,
                iteration: 1,
                message: format!(
                    "Response received ({} chars): {}...",
                    resp.content.len(),
                    truncate_chars(&preview, 80)
                ),
            });
            let filename = format!("{}_iter1.md", kind.config_key());
            let path = run_dir.join(&filename);
            let _ = std::fs::write(&path, &resp.content);
            let _ = tx.send(ProgressEvent::AgentFinished { kind, iteration: 1 });
        }
        Err(e) => {
            let err_str = e.to_string();
            let _ = tx.send(ProgressEvent::AgentError {
                kind,
                iteration: 1,
                error: err_str.clone(),
                details: Some(err_str),
            });
        }
    }
}

