use crate::execution::{BatchProgressEvent, ProgressEvent, RunOutcome};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

pub async fn run_multi<F, Fut>(
    runs: u32,
    concurrency: u32,
    batch_tx: mpsc::UnboundedSender<BatchProgressEvent>,
    cancel_flag: Arc<AtomicBool>,
    run_factory: F,
) where
    F: Fn(u32, mpsc::UnboundedSender<ProgressEvent>, Arc<AtomicBool>) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = (RunOutcome, Option<String>)> + Send + 'static,
{
    if runs == 0 {
        let _ = batch_tx.send(BatchProgressEvent::AllRunsDone);
        return;
    }

    let effective_concurrency = match concurrency {
        0 => runs.max(1),
        value => value.min(runs).max(1),
    };

    for run_id in 1..=runs {
        let _ = batch_tx.send(BatchProgressEvent::RunQueued { run_id });
    }

    let mut join_set = JoinSet::new();
    let mut task_ids = HashMap::new();
    let mut next_run_id = 1u32;
    let mut active = 0u32;

    while active < effective_concurrency && next_run_id <= runs {
        spawn_one_run(
            &mut join_set,
            &mut task_ids,
            next_run_id,
            batch_tx.clone(),
            cancel_flag.clone(),
            run_factory.clone(),
        );
        next_run_id += 1;
        active += 1;
    }

    while let Some(result) = join_set.join_next().await {
        active = active.saturating_sub(1);
        match result {
            Ok((run_id, outcome, error)) => {
                let _ = batch_tx.send(BatchProgressEvent::RunFinished {
                    run_id,
                    outcome,
                    error,
                });
            }
            Err(join_error) => {
                let run_id = task_ids.remove(&join_error.id()).unwrap_or(0);
                let _ = batch_tx.send(BatchProgressEvent::RunFinished {
                    run_id,
                    outcome: if cancel_flag.load(Ordering::Relaxed) {
                        RunOutcome::Cancelled
                    } else {
                        RunOutcome::Failed
                    },
                    error: Some(format!("Run task failed: {join_error}")),
                });
            }
        }

        if cancel_flag.load(Ordering::Relaxed) {
            continue;
        }

        while active < effective_concurrency && next_run_id <= runs {
            spawn_one_run(
                &mut join_set,
                &mut task_ids,
                next_run_id,
                batch_tx.clone(),
                cancel_flag.clone(),
                run_factory.clone(),
            );
            next_run_id += 1;
            active += 1;
        }
    }

    if cancel_flag.load(Ordering::Relaxed) {
        for run_id in next_run_id..=runs {
            let _ = batch_tx.send(BatchProgressEvent::RunFinished {
                run_id,
                outcome: RunOutcome::Cancelled,
                error: None,
            });
        }
    }

    let _ = batch_tx.send(BatchProgressEvent::AllRunsDone);
}

fn spawn_one_run<F, Fut>(
    join_set: &mut JoinSet<(u32, RunOutcome, Option<String>)>,
    task_ids: &mut HashMap<tokio::task::Id, u32>,
    run_id: u32,
    batch_tx: mpsc::UnboundedSender<BatchProgressEvent>,
    cancel_flag: Arc<AtomicBool>,
    run_factory: F,
) where
    F: Fn(u32, mpsc::UnboundedSender<ProgressEvent>, Arc<AtomicBool>) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = (RunOutcome, Option<String>)> + Send + 'static,
{
    let _ = batch_tx.send(BatchProgressEvent::RunStarted { run_id });

    let abort_handle = join_set.spawn(async move {
        if cancel_flag.load(Ordering::Relaxed) {
            return (run_id, RunOutcome::Cancelled, None);
        }

        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let forward_tx = batch_tx.clone();
        let forwarder = tokio::spawn(async move {
            while let Some(event) = progress_rx.recv().await {
                let _ = forward_tx.send(BatchProgressEvent::RunEvent { run_id, event });
            }
        });

        let (outcome, error) = run_factory(run_id, progress_tx, cancel_flag).await;
        let _ = forwarder.await;
        (run_id, outcome, error)
    });
    task_ids.insert(abort_handle.id(), run_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::time::Duration;

    fn collect_batch_events(
        mut rx: mpsc::UnboundedReceiver<BatchProgressEvent>,
    ) -> Vec<BatchProgressEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn run_multi_respects_concurrency_limit() {
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        run_multi(5, 2, tx, cancel, {
            let active = active.clone();
            let peak = peak.clone();
            move |_run_id, _progress_tx, _cancel| {
                let active = active.clone();
                let peak = peak.clone();
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    (RunOutcome::Done, None)
                }
            }
        })
        .await;

        let events = collect_batch_events(rx);
        let finished_done = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    BatchProgressEvent::RunFinished {
                        outcome: RunOutcome::Done,
                        ..
                    }
                )
            })
            .count();

        assert_eq!(finished_done, 5);
        assert!(peak.load(Ordering::SeqCst) <= 2);
        assert!(matches!(
            events.last(),
            Some(BatchProgressEvent::AllRunsDone)
        ));
    }

    #[tokio::test]
    async fn run_multi_cancels_unstarted_runs_after_cancel_flag_set() {
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));

        run_multi(
            4,
            1,
            tx,
            cancel.clone(),
            move |run_id, _progress_tx, cancel| async move {
                if run_id == 1 {
                    cancel.store(true, Ordering::Relaxed);
                }
                (RunOutcome::Cancelled, None)
            },
        )
        .await;

        let events = collect_batch_events(rx);
        let started = events
            .iter()
            .filter(|event| matches!(event, BatchProgressEvent::RunStarted { .. }))
            .count();
        let cancelled = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    BatchProgressEvent::RunFinished {
                        outcome: RunOutcome::Cancelled,
                        ..
                    }
                )
            })
            .count();

        assert_eq!(started, 1);
        assert_eq!(cancelled, 4);
        assert!(matches!(
            events.last(),
            Some(BatchProgressEvent::AllRunsDone)
        ));
    }
}
