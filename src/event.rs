use crossterm::event::{self, Event as CrosstermEvent, KeyEvent};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug)]
#[allow(dead_code)]
pub enum Event {
    Key(KeyEvent),
    Paste(String),
    Tick,
    Resize(u16, u16),
}

pub struct EventHandler {
    rx: mpsc::UnboundedReceiver<Event>,
    _tx: mpsc::UnboundedSender<Event>,
    shutdown: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl EventHandler {
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let sender = tx.clone();
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = shutdown.clone();
        let worker = std::thread::spawn(move || {
            let tick_interval = if tick_rate.is_zero() {
                Duration::from_millis(1)
            } else {
                tick_rate
            };
            let poll_interval = tick_interval.min(Duration::from_millis(50));
            let mut last_tick = std::time::Instant::now();
            loop {
                if worker_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if event::poll(poll_interval).unwrap_or(false) {
                    match event::read() {
                        Ok(CrosstermEvent::Key(key)) => {
                            if sender.send(Event::Key(key)).is_err() {
                                break;
                            }
                        }
                        Ok(CrosstermEvent::Paste(text)) => {
                            if sender.send(Event::Paste(text)).is_err() {
                                break;
                            }
                        }
                        Ok(CrosstermEvent::Resize(w, h)) => {
                            if sender.send(Event::Resize(w, h)).is_err() {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                if worker_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if last_tick.elapsed() >= tick_interval {
                    if sender.send(Event::Tick).is_err() {
                        break;
                    }
                    last_tick = std::time::Instant::now();
                }
            }
        });
        Self {
            rx,
            _tx: tx,
            shutdown,
            worker: Some(worker),
        }
    }

    pub async fn next(&mut self) -> Option<Event> {
        self.rx.recv().await
    }
}

impl Drop for EventHandler {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn event_handler_emits_event() {
        let mut handler = EventHandler::new(Duration::from_millis(5));
        let evt = tokio::time::timeout(Duration::from_millis(200), handler.next())
            .await
            .expect("timeout waiting for event");
        assert!(evt.is_some());
    }
}
