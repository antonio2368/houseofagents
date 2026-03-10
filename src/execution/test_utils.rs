use crate::error::AppError;
use crate::execution::ProgressEvent;
use crate::provider::{CompletionResponse, Provider, ProviderKind, SendFuture};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub(crate) struct MockProvider {
    kind: ProviderKind,
    responses: VecDeque<Result<CompletionResponse, AppError>>,
    received: Arc<Mutex<Vec<String>>>,
    live_tx: Option<mpsc::UnboundedSender<String>>,
}

pub(crate) struct PanicProvider {
    kind: ProviderKind,
    panic_message: &'static str,
}

impl MockProvider {
    pub(crate) fn with_responses(
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

    pub(crate) fn ok(kind: ProviderKind, content: &str, received: Arc<Mutex<Vec<String>>>) -> Self {
        Self::with_responses(kind, vec![ok_response(content)], received)
    }

    pub(crate) fn err(
        kind: ProviderKind,
        message: &str,
        received: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self::with_responses(
            kind,
            vec![Err(AppError::Provider {
                provider: "mock".to_string(),
                message: message.to_string(),
            })],
            received,
        )
    }
}

impl PanicProvider {
    pub(crate) fn new(kind: ProviderKind, panic_message: &'static str) -> Self {
        Self {
            kind,
            panic_message,
        }
    }
}

impl Provider for MockProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn clear_history(&mut self) {}

    fn set_live_log_sender(&mut self, tx: Option<mpsc::UnboundedSender<String>>) {
        self.live_tx = tx;
    }

    fn send(&mut self, message: &str) -> SendFuture<'_> {
        let message = message.to_string();
        Box::pin(async move {
            self.received.lock().expect("lock").push(message);
            if let Some(tx) = self.live_tx.as_ref() {
                let _ = tx.send("live".to_string());
            }
            self.responses.pop_front().unwrap_or_else(|| {
                Ok(CompletionResponse {
                    content: "default".to_string(),
                    debug_logs: Vec::new(),
                })
            })
        })
    }
}

impl Provider for PanicProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn clear_history(&mut self) {}

    fn send(&mut self, _message: &str) -> SendFuture<'_> {
        let msg = self.panic_message;
        Box::pin(async move {
            panic!("{}", msg);
        })
    }
}

pub(crate) fn ok_response(content: &str) -> Result<CompletionResponse, AppError> {
    Ok(CompletionResponse {
        content: content.to_string(),
        debug_logs: vec!["dbg".to_string()],
    })
}

pub(crate) fn collect_progress_events(
    mut rx: mpsc::UnboundedReceiver<ProgressEvent>,
) -> Vec<ProgressEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}
