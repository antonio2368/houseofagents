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
    session_id: Option<String>,
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
            session_id: None,
        }
    }

    pub(crate) fn ok(kind: ProviderKind, content: &str, received: Arc<Mutex<Vec<String>>>) -> Self {
        Self::with_responses(kind, vec![ok_response(content)], received)
    }

    pub(crate) fn with_session_id(mut self, sid: &str) -> Self {
        self.session_id = Some(sid.to_string());
        self
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

    fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

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
                    output_file_written: false,
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

/// Succeeds with a given response on the first call, then panics on all subsequent calls.
/// Useful for testing stale-output cleanup when a replica succeeds on pass N then panics on pass N+1.
pub(crate) struct SuccessThenPanicProvider {
    kind: ProviderKind,
    first_response: Mutex<Option<String>>,
    panic_message: &'static str,
}

impl SuccessThenPanicProvider {
    pub(crate) fn new(
        kind: ProviderKind,
        first_content: &str,
        panic_message: &'static str,
    ) -> Self {
        Self {
            kind,
            first_response: Mutex::new(Some(first_content.to_string())),
            panic_message,
        }
    }
}

impl Provider for SuccessThenPanicProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn clear_history(&mut self) {}

    fn send(&mut self, _message: &str) -> SendFuture<'_> {
        let maybe_content = self.first_response.lock().expect("lock").take();
        let panic_msg = self.panic_message;
        Box::pin(async move {
            if let Some(content) = maybe_content {
                Ok(CompletionResponse {
                    content,
                    debug_logs: Vec::new(),
                    output_file_written: false,
                })
            } else {
                panic!("{}", panic_msg);
            }
        })
    }
}

/// A mock provider that changes its session ID when send() returns an error,
/// simulating CliProvider::reset_after_send_error() behavior.
pub(crate) struct SessionMutatingProvider {
    kind: ProviderKind,
    session_id: String,
    post_error_session_id: String,
    error_message: String,
}

impl SessionMutatingProvider {
    pub(crate) fn new(
        kind: ProviderKind,
        initial_id: &str,
        post_error_id: &str,
        error_message: &str,
    ) -> Self {
        Self {
            kind,
            session_id: initial_id.to_string(),
            post_error_session_id: post_error_id.to_string(),
            error_message: error_message.to_string(),
        }
    }
}

impl Provider for SessionMutatingProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn clear_history(&mut self) {}

    fn session_id(&self) -> Option<&str> {
        Some(&self.session_id)
    }

    fn send(&mut self, _message: &str) -> SendFuture<'_> {
        Box::pin(async move {
            // Simulate reset_after_send_error changing the session ID
            self.session_id = self.post_error_session_id.clone();
            Err(AppError::Provider {
                provider: "mock".to_string(),
                message: self.error_message.clone(),
            })
        })
    }
}

pub(crate) fn ok_response(content: &str) -> Result<CompletionResponse, AppError> {
    Ok(CompletionResponse {
        content: content.to_string(),
        debug_logs: vec!["dbg".to_string()],
        output_file_written: false,
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
