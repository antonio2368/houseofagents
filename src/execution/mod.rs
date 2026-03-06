pub mod pipeline;
pub mod relay;
pub mod solo;
pub mod swarm;
#[cfg(test)]
pub(crate) mod test_utils;

use crate::provider::ProviderKind;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    Relay,
    Swarm,
    Solo,
    Pipeline,
}

impl ExecutionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionMode::Relay => "relay",
            ExecutionMode::Swarm => "swarm",
            ExecutionMode::Solo => "solo",
            ExecutionMode::Pipeline => "pipeline",
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            ExecutionMode::Relay => "Sequential cooperative - agents build on each other",
            ExecutionMode::Swarm => "Parallel cooperative - agents share results between rounds",
            ExecutionMode::Solo => "Independent parallel - each agent works alone",
            ExecutionMode::Pipeline => {
                "Custom pipeline \u{2014} build arbitrary DAGs of agent blocks"
            }
        }
    }

    pub fn all() -> &'static [ExecutionMode] {
        &[
            ExecutionMode::Relay,
            ExecutionMode::Swarm,
            ExecutionMode::Solo,
            ExecutionMode::Pipeline,
        ]
    }
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ExecutionMode::Relay => "Relay",
                ExecutionMode::Swarm => "Swarm",
                ExecutionMode::Solo => "Solo",
                ExecutionMode::Pipeline => "Pipeline",
            }
        )
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ProgressEvent {
    AgentStarted {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
    },
    AgentLog {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
        message: String,
    },
    AgentFinished {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
    },
    AgentError {
        agent: String,
        kind: ProviderKind,
        iteration: u32,
        error: String,
        /// Full error body/details for display
        details: Option<String>,
    },
    IterationComplete {
        iteration: u32,
    },
    BlockStarted {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
    },
    BlockLog {
        block_id: u32,
        agent_name: String,
        iteration: u32,
        message: String,
    },
    BlockFinished {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
    },
    BlockError {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
        error: String,
        details: Option<String>,
    },
    BlockSkipped {
        block_id: u32,
        agent_name: String,
        label: String,
        iteration: u32,
        reason: String,
    },
    AllDone,
}

pub async fn wait_for_cancel(cancel: &Arc<AtomicBool>) {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

pub fn truncate_chars(s: &str, max_chars: usize) -> String {
    let mut iter = s.chars();
    let mut out = String::new();
    for _ in 0..max_chars {
        match iter.next() {
            Some(ch) => out.push(ch),
            None => return out,
        }
    }
    if iter.next().is_some() {
        format!("{out}...")
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn execution_mode_as_str_values() {
        assert_eq!(ExecutionMode::Relay.as_str(), "relay");
        assert_eq!(ExecutionMode::Swarm.as_str(), "swarm");
        assert_eq!(ExecutionMode::Solo.as_str(), "solo");
    }

    #[test]
    fn execution_mode_display_values() {
        assert_eq!(ExecutionMode::Relay.to_string(), "Relay");
        assert_eq!(ExecutionMode::Swarm.to_string(), "Swarm");
        assert_eq!(ExecutionMode::Solo.to_string(), "Solo");
    }

    #[test]
    fn execution_mode_description_not_empty() {
        for mode in ExecutionMode::all() {
            assert!(!mode.description().trim().is_empty());
        }
    }

    #[test]
    fn truncate_chars_no_truncation() {
        assert_eq!(truncate_chars("hello", 5), "hello");
    }

    #[test]
    fn truncate_chars_with_truncation_ascii() {
        assert_eq!(truncate_chars("hello world", 5), "hello...");
    }

    #[test]
    fn truncate_chars_with_multibyte() {
        assert_eq!(truncate_chars("héllö", 3), "hél...");
    }

    #[test]
    fn truncate_chars_zero_limit() {
        assert_eq!(truncate_chars("abc", 0), "...");
    }

    #[tokio::test]
    async fn wait_for_cancel_returns_when_flag_set() {
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_setter = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            cancel_setter.store(true, Ordering::Relaxed);
        });
        tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            wait_for_cancel(&cancel),
        )
        .await
        .expect("wait_for_cancel should complete");
    }
}
