use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryKind {
    Decision,
    Observation,
    Summary,
    Principle,
}

impl MemoryKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryKind::Decision => "decision",
            MemoryKind::Observation => "observation",
            MemoryKind::Summary => "summary",
            MemoryKind::Principle => "principle",
        }
    }

    pub fn is_permanent(&self) -> bool {
        matches!(self, MemoryKind::Decision | MemoryKind::Principle)
    }

    #[allow(dead_code)]
    pub fn all() -> &'static [MemoryKind] {
        &[
            MemoryKind::Decision,
            MemoryKind::Observation,
            MemoryKind::Summary,
            MemoryKind::Principle,
        ]
    }
}

impl fmt::Display for MemoryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for MemoryKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "decision" => Ok(MemoryKind::Decision),
            "observation" => Ok(MemoryKind::Observation),
            "summary" => Ok(MemoryKind::Summary),
            "principle" => Ok(MemoryKind::Principle),
            _ => Err(format!("Unknown memory kind: {s}")),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Memory {
    pub id: i64,
    pub project_id: String,
    pub kind: MemoryKind,
    pub content: String,
    pub reasoning: String,
    pub source_run: String,
    pub source_agent: String,
    pub evidence_count: i64,
    pub tags: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub updated_at: String,
    pub recall_count: i64,
    pub last_recalled_at: Option<String>,
    pub archived: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedMemory {
    pub kind: MemoryKind,
    pub content: String,
    #[serde(default)]
    pub reasoning: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[allow(dead_code)]
pub struct RecalledSet {
    pub memories: Vec<Memory>,
    pub total_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_kind_as_str() {
        assert_eq!(MemoryKind::Decision.as_str(), "decision");
        assert_eq!(MemoryKind::Principle.as_str(), "principle");
    }

    #[test]
    fn memory_kind_from_str() {
        assert_eq!(
            "decision".parse::<MemoryKind>().unwrap(),
            MemoryKind::Decision
        );
        assert_eq!(
            "PRINCIPLE".parse::<MemoryKind>().unwrap(),
            MemoryKind::Principle
        );
        assert!("unknown".parse::<MemoryKind>().is_err());
    }

    #[test]
    fn memory_kind_permanent() {
        assert!(MemoryKind::Decision.is_permanent());
        assert!(MemoryKind::Principle.is_permanent());
        assert!(!MemoryKind::Observation.is_permanent());
        assert!(!MemoryKind::Summary.is_permanent());
    }

    #[test]
    fn extracted_memory_deserialize() {
        let json =
            r#"{"kind":"decision","content":"use X","reasoning":"because Y","tags":["arch"]}"#;
        let mem: ExtractedMemory = serde_json::from_str(json).unwrap();
        assert_eq!(mem.kind, MemoryKind::Decision);
        assert_eq!(mem.content, "use X");
        assert_eq!(mem.tags, vec!["arch"]);
    }

    #[test]
    fn extracted_memory_deserialize_defaults() {
        let json = r#"{"kind":"observation","content":"found X"}"#;
        let mem: ExtractedMemory = serde_json::from_str(json).unwrap();
        assert!(mem.reasoning.is_empty());
        assert!(mem.tags.is_empty());
    }
}
