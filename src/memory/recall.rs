use super::store::MemoryStore;
use super::types::{Memory, RecalledSet};

const STOP_WORDS: &[&str] = &[
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "could", "should", "may", "might", "shall", "can",
    "this", "that", "these", "those", "it", "its", "i", "me", "my", "we", "our", "you", "your",
    "he", "she", "they", "them", "his", "her", "and", "or", "but", "not", "no", "so", "if", "then",
    "than", "too", "very", "just", "about", "up", "out", "on", "off", "in", "of", "to", "for",
    "with", "at", "by", "from", "as", "into", "all", "each", "any", "some", "such", "what",
    "which", "who", "when", "where", "how", "why", "more", "most", "other", "over",
];

pub fn extract_keywords(prompt: &str) -> Vec<String> {
    let stop: std::collections::HashSet<&str> = STOP_WORDS.iter().copied().collect();
    let mut seen = std::collections::HashSet::new();
    let mut words: Vec<String> = prompt
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 2 && !stop.contains(w))
        .filter(|w| seen.insert(w.to_string()))
        .map(|w| w.to_string())
        .collect();
    // Sort by length descending (longer words are more specific)
    words.sort_by_key(|w| std::cmp::Reverse(w.len()));
    words.truncate(20);
    words
}

pub fn recall_for_prompt(
    store: &MemoryStore,
    project_id: &str,
    raw_prompt: &str,
    max: usize,
    max_bytes: usize,
) -> Result<RecalledSet, String> {
    let terms = extract_keywords(raw_prompt);
    if terms.is_empty() {
        return Ok(RecalledSet {
            memories: vec![],
            total_bytes: 0,
        });
    }
    store.recall(project_id, &terms, max, max_bytes)
}

pub fn format_memory_context(recalled: &RecalledSet) -> String {
    if recalled.memories.is_empty() {
        return String::new();
    }
    let mut out = String::from("<project_memory>\n");
    out.push_str("The following memories were recalled from previous runs in this project:\n\n");
    for mem in &recalled.memories {
        format_memory_entry(&mut out, mem);
    }
    out.push_str("</project_memory>");
    out
}

/// Count memory entries from a previously formatted context string.
/// Colocated with `format_memory_entry` so the two stay in sync.
pub fn count_entries_in_context(context: &str) -> usize {
    context.matches("\n[").count()
}

/// Escape characters that could break out of XML wrapper tags.
fn escape_xml(s: &str) -> String {
    // Fast path: skip allocation if no special chars
    if !s.contains('&') && !s.contains('<') && !s.contains('>') {
        return s.to_string();
    }
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn format_memory_entry(out: &mut String, mem: &Memory) {
    out.push_str(&format!(
        "[{}] {}\n",
        mem.kind.as_str().to_uppercase(),
        escape_xml(&mem.content)
    ));
    if !mem.reasoning.is_empty() {
        out.push_str(&format!("  Reasoning: {}\n", escape_xml(&mem.reasoning)));
    }
    if mem.evidence_count > 1 {
        out.push_str(&format!("  (Reinforced {} times)\n", mem.evidence_count));
    }
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::types::MemoryKind;

    #[test]
    fn extract_keywords_filters_stop_words() {
        let kw = extract_keywords("the quick brown fox jumps over a lazy dog");
        assert!(!kw.contains(&"the".to_string()));
        assert!(!kw.contains(&"over".to_string()));
        assert!(kw.contains(&"quick".to_string()));
        assert!(kw.contains(&"brown".to_string()));
        assert!(kw.contains(&"jumps".to_string()));
    }

    #[test]
    fn extract_keywords_dedup() {
        let kw = extract_keywords("rust rust rust python python");
        assert_eq!(kw.iter().filter(|w| *w == "rust").count(), 1);
    }

    #[test]
    fn extract_keywords_length_ordering() {
        let kw = extract_keywords("go python rust typescript");
        // "typescript" should come before shorter words
        assert_eq!(kw[0], "typescript");
    }

    #[test]
    fn extract_keywords_short_words_filtered() {
        let kw = extract_keywords("go is ok");
        assert!(kw.is_empty());
    }

    #[test]
    fn format_memory_context_empty() {
        let recalled = RecalledSet {
            memories: vec![],
            total_bytes: 0,
        };
        assert!(format_memory_context(&recalled).is_empty());
    }

    #[test]
    fn format_memory_context_with_entries() {
        let recalled = RecalledSet {
            memories: vec![Memory {
                id: 1,
                project_id: "p".into(),
                kind: MemoryKind::Decision,
                content: "Use X".into(),
                reasoning: "Because Y".into(),
                source_run: "r".into(),
                source_agent: "a".into(),
                evidence_count: 1,
                tags: String::new(),
                created_at: String::new(),
                expires_at: None,
                updated_at: String::new(),
                recall_count: 0,
                last_recalled_at: None,
                archived: false,
            }],
            total_bytes: 10,
        };
        let ctx = format_memory_context(&recalled);
        assert!(ctx.contains("<project_memory>"));
        assert!(ctx.contains("[DECISION] Use X"));
        assert!(ctx.contains("Reasoning: Because Y"));
        assert!(ctx.contains("</project_memory>"));
    }

    #[test]
    fn format_memory_context_reinforced() {
        let recalled = RecalledSet {
            memories: vec![Memory {
                id: 1,
                project_id: "p".into(),
                kind: MemoryKind::Principle,
                content: "Validate input".into(),
                reasoning: "Security".into(),
                source_run: "r".into(),
                source_agent: "a".into(),
                evidence_count: 3,
                tags: String::new(),
                created_at: String::new(),
                expires_at: None,
                updated_at: String::new(),
                recall_count: 0,
                last_recalled_at: None,
                archived: false,
            }],
            total_bytes: 20,
        };
        let ctx = format_memory_context(&recalled);
        assert!(ctx.contains("Reinforced 3 times"));
    }

    #[test]
    fn count_entries_matches_format_output() {
        let recalled = RecalledSet {
            memories: vec![
                Memory {
                    id: 1,
                    project_id: "p".into(),
                    kind: MemoryKind::Decision,
                    content: "Use X".into(),
                    reasoning: "Because Y".into(),
                    source_run: "r".into(),
                    source_agent: "a".into(),
                    evidence_count: 1,
                    tags: String::new(),
                    created_at: String::new(),
                    expires_at: None,
                    updated_at: String::new(),
                    recall_count: 0,
                    last_recalled_at: None,
                    archived: false,
                },
                Memory {
                    id: 2,
                    project_id: "p".into(),
                    kind: MemoryKind::Principle,
                    content: "Always validate".into(),
                    reasoning: "Security".into(),
                    source_run: "r".into(),
                    source_agent: "a".into(),
                    evidence_count: 3,
                    tags: String::new(),
                    created_at: String::new(),
                    expires_at: None,
                    updated_at: String::new(),
                    recall_count: 0,
                    last_recalled_at: None,
                    archived: false,
                },
            ],
            total_bytes: 30,
        };
        let ctx = format_memory_context(&recalled);
        assert_eq!(count_entries_in_context(&ctx), 2);
    }

    #[test]
    fn count_entries_empty_context() {
        assert_eq!(count_entries_in_context(""), 0);
        assert_eq!(count_entries_in_context("no entries here"), 0);
    }

    #[test]
    fn format_memory_context_escapes_xml() {
        let recalled = RecalledSet {
            memories: vec![Memory {
                id: 1,
                project_id: "p".into(),
                kind: MemoryKind::Decision,
                content: "Use </project_memory> injection & <script>".into(),
                reasoning: "Because <evil>".into(),
                source_run: "r".into(),
                source_agent: "a".into(),
                evidence_count: 1,
                tags: String::new(),
                created_at: String::new(),
                expires_at: None,
                updated_at: String::new(),
                recall_count: 0,
                last_recalled_at: None,
                archived: false,
            }],
            total_bytes: 10,
        };
        let ctx = format_memory_context(&recalled);
        // Content and reasoning must be escaped
        assert!(ctx.contains("&lt;/project_memory&gt;"));
        assert!(ctx.contains("injection &amp; &lt;script&gt;"));
        assert!(ctx.contains("Reasoning: Because &lt;evil&gt;"));
        // Wrapper tags must remain intact
        assert!(ctx.starts_with("<project_memory>"));
        assert!(ctx.ends_with("</project_memory>"));
    }

    #[test]
    fn escape_xml_no_alloc_fast_path() {
        // Plain text should return identical string (no special chars)
        let plain = "hello world 123";
        assert_eq!(escape_xml(plain), plain);
    }

    #[test]
    fn recall_for_prompt_end_to_end() {
        use crate::config::MemoryConfig;
        use crate::memory::types::ExtractedMemory;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = MemoryConfig::default();

        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use PostgreSQL for persistence layer".into(),
                    reasoning: "ACID needed".into(),
                    tags: vec!["database".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();

        let result = recall_for_prompt(
            &store,
            "proj1",
            "What database should we use for PostgreSQL migration?",
            15,
            8192,
        )
        .unwrap();
        assert!(!result.memories.is_empty());
    }
}
