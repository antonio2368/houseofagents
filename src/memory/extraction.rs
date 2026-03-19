use super::types::ExtractedMemory;
use crate::post_run::{PostRunPromptBudget, EXTRACTION_MAX_INPUT_BYTES};

pub fn build_extraction_prompt(
    files: &[(String, std::path::PathBuf)],
    observation_ttl_days: u32,
    summary_ttl_days: u32,
) -> Result<(String, usize), String> {
    let mut prompt = format!(
        "Extract reusable memories from these agent outputs. Return a JSON array.\n\n\
         Each object: {{\"kind\": \"decision|observation|summary|principle\", \
         \"content\": \"...\", \"reasoning\": \"...\", \"tags\": [\"...\"]}}\n\n\
         Kinds:\n\
         - decision: a choice made and why (permanent). Be specific about what was chosen and the alternatives rejected.\n\
         - observation: a factual finding about the environment, APIs, or tools (temporary, expires after {observation_ttl_days} days).\n\
         - summary: high-level run summary capturing the goal and outcome (temporary, expires after {summary_ttl_days} days). Limit to 1 per run.\n\
         - principle: a reusable rule or pattern worth following again (permanent, reinforced if repeated).\n\n\
         Quality rules:\n\
         - A memory should be useful to an agent that has never seen this run.\n\
         - Be specific about *what* and *why*, not *where* (file paths) or *when* (timestamps).\n\
         - Extract 3-8 memories per run. Fewer high-quality memories beat many low-quality ones.\n\
         - Use 1-3 lowercase domain tags per memory (e.g., \"database\", \"auth\", \"perf\").\n\n\
         Examples of GOOD memories:\n\
         - decision: \"Use connection pooling with max 20 connections for PostgreSQL\" reasoning: \"Single connections caused timeouts under load\"\n\
         - observation: \"The Gemini API returns 429 errors above 60 requests/minute\" reasoning: \"Discovered during load testing\"\n\
         - principle: \"Always validate pagination parameters before passing to SQL queries\" reasoning: \"Unbounded LIMIT/OFFSET caused full table scans\"\n\
         - summary: \"Implemented user authentication with OAuth2, replacing the legacy session-based system\" reasoning: \"Security audit required modern auth\"\n\n\
         Examples of BAD memories (do NOT produce these):\n\
         - \"Changed the database code\" (too vague, no actionable detail)\n\
         - \"Be careful with code\" (too generic, not actionable)\n\
         - \"The run completed successfully\" (trivial, not reusable)\n\
         - \"Modified line 42 in foo.rs\" (too specific to file location, ephemeral)\n\n\
         Return only the JSON array.\n\nAgent outputs:\n",
    );

    let mut budget = PostRunPromptBudget::with_limit(EXTRACTION_MAX_INPUT_BYTES);
    let mut appended_any = false;
    let mut skipped_count: usize = 0;

    // Iterate in reverse so that finalization/consolidation outputs (appended
    // last by callers) get budget priority over earlier agent outputs.
    for (label, path) in files.iter().rev() {
        // Check file size before reading to avoid loading huge files into memory
        // only to discard them when the budget is exceeded. Skip (don't break) so
        // smaller files later in the list (e.g. finalization summaries) still get included.
        if let Ok(meta) = std::fs::metadata(path) {
            if budget.would_exceed(meta.len() as usize) {
                skipped_count += 1;
                continue;
            }
        }
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if budget.add_text(&content, "Extraction input").is_err() {
            skipped_count += 1;
            continue; // Budget exceeded for this file — try remaining smaller ones
        }
        prompt.push_str(&format!("\n--- {label} ---\n{content}\n"));
        appended_any = true;
    }

    // Fallback: if every file exceeded the budget, truncate the first readable
    // file so that long single-agent runs still produce some memories.
    if !appended_any {
        let limit = EXTRACTION_MAX_INPUT_BYTES as usize;
        for (label, path) in files.iter().rev() {
            let content = match std::fs::read_to_string(path) {
                Ok(c) if !c.is_empty() => c,
                _ => continue,
            };
            let truncated = if content.len() > limit {
                &content[..floor_char_boundary(&content, limit)]
            } else {
                &content
            };
            prompt.push_str(&format!(
                "\n--- {label} (truncated to fit extraction budget) ---\n{truncated}\n"
            ));
            appended_any = true;
            break;
        }
    }

    if !appended_any {
        return Err("No file content available for extraction".into());
    }

    Ok((prompt, skipped_count))
}

/// Maximum memories accepted from a single extraction. Prevents a chatty model
/// from flooding the database even when the prompt asks for 3-8.
const MAX_EXTRACTED_MEMORIES: usize = 10;

pub fn parse_extraction_response(response: &str) -> Vec<ExtractedMemory> {
    let trimmed = response.trim();

    let mut result = None;

    // Try 1: raw JSON array
    if result.is_none() {
        if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(trimmed) {
            result = Some(memories);
        }
    }

    // Try 2: extract from ```json ... ``` fence
    if result.is_none() {
        if let Some(json_str) = extract_fenced_json(trimmed) {
            if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(json_str) {
                result = Some(memories);
            }
        }
    }

    // Try 3: bare ``` ... ``` fence
    if result.is_none() {
        if let Some(start) = trimmed.find("```") {
            let after = &trimmed[start + 3..];
            // Skip optional language tag on same line
            let content_start = after.find('\n').map(|i| i + 1).unwrap_or(0);
            if let Some(end) = after[content_start..].find("```") {
                let json_str = after[content_start..content_start + end].trim();
                if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(json_str) {
                    result = Some(memories);
                }
            }
        }
    }

    // Try 4: {"memories": [...]} wrapper
    if result.is_none() {
        if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Some(arr) = wrapper.get("memories").and_then(|v| v.as_array()) {
                if let Ok(memories) = serde_json::from_value::<Vec<ExtractedMemory>>(
                    serde_json::Value::Array(arr.clone()),
                ) {
                    result = Some(memories);
                }
            }
        }
    }

    // Apply hard cap to prevent DB flooding from chatty models
    let mut memories = result.unwrap_or_default();
    memories.truncate(MAX_EXTRACTED_MEMORIES);
    memories
}

/// Find the largest byte index <= `max` that is a valid char boundary.
pub(super) fn floor_char_boundary(s: &str, max: usize) -> usize {
    if max >= s.len() {
        return s.len();
    }
    let mut i = max;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn extract_fenced_json(s: &str) -> Option<&str> {
    let marker = "```json";
    let start = s.find(marker)?;
    let content_start = start + marker.len();
    let after = &s[content_start..];
    let newline = after.find('\n').map(|i| i + 1).unwrap_or(0);
    let end = after[newline..].find("```")?;
    Some(after[newline..newline + end].trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::types::MemoryKind;
    use tempfile::tempdir;

    #[test]
    fn build_extraction_prompt_inlines_content() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.md");
        std::fs::write(&file, "Agent output content here").unwrap();

        let (prompt, skipped) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180).unwrap();
        assert!(prompt.contains("Agent output content here"));
        assert!(prompt.contains("--- Agent1 ---"));
        assert_eq!(skipped, 0);
    }

    #[test]
    fn build_extraction_prompt_budget_exceeded() {
        let dir = tempdir().unwrap();
        let mut files = Vec::new();
        // Create files that together exceed 100KB
        for i in 0..20 {
            let file = dir.path().join(format!("agent{i}.md"));
            std::fs::write(&file, "x".repeat(10 * 1024)).unwrap();
            files.push((format!("Agent{i}"), file));
        }

        let (prompt, _skipped) = build_extraction_prompt(&files, 90, 180).unwrap();
        // Should not fail, just truncate
        assert!(prompt.len() < 150 * 1024);
    }

    #[test]
    fn build_extraction_prompt_truncates_oversized_single_file() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("big.md");
        // Create a single file larger than the extraction budget
        std::fs::write(&file, "y".repeat(200 * 1024)).unwrap();

        let (prompt, _skipped) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180).unwrap();
        assert!(prompt.contains("truncated to fit extraction budget"));
        // Should be capped close to 100KB, not the full 200KB
        assert!(prompt.len() < 110 * 1024);
    }

    #[test]
    fn build_extraction_prompt_prioritizes_later_files() {
        let dir = tempdir().unwrap();
        let fin_file = dir.path().join("finalization.md");
        std::fs::write(&fin_file, "FINALIZATION_MARKER unique content").unwrap();
        let mut files = Vec::new();
        for i in 0..15 {
            let f = dir.path().join(format!("agent{i}.md"));
            std::fs::write(&f, "x".repeat(10 * 1024)).unwrap();
            files.push((format!("Agent{i}"), f));
        }
        files.push(("Finalization".into(), fin_file));

        let (prompt, skipped) = build_extraction_prompt(&files, 90, 180).unwrap();
        assert!(prompt.contains("FINALIZATION_MARKER"));
        assert!(skipped > 0);
    }

    #[test]
    fn parse_raw_json_array() {
        let response = r#"[{"kind":"decision","content":"Use X","reasoning":"Y","tags":["a"]}]"#;
        let memories = parse_extraction_response(response);
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].kind, MemoryKind::Decision);
    }

    #[test]
    fn parse_json_fence() {
        let response = "Here are the memories:\n```json\n[{\"kind\":\"observation\",\"content\":\"Found X\"}]\n```";
        let memories = parse_extraction_response(response);
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].kind, MemoryKind::Observation);
    }

    #[test]
    fn parse_bare_fence() {
        let response = "```\n[{\"kind\":\"summary\",\"content\":\"Did Y\"}]\n```";
        let memories = parse_extraction_response(response);
        assert_eq!(memories.len(), 1);
    }

    #[test]
    fn parse_wrapper_object() {
        let response =
            r#"{"memories":[{"kind":"principle","content":"Always test","reasoning":"Quality"}]}"#;
        let memories = parse_extraction_response(response);
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].kind, MemoryKind::Principle);
    }

    #[test]
    fn parse_garbage_returns_empty() {
        let memories = parse_extraction_response("this is not json at all");
        assert!(memories.is_empty());
    }

    #[test]
    fn parse_empty_returns_empty() {
        let memories = parse_extraction_response("");
        assert!(memories.is_empty());
    }

    #[test]
    fn parse_truncates_to_hard_cap() {
        // Generate 15 valid memories — should be capped to MAX_EXTRACTED_MEMORIES
        let items: Vec<String> = (0..15)
            .map(|i| {
                format!(
                    r#"{{"kind":"observation","content":"Finding number {i}","reasoning":"test"}}"#
                )
            })
            .collect();
        let response = format!("[{}]", items.join(","));
        let memories = parse_extraction_response(&response);
        assert_eq!(memories.len(), MAX_EXTRACTED_MEMORIES);
    }
}
