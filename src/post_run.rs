use crate::app::ConsolidationTarget;
use crate::execution::ExecutionMode;
use crate::output::OutputManager;
use crate::provider;

// ---------------------------------------------------------------------------
// Filename parsing helpers
// ---------------------------------------------------------------------------

pub(crate) fn parse_agent_iteration_filename(name: &str, agent_key: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let prefix = format!("{agent_key}_iter");
    if !name.starts_with(&prefix) {
        return None;
    }
    let iter_str = name.trim_end_matches(".md").strip_prefix(&prefix)?;
    iter_str.parse::<u32>().ok()
}

/// Parse iteration from a pipeline block output filename.
/// Matches both named blocks (`{name}_b{id}_{agent}_iter{n}.md`)
/// and unnamed blocks (`block{id}_{agent}_iter{n}.md`).
pub(crate) fn parse_pipeline_iteration_filename(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let stem = name.trim_end_matches(".md");

    // Search right-to-left so block names containing "_b" (for example
    // "web_builder") still detect the actual "_b{id}_" marker.
    let mut search_end = stem.len();
    while let Some(rel) = stem[..search_end].rfind("_b") {
        let after_b = &stem[rel + 2..];
        if let Some(end_of_id) = after_b.find('_') {
            if after_b[..end_of_id].parse::<u32>().is_ok() {
                return parse_iteration_from_filename(name);
            }
        }
        search_end = rel;
    }

    if let Some(rest) = stem.strip_prefix("block") {
        if let Some(underscore) = rest.find('_') {
            if rest[..underscore].parse::<u32>().is_ok() {
                return parse_iteration_from_filename(name);
            }
        }
    }

    None
}

pub(crate) fn parse_iteration_from_filename(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let stem = name.trim_end_matches(".md");
    // Strip optional _loop{N} suffix before parsing iteration number
    let stem = if let Some(lp) = stem.rfind("_loop") {
        if stem[lp + 5..].parse::<u32>().is_ok() {
            &stem[..lp]
        } else {
            stem
        }
    } else {
        stem
    };
    let iter_pos = stem.rfind("_iter")?;
    let iter_str = &stem[iter_pos + 5..];
    iter_str.parse::<u32>().ok()
}

// ---------------------------------------------------------------------------
// Iteration discovery
// ---------------------------------------------------------------------------

pub(crate) fn find_last_iteration(run_dir: &std::path::Path, agent_keys: &[String]) -> Option<u32> {
    let mut max_iter: Option<u32> = None;
    let entries = std::fs::read_dir(run_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if agent_keys.is_empty() {
            if let Some(iter) = parse_pipeline_iteration_filename(&name) {
                max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
            }
        } else {
            for key in agent_keys {
                if let Some(iter) = parse_agent_iteration_filename(&name, key) {
                    max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
                }
            }
        }
    }
    max_iter
}

pub(crate) async fn find_last_iteration_async(
    run_dir: &std::path::Path,
    agent_keys: &[String],
) -> Option<u32> {
    let mut max_iter: Option<u32> = None;
    let mut entries = tokio::fs::read_dir(run_dir).await.ok()?;

    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let name = entry.file_name().to_string_lossy().to_string();
                if agent_keys.is_empty() {
                    if let Some(iter) = parse_pipeline_iteration_filename(&name) {
                        max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
                    }
                } else {
                    for key in agent_keys {
                        if let Some(iter) = parse_agent_iteration_filename(&name, key) {
                            max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    max_iter
}

// ---------------------------------------------------------------------------
// Natural sorting
// ---------------------------------------------------------------------------

/// Natural (numeric-aware) comparison for filenames so that e.g. `_r2` sorts
/// before `_r10` instead of lexicographic `_r1, _r10, ..., _r2`.
pub(crate) fn natural_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    let mut ai = a.as_bytes().iter().peekable();
    let mut bi = b.as_bytes().iter().peekable();

    loop {
        match (ai.peek(), bi.peek()) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some(&&ac), Some(&&bc)) => {
                if ac.is_ascii_digit() && bc.is_ascii_digit() {
                    let na = consume_number(&mut ai);
                    let nb = consume_number(&mut bi);
                    match na.cmp(&nb) {
                        std::cmp::Ordering::Equal => continue,
                        ord => return ord,
                    }
                }
                match ac.cmp(&bc) {
                    std::cmp::Ordering::Equal => {
                        ai.next();
                        bi.next();
                    }
                    ord => return ord,
                }
            }
        }
    }
}

fn consume_number(iter: &mut std::iter::Peekable<std::slice::Iter<u8>>) -> u64 {
    let mut n: u64 = 0;
    while let Some(&&ch) = iter.peek() {
        if ch.is_ascii_digit() {
            n = n.saturating_mul(10).saturating_add((ch - b'0') as u64);
            iter.next();
        } else {
            break;
        }
    }
    n
}

/// Given a list of `(filename, path)` pairs, keep only the highest `_loop{N}`
/// variant per stem. Files without a `_loop{N}` suffix pass through unchanged.
pub(crate) fn keep_highest_loop_pass(
    files: Vec<(String, std::path::PathBuf)>,
) -> Vec<(String, std::path::PathBuf)> {
    use std::collections::HashMap;
    let mut best: HashMap<String, (u32, String, std::path::PathBuf)> = HashMap::new();
    let mut no_loop: Vec<(String, std::path::PathBuf)> = Vec::new();

    for (name, path) in files {
        let stem = name.trim_end_matches(".md");
        if let Some(lp) = stem.rfind("_loop") {
            if let Ok(n) = stem[lp + 5..].parse::<u32>() {
                let base = stem[..lp].to_string();
                let entry =
                    best.entry(base)
                        .or_insert((0, String::new(), std::path::PathBuf::new()));
                if n >= entry.0 {
                    *entry = (n, name, path);
                }
                continue;
            }
        }
        // Check if there is already a looped variant for this stem
        let base = stem.to_string();
        if !best.contains_key(&base) {
            no_loop.push((name, path));
        }
    }

    // Remove no-loop entries that have a looped counterpart
    no_loop.retain(|(name, _)| {
        let stem = name.trim_end_matches(".md");
        !best.contains_key(stem)
    });

    let mut result = no_loop;
    result.extend(best.into_values().map(|(_, name, path)| (name, path)));
    result
}

// ---------------------------------------------------------------------------
// Output discovery
// ---------------------------------------------------------------------------

pub(crate) fn discover_final_outputs(
    run_dir: &std::path::Path,
    mode: ExecutionMode,
    selected_agents: &[String],
) -> Vec<(String, std::path::PathBuf)> {
    if mode == ExecutionMode::Pipeline {
        let Some(last_iteration) = find_last_iteration(run_dir, &[]) else {
            return Vec::new();
        };
        let mut files = std::fs::read_dir(run_dir)
            .ok()
            .into_iter()
            .flat_map(|entries| entries.flatten())
            .filter_map(|entry| {
                let path = entry.path();
                let name = path.file_name()?.to_str()?.to_string();
                if parse_pipeline_iteration_filename(&name) == Some(last_iteration) {
                    Some((name, path))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        // Keep only the highest loop pass per stem
        files = keep_highest_loop_pass(files);
        files.sort_by(|a, b| natural_cmp(&a.0, &b.0));
        return files;
    }

    let agent_keys = selected_agents
        .iter()
        .map(|n| OutputManager::sanitize_session_name(n))
        .collect::<Vec<_>>();
    let Some(last_iteration) = find_last_iteration(run_dir, &agent_keys) else {
        return Vec::new();
    };

    let mut files = Vec::new();
    for name in selected_agents {
        let file_key = OutputManager::sanitize_session_name(name);
        let path = run_dir.join(format!("{file_key}_iter{last_iteration}.md"));
        if path.exists() {
            files.push((name.clone(), path));
        }
    }
    files
}

pub(crate) async fn discover_final_outputs_async(
    run_dir: &std::path::Path,
    mode: ExecutionMode,
    selected_agents: &[String],
) -> Vec<(String, std::path::PathBuf)> {
    if mode == ExecutionMode::Pipeline {
        let Some(last_iteration) = find_last_iteration_async(run_dir, &[]).await else {
            return Vec::new();
        };

        let mut files = Vec::new();
        let mut entries = match tokio::fs::read_dir(run_dir).await {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };

        loop {
            match entries.next_entry().await {
                Ok(Some(entry)) => {
                    let path = entry.path();
                    let Some(name) = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(str::to_string)
                    else {
                        continue;
                    };
                    if parse_pipeline_iteration_filename(&name) == Some(last_iteration) {
                        files.push((name, path));
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        // Keep only the highest loop pass per stem
        files = keep_highest_loop_pass(files);
        files.sort_by(|a, b| natural_cmp(&a.0, &b.0));
        return files;
    }

    let agent_keys = selected_agents
        .iter()
        .map(|n| OutputManager::sanitize_session_name(n))
        .collect::<Vec<_>>();
    let Some(last_iteration) = find_last_iteration_async(run_dir, &agent_keys).await else {
        return Vec::new();
    };

    let mut files = Vec::new();
    for name in selected_agents {
        let file_key = OutputManager::sanitize_session_name(name);
        let path = run_dir.join(format!("{file_key}_iter{last_iteration}.md"));
        if tokio::fs::metadata(&path).await.is_ok() {
            files.push((name.clone(), path));
        }
    }
    files
}

// ---------------------------------------------------------------------------
// Consolidation prompt and budget
// ---------------------------------------------------------------------------

pub(crate) const POST_RUN_SYNTHESIS_MAX_INPUT_BYTES: u64 = 200 * 1024;
pub(crate) const CROSS_RUN_MAX_INPUT_BYTES: u64 = POST_RUN_SYNTHESIS_MAX_INPUT_BYTES;

pub(crate) struct PostRunPromptBudget {
    used_bytes: u64,
}

impl PostRunPromptBudget {
    pub(crate) fn new() -> Self {
        Self { used_bytes: 0 }
    }

    pub(crate) fn add_bytes(&mut self, bytes: u64, context: &str) -> Result<(), String> {
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        if self.used_bytes > POST_RUN_SYNTHESIS_MAX_INPUT_BYTES {
            return Err(format!(
                "{context} is too large to inline into a post-run synthesis prompt (limit: {} KB). Reduce the number of reports or shorten the outputs.",
                POST_RUN_SYNTHESIS_MAX_INPUT_BYTES / 1024
            ));
        }
        Ok(())
    }

    pub(crate) fn add_text(&mut self, text: &str, context: &str) -> Result<(), String> {
        self.add_bytes(text.len() as u64, context)
    }

    pub(crate) async fn add_file_async(
        &mut self,
        path: &std::path::Path,
        context: &str,
    ) -> Result<(), String> {
        let bytes = tokio::fs::metadata(path)
            .await
            .map_err(|e| format!("Failed to inspect {}: {e}", path.display()))?
            .len();
        self.add_bytes(bytes, context)
    }

    pub(crate) fn add_file_sync(
        &mut self,
        path: &std::path::Path,
        context: &str,
    ) -> Result<(), String> {
        let bytes = std::fs::metadata(path)
            .map_err(|e| format!("Failed to inspect {}: {e}", path.display()))?
            .len();
        self.add_bytes(bytes, context)
    }
}

pub(crate) struct ConsolidationRequest {
    pub(crate) run_dir: std::path::PathBuf,
    pub(crate) target: ConsolidationTarget,
    pub(crate) mode: ExecutionMode,
    pub(crate) selected_agents: Vec<String>,
    pub(crate) successful_runs: Vec<u32>,
    pub(crate) batch_stage1_done: bool,
    pub(crate) additional: String,
    pub(crate) agent_name: String,
    pub(crate) agent_use_cli: bool,
}

pub(crate) async fn build_file_consolidation_prompt(
    files: &[(String, std::path::PathBuf)],
    additional: &str,
    use_cli: bool,
) -> Result<String, String> {
    let mut prompt = String::from("Consolidate these outputs into one final markdown answer.\n\n");
    let mut budget = PostRunPromptBudget::new();
    budget.add_text(additional, "Additional consolidation instructions")?;

    if use_cli {
        prompt.push_str("Files to read:\n");
        for (label, path) in files {
            prompt.push_str(&format!("- {label}: {}\n", path.display()));
        }
        prompt.push_str("\nRead each file before writing.\n");
    } else {
        prompt.push_str("Input reports:\n");
        for (label, path) in files {
            budget
                .add_file_async(path, "Consolidation report input")
                .await?;
            let content = tokio::fs::read_to_string(path)
                .await
                .map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
            prompt.push_str(&format!("\n--- {label} ---\n{content}\n"));
        }
    }

    prompt.push_str(
        "\nInstructions:\n- Resolve disagreements and keep the strongest points.\n- Return one high-quality markdown response.\n- Do not write files and do not ask for filesystem permissions.\n- The application will save your response to disk.\n",
    );
    if !additional.is_empty() {
        prompt.push_str("\nAdditional instructions from user:\n");
        prompt.push_str(additional);
        prompt.push('\n');
    }
    Ok(prompt)
}

pub(crate) fn build_cross_run_consolidation_prompt(
    runs: &[(u32, String)],
    mode: ExecutionMode,
    agents: &[String],
    additional: &str,
) -> String {
    let mut prompt = format!(
        "You are reviewing {} independent runs of the same task.\nEach run executed in {} mode with agents: {}.\n",
        runs.len(),
        mode,
        if agents.is_empty() {
            "(pipeline blocks)".to_string()
        } else {
            agents.join(", ")
        }
    );

    for (run_id, content) in runs {
        prompt.push_str(&format!("\n--- Run {run_id} ---\n{content}\n"));
    }

    if !additional.is_empty() {
        prompt.push_str("\nAdditional instructions from user:\n");
        prompt.push_str(additional);
        prompt.push('\n');
    }

    prompt.push_str(
        "\nSynthesize these results. Identify consensus, highlight disagreements, and produce a final consolidated answer.\n",
    );
    prompt
}

pub(crate) async fn run_consolidation_with_provider_factory<F>(
    request: ConsolidationRequest,
    provider_factory: F,
) -> Result<String, String>
where
    F: Fn() -> Box<dyn provider::Provider>,
{
    match request.target {
        ConsolidationTarget::Single => {
            let files = discover_final_outputs_async(
                &request.run_dir,
                request.mode,
                &request.selected_agents,
            )
            .await;
            if files.is_empty() {
                return Err("No iteration outputs found to consolidate".to_string());
            }

            let prompt =
                build_file_consolidation_prompt(&files, &request.additional, request.agent_use_cli)
                    .await?;
            let file_key = OutputManager::sanitize_session_name(&request.agent_name);
            let output_path = request.run_dir.join(format!("consolidated_{file_key}.md"));
            let mut provider = provider_factory();
            let response = provider.send(&prompt).await.map_err(|e| e.to_string())?;
            tokio::fs::write(&output_path, &response.content)
                .await
                .map(|_| output_path.display().to_string())
                .map_err(|e| format!("Failed to write consolidation output: {e}"))
        }
        ConsolidationTarget::PerRun => {
            if request.successful_runs.is_empty() {
                return Err("No successful runs available for consolidation".to_string());
            }

            for run_id in request.successful_runs {
                let run_path = request.run_dir.join(format!("run_{run_id}"));
                let files =
                    discover_final_outputs_async(&run_path, request.mode, &request.selected_agents)
                        .await;
                if files.len() <= 1 {
                    continue;
                }

                let prompt = build_file_consolidation_prompt(
                    &files,
                    &request.additional,
                    request.agent_use_cli,
                )
                .await?;
                let mut provider = provider_factory();
                let response = provider.send(&prompt).await.map_err(|e| e.to_string())?;
                tokio::fs::write(run_path.join("consolidation.md"), response.content)
                    .await
                    .map_err(|e| format!("Failed to write per-run consolidation: {e}"))?;
            }

            Ok("Per-run consolidation completed".to_string())
        }
        ConsolidationTarget::AcrossRuns => {
            if request.successful_runs.is_empty() {
                return Err("No successful runs available for cross-run consolidation".to_string());
            }

            let mut run_inputs = Vec::new();
            let mut total_raw_bytes = 0u64;

            for run_id in request.successful_runs {
                let run_path = request.run_dir.join(format!("run_{run_id}"));
                if request.batch_stage1_done {
                    let path = run_path.join("consolidation.md");
                    match tokio::fs::metadata(&path).await {
                        Ok(_) => match tokio::fs::read_to_string(&path).await {
                            Ok(content) => {
                                total_raw_bytes =
                                    total_raw_bytes.saturating_add(content.len() as u64);
                                if total_raw_bytes > CROSS_RUN_MAX_INPUT_BYTES {
                                    return Err(
                                        "Combined cross-run input is too large. Reduce runs or shorten per-run outputs.".to_string(),
                                    );
                                }
                                run_inputs.push((run_id, content));
                                continue;
                            }
                            Err(e) => {
                                return Err(format!("Failed to read {}: {e}", path.display()));
                            }
                        },
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            return Err(format!("Failed to check {}: {e}", path.display()));
                        }
                    }
                }

                let files =
                    discover_final_outputs_async(&run_path, request.mode, &request.selected_agents)
                        .await;
                for (_, path) in &files {
                    total_raw_bytes += tokio::fs::metadata(path)
                        .await
                        .map(|meta| meta.len())
                        .unwrap_or(0);
                }
                if total_raw_bytes > CROSS_RUN_MAX_INPUT_BYTES {
                    return Err(if request.batch_stage1_done {
                        "Combined cross-run input is too large (some runs lacked per-run consolidation output). Reduce runs or shorten outputs.".to_string()
                    } else {
                        "Combined output too large. Run per-run consolidation first.".to_string()
                    });
                }

                let prompt = build_file_consolidation_prompt(&files, "", false).await?;
                run_inputs.push((run_id, prompt));
            }

            let prompt = build_cross_run_consolidation_prompt(
                &run_inputs,
                request.mode,
                &request.selected_agents,
                &request.additional,
            );
            let output_path = request.run_dir.join("cross_run_consolidation.md");
            let mut provider = provider_factory();
            let response = provider.send(&prompt).await.map_err(|e| e.to_string())?;
            tokio::fs::write(&output_path, &response.content)
                .await
                .map(|_| output_path.display().to_string())
                .map_err(|e| format!("Failed to write cross-run consolidation: {e}"))
        }
    }
}

// ---------------------------------------------------------------------------
// Diagnostics helpers
// ---------------------------------------------------------------------------

pub(crate) fn collect_report_files(run_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut dirs = vec![run_dir.to_path_buf()];
    if OutputManager::is_batch_root(run_dir) {
        dirs.extend(batch_run_directories(run_dir));
    }

    for dir in dirs {
        let mut dir_files = std::fs::read_dir(dir)
            .ok()
            .into_iter()
            .flat_map(|it| it.flatten())
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() {
                    return None;
                }
                let name = path.file_name()?.to_str()?.to_string();
                let is_md = path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("md"))
                    .unwrap_or(false);
                if is_md && name != "prompt.md" && name != "errors.md" {
                    Some(path)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        files.append(&mut dir_files);
    }
    files.sort();
    files
}

pub(crate) fn collect_application_errors(
    base_errors: &[String],
    run_dir: &std::path::Path,
) -> Vec<String> {
    let mut errors = base_errors.to_vec();

    let mut dirs = vec![run_dir.to_path_buf()];
    if OutputManager::is_batch_root(run_dir) {
        dirs.extend(batch_run_directories(run_dir));
    }

    for dir in dirs {
        let log_path = dir.join("_errors.log");
        if let Ok(content) = std::fs::read_to_string(&log_path) {
            for line in content.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    errors.push(trimmed.to_string());
                }
            }
        }
    }

    let mut seen = std::collections::HashSet::new();
    errors.retain(|e| seen.insert(e.clone()));
    errors
}

pub(crate) fn build_diagnostic_prompt(
    report_files: &[std::path::PathBuf],
    app_errors: &[String],
    use_cli: bool,
) -> Result<String, String> {
    let mut prompt = String::from(
        "Analyze all reports for OPERATIONAL errors only and produce a markdown report.\n",
    );
    let mut budget = PostRunPromptBudget::new();
    prompt.push_str(
        "Focus exclusively on errors that prevented an agent from completing its task:\n",
    );
    prompt.push_str("- API failures, timeouts, authentication errors\n");
    prompt.push_str("- CLI tool crashes, missing binaries, permission errors\n");
    prompt.push_str("- Agent permission denials (e.g. tool use blocked, sandbox restrictions, file access denied)\n");
    prompt.push_str("- Rate limits, network errors, malformed responses\n");
    prompt.push_str("- Provider returning empty or truncated output due to a fault\n\n");
    prompt.push_str("Do NOT report on:\n");
    prompt.push_str("- Quality or correctness of the agent's response content\n");
    prompt.push_str("- Whether the agent answered the user's prompt well\n");
    prompt.push_str("- Logical errors, hallucinations, or wrong answers in the output\n");
    prompt.push_str("- Style, formatting, or completeness of the response text\n\n");
    prompt.push_str("Write only the diagnostic report content.\n");
    prompt.push_str("Do not write files and do not ask for filesystem permissions.\n");
    prompt.push_str("The application will save your response to errors.md.\n\n");
    prompt.push_str(
        "Report structure:\n1) Summary\n2) Detected Issues\n3) Evidence\n4) Suggested Fixes\n\n",
    );
    prompt.push_str("If there are no operational errors, write a short summary stating all agents completed successfully.\n\n");

    prompt.push_str("Application-generated errors:\n");
    if app_errors.is_empty() {
        prompt.push_str("- none reported by application\n");
    } else {
        for err in app_errors {
            budget.add_text(err, "Application error diagnostics input")?;
            prompt.push_str("- ");
            prompt.push_str(err);
            prompt.push('\n');
        }
    }

    prompt.push_str("\nReports to analyze:\n");
    if report_files.is_empty() {
        prompt.push_str("- none\n");
        return Ok(prompt);
    }
    for path in report_files {
        prompt.push_str("- ");
        prompt.push_str(&path.display().to_string());
        prompt.push('\n');
    }

    if use_cli {
        prompt.push_str(
            "\nRead each listed file from disk before writing the report. Include permission/tool errors explicitly.\n",
        );
        return Ok(prompt);
    }

    prompt.push_str("\nReport contents:\n");
    for path in report_files {
        budget.add_file_sync(path, "Diagnostic report input")?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown.md");
        match std::fs::read_to_string(path) {
            Ok(content) => {
                prompt.push_str(&format!("\n=== BEGIN {name} ===\n"));
                prompt.push_str(&content);
                prompt.push_str(&format!("\n=== END {name} ===\n"));
            }
            Err(e) => {
                prompt.push_str(&format!("\n=== BEGIN {name} ===\n"));
                prompt.push_str(&format!("Failed to read file: {e}\n"));
                prompt.push_str(&format!("=== END {name} ===\n"));
            }
        }
    }

    Ok(prompt)
}

// ---------------------------------------------------------------------------
// Batch run directory discovery
// ---------------------------------------------------------------------------

pub(crate) fn batch_run_directories(run_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut dirs = std::fs::read_dir(run_dir)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .filter_map(|entry| {
            let path = entry.path();
            if !path.is_dir() {
                return None;
            }
            let name = path.file_name()?.to_str()?;
            if name.starts_with("run_") {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    dirs.sort();
    dirs
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn natural_cmp_basic_equal() {
        assert_eq!(natural_cmp("abc", "abc"), Ordering::Equal);
    }

    #[test]
    fn natural_cmp_numeric_ordering() {
        assert_eq!(natural_cmp("r2", "r10"), Ordering::Less);
        assert_eq!(natural_cmp("r10", "r2"), Ordering::Greater);
        assert_eq!(natural_cmp("r10", "r10"), Ordering::Equal);
    }

    #[test]
    fn natural_cmp_replica_filenames() {
        let mut names = vec![
            "block1_claude_r10_iter1.md",
            "block1_claude_r2_iter1.md",
            "block1_claude_r1_iter1.md",
            "block1_claude_r3_iter1.md",
        ];
        names.sort_by(|a, b| natural_cmp(a, b));
        assert_eq!(
            names,
            vec![
                "block1_claude_r1_iter1.md",
                "block1_claude_r2_iter1.md",
                "block1_claude_r3_iter1.md",
                "block1_claude_r10_iter1.md",
            ]
        );
    }

    #[test]
    fn natural_cmp_different_blocks_then_replicas() {
        let mut names = vec![
            "block2_gpt_r1_iter1.md",
            "block1_claude_r2_iter1.md",
            "block1_claude_r1_iter1.md",
        ];
        names.sort_by(|a, b| natural_cmp(a, b));
        assert_eq!(
            names,
            vec![
                "block1_claude_r1_iter1.md",
                "block1_claude_r2_iter1.md",
                "block2_gpt_r1_iter1.md",
            ]
        );
    }

    #[test]
    fn parse_agent_iteration_valid() {
        assert_eq!(
            parse_agent_iteration_filename("anthropic_iter3.md", "anthropic"),
            Some(3)
        );
    }

    #[test]
    fn parse_agent_iteration_wrong_agent() {
        assert_eq!(
            parse_agent_iteration_filename("openai_iter3.md", "anthropic"),
            None
        );
    }

    #[test]
    fn parse_agent_iteration_not_md() {
        assert_eq!(
            parse_agent_iteration_filename("anthropic_iter3.txt", "anthropic"),
            None
        );
    }

    #[test]
    fn parse_agent_iteration_no_number() {
        assert_eq!(
            parse_agent_iteration_filename("anthropic_iter.md", "anthropic"),
            None
        );
    }
}
