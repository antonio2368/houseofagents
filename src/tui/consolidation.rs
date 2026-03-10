use super::diagnostics::maybe_start_diagnostics;
use super::execution::validate_agent_runtime;
use super::resume::{
    find_last_iteration, find_last_iteration_async, parse_pipeline_iteration_filename,
};
use super::*;

/// Natural (numeric-aware) comparison for filenames so that e.g. `_r2` sorts
/// before `_r10` instead of lexicographic `_r1, _r10, ..., _r2`.
fn natural_cmp(a: &str, b: &str) -> std::cmp::Ordering {
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

pub(super) fn should_offer_consolidation(app: &App) -> bool {
    if app.running.cancel_flag.load(Ordering::Relaxed) {
        return false;
    }
    if app.running.multi_run_total > 1 {
        return !successful_run_ids(app).is_empty();
    }
    if app.selected_agents.len() <= 1 {
        return false;
    }
    if !matches!(
        app.selected_mode,
        ExecutionMode::Swarm | ExecutionMode::Pipeline
    ) {
        return false;
    }

    let Some(run_dir) = app.running.run_dir.as_ref() else {
        return false;
    };
    discover_final_outputs(run_dir, app.selected_mode, &app.selected_agents).len() > 1
}

pub(super) fn successful_run_ids(app: &App) -> Vec<u32> {
    app.running
        .multi_run_states
        .iter()
        .filter(|state| state.status == RunStatus::Done)
        .map(|state| state.run_id)
        .collect()
}

/// Given a list of `(filename, path)` pairs, keep only the highest `_loop{N}`
/// variant per stem. Files without a `_loop{N}` suffix pass through unchanged.
fn keep_highest_loop_pass(
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
                let entry = best.entry(base).or_insert((0, String::new(), std::path::PathBuf::new()));
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

pub(super) fn discover_final_outputs(
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
        .map(|n| App::agent_file_key(n))
        .collect::<Vec<_>>();
    let Some(last_iteration) = find_last_iteration(run_dir, &agent_keys) else {
        return Vec::new();
    };

    let mut files = Vec::new();
    for name in selected_agents {
        let file_key = App::agent_file_key(name);
        let path = run_dir.join(format!("{file_key}_iter{last_iteration}.md"));
        if path.exists() {
            files.push((name.clone(), path));
        }
    }
    files
}

pub(super) async fn discover_final_outputs_async(
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
        .map(|n| App::agent_file_key(n))
        .collect::<Vec<_>>();
    let Some(last_iteration) = find_last_iteration_async(run_dir, &agent_keys).await else {
        return Vec::new();
    };

    let mut files = Vec::new();
    for name in selected_agents {
        let file_key = App::agent_file_key(name);
        let path = run_dir.join(format!("{file_key}_iter{last_iteration}.md"));
        if tokio::fs::metadata(&path).await.is_ok() {
            files.push((name.clone(), path));
        }
    }
    files
}

pub(super) fn selected_consolidation_agent(app: &App) -> Result<(String, AgentConfig), String> {
    let agent_name = app
        .config
        .agents
        .get(app.running.consolidation_provider_cursor)
        .map(|a| a.name.clone())
        .ok_or_else(|| "Select an agent for consolidation".to_string())?;
    let agent_config = app
        .effective_agent_config(&agent_name)
        .cloned()
        .ok_or_else(|| format!("{agent_name} is not configured"))?;
    validate_agent_runtime(app, &agent_name, &agent_config)?;
    Ok((agent_name, agent_config))
}

pub(super) const POST_RUN_SYNTHESIS_MAX_INPUT_BYTES: u64 = 200 * 1024;
pub(super) const CROSS_RUN_MAX_INPUT_BYTES: u64 = POST_RUN_SYNTHESIS_MAX_INPUT_BYTES;

pub(super) struct PostRunPromptBudget {
    used_bytes: u64,
}

impl PostRunPromptBudget {
    pub(super) fn new() -> Self {
        Self { used_bytes: 0 }
    }

    pub(super) fn add_bytes(&mut self, bytes: u64, context: &str) -> Result<(), String> {
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        if self.used_bytes > POST_RUN_SYNTHESIS_MAX_INPUT_BYTES {
            return Err(format!(
                "{context} is too large to inline into a post-run synthesis prompt (limit: {} KB). Reduce the number of reports or shorten the outputs.",
                POST_RUN_SYNTHESIS_MAX_INPUT_BYTES / 1024
            ));
        }
        Ok(())
    }

    pub(super) fn add_text(&mut self, text: &str, context: &str) -> Result<(), String> {
        self.add_bytes(text.len() as u64, context)
    }

    pub(super) async fn add_file_async(
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

    pub(super) fn add_file_sync(
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

pub(super) async fn build_file_consolidation_prompt(
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

pub(super) fn build_cross_run_consolidation_prompt(
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
pub(super) struct ConsolidationRequest {
    run_dir: std::path::PathBuf,
    target: ConsolidationTarget,
    mode: ExecutionMode,
    selected_agents: Vec<String>,
    successful_runs: Vec<u32>,
    batch_stage1_done: bool,
    additional: String,
    agent_name: String,
    agent_use_cli: bool,
}

impl ConsolidationRequest {
    #[cfg(test)]
    pub(super) fn new(
        run_dir: std::path::PathBuf,
        target: ConsolidationTarget,
        mode: ExecutionMode,
        selected_agents: Vec<String>,
        successful_runs: Vec<u32>,
        batch_stage1_done: bool,
        additional: String,
        agent_name: String,
        agent_use_cli: bool,
    ) -> Self {
        Self {
            run_dir,
            target,
            mode,
            selected_agents,
            successful_runs,
            batch_stage1_done,
            additional,
            agent_name,
            agent_use_cli,
        }
    }
}

pub(super) async fn run_consolidation_with_provider_factory<F>(
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
            let file_key = App::agent_file_key(&request.agent_name);
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
                if !request.batch_stage1_done && total_raw_bytes > CROSS_RUN_MAX_INPUT_BYTES {
                    return Err(
                        "Combined output too large. Run per-run consolidation first.".to_string(),
                    );
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

pub(super) fn start_consolidation(app: &mut App) {
    if app.running.consolidation_running {
        return;
    }

    let run_dir = match app.running.run_dir.clone() {
        Some(path) => path,
        None => {
            app.error_modal = Some("No run directory found for consolidation".into());
            return;
        }
    };

    let (agent_name, agent_config) = match selected_consolidation_agent(app) {
        Ok(result) => result,
        Err(message) => {
            app.error_modal = Some(message);
            return;
        }
    };

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(
            app.effective_http_timeout_seconds().max(1),
        ))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            app.error_modal = Some(format!("Failed to create HTTP client: {e}"));
            return;
        }
    };

    let provider_kind = agent_config.provider;
    let provider_config = agent_config.to_provider_config();
    let additional = app.running.consolidation_prompt.trim().to_string();

    app.record_progress(ProgressEvent::AgentStarted {
        agent: agent_name.clone(),
        kind: provider_kind,
        iteration: 0,
    });
    app.record_progress(ProgressEvent::AgentLog {
        agent: agent_name.clone(),
        kind: provider_kind,
        iteration: 0,
        message: "consolidating reports".into(),
    });

    let (tx, rx) = mpsc::unbounded_channel();
    app.running.consolidation_rx = Some(rx);
    app.running.consolidation_running = true;
    app.running.consolidation_active = false;
    app.running.is_running = true;

    let mode = app.selected_mode;
    let selected_agents = app.selected_agents.clone();
    let successful_runs = successful_run_ids(app);
    let target = app.running.consolidation_target;
    let batch_stage1_done = app.running.batch_stage1_done;
    let default_max_tokens = app.config.default_max_tokens;
    let max_history_messages = app.config.max_history_messages;
    let max_history_bytes = app.config.max_history_bytes;
    let cli_timeout = app.effective_cli_timeout_seconds().max(1);

    tokio::spawn(async move {
        let result = run_consolidation_with_provider_factory(
            ConsolidationRequest {
                run_dir,
                target,
                mode,
                selected_agents,
                successful_runs,
                batch_stage1_done,
                additional,
                agent_name,
                agent_use_cli: agent_config.use_cli,
            },
            move || {
                provider::create_provider(
                    provider_kind,
                    &provider_config,
                    client.clone(),
                    default_max_tokens,
                    max_history_messages,
                    max_history_bytes,
                    cli_timeout,
                )
            },
        )
        .await;
        let _ = tx.send(result);
    });
}

pub(super) fn handle_consolidation_result(app: &mut App, result: Result<String, String>) {
    app.running.consolidation_running = false;
    app.running.is_running = false;
    app.running.consolidation_rx = None;

    let agent_name = app
        .config
        .agents
        .get(app.running.consolidation_provider_cursor)
        .map(|a| a.name.clone())
        .unwrap_or_default();
    let kind = app
        .effective_agent_config(&agent_name)
        .map(|a| a.provider)
        .unwrap_or(ProviderKind::Anthropic);

    match result {
        Ok(path) => {
            app.record_progress(ProgressEvent::AgentFinished {
                agent: agent_name.clone(),
                kind,
                iteration: 0,
            });
            app.record_progress(ProgressEvent::AgentLog {
                agent: agent_name,
                kind,
                iteration: 0,
                message: format!("Consolidation saved to {path}"),
            });

            if app.running.consolidation_target == ConsolidationTarget::PerRun {
                app.running.batch_stage1_done = true;
                app.running.consolidation_active = true;
                app.running.consolidation_phase = ConsolidationPhase::CrossRunConfirm;
                app.running.consolidation_target = ConsolidationTarget::AcrossRuns;
                app.running.consolidation_prompt.clear();
            } else {
                app.running.consolidation_active = false;
                maybe_start_diagnostics(app);
            }
        }
        Err(e) => {
            app.record_progress(ProgressEvent::AgentError {
                agent: agent_name,
                kind,
                iteration: 0,
                error: e.clone(),
                details: Some(e),
            });
            app.running.consolidation_active = false;
            maybe_start_diagnostics(app);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::natural_cmp;
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
}
