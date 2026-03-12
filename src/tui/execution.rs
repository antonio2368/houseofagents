use super::consolidation::should_offer_consolidation;
use super::diagnostics::maybe_start_diagnostics;
use super::input::{
    sync_pipeline_concurrency_buf, sync_pipeline_iterations_buf, sync_pipeline_runs_buf,
};
use super::resume::{
    find_last_complete_iteration_for_agents, find_latest_compatible_run, session_matches_resume,
};
use super::*;
use crate::runtime_support::effective_concurrency;

type BuiltExecutionOutput = (u32, Option<String>, HashMap<String, String>, OutputManager);

pub(super) fn start_pipeline_execution(app: &mut App) {
    sync_pipeline_iterations_buf(app);
    sync_pipeline_runs_buf(app);
    sync_pipeline_concurrency_buf(app);

    // Validate
    if app.pipeline.pipeline_def.blocks.is_empty() {
        app.error_modal = Some("Add at least one block before running".into());
        return;
    }
    if app.pipeline.pipeline_def.initial_prompt.trim().is_empty() {
        app.error_modal = Some("Enter an initial prompt".into());
        return;
    }
    let iterations: u32 = app.pipeline.pipeline_iterations_buf.parse().unwrap_or(0);
    if iterations < 1 {
        app.error_modal = Some("Iterations must be at least 1".into());
        return;
    }
    app.pipeline.pipeline_def.iterations = iterations;

    if let Err(e) = pipeline_mod::validate_pipeline(&app.pipeline.pipeline_def) {
        app.error_modal = Some(e.to_string());
        return;
    }

    // Check agent availability per block
    let avail_agents: std::collections::HashMap<String, bool> = app
        .available_agents()
        .into_iter()
        .map(|(a, avail)| (a.name.clone(), avail))
        .collect();
    for block in &app.pipeline.pipeline_def.blocks {
        for agent_name in &block.agents {
            match avail_agents.get(agent_name) {
                Some(true) => {}
                Some(false) => {
                    app.error_modal = Some(format!(
                        "{} is not available (block {})",
                        agent_name, block.id
                    ));
                    return;
                }
                None => {
                    app.error_modal = Some(format!(
                        "Agent '{}' not found (block {})",
                        agent_name, block.id
                    ));
                    return;
                }
            }
        }
    }

    // Set running state
    app.reset_running_state();

    // Copy prompt/session for running screen display
    app.prompt.prompt_text = app.pipeline.pipeline_def.initial_prompt.clone();
    app.prompt.session_name = app.pipeline.pipeline_session_name.clone();
    app.prompt.iterations = iterations;
    app.running.current_iteration = 1;
    app.running.final_iteration = iterations;

    // Build agent configs keyed by agent name
    let mut agent_configs: std::collections::HashMap<String, (ProviderKind, ProviderConfig, bool)> =
        std::collections::HashMap::new();

    for block in &app.pipeline.pipeline_def.blocks {
        for agent_name in &block.agents {
            if agent_configs.contains_key(agent_name) {
                continue;
            }
            if let Some(agent_cfg) = app.config.agents.iter().find(|a| a.name == *agent_name) {
                let agent_cfg = app
                    .effective_agent_config(&agent_cfg.name)
                    .unwrap_or(agent_cfg);
                agent_configs.insert(
                    agent_name.clone(),
                    (
                        agent_cfg.provider,
                        agent_cfg.to_provider_config(),
                        agent_cfg.use_cli,
                    ),
                );
            }
        }
    }

    // Pre-seed block rows from runtime replica table so IDs match the execution engine
    let rt = crate::execution::pipeline::build_runtime_table(&app.pipeline.pipeline_def);
    app.running.block_rows = rt
        .entries
        .iter()
        .map(|info| {
            let provider = agent_configs
                .get(&info.agent)
                .map(|(k, _, _)| *k)
                .unwrap_or(ProviderKind::Anthropic);
            crate::app::BlockStatusRow {
                block_id: info.runtime_id,
                source_block_id: info.source_block_id,
                replica_index: info.replica_index,
                label: info.display_label.clone(),
                agent_name: info.agent.clone(),
                provider,
                status: crate::app::AgentRowStatus::Pending,
            }
        })
        .collect();

    let loop_extra = crate::execution::pipeline::loop_extra_tasks(&app.pipeline.pipeline_def);
    app.running.expected_total_steps = (rt.entries.len() + loop_extra) * iterations as usize;

    // HTTP client
    let timeout_secs = app.effective_http_timeout_seconds().max(1);
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            app.error_modal = Some(format!("Failed to create HTTP client: {e}"));
            app.screen = Screen::Pipeline;
            app.running.is_running = false;
            return;
        }
    };

    let runs = app.pipeline.pipeline_runs.max(1);
    let concurrency = effective_concurrency(runs, app.pipeline.pipeline_concurrency);

    if runs > 1 {
        start_multi_pipeline_execution(app, client, iterations, runs, concurrency, agent_configs);
        return;
    }

    // Output — use session name (not pipeline filename) for output dir
    let session_name = if app.pipeline.pipeline_session_name.trim().is_empty() {
        None
    } else {
        Some(app.pipeline.pipeline_session_name.trim())
    };
    let base_path = app.config.resolved_output_dir();
    let output = match OutputManager::new(&base_path, session_name) {
        Ok(o) => o,
        Err(e) => {
            app.error_modal = Some(format!("Cannot create output dir: {e}"));
            app.screen = Screen::Pipeline;
            app.running.is_running = false;
            return;
        }
    };

    // Write pipeline definition snapshot
    if let Err(e) = output.write_prompt(&app.pipeline.pipeline_def.initial_prompt) {
        let _ = output.append_error(&format!("Failed to write prompt: {e}"));
    }
    if let Err(e) = output.write_pipeline_session_info(
        app.pipeline.pipeline_def.blocks.len(),
        app.pipeline.pipeline_def.connections.len(),
        app.pipeline.pipeline_def.loop_connections.len(),
        iterations,
        rt.entries.len() + loop_extra,
        app.pipeline
            .pipeline_save_path
            .as_ref()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str()),
    ) {
        let _ = output.append_error(&format!("Failed to write session info: {e}"));
    }
    // Normalize session configs before snapshotting
    app.pipeline.pipeline_def.normalize_session_configs();

    // Serialize pipeline definition
    match toml::to_string_pretty(&app.pipeline.pipeline_def) {
        Ok(toml_str) => {
            if let Err(e) = std::fs::write(output.run_dir().join("pipeline.toml"), toml_str) {
                let _ = output.append_error(&format!("Failed to write pipeline.toml: {e}"));
            }
        }
        Err(e) => {
            let _ = output.append_error(&format!("Failed to serialize pipeline: {e}"));
        }
    }

    app.running.run_dir = Some(output.run_dir().to_path_buf());

    let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();
    app.running.progress_rx = Some(progress_rx);
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    app.running.cancel_flag = cancel.clone();

    let pipeline_def = app.pipeline.pipeline_def.clone();
    let config = app.config.clone();
    let cli_timeout = app.effective_cli_timeout_seconds();
    let prompt_context = PromptRuntimeContext::new(
        pipeline_def.initial_prompt.clone(),
        app.config.diagnostic_provider.is_some(),
    );

    tokio::spawn(async move {
        let result = pipeline_mod::run_pipeline(
            &pipeline_def,
            &config,
            agent_configs,
            client,
            cli_timeout,
            &prompt_context,
            &output,
            progress_tx.clone(),
            cancel,
        )
        .await;

        if let Err(e) = result {
            let _ = progress_tx.send(ProgressEvent::AllDone);
            let _ = output.append_error(&format!("Pipeline failed: {e}"));
        }
    });
}

pub(super) fn resolve_selected_agent_configs(
    app: &App,
    agent_names: &[String],
) -> Result<Vec<AgentConfig>, String> {
    crate::runtime_support::resolve_selected_agent_configs(
        agent_names,
        &app.session_overrides,
        &app.config.agents,
        &app.cli_available,
    )
}

pub(super) fn pipeline_step_labels(def: &pipeline_mod::PipelineDefinition) -> Vec<String> {
    pipeline_mod::pipeline_step_labels(def)
}

pub(super) struct MultiExecutionParams {
    config: AppConfig,
    client: reqwest::Client,
    raw_prompt: String,
    prompt_context: PromptRuntimeContext,
    agent_names: Vec<String>,
    mode: ExecutionMode,
    iterations: u32,
    forward_prompt: bool,
    keep_session: bool,
    cli_timeout_secs: u64,
    runs: u32,
    concurrency: u32,
}

pub(super) fn start_multi_execution(app: &mut App, params: MultiExecutionParams) {
    let MultiExecutionParams {
        config,
        client,
        raw_prompt,
        prompt_context,
        agent_names,
        mode,
        iterations,
        forward_prompt,
        keep_session,
        cli_timeout_secs,
        runs,
        concurrency,
    } = params;

    let resolved_agents = match resolve_selected_agent_configs(app, &agent_names) {
        Ok(resolved) => resolved,
        Err(message) => {
            app.error_modal = Some(message);
            app.screen = Screen::Prompt;
            app.running.is_running = false;
            return;
        }
    };

    let output_dir = config.resolved_output_dir();
    let session_name = if app.prompt.session_name.trim().is_empty() {
        None
    } else {
        Some(app.prompt.session_name.trim().to_string())
    };
    let batch_root = match OutputManager::new_batch_parent(&output_dir, session_name.as_deref()) {
        Ok(output) => output,
        Err(e) => {
            app.error_modal = Some(format!("Failed to create batch output dir: {e}"));
            app.screen = Screen::Prompt;
            app.running.is_running = false;
            return;
        }
    };

    if let Err(e) = batch_root.write_batch_info(runs, concurrency, &mode, &agent_names, iterations)
    {
        app.error_modal = Some(format!("Failed to write batch metadata: {e}"));
        app.screen = Screen::Prompt;
        app.running.is_running = false;
        return;
    }

    app.running.run_dir = Some(batch_root.run_dir().clone());
    app.prompt.iterations = iterations;
    app.prompt.runs = runs;
    app.prompt.concurrency = concurrency;
    app.init_multi_run_state(runs, concurrency, agent_names.clone());

    let (batch_tx, batch_rx) = mpsc::unbounded_channel();
    app.running.batch_progress_rx = Some(batch_rx);
    let cancel = Arc::new(AtomicBool::new(false));
    app.running.cancel_flag = cancel.clone();

    let batch_root_dir = batch_root.run_dir().clone();
    let default_max_tokens = config.default_max_tokens;
    let max_history_messages = config.max_history_messages;
    let max_history_bytes = config.max_history_bytes;

    tokio::spawn(async move {
        run_multi(
            runs,
            concurrency,
            batch_tx,
            cancel,
            move |run_id, progress_tx, cancel| {
                let client = client.clone();
                let raw_prompt = raw_prompt.clone();
                let prompt_context = prompt_context.clone();
                let resolved_agents = resolved_agents.clone();
                let batch_root_dir = batch_root_dir.clone();
                let session_name = session_name.clone();
                async move {
                    let parent = match OutputManager::from_existing(batch_root_dir.clone()) {
                        Ok(parent) => parent,
                        Err(e) => {
                            return (
                                RunOutcome::Failed,
                                Some(format!("Failed to open batch root: {e}")),
                            );
                        }
                    };
                    let output = match parent.new_run_subdir(run_id) {
                        Ok(output) => output,
                        Err(e) => {
                            return (
                                RunOutcome::Failed,
                                Some(format!("Failed to create run_{run_id} output dir: {e}")),
                            );
                        }
                    };

                    if let Err(e) = output.write_prompt(&raw_prompt) {
                        let _ = output.append_error(&format!("Failed to write prompt: {e}"));
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write prompt: {e}")),
                        );
                    }

                    let run_models = resolved_agents
                        .iter()
                        .map(|cfg| {
                            (
                                cfg.name.clone(),
                                if cfg.model.trim().is_empty() {
                                    "(default)".to_string()
                                } else {
                                    cfg.model.clone()
                                },
                            )
                        })
                        .collect::<Vec<_>>();
                    let agent_info = resolved_agents
                        .iter()
                        .map(|cfg| (cfg.name.clone(), cfg.provider.config_key().to_string()))
                        .collect::<Vec<_>>();

                    if let Err(e) = output.write_session_info(
                        &mode,
                        &agent_info,
                        iterations,
                        session_name.as_deref(),
                        &run_models,
                        keep_session,
                    ) {
                        let _ = output.append_error(&format!("Failed to write session info: {e}"));
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write session info: {e}")),
                        );
                    }

                    let mut agents: Vec<(String, Box<dyn provider::Provider>)> = Vec::new();
                    let mut use_cli_by_agent = HashMap::new();
                    for agent_config in &resolved_agents {
                        use_cli_by_agent.insert(agent_config.name.clone(), agent_config.use_cli);
                        let pconfig = agent_config.to_provider_config();
                        agents.push((
                            agent_config.name.clone(),
                            provider::create_provider(
                                agent_config.provider,
                                &pconfig,
                                client.clone(),
                                default_max_tokens,
                                max_history_messages,
                                max_history_bytes,
                                cli_timeout_secs,
                            ),
                        ));
                    }

                    let result = match mode {
                        ExecutionMode::Relay => {
                            run_relay(
                                &prompt_context,
                                agents,
                                iterations,
                                1,
                                None,
                                forward_prompt,
                                keep_session,
                                use_cli_by_agent,
                                &output,
                                progress_tx,
                                cancel.clone(),
                            )
                            .await
                        }
                        ExecutionMode::Swarm => {
                            run_swarm(
                                &prompt_context,
                                agents,
                                iterations,
                                1,
                                HashMap::new(),
                                keep_session,
                                use_cli_by_agent,
                                &output,
                                progress_tx,
                                cancel.clone(),
                            )
                            .await
                        }
                        ExecutionMode::Pipeline => {
                            unreachable!("pipeline uses dedicated batch path")
                        }
                    };

                    if cancel.load(Ordering::Relaxed) {
                        return (RunOutcome::Cancelled, None);
                    }

                    match result {
                        Ok(()) => (RunOutcome::Done, None),
                        Err(e) => {
                            let err = e.to_string();
                            let _ = output.append_error(&err);
                            (RunOutcome::Failed, Some(err))
                        }
                    }
                }
            },
        )
        .await;
    });
}

pub(super) fn start_multi_pipeline_execution(
    app: &mut App,
    client: reqwest::Client,
    iterations: u32,
    runs: u32,
    concurrency: u32,
    agent_configs: HashMap<String, (ProviderKind, ProviderConfig, bool)>,
) {
    let output_dir = app.config.resolved_output_dir();
    let session_name = if app.pipeline.pipeline_session_name.trim().is_empty() {
        None
    } else {
        Some(app.pipeline.pipeline_session_name.trim().to_string())
    };
    let batch_root = match OutputManager::new_batch_parent(&output_dir, session_name.as_deref()) {
        Ok(output) => output,
        Err(e) => {
            app.error_modal = Some(format!("Cannot create batch output dir: {e}"));
            app.screen = Screen::Pipeline;
            app.running.is_running = false;
            return;
        }
    };

    let step_labels = pipeline_step_labels(&app.pipeline.pipeline_def);
    if let Err(e) = batch_root.write_batch_info(
        runs,
        concurrency,
        &ExecutionMode::Pipeline,
        &step_labels,
        iterations,
    ) {
        app.error_modal = Some(format!("Failed to write batch metadata: {e}"));
        app.screen = Screen::Pipeline;
        app.running.is_running = false;
        return;
    }

    app.running.run_dir = Some(batch_root.run_dir().clone());
    app.init_multi_run_state(runs, concurrency, step_labels);

    let config = app.config.clone();
    let cli_timeout = app.effective_cli_timeout_seconds();
    app.pipeline.pipeline_def.normalize_session_configs();
    let pipeline_def = app.pipeline.pipeline_def.clone();
    let prompt_context = PromptRuntimeContext::new(
        pipeline_def.initial_prompt.clone(),
        app.config.diagnostic_provider.is_some(),
    );
    let pipeline_source = app
        .pipeline
        .pipeline_save_path
        .as_ref()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .map(str::to_string);

    let (batch_tx, batch_rx) = mpsc::unbounded_channel();
    app.running.batch_progress_rx = Some(batch_rx);
    let cancel = Arc::new(AtomicBool::new(false));
    app.running.cancel_flag = cancel.clone();

    let batch_root_dir = batch_root.run_dir().clone();

    tokio::spawn(async move {
        run_multi(
            runs,
            concurrency,
            batch_tx,
            cancel,
            move |run_id, progress_tx, cancel| {
                let client = client.clone();
                let config = config.clone();
                let pipeline_def = pipeline_def.clone();
                let prompt_context = prompt_context.clone();
                let agent_configs = agent_configs.clone();
                let batch_root_dir = batch_root_dir.clone();
                let pipeline_source = pipeline_source.clone();
                async move {
                    let parent = match OutputManager::from_existing(batch_root_dir.clone()) {
                        Ok(parent) => parent,
                        Err(e) => {
                            return (
                                RunOutcome::Failed,
                                Some(format!("Failed to open batch root: {e}")),
                            );
                        }
                    };
                    let output = match parent.new_run_subdir(run_id) {
                        Ok(output) => output,
                        Err(e) => {
                            return (
                                RunOutcome::Failed,
                                Some(format!("Failed to create run_{run_id} output dir: {e}")),
                            );
                        }
                    };

                    if let Err(e) = output.write_prompt(&pipeline_def.initial_prompt) {
                        let _ = output.append_error(&format!("Failed to write prompt: {e}"));
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write prompt: {e}")),
                        );
                    }
                    let run_rt = crate::execution::pipeline::build_runtime_table(&pipeline_def);
                    let run_loop_extra =
                        crate::execution::pipeline::loop_extra_tasks(&pipeline_def);
                    if let Err(e) = output.write_pipeline_session_info(
                        pipeline_def.blocks.len(),
                        pipeline_def.connections.len(),
                        pipeline_def.loop_connections.len(),
                        iterations,
                        run_rt.entries.len() + run_loop_extra,
                        pipeline_source.as_deref(),
                    ) {
                        let _ = output.append_error(&format!("Failed to write session info: {e}"));
                        return (
                            RunOutcome::Failed,
                            Some(format!("Failed to write session info: {e}")),
                        );
                    }
                    match toml::to_string_pretty(&pipeline_def) {
                        Ok(toml_str) => {
                            if let Err(e) =
                                std::fs::write(output.run_dir().join("pipeline.toml"), toml_str)
                            {
                                let _ = output
                                    .append_error(&format!("Failed to write pipeline.toml: {e}"));
                                return (
                                    RunOutcome::Failed,
                                    Some(format!("Failed to write pipeline.toml: {e}")),
                                );
                            }
                        }
                        Err(e) => {
                            let err = format!("Failed to serialize pipeline: {e}");
                            let _ = output.append_error(&err);
                            return (RunOutcome::Failed, Some(err));
                        }
                    }

                    let result = pipeline_mod::run_pipeline(
                        &pipeline_def,
                        &config,
                        agent_configs,
                        client,
                        cli_timeout,
                        &prompt_context,
                        &output,
                        progress_tx,
                        cancel.clone(),
                    )
                    .await;

                    if cancel.load(Ordering::Relaxed) {
                        return (RunOutcome::Cancelled, None);
                    }

                    match result {
                        Ok(()) => (RunOutcome::Done, None),
                        Err(e) => {
                            let err = format!("Pipeline failed: {e}");
                            let _ = output.append_error(&err);
                            (RunOutcome::Failed, Some(err))
                        }
                    }
                }
            },
        )
        .await;
    });
}

pub(super) fn start_execution(app: &mut App) {
    app.reset_running_state();

    let config = app.config.clone();
    let http_timeout_secs = app.effective_http_timeout_seconds().max(1);
    let cli_timeout_secs = app.effective_cli_timeout_seconds().max(1);
    let runs = app.prompt.runs.max(1);
    let concurrency = effective_concurrency(runs, app.prompt.concurrency);
    let raw_prompt = app.prompt.prompt_text.clone();
    let session_name = if app.prompt.session_name.trim().is_empty() {
        None
    } else {
        Some(app.prompt.session_name.trim().to_string())
    };
    let prompt_context =
        PromptRuntimeContext::new(raw_prompt.clone(), app.config.diagnostic_provider.is_some());
    let agent_names = app.selected_agents.clone();
    let mode = app.selected_mode;
    let iterations = app.prompt.iterations;

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(http_timeout_secs))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            fail_execution_setup(app, format!("Failed to create HTTP client: {e}"));
            return;
        }
    };

    if runs > 1 {
        if app.prompt.resume_previous && matches!(mode, ExecutionMode::Relay | ExecutionMode::Swarm)
        {
            fail_execution_setup(
                app,
                "Resume is only supported for single-run execution".into(),
            );
            return;
        }
        start_multi_execution(
            app,
            MultiExecutionParams {
                config,
                client,
                raw_prompt,
                prompt_context,
                agent_names,
                mode,
                iterations,
                forward_prompt: app.prompt.forward_prompt,
                keep_session: app.prompt.keep_session,
                cli_timeout_secs,
                runs,
                concurrency,
            },
        );
        return;
    }

    let pending = crate::app::PendingSingleExecution {
        config: config.clone(),
        client,
        raw_prompt,
        session_name,
        prompt_context,
        agent_names: agent_names.clone(),
        mode,
        forward_prompt: app.prompt.forward_prompt,
        keep_session: app.prompt.keep_session,
        iterations,
        cli_timeout_secs,
    };

    if app.prompt.resume_previous && matches!(mode, ExecutionMode::Relay | ExecutionMode::Swarm) {
        let output_dir = config.resolved_output_dir();
        let session_name = pending.session_name.clone();
        let agent_names = pending.agent_names.clone();
        let keep_session = pending.keep_session;

        app.running.pending_single_execution = Some(pending);
        app.running.resume_prepare_rx = None;
        app.record_progress(ProgressEvent::AgentLog {
            agent: agent_names
                .first()
                .cloned()
                .unwrap_or_else(|| "resume".into()),
            kind: ProviderKind::Anthropic,
            iteration: 0,
            message: "preparing resume state".into(),
        });

        let (tx, rx) = mpsc::unbounded_channel();
        app.running.resume_prepare_rx = Some(rx);
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                prepare_resume_execution(output_dir, session_name, mode, agent_names, keep_session)
            })
            .await;

            let final_result = match result {
                Ok(inner) => inner,
                Err(e) => Err(format!("Resume preparation task failed: {e}")),
            };
            let _ = tx.send(final_result);
        });
        return;
    }

    continue_single_execution(app, pending, None);
}

pub(super) fn handle_resume_preparation_result(
    app: &mut App,
    result: Result<crate::app::ResumePreparation, String>,
) {
    app.running.resume_prepare_rx = None;
    let Some(pending) = app.running.pending_single_execution.take() else {
        return;
    };

    match result {
        Ok(prepared) => continue_single_execution(app, pending, Some(prepared)),
        Err(message) => fail_execution_setup(app, message),
    }
}

fn continue_single_execution(
    app: &mut App,
    pending: crate::app::PendingSingleExecution,
    resume: Option<crate::app::ResumePreparation>,
) {
    let mut agents: Vec<(String, Box<dyn provider::Provider>)> = Vec::new();
    let mut use_cli_by_agent = HashMap::new();
    let mut run_models = Vec::new();
    let mut agent_info = Vec::new();
    let mut fallback_agent_kind = None;

    for name in &pending.agent_names {
        let agent_config = match app.effective_agent_config(name).cloned() {
            Some(cfg) => cfg,
            None => {
                fail_execution_setup(app, format!("{name} is not configured"));
                return;
            }
        };

        if let Err(message) = validate_agent_runtime(app, name, &agent_config) {
            fail_execution_setup(app, message);
            return;
        }

        fallback_agent_kind.get_or_insert(agent_config.provider);
        run_models.push((
            name.clone(),
            if agent_config.model.trim().is_empty() {
                "(default)".to_string()
            } else {
                agent_config.model.clone()
            },
        ));
        agent_info.push((name.clone(), agent_config.provider.config_key().to_string()));
        app.running.agent_rows.push(crate::app::AgentStatusRow {
            name: name.clone(),
            provider: agent_config.provider,
            status: crate::app::AgentRowStatus::Pending,
        });
        use_cli_by_agent.insert(name.clone(), agent_config.use_cli);

        let pconfig = agent_config.to_provider_config();
        agents.push((
            name.clone(),
            provider::create_provider(
                agent_config.provider,
                &pconfig,
                pending.client.clone(),
                pending.config.default_max_tokens,
                pending.config.max_history_messages,
                pending.config.max_history_bytes,
                pending.cli_timeout_secs,
            ),
        ));
    }

    let resumed_run = resume.is_some();
    let (start_iteration, relay_initial_last_output, swarm_initial_outputs, output) =
        match build_execution_output(app, &pending, resume, &agent_info, &run_models) {
            Ok(data) => data,
            Err(message) => {
                fail_execution_setup(app, message);
                return;
            }
        };

    app.running.current_iteration = start_iteration;
    app.running.final_iteration = start_iteration + pending.iterations - 1;
    app.running.run_dir = Some(output.run_dir().clone());
    let (tx, rx) = mpsc::unbounded_channel::<ProgressEvent>();
    let cancel = Arc::new(AtomicBool::new(false));
    app.running.progress_rx = Some(rx);
    app.running.cancel_flag = cancel.clone();
    let fallback_agent_kind = fallback_agent_kind.unwrap_or(ProviderKind::Anthropic);
    let mode = pending.mode;
    let iterations = pending.iterations;
    let forward_prompt = pending.forward_prompt;
    let keep_session = pending.keep_session;
    let agent_names = pending.agent_names.clone();
    let prompt_context = pending.prompt_context.clone();

    if resumed_run {
        app.record_progress(ProgressEvent::AgentLog {
            agent: agent_names
                .first()
                .cloned()
                .unwrap_or_else(|| "resume".into()),
            kind: fallback_agent_kind,
            iteration: 0,
            message: format!(
                "resuming from iteration {}",
                start_iteration.saturating_sub(1)
            ),
        });
    }

    tokio::spawn(async move {
        let result = match mode {
            ExecutionMode::Relay => {
                run_relay(
                    &prompt_context,
                    agents,
                    iterations,
                    start_iteration,
                    relay_initial_last_output,
                    forward_prompt,
                    keep_session,
                    use_cli_by_agent.clone(),
                    &output,
                    tx.clone(),
                    cancel,
                )
                .await
            }
            ExecutionMode::Swarm => {
                run_swarm(
                    &prompt_context,
                    agents,
                    iterations,
                    start_iteration,
                    swarm_initial_outputs,
                    keep_session,
                    use_cli_by_agent,
                    &output,
                    tx.clone(),
                    cancel,
                )
                .await
            }
            ExecutionMode::Pipeline => return,
        };
        handle_execution_task_result(
            &tx,
            result,
            agent_names.first().cloned().unwrap_or_default(),
            fallback_agent_kind,
        );
    });
}

fn build_execution_output(
    _app: &mut App,
    pending: &crate::app::PendingSingleExecution,
    resume: Option<crate::app::ResumePreparation>,
    agent_info: &[(String, String)],
    run_models: &[(String, String)],
) -> Result<BuiltExecutionOutput, String> {
    if let Some(resume) = resume {
        let output = OutputManager::from_existing(resume.run_dir.clone())
            .map_err(|e| format!("Failed to open existing run dir: {e}"))?;
        output
            .append_error(&format!(
                "Resumed {} mode for {} additional iteration(s), starting at iter {}",
                pending.mode, pending.iterations, resume.start_iteration
            ))
            .map_err(|e| format!("Failed to write resume log entry: {e}"))?;
        return Ok((
            resume.start_iteration,
            resume.relay_initial_last_output,
            resume.swarm_initial_outputs,
            output,
        ));
    }

    let output_dir = pending.config.resolved_output_dir();
    let output = OutputManager::new(&output_dir, pending.session_name.as_deref())
        .map_err(|e| format!("Failed to create output dir: {e}"))?;
    output
        .write_prompt(&pending.raw_prompt)
        .map_err(|e| format!("Failed to write prompt file: {e}"))?;
    output
        .write_session_info(
            &pending.mode,
            agent_info,
            pending.iterations,
            pending.session_name.as_deref(),
            run_models,
            pending.keep_session,
        )
        .map_err(|e| format!("Failed to write session metadata: {e}"))?;
    Ok((1, None, HashMap::new(), output))
}

fn prepare_resume_execution(
    output_dir: std::path::PathBuf,
    session_name: Option<String>,
    mode: ExecutionMode,
    agent_names: Vec<String>,
    keep_session: bool,
) -> Result<crate::app::ResumePreparation, String> {
    let run_dir = if let Some(name) = session_name.as_deref().filter(|n| !n.trim().is_empty()) {
        match OutputManager::find_latest_session_run(&output_dir, name.trim()) {
            Ok(Some(path)) => path,
            Ok(None) => {
                return Err(format!(
                    "No previous run found for session '{}'",
                    name.trim()
                ));
            }
            Err(e) => return Err(format!("Failed to search previous runs: {e}")),
        }
    } else {
        find_latest_compatible_run(&output_dir, mode, &agent_names, keep_session)
            .ok_or_else(|| "No compatible previous run found to resume".to_string())?
    };

    let session_info = OutputManager::read_agent_session_info(&run_dir)
        .map_err(|e| format!("Failed to read session metadata: {e}"))?;
    if !session_matches_resume(&session_info, mode, &agent_names, keep_session) {
        return Err(format!(
            "Previous run at {} does not exactly match the selected {} configuration",
            run_dir.display(),
            mode
        ));
    }
    let last_iteration = find_last_complete_iteration_for_agents(&run_dir, &agent_names)
        .filter(|iteration| *iteration >= 1)
        .ok_or_else(|| "No previous iteration files found to resume".to_string())?;

    let mut relay_initial_last_output = None;
    let mut swarm_initial_outputs = HashMap::new();
    match mode {
        ExecutionMode::Relay => {
            let last_agent = agent_names
                .last()
                .ok_or_else(|| "No agents selected".to_string())?;
            let file_key = App::agent_file_key(last_agent);
            let prev_path = run_dir.join(format!("{file_key}_iter{last_iteration}.md"));
            relay_initial_last_output = Some(std::fs::read_to_string(&prev_path).map_err(|e| {
                format!(
                    "Failed to read previous relay output: {} ({e})",
                    prev_path.display()
                )
            })?);
        }
        ExecutionMode::Swarm => {
            for agent_name in &agent_names {
                let file_key = App::agent_file_key(agent_name);
                let prev_path = run_dir.join(format!("{file_key}_iter{last_iteration}.md"));
                if let Ok(content) = std::fs::read_to_string(&prev_path) {
                    swarm_initial_outputs.insert(agent_name.clone(), content);
                }
            }
            if swarm_initial_outputs.is_empty() {
                return Err("No previous swarm outputs found to resume".into());
            }
        }
        ExecutionMode::Pipeline => {}
    }

    Ok(crate::app::ResumePreparation {
        run_dir,
        start_iteration: last_iteration + 1,
        relay_initial_last_output,
        swarm_initial_outputs,
    })
}

fn fail_execution_setup(app: &mut App, message: String) {
    app.error_modal = Some(message);
    app.screen = Screen::Prompt;
    app.running.is_running = false;
    app.running.progress_rx = None;
    app.running.resume_prepare_rx = None;
    app.running.pending_single_execution = None;
}

pub(super) fn handle_execution_task_result(
    tx: &mpsc::UnboundedSender<ProgressEvent>,
    result: Result<(), crate::error::AppError>,
    agent_name: String,
    kind: ProviderKind,
) {
    if let Err(e) = result {
        let err_str = e.to_string();
        let _ = tx.send(ProgressEvent::AgentError {
            agent: agent_name,
            kind,
            iteration: 0,
            error: err_str.clone(),
            details: Some(err_str),
        });
        let _ = tx.send(ProgressEvent::AllDone);
    }
}

pub(super) fn validate_agent_runtime(
    app: &App,
    agent_label: &str,
    agent_config: &AgentConfig,
) -> Result<(), String> {
    crate::runtime_support::validate_agent_runtime(&app.cli_available, agent_label, agent_config)
}

pub(super) fn handle_progress(app: &mut App, event: ProgressEvent) {
    let is_done = matches!(event, ProgressEvent::AllDone);
    app.record_progress(event);
    if is_done {
        app.running.is_running = false;
        app.running.progress_rx = None;
        if should_offer_consolidation(app) {
            app.running.consolidation_active = true;
            app.running.consolidation_phase = ConsolidationPhase::Confirm;
            app.running.consolidation_provider_cursor = 0;
            app.running.consolidation_prompt.clear();
            app.running.consolidation_running = false;
            app.running.consolidation_rx = None;
        } else {
            maybe_start_diagnostics(app);
        }
    }
}

pub(super) fn handle_batch_progress(app: &mut App, event: BatchProgressEvent) {
    match event {
        BatchProgressEvent::RunQueued { run_id } => {
            if let Some(state) = multi_run_state_mut(app, run_id) {
                state.status = RunStatus::Queued;
            }
        }
        BatchProgressEvent::RunStarted { run_id } => {
            if let Some(state) = multi_run_state_mut(app, run_id) {
                state.status = RunStatus::Running;
                for step in &mut state.steps {
                    if step.status == RunStepStatus::Queued {
                        step.status = RunStepStatus::Pending;
                    }
                }
                state.push_log("run started".into());
            }
        }
        BatchProgressEvent::RunEvent { run_id, event } => {
            update_multi_run_state(app, run_id, &event);
            if matches!(
                event,
                ProgressEvent::AgentError { .. } | ProgressEvent::BlockError { .. }
            ) {
                app.record_progress(event);
            }
        }
        BatchProgressEvent::RunFinished {
            run_id,
            outcome,
            error,
        } => {
            if let Some(state) = multi_run_state_mut(app, run_id) {
                match outcome {
                    RunOutcome::Done => {
                        if state.status != RunStatus::Failed {
                            state.status = RunStatus::Done;
                        }
                    }
                    RunOutcome::Failed => {
                        state.status = RunStatus::Failed;
                        if let Some(ref message) = error {
                            state.error = Some(message.clone());
                            state.push_log(format!("error: {message}"));
                        }
                    }
                    RunOutcome::Cancelled => {
                        state.status = RunStatus::Cancelled;
                        state.push_log("cancelled".into());
                    }
                }
                // Mark any remaining Queued/Pending steps as terminal so
                // abandoned loop passes don't look perpetually unfinished.
                // Use state.status (which preserves Failed from BlockError
                // events) rather than outcome (which may be Done even when
                // blocks failed).
                let terminal_status = match state.status {
                    RunStatus::Done => RunStepStatus::Done,
                    RunStatus::Failed | RunStatus::Cancelled => RunStepStatus::Error,
                    // Queued/Running shouldn't happen here, but be safe.
                    RunStatus::Queued | RunStatus::Running => RunStepStatus::Done,
                };
                for step in &mut state.steps {
                    if matches!(step.status, RunStepStatus::Queued | RunStepStatus::Pending) {
                        step.status = terminal_status;
                    }
                }
            }
        }
        BatchProgressEvent::AllRunsDone => {
            app.running.is_running = false;
            app.running.batch_progress_rx = None;
            if should_offer_consolidation(app) {
                app.running.consolidation_active = true;
                app.running.consolidation_phase = ConsolidationPhase::Confirm;
                app.running.consolidation_target = ConsolidationTarget::PerRun;
                app.running.consolidation_provider_cursor = 0;
                app.running.consolidation_prompt.clear();
                app.running.consolidation_running = false;
                app.running.consolidation_rx = None;
            } else {
                maybe_start_diagnostics(app);
            }
        }
    }
}

pub(super) fn multi_run_state_mut(app: &mut App, run_id: u32) -> Option<&mut RunState> {
    let index = run_id.checked_sub(1)? as usize;
    if app
        .running
        .multi_run_states
        .get(index)
        .is_some_and(|state| state.run_id == run_id)
    {
        return app.running.multi_run_states.get_mut(index);
    }

    app.running
        .multi_run_states
        .iter_mut()
        .find(|state| state.run_id == run_id)
}

pub(super) fn update_multi_run_state(app: &mut App, run_id: u32, event: &ProgressEvent) {
    let Some(state) = multi_run_state_mut(app, run_id) else {
        return;
    };

    match event {
        ProgressEvent::AgentStarted { agent, .. } => {
            state.status = RunStatus::Running;
            update_step_status(state, agent, RunStepStatus::Running);
            state.push_log(format!("{agent}: started"));
        }
        ProgressEvent::AgentLog { agent, message, .. } => {
            state.push_log(format!("{agent}: {message}"));
        }
        ProgressEvent::AgentFinished { agent, .. } => {
            update_step_status(state, agent, RunStepStatus::Done);
            state.push_log(format!("{agent}: finished"));
        }
        ProgressEvent::AgentError { agent, error, .. } => {
            state.status = RunStatus::Failed;
            state.error = Some(error.clone());
            update_step_status(state, agent, RunStepStatus::Error);
            state.push_log(format!("{agent}: {error}"));
        }
        ProgressEvent::IterationComplete { iteration } => {
            state.push_log(format!("iteration {iteration} complete"));
        }
        ProgressEvent::BlockStarted {
            block_id,
            agent_name,
            label,
            loop_pass,
            ..
        } => {
            let step = resolve_block_step(state, *block_id, label, agent_name, *loop_pass);
            state.status = RunStatus::Running;
            update_step_status(state, &step, RunStepStatus::Running);
            state.push_log(format!("{step}: started"));
        }
        ProgressEvent::BlockLog {
            block_id,
            agent_name,
            loop_pass,
            message,
            ..
        } => {
            let step = resolve_block_step(state, *block_id, "", agent_name, *loop_pass);
            state.push_log(format!("{step}: {message}"));
        }
        ProgressEvent::BlockFinished {
            block_id,
            agent_name,
            label,
            loop_pass,
            ..
        } => {
            let step = resolve_block_step(state, *block_id, label, agent_name, *loop_pass);
            update_step_status(state, &step, RunStepStatus::Done);
            state.push_log(format!("{step}: finished"));
        }
        ProgressEvent::BlockError {
            block_id,
            agent_name,
            label,
            loop_pass,
            error,
            ..
        }
        | ProgressEvent::BlockSkipped {
            block_id,
            agent_name,
            label,
            loop_pass,
            reason: error,
            ..
        } => {
            let step = resolve_block_step(state, *block_id, label, agent_name, *loop_pass);
            state.status = RunStatus::Failed;
            state.error = Some(error.clone());
            update_step_status(state, &step, RunStepStatus::Error);
            state.push_log(format!("{step}: {error}"));
        }
        ProgressEvent::AgentStreamChunk { .. } => {}
        ProgressEvent::BlockStreamChunk { .. } => {}
        ProgressEvent::AllDone => {}
    }
}

pub(super) fn update_step_status(state: &mut RunState, label: &str, status: RunStepStatus) {
    if let Some(step) = state.steps.iter_mut().find(|step| step.label == label) {
        step.status = status;
    }
}

pub(super) fn format_block_step_label(block_id: u32, label: &str, agent_name: &str) -> String {
    pipeline_mod::format_block_step_label(block_id, label, agent_name)
}

fn format_block_step_label_with_pass(
    block_id: u32,
    label: &str,
    agent_name: &str,
    pass: u32,
) -> String {
    pipeline_mod::format_block_step_label_with_pass(block_id, label, agent_name, pass)
}

/// Resolve the step label for a block event, preferring the pass-specific
/// label when available (loop blocks) and falling back to the base label.
fn resolve_block_step(
    state: &crate::app::RunState,
    block_id: u32,
    label: &str,
    agent_name: &str,
    loop_pass: u32,
) -> String {
    let pass_label = format_block_step_label_with_pass(block_id, label, agent_name, loop_pass);
    if state.steps.iter().any(|s| s.label == pass_label) {
        pass_label
    } else {
        format_block_step_label(block_id, label, agent_name)
    }
}
