use super::diagnostics::maybe_start_diagnostics;
use super::execution::validate_agent_runtime;
use super::*;
use crate::post_run;

pub(super) fn should_offer_consolidation(app: &App) -> bool {
    if app.running.cancel_flag.load(Ordering::Relaxed) {
        return false;
    }
    if app.running.multi_run_total > 1 {
        return !successful_run_ids(app).is_empty();
    }
    if !matches!(
        app.selected_mode,
        ExecutionMode::Swarm | ExecutionMode::Pipeline
    ) {
        return false;
    }
    if app.selected_mode != ExecutionMode::Pipeline && app.selected_agents.len() <= 1 {
        return false;
    }

    let Some(run_dir) = app.running.run_dir.as_ref() else {
        return false;
    };
    post_run::discover_final_outputs(run_dir, app.selected_mode, &app.selected_agents).len() > 1
}

pub(super) fn successful_run_ids(app: &App) -> Vec<u32> {
    app.running
        .multi_run_states
        .iter()
        .filter(|state| state.status == RunStatus::Done)
        .map(|state| state.run_id)
        .collect()
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
        let result = post_run::run_consolidation_with_provider_factory(
            post_run::ConsolidationRequest {
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
