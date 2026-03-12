use super::execution::validate_agent_runtime;
use super::*;
use crate::post_run;

pub(super) fn maybe_start_diagnostics(app: &mut App) {
    if app.running.cancel_flag.load(Ordering::Relaxed)
        || app.running.diagnostic_running
        || app.running.diagnostic_rx.is_some()
    {
        return;
    }

    let diag_agent_name = match app.config.diagnostic_provider.as_deref() {
        Some(name) => name.to_string(),
        None => return,
    };
    let run_dir = match app.running.run_dir.clone() {
        Some(path) => path,
        None => return,
    };
    let agent_config = match app.effective_agent_config(&diag_agent_name).cloned() {
        Some(cfg) => cfg,
        None => {
            app.error_modal = Some(format!(
                "Diagnostic agent '{}' is not configured",
                diag_agent_name
            ));
            return;
        }
    };
    let diagnostic_kind = agent_config.provider;
    if let Err(message) = validate_agent_runtime(
        app,
        &format!("Diagnostic agent '{}'", diag_agent_name),
        &agent_config,
    ) {
        app.error_modal = Some(message);
        return;
    }
    let pconfig = agent_config.to_provider_config();
    let base_errors = app.error_ledger().iter().cloned().collect::<Vec<_>>();

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
    let provider = provider::create_provider(
        diagnostic_kind,
        &pconfig,
        client,
        app.config.default_max_tokens,
        app.config.max_history_messages,
        app.config.max_history_bytes,
        app.effective_cli_timeout_seconds().max(1),
    );

    app.record_progress(ProgressEvent::AgentLog {
        agent: diag_agent_name,
        kind: diagnostic_kind,
        iteration: 0,
        message: "analyzing reports for errors".into(),
    });
    app.running.diagnostic_running = true;
    app.running.is_running = true;

    let output_path = run_dir.join("errors.md");
    let (tx, rx) = mpsc::unbounded_channel();
    app.running.diagnostic_rx = Some(rx);

    tokio::spawn(async move {
        let mut provider = provider;
        let prompt_result = tokio::task::spawn_blocking(move || {
            let report_files = post_run::collect_report_files(&run_dir);
            let app_errors = post_run::collect_application_errors(&base_errors, &run_dir);
            post_run::build_diagnostic_prompt(&report_files, &app_errors, pconfig.use_cli)
        })
        .await;

        let result = match prompt_result {
            Ok(Ok(prompt)) => match provider.send(&prompt).await {
                Ok(resp) => match tokio::fs::write(&output_path, &resp.content).await {
                    Ok(()) => Ok(output_path.display().to_string()),
                    Err(e) => Err(format!("Failed to write errors.md: {e}")),
                },
                Err(e) => Err(e.to_string()),
            },
            Ok(Err(error)) => Err(error),
            Err(e) => Err(format!("Diagnostic preparation task failed: {e}")),
        };
        let _ = tx.send(result);
    });
}

pub(super) fn handle_diagnostic_result(app: &mut App, result: Result<String, String>) {
    app.running.diagnostic_running = false;
    app.running.is_running = false;
    app.running.diagnostic_rx = None;

    let agent_name = app
        .config
        .diagnostic_provider
        .clone()
        .unwrap_or_else(|| "diagnostics".into());
    let kind = app
        .effective_agent_config(&agent_name)
        .map(|a| a.provider)
        .unwrap_or(ProviderKind::Anthropic);

    match result {
        Ok(path) => {
            app.record_progress(ProgressEvent::AgentLog {
                agent: agent_name,
                kind,
                iteration: 0,
                message: format!("Diagnostic report saved to {path}"),
            });
        }
        Err(e) => {
            app.record_progress(ProgressEvent::AgentError {
                agent: agent_name,
                kind,
                iteration: 0,
                error: e.clone(),
                details: Some(e),
            });
        }
    }
}
