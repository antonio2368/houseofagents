use super::execution::validate_agent_runtime;
use super::*;
use crate::execution::pipeline::{
    self as pipeline_mod, build_runtime_table, compute_loop_sub_dag, root_blocks, terminal_blocks,
    topological_layers, validate_pipeline, RegularGraph,
};
use crate::execution::truncate_chars;

pub(super) fn start_setup_analysis(app: &mut App) {
    if app.setup_analysis.loading {
        return;
    }

    let diag_agent_name = match app.config.diagnostic_provider.as_deref() {
        Some(name) => name.to_string(),
        None => {
            app.setup_analysis.show_message(
                "No diagnostic_provider configured.\n\n\
                 Set diagnostic_provider = \"AgentName\" in config\n\
                 (press 'e' on the Home screen to configure)."
                    .into(),
            );
            return;
        }
    };

    let agent_config = match app.effective_agent_config(&diag_agent_name).cloned() {
        Some(cfg) => cfg,
        None => {
            app.setup_analysis.show_message(format!(
                "Diagnostic agent '{diag_agent_name}' is not configured."
            ));
            return;
        }
    };

    if let Err(msg) = validate_agent_runtime(app, &diag_agent_name, &agent_config) {
        app.setup_analysis.show_message(msg);
        return;
    }

    // Mode-specific pre-flight
    match app.screen {
        Screen::Prompt | Screen::Order => {
            if app.prompt.prompt_text.trim().is_empty() && !app.prompt.resume_previous {
                app.setup_analysis.show_message(
                    "No prompt entered. Enter a prompt first or enable Resume.".into(),
                );
                return;
            }
            // Validate each selected run agent's runtime
            for name in &app.selected_agents {
                if let Some(cfg) = app.effective_agent_config(name).cloned() {
                    if let Err(msg) = validate_agent_runtime(app, name, &cfg) {
                        app.setup_analysis.show_message(msg);
                        return;
                    }
                }
            }
        }
        Screen::Pipeline => {
            if app.pipeline.pipeline_def.blocks.is_empty() {
                app.setup_analysis
                    .show_message("Pipeline has no blocks. Add blocks before analyzing.".into());
                return;
            }
            if app.pipeline.pipeline_def.initial_prompt.trim().is_empty() {
                app.setup_analysis.show_message(
                    "Pipeline has no initial prompt. Enter one before analyzing.".into(),
                );
                return;
            }
            if let Err(e) = validate_pipeline(&app.pipeline.pipeline_def) {
                app.setup_analysis
                    .show_message(format!("Pipeline validation failed:\n\n{e}"));
                return;
            }
            // Validate each block's agent runtime (execution + finalization + sub-pipelines)
            {
                let mut validation_error: Option<String> = None;
                app.pipeline
                    .pipeline_def
                    .visit_all_agent_refs(&mut |agent_name, block_id| {
                        if validation_error.is_some() {
                            return;
                        }
                        let label = format!("{agent_name} (block {block_id})");
                        match app.effective_agent_config(agent_name).cloned() {
                            Some(cfg) => {
                                if let Err(msg) = validate_agent_runtime(app, &label, &cfg) {
                                    validation_error = Some(msg);
                                }
                            }
                            None => {
                                validation_error = Some(format!(
                                    "Agent '{agent_name}' not found (block {block_id})"
                                ));
                            }
                        }
                    });
                if let Some(err) = validation_error {
                    app.setup_analysis.show_message(err);
                    return;
                }
            }
        }
        _ => {}
    }

    let prompt = build_setup_analysis_prompt(app);

    // Collect resume context for spawned task
    let resume_previous = app.prompt.resume_previous;
    let session_name = app.prompt.session_name.clone();
    let output_dir = app.config.resolved_output_dir();
    let selected_mode = app.selected_mode;
    let on_prompt_screen = app.screen == Screen::Prompt;
    let keep_session = app.prompt.keep_session;

    // On the Order screen, apply cursor-to-front reorder to match what Enter would do.
    let selected_agents = if app.screen == Screen::Order
        && app.order_grabbed.is_none()
        && app.order_cursor > 0
        && app.order_cursor < app.selected_agents.len()
    {
        let mut effective = app.selected_agents.clone();
        let agent = effective.remove(app.order_cursor);
        effective.insert(0, agent);
        effective
    } else {
        app.selected_agents.clone()
    };

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(
            app.effective_http_timeout_seconds().max(1),
        ))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            app.setup_analysis
                .show_message(format!("HTTP client error: {e}"));
            return;
        }
    };

    let mut provider = provider::create_provider(
        agent_config.provider,
        &agent_config.to_provider_config(),
        client,
        app.config.default_max_tokens,
        app.config.max_history_messages,
        app.config.max_history_bytes,
        app.effective_cli_timeout_seconds().max(1),
        vec![],
    );

    app.setup_analysis.open_loading();

    let (tx, rx) = mpsc::unbounded_channel();
    app.setup_analysis.rx = Some(rx);

    tokio::spawn(async move {
        let mut full_prompt = prompt;

        // Resume lookup (if applicable)
        if resume_previous && selected_mode != ExecutionMode::Pipeline {
            let sname = session_name.clone();
            let odir = output_dir.clone();
            let agents = selected_agents.clone();
            let ks = keep_session;
            let mode = selected_mode;
            let is_prompt_screen = on_prompt_screen;
            let is_multi_agent_relay = mode == ExecutionMode::Relay && agents.len() > 1;

            if is_prompt_screen && is_multi_agent_relay {
                full_prompt.push_str(
                    "\n\nResume is enabled. Compatibility depends on the \
                     final agent order chosen on the next screen.",
                );
            } else {
                let resume_info = tokio::task::spawn_blocking(move || {
                    use crate::output::OutputManager;
                    use super::resume::{
                        find_last_complete_iteration_for_agents, find_latest_compatible_run,
                        session_matches_resume,
                    };

                    let run_dir = if sname.trim().is_empty() {
                        find_latest_compatible_run(&odir, mode, &agents, ks)
                    } else {
                        OutputManager::find_latest_session_run(&odir, sname.trim())
                            .ok()
                            .flatten()
                    };
                    match run_dir {
                        Some(rd) => {
                            // Verify session metadata matches current config
                            match OutputManager::read_agent_session_info(&rd) {
                                Ok(info)
                                    if session_matches_resume(&info, mode, &agents, ks) =>
                                {
                                    match find_last_complete_iteration_for_agents(&rd, &agents) {
                                        Some(iter) => format!(
                                            "\n\nResume: will continue from session at {}, iteration {}.",
                                            rd.display(),
                                            iter + 1
                                        ),
                                        None => "\n\nResume: no compatible prior run found.".into(),
                                    }
                                }
                                Ok(_) => format!(
                                    "\n\nResume: session at {} does not match current {} configuration.",
                                    rd.display(),
                                    mode
                                ),
                                Err(_) => "\n\nResume: no compatible prior run found.".into(),
                            }
                        }
                        None => "\n\nResume: no compatible prior run found.".into(),
                    }
                })
                .await
                .unwrap_or_else(|_| "\n\nResume lookup failed.".into());

                full_prompt.push_str(&resume_info);
            }
        }

        let result = match provider.send(&full_prompt).await {
            Ok(resp) => Ok(resp.content),
            Err(e) => Err(e.to_string()),
        };
        let _ = tx.send(result);
    });
}

pub(super) fn build_setup_analysis_prompt(app: &App) -> String {
    let mut prompt = String::with_capacity(2048);

    prompt.push_str(
        "You are analyzing a multi-agent execution setup. Describe clearly and \
         concisely what will happen when this run starts. Be specific about data \
         flow and ordering.\n\n\
         Describe one complete execution in detail first, then state totals for \
         iterations and runs. Paraphrase prompts — do not quote them verbatim.\n\
         Keep it concise and scannable. Use short paragraphs.\n\
         Do not write files. Do not ask for filesystem permissions.\n\n",
    );

    match app.selected_mode {
        ExecutionMode::Relay => build_relay_prompt(app, &mut prompt),
        ExecutionMode::Swarm => build_swarm_prompt(app, &mut prompt),
        ExecutionMode::Pipeline => build_pipeline_prompt(app, &mut prompt),
    }

    prompt
}

fn build_relay_prompt(app: &App, prompt: &mut String) {
    prompt.push_str("Mode: Relay\n");

    // Agents with provider/model
    prompt.push_str("Agents:\n");
    for name in &app.selected_agents {
        if let Some(cfg) = app.effective_agent_config(name) {
            prompt.push_str(&format!("  - {} ({}/{})\n", name, cfg.provider, cfg.model));
        } else {
            prompt.push_str(&format!("  - {name}\n"));
        }
    }

    prompt.push_str(&format!(
        "Agent order: {}\n",
        app.selected_agents.join(" -> ")
    ));

    // On Prompt screen with multi-agent relay, order can still change
    if app.screen == Screen::Prompt
        && app.selected_mode == ExecutionMode::Relay
        && app.selected_agents.len() > 1
    {
        prompt.push_str(
            "NOTE: This order can still be changed on the next screen before execution starts.\n",
        );
    }

    // On Order screen, model the cursor-to-front behavior
    if app.screen == Screen::Order {
        if app.order_grabbed.is_none()
            && app.order_cursor > 0
            && app.order_cursor < app.selected_agents.len()
        {
            let mut effective_order = app.selected_agents.clone();
            let agent = effective_order.remove(app.order_cursor);
            effective_order.insert(0, agent);
            prompt.push_str(&format!(
                "Effective execution order (cursor-to-front): {}\n",
                effective_order.join(" -> ")
            ));
        }
        prompt.push_str("Pressing Enter will start execution with this order.\n");
    }

    prompt.push_str(&format!("Iterations: {}\n", app.prompt.iterations));
    prompt.push_str(&format!(
        "Runs: {} (concurrency: {})\n",
        app.prompt.runs,
        if app.prompt.concurrency == 0 {
            "unlimited".to_string()
        } else {
            app.prompt.concurrency.to_string()
        }
    ));
    prompt.push_str(&format!(
        "Forward Prompt: {}\n",
        if app.prompt.forward_prompt {
            "on"
        } else {
            "off"
        }
    ));
    prompt.push_str(&format!(
        "Keep Session: {}\n",
        if app.prompt.keep_session { "on" } else { "off" }
    ));
    prompt.push_str(&format!(
        "Resume: {}\n",
        if app.prompt.resume_previous {
            "on"
        } else {
            "off"
        }
    ));

    append_prompt_text(app, prompt);

    if app.prompt.runs > 1 {
        prompt.push_str(&format!(
            "\nMulti-run: {} independent runs with concurrency {}. Each run is fully independent.\n",
            app.prompt.runs,
            if app.prompt.concurrency == 0 {
                "unlimited".to_string()
            } else {
                app.prompt.concurrency.to_string()
            }
        ));
    }
}

fn build_swarm_prompt(app: &App, prompt: &mut String) {
    prompt.push_str("Mode: Swarm\n");

    prompt.push_str("Agents:\n");
    for name in &app.selected_agents {
        if let Some(cfg) = app.effective_agent_config(name) {
            prompt.push_str(&format!("  - {} ({}/{})\n", name, cfg.provider, cfg.model));
        } else {
            prompt.push_str(&format!("  - {name}\n"));
        }
    }

    prompt.push_str(&format!("Iterations (rounds): {}\n", app.prompt.iterations));
    prompt.push_str(&format!(
        "Runs: {} (concurrency: {})\n",
        app.prompt.runs,
        if app.prompt.concurrency == 0 {
            "unlimited".to_string()
        } else {
            app.prompt.concurrency.to_string()
        }
    ));
    prompt.push_str(&format!(
        "Keep Session: {}\n",
        if app.prompt.keep_session { "on" } else { "off" }
    ));
    prompt.push_str(&format!(
        "Resume: {}\n",
        if app.prompt.resume_previous {
            "on"
        } else {
            "off"
        }
    ));

    append_prompt_text(app, prompt);

    if app.prompt.runs > 1 {
        prompt.push_str(&format!(
            "\nMulti-run: {} independent runs with concurrency {}. Each run is fully independent.\n",
            app.prompt.runs,
            if app.prompt.concurrency == 0 {
                "unlimited".to_string()
            } else {
                app.prompt.concurrency.to_string()
            }
        ));
    }
}

fn build_pipeline_prompt(app: &App, prompt: &mut String) {
    let def = &app.pipeline.pipeline_def;

    prompt.push_str("Mode: Pipeline\n");

    // Initial prompt
    let initial = truncate_chars(&def.initial_prompt, 500);
    prompt.push_str(&format!("Initial prompt: {initial}"));
    if def.initial_prompt.chars().count() > 500 {
        prompt.push_str("...");
    }
    prompt.push('\n');

    prompt.push_str(&format!(
        "Runs: {} (concurrency: {})\n",
        app.pipeline.pipeline_runs,
        if app.pipeline.pipeline_concurrency == 0 {
            "unlimited".to_string()
        } else {
            app.pipeline.pipeline_concurrency.to_string()
        }
    ));

    let profiles_dir = pipeline_mod::profiles_dir();

    // Blocks
    prompt.push_str("\nBlocks:\n");
    for block in &def.blocks {
        let label = pipeline_block_label(block);

        if block.is_sub_pipeline() {
            let sub = block.sub_pipeline.as_ref().unwrap();
            prompt.push_str(&format!(
                "  {} \"{}\" [Sub-Pipeline] — {} execution blocks, {} finalization blocks\n",
                block.id,
                label,
                sub.blocks.len(),
                sub.finalization_blocks.len()
            ));
            continue;
        }

        let agent_names: Vec<String> = block
            .agents
            .iter()
            .map(|a| {
                if let Some(cfg) = app.effective_agent_config(a) {
                    format!("{} ({})", a, cfg.model)
                } else {
                    a.clone()
                }
            })
            .collect();
        let agent_info = agent_names.join(", ");
        let snippet = truncate_chars(&block.prompt, 200);
        let agent_label = if block.agents.len() > 1 {
            "agents"
        } else {
            "agent"
        };
        let mut line = format!(
            "  {} \"{}\" — {}: {}",
            block.id, label, agent_label, agent_info
        );
        if !snippet.is_empty() {
            line.push_str(&format!(", prompt: {snippet}"));
            if block.prompt.chars().count() > 200 {
                line.push_str("...");
            }
        }
        if !block.profiles.is_empty() {
            let labels: Vec<String> = block
                .profiles
                .iter()
                .map(|p| {
                    if !pipeline_mod::is_valid_profile_name(p) {
                        format!("{p} [invalid]")
                    } else if profiles_dir.join(format!("{p}.md")).is_file() {
                        p.clone()
                    } else {
                        format!("{p} [missing]")
                    }
                })
                .collect();
            line.push_str(&format!(", profiles: {}", labels.join(", ")));
        }
        if block.replicas > 1 {
            line.push_str(&format!(", replicas: {}", block.replicas));
        }
        if let Some(ref sid) = block.session_id {
            line.push_str(&format!(", session: {sid}"));
        }
        prompt.push_str(&line);
        prompt.push('\n');
    }

    // Connections
    if !def.connections.is_empty() {
        prompt.push_str("\nConnections:\n");
        for conn in &def.connections {
            let from_label = def
                .blocks
                .iter()
                .find(|b| b.id == conn.from)
                .map(pipeline_block_label)
                .unwrap_or_else(|| format!("Block {}", conn.from));
            let to_label = def
                .blocks
                .iter()
                .find(|b| b.id == conn.to)
                .map(pipeline_block_label)
                .unwrap_or_else(|| format!("Block {}", conn.to));
            if conn.scatter {
                let delim = conn.effective_delimiter();
                prompt.push_str(&format!(
                    "  {} \"{}\" -> {} \"{}\" [SCATTER, delim: \"{}\"]\n",
                    conn.from, from_label, conn.to, to_label, delim
                ));
            } else {
                prompt.push_str(&format!(
                    "  {} \"{}\" -> {} \"{}\"\n",
                    conn.from, from_label, conn.to, to_label
                ));
            }
        }
    }

    // Scatter warnings
    let scatter_graph =
        if def.connections.iter().any(|c| c.scatter) && !def.loop_connections.is_empty() {
            Some(RegularGraph::from_def(def))
        } else {
            None
        };
    for conn in &def.connections {
        if !conn.scatter {
            continue;
        }
        let to_block = def.blocks.iter().find(|b| b.id == conn.to);
        let to_label = to_block
            .map(pipeline_block_label)
            .unwrap_or_else(|| format!("Block {}", conn.to));

        // Check if scatter target is in a loop sub-DAG and whether it's the restart block
        let mut in_loop = false;
        let mut is_restart_block = false;
        for lc in &def.loop_connections {
            if lc.to == conn.to {
                in_loop = true;
                is_restart_block = true;
                break;
            }
            if let Some(ref graph) = scatter_graph {
                if let Some(sub_dag) = compute_loop_sub_dag(graph, lc.from, lc.to) {
                    if sub_dag.contains(&conn.to) {
                        in_loop = true;
                        break;
                    }
                }
            }
        }
        if !in_loop {
            let capacity = to_block.map(|b| b.logical_task_count()).unwrap_or(1);
            prompt.push_str(&format!(
                "\nWARNING: Block {} \"{}\" receives scatter input but is not in a loop. \
                 Only {} items will be processed per run.\n",
                conn.to, to_label, capacity
            ));
        } else if in_loop && !is_restart_block {
            prompt.push_str(&format!(
                "\nWARNING: Block {} \"{}\" receives scatter input and is inside a loop sub-DAG \
                 but is not the loop restart block. Scatter targets inside a loop must be the \
                 restart block (the block the loop feeds back into).\n",
                conn.to, to_label
            ));
        }
        if let Some(b) = to_block {
            if b.replicas == 1 && !in_loop {
                prompt.push_str(&format!(
                    "\nNOTE: Block {} \"{}\" receives scatter input with 1 replica. \
                     Items will be processed sequentially.\n",
                    conn.to, to_label
                ));
            }
        }
    }

    // Loop connections
    if !def.loop_connections.is_empty() {
        prompt.push_str("\nLoop connections:\n");
        for lc in &def.loop_connections {
            let from_label = def
                .blocks
                .iter()
                .find(|b| b.id == lc.from)
                .map(pipeline_block_label)
                .unwrap_or_else(|| format!("Block {}", lc.from));
            let to_label = def
                .blocks
                .iter()
                .find(|b| b.id == lc.to)
                .map(pipeline_block_label)
                .unwrap_or_else(|| format!("Block {}", lc.to));
            let mut line = format!(
                "  {} \"{}\" -> {} \"{}\" (x{}",
                lc.from, from_label, lc.to, to_label, lc.count
            );
            if !lc.prompt.is_empty() {
                let snippet = truncate_chars(&lc.prompt, 200);
                line.push_str(&format!(", prompt: {snippet}"));
                if lc.prompt.chars().count() > 200 {
                    line.push_str("...");
                }
            }
            if !lc.break_agent.is_empty() {
                line.push_str(&format!(", break_agent: {}", lc.break_agent));
            }
            if !lc.break_condition.is_empty() {
                let snippet = truncate_chars(&lc.break_condition, 100);
                line.push_str(&format!(", break_condition: {snippet}"));
                if lc.break_condition.chars().count() > 100 {
                    line.push_str("...");
                }
            }
            line.push(')');
            prompt.push_str(&line);
            prompt.push('\n');
        }
    }

    // Execution layers
    if let Ok(layers) = topological_layers(def) {
        prompt.push_str("\nExecution layers (parallel groups):\n");
        for (i, layer) in layers.iter().enumerate() {
            let labels: Vec<String> = layer
                .iter()
                .filter_map(|id| {
                    def.blocks
                        .iter()
                        .find(|b| b.id == *id)
                        .map(|b| format!("{} \"{}\"", b.id, pipeline_block_label(b)))
                })
                .collect();
            prompt.push_str(&format!("  Layer {}: {}\n", i + 1, labels.join(", ")));
        }
    }

    // Root and terminal blocks
    let roots = root_blocks(def);
    let terminals = terminal_blocks(def);
    if !roots.is_empty() {
        let root_labels: Vec<String> = roots
            .iter()
            .filter_map(|id| {
                def.blocks
                    .iter()
                    .find(|b| b.id == *id)
                    .map(|b| format!("{}", b.id))
            })
            .collect();
        prompt.push_str(&format!(
            "\nRoot blocks (receive initial prompt): {}\n",
            root_labels.join(", ")
        ));
    }
    if !terminals.is_empty() {
        let term_labels: Vec<String> = terminals
            .iter()
            .filter_map(|id| {
                def.blocks
                    .iter()
                    .find(|b| b.id == *id)
                    .map(|b| format!("{}", b.id))
            })
            .collect();
        prompt.push_str(&format!(
            "Terminal blocks (loop feedback target): {}\n",
            term_labels.join(", ")
        ));
    }

    // Sessions
    let sessions = def.effective_sessions();
    if !sessions.is_empty() {
        prompt.push_str("\nSessions:\n");
        for s in &sessions {
            let block_ids_str: Vec<String> = s.block_ids.iter().map(|id| id.to_string()).collect();
            prompt.push_str(&format!(
                "  \"{}\" — {}: Blocks {} — keep across loop passes: {}\n",
                s.display_label,
                s.agent,
                block_ids_str.join(", "),
                if s.keep_across_loop_passes {
                    "yes"
                } else {
                    "no"
                }
            ));
        }
    }

    // Runtime table for replicas / multi-agent expansion
    let rt = build_runtime_table(def);
    let has_expansion = def.blocks.iter().any(|b| b.logical_task_count() > 1);
    if has_expansion {
        prompt.push_str(&format!(
            "\nRuntime blocks (after agent/replica expansion): {}\n",
            rt.entries.len()
        ));
    }

    if app.pipeline.pipeline_runs > 1 {
        prompt.push_str(&format!(
            "\nMulti-run: {} independent runs with concurrency {}. Each run is fully independent.\n",
            app.pipeline.pipeline_runs,
            if app.pipeline.pipeline_concurrency == 0 {
                "unlimited".to_string()
            } else {
                app.pipeline.pipeline_concurrency.to_string()
            }
        ));
    }

    // Finalization phase
    if def.has_finalization() {
        prompt.push_str("\n--- Finalization Phase ---\n");
        prompt.push_str(
            "Finalization blocks run after all execution blocks (including loop reruns) \
             complete. They receive data from execution blocks via data feeds.\n",
        );

        // Finalization blocks
        prompt.push_str("\nFinalization blocks:\n");
        for block in &def.finalization_blocks {
            let label = pipeline_block_label(block);
            let agent_names: Vec<String> = block
                .agents
                .iter()
                .map(|a| {
                    if let Some(cfg) = app.effective_agent_config(a) {
                        format!("{} ({})", a, cfg.model)
                    } else {
                        a.clone()
                    }
                })
                .collect();
            let agent_info = agent_names.join(", ");
            let snippet = truncate_chars(&block.prompt, 200);
            let agent_label = if block.agents.len() > 1 {
                "agents"
            } else {
                "agent"
            };

            // Classify by granularity using the same logic as the runtime executor
            let granularity_label = if def.is_per_run_finalization_block(block.id) {
                "per-run"
            } else {
                "all-runs"
            };

            let mut line = format!(
                "  {} \"{}\" — {}: {}, scope: {}",
                block.id, label, agent_label, agent_info, granularity_label
            );
            if !snippet.is_empty() {
                line.push_str(&format!(", prompt: {snippet}"));
                if block.prompt.chars().count() > 200 {
                    line.push_str("...");
                }
            }
            if !block.profiles.is_empty() {
                let labels: Vec<String> = block
                    .profiles
                    .iter()
                    .map(|p| {
                        if !pipeline_mod::is_valid_profile_name(p) {
                            format!("{p} [invalid]")
                        } else if profiles_dir.join(format!("{p}.md")).is_file() {
                            p.clone()
                        } else {
                            format!("{p} [missing]")
                        }
                    })
                    .collect();
                line.push_str(&format!(", profiles: {}", labels.join(", ")));
            }
            prompt.push_str(&line);
            prompt.push('\n');
        }

        // Data feeds
        if !def.data_feeds.is_empty() {
            prompt.push_str("\nData feeds (execution -> finalization):\n");
            for feed in &def.data_feeds {
                let from_label = if feed.from == crate::execution::pipeline::WILDCARD_BLOCK_ID {
                    "* (all execution blocks)".to_string()
                } else {
                    def.blocks
                        .iter()
                        .find(|b| b.id == feed.from)
                        .map(|b| format!("{} \"{}\"", b.id, pipeline_block_label(b)))
                        .unwrap_or_else(|| format!("Block {}", feed.from))
                };
                let to_label = def
                    .finalization_blocks
                    .iter()
                    .find(|b| b.id == feed.to)
                    .map(|b| format!("{} \"{}\"", b.id, pipeline_block_label(b)))
                    .unwrap_or_else(|| format!("Block {}", feed.to));
                let collection = match feed.collection {
                    pipeline_mod::FeedCollection::LastPass => "last pass",
                    pipeline_mod::FeedCollection::AllPasses => "all passes",
                };
                let granularity = match feed.granularity {
                    pipeline_mod::FeedGranularity::PerRun => "per-run",
                    pipeline_mod::FeedGranularity::AllRuns => "all-runs",
                };
                prompt.push_str(&format!(
                    "  {from_label} -> {to_label}: collect {collection}, scope {granularity}\n"
                ));
            }

            // Note about wildcard feeds
            let has_wildcard = def
                .data_feeds
                .iter()
                .any(|f| f.from == crate::execution::pipeline::WILDCARD_BLOCK_ID);
            if has_wildcard {
                prompt.push_str(
                    "  Note: Wildcard feeds (from=*) collect output from ALL execution blocks.\n",
                );
            }
        }

        // Finalization connections
        if !def.finalization_connections.is_empty() {
            prompt.push_str("\nFinalization connections:\n");
            for conn in &def.finalization_connections {
                let from_label = def
                    .finalization_blocks
                    .iter()
                    .find(|b| b.id == conn.from)
                    .map(pipeline_block_label)
                    .unwrap_or_else(|| format!("Block {}", conn.from));
                let to_label = def
                    .finalization_blocks
                    .iter()
                    .find(|b| b.id == conn.to)
                    .map(pipeline_block_label)
                    .unwrap_or_else(|| format!("Block {}", conn.to));
                prompt.push_str(&format!(
                    "  {} \"{}\" -> {} \"{}\"\n",
                    conn.from, from_label, conn.to, to_label
                ));
            }
        }
    }
}

fn append_prompt_text(app: &App, prompt: &mut String) {
    if !app.prompt.prompt_text.is_empty() {
        let truncated = truncate_chars(&app.prompt.prompt_text, 500);
        prompt.push_str(&format!("Prompt: {truncated}"));
        if app.prompt.prompt_text.chars().count() > 500 {
            prompt.push_str("...");
        }
        prompt.push('\n');
    }
}

fn pipeline_block_label(block: &pipeline_mod::PipelineBlock) -> String {
    if block.name.trim().is_empty() {
        format!("Block {}", block.id)
    } else {
        block.name.clone()
    }
}

pub(super) fn handle_setup_analysis_result(app: &mut App, result: Result<String, String>) {
    app.setup_analysis.rx = None;
    if !app.setup_analysis.active {
        // Popup was closed (Esc) before result arrived. Discard.
        app.setup_analysis.loading = false;
        return;
    }
    app.setup_analysis.loading = false;
    match result {
        Ok(content) => app.setup_analysis.content = content,
        Err(e) => app.setup_analysis.content = format!("Analysis failed:\n\n{e}"),
    }
}
