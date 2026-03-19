use super::consolidation::start_consolidation;
use super::diagnostics::maybe_start_diagnostics;
use super::execution::{start_execution, start_pipeline_execution};
use super::results::{
    batch_result_entry_at, batch_result_visible_len, has_batch_result_tree, load_results,
    update_preview, BatchResultEntry,
};
use super::text_edit::*;
use super::*;
use crate::app::PipelineFeedEditField;
use crate::config::AppConfig;

/// Upper bound for numeric fields such as iterations, runs, and concurrency.
const MAX_NUMERIC_FIELD: u32 = 99;

pub(super) fn handle_key(app: &mut App, key: KeyEvent) {
    // Ctrl+C: graceful quit from anywhere
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if app.running.is_running {
            app.running.cancel_flag.store(true, Ordering::Relaxed);
        }
        app.should_quit = true;
        return;
    }

    // Dismiss error/info modal with any key
    if app.error_modal.is_some() || app.info_modal.is_some() {
        app.error_modal = None;
        app.info_modal = None;
        return;
    }

    // Help popup has dedicated scroll + close handling
    if app.help_popup.active {
        handle_help_popup_key(app, key);
        return;
    }

    // Setup analysis popup handling
    if app.setup_analysis.active {
        handle_setup_analysis_key(app, key);
        return;
    }

    // Edit popup handling
    if app.edit_popup.visible {
        handle_edit_popup_key(app, key);
        return;
    }

    match app.screen {
        Screen::Home => handle_home_key(app, key),
        Screen::Prompt => handle_prompt_key(app, key),
        Screen::Order => handle_order_key(app, key),
        Screen::Running => handle_running_key(app, key),
        Screen::Results => handle_results_key(app, key),
        Screen::Pipeline => handle_pipeline_key(app, key),
        Screen::Memory => handle_memory_key(app, key),
    }
}

pub(super) fn handle_paste(app: &mut App, text: &str) {
    if app.error_modal.is_some() || app.info_modal.is_some() {
        app.error_modal = None;
        app.info_modal = None;
        return;
    }

    if app.help_popup.active {
        return;
    }

    if app.setup_analysis.active {
        return;
    }

    if app.edit_popup.visible {
        if app.edit_popup.editing {
            app.edit_popup.edit_buffer.push_str(text);
        }
        return;
    }

    if app.screen == Screen::Prompt && app.prompt.prompt_focus == PromptFocus::Text {
        insert_prompt_text(app, text);
    } else if app.screen == Screen::Pipeline {
        handle_pipeline_paste(app, text);
    }
}

pub(super) fn handle_home_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Char('?') => {
            app.help_popup.open(1);
        }
        KeyCode::Char('M') => {
            if app.effective_memory_enabled() && app.memory.store.is_some() {
                // Purge expired memories so long-lived sessions stay clean.
                if let Some(ref store) = app.memory.store {
                    let _ = store.cleanup_expired();
                }
                app.memory.pending_bulk_delete = false;
                refresh_memory_list(app);
                app.screen = Screen::Memory;
            }
        }
        KeyCode::Char('e') => {
            app.edit_popup.visible = true;
            app.edit_popup.section = EditPopupSection::Providers;
            app.edit_popup.cursor = 0;
            app.edit_popup.timeout_cursor = 0;
            app.edit_popup.memory_cursor = 0;
            app.edit_popup.editing = false;
            app.edit_popup.edit_buffer.clear();
        }
        KeyCode::Tab => {
            app.home_section = match app.home_section {
                HomeSection::Agents => HomeSection::Mode,
                HomeSection::Mode => HomeSection::Agents,
            };
            app.home_cursor = 0;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.home_cursor = app.home_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            let max = match app.home_section {
                HomeSection::Agents => app.config.agents.len().saturating_sub(1),
                HomeSection::Mode => ExecutionMode::all().len().saturating_sub(1),
            };
            if app.home_cursor < max {
                app.home_cursor += 1;
            }
        }
        KeyCode::Char(' ') => match app.home_section {
            HomeSection::Agents => {
                let agents = app.available_agents();
                if let Some((agent, available)) = agents.get(app.home_cursor) {
                    if *available {
                        let name = agent.name.clone();
                        app.toggle_agent(&name);
                    }
                }
            }
            HomeSection::Mode => {
                if let Some(mode) = ExecutionMode::all().get(app.home_cursor) {
                    app.selected_mode = *mode;
                }
            }
        },
        KeyCode::Enter => {
            if app.selected_mode == ExecutionMode::Pipeline {
                app.screen = Screen::Pipeline;
                app.pipeline.pipeline_focus = PipelineFocus::InitialPrompt;
                app.pipeline.pipeline_prompt_cursor =
                    app.pipeline.pipeline_def.initial_prompt.len();
                app.pipeline.pipeline_iterations_buf =
                    app.pipeline.pipeline_def.iterations.to_string();
                app.pipeline.pipeline_runs_buf = app.pipeline.pipeline_runs.to_string();
                app.pipeline.pipeline_concurrency_buf =
                    app.pipeline.pipeline_concurrency.to_string();
            } else if app.selected_agents.is_empty() {
                app.error_modal = Some("Select at least one agent".into());
            } else {
                app.screen = Screen::Prompt;
                app.prompt.prompt_focus = PromptFocus::Text;
                app.prompt.prompt_cursor = app.prompt.prompt_text.len();
                app.prompt.iterations_buf = app.prompt.iterations.to_string();
                app.prompt.runs_buf = app.prompt.runs.to_string();
                app.prompt.concurrency_buf = app.prompt.concurrency.to_string();
                app.prompt.resume_previous = false;
                app.prompt.forward_prompt = false;
            }
        }
        _ => {}
    }
}

pub(super) fn handle_help_popup_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('?') => {
            app.help_popup.close();
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.help_popup.scroll = app.help_popup.scroll.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.help_popup.scroll = app.help_popup.scroll.saturating_add(1);
        }
        KeyCode::PageUp => {
            app.help_popup.scroll = app.help_popup.scroll.saturating_sub(8);
        }
        KeyCode::PageDown => {
            app.help_popup.scroll = app.help_popup.scroll.saturating_add(8);
        }
        KeyCode::Home => {
            app.help_popup.scroll = 0;
        }
        KeyCode::End => {
            app.help_popup.scroll = u16::MAX;
        }
        KeyCode::Tab => {
            app.help_popup.tab = (app.help_popup.tab + 1) % app.help_popup.tab_count;
            app.help_popup.scroll = 0;
        }
        KeyCode::BackTab => {
            app.help_popup.tab = app
                .help_popup
                .tab
                .checked_sub(1)
                .unwrap_or(app.help_popup.tab_count - 1);
            app.help_popup.scroll = 0;
        }
        _ => {}
    }
}

fn handle_setup_analysis_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            app.setup_analysis.close();
        }
        _ if app.setup_analysis.loading => {
            // While loading: only Esc/q to close (handled above)
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.setup_analysis.scroll = app.setup_analysis.scroll.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.setup_analysis.scroll = app.setup_analysis.scroll.saturating_add(1);
        }
        KeyCode::PageUp => {
            app.setup_analysis.scroll = app.setup_analysis.scroll.saturating_sub(8);
        }
        KeyCode::PageDown => {
            app.setup_analysis.scroll = app.setup_analysis.scroll.saturating_add(8);
        }
        KeyCode::Home => {
            app.setup_analysis.scroll = 0;
        }
        KeyCode::End => {
            app.setup_analysis.scroll = u16::MAX;
        }
        _ => {}
    }
}

pub(super) fn handle_prompt_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            sync_iterations_buf(app);
            sync_runs_buf(app);
            sync_concurrency_buf(app);
            app.screen = Screen::Home;
        }
        KeyCode::Tab => {
            app.prompt.prompt_focus = match (&app.prompt.prompt_focus, app.selected_mode) {
                (PromptFocus::Text, _) => PromptFocus::SessionName,
                (PromptFocus::SessionName, _) => {
                    app.prompt.iterations_buf = app.prompt.iterations.to_string();
                    PromptFocus::Iterations
                }
                (PromptFocus::Iterations, _) => {
                    sync_iterations_buf(app);
                    app.prompt.runs_buf = app.prompt.runs.to_string();
                    PromptFocus::Runs
                }
                (PromptFocus::Runs, _) => {
                    sync_runs_buf(app);
                    app.prompt.concurrency_buf = app.prompt.concurrency.to_string();
                    PromptFocus::Concurrency
                }
                (PromptFocus::Concurrency, _) => {
                    sync_concurrency_buf(app);
                    PromptFocus::Resume
                }
                (PromptFocus::Resume, ExecutionMode::Relay) => PromptFocus::ForwardPrompt,
                (PromptFocus::Resume, _) => PromptFocus::KeepSession,
                (PromptFocus::ForwardPrompt, _) => PromptFocus::KeepSession,
                (PromptFocus::KeepSession, _) => PromptFocus::Text,
            };
        }
        KeyCode::BackTab => {
            app.prompt.prompt_focus = match (&app.prompt.prompt_focus, app.selected_mode) {
                (PromptFocus::Text, _) => PromptFocus::KeepSession,
                (PromptFocus::SessionName, _) => PromptFocus::Text,
                (PromptFocus::Iterations, _) => {
                    sync_iterations_buf(app);
                    PromptFocus::SessionName
                }
                (PromptFocus::Runs, _) => {
                    sync_runs_buf(app);
                    app.prompt.iterations_buf = app.prompt.iterations.to_string();
                    PromptFocus::Iterations
                }
                (PromptFocus::Concurrency, _) => {
                    sync_concurrency_buf(app);
                    app.prompt.runs_buf = app.prompt.runs.to_string();
                    PromptFocus::Runs
                }
                (PromptFocus::Resume, _) => {
                    app.prompt.concurrency_buf = app.prompt.concurrency.to_string();
                    PromptFocus::Concurrency
                }
                (PromptFocus::ForwardPrompt, _) => PromptFocus::Resume,
                (PromptFocus::KeepSession, ExecutionMode::Relay) => PromptFocus::ForwardPrompt,
                (PromptFocus::KeepSession, _) => PromptFocus::Resume,
            };
        }
        KeyCode::Char('?')
            if !matches!(
                app.prompt.prompt_focus,
                PromptFocus::Text | PromptFocus::SessionName
            ) =>
        {
            app.help_popup.open(1);
        }
        KeyCode::Char(' ') if app.prompt.prompt_focus == PromptFocus::Resume => {
            if !resume_allowed_in_prompt(app) {
                app.prompt.resume_previous = false;
                app.error_modal = Some("Resume is only supported for single-run execution".into());
            } else {
                app.prompt.resume_previous = !app.prompt.resume_previous;
            }
        }
        KeyCode::Char(' ') if app.prompt.prompt_focus == PromptFocus::ForwardPrompt => {
            app.prompt.forward_prompt = !app.prompt.forward_prompt;
        }
        KeyCode::Char(' ') if app.prompt.prompt_focus == PromptFocus::KeepSession => {
            app.prompt.keep_session = !app.prompt.keep_session;
        }
        KeyCode::F(5) | KeyCode::Enter
            if key.code == KeyCode::F(5) || app.prompt.prompt_focus != PromptFocus::Text =>
        {
            sync_iterations_buf(app);
            sync_runs_buf(app);
            sync_concurrency_buf(app);
            if app.prompt.prompt_text.trim().is_empty()
                && !(app.prompt.resume_previous
                    && matches!(
                        app.selected_mode,
                        ExecutionMode::Relay | ExecutionMode::Swarm
                    ))
            {
                app.error_modal = Some("Enter a prompt first".into());
                return;
            }
            if app.selected_mode == ExecutionMode::Relay && app.selected_agents.len() > 1 {
                app.screen = Screen::Order;
                app.order_cursor = 0;
                app.order_grabbed = None;
            } else {
                start_execution(app);
            }
        }
        KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            super::setup_analysis::start_setup_analysis(app);
        }
        _ => match app.prompt.prompt_focus {
            PromptFocus::Text => handle_prompt_text_key(app, key),
            PromptFocus::SessionName => match key.code {
                KeyCode::Char(c) => app.prompt.session_name.push(c),
                KeyCode::Backspace => {
                    app.prompt.session_name.pop();
                }
                _ => {}
            },
            PromptFocus::Iterations => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.prompt.iterations_buf.push(c);
                    app.prompt.iterations = app
                        .prompt
                        .iterations_buf
                        .parse()
                        .unwrap_or(1)
                        .clamp(1, MAX_NUMERIC_FIELD);
                    app.prompt.iterations_buf = app.prompt.iterations.to_string();
                }
                KeyCode::Backspace => {
                    app.prompt.iterations_buf.pop();
                    if app.prompt.iterations_buf.is_empty() {
                        app.prompt.iterations = 1;
                    } else {
                        app.prompt.iterations = app
                            .prompt
                            .iterations_buf
                            .parse()
                            .unwrap_or(1)
                            .clamp(1, MAX_NUMERIC_FIELD);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.prompt.iterations = (app.prompt.iterations + 1).min(MAX_NUMERIC_FIELD);
                    app.prompt.iterations_buf = app.prompt.iterations.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.prompt.iterations = app.prompt.iterations.saturating_sub(1).max(1);
                    app.prompt.iterations_buf = app.prompt.iterations.to_string();
                }
                _ => {}
            },
            PromptFocus::Runs => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.prompt.runs_buf.push(c);
                    sync_runs_buf(app);
                    enforce_prompt_resume_constraints(app);
                }
                KeyCode::Backspace => {
                    app.prompt.runs_buf.pop();
                    if app.prompt.runs_buf.is_empty() {
                        app.prompt.runs = 1;
                    } else {
                        sync_runs_buf(app);
                    }
                    enforce_prompt_resume_constraints(app);
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.prompt.runs = (app.prompt.runs + 1).min(MAX_NUMERIC_FIELD);
                    app.prompt.runs_buf = app.prompt.runs.to_string();
                    enforce_prompt_resume_constraints(app);
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.prompt.runs = app.prompt.runs.saturating_sub(1).max(1);
                    app.prompt.runs_buf = app.prompt.runs.to_string();
                    enforce_prompt_resume_constraints(app);
                }
                _ => {}
            },
            PromptFocus::Concurrency => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.prompt.concurrency_buf.push(c);
                    sync_concurrency_buf(app);
                }
                KeyCode::Backspace => {
                    app.prompt.concurrency_buf.pop();
                    if app.prompt.concurrency_buf.is_empty() {
                        app.prompt.concurrency = 0;
                    } else {
                        sync_concurrency_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.prompt.concurrency = (app.prompt.concurrency + 1).min(MAX_NUMERIC_FIELD);
                    app.prompt.concurrency_buf = app.prompt.concurrency.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.prompt.concurrency = app.prompt.concurrency.saturating_sub(1);
                    app.prompt.concurrency_buf = app.prompt.concurrency.to_string();
                }
                _ => {}
            },
            PromptFocus::Resume | PromptFocus::ForwardPrompt | PromptFocus::KeepSession => {}
        },
    }
}

// ---------------------------------------------------------------------------
// Generic text editing helpers (buffer, cursor) — used by prompt and pipeline
// ---------------------------------------------------------------------------

pub(super) fn handle_pipeline_paste(app: &mut App, text: &str) {
    if app.pipeline.pipeline_show_session_config {
        return;
    }
    if app.pipeline.pipeline_show_feed_list || app.pipeline.pipeline_show_feed_edit {
        return;
    }
    if app.pipeline.pipeline_show_edit {
        match app.pipeline.pipeline_edit_field {
            PipelineEditField::Name => {
                let clean = text.replace(['\n', '\r'], " ");
                app.pipeline.pipeline_edit_name_buf.push_str(&clean);
                app.pipeline.pipeline_edit_name_cursor = app.pipeline.pipeline_edit_name_buf.len();
            }
            PipelineEditField::Prompt => {
                insert_text(
                    &mut app.pipeline.pipeline_edit_prompt_buf,
                    &mut app.pipeline.pipeline_edit_prompt_cursor,
                    text,
                );
            }
            PipelineEditField::SessionId => {
                let clean = text.replace(['\n', '\r'], "");
                app.pipeline.pipeline_edit_session_buf.push_str(&clean);
                app.pipeline.pipeline_edit_session_cursor =
                    app.pipeline.pipeline_edit_session_buf.len();
            }
            PipelineEditField::Agent => {}
            PipelineEditField::Profile => {}
            PipelineEditField::Replicas => {
                let clean: String = text.chars().filter(|c| c.is_ascii_digit()).collect();
                app.pipeline.pipeline_edit_replicas_buf.push_str(&clean);
            }
        }
    } else if app.pipeline.pipeline_show_loop_edit {
        if app.pipeline.pipeline_loop_edit_field == PipelineLoopEditField::Prompt {
            insert_text(
                &mut app.pipeline.pipeline_loop_edit_prompt_buf,
                &mut app.pipeline.pipeline_loop_edit_prompt_cursor,
                text,
            );
        }
    } else if let Some(PipelineDialogMode::Save) = app.pipeline.pipeline_file_dialog {
        app.pipeline.pipeline_file_input.push_str(text);
    } else if let Some(PipelineDialogMode::Load) = app.pipeline.pipeline_file_dialog {
        let clean = text.replace(['\n', '\r'], "");
        if !clean.is_empty() {
            app.pipeline.pipeline_file_search_focus = true;
            app.pipeline.pipeline_file_search.push_str(&clean);
            recompute_pipeline_file_filter(app);
        }
    } else {
        match app.pipeline.pipeline_focus {
            PipelineFocus::InitialPrompt => {
                insert_text(
                    &mut app.pipeline.pipeline_def.initial_prompt,
                    &mut app.pipeline.pipeline_prompt_cursor,
                    text,
                );
            }
            PipelineFocus::SessionName => {
                let clean = text.replace(['\n', '\r'], "");
                app.pipeline.pipeline_session_name.push_str(&clean);
            }
            PipelineFocus::Iterations => {
                for ch in text.chars() {
                    if ch.is_ascii_digit() {
                        app.pipeline.pipeline_iterations_buf.push(ch);
                    }
                }
                if !app.pipeline.pipeline_iterations_buf.is_empty() {
                    let v: u32 = app
                        .pipeline
                        .pipeline_iterations_buf
                        .parse()
                        .unwrap_or(1)
                        .clamp(1, MAX_NUMERIC_FIELD);
                    app.pipeline.pipeline_iterations_buf = v.to_string();
                }
            }
            PipelineFocus::Runs => {
                for ch in text.chars() {
                    if ch.is_ascii_digit() {
                        app.pipeline.pipeline_runs_buf.push(ch);
                    }
                }
                sync_pipeline_runs_buf(app);
            }
            PipelineFocus::Concurrency => {
                for ch in text.chars() {
                    if ch.is_ascii_digit() {
                        app.pipeline.pipeline_concurrency_buf.push(ch);
                    }
                }
                sync_pipeline_concurrency_buf(app);
            }
            PipelineFocus::Builder => {}
        }
    }
}

pub(super) fn handle_pipeline_key(app: &mut App, key: KeyEvent) {
    // Dispatch priority: edit popup > file dialog > session config > remove-conn > connect mode > normal
    if app.pipeline.pipeline_show_edit {
        handle_pipeline_edit_key(app, key);
        return;
    }
    if app.pipeline.pipeline_file_dialog.is_some() {
        handle_pipeline_dialog_key(app, key);
        return;
    }
    if app.pipeline.pipeline_show_session_config {
        handle_pipeline_session_config_key(app, key);
        return;
    }
    if app.pipeline.pipeline_show_loop_edit {
        handle_pipeline_loop_edit_key(app, key);
        return;
    }
    // feed_edit checked before feed_list: Enter in list opens edit stacked on
    // top of list, so the edit popup must capture keys first.
    if app.pipeline.pipeline_show_feed_edit {
        handle_pipeline_feed_edit_key(app, key);
        return;
    }
    if app.pipeline.pipeline_show_feed_list {
        handle_pipeline_feed_list_key(app, key);
        return;
    }
    if app.pipeline.pipeline_removing_conn {
        handle_pipeline_remove_conn_key(app, key);
        return;
    }
    if app.pipeline.pipeline_loop_connecting_from.is_some() {
        handle_pipeline_loop_connect_key(app, key);
        return;
    }
    if app.pipeline.pipeline_feed_connecting_from.is_some() {
        handle_pipeline_feed_connect_key(app, key);
        return;
    }

    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

    // Connect mode
    if app.pipeline.pipeline_connecting_from.is_some() {
        match key.code {
            KeyCode::Esc => {
                app.pipeline.pipeline_connecting_from = None;
            }
            KeyCode::Enter => {
                if let (Some(from), Some(to)) = (
                    app.pipeline.pipeline_connecting_from,
                    app.pipeline.pipeline_block_cursor,
                ) {
                    let from_is_fin = app.pipeline.pipeline_def.is_finalization_block(from);
                    let to_is_fin = app.pipeline.pipeline_def.is_finalization_block(to);

                    if from == to {
                        app.error_modal = Some("Cannot connect block to itself".into());
                    } else if from_is_fin != to_is_fin {
                        app.error_modal = Some(
                            "Cannot connect blocks across phases \u{2014} use data feeds (f) instead."
                                .into(),
                        );
                    } else if from_is_fin && to_is_fin {
                        // Finalization connection
                        if app
                            .pipeline
                            .pipeline_def
                            .finalization_connections
                            .iter()
                            .any(|c| c.from == from && c.to == to)
                        {
                            app.error_modal = Some("Connection already exists".into());
                        } else if pipeline_mod::would_create_cycle(
                            &app.pipeline.pipeline_def.finalization_connections,
                            from,
                            to,
                        ) {
                            app.error_modal = Some("Would create a cycle".into());
                        } else {
                            app.pipeline
                                .pipeline_def
                                .finalization_connections
                                .push(pipeline_mod::PipelineConnection { from, to });
                            app.pipeline.pipeline_connecting_from = None;
                        }
                    } else {
                        // Execution connection
                        if app
                            .pipeline
                            .pipeline_def
                            .connections
                            .iter()
                            .any(|c| c.from == from && c.to == to)
                        {
                            app.error_modal = Some("Connection already exists".into());
                        } else if pipeline_mod::would_create_cycle(
                            &app.pipeline.pipeline_def.connections,
                            from,
                            to,
                        ) {
                            app.error_modal = Some("Would create a cycle".into());
                        } else {
                            app.pipeline
                                .pipeline_def
                                .connections
                                .push(pipeline_mod::PipelineConnection { from, to });
                            let warnings =
                                pipeline_mod::prune_invalid_loops(&mut app.pipeline.pipeline_def);
                            if !warnings.is_empty() {
                                app.error_modal = Some(warnings.join("\n"));
                            }
                            app.pipeline.pipeline_connecting_from = None;
                        }
                    }
                }
            }
            KeyCode::Up | KeyCode::Char('k') => pipeline_spatial_nav(app, NavAxis::Vertical, true),
            KeyCode::Down | KeyCode::Char('j') => {
                pipeline_spatial_nav(app, NavAxis::Vertical, false)
            }
            KeyCode::Left | KeyCode::Char('h') => {
                pipeline_spatial_nav(app, NavAxis::Horizontal, true)
            }
            KeyCode::Right | KeyCode::Char('l') => {
                pipeline_spatial_nav(app, NavAxis::Horizontal, false)
            }
            _ => {}
        }
        return;
    }

    // Normal pipeline keys
    match key.code {
        KeyCode::Esc => {
            sync_pipeline_iterations_buf(app);
            sync_pipeline_runs_buf(app);
            sync_pipeline_concurrency_buf(app);
            app.screen = Screen::Home;
        }
        KeyCode::Tab => {
            app.pipeline.pipeline_focus = match app.pipeline.pipeline_focus {
                PipelineFocus::InitialPrompt => PipelineFocus::SessionName,
                PipelineFocus::SessionName => PipelineFocus::Iterations,
                PipelineFocus::Iterations => PipelineFocus::Runs,
                PipelineFocus::Runs => PipelineFocus::Concurrency,
                PipelineFocus::Concurrency => PipelineFocus::Builder,
                PipelineFocus::Builder => PipelineFocus::InitialPrompt,
            };
        }
        KeyCode::BackTab => {
            app.pipeline.pipeline_focus = match app.pipeline.pipeline_focus {
                PipelineFocus::InitialPrompt => PipelineFocus::Builder,
                PipelineFocus::SessionName => PipelineFocus::InitialPrompt,
                PipelineFocus::Iterations => PipelineFocus::SessionName,
                PipelineFocus::Runs => PipelineFocus::Iterations,
                PipelineFocus::Concurrency => PipelineFocus::Runs,
                PipelineFocus::Builder => PipelineFocus::Concurrency,
            };
        }
        KeyCode::Char('?')
            if !ctrl
                && !matches!(
                    app.pipeline.pipeline_focus,
                    PipelineFocus::InitialPrompt | PipelineFocus::SessionName
                ) =>
        {
            app.help_popup.open(crate::screen::help::PIPELINE_TAB_COUNT);
        }
        // Ctrl+S: save — always open dialog, prefill with current name
        KeyCode::Char('s') if ctrl => {
            app.pipeline.pipeline_file_dialog = Some(PipelineDialogMode::Save);
            app.pipeline.pipeline_file_input = app
                .pipeline
                .pipeline_save_path
                .as_ref()
                .and_then(|p| p.file_stem())
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
        }
        // Ctrl+L: load
        KeyCode::Char('l') if ctrl => {
            app.pipeline.pipeline_file_dialog = Some(PipelineDialogMode::Load);
            app.pipeline.pipeline_file_list = pipeline_mod::list_pipeline_files()
                .unwrap_or_default()
                .iter()
                .filter_map(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|s| s.to_string())
                })
                .collect();
            app.pipeline.pipeline_file_cursor = 0;
            app.pipeline.pipeline_file_scroll = 0;
            app.pipeline.pipeline_file_search.clear();
            app.pipeline.pipeline_file_search_focus = true;
            app.pipeline.pipeline_file_filtered =
                (0..app.pipeline.pipeline_file_list.len()).collect();
        }
        // F5: run
        KeyCode::F(5) => {
            start_pipeline_execution(app);
        }
        KeyCode::Char('e') if ctrl => {
            super::setup_analysis::start_setup_analysis(app);
        }
        _ => match app.pipeline.pipeline_focus {
            PipelineFocus::InitialPrompt => {
                handle_text_key(
                    &mut app.pipeline.pipeline_def.initial_prompt,
                    &mut app.pipeline.pipeline_prompt_cursor,
                    key,
                );
            }
            PipelineFocus::SessionName => match key.code {
                KeyCode::Char(c) => app.pipeline.pipeline_session_name.push(c),
                KeyCode::Backspace => {
                    app.pipeline.pipeline_session_name.pop();
                }
                _ => {}
            },
            PipelineFocus::Iterations => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline.pipeline_iterations_buf.push(c);
                    sync_pipeline_iterations_buf(app);
                }
                KeyCode::Backspace => {
                    app.pipeline.pipeline_iterations_buf.pop();
                    if app.pipeline.pipeline_iterations_buf.is_empty() {
                        app.pipeline.pipeline_def.iterations = 1;
                    } else {
                        sync_pipeline_iterations_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.pipeline.pipeline_def.iterations =
                        (app.pipeline.pipeline_def.iterations + 1).min(MAX_NUMERIC_FIELD);
                    app.pipeline.pipeline_iterations_buf =
                        app.pipeline.pipeline_def.iterations.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.pipeline.pipeline_def.iterations = app
                        .pipeline
                        .pipeline_def
                        .iterations
                        .saturating_sub(1)
                        .max(1);
                    app.pipeline.pipeline_iterations_buf =
                        app.pipeline.pipeline_def.iterations.to_string();
                }
                _ => {}
            },
            PipelineFocus::Runs => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline.pipeline_runs_buf.push(c);
                    sync_pipeline_runs_buf(app);
                }
                KeyCode::Backspace => {
                    app.pipeline.pipeline_runs_buf.pop();
                    if app.pipeline.pipeline_runs_buf.is_empty() {
                        app.pipeline.pipeline_runs = 1;
                    } else {
                        sync_pipeline_runs_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.pipeline.pipeline_runs =
                        (app.pipeline.pipeline_runs + 1).min(MAX_NUMERIC_FIELD);
                    app.pipeline.pipeline_runs_buf = app.pipeline.pipeline_runs.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.pipeline.pipeline_runs =
                        app.pipeline.pipeline_runs.saturating_sub(1).max(1);
                    app.pipeline.pipeline_runs_buf = app.pipeline.pipeline_runs.to_string();
                }
                _ => {}
            },
            PipelineFocus::Concurrency => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline.pipeline_concurrency_buf.push(c);
                    sync_pipeline_concurrency_buf(app);
                }
                KeyCode::Backspace => {
                    app.pipeline.pipeline_concurrency_buf.pop();
                    if app.pipeline.pipeline_concurrency_buf.is_empty() {
                        app.pipeline.pipeline_concurrency = 0;
                    } else {
                        sync_pipeline_concurrency_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.pipeline.pipeline_concurrency =
                        (app.pipeline.pipeline_concurrency + 1).min(MAX_NUMERIC_FIELD);
                    app.pipeline.pipeline_concurrency_buf =
                        app.pipeline.pipeline_concurrency.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.pipeline.pipeline_concurrency =
                        app.pipeline.pipeline_concurrency.saturating_sub(1);
                    app.pipeline.pipeline_concurrency_buf =
                        app.pipeline.pipeline_concurrency.to_string();
                }
                _ => {}
            },
            PipelineFocus::Builder => handle_pipeline_builder_key(app, key),
        },
    }
}

pub(super) fn handle_pipeline_builder_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('s') => {
            app.pipeline.pipeline_def.normalize_session_configs();
            let sessions = app.pipeline.pipeline_def.effective_sessions();
            app.pipeline.pipeline_show_session_config = true;
            if !sessions.is_empty() {
                app.pipeline.pipeline_session_config_cursor = app
                    .pipeline
                    .pipeline_session_config_cursor
                    .min(sessions.len() - 1);
            } else {
                app.pipeline.pipeline_session_config_cursor = 0;
            }
        }
        KeyCode::Char('a') => {
            let pos = pipeline_mod::next_free_position(&app.pipeline.pipeline_def.blocks);
            let id = app.pipeline.pipeline_next_id;
            app.pipeline.pipeline_next_id += 1;
            let default_agent = app
                .config
                .agents
                .first()
                .map(|a| a.name.clone())
                .unwrap_or_else(|| "Claude".to_string());
            app.pipeline
                .pipeline_def
                .blocks
                .push(pipeline_mod::PipelineBlock {
                    id,
                    name: format!("Block#{id}"),
                    agents: vec![default_agent],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: None,
                    position: pos,
                    replicas: 1,
                });
            app.pipeline.pipeline_block_cursor = Some(id);
            pipeline_ensure_visible(app);
        }
        KeyCode::Char('A') => {
            let pos =
                pipeline_mod::next_free_position(&app.pipeline.pipeline_def.finalization_blocks);
            let id = app.pipeline.pipeline_next_id;
            app.pipeline.pipeline_next_id += 1;
            let default_agent = app
                .config
                .agents
                .first()
                .map(|a| a.name.clone())
                .unwrap_or_else(|| "Claude".to_string());
            app.pipeline
                .pipeline_def
                .finalization_blocks
                .push(pipeline_mod::PipelineBlock {
                    id,
                    name: format!("Fin#{id}"),
                    agents: vec![default_agent],
                    prompt: String::new(),
                    profiles: vec![],
                    session_id: None,
                    position: pos,
                    replicas: 1,
                });
            app.pipeline.pipeline_block_cursor = Some(id);
            pipeline_ensure_visible(app);
        }
        KeyCode::Char('f') => {
            if let Some(sel) = app.pipeline.pipeline_block_cursor {
                let is_exec = app.pipeline.pipeline_def.blocks.iter().any(|b| b.id == sel);
                let is_fin = app.pipeline.pipeline_def.is_finalization_block(sel);

                if is_exec {
                    // Always enter feed-connect mode from exec blocks
                    // (allows creating multiple feeds from the same block)
                    app.pipeline.pipeline_feed_connecting_from = Some(sel);
                } else if is_fin {
                    let incoming_feeds: Vec<(BlockId, BlockId)> = app
                        .pipeline
                        .pipeline_def
                        .data_feeds
                        .iter()
                        .filter(|f| f.to == sel)
                        .map(|f| (f.from, f.to))
                        .collect();
                    if incoming_feeds.is_empty() {
                        app.error_modal =
                            Some("Use 'f' on an execution block to create feeds.".into());
                    } else if incoming_feeds.len() == 1 {
                        app.pipeline.pipeline_show_feed_edit = true;
                        app.pipeline.pipeline_feed_edit_target = Some(incoming_feeds[0]);
                        app.pipeline.pipeline_feed_edit_field = PipelineFeedEditField::Collection;
                    } else {
                        app.pipeline.pipeline_show_feed_list = true;
                        app.pipeline.pipeline_feed_list_cursor = 0;
                        app.pipeline.pipeline_feed_list_target = Some(sel);
                    }
                } else {
                    app.error_modal = Some("No applicable feeds for this block".into());
                }
            }
        }
        KeyCode::Char('F') => {
            if let Some(sel) = app.pipeline.pipeline_block_cursor {
                let feeds: Vec<(usize, BlockId, BlockId)> = app
                    .pipeline
                    .pipeline_def
                    .data_feeds
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| f.from == sel || f.to == sel)
                    .map(|(i, f)| (i, f.from, f.to))
                    .collect();
                if feeds.is_empty() {
                    app.error_modal = Some("No data feeds on this block".into());
                } else if feeds.len() == 1 {
                    app.pipeline.pipeline_def.data_feeds.remove(feeds[0].0);
                } else {
                    // Multiple feeds: need user to pick which one.
                    let is_fin = app.pipeline.pipeline_def.is_finalization_block(sel);
                    if is_fin {
                        // Open feed list picker for targeted deletion
                        app.pipeline.pipeline_show_feed_list = true;
                        app.pipeline.pipeline_feed_list_cursor = 0;
                        app.pipeline.pipeline_feed_list_target = Some(sel);
                    } else {
                        app.error_modal = Some(
                            "Multiple feeds on this block. Use F on the \
                             finalization block to pick which feed to remove."
                                .into(),
                        );
                    }
                }
            }
        }
        KeyCode::Char('d') => {
            if let Some(sel) = app.pipeline.pipeline_block_cursor {
                if app.pipeline.pipeline_def.is_finalization_block(sel) {
                    // Finalization block deletion
                    let old_index = app
                        .pipeline
                        .pipeline_def
                        .finalization_blocks
                        .iter()
                        .position(|b| b.id == sel)
                        .unwrap_or(0);
                    app.pipeline
                        .pipeline_def
                        .finalization_blocks
                        .retain(|b| b.id != sel);
                    app.pipeline
                        .pipeline_def
                        .finalization_connections
                        .retain(|c| c.from != sel && c.to != sel);
                    app.pipeline.pipeline_def.data_feeds.retain(|f| f.to != sel);
                    app.pipeline.pipeline_def.normalize_session_configs();
                    // Find next cursor from all blocks
                    let all: Vec<BlockId> = app
                        .pipeline
                        .pipeline_def
                        .all_blocks()
                        .map(|b| b.id)
                        .collect();
                    if all.is_empty() {
                        app.pipeline.pipeline_block_cursor = None;
                    } else {
                        let new_idx = old_index.min(all.len() - 1);
                        app.pipeline.pipeline_block_cursor = Some(all[new_idx]);
                        pipeline_ensure_visible(app);
                    }
                } else {
                    // Execution block deletion
                    let old_index = app
                        .pipeline
                        .pipeline_def
                        .blocks
                        .iter()
                        .position(|b| b.id == sel)
                        .unwrap_or(0);
                    app.pipeline.pipeline_def.blocks.retain(|b| b.id != sel);
                    app.pipeline
                        .pipeline_def
                        .connections
                        .retain(|c| c.from != sel && c.to != sel);
                    app.pipeline
                        .pipeline_def
                        .loop_connections
                        .retain(|lc| lc.from != sel && lc.to != sel);
                    app.pipeline
                        .pipeline_def
                        .data_feeds
                        .retain(|f| f.from != sel);
                    // Prune loops whose sub-DAG lost an internal node
                    let warnings =
                        pipeline_mod::prune_invalid_loops(&mut app.pipeline.pipeline_def);
                    if !warnings.is_empty() {
                        app.error_modal = Some(warnings.join("\n"));
                    }
                    app.pipeline.pipeline_def.normalize_session_configs();
                    let all: Vec<BlockId> = app
                        .pipeline
                        .pipeline_def
                        .all_blocks()
                        .map(|b| b.id)
                        .collect();
                    if all.is_empty() {
                        app.pipeline.pipeline_block_cursor = None;
                    } else {
                        let new_idx = old_index.min(all.len() - 1);
                        app.pipeline.pipeline_block_cursor = Some(all[new_idx]);
                        pipeline_ensure_visible(app);
                    }
                }
            }
        }
        KeyCode::Char('e') | KeyCode::Enter => {
            if let Some(sel) = app.pipeline.pipeline_block_cursor {
                if let Some(block) = app
                    .pipeline
                    .pipeline_def
                    .blocks
                    .iter()
                    .find(|b| b.id == sel)
                    .or_else(|| {
                        app.pipeline
                            .pipeline_def
                            .finalization_blocks
                            .iter()
                            .find(|b| b.id == sel)
                    })
                {
                    app.pipeline.pipeline_show_edit = true;
                    app.pipeline.pipeline_edit_field = PipelineEditField::Name;
                    app.pipeline.pipeline_edit_name_buf = block.name.clone();
                    app.pipeline.pipeline_edit_name_cursor = block.name.len();
                    app.pipeline.pipeline_edit_agent_selection = app
                        .config
                        .agents
                        .iter()
                        .map(|a| block.agents.contains(&a.name))
                        .collect();
                    app.pipeline.pipeline_edit_agent_cursor = 0;
                    app.pipeline.pipeline_edit_agent_scroll = 0;
                    // Profile selection — include on-disk profiles + orphaned (missing) ones
                    app.pipeline.pipeline_edit_profile_original_order = block.profiles.clone();
                    let profile_files = pipeline_mod::list_profile_files().unwrap_or_default();
                    let mut profile_list: Vec<String> = profile_files
                        .iter()
                        .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(String::from))
                        .collect();
                    // Append orphaned profiles (assigned but not on disk) so user can see/remove them
                    let orphaned: Vec<String> = block
                        .profiles
                        .iter()
                        .filter(|p| !profile_list.contains(p))
                        .cloned()
                        .collect();
                    let orphan_start = profile_list.len();
                    profile_list.extend(orphaned);
                    app.pipeline.pipeline_edit_profile_list = profile_list;
                    app.pipeline.pipeline_edit_profile_selection = app
                        .pipeline
                        .pipeline_edit_profile_list
                        .iter()
                        .map(|name| block.profiles.contains(name))
                        .collect();
                    // Track which entries are orphaned (for UI marker)
                    app.pipeline.pipeline_edit_profile_orphaned = (orphan_start
                        ..app.pipeline.pipeline_edit_profile_list.len())
                        .map(|i| app.pipeline.pipeline_edit_profile_list[i].clone())
                        .collect();
                    app.pipeline.pipeline_edit_profile_cursor = 0;
                    app.pipeline.pipeline_edit_profile_scroll = 0;
                    app.pipeline.pipeline_edit_prompt_buf = block.prompt.clone();
                    app.pipeline.pipeline_edit_prompt_cursor = block.prompt.len();
                    // Finalization blocks use hardcoded session keys at runtime,
                    // so hide session editing for them.
                    let is_fin = app.pipeline.pipeline_def.is_finalization_block(sel);
                    app.pipeline.pipeline_edit_session_buf = if is_fin {
                        String::new()
                    } else {
                        block.session_id.clone().unwrap_or_default()
                    };
                    app.pipeline.pipeline_edit_session_cursor =
                        app.pipeline.pipeline_edit_session_buf.len();
                    app.pipeline.pipeline_edit_replicas_buf = block.replicas.to_string();
                }
            }
        }
        KeyCode::Char('c') => {
            if let Some(sel) = app.pipeline.pipeline_block_cursor {
                app.pipeline.pipeline_connecting_from = Some(sel);
            }
        }
        KeyCode::Char('x') => {
            if let Some(sel) = app.pipeline.pipeline_block_cursor {
                let regular_count = app
                    .pipeline
                    .pipeline_def
                    .connections
                    .iter()
                    .filter(|c| c.from == sel || c.to == sel)
                    .count();
                let loop_count = app
                    .pipeline
                    .pipeline_def
                    .loop_connections
                    .iter()
                    .filter(|lc| lc.from == sel || lc.to == sel)
                    .count();
                let fin_count = app
                    .pipeline
                    .pipeline_def
                    .finalization_connections
                    .iter()
                    .filter(|c| c.from == sel || c.to == sel)
                    .count();
                if regular_count + loop_count + fin_count == 0 {
                    app.error_modal = Some("No connections on this block".into());
                } else {
                    app.pipeline.pipeline_removing_conn = true;
                    app.pipeline.pipeline_conn_cursor = 0;
                }
            }
        }
        KeyCode::Char('o') => {
            if let Some(sel) = app.pipeline.pipeline_block_cursor {
                if app.pipeline.pipeline_def.is_finalization_block(sel) {
                    app.error_modal = Some("Loops are only available for execution blocks".into());
                } else if let Some(lc) = app
                    .pipeline
                    .pipeline_def
                    .loop_connections
                    .iter()
                    .find(|lc| lc.from == sel || lc.to == sel)
                {
                    // Open loop edit popup
                    app.pipeline.pipeline_show_loop_edit = true;
                    app.pipeline.pipeline_loop_edit_field = PipelineLoopEditField::Count;
                    app.pipeline.pipeline_loop_edit_target = Some((lc.from, lc.to));
                    app.pipeline.pipeline_loop_edit_count_buf = lc.count.to_string();
                    app.pipeline.pipeline_loop_edit_prompt_buf = lc.prompt.clone();
                    app.pipeline.pipeline_loop_edit_prompt_cursor = lc.prompt.len();
                } else {
                    // Enter loop connect mode
                    app.pipeline.pipeline_loop_connecting_from = Some(sel);
                }
            }
        }
        // Arrow/hjkl navigate selection, Shift+Arrow/Shift+hjkl move selected block.
        // Ctrl+Arrow scrolls the canvas.
        KeyCode::Up | KeyCode::Char('k') => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                app.pipeline.pipeline_canvas_offset.1 =
                    app.pipeline.pipeline_canvas_offset.1.saturating_sub(1);
            } else if pipeline_builder_move_mode(&key) {
                pipeline_move_selected_block(app, 0, -1);
                pipeline_ensure_visible(app);
            } else {
                pipeline_spatial_nav(app, NavAxis::Vertical, true);
                pipeline_ensure_visible(app);
            }
        }
        KeyCode::Char('K') => {
            pipeline_move_selected_block(app, 0, -1);
            pipeline_ensure_visible(app);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                app.pipeline.pipeline_canvas_offset.1 += 1;
            } else if pipeline_builder_move_mode(&key) {
                pipeline_move_selected_block(app, 0, 1);
                pipeline_ensure_visible(app);
            } else {
                pipeline_spatial_nav(app, NavAxis::Vertical, false);
                pipeline_ensure_visible(app);
            }
        }
        KeyCode::Char('J') => {
            pipeline_move_selected_block(app, 0, 1);
            pipeline_ensure_visible(app);
        }
        KeyCode::Left | KeyCode::Char('h') => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                app.pipeline.pipeline_canvas_offset.0 =
                    app.pipeline.pipeline_canvas_offset.0.saturating_sub(1);
            } else if pipeline_builder_move_mode(&key) {
                pipeline_move_selected_block(app, -1, 0);
                pipeline_ensure_visible(app);
            } else {
                pipeline_spatial_nav(app, NavAxis::Horizontal, true);
                pipeline_ensure_visible(app);
            }
        }
        KeyCode::Char('H') => {
            pipeline_move_selected_block(app, -1, 0);
            pipeline_ensure_visible(app);
        }
        KeyCode::Right | KeyCode::Char('l') => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                app.pipeline.pipeline_canvas_offset.0 += 1;
            } else if pipeline_builder_move_mode(&key) {
                pipeline_move_selected_block(app, 1, 0);
                pipeline_ensure_visible(app);
            } else {
                pipeline_spatial_nav(app, NavAxis::Horizontal, false);
                pipeline_ensure_visible(app);
            }
        }
        KeyCode::Char('L') => {
            pipeline_move_selected_block(app, 1, 0);
            pipeline_ensure_visible(app);
        }
        _ => {}
    }
}

pub(super) enum NavAxis {
    Horizontal,
    Vertical,
}

pub(super) fn pipeline_builder_move_mode(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::SHIFT)
}

pub(super) fn pipeline_move_selected_block(app: &mut App, dx: i16, dy: i16) {
    let has_any = !app.pipeline.pipeline_def.blocks.is_empty()
        || !app.pipeline.pipeline_def.finalization_blocks.is_empty();
    if !has_any {
        app.pipeline.pipeline_block_cursor = None;
        return;
    }

    let Some(sel_id) = app.pipeline.pipeline_block_cursor else {
        app.pipeline.pipeline_block_cursor =
            app.pipeline.pipeline_def.all_blocks().next().map(|b| b.id);
        return;
    };

    // Search execution blocks first
    if let Some(sel_idx) = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .position(|b| b.id == sel_id)
    {
        let (sx, sy) = app.pipeline.pipeline_def.blocks[sel_idx].position;
        let nx_i = sx as i32 + dx as i32;
        let ny_i = sy as i32 + dy as i32;
        if nx_i < 0 || ny_i < 0 || nx_i > u16::MAX as i32 || ny_i > u16::MAX as i32 {
            return;
        }
        let next_pos = (nx_i as u16, ny_i as u16);
        if next_pos == (sx, sy) {
            return;
        }
        if let Some(other_idx) = app
            .pipeline
            .pipeline_def
            .blocks
            .iter()
            .position(|b| b.id != sel_id && b.position == next_pos)
        {
            app.pipeline.pipeline_def.blocks[other_idx].position = (sx, sy);
        }
        app.pipeline.pipeline_def.blocks[sel_idx].position = next_pos;
        return;
    }

    // Search finalization blocks
    if let Some(sel_idx) = app
        .pipeline
        .pipeline_def
        .finalization_blocks
        .iter()
        .position(|b| b.id == sel_id)
    {
        let (sx, sy) = app.pipeline.pipeline_def.finalization_blocks[sel_idx].position;
        let nx_i = sx as i32 + dx as i32;
        let ny_i = sy as i32 + dy as i32;
        if nx_i < 0 || ny_i < 0 || nx_i > u16::MAX as i32 || ny_i > u16::MAX as i32 {
            return;
        }
        let next_pos = (nx_i as u16, ny_i as u16);
        if next_pos == (sx, sy) {
            return;
        }
        if let Some(other_idx) = app
            .pipeline
            .pipeline_def
            .finalization_blocks
            .iter()
            .position(|b| b.id != sel_id && b.position == next_pos)
        {
            app.pipeline.pipeline_def.finalization_blocks[other_idx].position = (sx, sy);
        }
        app.pipeline.pipeline_def.finalization_blocks[sel_idx].position = next_pos;
        return;
    }

    // Fallback: select first available block
    app.pipeline.pipeline_block_cursor =
        app.pipeline.pipeline_def.all_blocks().next().map(|b| b.id);
}

fn handle_pipeline_session_config_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.pipeline.pipeline_show_session_config = false;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.pipeline.pipeline_session_config_cursor = app
                .pipeline
                .pipeline_session_config_cursor
                .saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            let sessions = app.pipeline.pipeline_def.effective_sessions();
            if !sessions.is_empty() {
                app.pipeline.pipeline_session_config_cursor =
                    (app.pipeline.pipeline_session_config_cursor + 1).min(sessions.len() - 1);
            }
        }
        KeyCode::Left | KeyCode::Char('h') => {
            app.pipeline.pipeline_session_config_col =
                app.pipeline.pipeline_session_config_col.saturating_sub(1);
        }
        KeyCode::Right | KeyCode::Char('l') => {
            app.pipeline.pipeline_session_config_col =
                (app.pipeline.pipeline_session_config_col + 1).min(1);
        }
        KeyCode::Char(' ') | KeyCode::Enter => {
            let sessions = app.pipeline.pipeline_def.effective_sessions();
            if sessions.is_empty() {
                return;
            }
            let cursor = app
                .pipeline
                .pipeline_session_config_cursor
                .min(sessions.len() - 1);
            let session = &sessions[cursor];
            let col = app.pipeline.pipeline_session_config_col;
            if col == 0 {
                let new_keep = !session.keep_across_iterations;
                app.pipeline
                    .pipeline_def
                    .set_keep_session_across_iterations(
                        &session.agent,
                        &session.session_key,
                        new_keep,
                    );
            } else {
                let new_keep = !session.keep_across_loop_passes;
                app.pipeline
                    .pipeline_def
                    .set_keep_session_across_loop_passes(
                        &session.agent,
                        &session.session_key,
                        new_keep,
                    );
            }
            app.pipeline.pipeline_def.normalize_session_configs();
            // Re-clamp cursor
            let sessions = app.pipeline.pipeline_def.effective_sessions();
            if !sessions.is_empty() {
                app.pipeline.pipeline_session_config_cursor = app
                    .pipeline
                    .pipeline_session_config_cursor
                    .min(sessions.len() - 1);
            }
        }
        _ => {}
    }
}

pub(super) fn pipeline_spatial_nav(app: &mut App, axis: NavAxis, negative: bool) {
    use crate::screen::pipeline::{separator_y_offset, CELL_H};

    let Some(sel_id) = app.pipeline.pipeline_block_cursor else {
        // Select first block if none selected
        app.pipeline.pipeline_block_cursor =
            app.pipeline.pipeline_def.all_blocks().next().map(|b| b.id);
        return;
    };

    // Build (id, screen_x, screen_y) for all blocks
    let sep_y = separator_y_offset(&app.pipeline.pipeline_def.blocks);
    let fin_y_off = sep_y as i32 + CELL_H as i32 / 2;

    let all_positions: Vec<(BlockId, i32, i32)> = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .map(|b| (b.id, b.position.0 as i32, b.position.1 as i32))
        .chain(
            app.pipeline
                .pipeline_def
                .finalization_blocks
                .iter()
                .map(|b| (b.id, b.position.0 as i32, b.position.1 as i32 + fin_y_off)),
        )
        .collect();

    let Some(&(_, sx, sy)) = all_positions.iter().find(|(id, _, _)| *id == sel_id) else {
        return;
    };

    let mut best: Option<(BlockId, i32)> = None;
    for &(bid, bx, by) in &all_positions {
        if bid == sel_id {
            continue;
        }
        let (dx, dy) = (bx - sx, by - sy);

        let in_direction = match (&axis, negative) {
            (NavAxis::Horizontal, true) => dx < 0 && dx.abs() >= dy.abs(),
            (NavAxis::Horizontal, false) => dx > 0 && dx.abs() >= dy.abs(),
            (NavAxis::Vertical, true) => dy < 0 && dy.abs() > dx.abs(),
            (NavAxis::Vertical, false) => dy > 0 && dy.abs() > dx.abs(),
        };
        if !in_direction {
            continue;
        }
        let dist = dx * dx + dy * dy;
        if best.is_none_or(|(_, bd)| dist < bd) {
            best = Some((bid, dist));
        }
    }
    if let Some((id, _)) = best {
        app.pipeline.pipeline_block_cursor = Some(id);
    }
}

pub(super) fn pipeline_ensure_visible(app: &mut App) {
    use crate::screen::pipeline::{separator_y_offset, BLOCK_H, BLOCK_W, CELL_H, CELL_W};

    let Some(sel_id) = app.pipeline.pipeline_block_cursor else {
        return;
    };

    // Search execution blocks
    let mut found_pos: Option<(i16, i16)> = None;
    if let Some(block) = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .find(|b| b.id == sel_id)
    {
        found_pos = Some((
            block.position.0 as i16 * CELL_W as i16,
            block.position.1 as i16 * CELL_H as i16,
        ));
    }
    // Search finalization blocks (apply separator offset)
    if found_pos.is_none() {
        if let Some(block) = app
            .pipeline
            .pipeline_def
            .finalization_blocks
            .iter()
            .find(|b| b.id == sel_id)
        {
            let sep_y = separator_y_offset(&app.pipeline.pipeline_def.blocks);
            let fin_y_off = sep_y + CELL_H as i16 / 2;
            found_pos = Some((
                block.position.0 as i16 * CELL_W as i16,
                block.position.1 as i16 * CELL_H as i16 + fin_y_off,
            ));
        }
    }

    let Some((sx, sy)) = found_pos else {
        return;
    };

    // Canvas inner = terminal size minus chrome (title 3 + prompt 6 + help 2 + borders 2)
    let (term_w, term_h) = crossterm::terminal::size().unwrap_or((80, 24));
    let visible_w = (term_w.saturating_sub(4)) as i16; // border + padding
    let visible_h = (term_h.saturating_sub(13)) as i16; // title+prompt+help+borders

    let ox = &mut app.pipeline.pipeline_canvas_offset.0;
    let oy = &mut app.pipeline.pipeline_canvas_offset.1;

    if sx < *ox {
        *ox = sx;
    }
    if sx + BLOCK_W as i16 > *ox + visible_w {
        *ox = sx + BLOCK_W as i16 - visible_w;
    }
    if sy < *oy {
        *oy = sy;
    }
    if sy + BLOCK_H as i16 > *oy + visible_h {
        *oy = sy + BLOCK_H as i16 - visible_h;
    }
}

pub(super) fn handle_pipeline_edit_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.pipeline.pipeline_show_edit = false;
        }
        KeyCode::Tab | KeyCode::BackTab => {
            let is_fin_block = app
                .pipeline
                .pipeline_block_cursor
                .map(|sel| app.pipeline.pipeline_def.is_finalization_block(sel))
                .unwrap_or(false);
            app.pipeline.pipeline_edit_field = if key.code == KeyCode::Tab {
                match app.pipeline.pipeline_edit_field {
                    PipelineEditField::Name => PipelineEditField::Agent,
                    PipelineEditField::Agent => PipelineEditField::Profile,
                    PipelineEditField::Profile => PipelineEditField::Prompt,
                    PipelineEditField::Prompt if is_fin_block => PipelineEditField::Replicas,
                    PipelineEditField::Prompt => PipelineEditField::SessionId,
                    PipelineEditField::SessionId => PipelineEditField::Replicas,
                    PipelineEditField::Replicas => PipelineEditField::Name,
                }
            } else {
                match app.pipeline.pipeline_edit_field {
                    PipelineEditField::Name => PipelineEditField::Replicas,
                    PipelineEditField::Agent => PipelineEditField::Name,
                    PipelineEditField::Profile => PipelineEditField::Agent,
                    PipelineEditField::Prompt => PipelineEditField::Profile,
                    PipelineEditField::SessionId => PipelineEditField::Prompt,
                    PipelineEditField::Replicas if is_fin_block => PipelineEditField::Prompt,
                    PipelineEditField::Replicas => PipelineEditField::SessionId,
                }
            };
        }
        KeyCode::Enter => {
            match app.pipeline.pipeline_edit_field {
                PipelineEditField::Name
                | PipelineEditField::Agent
                | PipelineEditField::Profile
                | PipelineEditField::SessionId
                | PipelineEditField::Replicas => {
                    // Confirm and save
                    if let Some(sel) = app.pipeline.pipeline_block_cursor {
                        // Check before mutable borrow of the block
                        let editing_fin = app.pipeline.pipeline_def.is_finalization_block(sel);
                        if let Some(block) = app
                            .pipeline
                            .pipeline_def
                            .blocks
                            .iter_mut()
                            .find(|b| b.id == sel)
                            .or_else(|| {
                                app.pipeline
                                    .pipeline_def
                                    .finalization_blocks
                                    .iter_mut()
                                    .find(|b| b.id == sel)
                            })
                        {
                            block.name = app.pipeline.pipeline_edit_name_buf.clone();
                            block.agents = app
                                .config
                                .agents
                                .iter()
                                .zip(&app.pipeline.pipeline_edit_agent_selection)
                                .filter(|(_, &selected)| selected)
                                .map(|(a, _)| a.name.clone())
                                .collect();
                            if block.agents.is_empty() {
                                block.agents = vec![app
                                    .config
                                    .agents
                                    .first()
                                    .map(|a| a.name.clone())
                                    .unwrap_or_else(|| "Claude".into())];
                            }
                            block.prompt = app.pipeline.pipeline_edit_prompt_buf.clone();
                            {
                                // Collect selected profile names from UI
                                let selected: Vec<String> = app
                                    .pipeline
                                    .pipeline_edit_profile_list
                                    .iter()
                                    .zip(&app.pipeline.pipeline_edit_profile_selection)
                                    .filter(|(_, &selected)| selected)
                                    .map(|(name, _)| name.clone())
                                    .collect();
                                // Preserve original order: keep existing profiles in their
                                // original position, append newly-added ones at the end.
                                let orig = &app.pipeline.pipeline_edit_profile_original_order;
                                let mut ordered: Vec<String> = orig
                                    .iter()
                                    .filter(|p| selected.contains(p))
                                    .cloned()
                                    .collect();
                                for name in &selected {
                                    if !orig.contains(name) {
                                        ordered.push(name.clone());
                                    }
                                }
                                block.profiles = ordered;
                            }
                            // Finalization blocks use hardcoded session keys at
                            // runtime — don't save user-edited session_id.
                            if !editing_fin {
                                block.session_id =
                                    if app.pipeline.pipeline_edit_session_buf.is_empty() {
                                        None
                                    } else {
                                        Some(app.pipeline.pipeline_edit_session_buf.clone())
                                    };
                            }
                            let agent_count = block.agents.len() as u32;
                            let max_replicas = (32 / agent_count.max(1)).max(1);
                            block.replicas = app
                                .pipeline
                                .pipeline_edit_replicas_buf
                                .parse::<u32>()
                                .unwrap_or(1)
                                .clamp(1, max_replicas);
                        }
                    }
                    app.pipeline.pipeline_def.normalize_session_configs();
                    app.pipeline.pipeline_show_edit = false;
                }
                PipelineEditField::Prompt => {
                    // Enter inserts newline in prompt field
                    insert_text(
                        &mut app.pipeline.pipeline_edit_prompt_buf,
                        &mut app.pipeline.pipeline_edit_prompt_cursor,
                        "\n",
                    );
                }
            }
        }
        _ => match app.pipeline.pipeline_edit_field {
            PipelineEditField::Name => match key.code {
                KeyCode::Char(c) => {
                    app.pipeline.pipeline_edit_name_buf.push(c);
                    app.pipeline.pipeline_edit_name_cursor =
                        app.pipeline.pipeline_edit_name_buf.len();
                }
                KeyCode::Backspace => {
                    app.pipeline.pipeline_edit_name_buf.pop();
                    app.pipeline.pipeline_edit_name_cursor =
                        app.pipeline.pipeline_edit_name_buf.len();
                }
                _ => {}
            },
            PipelineEditField::Agent => match key.code {
                KeyCode::Up | KeyCode::Char('k') => {
                    if !app.config.agents.is_empty() {
                        app.pipeline.pipeline_edit_agent_cursor =
                            app.pipeline.pipeline_edit_agent_cursor.saturating_sub(1);
                        if app.pipeline.pipeline_edit_agent_cursor
                            < app.pipeline.pipeline_edit_agent_scroll
                        {
                            app.pipeline.pipeline_edit_agent_scroll =
                                app.pipeline.pipeline_edit_agent_cursor;
                        }
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if !app.config.agents.is_empty() {
                        let max = app.config.agents.len().saturating_sub(1);
                        if app.pipeline.pipeline_edit_agent_cursor < max {
                            app.pipeline.pipeline_edit_agent_cursor += 1;
                        }
                        let visible = app.pipeline.pipeline_edit_agent_visible.get().max(1);
                        if app.pipeline.pipeline_edit_agent_cursor
                            >= app.pipeline.pipeline_edit_agent_scroll + visible
                        {
                            app.pipeline.pipeline_edit_agent_scroll =
                                app.pipeline.pipeline_edit_agent_cursor + 1 - visible;
                        }
                    }
                }
                KeyCode::Char(' ') => {
                    let idx = app.pipeline.pipeline_edit_agent_cursor;
                    if idx < app.pipeline.pipeline_edit_agent_selection.len() {
                        let currently_selected = app.pipeline.pipeline_edit_agent_selection[idx];
                        // Prevent deselecting the last agent
                        let selected_count = app
                            .pipeline
                            .pipeline_edit_agent_selection
                            .iter()
                            .filter(|&&s| s)
                            .count();
                        if !currently_selected || selected_count > 1 {
                            app.pipeline.pipeline_edit_agent_selection[idx] = !currently_selected;
                            // Re-clamp replicas to new agents × replicas cap
                            if !app.pipeline.pipeline_edit_replicas_buf.is_empty() {
                                sync_pipeline_edit_replicas_buf(app);
                            }
                        }
                    }
                }
                _ => {}
            },
            PipelineEditField::Profile => match key.code {
                KeyCode::Up | KeyCode::Char('k') => {
                    if !app.pipeline.pipeline_edit_profile_list.is_empty() {
                        app.pipeline.pipeline_edit_profile_cursor =
                            app.pipeline.pipeline_edit_profile_cursor.saturating_sub(1);
                        if app.pipeline.pipeline_edit_profile_cursor
                            < app.pipeline.pipeline_edit_profile_scroll
                        {
                            app.pipeline.pipeline_edit_profile_scroll =
                                app.pipeline.pipeline_edit_profile_cursor;
                        }
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if !app.pipeline.pipeline_edit_profile_list.is_empty() {
                        let max = app
                            .pipeline
                            .pipeline_edit_profile_list
                            .len()
                            .saturating_sub(1);
                        if app.pipeline.pipeline_edit_profile_cursor < max {
                            app.pipeline.pipeline_edit_profile_cursor += 1;
                        }
                        let visible = app.pipeline.pipeline_edit_profile_visible.get().max(1);
                        if app.pipeline.pipeline_edit_profile_cursor
                            >= app.pipeline.pipeline_edit_profile_scroll + visible
                        {
                            app.pipeline.pipeline_edit_profile_scroll =
                                app.pipeline.pipeline_edit_profile_cursor + 1 - visible;
                        }
                    }
                }
                KeyCode::Char(' ') => {
                    let idx = app.pipeline.pipeline_edit_profile_cursor;
                    if idx < app.pipeline.pipeline_edit_profile_selection.len() {
                        app.pipeline.pipeline_edit_profile_selection[idx] =
                            !app.pipeline.pipeline_edit_profile_selection[idx];
                    }
                }
                _ => {}
            },
            PipelineEditField::Prompt => {
                handle_text_key(
                    &mut app.pipeline.pipeline_edit_prompt_buf,
                    &mut app.pipeline.pipeline_edit_prompt_cursor,
                    key,
                );
            }
            PipelineEditField::SessionId => match key.code {
                KeyCode::Char(c) => {
                    app.pipeline.pipeline_edit_session_buf.push(c);
                    app.pipeline.pipeline_edit_session_cursor =
                        app.pipeline.pipeline_edit_session_buf.len();
                }
                KeyCode::Backspace => {
                    app.pipeline.pipeline_edit_session_buf.pop();
                    app.pipeline.pipeline_edit_session_cursor =
                        app.pipeline.pipeline_edit_session_buf.len();
                }
                _ => {}
            },
            PipelineEditField::Replicas => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline.pipeline_edit_replicas_buf.push(c);
                    sync_pipeline_edit_replicas_buf(app);
                }
                KeyCode::Backspace => {
                    app.pipeline.pipeline_edit_replicas_buf.pop();
                    if app.pipeline.pipeline_edit_replicas_buf.is_empty() {
                        // Keep showing empty, will default to 1 on save
                    } else {
                        sync_pipeline_edit_replicas_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    let max = pipeline_edit_max_replicas(app);
                    let cur: u32 = app.pipeline.pipeline_edit_replicas_buf.parse().unwrap_or(1);
                    let next = (cur + 1).min(max);
                    app.pipeline.pipeline_edit_replicas_buf = next.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    let cur: u32 = app.pipeline.pipeline_edit_replicas_buf.parse().unwrap_or(1);
                    let next = cur.saturating_sub(1).max(1);
                    app.pipeline.pipeline_edit_replicas_buf = next.to_string();
                }
                _ => {}
            },
        },
    }
}

pub(super) fn handle_pipeline_dialog_key(app: &mut App, key: KeyEvent) {
    match app.pipeline.pipeline_file_dialog {
        Some(PipelineDialogMode::Save) => match key.code {
            KeyCode::Esc => {
                app.pipeline.pipeline_file_dialog = None;
            }
            KeyCode::Enter => {
                if !app.pipeline.pipeline_file_input.is_empty() {
                    let dir = pipeline_mod::ensure_pipelines_dir();
                    match dir {
                        Ok(dir) => {
                            let filename =
                                format!("{}.toml", app.pipeline.pipeline_file_input.trim());
                            let path = dir.join(&filename);
                            match pipeline_mod::save_pipeline(&app.pipeline.pipeline_def, &path) {
                                Ok(()) => {
                                    app.pipeline.pipeline_save_path = Some(path);
                                    app.pipeline.pipeline_file_dialog = None;
                                }
                                Err(e) => {
                                    app.error_modal = Some(format!("Save failed: {e}"));
                                }
                            }
                        }
                        Err(e) => {
                            app.error_modal = Some(format!("Cannot create pipelines dir: {e}"));
                        }
                    }
                }
            }
            KeyCode::Char(c) => {
                app.pipeline.pipeline_file_input.push(c);
            }
            KeyCode::Backspace => {
                app.pipeline.pipeline_file_input.pop();
            }
            _ => {}
        },
        Some(PipelineDialogMode::Load) => {
            if key.code == KeyCode::Tab {
                // Toggle focus between search box and file list.
                // Stay in search when the filtered list is empty — there is nothing
                // to navigate so switching to the list would be a no-op dead-end.
                app.pipeline.pipeline_file_search_focus = !app.pipeline.pipeline_file_search_focus
                    || app.pipeline.pipeline_file_filtered.is_empty();
                clamp_file_cursor(app);
                scroll_file_list_to_cursor(app);
            } else if app.pipeline.pipeline_file_search_focus {
                match key.code {
                    KeyCode::Char(c) => {
                        app.pipeline.pipeline_file_search.push(c);
                        recompute_pipeline_file_filter(app);
                    }
                    KeyCode::Backspace => {
                        app.pipeline.pipeline_file_search.pop();
                        recompute_pipeline_file_filter(app);
                    }
                    KeyCode::Enter => {
                        if app.pipeline.pipeline_file_filtered.len() == 1 {
                            load_pipeline_by_filtered_cursor(app);
                        } else if !app.pipeline.pipeline_file_filtered.is_empty() {
                            app.pipeline.pipeline_file_search_focus = false;
                            clamp_file_cursor(app);
                        }
                    }
                    KeyCode::Esc => {
                        if !app.pipeline.pipeline_file_search.is_empty() {
                            app.pipeline.pipeline_file_search.clear();
                            recompute_pipeline_file_filter(app);
                        } else {
                            close_load_dialog(app);
                        }
                    }
                    _ => {}
                }
            } else {
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        app.pipeline.pipeline_file_cursor =
                            app.pipeline.pipeline_file_cursor.saturating_sub(1);
                        scroll_file_list_to_cursor(app);
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if app.pipeline.pipeline_file_cursor + 1
                            < app.pipeline.pipeline_file_filtered.len()
                        {
                            app.pipeline.pipeline_file_cursor += 1;
                        }
                        scroll_file_list_to_cursor(app);
                    }
                    KeyCode::Enter => {
                        load_pipeline_by_filtered_cursor(app);
                    }
                    KeyCode::Esc => {
                        close_load_dialog(app);
                    }
                    KeyCode::Char(c) => {
                        // Switch to search and forward the char
                        app.pipeline.pipeline_file_search_focus = true;
                        app.pipeline.pipeline_file_search.push(c);
                        recompute_pipeline_file_filter(app);
                    }
                    _ => {}
                }
            }
        }
        None => {}
    }
}

fn recompute_pipeline_file_filter(app: &mut App) {
    let query = app.pipeline.pipeline_file_search.to_lowercase();
    app.pipeline.pipeline_file_filtered = app
        .pipeline
        .pipeline_file_list
        .iter()
        .enumerate()
        .filter(|(_, name)| {
            if query.is_empty() {
                return true;
            }
            // Match against the filename stem so queries like "t" don't hit
            // the ".toml" extension on every file.
            let stem = name.strip_suffix(".toml").unwrap_or(name);
            stem.to_lowercase().contains(&query)
        })
        .map(|(i, _)| i)
        .collect();
    // Reset cursor and scroll — the old position may be out of bounds after filtering.
    app.pipeline.pipeline_file_cursor = 0;
    app.pipeline.pipeline_file_scroll = 0;
}

/// Re-sync file-list scroll after a terminal resize so the cursor stays visible.
/// Called from the resize event handler in `mod.rs`.
pub(super) fn adjust_file_dialog_scroll(app: &mut App) {
    if matches!(
        app.pipeline.pipeline_file_dialog,
        Some(PipelineDialogMode::Load)
    ) {
        clamp_file_cursor(app);
        scroll_file_list_to_cursor(app);
    }
}

/// Keep `pipeline_file_scroll` so that `pipeline_file_cursor` is always visible.
/// The actual viewport height is captured by the renderer in
/// `pipeline_file_visible` (same `Cell` pattern used by the edit popup).
fn scroll_file_list_to_cursor(app: &mut App) {
    let visible = app.pipeline.pipeline_file_visible.get().max(1);
    let cursor = app.pipeline.pipeline_file_cursor;
    let scroll = &mut app.pipeline.pipeline_file_scroll;
    if cursor < *scroll {
        *scroll = cursor;
    } else if cursor >= *scroll + visible {
        *scroll = cursor + 1 - visible;
    }
}

/// Ensure cursor stays within the filtered list bounds (defensive guard).
fn clamp_file_cursor(app: &mut App) {
    let len = app.pipeline.pipeline_file_filtered.len();
    if len == 0 {
        app.pipeline.pipeline_file_cursor = 0;
    } else if app.pipeline.pipeline_file_cursor >= len {
        app.pipeline.pipeline_file_cursor = len - 1;
    }
}

/// Close the Load dialog and release transient state so it does not linger in memory.
fn close_load_dialog(app: &mut App) {
    app.pipeline.pipeline_file_dialog = None;
    app.pipeline.pipeline_file_search.clear();
    app.pipeline.pipeline_file_filtered.clear();
    app.pipeline.pipeline_file_list.clear();
    app.pipeline.pipeline_file_cursor = 0;
    app.pipeline.pipeline_file_scroll = 0;
    app.pipeline.pipeline_file_search_focus = true;
}

fn load_pipeline_by_filtered_cursor(app: &mut App) {
    let filename = app
        .pipeline
        .pipeline_file_filtered
        .get(app.pipeline.pipeline_file_cursor)
        .and_then(|&i| app.pipeline.pipeline_file_list.get(i))
        .cloned();
    if let Some(filename) = filename {
        let path = pipeline_mod::pipelines_dir().join(&filename);
        match pipeline_mod::load_pipeline(&path) {
            Ok(def) => {
                let max_id = def.all_blocks().map(|b| b.id).max().unwrap_or(0);
                app.pipeline.pipeline_next_id = max_id + 1;
                app.pipeline.pipeline_def = def;
                app.pipeline.pipeline_save_path = Some(path);
                app.pipeline.pipeline_block_cursor =
                    app.pipeline.pipeline_def.all_blocks().next().map(|b| b.id);
                // Reset overlay state that may reference old definition
                app.pipeline.pipeline_show_feed_list = false;
                app.pipeline.pipeline_feed_list_target = None;
                app.pipeline.pipeline_feed_list_cursor = 0;
                app.pipeline.pipeline_show_feed_edit = false;
                app.pipeline.pipeline_feed_edit_target = None;
                app.pipeline.pipeline_feed_edit_field = PipelineFeedEditField::Collection;
                app.pipeline.pipeline_show_loop_edit = false;
                app.pipeline.pipeline_loop_edit_target = None;
                close_load_dialog(app);
            }
            Err(e) => {
                app.error_modal = Some(format!("Load failed: {e}"));
            }
        }
    }
}

pub(super) fn handle_pipeline_remove_conn_key(app: &mut App, key: KeyEvent) {
    let sel = app.pipeline.pipeline_block_cursor.unwrap_or(0);

    enum ConnRef {
        Regular(usize),
        Loop(usize),
        Finalization(usize),
    }
    let mut refs: Vec<ConnRef> = Vec::new();
    for (i, c) in app.pipeline.pipeline_def.connections.iter().enumerate() {
        if c.from == sel || c.to == sel {
            refs.push(ConnRef::Regular(i));
        }
    }
    for (i, lc) in app
        .pipeline
        .pipeline_def
        .loop_connections
        .iter()
        .enumerate()
    {
        if lc.from == sel || lc.to == sel {
            refs.push(ConnRef::Loop(i));
        }
    }
    for (i, c) in app
        .pipeline
        .pipeline_def
        .finalization_connections
        .iter()
        .enumerate()
    {
        if c.from == sel || c.to == sel {
            refs.push(ConnRef::Finalization(i));
        }
    }

    match key.code {
        KeyCode::Esc => {
            app.pipeline.pipeline_removing_conn = false;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.pipeline.pipeline_conn_cursor = app.pipeline.pipeline_conn_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.pipeline.pipeline_conn_cursor + 1 < refs.len() {
                app.pipeline.pipeline_conn_cursor += 1;
            }
        }
        KeyCode::Enter => {
            if let Some(conn_ref) = refs.get(app.pipeline.pipeline_conn_cursor) {
                match conn_ref {
                    ConnRef::Regular(idx) => {
                        app.pipeline.pipeline_def.connections.remove(*idx);
                        let warnings =
                            pipeline_mod::prune_invalid_loops(&mut app.pipeline.pipeline_def);
                        if !warnings.is_empty() {
                            app.error_modal = Some(warnings.join("\n"));
                        }
                    }
                    ConnRef::Loop(idx) => {
                        app.pipeline.pipeline_def.loop_connections.remove(*idx);
                    }
                    ConnRef::Finalization(idx) => {
                        app.pipeline
                            .pipeline_def
                            .finalization_connections
                            .remove(*idx);
                    }
                }
            }
            app.pipeline.pipeline_removing_conn = false;
        }
        _ => {}
    }
}

fn handle_pipeline_loop_connect_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.pipeline.pipeline_loop_connecting_from = None;
        }
        KeyCode::Enter => {
            if let (Some(from), Some(to)) = (
                app.pipeline.pipeline_loop_connecting_from,
                app.pipeline.pipeline_block_cursor,
            ) {
                if from == to {
                    app.error_modal = Some("Cannot loop a block to itself".into());
                } else if app
                    .pipeline
                    .pipeline_def
                    .loop_connections
                    .iter()
                    .any(|lc| lc.from == from || lc.to == from || lc.from == to || lc.to == to)
                {
                    app.error_modal = Some("Block already a loop endpoint".into());
                } else {
                    // Ancestry validation: `to` must be a regular-graph ancestor of `from`
                    let graph = pipeline_mod::RegularGraph::from_def(&app.pipeline.pipeline_def);
                    match pipeline_mod::compute_loop_sub_dag(&graph, from, to) {
                        None => {
                            app.error_modal = Some(
                                "Target is not an ancestor of source via regular connections"
                                    .into(),
                            );
                        }
                        Some(sub_dag_blocks) => {
                            // Check for overlapping sub-DAGs with existing loops
                            let mut overlap = false;
                            for existing_lc in &app.pipeline.pipeline_def.loop_connections {
                                if let Some(existing_blocks) = pipeline_mod::compute_loop_sub_dag(
                                    &graph,
                                    existing_lc.from,
                                    existing_lc.to,
                                ) {
                                    if sub_dag_blocks.iter().any(|b| existing_blocks.contains(b)) {
                                        overlap = true;
                                        break;
                                    }
                                }
                            }
                            if overlap {
                                app.error_modal = Some("Loop sub-DAGs would overlap".into());
                            } else {
                                app.pipeline.pipeline_def.loop_connections.push(
                                    pipeline_mod::LoopConnection {
                                        from,
                                        to,
                                        count: 1,
                                        prompt: String::new(),
                                    },
                                );
                                app.pipeline.pipeline_loop_connecting_from = None;
                            }
                        }
                    }
                }
            }
        }
        KeyCode::Up | KeyCode::Char('k') => pipeline_spatial_nav(app, NavAxis::Vertical, true),
        KeyCode::Down | KeyCode::Char('j') => pipeline_spatial_nav(app, NavAxis::Vertical, false),
        KeyCode::Left | KeyCode::Char('h') => pipeline_spatial_nav(app, NavAxis::Horizontal, true),
        KeyCode::Right | KeyCode::Char('l') => {
            pipeline_spatial_nav(app, NavAxis::Horizontal, false)
        }
        _ => {}
    }
}

fn handle_pipeline_feed_connect_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.pipeline.pipeline_feed_connecting_from = None;
        }
        KeyCode::Enter => {
            if let (Some(from), Some(to)) = (
                app.pipeline.pipeline_feed_connecting_from,
                app.pipeline.pipeline_block_cursor,
            ) {
                if !app.pipeline.pipeline_def.is_finalization_block(to) {
                    app.error_modal = Some("Feed target must be a finalization block".into());
                } else if app
                    .pipeline
                    .pipeline_def
                    .data_feeds
                    .iter()
                    .any(|f| f.from == from && f.to == to)
                {
                    app.error_modal = Some("Data feed already exists".into());
                } else {
                    app.pipeline
                        .pipeline_def
                        .data_feeds
                        .push(pipeline_mod::DataFeed {
                            from,
                            to,
                            collection: pipeline_mod::FeedCollection::LastIteration,
                            granularity: pipeline_mod::FeedGranularity::PerRun,
                        });
                    app.pipeline.pipeline_feed_connecting_from = None;
                    // Open feed edit popup immediately
                    app.pipeline.pipeline_show_feed_edit = true;
                    app.pipeline.pipeline_feed_edit_target = Some((from, to));
                    app.pipeline.pipeline_feed_edit_field = PipelineFeedEditField::Collection;
                }
            }
        }
        KeyCode::Up | KeyCode::Char('k') => pipeline_spatial_nav(app, NavAxis::Vertical, true),
        KeyCode::Down | KeyCode::Char('j') => pipeline_spatial_nav(app, NavAxis::Vertical, false),
        KeyCode::Left | KeyCode::Char('h') => pipeline_spatial_nav(app, NavAxis::Horizontal, true),
        KeyCode::Right | KeyCode::Char('l') => {
            pipeline_spatial_nav(app, NavAxis::Horizontal, false)
        }
        _ => {}
    }
}

fn handle_pipeline_feed_edit_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.pipeline.pipeline_show_feed_edit = false;
            app.pipeline.pipeline_feed_edit_target = None;
        }
        KeyCode::Tab => {
            app.pipeline.pipeline_feed_edit_field = match app.pipeline.pipeline_feed_edit_field {
                PipelineFeedEditField::Collection => PipelineFeedEditField::Granularity,
                PipelineFeedEditField::Granularity => PipelineFeedEditField::Collection,
            };
        }
        KeyCode::Char(' ') | KeyCode::Enter => {
            if let Some((from_id, to_id)) = app.pipeline.pipeline_feed_edit_target {
                if let Some(feed) = app
                    .pipeline
                    .pipeline_def
                    .data_feeds
                    .iter_mut()
                    .find(|f| f.from == from_id && f.to == to_id)
                {
                    match app.pipeline.pipeline_feed_edit_field {
                        PipelineFeedEditField::Collection => {
                            feed.collection = match feed.collection {
                                pipeline_mod::FeedCollection::LastIteration => {
                                    pipeline_mod::FeedCollection::AllIterations
                                }
                                pipeline_mod::FeedCollection::AllIterations => {
                                    pipeline_mod::FeedCollection::LastIteration
                                }
                            };
                        }
                        PipelineFeedEditField::Granularity => {
                            feed.granularity = match feed.granularity {
                                pipeline_mod::FeedGranularity::PerRun => {
                                    pipeline_mod::FeedGranularity::AllRuns
                                }
                                pipeline_mod::FeedGranularity::AllRuns => {
                                    pipeline_mod::FeedGranularity::PerRun
                                }
                            };
                        }
                    }
                }
            }
        }
        KeyCode::Up | KeyCode::Left | KeyCode::Char('k') | KeyCode::Char('h') => {
            app.pipeline.pipeline_feed_edit_field = match app.pipeline.pipeline_feed_edit_field {
                PipelineFeedEditField::Collection => PipelineFeedEditField::Granularity,
                PipelineFeedEditField::Granularity => PipelineFeedEditField::Collection,
            };
        }
        KeyCode::Down | KeyCode::Right | KeyCode::Char('j') | KeyCode::Char('l') => {
            app.pipeline.pipeline_feed_edit_field = match app.pipeline.pipeline_feed_edit_field {
                PipelineFeedEditField::Collection => PipelineFeedEditField::Granularity,
                PipelineFeedEditField::Granularity => PipelineFeedEditField::Collection,
            };
        }
        _ => {}
    }
}

fn handle_pipeline_feed_list_key(app: &mut App, key: KeyEvent) {
    let fin_id = match app.pipeline.pipeline_feed_list_target {
        Some(id) => id,
        None => return,
    };

    // Collect (data_feeds_index, from, to) so deletion uses exact index
    let feeds: Vec<(usize, BlockId, BlockId)> = app
        .pipeline
        .pipeline_def
        .data_feeds
        .iter()
        .enumerate()
        .filter(|(_, f)| f.to == fin_id)
        .map(|(i, f)| (i, f.from, f.to))
        .collect();

    if feeds.is_empty() {
        app.pipeline.pipeline_show_feed_list = false;
        app.pipeline.pipeline_feed_list_target = None;
        app.pipeline.pipeline_feed_list_cursor = 0;
        return;
    }

    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            let len = feeds.len();
            app.pipeline.pipeline_feed_list_cursor = if app.pipeline.pipeline_feed_list_cursor == 0
            {
                len - 1
            } else {
                app.pipeline.pipeline_feed_list_cursor - 1
            };
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.pipeline.pipeline_feed_list_cursor =
                (app.pipeline.pipeline_feed_list_cursor + 1) % feeds.len();
        }
        KeyCode::Enter => {
            let idx = app.pipeline.pipeline_feed_list_cursor;
            if let Some(&(_, from, to)) = feeds.get(idx) {
                app.pipeline.pipeline_feed_edit_target = Some((from, to));
                app.pipeline.pipeline_show_feed_edit = true;
                app.pipeline.pipeline_feed_edit_field = PipelineFeedEditField::Collection;
            }
        }
        KeyCode::Esc => {
            app.pipeline.pipeline_show_feed_list = false;
            app.pipeline.pipeline_feed_list_target = None;
            app.pipeline.pipeline_feed_list_cursor = 0;
        }
        KeyCode::Char('F') => {
            let cursor = app.pipeline.pipeline_feed_list_cursor;
            if let Some(&(data_idx, _, _)) = feeds.get(cursor) {
                app.pipeline.pipeline_def.data_feeds.remove(data_idx);
                let remaining = app
                    .pipeline
                    .pipeline_def
                    .data_feeds
                    .iter()
                    .filter(|f| f.to == fin_id)
                    .count();
                if remaining == 0 {
                    app.pipeline.pipeline_show_feed_list = false;
                    app.pipeline.pipeline_feed_list_target = None;
                    app.pipeline.pipeline_feed_list_cursor = 0;
                } else {
                    app.pipeline.pipeline_feed_list_cursor = cursor.min(remaining - 1);
                }
            }
        }
        _ => {}
    }
}

fn handle_pipeline_loop_edit_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.pipeline.pipeline_show_loop_edit = false;
        }
        KeyCode::Tab => {
            app.pipeline.pipeline_loop_edit_field = match app.pipeline.pipeline_loop_edit_field {
                PipelineLoopEditField::Count => PipelineLoopEditField::Prompt,
                PipelineLoopEditField::Prompt => PipelineLoopEditField::Count,
            };
        }
        KeyCode::Enter => {
            match app.pipeline.pipeline_loop_edit_field {
                PipelineLoopEditField::Count => {
                    // Save and close
                    let count: u32 = app
                        .pipeline
                        .pipeline_loop_edit_count_buf
                        .parse()
                        .unwrap_or(1)
                        .clamp(1, MAX_NUMERIC_FIELD);
                    if let Some((from, to)) = app.pipeline.pipeline_loop_edit_target {
                        if let Some(lc) = app
                            .pipeline
                            .pipeline_def
                            .loop_connections
                            .iter_mut()
                            .find(|lc| lc.from == from && lc.to == to)
                        {
                            lc.count = count;
                            lc.prompt = app.pipeline.pipeline_loop_edit_prompt_buf.clone();
                        }
                    }
                    app.pipeline.pipeline_show_loop_edit = false;
                }
                PipelineLoopEditField::Prompt => {
                    // Insert newline in prompt field
                    insert_text(
                        &mut app.pipeline.pipeline_loop_edit_prompt_buf,
                        &mut app.pipeline.pipeline_loop_edit_prompt_cursor,
                        "\n",
                    );
                }
            }
        }
        _ => match app.pipeline.pipeline_loop_edit_field {
            PipelineLoopEditField::Count => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline.pipeline_loop_edit_count_buf.push(c);
                    let v: u32 = app
                        .pipeline
                        .pipeline_loop_edit_count_buf
                        .parse()
                        .unwrap_or(1)
                        .clamp(1, MAX_NUMERIC_FIELD);
                    app.pipeline.pipeline_loop_edit_count_buf = v.to_string();
                }
                KeyCode::Backspace => {
                    app.pipeline.pipeline_loop_edit_count_buf.pop();
                    if app.pipeline.pipeline_loop_edit_count_buf.is_empty() {
                        app.pipeline.pipeline_loop_edit_count_buf = "1".into();
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    let cur: u32 = app
                        .pipeline
                        .pipeline_loop_edit_count_buf
                        .parse()
                        .unwrap_or(1);
                    let next = (cur + 1).min(MAX_NUMERIC_FIELD);
                    app.pipeline.pipeline_loop_edit_count_buf = next.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    let cur: u32 = app
                        .pipeline
                        .pipeline_loop_edit_count_buf
                        .parse()
                        .unwrap_or(1);
                    let next = cur.saturating_sub(1).max(1);
                    app.pipeline.pipeline_loop_edit_count_buf = next.to_string();
                }
                _ => {}
            },
            PipelineLoopEditField::Prompt => {
                handle_text_key(
                    &mut app.pipeline.pipeline_loop_edit_prompt_buf,
                    &mut app.pipeline.pipeline_loop_edit_prompt_cursor,
                    key,
                );
            }
        },
    }
}

pub(super) fn handle_order_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.screen = Screen::Prompt;
            app.order_grabbed = None;
        }
        KeyCode::Char('?') => {
            app.help_popup.open(1);
        }
        KeyCode::Up | KeyCode::Char('k') => app.move_order_up(),
        KeyCode::Down | KeyCode::Char('j') => app.move_order_down(),
        KeyCode::Char(' ') => {
            if app.order_grabbed.is_some() {
                app.order_grabbed = None;
            } else {
                app.order_grabbed = Some(app.order_cursor);
            }
        }
        KeyCode::Enter => {
            if app.order_grabbed.is_none()
                && app.order_cursor > 0
                && app.order_cursor < app.selected_agents.len()
            {
                let agent = app.selected_agents.remove(app.order_cursor);
                app.selected_agents.insert(0, agent);
                app.order_cursor = 0;
            }
            app.order_grabbed = None;
            start_execution(app);
        }
        KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            super::setup_analysis::start_setup_analysis(app);
        }
        _ => {}
    }
}

pub(super) fn handle_running_key(app: &mut App, key: KeyEvent) {
    if app.running.diagnostic_running {
        if key.code == KeyCode::Char('q') {
            app.should_quit = true;
        }
        return;
    }

    if app.running.consolidation_running {
        if key.code == KeyCode::Char('q') {
            app.should_quit = true;
        }
        return;
    }

    if app.running.consolidation_active {
        handle_consolidation_key(app, key);
        return;
    }

    match key.code {
        KeyCode::Up | KeyCode::Char('k') if app.running.multi_run_total > 1 => {
            app.running.multi_run_cursor = app.running.multi_run_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') if app.running.multi_run_total > 1 => {
            if app.running.multi_run_cursor + 1 < app.running.multi_run_states.len() {
                app.running.multi_run_cursor += 1;
            }
        }
        KeyCode::Esc if app.running.is_running => {
            app.running.cancel_flag.store(true, Ordering::Relaxed);
        }
        KeyCode::Enter if !app.running.is_running => {
            app.screen = Screen::Results;
            load_results(app);
        }
        KeyCode::Char('q') if !app.running.is_running => {
            app.should_quit = true;
        }
        KeyCode::Char('l') if app.running.multi_run_total <= 1 => {
            app.running.show_activity_log = !app.running.show_activity_log;
        }
        KeyCode::Char('p') if app.running.multi_run_total <= 1 => {
            if app.running.preview_target.is_some() {
                app.running.preview_target = None;
            } else {
                app.running.preview_target = running_stream_targets(app).into_iter().next();
            }
        }
        KeyCode::Tab
            if app.running.multi_run_total <= 1 && app.running.preview_target.is_some() =>
        {
            let targets = running_stream_targets(app);
            if !targets.is_empty() {
                let current_idx = app
                    .running
                    .preview_target
                    .as_ref()
                    .and_then(|current| targets.iter().position(|t| t == current))
                    .unwrap_or(0);
                let next = (current_idx + 1) % targets.len();
                app.running.preview_target = Some(targets[next].clone());
            }
        }
        KeyCode::BackTab
            if app.running.multi_run_total <= 1 && app.running.preview_target.is_some() =>
        {
            let targets = running_stream_targets(app);
            if !targets.is_empty() {
                let current_idx = app
                    .running
                    .preview_target
                    .as_ref()
                    .and_then(|current| targets.iter().position(|t| t == current))
                    .unwrap_or(0);
                let prev = if current_idx == 0 {
                    targets.len() - 1
                } else {
                    current_idx - 1
                };
                app.running.preview_target = Some(targets[prev].clone());
            }
        }
        _ => {}
    }
}

fn running_stream_targets(app: &App) -> Vec<crate::app::StreamTarget> {
    let candidates: Vec<crate::app::StreamTarget> =
        if app.selected_mode == crate::execution::ExecutionMode::Pipeline {
            app.block_rows()
                .iter()
                .map(|row| crate::app::StreamTarget::Block(row.block_id))
                .collect()
        } else {
            app.agent_rows()
                .iter()
                .map(|row| crate::app::StreamTarget::Agent(row.name.clone()))
                .collect()
        };
    candidates
        .into_iter()
        .filter(|t| app.stream_buffer(t).is_some_and(|b| !b.text().is_empty()))
        .collect()
}

pub(super) fn handle_consolidation_key(app: &mut App, key: KeyEvent) {
    if app.running.consolidation_running {
        if key.code == KeyCode::Char('q') {
            app.should_quit = true;
        }
        return;
    }

    match app.running.consolidation_phase {
        ConsolidationPhase::Confirm => match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                app.running.consolidation_target = if app.running.multi_run_total > 1 {
                    ConsolidationTarget::PerRun
                } else {
                    ConsolidationTarget::Single
                };
                app.running.consolidation_phase = if app.config.agents.len() <= 1 {
                    ConsolidationPhase::Prompt
                } else {
                    ConsolidationPhase::Provider
                };
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                if app.running.multi_run_total > 1 {
                    app.running.consolidation_target = ConsolidationTarget::AcrossRuns;
                    app.running.consolidation_phase = ConsolidationPhase::CrossRunConfirm;
                    app.running.consolidation_prompt.clear();
                } else {
                    app.running.consolidation_active = false;
                    maybe_start_diagnostics(app);
                }
            }
            KeyCode::Char('q') => app.should_quit = true,
            _ => {}
        },
        ConsolidationPhase::Provider => match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                app.running.consolidation_provider_cursor =
                    app.running.consolidation_provider_cursor.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let max = app.config.agents.len().saturating_sub(1);
                if app.running.consolidation_provider_cursor < max {
                    app.running.consolidation_provider_cursor += 1;
                }
            }
            KeyCode::Enter => {
                app.running.consolidation_phase =
                    if app.running.consolidation_target == ConsolidationTarget::AcrossRuns {
                        ConsolidationPhase::CrossRunPrompt
                    } else {
                        ConsolidationPhase::Prompt
                    };
            }
            KeyCode::Esc => {
                app.running.consolidation_phase =
                    if app.running.consolidation_target == ConsolidationTarget::AcrossRuns {
                        ConsolidationPhase::CrossRunConfirm
                    } else {
                        ConsolidationPhase::Confirm
                    };
            }
            KeyCode::Char('q') => app.should_quit = true,
            _ => {}
        },
        ConsolidationPhase::Prompt => match key.code {
            KeyCode::Enter => start_consolidation(app),
            KeyCode::Backspace => {
                app.running.consolidation_prompt.pop();
            }
            KeyCode::Char(c) => app.running.consolidation_prompt.push(c),
            KeyCode::Esc => {
                if app.config.agents.len() <= 1 {
                    app.running.consolidation_phase = ConsolidationPhase::Confirm;
                } else {
                    app.running.consolidation_phase = ConsolidationPhase::Provider;
                }
            }
            _ => {}
        },
        ConsolidationPhase::CrossRunConfirm => match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                app.running.consolidation_target = ConsolidationTarget::AcrossRuns;
                app.running.consolidation_phase = if app.config.agents.len() <= 1 {
                    ConsolidationPhase::CrossRunPrompt
                } else {
                    ConsolidationPhase::Provider
                };
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                app.running.consolidation_active = false;
                maybe_start_diagnostics(app);
            }
            KeyCode::Char('q') => app.should_quit = true,
            _ => {}
        },
        ConsolidationPhase::CrossRunPrompt => match key.code {
            KeyCode::Enter => start_consolidation(app),
            KeyCode::Backspace => {
                app.running.consolidation_prompt.pop();
            }
            KeyCode::Char(c) => app.running.consolidation_prompt.push(c),
            KeyCode::Esc => {
                app.running.consolidation_phase = if app.config.agents.len() <= 1 {
                    ConsolidationPhase::CrossRunConfirm
                } else {
                    ConsolidationPhase::Provider
                };
            }
            _ => {}
        },
    }
}

pub(super) fn handle_results_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Up | KeyCode::Char('k') => {
            app.results.result_cursor = app.results.result_cursor.saturating_sub(1);
            update_preview(app);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if has_batch_result_tree(app) {
                let max = batch_result_visible_len(app).saturating_sub(1);
                if app.results.result_cursor < max {
                    app.results.result_cursor += 1;
                }
            } else if app.results.result_cursor < app.results.result_files.len().saturating_sub(1) {
                app.results.result_cursor += 1;
            }
            update_preview(app);
        }
        KeyCode::Enter | KeyCode::Char('l') if has_batch_result_tree(app) => {
            match batch_result_entry_at(app, app.results.result_cursor) {
                Some(BatchResultEntry::RunHeader(run_id)) => {
                    if !app.results.batch_result_expanded.insert(run_id) {
                        app.results.batch_result_expanded.remove(&run_id);
                    }
                    update_preview(app);
                }
                Some(BatchResultEntry::FinalizationHeader) => {
                    app.results.batch_result_finalization_expanded =
                        !app.results.batch_result_finalization_expanded;
                    update_preview(app);
                }
                _ => {
                    app.reset_to_home();
                }
            }
        }
        KeyCode::Esc => app.reset_to_home(),
        KeyCode::Enter => app.reset_to_home(),
        _ => {}
    }
}

pub(super) fn handle_edit_popup_key(app: &mut App, key: KeyEvent) {
    // Model picker mode
    if app.edit_popup.model_picker_active {
        handle_model_picker_key(app, key);
        return;
    }

    // While a config save is in flight, only allow Esc (to close).
    // All other keys are blocked so the user cannot modify state that
    // the completion handler will overwrite.
    if app.edit_popup.config_save_in_progress {
        if key.code == KeyCode::Esc {
            app.edit_popup.visible = false;
        }
        return;
    }

    match key.code {
        KeyCode::Esc => {
            if app.edit_popup.editing {
                app.edit_popup.editing = false;
                app.edit_popup.edit_buffer.clear();
            } else {
                app.edit_popup.visible = false;
            }
        }
        KeyCode::Tab if !app.edit_popup.editing => {
            app.edit_popup.section = match app.edit_popup.section {
                EditPopupSection::Providers => EditPopupSection::Timeouts,
                EditPopupSection::Timeouts => EditPopupSection::Memory,
                EditPopupSection::Memory => EditPopupSection::Providers,
            };
            app.edit_popup.edit_buffer.clear();
        }
        KeyCode::BackTab if !app.edit_popup.editing => {
            app.edit_popup.section = match app.edit_popup.section {
                EditPopupSection::Providers => EditPopupSection::Memory,
                EditPopupSection::Timeouts => EditPopupSection::Providers,
                EditPopupSection::Memory => EditPopupSection::Timeouts,
            };
            app.edit_popup.edit_buffer.clear();
        }
        KeyCode::Up | KeyCode::Char('k') if !app.edit_popup.editing => {
            match app.edit_popup.section {
                EditPopupSection::Providers => {
                    app.edit_popup.cursor = app.edit_popup.cursor.saturating_sub(1);
                }
                EditPopupSection::Timeouts => {
                    app.edit_popup.timeout_cursor = app.edit_popup.timeout_cursor.saturating_sub(1);
                }
                EditPopupSection::Memory => {
                    app.edit_popup.memory_cursor = app.edit_popup.memory_cursor.saturating_sub(1);
                }
            }
        }
        KeyCode::Down | KeyCode::Char('j') if !app.edit_popup.editing => {
            match app.edit_popup.section {
                EditPopupSection::Providers => {
                    let max = app.config.agents.len().saturating_sub(1);
                    if app.edit_popup.cursor < max {
                        app.edit_popup.cursor += 1;
                    }
                }
                EditPopupSection::Timeouts => {
                    let max = timeout_field_count().saturating_sub(1);
                    if app.edit_popup.timeout_cursor < max {
                        app.edit_popup.timeout_cursor += 1;
                    }
                }
                EditPopupSection::Memory => {
                    let max = MEM_FIELD_COUNT.saturating_sub(1);
                    if app.edit_popup.memory_cursor < max {
                        app.edit_popup.memory_cursor += 1;
                    }
                }
            }
        }
        KeyCode::Char('a')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            app.edit_popup.field = EditField::ApiKey;
            app.edit_popup.edit_buffer = effective_section_config(app)
                .map(|c| c.api_key)
                .unwrap_or_default();
            app.edit_popup.editing = true;
        }
        KeyCode::Char('m')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            app.edit_popup.field = EditField::Model;
            app.edit_popup.edit_buffer = effective_section_config(app)
                .map(|c| c.model)
                .unwrap_or_default();
            app.edit_popup.editing = true;
        }
        KeyCode::Char('x')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            app.edit_popup.field = EditField::ExtraCliArgs;
            app.edit_popup.edit_buffer = effective_section_config(app)
                .map(|c| c.extra_cli_args)
                .unwrap_or_default();
            app.edit_popup.editing = true;
        }
        KeyCode::Char('l')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            app.edit_popup.field = EditField::Model;
            start_model_fetch(app);
        }
        KeyCode::Char('o') if !app.edit_popup.editing => {
            app.edit_popup.field = EditField::OutputDir;
            app.edit_popup.edit_buffer = app.config.output_dir.clone();
            app.edit_popup.editing = true;
        }
        KeyCode::Char('s') if !app.edit_popup.editing => {
            save_config_globally(app);
        }
        KeyCode::Char('c')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            toggle_cli_mode(app);
        }
        KeyCode::Char('d')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            toggle_diagnostic_agent(app);
        }
        KeyCode::Char('t')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            cycle_reasoning(app);
        }
        KeyCode::Char('b')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            toggle_cli_print_mode(app);
        }
        KeyCode::Char('n')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            add_new_agent(app);
        }
        KeyCode::Char('p')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            cycle_agent_provider(app);
        }
        KeyCode::Char('r')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            if let Some(agent) = app.config.agents.get(app.edit_popup.cursor) {
                app.edit_popup.field = EditField::AgentName;
                app.edit_popup.edit_buffer = agent.name.clone();
                app.edit_popup.editing = true;
            }
        }
        KeyCode::Delete | KeyCode::Backspace
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Providers =>
        {
            remove_agent(app);
        }
        KeyCode::Char('e') if !app.edit_popup.editing => match app.edit_popup.section {
            EditPopupSection::Timeouts => begin_timeout_edit(app),
            EditPopupSection::Memory => begin_memory_edit(app),
            _ => {}
        },
        KeyCode::Char(' ')
            if !app.edit_popup.editing && app.edit_popup.section == EditPopupSection::Memory =>
        {
            toggle_memory_field(app);
        }
        KeyCode::Enter if !app.edit_popup.editing => match app.edit_popup.section {
            EditPopupSection::Timeouts => begin_timeout_edit(app),
            EditPopupSection::Memory => {
                if is_memory_toggle_field(app.edit_popup.memory_cursor) {
                    toggle_memory_field(app);
                } else {
                    begin_memory_edit(app);
                }
            }
            _ => {
                app.edit_popup.field = EditField::ApiKey;
                app.edit_popup.edit_buffer = effective_section_config(app)
                    .map(|c| c.api_key)
                    .unwrap_or_default();
                app.edit_popup.editing = true;
            }
        },
        KeyCode::Enter if app.edit_popup.editing => {
            if matches!(app.edit_popup.field, EditField::OutputDir) {
                let new_output_dir = app.edit_popup.edit_buffer.trim();
                if new_output_dir.is_empty() {
                    app.error_modal = Some("Output directory cannot be empty".into());
                } else {
                    let old_output_dir = app.config.output_dir.clone();
                    app.config.output_dir = new_output_dir.to_string();
                    // Reopen memory store if db_path is derived from output_dir
                    if app.effective_memory_enabled()
                        && app.config.memory.db_path.is_empty()
                        && new_output_dir != old_output_dir
                    {
                        let db_path = app.config.resolved_output_dir().join("memory.db");
                        match crate::memory::store::MemoryStore::open(&db_path) {
                            Ok(store) => {
                                let stale_days = app.effective_memory_stale_permanent_days();
                                if stale_days > 0 {
                                    let _ = store.archive_stale_permanent(stale_days);
                                }
                                app.memory.store = Some(store);
                                app.memory.project_id = crate::memory::project::detect_project_id(
                                    &app.config.memory.project_id,
                                );
                            }
                            Err(e) => {
                                // Clear stale store so recall/insert don't silently
                                // use the old DB while artifacts go to the new dir.
                                app.memory.store = None;
                                app.error_modal = Some(format!(
                                    "Memory store failed to reopen at new output_dir: {e}"
                                ));
                            }
                        }
                    }
                }
            } else if matches!(app.edit_popup.field, EditField::AgentName) {
                commit_agent_rename(app);
                return;
            } else if app.edit_popup.section == EditPopupSection::Timeouts
                || matches!(app.edit_popup.field, EditField::TimeoutSeconds)
            {
                if let Err(e) = set_timeout_override_from_buffer(app) {
                    app.error_modal = Some(e);
                    return;
                }
            } else if matches!(app.edit_popup.field, EditField::MemoryValue) {
                if let Err(e) = set_memory_override_from_buffer(app) {
                    app.error_modal = Some(e);
                    return;
                }
            } else {
                let mut config =
                    effective_section_config(app).unwrap_or_else(empty_provider_config);
                match app.edit_popup.field {
                    EditField::ApiKey => config.api_key = app.edit_popup.edit_buffer.clone(),
                    EditField::Model => config.model = app.edit_popup.edit_buffer.clone(),
                    EditField::ExtraCliArgs => {
                        config.extra_cli_args = app.edit_popup.edit_buffer.clone()
                    }
                    EditField::OutputDir
                    | EditField::TimeoutSeconds
                    | EditField::AgentName
                    | EditField::MemoryValue => {}
                }
                set_section_config_override(app, config);
            }
            app.edit_popup.edit_buffer.clear();
            app.edit_popup.editing = false;
        }
        KeyCode::Backspace if app.edit_popup.editing => {
            app.edit_popup.edit_buffer.pop();
        }
        KeyCode::Char(c) if app.edit_popup.editing => {
            if matches!(app.edit_popup.field, EditField::TimeoutSeconds) {
                if c.is_ascii_digit() {
                    app.edit_popup.edit_buffer.push(c);
                }
            } else if matches!(app.edit_popup.field, EditField::MemoryValue) {
                // Extraction agent allows any char; other memory fields are numeric
                if app.edit_popup.memory_cursor == MEM_EXTRACTION_AGENT || c.is_ascii_digit() {
                    app.edit_popup.edit_buffer.push(c);
                }
            } else {
                app.edit_popup.edit_buffer.push(c);
            }
        }
        _ => {}
    }
}

pub(super) fn handle_model_picker_key(app: &mut App, key: KeyEvent) {
    if app.edit_popup.model_picker_loading {
        // Only allow Esc while loading
        if key.code == KeyCode::Esc {
            app.edit_popup.model_picker_active = false;
            app.edit_popup.model_picker_loading = false;
            app.edit_popup.model_picker_rx = None;
        }
        return;
    }

    match key.code {
        KeyCode::Esc => {
            app.edit_popup.model_picker_active = false;
            app.edit_popup.model_picker_list.clear();
            app.edit_popup.model_picker_filter.clear();
        }
        KeyCode::Up => {
            app.edit_popup.model_picker_cursor =
                app.edit_popup.model_picker_cursor.saturating_sub(1);
        }
        KeyCode::Down => {
            let filtered_len = filtered_model_count(app);
            if app.edit_popup.model_picker_cursor < filtered_len.saturating_sub(1) {
                app.edit_popup.model_picker_cursor += 1;
            }
        }
        KeyCode::Enter => {
            let filtered = filtered_models(app);
            if let Some(model) = filtered.get(app.edit_popup.model_picker_cursor).cloned() {
                let mut config =
                    effective_section_config(app).unwrap_or_else(empty_provider_config);
                config.model = model;
                set_section_config_override(app, config);
            }
            app.edit_popup.model_picker_active = false;
            app.edit_popup.model_picker_list.clear();
            app.edit_popup.model_picker_filter.clear();
        }
        KeyCode::Char(c) => {
            app.edit_popup.model_picker_filter.push(c);
            app.edit_popup.model_picker_cursor = 0;
        }
        KeyCode::Backspace => {
            app.edit_popup.model_picker_filter.pop();
            app.edit_popup.model_picker_cursor = 0;
        }
        _ => {}
    }
}

/// Return the filtered model list on demand (no separate stored copy).
pub(super) fn filtered_models(app: &App) -> Vec<String> {
    let filter = app.edit_popup.model_picker_filter.to_lowercase();
    if filter.is_empty() {
        app.edit_popup.model_picker_list.clone()
    } else {
        app.edit_popup
            .model_picker_list
            .iter()
            .filter(|m| m.to_lowercase().contains(&filter))
            .cloned()
            .collect()
    }
}

fn filtered_model_count(app: &App) -> usize {
    let filter = app.edit_popup.model_picker_filter.to_lowercase();
    if filter.is_empty() {
        app.edit_popup.model_picker_list.len()
    } else {
        app.edit_popup
            .model_picker_list
            .iter()
            .filter(|m| m.to_lowercase().contains(&filter))
            .count()
    }
}

pub(super) fn start_model_fetch(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    let api_key = match effective_section_config(app) {
        Some(c) if !c.api_key.is_empty() => c.api_key,
        _ => {
            app.error_modal = Some(
                "Add API key to fetch model list. You can type model manually with [m].".into(),
            );
            return;
        }
    };

    let timeout_secs = app.effective_model_fetch_timeout_seconds().max(1);
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            app.edit_popup.model_picker_active = false;
            app.edit_popup.model_picker_loading = false;
            app.edit_popup.model_picker_rx = None;
            app.error_modal = Some(format!("Failed to create HTTP client: {e}"));
            return;
        }
    };

    app.edit_popup.model_picker_active = true;
    app.edit_popup.model_picker_loading = true;
    app.edit_popup.model_picker_list.clear();
    app.edit_popup.model_picker_filter.clear();
    app.edit_popup.model_picker_cursor = 0;

    let (tx, rx) = mpsc::unbounded_channel();
    app.edit_popup.model_picker_rx = Some(rx);

    tokio::spawn(async move {
        let result = provider::list_models(kind, &api_key, &client).await;
        let _ = tx.send(result);
    });
}

pub(super) fn handle_model_list_result(app: &mut App, result: Result<Vec<String>, String>) {
    app.edit_popup.model_picker_loading = false;
    app.edit_popup.model_picker_rx = None;
    match result {
        Ok(models) if models.is_empty() => {
            app.edit_popup.model_picker_active = false;
            app.error_modal = Some("No models returned by API".into());
        }
        Ok(models) => {
            // Pre-select the current model if it's in the list
            let current_model = effective_section_config(app);
            app.edit_popup.model_picker_cursor = current_model
                .as_ref()
                .and_then(|c| models.iter().position(|x| x == &c.model))
                .unwrap_or(0);
            app.edit_popup.model_picker_list = models;
        }
        Err(e) => {
            app.edit_popup.model_picker_active = false;
            app.error_modal = Some(format!("Failed to fetch models: {e}"));
        }
    }
}

pub(super) fn save_config_globally(app: &mut App) {
    if app.edit_popup.config_save_in_progress {
        return;
    }

    // Build a merged config clone for saving. We do NOT mutate app.config here
    // so that a save failure leaves it untouched. The success handler applies
    // the merge once we know the write succeeded.
    let mut config_to_save = app.config.clone();
    for (name, override_agent) in &app.session_overrides {
        if let Some(idx) = config_to_save.agents.iter().position(|a| &a.name == name) {
            config_to_save.agents[idx] = override_agent.clone();
        }
    }
    if let Some(value) = app.session_http_timeout_seconds {
        config_to_save.http_timeout_seconds = value;
    }
    if let Some(value) = app.session_model_fetch_timeout_seconds {
        config_to_save.model_fetch_timeout_seconds = value;
    }
    if let Some(value) = app.session_cli_timeout_seconds {
        config_to_save.cli_timeout_seconds = value;
    }
    if let Some(v) = app.session_memory_enabled {
        config_to_save.memory.enabled = v;
    }
    if let Some(v) = app.session_memory_max_recall {
        config_to_save.memory.max_recall = v;
    }
    if let Some(v) = app.session_memory_max_recall_bytes {
        config_to_save.memory.max_recall_bytes = v;
    }
    if let Some(v) = app.session_memory_observation_ttl_days {
        config_to_save.memory.observation_ttl_days = v;
    }
    if let Some(v) = app.session_memory_summary_ttl_days {
        config_to_save.memory.summary_ttl_days = v;
    }
    if let Some(ref v) = app.session_memory_extraction_agent {
        config_to_save.memory.extraction_agent = v.clone();
    }
    if let Some(v) = app.session_memory_disable_extraction {
        config_to_save.memory.disable_extraction = v;
    }
    if let Some(v) = app.session_memory_stale_permanent_days {
        config_to_save.memory.stale_permanent_days = v;
    }
    app.edit_popup.config_save_in_progress = true;
    let path_override = app.config_path_override.clone();

    let (tx, rx) = mpsc::unbounded_channel();
    app.edit_popup.config_save_rx = Some(rx);

    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let save_result = match path_override.as_deref() {
                Some(path) => config_to_save
                    .save_with_override(Some(path))
                    .map_err(|e| e.to_string()),
                None => config_to_save.save().map_err(|e| e.to_string()),
            };
            // On success, return the exact config that was written to disk.
            // This eliminates the race where overrides change between save
            // dispatch and the success handler.
            save_result.map(|()| config_to_save)
        })
        .await;

        let final_result = match result {
            Ok(inner) => inner,
            Err(e) => Err(format!("Save task failed: {e}")),
        };
        let _ = tx.send(final_result);
    });
}

pub(super) fn handle_config_save_result(app: &mut App, result: Result<AppConfig, String>) {
    app.edit_popup.config_save_in_progress = false;
    app.edit_popup.config_save_rx = None;

    match result {
        Ok(saved_config) => {
            // Replace app.config with the exact config written to disk.
            // This eliminates the race where the user edits overrides while
            // the async save is in flight — we always converge to the saved state.
            app.config = saved_config;
            app.session_overrides.clear();
            app.session_http_timeout_seconds = None;
            app.session_model_fetch_timeout_seconds = None;
            app.session_cli_timeout_seconds = None;
            app.session_memory_enabled = None;
            app.session_memory_max_recall = None;
            app.session_memory_max_recall_bytes = None;
            app.session_memory_observation_ttl_days = None;
            app.session_memory_summary_ttl_days = None;
            app.session_memory_extraction_agent = None;
            app.session_memory_disable_extraction = None;
            app.session_memory_stale_permanent_days = None;
            app.error_modal = None;
            app.info_modal = Some("Config saved to disk".into());
        }
        Err(e) => {
            app.info_modal = None;
            app.error_modal = Some(format!("Failed to save config: {e}"));
        }
    }
}

pub(super) fn cycle_reasoning(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    let mut config = effective_section_config(app).unwrap_or_else(empty_provider_config);

    match kind {
        ProviderKind::OpenAI => {
            config.reasoning_effort = match config.reasoning_effort.as_deref() {
                None => Some("low".into()),
                Some("low") => Some("medium".into()),
                Some("medium") => Some("high".into()),
                Some("high") => Some("xhigh".into()),
                _ => None,
            };
        }
        ProviderKind::Anthropic => {
            config.thinking_effort = match config.thinking_effort.as_deref() {
                None => Some("low".into()),
                Some("low") => Some("medium".into()),
                Some("medium") => Some("high".into()),
                Some("high") => Some("max".into()),
                _ => None,
            };
        }
        ProviderKind::Gemini => {
            config.thinking_effort = match config.thinking_effort.as_deref() {
                None => Some("low".into()),
                Some("low") => Some("medium".into()),
                Some("medium") => Some("high".into()),
                _ => None,
            };
        }
    }
    set_section_config_override(app, config);
}

pub(super) fn toggle_diagnostic_agent(app: &mut App) {
    let agent_name = match app.config.agents.get(app.edit_popup.cursor) {
        Some(a) => a.name.clone(),
        None => return,
    };
    if app.config.diagnostic_provider.as_deref() == Some(agent_name.as_str()) {
        app.config.diagnostic_provider = None;
    } else {
        app.config.diagnostic_provider = Some(agent_name);
    }
}

pub(super) fn toggle_cli_mode(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    let cli_installed = app.cli_available.get(&kind).copied().unwrap_or(false);
    let mut config = effective_section_config(app).unwrap_or_else(empty_provider_config);

    if !config.use_cli && !cli_installed {
        app.error_modal = Some(format!("{} CLI not installed", kind.display_name()));
        return;
    }

    config.use_cli = !config.use_cli;
    set_section_config_override(app, config);
}

pub(super) fn toggle_cli_print_mode(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    if kind != ProviderKind::Anthropic {
        return;
    }
    let mut config = effective_section_config(app).unwrap_or_else(empty_provider_config);
    config.cli_print_mode = !config.cli_print_mode;
    set_section_config_override(app, config);
}

pub(super) fn add_new_agent(app: &mut App) {
    let existing: Vec<String> = app
        .config
        .agents
        .iter()
        .map(|a| a.name.to_lowercase())
        .collect();
    let mut idx = 1u32;
    let name = loop {
        let candidate = format!("Agent-{idx}");
        if !existing.contains(&candidate.to_lowercase()) {
            break candidate;
        }
        idx += 1;
    };
    app.config.agents.push(AgentConfig {
        name: name.clone(),
        provider: ProviderKind::Anthropic,
        api_key: String::new(),
        model: String::new(),
        reasoning_effort: None,
        thinking_effort: None,
        use_cli: false,
        cli_print_mode: true,
        extra_cli_args: String::new(),
    });
    app.edit_popup.cursor = app.config.agents.len() - 1;
    app.edit_popup.field = EditField::AgentName;
    app.edit_popup.edit_buffer = name;
    app.edit_popup.editing = true;
}

pub(super) fn remove_agent(app: &mut App) {
    if app.config.agents.len() <= 1 {
        app.error_modal = Some("Cannot remove the last agent".into());
        return;
    }
    let idx = app.edit_popup.cursor;
    if idx >= app.config.agents.len() {
        return;
    }
    let removed_name = app.config.agents[idx].name.clone();
    app.config.agents.remove(idx);
    app.session_overrides.remove(&removed_name);
    app.selected_agents.retain(|n| n != &removed_name);
    if app.config.diagnostic_provider.as_deref() == Some(removed_name.as_str()) {
        app.config.diagnostic_provider = None;
    }
    // Clear extraction agent if it referenced the removed agent
    if app.config.memory.extraction_agent == removed_name {
        app.config.memory.extraction_agent = String::new();
    }
    if app.session_memory_extraction_agent.as_deref() == Some(removed_name.as_str()) {
        app.session_memory_extraction_agent = None;
    }
    app.edit_popup.cursor = app
        .edit_popup
        .cursor
        .min(app.config.agents.len().saturating_sub(1));
}

pub(super) fn cycle_agent_provider(app: &mut App) {
    let idx = app.edit_popup.cursor;
    if let Some(agent) = app.config.agents.get_mut(idx) {
        let old_kind = agent.provider;
        agent.provider = match old_kind {
            ProviderKind::Anthropic => ProviderKind::OpenAI,
            ProviderKind::OpenAI => ProviderKind::Gemini,
            ProviderKind::Gemini => ProviderKind::Anthropic,
        };
        // Clear provider-specific effort
        match old_kind {
            ProviderKind::OpenAI => agent.reasoning_effort = None,
            ProviderKind::Anthropic | ProviderKind::Gemini => agent.thinking_effort = None,
        }
        let name = agent.name.clone();
        if let Some(ov) = app.session_overrides.get_mut(&name) {
            ov.provider = agent.provider;
            match old_kind {
                ProviderKind::OpenAI => ov.reasoning_effort = None,
                ProviderKind::Anthropic | ProviderKind::Gemini => ov.thinking_effort = None,
            }
        }
    }
}

pub(super) fn commit_agent_rename(app: &mut App) {
    let new_name = app.edit_popup.edit_buffer.trim().to_string();
    if new_name.is_empty() {
        app.error_modal = Some("Agent name cannot be empty".into());
        return;
    }
    let idx = app.edit_popup.cursor;
    let old_name = match app.config.agents.get(idx) {
        Some(a) => a.name.clone(),
        None => return,
    };
    // Check uniqueness (case-insensitive, excluding current)
    let lower = new_name.to_lowercase();
    for (i, a) in app.config.agents.iter().enumerate() {
        if i != idx && a.name.to_lowercase() == lower {
            app.error_modal = Some(format!("Agent name '{new_name}' already exists"));
            return;
        }
    }
    // Check sanitized collision
    let sanitized = OutputManager::sanitize_session_name(&new_name).to_lowercase();
    for (i, a) in app.config.agents.iter().enumerate() {
        if i != idx && OutputManager::sanitize_session_name(&a.name).to_lowercase() == sanitized {
            app.error_modal = Some(format!(
                "Agent name '{}' would collide with '{}' after sanitization",
                new_name, a.name
            ));
            return;
        }
    }
    // Apply rename
    app.config.agents[idx].name = new_name.clone();
    if let Some(ov) = app.session_overrides.remove(&old_name) {
        let mut updated = ov;
        updated.name = new_name.clone();
        app.session_overrides.insert(new_name.clone(), updated);
    }
    for entry in &mut app.selected_agents {
        if entry == &old_name {
            *entry = new_name.clone();
        }
    }
    if app.config.diagnostic_provider.as_deref() == Some(old_name.as_str()) {
        app.config.diagnostic_provider = Some(new_name.clone());
    }
    // Follow rename for extraction agent references
    if app.config.memory.extraction_agent == old_name {
        app.config.memory.extraction_agent = new_name.clone();
    }
    if app.session_memory_extraction_agent.as_deref() == Some(old_name.as_str()) {
        app.session_memory_extraction_agent = Some(new_name);
    }
    app.edit_popup.edit_buffer.clear();
    app.edit_popup.editing = false;
}

pub(super) fn timeout_field_count() -> usize {
    3
}

pub(super) fn begin_timeout_edit(app: &mut App) {
    app.edit_popup.field = EditField::TimeoutSeconds;
    app.edit_popup.edit_buffer = match app.edit_popup.timeout_cursor {
        0 => app.effective_http_timeout_seconds(),
        1 => app.effective_model_fetch_timeout_seconds(),
        2 => app.effective_cli_timeout_seconds(),
        _ => app.effective_http_timeout_seconds(),
    }
    .to_string();
    app.edit_popup.editing = true;
}

pub(super) fn set_timeout_override_from_buffer(app: &mut App) -> Result<(), String> {
    let raw = app.edit_popup.edit_buffer.trim();
    if raw.is_empty() {
        return Err("Timeout cannot be empty".into());
    }
    let parsed = raw
        .parse::<u64>()
        .map_err(|_| "Timeout must be a positive integer (seconds)".to_string())?;
    if parsed == 0 {
        return Err("Timeout must be at least 1 second".into());
    }

    match app.edit_popup.timeout_cursor {
        0 => app.session_http_timeout_seconds = Some(parsed),
        1 => app.session_model_fetch_timeout_seconds = Some(parsed),
        2 => app.session_cli_timeout_seconds = Some(parsed),
        _ => return Err("Invalid timeout field".into()),
    }

    Ok(())
}

// Memory config field indices. Keep in sync with the MemoryRow vec in
// screen/home.rs::draw_edit_popup (EditPopupSection::Memory branch).
pub(crate) const MEM_ENABLED: usize = 0;
pub(crate) const MEM_MAX_RECALL: usize = 1;
pub(crate) const MEM_MAX_RECALL_BYTES: usize = 2;
pub(crate) const MEM_OBSERVATION_TTL: usize = 3;
pub(crate) const MEM_SUMMARY_TTL: usize = 4;
pub(crate) const MEM_STALE_PERMANENT_DAYS: usize = 5;
pub(crate) const MEM_EXTRACTION_AGENT: usize = 6;
pub(crate) const MEM_DISABLE_EXTRACTION: usize = 7;
pub(crate) const MEM_FIELD_COUNT: usize = 8;

// Compile-time guard: if you add a new MEM_* field, bump MEM_FIELD_COUNT too.
const _: () = assert!(MEM_DISABLE_EXTRACTION + 1 == MEM_FIELD_COUNT);

/// Whether the given memory cursor index is a boolean toggle field.
pub(super) fn is_memory_toggle_field(cursor: usize) -> bool {
    matches!(cursor, MEM_ENABLED | MEM_DISABLE_EXTRACTION)
}

pub(super) fn toggle_memory_field(app: &mut App) {
    match app.edit_popup.memory_cursor {
        MEM_ENABLED => {
            let current = app.effective_memory_enabled();
            app.session_memory_enabled = Some(!current);
            if current {
                // Disabling: drop the store so a later re-enable (possibly
                // after an output_dir change) opens a fresh one.
                app.memory.store = None;
            } else if app.memory.store.is_none() {
                let db_path = if app.config.memory.db_path.is_empty() {
                    app.config.resolved_output_dir().join("memory.db")
                } else {
                    std::path::PathBuf::from(&app.config.memory.db_path)
                };
                match crate::memory::store::MemoryStore::open(&db_path) {
                    Ok(store) => {
                        let stale_days = app.effective_memory_stale_permanent_days();
                        if stale_days > 0 {
                            let _ = store.archive_stale_permanent(stale_days);
                        }
                        app.memory.store = Some(store);
                        app.memory.project_id = crate::memory::project::detect_project_id(
                            &app.config.memory.project_id,
                        );
                    }
                    Err(e) => {
                        app.error_modal = Some(format!("Failed to open memory store: {e}"));
                    }
                }
            }
        }
        MEM_DISABLE_EXTRACTION => {
            let current = app.effective_memory_disable_extraction();
            app.session_memory_disable_extraction = Some(!current);
        }
        _ => {}
    }
}

pub(super) fn begin_memory_edit(app: &mut App) {
    // Don't start text edit for toggle fields
    if is_memory_toggle_field(app.edit_popup.memory_cursor) {
        toggle_memory_field(app);
        return;
    }
    app.edit_popup.field = EditField::MemoryValue;
    app.edit_popup.edit_buffer = match app.edit_popup.memory_cursor {
        MEM_MAX_RECALL => app.effective_memory_max_recall().to_string(),
        MEM_MAX_RECALL_BYTES => app.effective_memory_max_recall_bytes().to_string(),
        MEM_OBSERVATION_TTL => app.effective_memory_observation_ttl_days().to_string(),
        MEM_SUMMARY_TTL => app.effective_memory_summary_ttl_days().to_string(),
        MEM_STALE_PERMANENT_DAYS => app.effective_memory_stale_permanent_days().to_string(),
        MEM_EXTRACTION_AGENT => app.effective_memory_extraction_agent().to_string(),
        _ => String::new(),
    };
    app.edit_popup.editing = true;
}

pub(super) fn set_memory_override_from_buffer(app: &mut App) -> Result<(), String> {
    let raw = app.edit_popup.edit_buffer.trim().to_string();
    match app.edit_popup.memory_cursor {
        MEM_MAX_RECALL => {
            let v = raw
                .parse::<usize>()
                .map_err(|_| "Max recall must be a positive integer".to_string())?;
            if v == 0 {
                return Err("Max recall must be at least 1".into());
            }
            app.session_memory_max_recall = Some(v);
        }
        MEM_MAX_RECALL_BYTES => {
            let v = raw
                .parse::<usize>()
                .map_err(|_| "Max recall bytes must be a positive integer".to_string())?;
            if v == 0 {
                return Err("Max recall bytes must be at least 1".into());
            }
            app.session_memory_max_recall_bytes = Some(v);
        }
        MEM_OBSERVATION_TTL => {
            let v = raw
                .parse::<u32>()
                .map_err(|_| "Observation TTL must be a positive integer".to_string())?;
            if v == 0 {
                return Err("Observation TTL must be at least 1 day".into());
            }
            app.session_memory_observation_ttl_days = Some(v);
        }
        MEM_SUMMARY_TTL => {
            let v = raw
                .parse::<u32>()
                .map_err(|_| "Summary TTL must be a positive integer".to_string())?;
            if v == 0 {
                return Err("Summary TTL must be at least 1 day".into());
            }
            app.session_memory_summary_ttl_days = Some(v);
        }
        MEM_STALE_PERMANENT_DAYS => {
            let v = raw
                .parse::<u32>()
                .map_err(|_| "Stale permanent days must be a non-negative integer".to_string())?;
            app.session_memory_stale_permanent_days = Some(v);
        }
        MEM_EXTRACTION_AGENT => {
            // Empty string means "auto" — always valid.
            if !raw.is_empty()
                && !app.config.agents.iter().any(|a| a.name == raw)
                && !app.session_overrides.values().any(|a| a.name == raw)
            {
                let mut names: Vec<&str> =
                    app.config.agents.iter().map(|a| a.name.as_str()).collect();
                for ov in app.session_overrides.values() {
                    if !names.contains(&ov.name.as_str()) {
                        names.push(&ov.name);
                    }
                }
                return Err(format!(
                    "No agent named '{}'. Available: {}",
                    raw,
                    names.join(", ")
                ));
            }
            app.session_memory_extraction_agent = Some(raw);
        }
        _ => return Err("Invalid memory field".into()),
    }
    Ok(())
}

pub(super) fn selected_kind_for_edit(app: &App) -> Option<ProviderKind> {
    app.config
        .agents
        .get(app.edit_popup.cursor)
        .map(|a| a.provider)
}

/// Returns a ProviderConfig view for the current edit selection (Providers section).
pub(super) fn effective_section_config(app: &App) -> Option<ProviderConfig> {
    let name = app
        .config
        .agents
        .get(app.edit_popup.cursor)
        .map(|a| a.name.as_str())?;
    app.effective_agent_config(name)
        .map(|a| a.to_provider_config())
}

pub(super) fn set_section_config_override(app: &mut App, config: ProviderConfig) {
    if let Some(agent) = app.config.agents.get(app.edit_popup.cursor) {
        let name = agent.name.clone();
        let provider = agent.provider;
        let agent_config = AgentConfig::from_provider_config(name.clone(), provider, &config);
        app.session_overrides.insert(name, agent_config);
    }
}

pub(super) fn empty_provider_config() -> ProviderConfig {
    ProviderConfig {
        api_key: String::new(),
        model: String::new(),
        reasoning_effort: None,
        thinking_effort: None,
        use_cli: false,
        cli_print_mode: true,
        extra_cli_args: String::new(),
    }
}

pub(super) fn resume_allowed_in_prompt(app: &App) -> bool {
    !(app.prompt.runs > 1
        && matches!(
            app.selected_mode,
            ExecutionMode::Relay | ExecutionMode::Swarm
        ))
}

pub(super) fn enforce_prompt_resume_constraints(app: &mut App) {
    if !resume_allowed_in_prompt(app) {
        app.prompt.resume_previous = false;
    }
}

pub(super) fn sync_iterations_buf(app: &mut App) {
    if app.prompt.iterations_buf.is_empty() {
        app.prompt.iterations = 1;
    } else {
        app.prompt.iterations = app
            .prompt
            .iterations_buf
            .parse()
            .unwrap_or(1)
            .clamp(1, MAX_NUMERIC_FIELD);
    }
    app.prompt.iterations_buf = app.prompt.iterations.to_string();
}

pub(super) fn sync_runs_buf(app: &mut App) {
    if app.prompt.runs_buf.is_empty() {
        app.prompt.runs = 1;
    } else {
        app.prompt.runs = app
            .prompt
            .runs_buf
            .parse()
            .unwrap_or(1)
            .clamp(1, MAX_NUMERIC_FIELD);
    }
    app.prompt.runs_buf = app.prompt.runs.to_string();
}

pub(super) fn sync_concurrency_buf(app: &mut App) {
    if app.prompt.concurrency_buf.is_empty() {
        app.prompt.concurrency = 0;
    } else {
        app.prompt.concurrency = app
            .prompt
            .concurrency_buf
            .parse()
            .unwrap_or(0)
            .min(MAX_NUMERIC_FIELD);
    }
    app.prompt.concurrency_buf = app.prompt.concurrency.to_string();
}

pub(super) fn sync_pipeline_iterations_buf(app: &mut App) {
    if app.pipeline.pipeline_iterations_buf.is_empty() {
        app.pipeline.pipeline_def.iterations = 1;
    } else {
        app.pipeline.pipeline_def.iterations = app
            .pipeline
            .pipeline_iterations_buf
            .parse()
            .unwrap_or(1)
            .clamp(1, MAX_NUMERIC_FIELD);
    }
    app.pipeline.pipeline_iterations_buf = app.pipeline.pipeline_def.iterations.to_string();
}

pub(super) fn sync_pipeline_runs_buf(app: &mut App) {
    if app.pipeline.pipeline_runs_buf.is_empty() {
        app.pipeline.pipeline_runs = 1;
    } else {
        app.pipeline.pipeline_runs = app
            .pipeline
            .pipeline_runs_buf
            .parse()
            .unwrap_or(1)
            .clamp(1, MAX_NUMERIC_FIELD);
    }
    app.pipeline.pipeline_runs_buf = app.pipeline.pipeline_runs.to_string();
}

pub(super) fn sync_pipeline_concurrency_buf(app: &mut App) {
    if app.pipeline.pipeline_concurrency_buf.is_empty() {
        app.pipeline.pipeline_concurrency = 0;
    } else {
        app.pipeline.pipeline_concurrency = app
            .pipeline
            .pipeline_concurrency_buf
            .parse()
            .unwrap_or(0)
            .min(MAX_NUMERIC_FIELD);
    }
    app.pipeline.pipeline_concurrency_buf = app.pipeline.pipeline_concurrency.to_string();
}

fn pipeline_edit_max_replicas(app: &App) -> u32 {
    let selected = app
        .pipeline
        .pipeline_edit_agent_selection
        .iter()
        .filter(|&&s| s)
        .count()
        .max(1) as u32;
    (32 / selected).max(1)
}

fn sync_pipeline_edit_replicas_buf(app: &mut App) {
    let max = pipeline_edit_max_replicas(app);
    let v: u32 = app
        .pipeline
        .pipeline_edit_replicas_buf
        .parse()
        .unwrap_or(1)
        .clamp(1, max);
    app.pipeline.pipeline_edit_replicas_buf = v.to_string();
}

// ---------------------------------------------------------------------------
// Memory management screen
// ---------------------------------------------------------------------------

fn refresh_memory_list(app: &mut App) {
    if let Some(ref store) = app.memory.store {
        if let Ok(memories) = store.list(
            &app.memory.project_id,
            app.memory.management_kind_filter,
            app.memory.management_never_recalled_filter,
            app.memory.management_show_archived,
        ) {
            app.memory.management_memories = memories;
            if app.memory.management_cursor >= app.memory.management_memories.len() {
                app.memory.management_cursor =
                    app.memory.management_memories.len().saturating_sub(1);
            }
        }
        app.memory.management_total_count = store
            .count(
                &app.memory.project_id,
                app.memory.management_kind_filter,
                app.memory.management_never_recalled_filter,
                app.memory.management_show_archived,
            )
            .unwrap_or(app.memory.management_memories.len() as u64);
        app.memory.cached_db_size = store.db_size_bytes();
    }
}

fn handle_memory_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            app.memory.management_cursor = app.memory.management_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            let max = app.memory.management_memories.len().saturating_sub(1);
            if app.memory.management_cursor < max {
                app.memory.management_cursor += 1;
            }
        }
        KeyCode::Char('d') => {
            if let Some(mem) = app
                .memory
                .management_memories
                .get(app.memory.management_cursor)
            {
                let id = mem.id;
                if let Some(ref store) = app.memory.store {
                    let _ = store.delete(id);
                }
                refresh_memory_list(app);
            }
        }
        KeyCode::Char('D') => {
            if app.memory.management_memories.is_empty() {
                return;
            }
            if app.memory.pending_bulk_delete {
                // Second D: execute bulk delete in a single batched statement
                let ids: Vec<i64> = app
                    .memory
                    .management_memories
                    .iter()
                    .map(|m| m.id)
                    .collect();
                if let Some(ref store) = app.memory.store {
                    let _ = store.delete_batch(&ids);
                }
                app.memory.pending_bulk_delete = false;
                refresh_memory_list(app);
            } else {
                // First D: arm confirmation
                app.memory.pending_bulk_delete = true;
            }
            return; // skip the pending_bulk_delete reset below
        }
        KeyCode::Char('f') => {
            // Cycle kind filter: None → Decision → Observation → Summary → Principle → None
            use crate::memory::types::MemoryKind;
            app.memory.management_kind_filter = match app.memory.management_kind_filter {
                None => Some(MemoryKind::Decision),
                Some(MemoryKind::Decision) => Some(MemoryKind::Observation),
                Some(MemoryKind::Observation) => Some(MemoryKind::Summary),
                Some(MemoryKind::Summary) => Some(MemoryKind::Principle),
                Some(MemoryKind::Principle) => None,
            };
            refresh_memory_list(app);
        }
        KeyCode::Char('r') => {
            // Toggle "never recalled" filter
            app.memory.management_never_recalled_filter =
                !app.memory.management_never_recalled_filter;
            refresh_memory_list(app);
        }
        KeyCode::Char('a') => {
            app.memory.management_show_archived = !app.memory.management_show_archived;
            app.memory.management_cursor = 0;
            refresh_memory_list(app);
        }
        KeyCode::Char('u') => {
            if app.memory.management_show_archived {
                if let Some(mem) = app
                    .memory
                    .management_memories
                    .get(app.memory.management_cursor)
                {
                    let id = mem.id;
                    if let Some(ref store) = app.memory.store {
                        let _ = store.unarchive(id);
                    }
                    refresh_memory_list(app);
                }
            }
        }
        KeyCode::Esc | KeyCode::Char('q') => {
            app.screen = Screen::Home;
        }
        _ => {}
    }
    // Any key other than D clears the pending bulk-delete confirmation.
    // The D arm returns early to skip this line.
    app.memory.pending_bulk_delete = false;
}
