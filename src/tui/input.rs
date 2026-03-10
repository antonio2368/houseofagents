use super::consolidation::start_consolidation;
use super::diagnostics::maybe_start_diagnostics;
use super::execution::{start_execution, start_pipeline_execution};
use super::results::{
    batch_result_entry_at, batch_result_visible_len, has_batch_result_tree, load_results,
    update_preview, BatchResultEntry,
};
use super::text_edit::*;
use super::*;

pub(super) fn handle_key(app: &mut App, key: KeyEvent) {
    // Ctrl+C: graceful quit from anywhere
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if app.running.is_running {
            app.running.cancel_flag.store(true, Ordering::Relaxed);
        }
        app.should_quit = true;
        return;
    }

    // Dismiss error modal with any key
    if app.error_modal.is_some() {
        app.error_modal = None;
        return;
    }

    // Help popup has dedicated scroll + close handling
    if app.show_help_popup {
        handle_help_popup_key(app, key);
        return;
    }

    // Edit popup handling
    if app.edit_popup.show_edit_popup {
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
    }
}

pub(super) fn handle_paste(app: &mut App, text: &str) {
    if app.error_modal.is_some() {
        app.error_modal = None;
        return;
    }

    if app.show_help_popup {
        return;
    }

    if app.edit_popup.show_edit_popup {
        if app.edit_popup.edit_popup_editing {
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
            app.show_help_popup = true;
            app.help_popup_scroll = 0;
        }
        KeyCode::Char('e') => {
            app.edit_popup.show_edit_popup = true;
            app.edit_popup.edit_popup_section = EditPopupSection::Providers;
            app.edit_popup.edit_popup_cursor = 0;
            app.edit_popup.edit_popup_timeout_cursor = 0;
            app.edit_popup.edit_popup_editing = false;
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
            app.show_help_popup = false;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.help_popup_scroll = app.help_popup_scroll.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.help_popup_scroll = app.help_popup_scroll.saturating_add(1);
        }
        KeyCode::PageUp => {
            app.help_popup_scroll = app.help_popup_scroll.saturating_sub(8);
        }
        KeyCode::PageDown => {
            app.help_popup_scroll = app.help_popup_scroll.saturating_add(8);
        }
        KeyCode::Home => {
            app.help_popup_scroll = 0;
        }
        KeyCode::End => {
            app.help_popup_scroll = u16::MAX;
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
                (PromptFocus::Resume, _) => PromptFocus::Text,
                (PromptFocus::ForwardPrompt, _) => PromptFocus::Text,
            };
        }
        KeyCode::BackTab => {
            app.prompt.prompt_focus = match (&app.prompt.prompt_focus, app.selected_mode) {
                (PromptFocus::Text, ExecutionMode::Relay) => PromptFocus::ForwardPrompt,
                (PromptFocus::Text, _) => PromptFocus::Resume,
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
            };
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
                    app.prompt.iterations =
                        app.prompt.iterations_buf.parse().unwrap_or(1).clamp(1, 99);
                    app.prompt.iterations_buf = app.prompt.iterations.to_string();
                }
                KeyCode::Backspace => {
                    app.prompt.iterations_buf.pop();
                    if app.prompt.iterations_buf.is_empty() {
                        app.prompt.iterations = 1;
                    } else {
                        app.prompt.iterations =
                            app.prompt.iterations_buf.parse().unwrap_or(1).clamp(1, 99);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.prompt.iterations = (app.prompt.iterations + 1).min(99);
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
                    app.prompt.runs = (app.prompt.runs + 1).min(99);
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
                    app.prompt.concurrency = (app.prompt.concurrency + 1).min(99);
                    app.prompt.concurrency_buf = app.prompt.concurrency.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.prompt.concurrency = app.prompt.concurrency.saturating_sub(1);
                    app.prompt.concurrency_buf = app.prompt.concurrency.to_string();
                }
                _ => {}
            },
            PromptFocus::Resume | PromptFocus::ForwardPrompt => {}
        },
    }
}

// ---------------------------------------------------------------------------
// Generic text editing helpers (buffer, cursor) — used by prompt and pipeline
// ---------------------------------------------------------------------------

pub(super) fn handle_pipeline_paste(app: &mut App, text: &str) {
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
        }
    } else if let Some(PipelineDialogMode::Save) = app.pipeline.pipeline_file_dialog {
        app.pipeline.pipeline_file_input.push_str(text);
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
                        .clamp(1, 99);
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
    // Dispatch priority: edit popup > file dialog > remove-conn > connect mode > normal
    if app.pipeline.pipeline_show_edit {
        handle_pipeline_edit_key(app, key);
        return;
    }
    if app.pipeline.pipeline_file_dialog.is_some() {
        handle_pipeline_dialog_key(app, key);
        return;
    }
    if app.pipeline.pipeline_removing_conn {
        handle_pipeline_remove_conn_key(app, key);
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
                    if from == to {
                        app.error_modal = Some("Cannot connect block to itself".into());
                    } else if app
                        .pipeline
                        .pipeline_def
                        .connections
                        .iter()
                        .any(|c| c.from == from && c.to == to)
                    {
                        app.error_modal = Some("Connection already exists".into());
                    } else if pipeline_mod::would_create_cycle(&app.pipeline.pipeline_def, from, to)
                    {
                        app.error_modal = Some("Would create a cycle".into());
                    } else {
                        app.pipeline
                            .pipeline_def
                            .connections
                            .push(pipeline_mod::PipelineConnection { from, to });
                        app.pipeline.pipeline_connecting_from = None;
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
        KeyCode::Char('?') if !ctrl => {
            app.show_help_popup = true;
            app.help_popup_scroll = 0;
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
        }
        // F5: run
        KeyCode::F(5) => {
            start_pipeline_execution(app);
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
                        (app.pipeline.pipeline_def.iterations + 1).min(99);
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
                    app.pipeline.pipeline_runs = (app.pipeline.pipeline_runs + 1).min(99);
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
                        (app.pipeline.pipeline_concurrency + 1).min(99);
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
        KeyCode::Char('a') => {
            let pos = pipeline_mod::next_free_position(&app.pipeline.pipeline_def);
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
                    agent: default_agent,
                    prompt: String::new(),
                    session_id: None,
                    position: pos,
                });
            app.pipeline.pipeline_block_cursor = Some(id);
            pipeline_ensure_visible(app);
        }
        KeyCode::Char('d') => {
            if let Some(sel) = app.pipeline.pipeline_block_cursor {
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
                if app.pipeline.pipeline_def.blocks.is_empty() {
                    app.pipeline.pipeline_block_cursor = None;
                } else {
                    let new_idx = old_index.min(app.pipeline.pipeline_def.blocks.len() - 1);
                    app.pipeline.pipeline_block_cursor =
                        Some(app.pipeline.pipeline_def.blocks[new_idx].id);
                    pipeline_ensure_visible(app);
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
                {
                    app.pipeline.pipeline_show_edit = true;
                    app.pipeline.pipeline_edit_field = PipelineEditField::Name;
                    app.pipeline.pipeline_edit_name_buf = block.name.clone();
                    app.pipeline.pipeline_edit_name_cursor = block.name.len();
                    app.pipeline.pipeline_edit_agent_idx = app
                        .config
                        .agents
                        .iter()
                        .position(|a| a.name == block.agent)
                        .unwrap_or(0);
                    app.pipeline.pipeline_edit_prompt_buf = block.prompt.clone();
                    app.pipeline.pipeline_edit_prompt_cursor = block.prompt.len();
                    app.pipeline.pipeline_edit_session_buf =
                        block.session_id.clone().unwrap_or_default();
                    app.pipeline.pipeline_edit_session_cursor =
                        app.pipeline.pipeline_edit_session_buf.len();
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
                let conn_count = app
                    .pipeline
                    .pipeline_def
                    .connections
                    .iter()
                    .filter(|c| c.from == sel || c.to == sel)
                    .count();
                if conn_count == 0 {
                    app.error_modal = Some("No connections on this block".into());
                } else {
                    app.pipeline.pipeline_removing_conn = true;
                    app.pipeline.pipeline_conn_cursor = 0;
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
    if app.pipeline.pipeline_def.blocks.is_empty() {
        app.pipeline.pipeline_block_cursor = None;
        return;
    }

    let Some(sel_id) = app.pipeline.pipeline_block_cursor else {
        app.pipeline.pipeline_block_cursor = app.pipeline.pipeline_def.blocks.first().map(|b| b.id);
        return;
    };
    let Some(sel_idx) = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .position(|b| b.id == sel_id)
    else {
        app.pipeline.pipeline_block_cursor = app.pipeline.pipeline_def.blocks.first().map(|b| b.id);
        return;
    };

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
}

pub(super) fn pipeline_spatial_nav(app: &mut App, axis: NavAxis, negative: bool) {
    let Some(sel_id) = app.pipeline.pipeline_block_cursor else {
        // Select first block if none selected
        app.pipeline.pipeline_block_cursor = app.pipeline.pipeline_def.blocks.first().map(|b| b.id);
        return;
    };
    let Some(sel_block) = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .find(|b| b.id == sel_id)
    else {
        return;
    };
    let (sx, sy) = (sel_block.position.0 as i32, sel_block.position.1 as i32);

    let mut best: Option<(BlockId, i32)> = None;
    for block in &app.pipeline.pipeline_def.blocks {
        if block.id == sel_id {
            continue;
        }
        let (bx, by) = (block.position.0 as i32, block.position.1 as i32);
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
            best = Some((block.id, dist));
        }
    }
    if let Some((id, _)) = best {
        app.pipeline.pipeline_block_cursor = Some(id);
    }
}

pub(super) fn pipeline_ensure_visible(app: &mut App) {
    use crate::screen::pipeline::{BLOCK_H, BLOCK_W, CELL_H, CELL_W};

    let Some(sel_id) = app.pipeline.pipeline_block_cursor else {
        return;
    };
    let Some(block) = app
        .pipeline
        .pipeline_def
        .blocks
        .iter()
        .find(|b| b.id == sel_id)
    else {
        return;
    };

    let sx = block.position.0 as i16 * CELL_W as i16;
    let sy = block.position.1 as i16 * CELL_H as i16;

    // Canvas inner ≈ terminal size minus chrome (title 3 + prompt 6 + help 2 + borders 2)
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
        KeyCode::Tab => {
            app.pipeline.pipeline_edit_field = match app.pipeline.pipeline_edit_field {
                PipelineEditField::Name => PipelineEditField::Agent,
                PipelineEditField::Agent => PipelineEditField::Prompt,
                PipelineEditField::Prompt => PipelineEditField::SessionId,
                PipelineEditField::SessionId => PipelineEditField::Name,
            };
        }
        KeyCode::Enter => {
            match app.pipeline.pipeline_edit_field {
                PipelineEditField::Name
                | PipelineEditField::Agent
                | PipelineEditField::SessionId => {
                    // Confirm and save
                    if let Some(sel) = app.pipeline.pipeline_block_cursor {
                        if let Some(block) = app
                            .pipeline
                            .pipeline_def
                            .blocks
                            .iter_mut()
                            .find(|b| b.id == sel)
                        {
                            block.name = app.pipeline.pipeline_edit_name_buf.clone();
                            block.agent = app
                                .config
                                .agents
                                .get(app.pipeline.pipeline_edit_agent_idx)
                                .map(|a| a.name.clone())
                                .unwrap_or_else(|| "Claude".to_string());
                            block.prompt = app.pipeline.pipeline_edit_prompt_buf.clone();
                            block.session_id = if app.pipeline.pipeline_edit_session_buf.is_empty()
                            {
                                None
                            } else {
                                Some(app.pipeline.pipeline_edit_session_buf.clone())
                            };
                        }
                    }
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
                KeyCode::Left | KeyCode::Char('k') => {
                    let len = app.config.agents.len().max(1);
                    app.pipeline.pipeline_edit_agent_idx =
                        (app.pipeline.pipeline_edit_agent_idx + len - 1) % len;
                }
                KeyCode::Right | KeyCode::Char('j') => {
                    let len = app.config.agents.len().max(1);
                    app.pipeline.pipeline_edit_agent_idx =
                        (app.pipeline.pipeline_edit_agent_idx + 1) % len;
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
        Some(PipelineDialogMode::Load) => match key.code {
            KeyCode::Esc => {
                app.pipeline.pipeline_file_dialog = None;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.pipeline.pipeline_file_cursor =
                    app.pipeline.pipeline_file_cursor.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if app.pipeline.pipeline_file_cursor + 1 < app.pipeline.pipeline_file_list.len() {
                    app.pipeline.pipeline_file_cursor += 1;
                }
            }
            KeyCode::Enter => {
                if let Some(filename) = app
                    .pipeline
                    .pipeline_file_list
                    .get(app.pipeline.pipeline_file_cursor)
                {
                    let path = pipeline_mod::pipelines_dir().join(filename);
                    match pipeline_mod::load_pipeline(&path) {
                        Ok(def) => {
                            let max_id = def.blocks.iter().map(|b| b.id).max().unwrap_or(0);
                            app.pipeline.pipeline_next_id = max_id + 1;
                            app.pipeline.pipeline_def = def;
                            app.pipeline.pipeline_save_path = Some(path);
                            app.pipeline.pipeline_block_cursor =
                                app.pipeline.pipeline_def.blocks.first().map(|b| b.id);
                            app.pipeline.pipeline_file_dialog = None;
                        }
                        Err(e) => {
                            app.error_modal = Some(format!("Load failed: {e}"));
                        }
                    }
                }
            }
            _ => {}
        },
        None => {}
    }
}

pub(super) fn handle_pipeline_remove_conn_key(app: &mut App, key: KeyEvent) {
    let sel = app.pipeline.pipeline_block_cursor.unwrap_or(0);
    let conns: Vec<usize> = app
        .pipeline
        .pipeline_def
        .connections
        .iter()
        .enumerate()
        .filter(|(_, c)| c.from == sel || c.to == sel)
        .map(|(i, _)| i)
        .collect();

    match key.code {
        KeyCode::Esc => {
            app.pipeline.pipeline_removing_conn = false;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.pipeline.pipeline_conn_cursor = app.pipeline.pipeline_conn_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.pipeline.pipeline_conn_cursor + 1 < conns.len() {
                app.pipeline.pipeline_conn_cursor += 1;
            }
        }
        KeyCode::Enter => {
            if let Some(&conn_idx) = conns.get(app.pipeline.pipeline_conn_cursor) {
                app.pipeline.pipeline_def.connections.remove(conn_idx);
            }
            app.pipeline.pipeline_removing_conn = false;
        }
        _ => {}
    }
}

pub(super) fn handle_order_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.screen = Screen::Prompt;
            app.order_grabbed = None;
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
        .filter(|t| {
            app.stream_buffer(t)
                .is_some_and(|b| !b.text().is_empty())
        })
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
            if let Some(BatchResultEntry::RunHeader(run_id)) =
                batch_result_entry_at(app, app.results.result_cursor)
            {
                if !app.results.batch_result_expanded.insert(run_id) {
                    app.results.batch_result_expanded.remove(&run_id);
                }
                update_preview(app);
            } else {
                app.reset_to_home();
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

    match key.code {
        KeyCode::Esc => {
            if app.edit_popup.edit_popup_editing {
                app.edit_popup.edit_popup_editing = false;
                app.edit_popup.edit_buffer.clear();
            } else {
                app.edit_popup.show_edit_popup = false;
            }
        }
        KeyCode::Tab if !app.edit_popup.edit_popup_editing => {
            app.edit_popup.edit_popup_section = match app.edit_popup.edit_popup_section {
                EditPopupSection::Providers => EditPopupSection::Timeouts,
                EditPopupSection::Timeouts => EditPopupSection::Providers,
            };
            app.edit_popup.edit_buffer.clear();
        }
        KeyCode::Up | KeyCode::Char('k') if !app.edit_popup.edit_popup_editing => {
            match app.edit_popup.edit_popup_section {
                EditPopupSection::Providers => {
                    app.edit_popup.edit_popup_cursor =
                        app.edit_popup.edit_popup_cursor.saturating_sub(1);
                }
                EditPopupSection::Timeouts => {
                    app.edit_popup.edit_popup_timeout_cursor =
                        app.edit_popup.edit_popup_timeout_cursor.saturating_sub(1);
                }
            }
        }
        KeyCode::Down | KeyCode::Char('j') if !app.edit_popup.edit_popup_editing => {
            match app.edit_popup.edit_popup_section {
                EditPopupSection::Providers => {
                    let max = app.config.agents.len().saturating_sub(1);
                    if app.edit_popup.edit_popup_cursor < max {
                        app.edit_popup.edit_popup_cursor += 1;
                    }
                }
                EditPopupSection::Timeouts => {
                    let max = timeout_field_count().saturating_sub(1);
                    if app.edit_popup.edit_popup_timeout_cursor < max {
                        app.edit_popup.edit_popup_timeout_cursor += 1;
                    }
                }
            }
        }
        KeyCode::Char('a')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            app.edit_popup.edit_popup_field = EditField::ApiKey;
            app.edit_popup.edit_buffer = effective_section_config(app)
                .map(|c| c.api_key)
                .unwrap_or_default();
            app.edit_popup.edit_popup_editing = true;
        }
        KeyCode::Char('m')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            app.edit_popup.edit_popup_field = EditField::Model;
            app.edit_popup.edit_buffer = effective_section_config(app)
                .map(|c| c.model)
                .unwrap_or_default();
            app.edit_popup.edit_popup_editing = true;
        }
        KeyCode::Char('x')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            app.edit_popup.edit_popup_field = EditField::ExtraCliArgs;
            app.edit_popup.edit_buffer = effective_section_config(app)
                .map(|c| c.extra_cli_args)
                .unwrap_or_default();
            app.edit_popup.edit_popup_editing = true;
        }
        KeyCode::Char('l')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            app.edit_popup.edit_popup_field = EditField::Model;
            start_model_fetch(app);
        }
        KeyCode::Char('o') if !app.edit_popup.edit_popup_editing => {
            app.edit_popup.edit_popup_field = EditField::OutputDir;
            app.edit_popup.edit_buffer = app.config.output_dir.clone();
            app.edit_popup.edit_popup_editing = true;
        }
        KeyCode::Char('s') if !app.edit_popup.edit_popup_editing => {
            save_config_globally(app);
        }
        KeyCode::Char('c')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            toggle_cli_mode(app);
        }
        KeyCode::Char('d')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            toggle_diagnostic_agent(app);
        }
        KeyCode::Char('t')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            cycle_reasoning(app);
        }
        KeyCode::Char('b')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            toggle_cli_print_mode(app);
        }
        KeyCode::Char('n')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            add_new_agent(app);
        }
        KeyCode::Char('p')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            cycle_agent_provider(app);
        }
        KeyCode::Char('r')
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            if let Some(agent) = app.config.agents.get(app.edit_popup.edit_popup_cursor) {
                app.edit_popup.edit_popup_field = EditField::AgentName;
                app.edit_popup.edit_buffer = agent.name.clone();
                app.edit_popup.edit_popup_editing = true;
            }
        }
        KeyCode::Delete | KeyCode::Backspace
            if !app.edit_popup.edit_popup_editing
                && app.edit_popup.edit_popup_section == EditPopupSection::Providers =>
        {
            remove_agent(app);
        }
        KeyCode::Char('e') if !app.edit_popup.edit_popup_editing => {
            if app.edit_popup.edit_popup_section == EditPopupSection::Timeouts {
                begin_timeout_edit(app);
            }
        }
        KeyCode::Enter if !app.edit_popup.edit_popup_editing => {
            if app.edit_popup.edit_popup_section == EditPopupSection::Timeouts {
                begin_timeout_edit(app);
            } else {
                app.edit_popup.edit_popup_field = EditField::ApiKey;
                app.edit_popup.edit_buffer = effective_section_config(app)
                    .map(|c| c.api_key)
                    .unwrap_or_default();
                app.edit_popup.edit_popup_editing = true;
            }
        }
        KeyCode::Enter if app.edit_popup.edit_popup_editing => {
            if matches!(app.edit_popup.edit_popup_field, EditField::OutputDir) {
                let new_output_dir = app.edit_popup.edit_buffer.trim();
                if new_output_dir.is_empty() {
                    app.error_modal = Some("Output directory cannot be empty".into());
                } else {
                    app.config.output_dir = new_output_dir.to_string();
                }
            } else if matches!(app.edit_popup.edit_popup_field, EditField::AgentName) {
                commit_agent_rename(app);
                return;
            } else if app.edit_popup.edit_popup_section == EditPopupSection::Timeouts
                || matches!(app.edit_popup.edit_popup_field, EditField::TimeoutSeconds)
            {
                if let Err(e) = set_timeout_override_from_buffer(app) {
                    app.error_modal = Some(e);
                    return;
                }
            } else {
                let mut config =
                    effective_section_config(app).unwrap_or_else(empty_provider_config);
                match app.edit_popup.edit_popup_field {
                    EditField::ApiKey => config.api_key = app.edit_popup.edit_buffer.clone(),
                    EditField::Model => config.model = app.edit_popup.edit_buffer.clone(),
                    EditField::ExtraCliArgs => {
                        config.extra_cli_args = app.edit_popup.edit_buffer.clone()
                    }
                    EditField::OutputDir | EditField::TimeoutSeconds | EditField::AgentName => {}
                }
                set_section_config_override(app, config);
            }
            app.edit_popup.edit_buffer.clear();
            app.edit_popup.edit_popup_editing = false;
        }
        KeyCode::Backspace if app.edit_popup.edit_popup_editing => {
            app.edit_popup.edit_buffer.pop();
        }
        KeyCode::Char(c) if app.edit_popup.edit_popup_editing => {
            if matches!(app.edit_popup.edit_popup_field, EditField::TimeoutSeconds) {
                if c.is_ascii_digit() {
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
            app.edit_popup.model_picker_all_models.clear();
            app.edit_popup.model_picker_filter.clear();
        }
        KeyCode::Up => {
            app.edit_popup.model_picker_cursor =
                app.edit_popup.model_picker_cursor.saturating_sub(1);
        }
        KeyCode::Down => {
            if app.edit_popup.model_picker_cursor
                < app.edit_popup.model_picker_list.len().saturating_sub(1)
            {
                app.edit_popup.model_picker_cursor += 1;
            }
        }
        KeyCode::Enter => {
            if let Some(model) = app
                .edit_popup
                .model_picker_list
                .get(app.edit_popup.model_picker_cursor)
                .cloned()
            {
                let mut config =
                    effective_section_config(app).unwrap_or_else(empty_provider_config);
                config.model = model;
                set_section_config_override(app, config);
            }
            app.edit_popup.model_picker_active = false;
            app.edit_popup.model_picker_list.clear();
            app.edit_popup.model_picker_all_models.clear();
            app.edit_popup.model_picker_filter.clear();
        }
        KeyCode::Char(c) => {
            app.edit_popup.model_picker_filter.push(c);
            apply_model_filter(app);
        }
        KeyCode::Backspace => {
            app.edit_popup.model_picker_filter.pop();
            apply_model_filter(app);
        }
        _ => {}
    }
}

pub(super) fn apply_model_filter(app: &mut App) {
    let filter = app.edit_popup.model_picker_filter.to_lowercase();
    app.edit_popup.model_picker_list = if filter.is_empty() {
        app.edit_popup.model_picker_all_models.clone()
    } else {
        app.edit_popup
            .model_picker_all_models
            .iter()
            .filter(|m| m.to_lowercase().contains(&filter))
            .cloned()
            .collect()
    };
    app.edit_popup.model_picker_cursor = 0;
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
    app.edit_popup.model_picker_all_models.clear();
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
            app.edit_popup.model_picker_all_models = models.clone();
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

    // Merge session agent overrides into config.agents
    for (name, override_agent) in &app.session_overrides {
        if let Some(idx) = app.config.agents.iter().position(|a| &a.name == name) {
            app.config.agents[idx] = override_agent.clone();
        }
    }
    if let Some(value) = app.session_http_timeout_seconds {
        app.config.http_timeout_seconds = value;
    }
    if let Some(value) = app.session_model_fetch_timeout_seconds {
        app.config.model_fetch_timeout_seconds = value;
    }
    if let Some(value) = app.session_cli_timeout_seconds {
        app.config.cli_timeout_seconds = value;
    }
    app.edit_popup.config_save_in_progress = true;
    let config_to_save = app.config.clone();
    let path_override = app.config_path_override.clone();

    let (tx, rx) = mpsc::unbounded_channel();
    app.edit_popup.config_save_rx = Some(rx);

    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || match path_override.as_deref() {
            Some(path) => config_to_save
                .save_with_override(Some(path))
                .map_err(|e| e.to_string()),
            None => config_to_save.save().map_err(|e| e.to_string()),
        })
        .await;

        let final_result = match result {
            Ok(inner) => inner,
            Err(e) => Err(format!("Save task failed: {e}")),
        };
        let _ = tx.send(final_result);
    });
}

pub(super) fn handle_config_save_result(app: &mut App, result: Result<(), String>) {
    app.edit_popup.config_save_in_progress = false;
    app.edit_popup.config_save_rx = None;

    match result {
        Ok(()) => {
            app.session_overrides.clear();
            app.session_http_timeout_seconds = None;
            app.session_model_fetch_timeout_seconds = None;
            app.session_cli_timeout_seconds = None;
            app.error_modal = Some("Config saved to disk".into());
        }
        Err(e) => {
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
    let agent_name = match app.config.agents.get(app.edit_popup.edit_popup_cursor) {
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
    app.edit_popup.edit_popup_cursor = app.config.agents.len() - 1;
    app.edit_popup.edit_popup_field = EditField::AgentName;
    app.edit_popup.edit_buffer = name;
    app.edit_popup.edit_popup_editing = true;
}

pub(super) fn remove_agent(app: &mut App) {
    if app.config.agents.len() <= 1 {
        app.error_modal = Some("Cannot remove the last agent".into());
        return;
    }
    let idx = app.edit_popup.edit_popup_cursor;
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
    app.edit_popup.edit_popup_cursor = app
        .edit_popup
        .edit_popup_cursor
        .min(app.config.agents.len().saturating_sub(1));
}

pub(super) fn cycle_agent_provider(app: &mut App) {
    let idx = app.edit_popup.edit_popup_cursor;
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
    let idx = app.edit_popup.edit_popup_cursor;
    let old_name = match app.config.agents.get(idx) {
        Some(a) => a.name.clone(),
        None => return,
    };
    // Check uniqueness (case-insensitive, excluding current)
    let lower = new_name.to_lowercase();
    for (i, a) in app.config.agents.iter().enumerate() {
        if i != idx && a.name.to_lowercase() == lower {
            app.error_modal = Some(format!("Agent name '{}' already exists", new_name));
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
        app.config.diagnostic_provider = Some(new_name);
    }
    app.edit_popup.edit_buffer.clear();
    app.edit_popup.edit_popup_editing = false;
}

pub(super) fn timeout_field_count() -> usize {
    3
}

pub(super) fn begin_timeout_edit(app: &mut App) {
    app.edit_popup.edit_popup_field = EditField::TimeoutSeconds;
    app.edit_popup.edit_buffer = match app.edit_popup.edit_popup_timeout_cursor {
        0 => app.effective_http_timeout_seconds(),
        1 => app.effective_model_fetch_timeout_seconds(),
        2 => app.effective_cli_timeout_seconds(),
        _ => app.effective_http_timeout_seconds(),
    }
    .to_string();
    app.edit_popup.edit_popup_editing = true;
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

    match app.edit_popup.edit_popup_timeout_cursor {
        0 => app.session_http_timeout_seconds = Some(parsed),
        1 => app.session_model_fetch_timeout_seconds = Some(parsed),
        2 => app.session_cli_timeout_seconds = Some(parsed),
        _ => return Err("Invalid timeout field".into()),
    }

    Ok(())
}

pub(super) fn selected_kind_for_edit(app: &App) -> Option<ProviderKind> {
    app.config
        .agents
        .get(app.edit_popup.edit_popup_cursor)
        .map(|a| a.provider)
}

/// Returns a ProviderConfig view for the current edit selection (Providers section).
pub(super) fn effective_section_config(app: &App) -> Option<ProviderConfig> {
    let name = app
        .config
        .agents
        .get(app.edit_popup.edit_popup_cursor)
        .map(|a| a.name.as_str())?;
    app.effective_agent_config(name)
        .map(|a| a.to_provider_config())
}

pub(super) fn set_section_config_override(app: &mut App, config: ProviderConfig) {
    if let Some(agent) = app.config.agents.get(app.edit_popup.edit_popup_cursor) {
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
        app.prompt.iterations = app.prompt.iterations_buf.parse().unwrap_or(1).clamp(1, 99);
    }
    app.prompt.iterations_buf = app.prompt.iterations.to_string();
}

pub(super) fn sync_runs_buf(app: &mut App) {
    if app.prompt.runs_buf.is_empty() {
        app.prompt.runs = 1;
    } else {
        app.prompt.runs = app.prompt.runs_buf.parse().unwrap_or(1).clamp(1, 99);
    }
    app.prompt.runs_buf = app.prompt.runs.to_string();
}

pub(super) fn sync_concurrency_buf(app: &mut App) {
    if app.prompt.concurrency_buf.is_empty() {
        app.prompt.concurrency = 0;
    } else {
        app.prompt.concurrency = app.prompt.concurrency_buf.parse().unwrap_or(0).min(99);
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
            .clamp(1, 99);
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
            .clamp(1, 99);
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
            .min(99);
    }
    app.pipeline.pipeline_concurrency_buf = app.pipeline.pipeline_concurrency.to_string();
}

pub(super) fn effective_concurrency(runs: u32, concurrency: u32) -> u32 {
    match concurrency {
        0 => runs.max(1),
        value => value.min(runs).max(1),
    }
}
