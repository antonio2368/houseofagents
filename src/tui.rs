use crate::app::{
    App, ConsolidationPhase, EditField, EditPopupSection, HomeSection, PromptFocus, Screen,
};
use crate::config::ProviderConfig;
use crate::event::{Event, EventHandler};
use crate::execution::relay::run_relay;
use crate::execution::solo::run_solo;
use crate::execution::swarm::run_swarm;
use crate::execution::{ExecutionMode, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::{self, ProviderKind};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io::{self, stdout};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub fn restore_terminal() -> io::Result<()> {
    terminal::disable_raw_mode()?;
    execute!(stdout(), LeaveAlternateScreen)?;
    Ok(())
}

pub async fn run(app: &mut App) -> anyhow::Result<()> {
    terminal::enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen)?;
    let _terminal_guard = TerminalRestoreGuard;

    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut events = EventHandler::new(Duration::from_millis(100));

    loop {
        terminal.draw(|f| crate::screen::draw(f, app))?;

        tokio::select! {
            Some(event) = events.next() => {
                match event {
                    Event::Key(key) => {
                        handle_key(app, key);
                    }
                    Event::Tick => {}
                    Event::Resize(_, _) => {}
                }
            }
            Some(progress) = async {
                if let Some(ref mut rx) = app.progress_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<ProgressEvent>>().await
                }
            } => {
                handle_progress(app, progress);
            }
            Some(result) = async {
                if let Some(ref mut rx) = app.model_picker_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<Vec<String>, String>>>().await
                }
            } => {
                handle_model_list_result(app, result);
            }
            Some(save_result) = async {
                if let Some(ref mut rx) = app.config_save_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<(), String>>>().await
                }
            } => {
                handle_config_save_result(app, save_result);
            }
            Some(consolidation_result) = async {
                if let Some(ref mut rx) = app.consolidation_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<String, String>>>().await
                }
            } => {
                handle_consolidation_result(app, consolidation_result);
            }
            Some(diagnostic_result) = async {
                if let Some(ref mut rx) = app.diagnostic_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<String, String>>>().await
                }
            } => {
                handle_diagnostic_result(app, diagnostic_result);
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

struct TerminalRestoreGuard;

impl Drop for TerminalRestoreGuard {
    fn drop(&mut self) {
        let _ = restore_terminal();
    }
}

fn handle_key(app: &mut App, key: KeyEvent) {
    // Ctrl+C: graceful quit from anywhere
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if app.is_running {
            app.cancel_flag.store(true, Ordering::Relaxed);
        }
        app.should_quit = true;
        return;
    }

    // Dismiss error modal with any key
    if app.error_modal.is_some() {
        app.error_modal = None;
        return;
    }

    // Dismiss help popup with any key
    if app.show_help_popup {
        app.show_help_popup = false;
        return;
    }

    // Edit popup handling
    if app.show_edit_popup {
        handle_edit_popup_key(app, key);
        return;
    }

    match app.screen {
        Screen::Home => handle_home_key(app, key),
        Screen::Prompt => handle_prompt_key(app, key),
        Screen::Order => handle_order_key(app, key),
        Screen::Running => handle_running_key(app, key),
        Screen::Results => handle_results_key(app, key),
    }
}

fn handle_home_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Char('?') => {
            app.show_help_popup = true;
        }
        KeyCode::Char('e') => {
            app.show_edit_popup = true;
            app.edit_popup_section = EditPopupSection::Providers;
            app.edit_popup_cursor = 0;
            app.edit_popup_diagnostic_cursor = diagnostic_provider_kind(app)
                .and_then(|kind| ProviderKind::all().iter().position(|k| *k == kind))
                .unwrap_or(0);
            app.edit_popup_editing = false;
            app.edit_buffer.clear();
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
                HomeSection::Agents => ProviderKind::all().len().saturating_sub(1),
                HomeSection::Mode => ExecutionMode::all().len().saturating_sub(1),
            };
            if app.home_cursor < max {
                app.home_cursor += 1;
            }
        }
        KeyCode::Char(' ') => match app.home_section {
            HomeSection::Agents => {
                let providers = app.available_providers();
                if let Some((kind, available)) = providers.get(app.home_cursor) {
                    if *available {
                        app.toggle_agent(*kind);
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
            if app.selected_agents.is_empty() {
                app.error_modal = Some("Select at least one agent".into());
            } else {
                app.screen = Screen::Prompt;
                app.prompt_focus = PromptFocus::Text;
                app.iterations_buf = app.iterations.to_string();
                app.resume_previous = false;
            }
        }
        _ => {}
    }
}

fn handle_prompt_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            sync_iterations_buf(app);
            app.screen = Screen::Home;
        }
        KeyCode::Tab => {
            app.prompt_focus = match app.prompt_focus {
                PromptFocus::Text => PromptFocus::SessionName,
                PromptFocus::SessionName => {
                    app.iterations_buf = app.iterations.to_string();
                    PromptFocus::Iterations
                }
                PromptFocus::Iterations => {
                    sync_iterations_buf(app);
                    PromptFocus::Text
                }
            };
        }
        KeyCode::BackTab => {
            app.prompt_focus = match app.prompt_focus {
                PromptFocus::Text => {
                    app.iterations_buf = app.iterations.to_string();
                    PromptFocus::Iterations
                }
                PromptFocus::SessionName => PromptFocus::Text,
                PromptFocus::Iterations => {
                    sync_iterations_buf(app);
                    PromptFocus::SessionName
                }
            };
        }
        KeyCode::Char('r')
            if app.selected_mode != ExecutionMode::Solo
                && app.prompt_focus != PromptFocus::Text =>
        {
            app.resume_previous = !app.resume_previous;
        }
        KeyCode::F(5) | KeyCode::Enter
            if key.code == KeyCode::F(5) || app.prompt_focus != PromptFocus::Text =>
        {
            sync_iterations_buf(app);
            if app.prompt_text.trim().is_empty()
                && !(app.resume_previous
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
        _ => match app.prompt_focus {
            PromptFocus::Text => match key.code {
                KeyCode::Char(c) => app.prompt_text.push(c),
                KeyCode::Backspace => {
                    app.prompt_text.pop();
                }
                KeyCode::Enter => app.prompt_text.push('\n'),
                _ => {}
            },
            PromptFocus::SessionName => match key.code {
                KeyCode::Char(c) => app.session_name.push(c),
                KeyCode::Backspace => {
                    app.session_name.pop();
                }
                _ => {}
            },
            PromptFocus::Iterations => {
                if app.selected_mode == ExecutionMode::Solo {
                    return;
                }
                match key.code {
                    KeyCode::Char(c) if c.is_ascii_digit() => {
                        app.iterations_buf.push(c);
                        app.iterations = app.iterations_buf.parse().unwrap_or(1).clamp(1, 99);
                        // Re-sync buf in case clamp changed the value
                        app.iterations_buf = app.iterations.to_string();
                    }
                    KeyCode::Backspace => {
                        app.iterations_buf.pop();
                        if app.iterations_buf.is_empty() {
                            app.iterations = 1;
                        } else {
                            app.iterations = app.iterations_buf.parse().unwrap_or(1).clamp(1, 99);
                        }
                    }
                    KeyCode::Up | KeyCode::Char('+') => {
                        app.iterations = (app.iterations + 1).min(99);
                        app.iterations_buf = app.iterations.to_string();
                    }
                    KeyCode::Down | KeyCode::Char('-') => {
                        app.iterations = app.iterations.saturating_sub(1).max(1);
                        app.iterations_buf = app.iterations.to_string();
                    }
                    _ => {}
                }
            }
        },
    }
}

fn handle_order_key(app: &mut App, key: KeyEvent) {
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

fn handle_running_key(app: &mut App, key: KeyEvent) {
    if app.diagnostic_running {
        if key.code == KeyCode::Char('q') {
            app.should_quit = true;
        }
        return;
    }

    if app.consolidation_running {
        if key.code == KeyCode::Char('q') {
            app.should_quit = true;
        }
        return;
    }

    if app.consolidation_active {
        handle_consolidation_key(app, key);
        return;
    }

    match key.code {
        KeyCode::Esc if app.is_running => {
            app.cancel_flag.store(true, Ordering::Relaxed);
        }
        KeyCode::Enter if !app.is_running => {
            app.screen = Screen::Results;
            load_results(app);
        }
        KeyCode::Char('q') if !app.is_running => {
            app.should_quit = true;
        }
        _ => {}
    }
}

fn handle_consolidation_key(app: &mut App, key: KeyEvent) {
    if app.consolidation_running {
        if key.code == KeyCode::Char('q') {
            app.should_quit = true;
        }
        return;
    }

    match app.consolidation_phase {
        ConsolidationPhase::Confirm => match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                app.consolidation_phase = if app.selected_agents.len() <= 1 {
                    ConsolidationPhase::Prompt
                } else {
                    ConsolidationPhase::Provider
                };
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                app.consolidation_active = false;
                maybe_start_diagnostics(app);
            }
            KeyCode::Char('q') => app.should_quit = true,
            _ => {}
        },
        ConsolidationPhase::Provider => match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                app.consolidation_provider_cursor =
                    app.consolidation_provider_cursor.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let max = app.selected_agents.len().saturating_sub(1);
                if app.consolidation_provider_cursor < max {
                    app.consolidation_provider_cursor += 1;
                }
            }
            KeyCode::Enter => {
                app.consolidation_phase = ConsolidationPhase::Prompt;
            }
            KeyCode::Esc => {
                app.consolidation_phase = ConsolidationPhase::Confirm;
            }
            KeyCode::Char('q') => app.should_quit = true,
            _ => {}
        },
        ConsolidationPhase::Prompt => match key.code {
            KeyCode::Enter => start_consolidation(app),
            KeyCode::Backspace => {
                app.consolidation_prompt.pop();
            }
            KeyCode::Char(c) => app.consolidation_prompt.push(c),
            KeyCode::Esc => {
                if app.selected_agents.len() <= 1 {
                    app.consolidation_phase = ConsolidationPhase::Confirm;
                } else {
                    app.consolidation_phase = ConsolidationPhase::Provider;
                }
            }
            _ => {}
        },
    }
}

fn handle_results_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Up | KeyCode::Char('k') => {
            app.result_cursor = app.result_cursor.saturating_sub(1);
            update_preview(app);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.result_cursor < app.result_files.len().saturating_sub(1) {
                app.result_cursor += 1;
            }
            update_preview(app);
        }
        KeyCode::Enter | KeyCode::Esc => reset_to_home(app),
        _ => {}
    }
}

fn reset_to_home(app: &mut App) {
    app.screen = Screen::Home;
    app.prompt_text.clear();
    app.session_name.clear();
    app.selected_agents.clear();
    app.progress_events.clear();
    app.result_files.clear();
    app.result_preview.clear();
    app.iterations = 1;
    app.iterations_buf = "1".into();
    app.resume_previous = false;
    app.consolidation_active = false;
    app.consolidation_phase = ConsolidationPhase::Confirm;
    app.consolidation_provider_cursor = 0;
    app.consolidation_prompt.clear();
    app.consolidation_running = false;
    app.consolidation_rx = None;
    app.diagnostic_running = false;
    app.diagnostic_rx = None;
}

fn handle_edit_popup_key(app: &mut App, key: KeyEvent) {
    // Model picker mode
    if app.model_picker_active {
        handle_model_picker_key(app, key);
        return;
    }

    match key.code {
        KeyCode::Esc => {
            if app.edit_popup_editing {
                app.edit_popup_editing = false;
                app.edit_buffer.clear();
            } else {
                app.show_edit_popup = false;
            }
        }
        KeyCode::Tab if !app.edit_popup_editing => {
            app.edit_popup_section = match app.edit_popup_section {
                EditPopupSection::Providers => EditPopupSection::Diagnostics,
                EditPopupSection::Diagnostics => EditPopupSection::Providers,
            };
            app.edit_buffer.clear();
        }
        KeyCode::Up | KeyCode::Char('k') if !app.edit_popup_editing => {
            match app.edit_popup_section {
                EditPopupSection::Providers => {
                    app.edit_popup_cursor = app.edit_popup_cursor.saturating_sub(1);
                }
                EditPopupSection::Diagnostics => {
                    app.edit_popup_diagnostic_cursor =
                        app.edit_popup_diagnostic_cursor.saturating_sub(1);
                }
            }
        }
        KeyCode::Down | KeyCode::Char('j') if !app.edit_popup_editing => {
            let max = ProviderKind::all().len().saturating_sub(1);
            match app.edit_popup_section {
                EditPopupSection::Providers => {
                    if app.edit_popup_cursor < max {
                        app.edit_popup_cursor += 1;
                    }
                }
                EditPopupSection::Diagnostics => {
                    if app.edit_popup_diagnostic_cursor < max {
                        app.edit_popup_diagnostic_cursor += 1;
                    }
                }
            }
        }
        KeyCode::Char('a') if !app.edit_popup_editing => {
            app.edit_popup_field = EditField::ApiKey;
            app.edit_buffer = selected_kind_for_edit(app)
                .and_then(|kind| effective_section_config(app, app.edit_popup_section, kind))
                .map(|c| c.api_key)
                .unwrap_or_default();
            app.edit_popup_editing = true;
        }
        KeyCode::Char('m') if !app.edit_popup_editing => {
            if app.edit_popup_section == EditPopupSection::Diagnostics
                && !is_selected_diagnostic_active(app)
            {
                app.error_modal = Some("Enable this diagnostics provider with [d] first".into());
                return;
            }
            app.edit_popup_field = EditField::Model;
            app.edit_buffer = selected_kind_for_edit(app)
                .and_then(|kind| effective_section_config(app, app.edit_popup_section, kind))
                .map(|c| c.model)
                .unwrap_or_default();
            app.edit_popup_editing = true;
        }
        KeyCode::Char('x') if !app.edit_popup_editing => {
            app.edit_popup_field = EditField::ExtraCliArgs;
            app.edit_buffer = selected_kind_for_edit(app)
                .and_then(|kind| effective_section_config(app, app.edit_popup_section, kind))
                .map(|c| c.extra_cli_args)
                .unwrap_or_default();
            app.edit_popup_editing = true;
        }
        KeyCode::Char('l') if !app.edit_popup_editing => {
            if app.edit_popup_section == EditPopupSection::Diagnostics
                && !is_selected_diagnostic_active(app)
            {
                app.error_modal = Some("Enable this diagnostics provider with [d] first".into());
                return;
            }
            app.edit_popup_field = EditField::Model;
            start_model_fetch(app);
        }
        KeyCode::Char('o') if !app.edit_popup_editing => {
            app.edit_popup_field = EditField::OutputDir;
            app.edit_buffer = app.config.output_dir.clone();
            app.edit_popup_editing = true;
        }
        KeyCode::Char('s') if !app.edit_popup_editing => {
            save_config_globally(app);
        }
        KeyCode::Char('c') if !app.edit_popup_editing => {
            toggle_cli_mode(app);
        }
        KeyCode::Char('d') if !app.edit_popup_editing => {
            if app.edit_popup_section == EditPopupSection::Diagnostics {
                cycle_diagnostic_provider(app);
            } else {
                app.error_modal = Some(
                    "Switch to Diagnostics section with Tab to set diagnostic provider".into(),
                );
            }
        }
        KeyCode::Char('t') if !app.edit_popup_editing => {
            if app.edit_popup_section == EditPopupSection::Diagnostics
                && !is_selected_diagnostic_active(app)
            {
                app.error_modal = Some("Enable this diagnostics provider with [d] first".into());
                return;
            }
            cycle_reasoning(app);
        }
        KeyCode::Enter if !app.edit_popup_editing => {
            if app.edit_popup_section == EditPopupSection::Diagnostics {
                cycle_diagnostic_provider(app);
            } else {
                app.edit_popup_field = EditField::ApiKey;
                app.edit_buffer = selected_kind_for_edit(app)
                    .and_then(|kind| effective_section_config(app, app.edit_popup_section, kind))
                    .map(|c| c.api_key)
                    .unwrap_or_default();
                app.edit_popup_editing = true;
            }
        }
        KeyCode::Enter if app.edit_popup_editing => {
            if matches!(app.edit_popup_field, EditField::OutputDir) {
                let new_output_dir = app.edit_buffer.trim();
                if new_output_dir.is_empty() {
                    app.error_modal = Some("Output directory cannot be empty".into());
                } else {
                    app.config.output_dir = new_output_dir.to_string();
                }
            } else if let Some(kind) = selected_kind_for_edit(app) {
                let mut config = effective_section_config(app, app.edit_popup_section, kind)
                    .unwrap_or_else(empty_provider_config);
                match app.edit_popup_field {
                    EditField::ApiKey => config.api_key = app.edit_buffer.clone(),
                    EditField::Model => config.model = app.edit_buffer.clone(),
                    EditField::ExtraCliArgs => config.extra_cli_args = app.edit_buffer.clone(),
                    EditField::OutputDir => {}
                }
                set_section_config_override(app, app.edit_popup_section, kind, config);
            }
            app.edit_buffer.clear();
            app.edit_popup_editing = false;
        }
        KeyCode::Backspace if app.edit_popup_editing => {
            app.edit_buffer.pop();
        }
        KeyCode::Char(c) if app.edit_popup_editing => {
            app.edit_buffer.push(c);
        }
        _ => {}
    }
}

fn handle_model_picker_key(app: &mut App, key: KeyEvent) {
    if app.model_picker_loading {
        // Only allow Esc while loading
        if key.code == KeyCode::Esc {
            app.model_picker_active = false;
            app.model_picker_loading = false;
            app.model_picker_rx = None;
        }
        return;
    }

    match key.code {
        KeyCode::Esc => {
            app.model_picker_active = false;
            app.model_picker_list.clear();
            app.model_picker_all_models.clear();
            app.model_picker_filter.clear();
        }
        KeyCode::Up => {
            app.model_picker_cursor = app.model_picker_cursor.saturating_sub(1);
        }
        KeyCode::Down => {
            if app.model_picker_cursor < app.model_picker_list.len().saturating_sub(1) {
                app.model_picker_cursor += 1;
            }
        }
        KeyCode::Enter => {
            if let Some(model) = app.model_picker_list.get(app.model_picker_cursor).cloned() {
                if let Some(kind) = selected_kind_for_edit(app) {
                    let mut config = effective_section_config(app, app.edit_popup_section, kind)
                        .unwrap_or_else(empty_provider_config);
                    config.model = model;
                    set_section_config_override(app, app.edit_popup_section, kind, config);
                }
            }
            app.model_picker_active = false;
            app.model_picker_list.clear();
            app.model_picker_all_models.clear();
            app.model_picker_filter.clear();
        }
        KeyCode::Char(c) => {
            app.model_picker_filter.push(c);
            apply_model_filter(app);
        }
        KeyCode::Backspace => {
            app.model_picker_filter.pop();
            apply_model_filter(app);
        }
        _ => {}
    }
}

fn apply_model_filter(app: &mut App) {
    let filter = app.model_picker_filter.to_lowercase();
    app.model_picker_list = if filter.is_empty() {
        app.model_picker_all_models.clone()
    } else {
        app.model_picker_all_models
            .iter()
            .filter(|m| m.to_lowercase().contains(&filter))
            .cloned()
            .collect()
    };
    app.model_picker_cursor = 0;
}

fn start_model_fetch(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    let config = match effective_section_config(app, app.edit_popup_section, kind) {
        Some(c) => c,
        None => {
            app.error_modal = Some(
                "Add API key to fetch model list. You can type model manually with [m].".into(),
            );
            return;
        }
    };

    let has_key = !config.api_key.is_empty();

    if !has_key {
        app.error_modal =
            Some("Add API key to fetch model list. You can type model manually with [m].".into());
        return;
    }

    app.model_picker_active = true;
    app.model_picker_loading = true;
    app.model_picker_all_models.clear();
    app.model_picker_list.clear();
    app.model_picker_filter.clear();
    app.model_picker_cursor = 0;

    let (tx, rx) = mpsc::unbounded_channel();
    app.model_picker_rx = Some(rx);

    let api_key = config.api_key.clone();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client");

    tokio::spawn(async move {
        let result = provider::list_models(kind, &api_key, &client).await;
        let _ = tx.send(result);
    });
}

fn handle_model_list_result(app: &mut App, result: Result<Vec<String>, String>) {
    app.model_picker_loading = false;
    app.model_picker_rx = None;
    match result {
        Ok(models) if models.is_empty() => {
            app.model_picker_active = false;
            app.error_modal = Some("No models returned by API".into());
        }
        Ok(models) => {
            // Pre-select the current model if it's in the list
            let current_model = selected_kind_for_edit(app)
                .and_then(|k| effective_section_config(app, app.edit_popup_section, k))
                .map(|c| c.model);
            app.model_picker_cursor = current_model
                .and_then(|m| models.iter().position(|x| *x == m))
                .unwrap_or(0);
            app.model_picker_all_models = models.clone();
            app.model_picker_list = models;
        }
        Err(e) => {
            app.model_picker_active = false;
            app.error_modal = Some(format!("Failed to fetch models: {e}"));
        }
    }
}

fn save_config_globally(app: &mut App) {
    if app.config_save_in_progress {
        return;
    }

    // Merge session overrides into config
    for kind in ProviderKind::all() {
        let key = kind.config_key().to_string();
        if let Some(override_config) = app.session_overrides.get(&key) {
            app.config.providers.insert(key, override_config.clone());
        }
    }
    for kind in ProviderKind::all() {
        let key = kind.config_key().to_string();
        if let Some(override_config) = app.session_diagnostic_overrides.get(&key) {
            app.config.diagnostics.insert(key, override_config.clone());
        }
    }
    app.config_save_in_progress = true;
    let config_to_save = app.config.clone();
    let path_override = app.config_path_override.clone();

    let (tx, rx) = mpsc::unbounded_channel();
    app.config_save_rx = Some(rx);

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

fn handle_config_save_result(app: &mut App, result: Result<(), String>) {
    app.config_save_in_progress = false;
    app.config_save_rx = None;

    match result {
        Ok(()) => {
            app.session_overrides.clear();
            app.session_diagnostic_overrides.clear();
            app.error_modal = Some("Config saved to disk".into());
        }
        Err(e) => {
            app.error_modal = Some(format!("Failed to save config: {e}"));
        }
    }
}

fn cycle_reasoning(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    let mut config = effective_section_config(app, app.edit_popup_section, kind)
        .unwrap_or_else(empty_provider_config);

    match kind {
        ProviderKind::OpenAI => {
            // Cycle: None -> low -> medium -> high -> None
            config.reasoning_effort = match config.reasoning_effort.as_deref() {
                None => Some("low".into()),
                Some("low") => Some("medium".into()),
                Some("medium") => Some("high".into()),
                _ => None,
            };
        }
        ProviderKind::Anthropic | ProviderKind::Gemini => {
            // Cycle: None -> low -> medium -> high -> None
            config.thinking_effort = match config.thinking_effort.as_deref() {
                None => Some("low".into()),
                Some("low") => Some("medium".into()),
                Some("medium") => Some("high".into()),
                _ => None,
            };
        }
    }
    set_section_config_override(app, app.edit_popup_section, kind, config);
}

fn cycle_diagnostic_provider(app: &mut App) {
    let Some(kind) = ProviderKind::all()
        .get(app.edit_popup_diagnostic_cursor)
        .copied()
    else {
        return;
    };
    let key = kind.config_key().to_string();
    if app.config.diagnostic_provider.as_deref() == Some(key.as_str()) {
        app.config.diagnostic_provider = None;
    } else {
        app.config.diagnostic_provider = Some(key);
    }
}

fn toggle_cli_mode(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    let cli_installed = app.cli_available.get(&kind).copied().unwrap_or(false);
    let mut config = effective_section_config(app, app.edit_popup_section, kind)
        .unwrap_or_else(empty_provider_config);

    // Allow turning CLI mode off even when binary is missing, but block turning it on.
    if !config.use_cli && !cli_installed {
        app.error_modal = Some(format!("{} CLI not installed", kind.display_name()));
        return;
    }

    config.use_cli = !config.use_cli;
    set_section_config_override(app, app.edit_popup_section, kind, config);
}

fn selected_kind_for_edit(app: &App) -> Option<ProviderKind> {
    match app.edit_popup_section {
        EditPopupSection::Providers => ProviderKind::all().get(app.edit_popup_cursor).copied(),
        EditPopupSection::Diagnostics => ProviderKind::all()
            .get(app.edit_popup_diagnostic_cursor)
            .copied(),
    }
}

fn is_selected_diagnostic_active(app: &App) -> bool {
    selected_kind_for_edit(app)
        .map(|kind| app.config.diagnostic_provider.as_deref() == Some(kind.config_key()))
        .unwrap_or(false)
}

fn effective_section_config(
    app: &App,
    section: EditPopupSection,
    kind: ProviderKind,
) -> Option<ProviderConfig> {
    match section {
        EditPopupSection::Providers => app.effective_provider_config(kind),
        EditPopupSection::Diagnostics => app.effective_diagnostic_config(kind),
    }
}

fn set_section_config_override(
    app: &mut App,
    section: EditPopupSection,
    kind: ProviderKind,
    config: ProviderConfig,
) {
    let key = kind.config_key().to_string();
    match section {
        EditPopupSection::Providers => {
            app.session_overrides.insert(key, config);
        }
        EditPopupSection::Diagnostics => {
            app.session_diagnostic_overrides.insert(key, config);
        }
    }
}

fn empty_provider_config() -> ProviderConfig {
    ProviderConfig {
        api_key: String::new(),
        model: String::new(),
        reasoning_effort: None,
        thinking_effort: None,
        use_cli: false,
        extra_cli_args: String::new(),
    }
}

fn sync_iterations_buf(app: &mut App) {
    if app.iterations_buf.is_empty() {
        app.iterations = 1;
    } else {
        app.iterations = app.iterations_buf.parse().unwrap_or(1).clamp(1, 99);
    }
    app.iterations_buf = app.iterations.to_string();
}

fn start_execution(app: &mut App) {
    app.screen = Screen::Running;
    app.progress_events.clear();
    app.is_running = true;
    app.run_error = None;
    app.consolidation_active = false;
    app.consolidation_phase = ConsolidationPhase::Confirm;
    app.consolidation_provider_cursor = 0;
    app.consolidation_prompt.clear();
    app.consolidation_running = false;
    app.consolidation_rx = None;
    app.diagnostic_running = false;
    app.diagnostic_rx = None;

    let config = app.config.clone();
    let has_any_cli = app.selected_agents.iter().any(|kind| {
        app.effective_provider_config(*kind)
            .map(|c| c.use_cli)
            .unwrap_or(false)
    });
    let mut prompt = if has_any_cli {
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        if cwd.is_empty() {
            app.prompt_text.clone()
        } else {
            format!(
                "Working directory: {}\nYou have access to the data and files in this directory for context.\n\n{}",
                cwd, app.prompt_text
            )
        }
    } else {
        app.prompt_text.clone()
    };
    if diagnostic_provider_kind(app).is_some() {
        prompt.push_str(
            "\n\nWrite any encountered issues (for example permission, tool, or environment issues) to an explicit \"Errors\" section of your report.",
        );
    }
    let agents = app.selected_agents.clone();
    let mode = app.selected_mode;
    let iterations = if mode == ExecutionMode::Solo {
        1
    } else {
        app.iterations
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .expect("Failed to create HTTP client");

    let mut providers: Vec<Box<dyn provider::Provider>> = Vec::new();
    let mut use_cli_by_kind: std::collections::HashMap<ProviderKind, bool> =
        std::collections::HashMap::new();
    let mut run_models: Vec<(ProviderKind, String)> = Vec::new();
    for kind in &agents {
        let pconfig = match app.effective_provider_config(*kind) {
            Some(cfg) => cfg,
            None => {
                app.error_modal = Some(format!("{} is not configured", kind.display_name()));
                app.screen = Screen::Prompt;
                app.is_running = false;
                return;
            }
        };

        if pconfig.use_cli && !app.cli_available.get(kind).copied().unwrap_or(false) {
            app.error_modal = Some(format!("{} CLI is not installed", kind.display_name()));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }

        if !pconfig.use_cli && pconfig.api_key.trim().is_empty() {
            app.error_modal = Some(format!("{} API key is missing", kind.display_name()));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }

        run_models.push((
            *kind,
            if pconfig.model.trim().is_empty() {
                "(default)".to_string()
            } else {
                pconfig.model.clone()
            },
        ));
        use_cli_by_kind.insert(*kind, pconfig.use_cli);

        providers.push(provider::create_provider(
            *kind,
            &pconfig,
            client.clone(),
            config.default_max_tokens,
            config.max_history_messages,
        ));
    }

    let output_dir = config.resolved_output_dir();
    let session_name = if app.session_name.is_empty() {
        None
    } else {
        Some(app.session_name.as_str())
    };
    let mut start_iteration = 1u32;
    let mut relay_initial_last_output: Option<String> = None;
    let mut swarm_initial_outputs: std::collections::HashMap<ProviderKind, String> =
        std::collections::HashMap::new();
    let mut resumed_run = false;

    let output = if app.resume_previous
        && matches!(mode, ExecutionMode::Relay | ExecutionMode::Swarm)
    {
        let run_dir = if let Some(name) = session_name.filter(|n| !n.trim().is_empty()) {
            match OutputManager::find_latest_session_run(&output_dir, name.trim()) {
                Ok(Some(path)) => path,
                Ok(None) => {
                    app.error_modal = Some(format!(
                        "No previous run found for session '{}'",
                        name.trim()
                    ));
                    app.screen = Screen::Prompt;
                    app.is_running = false;
                    return;
                }
                Err(e) => {
                    app.error_modal = Some(format!("Failed to search previous runs: {e}"));
                    app.screen = Screen::Prompt;
                    app.is_running = false;
                    return;
                }
            }
        } else {
            match find_latest_compatible_run(&output_dir, mode, &agents) {
                Some(path) => path,
                None => {
                    app.error_modal = Some("No compatible previous run found to resume".into());
                    app.screen = Screen::Prompt;
                    app.is_running = false;
                    return;
                }
            }
        };

        let last_iteration = match find_last_complete_iteration_for_agents(&run_dir, &agents) {
            Some(i) if i >= 1 => i,
            _ => {
                app.error_modal = Some("No previous iteration files found to resume".into());
                app.screen = Screen::Prompt;
                app.is_running = false;
                return;
            }
        };
        start_iteration = last_iteration + 1;

        match mode {
            ExecutionMode::Relay => {
                let last_kind = match agents.last().copied() {
                    Some(k) => k,
                    None => {
                        app.error_modal = Some("No agents selected".into());
                        app.screen = Screen::Prompt;
                        app.is_running = false;
                        return;
                    }
                };
                let prev_path = run_dir.join(format!(
                    "{}_iter{}.md",
                    last_kind.config_key(),
                    last_iteration
                ));
                relay_initial_last_output = match std::fs::read_to_string(&prev_path) {
                    Ok(content) => Some(content),
                    Err(e) => {
                        app.error_modal = Some(format!(
                            "Failed to read previous relay output: {} ({e})",
                            prev_path.display()
                        ));
                        app.screen = Screen::Prompt;
                        app.is_running = false;
                        return;
                    }
                };
            }
            ExecutionMode::Swarm => {
                for kind in &agents {
                    let prev_path =
                        run_dir.join(format!("{}_iter{}.md", kind.config_key(), last_iteration));
                    if let Ok(content) = std::fs::read_to_string(&prev_path) {
                        swarm_initial_outputs.insert(*kind, content);
                    }
                }
                if swarm_initial_outputs.is_empty() {
                    app.error_modal = Some("No previous swarm outputs found to resume".into());
                    app.screen = Screen::Prompt;
                    app.is_running = false;
                    return;
                }
            }
            ExecutionMode::Solo => {}
        }

        resumed_run = true;
        match OutputManager::from_existing(run_dir) {
            Ok(o) => o,
            Err(e) => {
                app.error_modal = Some(format!("Failed to open existing run dir: {e}"));
                app.screen = Screen::Prompt;
                app.is_running = false;
                return;
            }
        }
    } else {
        match OutputManager::new(&output_dir, session_name) {
            Ok(o) => o,
            Err(e) => {
                app.error_modal = Some(format!("Failed to create output dir: {e}"));
                app.screen = Screen::Prompt;
                app.is_running = false;
                return;
            }
        }
    };

    if !resumed_run {
        let _ = output.write_prompt(&prompt);
        let _ = output.write_session_info(&mode, &agents, iterations, session_name, &run_models);
    } else {
        let _ = output.append_error(&format!(
            "Resumed {} mode for {} additional iteration(s), starting at iter {}",
            mode, iterations, start_iteration
        ));
    }

    // Store run dir for results screen
    app.run_dir = Some(output.run_dir().clone());

    // Create progress channel
    let (tx, rx) = mpsc::unbounded_channel::<ProgressEvent>();
    let cancel = Arc::new(AtomicBool::new(false));

    app.progress_rx = Some(rx);
    app.cancel_flag = cancel.clone();

    tokio::spawn(async move {
        let result = match mode {
            ExecutionMode::Solo => run_solo(&prompt, providers, &output, tx.clone(), cancel).await,
            ExecutionMode::Relay => {
                run_relay(
                    &prompt,
                    providers,
                    iterations,
                    start_iteration,
                    relay_initial_last_output,
                    use_cli_by_kind.clone(),
                    &output,
                    tx.clone(),
                    cancel,
                )
                .await
            }
            ExecutionMode::Swarm => {
                run_swarm(
                    &prompt,
                    providers,
                    iterations,
                    start_iteration,
                    swarm_initial_outputs,
                    use_cli_by_kind,
                    &output,
                    tx.clone(),
                    cancel,
                )
                .await
            }
        };
        if let Err(e) = result {
            let err_str = e.to_string();
            let _ = tx.send(ProgressEvent::AgentError {
                kind: agents.first().copied().unwrap_or(ProviderKind::Anthropic),
                iteration: 0,
                error: err_str.clone(),
                details: Some(err_str),
            });
            let _ = tx.send(ProgressEvent::AllDone);
        }
    });
}

fn handle_progress(app: &mut App, event: ProgressEvent) {
    let is_done = matches!(event, ProgressEvent::AllDone);
    app.progress_events.push(event);
    if is_done {
        app.is_running = false;
        app.progress_rx = None;
        if should_offer_consolidation(app) {
            app.consolidation_active = true;
            app.consolidation_phase = ConsolidationPhase::Confirm;
            app.consolidation_provider_cursor = 0;
            app.consolidation_prompt.clear();
            app.consolidation_running = false;
            app.consolidation_rx = None;
        } else {
            maybe_start_diagnostics(app);
        }
    }
}

fn should_offer_consolidation(app: &App) -> bool {
    if app.cancel_flag.load(Ordering::Relaxed) {
        return false;
    }
    if !matches!(
        app.selected_mode,
        ExecutionMode::Swarm | ExecutionMode::Solo
    ) {
        return false;
    }

    let Some(run_dir) = app.run_dir.as_ref() else {
        return false;
    };
    find_last_iteration(run_dir).is_some()
}

fn find_last_iteration(run_dir: &std::path::Path) -> Option<u32> {
    let mut max_iter: Option<u32> = None;
    let entries = std::fs::read_dir(run_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(iter) = parse_iteration_from_filename(&name) {
            max_iter = Some(max_iter.map_or(iter, |m| m.max(iter)));
        }
    }
    max_iter
}

fn find_last_complete_iteration_for_agents(
    run_dir: &std::path::Path,
    agents: &[ProviderKind],
) -> Option<u32> {
    use std::collections::{HashMap, HashSet};

    if agents.is_empty() {
        return None;
    }

    let mut iter_to_agents: HashMap<u32, HashSet<&'static str>> = HashMap::new();
    let entries = std::fs::read_dir(run_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        for kind in agents {
            if let Some(iter) = parse_agent_iteration_filename(&name, kind.config_key()) {
                iter_to_agents
                    .entry(iter)
                    .or_default()
                    .insert(kind.config_key());
            }
        }
    }

    iter_to_agents
        .into_iter()
        .filter(|(_, present_agents)| present_agents.len() == agents.len())
        .map(|(iter, _)| iter)
        .max()
}

fn find_latest_compatible_run(
    base_dir: &std::path::Path,
    mode: ExecutionMode,
    agents: &[ProviderKind],
) -> Option<std::path::PathBuf> {
    let mut dirs: Vec<(String, std::path::PathBuf)> = std::fs::read_dir(base_dir)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            if entry.file_type().ok()?.is_dir() {
                Some((
                    entry.file_name().to_string_lossy().to_string(),
                    entry.path(),
                ))
            } else {
                None
            }
        })
        .collect();
    dirs.sort_by(|a, b| b.0.cmp(&a.0));

    for (_, run_dir) in dirs {
        if !run_dir_matches_mode_and_agents(&run_dir, mode, agents) {
            continue;
        }
        if find_last_complete_iteration_for_agents(&run_dir, agents).is_some() {
            return Some(run_dir);
        }
    }
    None
}

fn run_dir_matches_mode_and_agents(
    run_dir: &std::path::Path,
    mode: ExecutionMode,
    agents: &[ProviderKind],
) -> bool {
    let session_path = run_dir.join("session.toml");
    let Ok(content) = std::fs::read_to_string(session_path) else {
        return false;
    };
    let Ok(value) = content.parse::<toml::Value>() else {
        return false;
    };

    if let Some(stored_mode) = value.get("mode").and_then(|v| v.as_str()) {
        if stored_mode != mode.as_str() {
            return false;
        }
    }

    if let Some(stored_agents) = value.get("agents").and_then(|v| v.as_array()) {
        let stored: std::collections::HashSet<String> = stored_agents
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        for kind in agents {
            if !stored.contains(kind.config_key()) {
                return false;
            }
        }
    }

    true
}

fn parse_agent_iteration_filename(name: &str, agent_key: &str) -> Option<u32> {
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

fn parse_iteration_from_filename(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let (prefix, _ext) = name.rsplit_once('.')?;
    let (_, iter_part) = prefix.rsplit_once("_iter")?;
    iter_part.parse::<u32>().ok()
}

fn start_consolidation(app: &mut App) {
    if app.consolidation_running {
        return;
    }

    let run_dir = match app.run_dir.clone() {
        Some(p) => p,
        None => {
            app.error_modal = Some("No run directory found for consolidation".into());
            return;
        }
    };

    let last_iteration = match find_last_iteration(&run_dir) {
        Some(i) => i,
        None => {
            app.error_modal = Some("No iteration outputs found to consolidate".into());
            return;
        }
    };

    let provider_kind = match app
        .selected_agents
        .get(app.consolidation_provider_cursor)
        .copied()
    {
        Some(k) => k,
        None => {
            app.error_modal = Some("Select a provider for consolidation".into());
            return;
        }
    };

    let pconfig = match app.effective_provider_config(provider_kind) {
        Some(cfg) => cfg,
        None => {
            app.error_modal = Some(format!(
                "{} is not configured",
                provider_kind.display_name()
            ));
            return;
        }
    };

    if pconfig.use_cli
        && !app
            .cli_available
            .get(&provider_kind)
            .copied()
            .unwrap_or(false)
    {
        app.error_modal = Some(format!(
            "{} CLI is not installed",
            provider_kind.display_name()
        ));
        return;
    }
    if !pconfig.use_cli && pconfig.api_key.trim().is_empty() {
        app.error_modal = Some(format!(
            "{} API key is missing",
            provider_kind.display_name()
        ));
        return;
    }

    let mut file_lines = Vec::new();
    for kind in &app.selected_agents {
        let path = run_dir.join(format!("{}_iter{}.md", kind.config_key(), last_iteration));
        if path.exists() {
            file_lines.push(format!("- {}: {}", kind.display_name(), path.display()));
        }
    }
    if file_lines.is_empty() {
        app.error_modal = Some("No final iteration files found to consolidate".into());
        return;
    }

    let additional = app.consolidation_prompt.trim().to_string();
    let mut prompt = String::from(
        "Consolidate the final iteration outputs into one final answer.\n\nFiles to read:\n",
    );
    for line in file_lines {
        prompt.push_str(&line);
        prompt.push('\n');
    }
    prompt.push_str(
        "\nInstructions:\n- Read each file before writing.\n- Resolve disagreements and keep the strongest points.\n- Return a single high-quality markdown response.\n- Do not write files and do not ask for filesystem permissions.\n- The application will save your response to disk.\n",
    );
    if !additional.is_empty() {
        prompt.push_str("\nAdditional instructions from user:\n");
        prompt.push_str(&additional);
        prompt.push('\n');
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .expect("Failed to create HTTP client");

    let provider = provider::create_provider(
        provider_kind,
        &pconfig,
        client,
        app.config.default_max_tokens,
        app.config.max_history_messages,
    );

    app.progress_events.push(ProgressEvent::AgentStarted {
        kind: provider_kind,
        iteration: last_iteration + 1,
    });
    app.progress_events.push(ProgressEvent::AgentLog {
        kind: provider_kind,
        iteration: last_iteration + 1,
        message: format!("{} consolidating reports", provider_kind.display_name()),
    });

    let output_path = run_dir.join(format!("consolidated_{}.md", provider_kind.config_key()));
    let (tx, rx) = mpsc::unbounded_channel();
    app.consolidation_rx = Some(rx);
    app.consolidation_running = true;
    app.consolidation_active = false;
    app.is_running = true;

    tokio::spawn(async move {
        let mut provider = provider;
        let result = match provider.send(&prompt).await {
            Ok(resp) => match std::fs::write(&output_path, &resp.content) {
                Ok(()) => Ok(output_path.display().to_string()),
                Err(e) => Err(format!("Failed to write consolidation output: {e}")),
            },
            Err(e) => Err(e.to_string()),
        };
        let _ = tx.send(result);
    });
}

fn handle_consolidation_result(app: &mut App, result: Result<String, String>) {
    app.consolidation_running = false;
    app.is_running = false;
    app.consolidation_rx = None;

    let kind = app
        .selected_agents
        .get(app.consolidation_provider_cursor)
        .copied()
        .unwrap_or(ProviderKind::Anthropic);
    let iteration = app
        .run_dir
        .as_deref()
        .and_then(find_last_iteration)
        .unwrap_or(1)
        + 1;

    match result {
        Ok(path) => {
            app.progress_events
                .push(ProgressEvent::AgentFinished { kind, iteration });
            app.progress_events.push(ProgressEvent::AgentLog {
                kind,
                iteration,
                message: format!("Consolidation saved to {path}"),
            });
            app.consolidation_active = false;
            app.error_modal = Some("Consolidation completed".into());
        }
        Err(e) => {
            app.progress_events.push(ProgressEvent::AgentError {
                kind,
                iteration,
                error: e.clone(),
                details: Some(e),
            });
            app.consolidation_active = false;
        }
    }

    maybe_start_diagnostics(app);
}

fn maybe_start_diagnostics(app: &mut App) {
    if app.cancel_flag.load(Ordering::Relaxed)
        || app.diagnostic_running
        || app.diagnostic_rx.is_some()
    {
        return;
    }

    let diagnostic_kind = match diagnostic_provider_kind(app) {
        Some(kind) => kind,
        None => return,
    };
    let run_dir = match app.run_dir.clone() {
        Some(path) => path,
        None => return,
    };
    let pconfig = match app.effective_diagnostic_config(diagnostic_kind) {
        Some(cfg) => cfg,
        None => {
            app.error_modal = Some(format!(
                "Diagnostic provider {} is not configured in Diagnostics section",
                diagnostic_kind.display_name()
            ));
            return;
        }
    };

    if pconfig.use_cli
        && !app
            .cli_available
            .get(&diagnostic_kind)
            .copied()
            .unwrap_or(false)
    {
        app.error_modal = Some(format!(
            "Diagnostic provider {} CLI is not installed",
            diagnostic_kind.display_name()
        ));
        return;
    }
    if !pconfig.use_cli && pconfig.api_key.trim().is_empty() {
        app.error_modal = Some(format!(
            "Diagnostic provider {} API key is missing",
            diagnostic_kind.display_name()
        ));
        return;
    }

    let report_files = collect_report_files(&run_dir);
    let app_errors = collect_application_errors(app, &run_dir);
    let prompt = build_diagnostic_prompt(&report_files, &app_errors, pconfig.use_cli);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .expect("Failed to create HTTP client");
    let provider = provider::create_provider(
        diagnostic_kind,
        &pconfig,
        client,
        app.config.default_max_tokens,
        app.config.max_history_messages,
    );

    app.progress_events.push(ProgressEvent::AgentLog {
        kind: diagnostic_kind,
        iteration: 0,
        message: format!(
            "{} analyzing reports for errors",
            diagnostic_kind.display_name()
        ),
    });
    app.diagnostic_running = true;
    app.is_running = true;

    let output_path = run_dir.join("errors.md");
    let (tx, rx) = mpsc::unbounded_channel();
    app.diagnostic_rx = Some(rx);

    tokio::spawn(async move {
        let mut provider = provider;
        let result = match provider.send(&prompt).await {
            Ok(resp) => match std::fs::write(&output_path, &resp.content) {
                Ok(()) => Ok(output_path.display().to_string()),
                Err(e) => Err(format!("Failed to write errors.md: {e}")),
            },
            Err(e) => Err(e.to_string()),
        };
        let _ = tx.send(result);
    });
}

fn handle_diagnostic_result(app: &mut App, result: Result<String, String>) {
    app.diagnostic_running = false;
    app.is_running = false;
    app.diagnostic_rx = None;

    let kind = diagnostic_provider_kind(app).unwrap_or(ProviderKind::Anthropic);

    match result {
        Ok(path) => {
            app.progress_events.push(ProgressEvent::AgentLog {
                kind,
                iteration: 0,
                message: format!("Diagnostic report saved to {path}"),
            });
        }
        Err(e) => {
            app.progress_events.push(ProgressEvent::AgentError {
                kind,
                iteration: 0,
                error: e.clone(),
                details: Some(e),
            });
        }
    }
}

fn diagnostic_provider_kind(app: &App) -> Option<ProviderKind> {
    let raw = app
        .config
        .diagnostic_provider
        .as_deref()?
        .trim()
        .to_lowercase();
    ProviderKind::all()
        .iter()
        .copied()
        .find(|kind| kind.config_key() == raw || kind.display_name().to_lowercase() == raw)
}

fn collect_report_files(run_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(run_dir)
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
        .collect();
    files.sort();
    files
}

fn collect_application_errors(app: &App, run_dir: &std::path::Path) -> Vec<String> {
    let mut errors = Vec::new();
    for event in &app.progress_events {
        if let ProgressEvent::AgentError {
            kind,
            iteration,
            error,
            details,
        } = event
        {
            let body = details.as_deref().unwrap_or(error);
            errors.push(format!(
                "[{} iter {}] {}",
                kind.display_name(),
                iteration,
                body
            ));
        }
    }

    let log_path = run_dir.join("_errors.log");
    if let Ok(content) = std::fs::read_to_string(&log_path) {
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                errors.push(trimmed.to_string());
            }
        }
    }

    let mut seen = std::collections::HashSet::new();
    errors.retain(|e| seen.insert(e.clone()));
    errors
}

fn build_diagnostic_prompt(
    report_files: &[std::path::PathBuf],
    app_errors: &[String],
    use_cli: bool,
) -> String {
    let mut prompt = String::from(
        "Analyze all reports for operational/tooling errors and produce a markdown report.\n",
    );
    prompt.push_str("Write only the diagnostic report content.\n");
    prompt.push_str("Do not write files and do not ask for filesystem permissions.\n");
    prompt.push_str("The application will save your response to errors.md.\n\n");
    prompt.push_str(
        "Report structure:\n1) Summary\n2) Detected Issues\n3) Evidence\n4) Suggested Fixes\n\n",
    );

    prompt.push_str("Application-generated errors:\n");
    if app_errors.is_empty() {
        prompt.push_str("- none reported by application\n");
    } else {
        for err in app_errors {
            prompt.push_str("- ");
            prompt.push_str(err);
            prompt.push('\n');
        }
    }

    prompt.push_str("\nReports to analyze:\n");
    if report_files.is_empty() {
        prompt.push_str("- none\n");
        return prompt;
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
        return prompt;
    }

    prompt.push_str("\nReport contents:\n");
    for path in report_files {
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

    prompt
}

fn load_results(app: &mut App) {
    if let Some(ref run_dir) = app.run_dir {
        let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(run_dir) {
            for entry in entries.flatten() {
                if entry.path().is_file() {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        app.result_files = files;
    }
    app.result_cursor = 0;
    update_preview(app);
}

fn update_preview(app: &mut App) {
    if let Some(path) = app.result_files.get(app.result_cursor) {
        app.result_preview =
            std::fs::read_to_string(path).unwrap_or_else(|e| format!("Error: {e}"));
    } else {
        app.result_preview = String::new();
    }
}
