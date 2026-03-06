use crate::app::{
    App, ConsolidationPhase, EditField, EditPopupSection, HomeSection, PipelineDialogMode,
    PipelineEditField, PipelineFocus, PromptFocus, Screen,
};
use crate::config::{AgentConfig, ProviderConfig};
use crate::event::{Event, EventHandler};
use crate::execution::pipeline::{self as pipeline_mod, BlockId};
use crate::execution::relay::run_relay;
use crate::execution::solo::run_solo;
use crate::execution::swarm::run_swarm;
use crate::execution::{ExecutionMode, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::{self, ProviderKind};

use crossterm::event::{
    DisableBracketedPaste, EnableBracketedPaste, KeyCode, KeyEvent, KeyModifiers,
};
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
    execute!(stdout(), LeaveAlternateScreen, DisableBracketedPaste)?;
    Ok(())
}

pub async fn run(app: &mut App) -> anyhow::Result<()> {
    terminal::enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen, EnableBracketedPaste)?;
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
                    Event::Paste(text) => handle_paste(app, &text),
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

    // Help popup has dedicated scroll + close handling
    if app.show_help_popup {
        handle_help_popup_key(app, key);
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
        Screen::Pipeline => handle_pipeline_key(app, key),
    }
}

fn handle_paste(app: &mut App, text: &str) {
    if app.error_modal.is_some() {
        app.error_modal = None;
        return;
    }

    if app.show_help_popup {
        return;
    }

    if app.show_edit_popup {
        if app.edit_popup_editing {
            app.edit_buffer.push_str(text);
        }
        return;
    }

    if app.screen == Screen::Prompt && app.prompt_focus == PromptFocus::Text {
        insert_prompt_text(app, text);
    } else if app.screen == Screen::Pipeline {
        handle_pipeline_paste(app, text);
    }
}

fn handle_home_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Char('?') => {
            app.show_help_popup = true;
            app.help_popup_scroll = 0;
        }
        KeyCode::Char('e') => {
            app.show_edit_popup = true;
            app.edit_popup_section = EditPopupSection::Providers;
            app.edit_popup_cursor = 0;
            app.edit_popup_timeout_cursor = 0;
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
                app.pipeline_focus = PipelineFocus::InitialPrompt;
                app.pipeline_prompt_cursor = app.pipeline_def.initial_prompt.len();
                app.pipeline_iterations_buf = app.pipeline_def.iterations.to_string();
            } else if app.selected_agents.is_empty() {
                app.error_modal = Some("Select at least one agent".into());
            } else {
                app.screen = Screen::Prompt;
                app.prompt_focus = PromptFocus::Text;
                app.prompt_cursor = app.prompt_text.len();
                app.iterations_buf = app.iterations.to_string();
                app.resume_previous = false;
                app.forward_prompt = false;
            }
        }
        _ => {}
    }
}

fn handle_help_popup_key(app: &mut App, key: KeyEvent) {
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

fn handle_prompt_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            sync_iterations_buf(app);
            app.screen = Screen::Home;
        }
        KeyCode::Tab => {
            app.prompt_focus = match (&app.prompt_focus, app.selected_mode) {
                (PromptFocus::Text, _) => PromptFocus::SessionName,
                (PromptFocus::SessionName, ExecutionMode::Solo) => PromptFocus::Text,
                (PromptFocus::SessionName, _) => {
                    app.iterations_buf = app.iterations.to_string();
                    PromptFocus::Iterations
                }
                (PromptFocus::Iterations, _) => {
                    sync_iterations_buf(app);
                    PromptFocus::Resume
                }
                (PromptFocus::Resume, ExecutionMode::Relay) => PromptFocus::ForwardPrompt,
                (PromptFocus::Resume, _) => PromptFocus::Text,
                (PromptFocus::ForwardPrompt, _) => PromptFocus::Text,
            };
        }
        KeyCode::BackTab => {
            app.prompt_focus = match (&app.prompt_focus, app.selected_mode) {
                (PromptFocus::Text, ExecutionMode::Solo) => PromptFocus::SessionName,
                (PromptFocus::Text, ExecutionMode::Relay) => PromptFocus::ForwardPrompt,
                (PromptFocus::Text, _) => PromptFocus::Resume,
                (PromptFocus::SessionName, _) => PromptFocus::Text,
                (PromptFocus::Iterations, _) => {
                    sync_iterations_buf(app);
                    PromptFocus::SessionName
                }
                (PromptFocus::Resume, _) => {
                    app.iterations_buf = app.iterations.to_string();
                    PromptFocus::Iterations
                }
                (PromptFocus::ForwardPrompt, _) => PromptFocus::Resume,
            };
        }
        KeyCode::Char(' ')
            if app.prompt_focus == PromptFocus::Resume =>
        {
            app.resume_previous = !app.resume_previous;
        }
        KeyCode::Char(' ')
            if app.prompt_focus == PromptFocus::ForwardPrompt =>
        {
            app.forward_prompt = !app.forward_prompt;
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
            PromptFocus::Text => handle_prompt_text_key(app, key),
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
            PromptFocus::Resume | PromptFocus::ForwardPrompt => {}
        },
    }
}

// ---------------------------------------------------------------------------
// Generic text editing helpers (buffer, cursor) — used by prompt and pipeline
// ---------------------------------------------------------------------------

fn handle_text_key(text: &mut String, cursor: &mut usize, key: KeyEvent) {
    clamp_cursor(text, cursor);
    let alt = key.modifiers.contains(KeyModifiers::ALT);
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

    match key.code {
        KeyCode::Left => {
            if alt {
                move_cursor_word_left(text, cursor);
            } else {
                *cursor = prev_char_boundary(text, *cursor);
            }
        }
        KeyCode::Up => move_cursor_line_up(text, cursor),
        KeyCode::Right => {
            if alt {
                move_cursor_word_right(text, cursor);
            } else {
                *cursor = next_char_boundary(text, *cursor);
            }
        }
        KeyCode::Down => move_cursor_line_down(text, cursor),
        KeyCode::Home => *cursor = 0,
        KeyCode::End => *cursor = text.len(),
        KeyCode::Backspace => {
            if alt {
                delete_word_left(text, cursor);
            } else {
                delete_char_left(text, cursor);
            }
        }
        KeyCode::Enter => {
            insert_text(text, cursor, "\n");
        }
        KeyCode::Char(c) if !alt && !ctrl => {
            let mut s = [0u8; 4];
            insert_text(text, cursor, c.encode_utf8(&mut s));
        }
        _ => {}
    }
}

fn insert_text(text: &mut String, cursor: &mut usize, input: &str) {
    if input.is_empty() {
        return;
    }
    clamp_cursor(text, cursor);
    let normalized = if input.contains('\r') {
        std::borrow::Cow::Owned(input.replace("\r\n", "\n").replace('\r', "\n"))
    } else {
        std::borrow::Cow::Borrowed(input)
    };
    text.insert_str(*cursor, &normalized);
    *cursor += normalized.len();
}

fn clamp_cursor(text: &str, cursor: &mut usize) {
    *cursor = (*cursor).min(text.len());
    while *cursor > 0 && !text.is_char_boundary(*cursor) {
        *cursor -= 1;
    }
}

fn prev_char_boundary(s: &str, idx: usize) -> usize {
    if idx == 0 {
        return 0;
    }
    s[..idx].char_indices().last().map(|(i, _)| i).unwrap_or(0)
}

fn next_char_boundary(s: &str, idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    let mut iter = s[idx..].char_indices();
    let _ = iter.next();
    if let Some((offset, _)) = iter.next() {
        idx + offset
    } else {
        s.len()
    }
}

fn char_before(s: &str, idx: usize) -> Option<char> {
    if idx == 0 {
        None
    } else {
        s[..idx].chars().next_back()
    }
}

fn char_at(s: &str, idx: usize) -> Option<char> {
    if idx >= s.len() {
        None
    } else {
        s[idx..].chars().next()
    }
}

fn move_cursor_word_left(text: &str, cursor: &mut usize) {
    let mut idx = *cursor;
    while idx > 0 {
        let Some(ch) = char_before(text, idx) else {
            break;
        };
        if ch.is_whitespace() {
            idx = prev_char_boundary(text, idx);
        } else {
            break;
        }
    }
    while idx > 0 {
        let Some(ch) = char_before(text, idx) else {
            break;
        };
        if !ch.is_whitespace() {
            idx = prev_char_boundary(text, idx);
        } else {
            break;
        }
    }
    *cursor = idx;
}

fn move_cursor_word_right(text: &str, cursor: &mut usize) {
    let mut idx = *cursor;
    let len = text.len();
    while idx < len {
        let Some(ch) = char_at(text, idx) else {
            break;
        };
        if ch.is_whitespace() {
            idx = next_char_boundary(text, idx);
        } else {
            break;
        }
    }
    while idx < len {
        let Some(ch) = char_at(text, idx) else {
            break;
        };
        if !ch.is_whitespace() {
            idx = next_char_boundary(text, idx);
        } else {
            break;
        }
    }
    *cursor = idx;
}

fn line_start(text: &str, cursor: usize) -> usize {
    text[..cursor].rfind('\n').map_or(0, |idx| idx + 1)
}

fn line_end(text: &str, cursor: usize) -> usize {
    text[cursor..]
        .find('\n')
        .map_or(text.len(), |offset| cursor + offset)
}

fn char_offset_in_line(text: &str, cursor: usize, start: usize) -> usize {
    text[start..cursor].chars().count()
}

fn byte_index_for_char_offset(text: &str, start: usize, end: usize, char_offset: usize) -> usize {
    if start >= end {
        return start;
    }
    for (seen, (offset, _)) in text[start..end].char_indices().enumerate() {
        if seen == char_offset {
            return start + offset;
        }
    }
    end
}

fn move_cursor_line_up(text: &str, cursor: &mut usize) {
    clamp_cursor(text, cursor);
    let curr_start = line_start(text, *cursor);
    if curr_start == 0 {
        return;
    }
    let target_col = char_offset_in_line(text, *cursor, curr_start);
    let prev_end = curr_start - 1;
    let prev_start = text[..prev_end].rfind('\n').map_or(0, |idx| idx + 1);
    *cursor = byte_index_for_char_offset(text, prev_start, prev_end, target_col);
}

fn move_cursor_line_down(text: &str, cursor: &mut usize) {
    clamp_cursor(text, cursor);
    let curr_start = line_start(text, *cursor);
    let curr_end = line_end(text, *cursor);
    if curr_end == text.len() {
        return;
    }
    let target_col = char_offset_in_line(text, *cursor, curr_start);
    let next_start = curr_end + 1;
    let next_end = text[next_start..]
        .find('\n')
        .map_or(text.len(), |offset| next_start + offset);
    *cursor = byte_index_for_char_offset(text, next_start, next_end, target_col);
}

fn delete_char_left(text: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let start = prev_char_boundary(text, *cursor);
    text.replace_range(start..*cursor, "");
    *cursor = start;
}

fn delete_word_left(text: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let mut start = *cursor;
    while start > 0 {
        let Some(ch) = char_before(text, start) else {
            break;
        };
        if ch.is_whitespace() {
            start = prev_char_boundary(text, start);
        } else {
            break;
        }
    }
    while start > 0 {
        let Some(ch) = char_before(text, start) else {
            break;
        };
        if !ch.is_whitespace() {
            start = prev_char_boundary(text, start);
        } else {
            break;
        }
    }
    text.replace_range(start..*cursor, "");
    *cursor = start;
}

// Thin wrappers for prompt screen (backward compat)

fn handle_prompt_text_key(app: &mut App, key: KeyEvent) {
    handle_text_key(&mut app.prompt_text, &mut app.prompt_cursor, key);
}

fn insert_prompt_text(app: &mut App, text: &str) {
    insert_text(&mut app.prompt_text, &mut app.prompt_cursor, text);
}

// ---------------------------------------------------------------------------
// Pipeline key handling
// ---------------------------------------------------------------------------

fn handle_pipeline_paste(app: &mut App, text: &str) {
    if app.pipeline_show_edit {
        match app.pipeline_edit_field {
            PipelineEditField::Name => {
                let clean = text.replace(['\n', '\r'], " ");
                app.pipeline_edit_name_buf.push_str(&clean);
                app.pipeline_edit_name_cursor = app.pipeline_edit_name_buf.len();
            }
            PipelineEditField::Prompt => {
                insert_text(
                    &mut app.pipeline_edit_prompt_buf,
                    &mut app.pipeline_edit_prompt_cursor,
                    text,
                );
            }
            PipelineEditField::SessionId => {
                let clean = text.replace(['\n', '\r'], "");
                app.pipeline_edit_session_buf.push_str(&clean);
                app.pipeline_edit_session_cursor = app.pipeline_edit_session_buf.len();
            }
            PipelineEditField::Agent => {}
        }
    } else if let Some(PipelineDialogMode::Save) = app.pipeline_file_dialog {
        app.pipeline_file_input.push_str(text);
    } else {
        match app.pipeline_focus {
            PipelineFocus::InitialPrompt => {
                insert_text(
                    &mut app.pipeline_def.initial_prompt,
                    &mut app.pipeline_prompt_cursor,
                    text,
                );
            }
            PipelineFocus::SessionName => {
                let clean = text.replace(['\n', '\r'], "");
                app.pipeline_session_name.push_str(&clean);
            }
            PipelineFocus::Iterations => {
                for ch in text.chars() {
                    if ch.is_ascii_digit() {
                        app.pipeline_iterations_buf.push(ch);
                    }
                }
                if !app.pipeline_iterations_buf.is_empty() {
                    let v: u32 = app.pipeline_iterations_buf.parse().unwrap_or(1).clamp(1, 99);
                    app.pipeline_iterations_buf = v.to_string();
                }
            }
            PipelineFocus::Builder => {}
        }
    }
}

fn handle_pipeline_key(app: &mut App, key: KeyEvent) {
    // Dispatch priority: edit popup > file dialog > remove-conn > connect mode > normal
    if app.pipeline_show_edit {
        handle_pipeline_edit_key(app, key);
        return;
    }
    if app.pipeline_file_dialog.is_some() {
        handle_pipeline_dialog_key(app, key);
        return;
    }
    if app.pipeline_removing_conn {
        handle_pipeline_remove_conn_key(app, key);
        return;
    }

    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

    // Connect mode
    if app.pipeline_connecting_from.is_some() {
        match key.code {
            KeyCode::Esc => {
                app.pipeline_connecting_from = None;
            }
            KeyCode::Enter => {
                if let (Some(from), Some(to)) =
                    (app.pipeline_connecting_from, app.pipeline_block_cursor)
                {
                    if from == to {
                        app.error_modal = Some("Cannot connect block to itself".into());
                    } else if app
                        .pipeline_def
                        .connections
                        .iter()
                        .any(|c| c.from == from && c.to == to)
                    {
                        app.error_modal = Some("Connection already exists".into());
                    } else if pipeline_mod::would_create_cycle(&app.pipeline_def, from, to) {
                        app.error_modal = Some("Would create a cycle".into());
                    } else {
                        app.pipeline_def.connections.push(
                            pipeline_mod::PipelineConnection { from, to },
                        );
                        app.pipeline_connecting_from = None;
                    }
                }
            }
            KeyCode::Up | KeyCode::Char('k') => pipeline_spatial_nav(app, NavAxis::Vertical, true),
            KeyCode::Down | KeyCode::Char('j') => pipeline_spatial_nav(app, NavAxis::Vertical, false),
            KeyCode::Left | KeyCode::Char('h') => pipeline_spatial_nav(app, NavAxis::Horizontal, true),
            KeyCode::Right | KeyCode::Char('l') => pipeline_spatial_nav(app, NavAxis::Horizontal, false),
            _ => {}
        }
        return;
    }

    // Normal pipeline keys
    match key.code {
        KeyCode::Esc => {
            app.screen = Screen::Home;
        }
        KeyCode::Tab => {
            app.pipeline_focus = match app.pipeline_focus {
                PipelineFocus::InitialPrompt => PipelineFocus::SessionName,
                PipelineFocus::SessionName => PipelineFocus::Iterations,
                PipelineFocus::Iterations => PipelineFocus::Builder,
                PipelineFocus::Builder => PipelineFocus::InitialPrompt,
            };
        }
        KeyCode::BackTab => {
            app.pipeline_focus = match app.pipeline_focus {
                PipelineFocus::InitialPrompt => PipelineFocus::Builder,
                PipelineFocus::SessionName => PipelineFocus::InitialPrompt,
                PipelineFocus::Iterations => PipelineFocus::SessionName,
                PipelineFocus::Builder => PipelineFocus::Iterations,
            };
        }
        KeyCode::Char('?') if !ctrl => {
            app.show_help_popup = true;
            app.help_popup_scroll = 0;
        }
        // Ctrl+S: save — always open dialog, prefill with current name
        KeyCode::Char('s') if ctrl => {
            app.pipeline_file_dialog = Some(PipelineDialogMode::Save);
            app.pipeline_file_input = app
                .pipeline_save_path
                .as_ref()
                .and_then(|p| p.file_stem())
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
        }
        // Ctrl+L: load
        KeyCode::Char('l') if ctrl => {
            app.pipeline_file_dialog = Some(PipelineDialogMode::Load);
            app.pipeline_file_list = pipeline_mod::list_pipeline_files()
                .unwrap_or_default()
                .iter()
                .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(|s| s.to_string()))
                .collect();
            app.pipeline_file_cursor = 0;
        }
        // F5: run
        KeyCode::F(5) => {
            start_pipeline_execution(app);
        }
        _ => match app.pipeline_focus {
            PipelineFocus::InitialPrompt => {
                handle_text_key(
                    &mut app.pipeline_def.initial_prompt,
                    &mut app.pipeline_prompt_cursor,
                    key,
                );
            }
            PipelineFocus::SessionName => match key.code {
                KeyCode::Char(c) => app.pipeline_session_name.push(c),
                KeyCode::Backspace => { app.pipeline_session_name.pop(); }
                _ => {}
            },
            PipelineFocus::Iterations => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline_iterations_buf.push(c);
                    let v: u32 = app.pipeline_iterations_buf.parse().unwrap_or(1).clamp(1, 99);
                    app.pipeline_iterations_buf = v.to_string();
                }
                KeyCode::Backspace => {
                    app.pipeline_iterations_buf.pop();
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    let v: u32 = app.pipeline_iterations_buf.parse().unwrap_or(1);
                    let v = (v + 1).min(99);
                    app.pipeline_iterations_buf = v.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    let v: u32 = app.pipeline_iterations_buf.parse().unwrap_or(1);
                    let v = v.saturating_sub(1).max(1);
                    app.pipeline_iterations_buf = v.to_string();
                }
                _ => {}
            },
            PipelineFocus::Builder => handle_pipeline_builder_key(app, key),
        },
    }
}

fn handle_pipeline_builder_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('a') => {
            let pos = pipeline_mod::next_free_position(&app.pipeline_def);
            let id = app.pipeline_next_id;
            app.pipeline_next_id += 1;
            let default_agent = app.config.agents.first()
                .map(|a| a.name.clone())
                .unwrap_or_else(|| "Claude".to_string());
            app.pipeline_def.blocks.push(pipeline_mod::PipelineBlock {
                id,
                name: format!("Block#{id}"),
                agent: default_agent,
                prompt: String::new(),
                session_id: None,
                position: pos,
            });
            app.pipeline_block_cursor = Some(id);
            pipeline_ensure_visible(app);
        }
        KeyCode::Char('d') => {
            if let Some(sel) = app.pipeline_block_cursor {
                let old_index = app
                    .pipeline_def
                    .blocks
                    .iter()
                    .position(|b| b.id == sel)
                    .unwrap_or(0);
                app.pipeline_def.blocks.retain(|b| b.id != sel);
                app.pipeline_def
                    .connections
                    .retain(|c| c.from != sel && c.to != sel);
                if app.pipeline_def.blocks.is_empty() {
                    app.pipeline_block_cursor = None;
                } else {
                    let new_idx = old_index.min(app.pipeline_def.blocks.len() - 1);
                    app.pipeline_block_cursor = Some(app.pipeline_def.blocks[new_idx].id);
                    pipeline_ensure_visible(app);
                }
            }
        }
        KeyCode::Char('e') | KeyCode::Enter => {
            if let Some(sel) = app.pipeline_block_cursor {
                if let Some(block) = app.pipeline_def.blocks.iter().find(|b| b.id == sel) {
                    app.pipeline_show_edit = true;
                    app.pipeline_edit_field = PipelineEditField::Name;
                    app.pipeline_edit_name_buf = block.name.clone();
                    app.pipeline_edit_name_cursor = block.name.len();
                    app.pipeline_edit_agent_idx = app.config.agents
                        .iter()
                        .position(|a| a.name == block.agent)
                        .unwrap_or(0);
                    app.pipeline_edit_prompt_buf = block.prompt.clone();
                    app.pipeline_edit_prompt_cursor = block.prompt.len();
                    app.pipeline_edit_session_buf =
                        block.session_id.clone().unwrap_or_default();
                    app.pipeline_edit_session_cursor = app.pipeline_edit_session_buf.len();
                }
            }
        }
        KeyCode::Char('c') => {
            if let Some(sel) = app.pipeline_block_cursor {
                app.pipeline_connecting_from = Some(sel);
            }
        }
        KeyCode::Char('x') => {
            if let Some(sel) = app.pipeline_block_cursor {
                let conn_count = app
                    .pipeline_def
                    .connections
                    .iter()
                    .filter(|c| c.from == sel || c.to == sel)
                    .count();
                if conn_count == 0 {
                    app.error_modal = Some("No connections on this block".into());
                } else {
                    app.pipeline_removing_conn = true;
                    app.pipeline_conn_cursor = 0;
                }
            }
        }
        // Arrow/hjkl navigate selection, Shift+Arrow/Shift+hjkl move selected block.
        // Ctrl+Arrow scrolls the canvas.
        KeyCode::Up | KeyCode::Char('k') => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                app.pipeline_canvas_offset.1 = app.pipeline_canvas_offset.1.saturating_sub(1);
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
                app.pipeline_canvas_offset.1 += 1;
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
                app.pipeline_canvas_offset.0 = app.pipeline_canvas_offset.0.saturating_sub(1);
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
                app.pipeline_canvas_offset.0 += 1;
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

enum NavAxis {
    Horizontal,
    Vertical,
}

fn pipeline_builder_move_mode(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::SHIFT)
}

fn pipeline_move_selected_block(app: &mut App, dx: i16, dy: i16) {
    if app.pipeline_def.blocks.is_empty() {
        app.pipeline_block_cursor = None;
        return;
    }

    let Some(sel_id) = app.pipeline_block_cursor else {
        app.pipeline_block_cursor = app.pipeline_def.blocks.first().map(|b| b.id);
        return;
    };
    let Some(sel_idx) = app.pipeline_def.blocks.iter().position(|b| b.id == sel_id) else {
        app.pipeline_block_cursor = app.pipeline_def.blocks.first().map(|b| b.id);
        return;
    };

    let (sx, sy) = app.pipeline_def.blocks[sel_idx].position;
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
        .pipeline_def
        .blocks
        .iter()
        .position(|b| b.id != sel_id && b.position == next_pos)
    {
        app.pipeline_def.blocks[other_idx].position = (sx, sy);
    }

    app.pipeline_def.blocks[sel_idx].position = next_pos;
}

fn pipeline_spatial_nav(app: &mut App, axis: NavAxis, negative: bool) {
    let Some(sel_id) = app.pipeline_block_cursor else {
        // Select first block if none selected
        app.pipeline_block_cursor = app.pipeline_def.blocks.first().map(|b| b.id);
        return;
    };
    let Some(sel_block) = app.pipeline_def.blocks.iter().find(|b| b.id == sel_id) else {
        return;
    };
    let (sx, sy) = (sel_block.position.0 as i32, sel_block.position.1 as i32);

    let mut best: Option<(BlockId, i32)> = None;
    for block in &app.pipeline_def.blocks {
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
        app.pipeline_block_cursor = Some(id);
    }
}

fn pipeline_ensure_visible(app: &mut App) {
    use crate::screen::pipeline::{BLOCK_H, BLOCK_W, CELL_H, CELL_W};

    let Some(sel_id) = app.pipeline_block_cursor else {
        return;
    };
    let Some(block) = app.pipeline_def.blocks.iter().find(|b| b.id == sel_id) else {
        return;
    };

    let sx = block.position.0 as i16 * CELL_W as i16;
    let sy = block.position.1 as i16 * CELL_H as i16;

    // Canvas inner ≈ terminal size minus chrome (title 3 + prompt 6 + help 2 + borders 2)
    let (term_w, term_h) = crossterm::terminal::size().unwrap_or((80, 24));
    let visible_w = (term_w.saturating_sub(4)) as i16;   // border + padding
    let visible_h = (term_h.saturating_sub(13)) as i16;  // title+prompt+help+borders

    let ox = &mut app.pipeline_canvas_offset.0;
    let oy = &mut app.pipeline_canvas_offset.1;

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

fn handle_pipeline_edit_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.pipeline_show_edit = false;
        }
        KeyCode::Tab => {
            app.pipeline_edit_field = match app.pipeline_edit_field {
                PipelineEditField::Name => PipelineEditField::Agent,
                PipelineEditField::Agent => PipelineEditField::Prompt,
                PipelineEditField::Prompt => PipelineEditField::SessionId,
                PipelineEditField::SessionId => PipelineEditField::Name,
            };
        }
        KeyCode::Enter => {
            match app.pipeline_edit_field {
                PipelineEditField::Name
                | PipelineEditField::Agent
                | PipelineEditField::SessionId => {
                    // Confirm and save
                    if let Some(sel) = app.pipeline_block_cursor {
                        if let Some(block) =
                            app.pipeline_def.blocks.iter_mut().find(|b| b.id == sel)
                        {
                            block.name = app.pipeline_edit_name_buf.clone();
                            block.agent = app.config.agents
                                .get(app.pipeline_edit_agent_idx)
                                .map(|a| a.name.clone())
                                .unwrap_or_else(|| "Claude".to_string());
                            block.prompt = app.pipeline_edit_prompt_buf.clone();
                            block.session_id = if app.pipeline_edit_session_buf.is_empty() {
                                None
                            } else {
                                Some(app.pipeline_edit_session_buf.clone())
                            };
                        }
                    }
                    app.pipeline_show_edit = false;
                }
                PipelineEditField::Prompt => {
                    // Enter inserts newline in prompt field
                    insert_text(
                        &mut app.pipeline_edit_prompt_buf,
                        &mut app.pipeline_edit_prompt_cursor,
                        "\n",
                    );
                }
            }
        }
        _ => match app.pipeline_edit_field {
            PipelineEditField::Name => match key.code {
                KeyCode::Char(c) => {
                    app.pipeline_edit_name_buf.push(c);
                    app.pipeline_edit_name_cursor = app.pipeline_edit_name_buf.len();
                }
                KeyCode::Backspace => {
                    app.pipeline_edit_name_buf.pop();
                    app.pipeline_edit_name_cursor = app.pipeline_edit_name_buf.len();
                }
                _ => {}
            },
            PipelineEditField::Agent => match key.code {
                KeyCode::Left | KeyCode::Char('k') => {
                    let len = app.config.agents.len().max(1);
                    app.pipeline_edit_agent_idx =
                        (app.pipeline_edit_agent_idx + len - 1) % len;
                }
                KeyCode::Right | KeyCode::Char('j') => {
                    let len = app.config.agents.len().max(1);
                    app.pipeline_edit_agent_idx =
                        (app.pipeline_edit_agent_idx + 1) % len;
                }
                _ => {}
            },
            PipelineEditField::Prompt => {
                handle_text_key(
                    &mut app.pipeline_edit_prompt_buf,
                    &mut app.pipeline_edit_prompt_cursor,
                    key,
                );
            }
            PipelineEditField::SessionId => match key.code {
                KeyCode::Char(c) => {
                    app.pipeline_edit_session_buf.push(c);
                    app.pipeline_edit_session_cursor = app.pipeline_edit_session_buf.len();
                }
                KeyCode::Backspace => {
                    app.pipeline_edit_session_buf.pop();
                    app.pipeline_edit_session_cursor = app.pipeline_edit_session_buf.len();
                }
                _ => {}
            },
        },
    }
}

fn handle_pipeline_dialog_key(app: &mut App, key: KeyEvent) {
    match app.pipeline_file_dialog {
        Some(PipelineDialogMode::Save) => match key.code {
            KeyCode::Esc => {
                app.pipeline_file_dialog = None;
            }
            KeyCode::Enter => {
                if !app.pipeline_file_input.is_empty() {
                    let dir = pipeline_mod::ensure_pipelines_dir();
                    match dir {
                        Ok(dir) => {
                            let filename =
                                format!("{}.toml", app.pipeline_file_input.trim());
                            let path = dir.join(&filename);
                            match pipeline_mod::save_pipeline(&app.pipeline_def, &path) {
                                Ok(()) => {
                                    app.pipeline_save_path = Some(path);
                                    app.pipeline_file_dialog = None;
                                }
                                Err(e) => {
                                    app.error_modal = Some(format!("Save failed: {e}"));
                                }
                            }
                        }
                        Err(e) => {
                            app.error_modal =
                                Some(format!("Cannot create pipelines dir: {e}"));
                        }
                    }
                }
            }
            KeyCode::Char(c) => {
                app.pipeline_file_input.push(c);
            }
            KeyCode::Backspace => {
                app.pipeline_file_input.pop();
            }
            _ => {}
        },
        Some(PipelineDialogMode::Load) => match key.code {
            KeyCode::Esc => {
                app.pipeline_file_dialog = None;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.pipeline_file_cursor = app.pipeline_file_cursor.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if app.pipeline_file_cursor + 1 < app.pipeline_file_list.len() {
                    app.pipeline_file_cursor += 1;
                }
            }
            KeyCode::Enter => {
                if let Some(filename) = app.pipeline_file_list.get(app.pipeline_file_cursor) {
                    let path = pipeline_mod::pipelines_dir().join(filename);
                    match pipeline_mod::load_pipeline(&path) {
                        Ok(def) => {
                            let max_id = def.blocks.iter().map(|b| b.id).max().unwrap_or(0);
                            app.pipeline_next_id = max_id + 1;
                            app.pipeline_def = def;
                            app.pipeline_save_path = Some(path);
                            app.pipeline_block_cursor =
                                app.pipeline_def.blocks.first().map(|b| b.id);
                            app.pipeline_file_dialog = None;
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

fn handle_pipeline_remove_conn_key(app: &mut App, key: KeyEvent) {
    let sel = app.pipeline_block_cursor.unwrap_or(0);
    let conns: Vec<usize> = app
        .pipeline_def
        .connections
        .iter()
        .enumerate()
        .filter(|(_, c)| c.from == sel || c.to == sel)
        .map(|(i, _)| i)
        .collect();

    match key.code {
        KeyCode::Esc => {
            app.pipeline_removing_conn = false;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.pipeline_conn_cursor = app.pipeline_conn_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.pipeline_conn_cursor + 1 < conns.len() {
                app.pipeline_conn_cursor += 1;
            }
        }
        KeyCode::Enter => {
            if let Some(&conn_idx) = conns.get(app.pipeline_conn_cursor) {
                app.pipeline_def.connections.remove(conn_idx);
            }
            app.pipeline_removing_conn = false;
        }
        _ => {}
    }
}

fn start_pipeline_execution(app: &mut App) {
    // Validate
    if app.pipeline_def.blocks.is_empty() {
        app.error_modal = Some("Add at least one block before running".into());
        return;
    }
    if app.pipeline_def.initial_prompt.trim().is_empty() {
        app.error_modal = Some("Enter an initial prompt".into());
        return;
    }
    let iterations: u32 = app
        .pipeline_iterations_buf
        .parse()
        .unwrap_or(0);
    if iterations < 1 {
        app.error_modal = Some("Iterations must be at least 1".into());
        return;
    }
    app.pipeline_def.iterations = iterations;

    if pipeline_mod::topological_layers(&app.pipeline_def).is_err() {
        app.error_modal = Some("Pipeline contains a cycle".into());
        return;
    }

    // Check agent availability per block
    let avail_agents: std::collections::HashMap<String, bool> =
        app.available_agents().into_iter().map(|(a, avail)| (a.name.clone(), avail)).collect();
    for block in &app.pipeline_def.blocks {
        match avail_agents.get(&block.agent) {
            Some(true) => {}
            Some(false) => {
                app.error_modal = Some(format!("{} is not available (block {})", block.agent, block.id));
                return;
            }
            None => {
                app.error_modal = Some(format!("Agent '{}' not found (block {})", block.agent, block.id));
                return;
            }
        }
    }

    // Set running state
    app.screen = Screen::Running;
    app.progress_events.clear();
    app.is_running = true;
    app.run_error = None;
    app.consolidation_active = false;
    app.diagnostic_running = false;

    // Copy prompt/session for running screen display
    app.prompt_text = app.pipeline_def.initial_prompt.clone();
    app.session_name = app.pipeline_session_name.clone();
    app.iterations = iterations;

    // Build agent configs keyed by agent name
    let mut agent_configs: std::collections::HashMap<String, (ProviderKind, ProviderConfig, bool)> =
        std::collections::HashMap::new();

    for block in &app.pipeline_def.blocks {
        if agent_configs.contains_key(&block.agent) {
            continue;
        }
        if let Some(agent_cfg) = app.config.agents.iter().find(|a| a.name == block.agent) {
            let agent_cfg = app.effective_agent_config(&agent_cfg.name).unwrap_or(agent_cfg);
            agent_configs.insert(
                block.agent.clone(),
                (agent_cfg.provider, agent_cfg.to_provider_config(), agent_cfg.use_cli),
            );
        }
    }

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
            app.is_running = false;
            return;
        }
    };

    // Output — use session name (not pipeline filename) for output dir
    let session_name = if app.pipeline_session_name.trim().is_empty() {
        None
    } else {
        Some(app.pipeline_session_name.trim())
    };
    let base_path = app.config.resolved_output_dir();
    let output = match OutputManager::new(
        &base_path,
        session_name,
    ) {
        Ok(o) => o,
        Err(e) => {
            app.error_modal = Some(format!("Cannot create output dir: {e}"));
            app.screen = Screen::Pipeline;
            app.is_running = false;
            return;
        }
    };

    // Write pipeline definition snapshot
    if let Err(e) = output.write_prompt(&app.pipeline_def.initial_prompt) {
        let _ = output.append_error(&format!("Failed to write prompt: {e}"));
    }
    if let Err(e) = output.write_pipeline_session_info(
        app.pipeline_def.blocks.len(),
        app.pipeline_def.connections.len(),
        iterations,
        app.pipeline_save_path
            .as_ref()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str()),
    ) {
        let _ = output.append_error(&format!("Failed to write session info: {e}"));
    }
    // Serialize pipeline definition
    match toml::to_string_pretty(&app.pipeline_def) {
        Ok(toml_str) => {
            if let Err(e) = std::fs::write(output.run_dir().join("pipeline.toml"), toml_str) {
                let _ = output.append_error(&format!("Failed to write pipeline.toml: {e}"));
            }
        }
        Err(e) => {
            let _ = output.append_error(&format!("Failed to serialize pipeline: {e}"));
        }
    }

    app.run_dir = Some(output.run_dir().to_path_buf());

    let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();
    app.progress_rx = Some(progress_rx);
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    app.cancel_flag = cancel.clone();

    let pipeline_def = app.pipeline_def.clone();
    let config = app.config.clone();
    let cli_timeout = app.effective_cli_timeout_seconds();

    tokio::spawn(async move {
        let result = pipeline_mod::run_pipeline(
            &pipeline_def,
            &config,
            agent_configs,
            client,
            cli_timeout,
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
                app.consolidation_phase = if app.config.agents.len() <= 1 {
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
                let max = app.config.agents.len().saturating_sub(1);
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
                if app.config.agents.len() <= 1 {
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
    app.prompt_cursor = 0;
    app.session_name.clear();
    app.selected_agents.clear();
    app.progress_events.clear();
    app.result_files.clear();
    app.result_preview.clear();
    app.iterations = 1;
    app.iterations_buf = "1".into();
    app.resume_previous = false;
    app.forward_prompt = false;
    app.consolidation_active = false;
    app.consolidation_phase = ConsolidationPhase::Confirm;
    app.consolidation_provider_cursor = 0;
    app.consolidation_prompt.clear();
    app.consolidation_running = false;
    app.consolidation_rx = None;
    app.diagnostic_running = false;
    app.diagnostic_rx = None;

    // Pipeline state
    app.pipeline_def = pipeline_mod::PipelineDefinition::default();
    app.pipeline_next_id = 1;
    app.pipeline_block_cursor = None;
    app.pipeline_focus = PipelineFocus::InitialPrompt;
    app.pipeline_canvas_offset = (0, 0);
    app.pipeline_prompt_cursor = 0;
    app.pipeline_session_name.clear();
    app.pipeline_iterations_buf = "1".into();
    app.pipeline_connecting_from = None;
    app.pipeline_removing_conn = false;
    app.pipeline_conn_cursor = 0;
    app.pipeline_show_edit = false;
    app.pipeline_edit_field = PipelineEditField::Name;
    app.pipeline_edit_name_buf.clear();
    app.pipeline_edit_name_cursor = 0;
    app.pipeline_edit_agent_idx = 0;
    app.pipeline_edit_prompt_buf.clear();
    app.pipeline_edit_prompt_cursor = 0;
    app.pipeline_edit_session_buf.clear();
    app.pipeline_edit_session_cursor = 0;
    app.pipeline_file_dialog = None;
    app.pipeline_file_input.clear();
    app.pipeline_file_list.clear();
    app.pipeline_file_cursor = 0;
    app.pipeline_save_path = None;
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
                EditPopupSection::Providers => EditPopupSection::Timeouts,
                EditPopupSection::Timeouts => EditPopupSection::Providers,
            };
            app.edit_buffer.clear();
        }
        KeyCode::Up | KeyCode::Char('k') if !app.edit_popup_editing => {
            match app.edit_popup_section {
                EditPopupSection::Providers => {
                    app.edit_popup_cursor = app.edit_popup_cursor.saturating_sub(1);
                }
                EditPopupSection::Timeouts => {
                    app.edit_popup_timeout_cursor = app.edit_popup_timeout_cursor.saturating_sub(1);
                }
            }
        }
        KeyCode::Down | KeyCode::Char('j') if !app.edit_popup_editing => {
            match app.edit_popup_section {
                EditPopupSection::Providers => {
                    let max = app.config.agents.len().saturating_sub(1);
                    if app.edit_popup_cursor < max {
                        app.edit_popup_cursor += 1;
                    }
                }
                EditPopupSection::Timeouts => {
                    let max = timeout_field_count().saturating_sub(1);
                    if app.edit_popup_timeout_cursor < max {
                        app.edit_popup_timeout_cursor += 1;
                    }
                }
            }
        }
        KeyCode::Char('a')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            app.edit_popup_field = EditField::ApiKey;
            app.edit_buffer = effective_section_config(app)
                .map(|c| c.api_key)
                .unwrap_or_default();
            app.edit_popup_editing = true;
        }
        KeyCode::Char('m')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            app.edit_popup_field = EditField::Model;
            app.edit_buffer = effective_section_config(app)
                .map(|c| c.model)
                .unwrap_or_default();
            app.edit_popup_editing = true;
        }
        KeyCode::Char('x')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            app.edit_popup_field = EditField::ExtraCliArgs;
            app.edit_buffer = effective_section_config(app)
                .map(|c| c.extra_cli_args)
                .unwrap_or_default();
            app.edit_popup_editing = true;
        }
        KeyCode::Char('l')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
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
        KeyCode::Char('c')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            toggle_cli_mode(app);
        }
        KeyCode::Char('d')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            toggle_diagnostic_agent(app);
        }
        KeyCode::Char('t')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            cycle_reasoning(app);
        }
        KeyCode::Char('b')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            toggle_cli_print_mode(app);
        }
        KeyCode::Char('n')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            add_new_agent(app);
        }
        KeyCode::Char('p')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            cycle_agent_provider(app);
        }
        KeyCode::Char('r')
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            if let Some(agent) = app.config.agents.get(app.edit_popup_cursor) {
                app.edit_popup_field = EditField::AgentName;
                app.edit_buffer = agent.name.clone();
                app.edit_popup_editing = true;
            }
        }
        KeyCode::Delete | KeyCode::Backspace
            if !app.edit_popup_editing && app.edit_popup_section == EditPopupSection::Providers =>
        {
            remove_agent(app);
        }
        KeyCode::Char('e') if !app.edit_popup_editing => {
            if app.edit_popup_section == EditPopupSection::Timeouts {
                begin_timeout_edit(app);
            }
        }
        KeyCode::Enter if !app.edit_popup_editing => {
            if app.edit_popup_section == EditPopupSection::Timeouts {
                begin_timeout_edit(app);
            } else {
                app.edit_popup_field = EditField::ApiKey;
                app.edit_buffer = effective_section_config(app)
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
            } else if matches!(app.edit_popup_field, EditField::AgentName) {
                commit_agent_rename(app);
                return;
            } else if app.edit_popup_section == EditPopupSection::Timeouts
                || matches!(app.edit_popup_field, EditField::TimeoutSeconds)
            {
                if let Err(e) = set_timeout_override_from_buffer(app) {
                    app.error_modal = Some(e);
                    return;
                }
            } else {
                let mut config = effective_section_config(app)
                    .unwrap_or_else(empty_provider_config);
                match app.edit_popup_field {
                    EditField::ApiKey => config.api_key = app.edit_buffer.clone(),
                    EditField::Model => config.model = app.edit_buffer.clone(),
                    EditField::ExtraCliArgs => config.extra_cli_args = app.edit_buffer.clone(),
                    EditField::OutputDir | EditField::TimeoutSeconds | EditField::AgentName => {}
                }
                set_section_config_override(app, config);
            }
            app.edit_buffer.clear();
            app.edit_popup_editing = false;
        }
        KeyCode::Backspace if app.edit_popup_editing => {
            app.edit_buffer.pop();
        }
        KeyCode::Char(c) if app.edit_popup_editing => {
            if matches!(app.edit_popup_field, EditField::TimeoutSeconds) {
                if c.is_ascii_digit() {
                    app.edit_buffer.push(c);
                }
            } else {
                app.edit_buffer.push(c);
            }
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
                let mut config = effective_section_config(app)
                    .unwrap_or_else(empty_provider_config);
                config.model = model;
                set_section_config_override(app, config);
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
            app.model_picker_active = false;
            app.model_picker_loading = false;
            app.model_picker_rx = None;
            app.error_modal = Some(format!("Failed to create HTTP client: {e}"));
            return;
        }
    };

    app.model_picker_active = true;
    app.model_picker_loading = true;
    app.model_picker_all_models.clear();
    app.model_picker_list.clear();
    app.model_picker_filter.clear();
    app.model_picker_cursor = 0;

    let (tx, rx) = mpsc::unbounded_channel();
    app.model_picker_rx = Some(rx);

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
            let current_model = effective_section_config(app);
            app.model_picker_cursor = current_model
                .as_ref()
                .and_then(|c| models.iter().position(|x| x == &c.model))
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

fn cycle_reasoning(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    let mut config = effective_section_config(app)
        .unwrap_or_else(empty_provider_config);

    match kind {
        ProviderKind::OpenAI => {
            config.reasoning_effort = match config.reasoning_effort.as_deref() {
                None => Some("low".into()),
                Some("low") => Some("medium".into()),
                Some("medium") => Some("high".into()),
                _ => None,
            };
        }
        ProviderKind::Anthropic | ProviderKind::Gemini => {
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

fn toggle_diagnostic_agent(app: &mut App) {
    let agent_name = match app.config.agents.get(app.edit_popup_cursor) {
        Some(a) => a.name.clone(),
        None => return,
    };
    if app.config.diagnostic_provider.as_deref() == Some(agent_name.as_str()) {
        app.config.diagnostic_provider = None;
    } else {
        app.config.diagnostic_provider = Some(agent_name);
    }
}

fn toggle_cli_mode(app: &mut App) {
    let kind = match selected_kind_for_edit(app) {
        Some(k) => k,
        None => return,
    };
    let cli_installed = app.cli_available.get(&kind).copied().unwrap_or(false);
    let mut config = effective_section_config(app)
        .unwrap_or_else(empty_provider_config);

    if !config.use_cli && !cli_installed {
        app.error_modal = Some(format!("{} CLI not installed", kind.display_name()));
        return;
    }

    config.use_cli = !config.use_cli;
    set_section_config_override(app, config);
}

fn toggle_cli_print_mode(app: &mut App) {
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

fn add_new_agent(app: &mut App) {
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
    app.edit_popup_cursor = app.config.agents.len() - 1;
    app.edit_popup_field = EditField::AgentName;
    app.edit_buffer = name;
    app.edit_popup_editing = true;
}

fn remove_agent(app: &mut App) {
    if app.config.agents.len() <= 1 {
        app.error_modal = Some("Cannot remove the last agent".into());
        return;
    }
    let idx = app.edit_popup_cursor;
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
    app.edit_popup_cursor = app.edit_popup_cursor.min(app.config.agents.len().saturating_sub(1));
}

fn cycle_agent_provider(app: &mut App) {
    let idx = app.edit_popup_cursor;
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

fn commit_agent_rename(app: &mut App) {
    let new_name = app.edit_buffer.trim().to_string();
    if new_name.is_empty() {
        app.error_modal = Some("Agent name cannot be empty".into());
        return;
    }
    let idx = app.edit_popup_cursor;
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
        if i != idx
            && OutputManager::sanitize_session_name(&a.name).to_lowercase() == sanitized
        {
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
    app.edit_buffer.clear();
    app.edit_popup_editing = false;
}

fn timeout_field_count() -> usize {
    3
}

fn begin_timeout_edit(app: &mut App) {
    app.edit_popup_field = EditField::TimeoutSeconds;
    app.edit_buffer = match app.edit_popup_timeout_cursor {
        0 => app.effective_http_timeout_seconds(),
        1 => app.effective_model_fetch_timeout_seconds(),
        2 => app.effective_cli_timeout_seconds(),
        _ => app.effective_http_timeout_seconds(),
    }
    .to_string();
    app.edit_popup_editing = true;
}

fn set_timeout_override_from_buffer(app: &mut App) -> Result<(), String> {
    let raw = app.edit_buffer.trim();
    if raw.is_empty() {
        return Err("Timeout cannot be empty".into());
    }
    let parsed = raw
        .parse::<u64>()
        .map_err(|_| "Timeout must be a positive integer (seconds)".to_string())?;
    if parsed == 0 {
        return Err("Timeout must be at least 1 second".into());
    }

    match app.edit_popup_timeout_cursor {
        0 => app.session_http_timeout_seconds = Some(parsed),
        1 => app.session_model_fetch_timeout_seconds = Some(parsed),
        2 => app.session_cli_timeout_seconds = Some(parsed),
        _ => return Err("Invalid timeout field".into()),
    }

    Ok(())
}

fn selected_kind_for_edit(app: &App) -> Option<ProviderKind> {
    app.config.agents.get(app.edit_popup_cursor).map(|a| a.provider)
}

/// Returns a ProviderConfig view for the current edit selection (Providers section).
fn effective_section_config(app: &App) -> Option<ProviderConfig> {
    let name = app.config.agents.get(app.edit_popup_cursor).map(|a| a.name.as_str())?;
    app.effective_agent_config(name).map(|a| a.to_provider_config())
}

fn set_section_config_override(app: &mut App, config: ProviderConfig) {
    if let Some(agent) = app.config.agents.get(app.edit_popup_cursor) {
        let name = agent.name.clone();
        let provider = agent.provider;
        let agent_config = AgentConfig::from_provider_config(name.clone(), provider, &config);
        app.session_overrides.insert(name, agent_config);
    }
}

fn empty_provider_config() -> ProviderConfig {
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
    let http_timeout_secs = app.effective_http_timeout_seconds().max(1);
    let cli_timeout_secs = app.effective_cli_timeout_seconds().max(1);
    let has_any_cli = app.selected_agents.iter().any(|name| {
        app.effective_agent_config(name)
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
    if app.config.diagnostic_provider.is_some() {
        prompt.push_str(
            "\n\nWrite any encountered issues (for example permission, tool, or environment issues) to an explicit \"Errors\" section of your report.",
        );
    }
    let agent_names = app.selected_agents.clone();
    let mode = app.selected_mode;
    let forward_prompt_flag = app.forward_prompt;
    let iterations = if mode == ExecutionMode::Solo {
        1
    } else {
        app.iterations
    };

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(http_timeout_secs))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            app.error_modal = Some(format!("Failed to create HTTP client: {e}"));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }
    };

    let mut agents: Vec<(String, Box<dyn provider::Provider>)> = Vec::new();
    let mut use_cli_by_agent: std::collections::HashMap<String, bool> =
        std::collections::HashMap::new();
    let mut run_models: Vec<(String, String)> = Vec::new();
    let mut agent_info: Vec<(String, String)> = Vec::new();
    for name in &agent_names {
        let agent_config = match app.effective_agent_config(name).cloned() {
            Some(cfg) => cfg,
            None => {
                app.error_modal = Some(format!("{name} is not configured"));
                app.screen = Screen::Prompt;
                app.is_running = false;
                return;
            }
        };

        if agent_config.use_cli
            && !app
                .cli_available
                .get(&agent_config.provider)
                .copied()
                .unwrap_or(false)
        {
            app.error_modal = Some(format!(
                "{} CLI is not installed",
                agent_config.provider.display_name()
            ));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }

        if !agent_config.use_cli && agent_config.api_key.trim().is_empty() {
            app.error_modal = Some(format!("{name} API key is missing"));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }

        run_models.push((
            name.clone(),
            if agent_config.model.trim().is_empty() {
                "(default)".to_string()
            } else {
                agent_config.model.clone()
            },
        ));
        agent_info.push((name.clone(), agent_config.provider.config_key().to_string()));
        use_cli_by_agent.insert(name.clone(), agent_config.use_cli);

        let pconfig = agent_config.to_provider_config();
        agents.push((
            name.clone(),
            provider::create_provider(
                agent_config.provider,
                &pconfig,
                client.clone(),
                config.default_max_tokens,
                config.max_history_messages,
                cli_timeout_secs,
            ),
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
    let mut swarm_initial_outputs: std::collections::HashMap<String, String> =
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
            match find_latest_compatible_run(&output_dir, mode, &agent_names) {
                Some(path) => path,
                None => {
                    app.error_modal = Some("No compatible previous run found to resume".into());
                    app.screen = Screen::Prompt;
                    app.is_running = false;
                    return;
                }
            }
        };

        let last_iteration =
            match find_last_complete_iteration_for_agents(&run_dir, &agent_names) {
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
                let last_agent = match agent_names.last() {
                    Some(n) => n,
                    None => {
                        app.error_modal = Some("No agents selected".into());
                        app.screen = Screen::Prompt;
                        app.is_running = false;
                        return;
                    }
                };
                let file_key = App::agent_file_key(last_agent);
                let prev_path =
                    run_dir.join(format!("{}_iter{}.md", file_key, last_iteration));
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
                for agent_name in &agent_names {
                    let file_key = App::agent_file_key(agent_name);
                    let prev_path =
                        run_dir.join(format!("{}_iter{}.md", file_key, last_iteration));
                    if let Ok(content) = std::fs::read_to_string(&prev_path) {
                        swarm_initial_outputs.insert(agent_name.clone(), content);
                    }
                }
                if swarm_initial_outputs.is_empty() {
                    app.error_modal = Some("No previous swarm outputs found to resume".into());
                    app.screen = Screen::Prompt;
                    app.is_running = false;
                    return;
                }
            }
            ExecutionMode::Solo | ExecutionMode::Pipeline => {}
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
        if let Err(e) = output.write_prompt(&prompt) {
            app.error_modal = Some(format!("Failed to write prompt file: {e}"));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }
        if let Err(e) =
            output.write_session_info(&mode, &agent_info, iterations, session_name, &run_models)
        {
            app.error_modal = Some(format!("Failed to write session metadata: {e}"));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }
    } else if let Err(e) = output.append_error(&format!(
        "Resumed {} mode for {} additional iteration(s), starting at iter {}",
        mode, iterations, start_iteration
    )) {
        app.error_modal = Some(format!("Failed to write resume log entry: {e}"));
        app.screen = Screen::Prompt;
        app.is_running = false;
        return;
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
            ExecutionMode::Solo => run_solo(&prompt, agents, &output, tx.clone(), cancel).await,
            ExecutionMode::Relay => {
                run_relay(
                    &prompt,
                    agents,
                    iterations,
                    start_iteration,
                    relay_initial_last_output,
                    forward_prompt_flag,
                    use_cli_by_agent.clone(),
                    &output,
                    tx.clone(),
                    cancel,
                )
                .await
            }
            ExecutionMode::Swarm => {
                run_swarm(
                    &prompt,
                    agents,
                    iterations,
                    start_iteration,
                    swarm_initial_outputs,
                    use_cli_by_agent,
                    &output,
                    tx.clone(),
                    cancel,
                )
                .await
            }
            ExecutionMode::Pipeline => {
                // Pipeline execution is handled by start_pipeline_execution
                return;
            }
        };
        if let Err(e) = result {
            let err_str = e.to_string();
            let _ = tx.send(ProgressEvent::AgentError {
                agent: agent_names.first().cloned().unwrap_or_default(),
                kind: ProviderKind::Anthropic,
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
    if app.selected_agents.len() <= 1 {
        return false;
    }
    if !matches!(
        app.selected_mode,
        ExecutionMode::Swarm | ExecutionMode::Solo | ExecutionMode::Pipeline
    ) {
        return false;
    }

    let Some(run_dir) = app.run_dir.as_ref() else {
        return false;
    };
    let agent_keys: Vec<String> = app.selected_agents.iter().map(|n| App::agent_file_key(n)).collect();
    find_last_iteration(run_dir, &agent_keys).is_some()
}

fn find_last_iteration(run_dir: &std::path::Path, agent_keys: &[String]) -> Option<u32> {
    let mut max_iter: Option<u32> = None;
    let entries = std::fs::read_dir(run_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if agent_keys.is_empty() {
            // Pipeline mode: only match known block output filename patterns
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

fn find_last_complete_iteration_for_agents(
    run_dir: &std::path::Path,
    agents: &[String],
) -> Option<u32> {
    use std::collections::{HashMap, HashSet};

    if agents.is_empty() {
        return None;
    }

    let agent_keys: Vec<String> = agents.iter().map(|n| App::agent_file_key(n)).collect();
    let mut iter_to_agents: HashMap<u32, HashSet<String>> = HashMap::new();
    let entries = std::fs::read_dir(run_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        for key in &agent_keys {
            if let Some(iter) = parse_agent_iteration_filename(&name, key) {
                iter_to_agents
                    .entry(iter)
                    .or_default()
                    .insert(key.clone());
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
    agents: &[String],
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
    agents: &[String],
) -> bool {
    let session_path = run_dir.join("session.toml");
    let Ok(content) = std::fs::read_to_string(session_path) else {
        return false;
    };
    let Ok(value) = content.parse::<toml::Value>() else {
        return false;
    };

    let Some(stored_mode) = value.get("mode").and_then(|v| v.as_str()) else {
        return false;
    };
    if stored_mode != mode.as_str() {
        return false;
    }

    let Some(stored_agents) = value.get("agents").and_then(|v| v.as_array()) else {
        return false;
    };
    let stored: std::collections::HashSet<String> = stored_agents
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    for agent_name in agents {
        if !stored.contains(agent_name) {
            return false;
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

/// Parse iteration from a pipeline block output filename.
/// Matches both named blocks (`{name}_b{id}_{agent}_iter{n}.md`)
/// and unnamed blocks (`block{id}_{agent}_iter{n}.md`).
fn parse_pipeline_iteration_filename(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let stem = name.trim_end_matches(".md");

    // Named blocks: must contain _b{digits}_ somewhere.
    // Search right-to-left so block names containing "_b" (e.g. "web_builder") are handled.
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

    // Unnamed blocks: must start with "block{digits}_".
    if let Some(rest) = stem.strip_prefix("block") {
        if let Some(underscore) = rest.find('_') {
            if rest[..underscore].parse::<u32>().is_ok() {
                return parse_iteration_from_filename(name);
            }
        }
    }

    None
}

fn parse_iteration_from_filename(name: &str) -> Option<u32> {
    // Match any pattern of {agent_key}_iter{N}.md
    if !name.ends_with(".md") {
        return None;
    }
    // Generic: find _iter{N} before .md suffix — works for both agent and block filenames
    let stem = name.trim_end_matches(".md");
    let iter_pos = stem.rfind("_iter")?;
    let iter_str = &stem[iter_pos + 5..];
    iter_str.parse::<u32>().ok()
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

    let agent_keys: Vec<String> = app.selected_agents.iter().map(|n| App::agent_file_key(n)).collect();
    let last_iteration = match find_last_iteration(&run_dir, &agent_keys) {
        Some(i) => i,
        None => {
            app.error_modal = Some("No iteration outputs found to consolidate".into());
            return;
        }
    };

    let agent_name = match app
        .config
        .agents
        .get(app.consolidation_provider_cursor)
        .map(|a| a.name.clone())
    {
        Some(n) => n,
        None => {
            app.error_modal = Some("Select an agent for consolidation".into());
            return;
        }
    };

    let agent_config = match app.effective_agent_config(&agent_name).cloned() {
        Some(cfg) => cfg,
        None => {
            app.error_modal = Some(format!("{agent_name} is not configured"));
            return;
        }
    };

    if agent_config.use_cli
        && !app
            .cli_available
            .get(&agent_config.provider)
            .copied()
            .unwrap_or(false)
    {
        app.error_modal = Some(format!(
            "{} CLI is not installed",
            agent_config.provider.display_name()
        ));
        return;
    }
    if !agent_config.use_cli && agent_config.api_key.trim().is_empty() {
        app.error_modal = Some(format!("{agent_name} API key is missing"));
        return;
    }

    let mut file_lines = Vec::new();
    if app.selected_mode == ExecutionMode::Pipeline {
        // Scan run dir for block output files from the target iteration
        if let Ok(entries) = std::fs::read_dir(&run_dir) {
            let mut paths: Vec<(String, std::path::PathBuf)> = entries
                .flatten()
                .filter_map(|entry| {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if parse_pipeline_iteration_filename(&name) == Some(last_iteration) {
                        Some((name, entry.path()))
                    } else {
                        None
                    }
                })
                .collect();
            paths.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, path) in paths {
                file_lines.push(format!("- {name}: {}", path.display()));
            }
        }
    } else {
        for name in &app.selected_agents {
            let file_key = App::agent_file_key(name);
            let path = run_dir.join(format!("{file_key}_iter{last_iteration}.md"));
            if path.exists() {
                file_lines.push(format!("- {name}: {}", path.display()));
            }
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

    let pconfig = agent_config.to_provider_config();
    let provider_kind = agent_config.provider;
    let provider = provider::create_provider(
        provider_kind,
        &pconfig,
        client,
        app.config.default_max_tokens,
        app.config.max_history_messages,
        app.effective_cli_timeout_seconds().max(1),
    );

    app.progress_events.push(ProgressEvent::AgentStarted {
        agent: agent_name.clone(),
        kind: provider_kind,
        iteration: last_iteration + 1,
    });
    app.progress_events.push(ProgressEvent::AgentLog {
        agent: agent_name.clone(),
        kind: provider_kind,
        iteration: last_iteration + 1,
        message: "consolidating reports".into(),
    });

    let file_key = App::agent_file_key(&agent_name);
    let output_path = run_dir.join(format!("consolidated_{file_key}.md"));
    let (tx, rx) = mpsc::unbounded_channel();
    app.consolidation_rx = Some(rx);
    app.consolidation_running = true;
    app.consolidation_active = false;
    app.is_running = true;

    tokio::spawn(async move {
        let mut provider = provider;
        let result = match provider.send(&prompt).await {
            Ok(resp) => match tokio::fs::write(&output_path, &resp.content).await {
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

    let agent_name = app
        .config
        .agents
        .get(app.consolidation_provider_cursor)
        .map(|a| a.name.clone())
        .unwrap_or_default();
    let kind = app
        .effective_agent_config(&agent_name)
        .map(|a| a.provider)
        .unwrap_or(ProviderKind::Anthropic);
    let agent_keys: Vec<String> = app.selected_agents.iter().map(|n| App::agent_file_key(n)).collect();
    let iteration = app
        .run_dir
        .as_deref()
        .and_then(|d| find_last_iteration(d, &agent_keys))
        .unwrap_or(1)
        + 1;

    match result {
        Ok(path) => {
            app.progress_events.push(ProgressEvent::AgentFinished {
                agent: agent_name.clone(),
                kind,
                iteration,
            });
            app.progress_events.push(ProgressEvent::AgentLog {
                agent: agent_name,
                kind,
                iteration,
                message: format!("Consolidation saved to {path}"),
            });
            app.consolidation_active = false;
            app.error_modal = Some("Consolidation completed".into());
        }
        Err(e) => {
            app.progress_events.push(ProgressEvent::AgentError {
                agent: agent_name,
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

    let diag_agent_name = match app.config.diagnostic_provider.as_deref() {
        Some(name) => name.to_string(),
        None => return,
    };
    let run_dir = match app.run_dir.clone() {
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
    let pconfig = agent_config.to_provider_config();

    if pconfig.use_cli
        && !app
            .cli_available
            .get(&diagnostic_kind)
            .copied()
            .unwrap_or(false)
    {
        app.error_modal = Some(format!(
            "Diagnostic agent '{}' CLI is not installed",
            diag_agent_name
        ));
        return;
    }
    if !pconfig.use_cli && pconfig.api_key.trim().is_empty() {
        app.error_modal = Some(format!(
            "Diagnostic agent '{}' API key is missing",
            diag_agent_name
        ));
        return;
    }

    let report_files = collect_report_files(&run_dir);
    let app_errors = collect_application_errors(app, &run_dir);
    let prompt = build_diagnostic_prompt(&report_files, &app_errors, pconfig.use_cli);

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
        app.effective_cli_timeout_seconds().max(1),
    );

    app.progress_events.push(ProgressEvent::AgentLog {
        agent: diag_agent_name,
        kind: diagnostic_kind,
        iteration: 0,
        message: "analyzing reports for errors".into(),
    });
    app.diagnostic_running = true;
    app.is_running = true;

    let output_path = run_dir.join("errors.md");
    let (tx, rx) = mpsc::unbounded_channel();
    app.diagnostic_rx = Some(rx);

    tokio::spawn(async move {
        let mut provider = provider;
        let result = match provider.send(&prompt).await {
            Ok(resp) => match tokio::fs::write(&output_path, &resp.content).await {
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
            app.progress_events.push(ProgressEvent::AgentLog {
                agent: agent_name,
                kind,
                iteration: 0,
                message: format!("Diagnostic report saved to {path}"),
            });
        }
        Err(e) => {
            app.progress_events.push(ProgressEvent::AgentError {
                agent: agent_name,
                kind,
                iteration: 0,
                error: e.clone(),
                details: Some(e),
            });
        }
    }
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
            agent,
            iteration,
            error,
            details,
            ..
        } = event
        {
            let body = details.as_deref().unwrap_or(error);
            errors.push(format!("[{agent} iter {iteration}] {body}"));
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
        "Analyze all reports for OPERATIONAL errors only and produce a markdown report.\n",
    );
    prompt.push_str("Focus exclusively on errors that prevented an agent from completing its task:\n");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::execution::ProgressEvent;
    use crossterm::event::KeyEvent;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    fn test_config() -> AppConfig {
        AppConfig {
            output_dir: "/tmp".to_string(),
            default_max_tokens: 4096,
            max_history_messages: 50,
            http_timeout_seconds: 120,
            model_fetch_timeout_seconds: 30,
            cli_timeout_seconds: 600,
            diagnostic_provider: None,
            agents: Vec::new(),
            providers: HashMap::new(),
        }
    }

    fn test_app() -> App {
        App::new(test_config())
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn write_session_toml(run_dir: &Path, mode: &str, agents: &[&str]) {
        let agent_values = agents
            .iter()
            .map(|a| format!("\"{a}\""))
            .collect::<Vec<_>>()
            .join(", ");
        fs::write(
            run_dir.join("session.toml"),
            format!("mode = \"{mode}\"\nagents = [{agent_values}]\n"),
        )
        .unwrap();
    }

    fn write_agent_iter(run_dir: &Path, agent_key: &str, iter: u32) {
        fs::write(
            run_dir.join(format!("{agent_key}_iter{iter}.md")),
            format!("{agent_key} {iter}"),
        )
        .unwrap();
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

    #[test]
    fn parse_iteration_from_filename_valid() {
        assert_eq!(parse_iteration_from_filename("openai_iter5.md"), Some(5));
    }

    #[test]
    fn parse_iteration_from_filename_consolidated() {
        assert_eq!(
            parse_iteration_from_filename("consolidated_anthropic.md"),
            None
        );
    }

    #[test]
    fn parse_iteration_from_filename_non_md() {
        assert_eq!(parse_iteration_from_filename("session.toml"), None);
    }

    #[test]
    fn parse_iteration_from_filename_accepts_any_iter_suffix() {
        // Low-level parser matches any {name}_iter{N}.md pattern;
        // find_last_iteration filters by known agent keys.
        assert_eq!(parse_iteration_from_filename("notes_iter42.md"), Some(42));
    }

    #[test]
    fn parse_block_iteration_named_block() {
        // {name}_b{id}_{agent}_iter{n}.md
        assert_eq!(
            parse_pipeline_iteration_filename("Analyzer_b1_Claude_iter2.md"),
            Some(2)
        );
    }

    #[test]
    fn parse_pipeline_iteration_unnamed_block() {
        // unnamed blocks use block{id}_{agent}_iter{n}.md pattern
        assert_eq!(
            parse_pipeline_iteration_filename("block1_openai_iter5.md"),
            Some(5)
        );
    }

    #[test]
    fn parse_block_iteration_different_agent() {
        assert_eq!(
            parse_pipeline_iteration_filename("Reviewer_b3_Gemini_iter5.md"),
            Some(5)
        );
    }

    #[test]
    fn parse_block_iteration_not_md() {
        assert_eq!(
            parse_pipeline_iteration_filename("Analyzer_b1_Claude_iter2.txt"),
            None
        );
    }

    #[test]
    fn parse_block_iteration_no_block_id_marker() {
        assert_eq!(
            parse_pipeline_iteration_filename("Claude_iter2.md"),
            None
        );
    }

    #[test]
    fn parse_block_iteration_non_numeric_block_id() {
        assert_eq!(
            parse_pipeline_iteration_filename("Analyzer_bx_Claude_iter2.md"),
            None
        );
    }

    #[test]
    fn parse_block_iteration_name_contains_b() {
        // Block name "web_builder" contains "_b" — parser must skip it and find _b7_
        assert_eq!(
            parse_pipeline_iteration_filename("web_builder_b7_Claude_iter1.md"),
            Some(1)
        );
    }

    #[test]
    fn parse_iteration_from_filename_matches_block_files() {
        // Named block format
        assert_eq!(
            parse_iteration_from_filename("Reviewer_b2_Gemini_iter4.md"),
            Some(4)
        );
        // Unnamed block fallback format
        assert_eq!(
            parse_iteration_from_filename("block2_gemini_iter4.md"),
            Some(4)
        );
    }

    #[test]
    fn find_last_iteration_includes_block_files() {
        let dir = tempdir().unwrap();
        write_agent_iter(dir.path(), "anthropic", 1);
        fs::write(dir.path().join("Analyzer_b1_Claude_iter3.md"), "test").unwrap();
        fs::write(dir.path().join("Reviewer_b2_Gemini_iter3.md"), "test").unwrap();
        assert_eq!(find_last_iteration(dir.path(), &[]), Some(3));
    }

    #[test]
    fn find_last_iteration_multiple_files() {
        let dir = tempdir().unwrap();
        write_agent_iter(dir.path(), "Claude", 1);
        write_agent_iter(dir.path(), "Codex", 3);
        write_agent_iter(dir.path(), "Gemini", 2);

        let keys: Vec<String> = ["Claude", "Codex", "Gemini"]
            .iter()
            .map(|n| App::agent_file_key(n))
            .collect();
        assert_eq!(find_last_iteration(dir.path(), &keys), Some(3));
    }

    #[test]
    fn find_last_iteration_ignores_unknown_files() {
        let dir = tempdir().unwrap();
        write_agent_iter(dir.path(), "Claude", 2);
        // Stray file that looks like an iteration but isn't a known agent
        std::fs::write(dir.path().join("notes_iter42.md"), "stray").unwrap();

        let keys = vec![App::agent_file_key("Claude")];
        assert_eq!(find_last_iteration(dir.path(), &keys), Some(2));
    }

    #[test]
    fn run_dir_matches_exact() {
        let dir = tempdir().unwrap();
        write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

        assert!(run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()]
        ));
    }

    #[test]
    fn run_dir_matches_wrong_mode() {
        let dir = tempdir().unwrap();
        write_session_toml(dir.path(), "swarm", &["anthropic", "openai"]);

        assert!(!run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()]
        ));
    }

    #[test]
    fn run_dir_matches_subset_agents() {
        let dir = tempdir().unwrap();
        write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

        assert!(run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string()]
        ));
    }

    #[test]
    fn run_dir_matches_missing_agent() {
        let dir = tempdir().unwrap();
        write_session_toml(dir.path(), "relay", &["anthropic"]);

        assert!(!run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "gemini".to_string()]
        ));
    }

    #[test]
    fn run_dir_matches_no_session_toml() {
        let dir = tempdir().unwrap();
        assert!(!run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string()]
        ));
    }

    #[test]
    fn run_dir_matches_mode_and_agents_missing_mode_field() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("session.toml"),
            "agents = [\"anthropic\", \"openai\"]\n",
        )
        .unwrap();

        assert!(!run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()]
        ));
    }

    #[test]
    fn run_dir_matches_mode_and_agents_missing_agents_field() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("session.toml"), "mode = \"relay\"\n").unwrap();

        assert!(!run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string(), "openai".to_string()]
        ));
    }

    #[test]
    fn find_latest_compatible_run_ordering() {
        let dir = tempdir().unwrap();

        let run1 = dir.path().join("20260101_000000");
        fs::create_dir_all(&run1).unwrap();
        write_session_toml(&run1, "relay", &["anthropic", "openai"]);
        write_agent_iter(&run1, "anthropic", 1);
        write_agent_iter(&run1, "openai", 1);

        let run2 = dir.path().join("20260201_000000");
        fs::create_dir_all(&run2).unwrap();
        write_session_toml(&run2, "relay", &["anthropic", "openai"]);
        write_agent_iter(&run2, "anthropic", 1);
        write_agent_iter(&run2, "openai", 1);

        let run3 = dir.path().join("20260301_000000");
        fs::create_dir_all(&run3).unwrap();
        write_session_toml(&run3, "swarm", &["anthropic", "openai"]);
        write_agent_iter(&run3, "anthropic", 1);
        write_agent_iter(&run3, "openai", 1);

        assert_eq!(
            find_latest_compatible_run(
                dir.path(),
                ExecutionMode::Relay,
                &["anthropic".to_string(), "openai".to_string()]
            ),
            Some(run2)
        );
    }

    #[test]
    fn find_latest_compatible_run_none() {
        let dir = tempdir().unwrap();
        let run = dir.path().join("20260101_000000");
        fs::create_dir_all(&run).unwrap();
        write_session_toml(&run, "relay", &["anthropic", "openai"]);
        write_agent_iter(&run, "anthropic", 1);

        assert_eq!(
            find_latest_compatible_run(
                dir.path(),
                ExecutionMode::Relay,
                &["anthropic".to_string(), "openai".to_string()]
            ),
            None
        );
    }

    #[test]
    fn find_latest_compatible_run_ignores_invalid_session_toml() {
        let dir = tempdir().unwrap();

        let good = dir.path().join("20260101_000000");
        fs::create_dir_all(&good).unwrap();
        write_session_toml(&good, "relay", &["anthropic", "openai"]);
        write_agent_iter(&good, "anthropic", 1);
        write_agent_iter(&good, "openai", 1);

        let bad = dir.path().join("20260201_000000");
        fs::create_dir_all(&bad).unwrap();
        fs::write(bad.join("session.toml"), "mode = ").unwrap();
        write_agent_iter(&bad, "anthropic", 1);
        write_agent_iter(&bad, "openai", 1);

        assert_eq!(
            find_latest_compatible_run(
                dir.path(),
                ExecutionMode::Relay,
                &["anthropic".to_string(), "openai".to_string()]
            ),
            Some(good)
        );
    }

    #[test]
    fn find_last_complete_iteration_for_agents_returns_last_complete() {
        let dir = tempdir().unwrap();
        write_agent_iter(dir.path(), "anthropic", 1);
        write_agent_iter(dir.path(), "openai", 1);
        write_agent_iter(dir.path(), "anthropic", 2);

        assert_eq!(
            find_last_complete_iteration_for_agents(
                dir.path(),
                &["anthropic".to_string(), "openai".to_string()]
            ),
            Some(1)
        );
    }

    #[test]
    fn find_last_complete_iteration_for_agents_none_complete() {
        let dir = tempdir().unwrap();
        write_agent_iter(dir.path(), "anthropic", 1);
        write_agent_iter(dir.path(), "openai", 2);

        assert_eq!(
            find_last_complete_iteration_for_agents(
                dir.path(),
                &["anthropic".to_string(), "openai".to_string()]
            ),
            None
        );
    }

    #[test]
    fn collect_report_files_excludes_prompt_and_errors() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("prompt.md"), "prompt").unwrap();
        fs::write(dir.path().join("errors.md"), "errors").unwrap();
        fs::write(dir.path().join("anthropic_iter1.md"), "report").unwrap();
        fs::write(dir.path().join("session.toml"), "mode = \"relay\"").unwrap();

        let files = collect_report_files(dir.path());
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].file_name().and_then(|n| n.to_str()),
            Some("anthropic_iter1.md")
        );
    }

    #[test]
    fn collect_report_files_sorted() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("b.md"), "b").unwrap();
        fs::write(dir.path().join("a.md"), "a").unwrap();
        fs::write(dir.path().join("c.md"), "c").unwrap();

        let files = collect_report_files(dir.path());
        let names: Vec<String> = files
            .iter()
            .map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        assert_eq!(names, vec!["a.md", "b.md", "c.md"]);
    }

    #[test]
    fn collect_report_files_empty_dir() {
        let dir = tempdir().unwrap();
        assert!(collect_report_files(dir.path()).is_empty());
    }

    #[test]
    fn collect_application_errors_deduplicates() {
        let dir = tempdir().unwrap();
        let mut app = test_app();
        app.progress_events.push(ProgressEvent::AgentError {
            agent: "Claude".to_string(),
            kind: ProviderKind::Anthropic,
            iteration: 1,
            error: "boom".to_string(),
            details: None,
        });
        app.progress_events.push(ProgressEvent::AgentError {
            agent: "Claude".to_string(),
            kind: ProviderKind::Anthropic,
            iteration: 1,
            error: "boom".to_string(),
            details: None,
        });
        fs::write(dir.path().join("_errors.log"), "[Claude iter 1] boom\n").unwrap();

        let errors = collect_application_errors(&app, dir.path());
        assert_eq!(errors, vec!["[Claude iter 1] boom".to_string()]);
    }

    #[test]
    fn collect_application_errors_merges_log_file() {
        let dir = tempdir().unwrap();
        let mut app = test_app();
        app.progress_events.push(ProgressEvent::AgentError {
            agent: "Codex".to_string(),
            kind: ProviderKind::OpenAI,
            iteration: 2,
            error: "api failed".to_string(),
            details: Some("rate limited".to_string()),
        });
        fs::write(dir.path().join("_errors.log"), "tool timeout\n\n").unwrap();

        let errors = collect_application_errors(&app, dir.path());
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().any(|e| e == "[Codex iter 2] rate limited"));
        assert!(errors.iter().any(|e| e == "tool timeout"));
    }

    #[test]
    fn collect_application_errors_empty_sources() {
        let dir = tempdir().unwrap();
        let app = test_app();
        assert!(collect_application_errors(&app, dir.path()).is_empty());
    }

    #[test]
    fn build_diagnostic_prompt_cli_mode() {
        let dir = tempdir().unwrap();
        let report = dir.path().join("anthropic_iter1.md");
        fs::write(&report, "hidden content").unwrap();

        let prompt = build_diagnostic_prompt(&[report], &[], true);
        assert!(prompt.contains("Read each listed file from disk"));
        assert!(!prompt.contains("hidden content"));
    }

    #[test]
    fn build_diagnostic_prompt_api_mode() {
        let dir = tempdir().unwrap();
        let report = dir.path().join("anthropic_iter1.md");
        fs::write(&report, "file body").unwrap();

        let prompt = build_diagnostic_prompt(&[report], &[], false);
        assert!(prompt.contains("=== BEGIN anthropic_iter1.md ==="));
        assert!(prompt.contains("file body"));
        assert!(prompt.contains("=== END anthropic_iter1.md ==="));
    }

    #[test]
    fn build_diagnostic_prompt_no_report_files() {
        let prompt = build_diagnostic_prompt(&[], &[], false);
        assert!(prompt.contains("Reports to analyze:\n- none"));
        assert!(!prompt.contains("Report contents:"));
    }

    #[test]
    fn build_diagnostic_prompt_unreadable_file_in_api_mode() {
        let missing = PathBuf::from("/tmp/does-not-exist-for-houseofagents-tests.md");
        let prompt = build_diagnostic_prompt(&[missing], &[], false);
        assert!(prompt.contains("Failed to read file:"));
    }

    #[test]
    fn prev_char_boundary_ascii() {
        assert_eq!(prev_char_boundary("hello", 3), 2);
    }

    #[test]
    fn prev_char_boundary_multibyte() {
        assert_eq!(prev_char_boundary("héllo", 3), 1);
    }

    #[test]
    fn prev_char_boundary_at_zero() {
        assert_eq!(prev_char_boundary("hello", 0), 0);
    }

    #[test]
    fn next_char_boundary_ascii() {
        assert_eq!(next_char_boundary("hello", 2), 3);
    }

    #[test]
    fn next_char_boundary_at_end() {
        assert_eq!(next_char_boundary("hello", 5), 5);
    }

    #[test]
    fn next_char_boundary_multibyte() {
        assert_eq!(next_char_boundary("café", 3), 5);
    }

    #[test]
    fn word_move_left_skips_whitespace_then_word() {
        let mut app = test_app();
        app.prompt_text = "hello world".to_string();
        app.prompt_cursor = app.prompt_text.len();

        move_cursor_word_left(&app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_cursor, 6);
    }

    #[test]
    fn move_cursor_word_right_skips_whitespace_and_word() {
        let mut app = test_app();
        app.prompt_text = "hello   world  next".to_string();
        app.prompt_cursor = 5;

        move_cursor_word_right(&app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_cursor, 13);
    }

    #[test]
    fn move_cursor_line_up_and_down_preserves_column() {
        let mut app = test_app();
        app.prompt_text = "abc\ndefg\nhi".to_string();
        let second_line_start = "abc\n".len();
        app.prompt_cursor = second_line_start + 2; // after 'e'

        move_cursor_line_up(&app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_cursor, 2);

        move_cursor_line_down(&app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_cursor, second_line_start + 2);
    }

    #[test]
    fn move_cursor_line_up_and_down_clamp_to_line_length() {
        let mut app = test_app();
        app.prompt_text = "a\nlonger".to_string();
        app.prompt_cursor = app.prompt_text.len();

        move_cursor_line_up(&app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_cursor, 1);

        move_cursor_line_down(&app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_cursor, 3);
    }

    #[test]
    fn move_cursor_line_up_down_noop_at_edges() {
        let mut app = test_app();
        app.prompt_text = "top\nbottom".to_string();

        app.prompt_cursor = 1;
        move_cursor_line_up(&app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_cursor, 1);

        app.prompt_cursor = app.prompt_text.len();
        move_cursor_line_down(&app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_cursor, app.prompt_text.len());
    }

    #[test]
    fn delete_char_left_multibyte() {
        let mut app = test_app();
        app.prompt_text = "aé".to_string();
        app.prompt_cursor = app.prompt_text.len();

        delete_char_left(&mut app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_text, "a");
        assert_eq!(app.prompt_cursor, 1);
    }

    #[test]
    fn delete_word_left_multibyte_boundary_safe() {
        let mut app = test_app();
        app.prompt_text = "hello café".to_string();
        app.prompt_cursor = app.prompt_text.len();

        delete_word_left(&mut app.prompt_text, &mut app.prompt_cursor);
        assert_eq!(app.prompt_text, "hello ");
        assert_eq!(app.prompt_cursor, 6);
    }

    #[test]
    fn sync_iterations_buf_empty() {
        let mut app = test_app();
        app.iterations_buf.clear();
        sync_iterations_buf(&mut app);
        assert_eq!(app.iterations, 1);
        assert_eq!(app.iterations_buf, "1");
    }

    #[test]
    fn sync_iterations_buf_valid() {
        let mut app = test_app();
        app.iterations_buf = "5".to_string();
        sync_iterations_buf(&mut app);
        assert_eq!(app.iterations, 5);
        assert_eq!(app.iterations_buf, "5");
    }

    #[test]
    fn sync_iterations_buf_over_99() {
        let mut app = test_app();
        app.iterations_buf = "150".to_string();
        sync_iterations_buf(&mut app);
        assert_eq!(app.iterations, 99);
        assert_eq!(app.iterations_buf, "99");
    }

    #[test]
    fn sync_iterations_buf_zero() {
        let mut app = test_app();
        app.iterations_buf = "0".to_string();
        sync_iterations_buf(&mut app);
        assert_eq!(app.iterations, 1);
        assert_eq!(app.iterations_buf, "1");
    }

    #[test]
    fn sync_iterations_buf_non_numeric() {
        let mut app = test_app();
        app.iterations_buf = "abc".to_string();
        sync_iterations_buf(&mut app);
        assert_eq!(app.iterations, 1);
        assert_eq!(app.iterations_buf, "1");
    }

    #[test]
    fn prompt_focus_cycle_solo_tab_and_backtab() {
        let mut app = test_app();
        app.selected_mode = ExecutionMode::Solo;
        app.prompt_focus = PromptFocus::Text;

        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::SessionName);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Text);

        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::SessionName);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Text);
    }

    #[test]
    fn prompt_focus_cycle_swarm_tab_and_backtab() {
        let mut app = test_app();
        app.selected_mode = ExecutionMode::Swarm;
        app.prompt_focus = PromptFocus::Text;

        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::SessionName);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Iterations);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Resume);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Text);

        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Resume);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Iterations);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::SessionName);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Text);
    }

    #[test]
    fn prompt_focus_cycle_relay_tab_and_backtab() {
        let mut app = test_app();
        app.selected_mode = ExecutionMode::Relay;
        app.prompt_focus = PromptFocus::Text;

        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::SessionName);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Iterations);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Resume);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::ForwardPrompt);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Text);

        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::ForwardPrompt);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Resume);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Iterations);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::SessionName);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Text);
    }

    #[test]
    fn handle_paste_inserts_prompt_text_and_normalizes_newlines() {
        let mut app = test_app();
        app.screen = Screen::Prompt;
        app.prompt_focus = PromptFocus::Text;
        app.prompt_text = "ab".to_string();
        app.prompt_cursor = 1;

        handle_paste(&mut app, "X\r\nY\rZ");
        assert_eq!(app.prompt_text, "aX\nY\nZb");
        assert_eq!(app.prompt_cursor, "aX\nY\nZ".len());
    }

    #[test]
    fn handle_paste_ignored_outside_prompt_text() {
        let mut app = test_app();
        app.screen = Screen::Prompt;
        app.prompt_focus = PromptFocus::SessionName;
        app.prompt_text = "base".to_string();
        app.prompt_cursor = 2;

        handle_paste(&mut app, "ZZZ");
        assert_eq!(app.prompt_text, "base");
        assert_eq!(app.prompt_cursor, 2);
    }

    #[test]
    fn pipeline_move_selected_block_moves_into_empty_cell() {
        let mut app = test_app();
        app.pipeline_def.blocks = vec![pipeline_mod::PipelineBlock {
            id: 1,
            name: "one".into(),
            agent: "agent".into(),
            prompt: String::new(),
            session_id: None,
            position: (2, 2),
        }];
        app.pipeline_block_cursor = Some(1);

        pipeline_move_selected_block(&mut app, 1, 0);

        assert_eq!(app.pipeline_def.blocks[0].position, (3, 2));
        assert_eq!(app.pipeline_block_cursor, Some(1));
    }

    #[test]
    fn pipeline_move_selected_block_swaps_when_target_occupied() {
        let mut app = test_app();
        app.pipeline_def.blocks = vec![
            pipeline_mod::PipelineBlock {
                id: 1,
                name: "one".into(),
                agent: "agent".into(),
                prompt: String::new(),
                session_id: None,
                position: (2, 2),
            },
            pipeline_mod::PipelineBlock {
                id: 2,
                name: "two".into(),
                agent: "agent".into(),
                prompt: String::new(),
                session_id: None,
                position: (3, 2),
            },
        ];
        app.pipeline_block_cursor = Some(1);

        pipeline_move_selected_block(&mut app, 1, 0);

        let b1 = app.pipeline_def.blocks.iter().find(|b| b.id == 1).unwrap();
        let b2 = app.pipeline_def.blocks.iter().find(|b| b.id == 2).unwrap();
        assert_eq!(b1.position, (3, 2));
        assert_eq!(b2.position, (2, 2));
    }

    #[test]
    fn pipeline_builder_arrow_navigates_shift_arrow_moves_block() {
        let mut app = test_app();
        app.pipeline_def.blocks = vec![
            pipeline_mod::PipelineBlock {
                id: 1,
                name: "one".into(),
                agent: "agent".into(),
                prompt: String::new(),
                session_id: None,
                position: (2, 2),
            },
            pipeline_mod::PipelineBlock {
                id: 2,
                name: "two".into(),
                agent: "agent".into(),
                prompt: String::new(),
                session_id: None,
                position: (3, 2),
            },
        ];
        app.pipeline_block_cursor = Some(1);

        // Unmodified arrow navigates to the nearest block (does not move block 1)
        handle_pipeline_builder_key(&mut app, KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
        let after_nav = app.pipeline_def.blocks.iter().find(|b| b.id == 1).unwrap();
        assert_eq!(after_nav.position, (2, 2), "unmodified arrow should not move the block");
        assert_eq!(app.pipeline_block_cursor, Some(2), "cursor should navigate to block 2");

        // Shift+arrow moves the selected block
        handle_pipeline_builder_key(&mut app, KeyEvent::new(KeyCode::Right, KeyModifiers::SHIFT));
        let moved = app.pipeline_def.blocks.iter().find(|b| b.id == 2).unwrap();
        assert_eq!(moved.position, (4, 2), "Shift+arrow should move the block");
        assert_eq!(app.pipeline_block_cursor, Some(2), "cursor stays on the moved block");
    }
}
