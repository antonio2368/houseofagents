use crate::app::{
    App, BatchRunGroup, ConsolidationPhase, ConsolidationTarget, EditField, EditPopupSection,
    HomeSection, PipelineDialogMode, PipelineEditField, PipelineFocus, PromptFocus, RunState,
    RunStatus, RunStepStatus, Screen,
};
use crate::config::{AgentConfig, AppConfig, ProviderConfig};
use crate::event::{Event, EventHandler};
use crate::execution::multi::run_multi;
use crate::execution::pipeline::{self as pipeline_mod, BlockId};
use crate::execution::relay::run_relay;
use crate::execution::solo::run_solo;
use crate::execution::swarm::run_swarm;
use crate::execution::{
    BatchProgressEvent, ExecutionMode, ProgressEvent, PromptRuntimeContext, RunOutcome,
};
use crate::output::{AgentSessionInfo, OutputManager};
use crate::provider::{self, ProviderKind};

use crossterm::event::{
    DisableBracketedPaste, EnableBracketedPaste, KeyCode, KeyEvent, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::collections::HashMap;
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
            Some(progress) = async {
                if let Some(ref mut rx) = app.batch_progress_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<BatchProgressEvent>>().await
                }
            } => {
                handle_batch_progress(app, progress);
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
                app.pipeline_runs_buf = app.pipeline_runs.to_string();
                app.pipeline_concurrency_buf = app.pipeline_concurrency.to_string();
            } else if app.selected_agents.is_empty() {
                app.error_modal = Some("Select at least one agent".into());
            } else {
                app.screen = Screen::Prompt;
                app.prompt_focus = PromptFocus::Text;
                app.prompt_cursor = app.prompt_text.len();
                app.iterations_buf = app.iterations.to_string();
                app.runs_buf = app.runs.to_string();
                app.concurrency_buf = app.concurrency.to_string();
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
            sync_runs_buf(app);
            sync_concurrency_buf(app);
            app.screen = Screen::Home;
        }
        KeyCode::Tab => {
            app.prompt_focus = match (&app.prompt_focus, app.selected_mode) {
                (PromptFocus::Text, _) => PromptFocus::SessionName,
                (PromptFocus::SessionName, _) => {
                    app.iterations_buf = app.iterations.to_string();
                    PromptFocus::Iterations
                }
                (PromptFocus::Iterations, _) => {
                    sync_iterations_buf(app);
                    app.runs_buf = app.runs.to_string();
                    PromptFocus::Runs
                }
                (PromptFocus::Runs, _) => {
                    sync_runs_buf(app);
                    app.concurrency_buf = app.concurrency.to_string();
                    PromptFocus::Concurrency
                }
                (PromptFocus::Concurrency, ExecutionMode::Solo) => PromptFocus::Text,
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
            app.prompt_focus = match (&app.prompt_focus, app.selected_mode) {
                (PromptFocus::Text, ExecutionMode::Solo) => PromptFocus::Concurrency,
                (PromptFocus::Text, ExecutionMode::Relay) => PromptFocus::ForwardPrompt,
                (PromptFocus::Text, _) => PromptFocus::Resume,
                (PromptFocus::SessionName, _) => PromptFocus::Text,
                (PromptFocus::Iterations, _) => {
                    sync_iterations_buf(app);
                    PromptFocus::SessionName
                }
                (PromptFocus::Runs, _) => {
                    sync_runs_buf(app);
                    app.iterations_buf = app.iterations.to_string();
                    PromptFocus::Iterations
                }
                (PromptFocus::Concurrency, _) => {
                    sync_concurrency_buf(app);
                    app.runs_buf = app.runs.to_string();
                    PromptFocus::Runs
                }
                (PromptFocus::Resume, _) => {
                    app.concurrency_buf = app.concurrency.to_string();
                    PromptFocus::Concurrency
                }
                (PromptFocus::ForwardPrompt, _) => PromptFocus::Resume,
            };
        }
        KeyCode::Char(' ') if app.prompt_focus == PromptFocus::Resume => {
            if !resume_allowed_in_prompt(app) {
                app.resume_previous = false;
                app.error_modal = Some("Resume is only supported for single-run execution".into());
            } else {
                app.resume_previous = !app.resume_previous;
            }
        }
        KeyCode::Char(' ') if app.prompt_focus == PromptFocus::ForwardPrompt => {
            app.forward_prompt = !app.forward_prompt;
        }
        KeyCode::F(5) | KeyCode::Enter
            if key.code == KeyCode::F(5) || app.prompt_focus != PromptFocus::Text =>
        {
            sync_iterations_buf(app);
            sync_runs_buf(app);
            sync_concurrency_buf(app);
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
            PromptFocus::Runs => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.runs_buf.push(c);
                    sync_runs_buf(app);
                    enforce_prompt_resume_constraints(app);
                }
                KeyCode::Backspace => {
                    app.runs_buf.pop();
                    if app.runs_buf.is_empty() {
                        app.runs = 1;
                    } else {
                        sync_runs_buf(app);
                    }
                    enforce_prompt_resume_constraints(app);
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.runs = (app.runs + 1).min(99);
                    app.runs_buf = app.runs.to_string();
                    enforce_prompt_resume_constraints(app);
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.runs = app.runs.saturating_sub(1).max(1);
                    app.runs_buf = app.runs.to_string();
                    enforce_prompt_resume_constraints(app);
                }
                _ => {}
            },
            PromptFocus::Concurrency => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.concurrency_buf.push(c);
                    sync_concurrency_buf(app);
                }
                KeyCode::Backspace => {
                    app.concurrency_buf.pop();
                    if app.concurrency_buf.is_empty() {
                        app.concurrency = 0;
                    } else {
                        sync_concurrency_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.concurrency = (app.concurrency + 1).min(99);
                    app.concurrency_buf = app.concurrency.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.concurrency = app.concurrency.saturating_sub(1);
                    app.concurrency_buf = app.concurrency.to_string();
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
                    let v: u32 = app
                        .pipeline_iterations_buf
                        .parse()
                        .unwrap_or(1)
                        .clamp(1, 99);
                    app.pipeline_iterations_buf = v.to_string();
                }
            }
            PipelineFocus::Runs => {
                for ch in text.chars() {
                    if ch.is_ascii_digit() {
                        app.pipeline_runs_buf.push(ch);
                    }
                }
                sync_pipeline_runs_buf(app);
            }
            PipelineFocus::Concurrency => {
                for ch in text.chars() {
                    if ch.is_ascii_digit() {
                        app.pipeline_concurrency_buf.push(ch);
                    }
                }
                sync_pipeline_concurrency_buf(app);
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
                        app.pipeline_def
                            .connections
                            .push(pipeline_mod::PipelineConnection { from, to });
                        app.pipeline_connecting_from = None;
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
            app.pipeline_focus = match app.pipeline_focus {
                PipelineFocus::InitialPrompt => PipelineFocus::SessionName,
                PipelineFocus::SessionName => PipelineFocus::Iterations,
                PipelineFocus::Iterations => PipelineFocus::Runs,
                PipelineFocus::Runs => PipelineFocus::Concurrency,
                PipelineFocus::Concurrency => PipelineFocus::Builder,
                PipelineFocus::Builder => PipelineFocus::InitialPrompt,
            };
        }
        KeyCode::BackTab => {
            app.pipeline_focus = match app.pipeline_focus {
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
                .filter_map(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|s| s.to_string())
                })
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
                KeyCode::Backspace => {
                    app.pipeline_session_name.pop();
                }
                _ => {}
            },
            PipelineFocus::Iterations => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline_iterations_buf.push(c);
                    sync_pipeline_iterations_buf(app);
                }
                KeyCode::Backspace => {
                    app.pipeline_iterations_buf.pop();
                    if app.pipeline_iterations_buf.is_empty() {
                        app.pipeline_def.iterations = 1;
                    } else {
                        sync_pipeline_iterations_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.pipeline_def.iterations = (app.pipeline_def.iterations + 1).min(99);
                    app.pipeline_iterations_buf = app.pipeline_def.iterations.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.pipeline_def.iterations =
                        app.pipeline_def.iterations.saturating_sub(1).max(1);
                    app.pipeline_iterations_buf = app.pipeline_def.iterations.to_string();
                }
                _ => {}
            },
            PipelineFocus::Runs => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline_runs_buf.push(c);
                    sync_pipeline_runs_buf(app);
                }
                KeyCode::Backspace => {
                    app.pipeline_runs_buf.pop();
                    if app.pipeline_runs_buf.is_empty() {
                        app.pipeline_runs = 1;
                    } else {
                        sync_pipeline_runs_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.pipeline_runs = (app.pipeline_runs + 1).min(99);
                    app.pipeline_runs_buf = app.pipeline_runs.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.pipeline_runs = app.pipeline_runs.saturating_sub(1).max(1);
                    app.pipeline_runs_buf = app.pipeline_runs.to_string();
                }
                _ => {}
            },
            PipelineFocus::Concurrency => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    app.pipeline_concurrency_buf.push(c);
                    sync_pipeline_concurrency_buf(app);
                }
                KeyCode::Backspace => {
                    app.pipeline_concurrency_buf.pop();
                    if app.pipeline_concurrency_buf.is_empty() {
                        app.pipeline_concurrency = 0;
                    } else {
                        sync_pipeline_concurrency_buf(app);
                    }
                }
                KeyCode::Up | KeyCode::Char('+') => {
                    app.pipeline_concurrency = (app.pipeline_concurrency + 1).min(99);
                    app.pipeline_concurrency_buf = app.pipeline_concurrency.to_string();
                }
                KeyCode::Down | KeyCode::Char('-') => {
                    app.pipeline_concurrency = app.pipeline_concurrency.saturating_sub(1);
                    app.pipeline_concurrency_buf = app.pipeline_concurrency.to_string();
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
            let default_agent = app
                .config
                .agents
                .first()
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
                    app.pipeline_edit_agent_idx = app
                        .config
                        .agents
                        .iter()
                        .position(|a| a.name == block.agent)
                        .unwrap_or(0);
                    app.pipeline_edit_prompt_buf = block.prompt.clone();
                    app.pipeline_edit_prompt_cursor = block.prompt.len();
                    app.pipeline_edit_session_buf = block.session_id.clone().unwrap_or_default();
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
    let visible_w = (term_w.saturating_sub(4)) as i16; // border + padding
    let visible_h = (term_h.saturating_sub(13)) as i16; // title+prompt+help+borders

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
                            block.agent = app
                                .config
                                .agents
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
                    app.pipeline_edit_agent_idx = (app.pipeline_edit_agent_idx + len - 1) % len;
                }
                KeyCode::Right | KeyCode::Char('j') => {
                    let len = app.config.agents.len().max(1);
                    app.pipeline_edit_agent_idx = (app.pipeline_edit_agent_idx + 1) % len;
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
                            let filename = format!("{}.toml", app.pipeline_file_input.trim());
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
                            app.error_modal = Some(format!("Cannot create pipelines dir: {e}"));
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
    sync_pipeline_iterations_buf(app);
    sync_pipeline_runs_buf(app);
    sync_pipeline_concurrency_buf(app);

    // Validate
    if app.pipeline_def.blocks.is_empty() {
        app.error_modal = Some("Add at least one block before running".into());
        return;
    }
    if app.pipeline_def.initial_prompt.trim().is_empty() {
        app.error_modal = Some("Enter an initial prompt".into());
        return;
    }
    let iterations: u32 = app.pipeline_iterations_buf.parse().unwrap_or(0);
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
    let avail_agents: std::collections::HashMap<String, bool> = app
        .available_agents()
        .into_iter()
        .map(|(a, avail)| (a.name.clone(), avail))
        .collect();
    for block in &app.pipeline_def.blocks {
        match avail_agents.get(&block.agent) {
            Some(true) => {}
            Some(false) => {
                app.error_modal = Some(format!(
                    "{} is not available (block {})",
                    block.agent, block.id
                ));
                return;
            }
            None => {
                app.error_modal = Some(format!(
                    "Agent '{}' not found (block {})",
                    block.agent, block.id
                ));
                return;
            }
        }
    }

    // Set running state
    reset_running_state(app);

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
            let agent_cfg = app
                .effective_agent_config(&agent_cfg.name)
                .unwrap_or(agent_cfg);
            agent_configs.insert(
                block.agent.clone(),
                (
                    agent_cfg.provider,
                    agent_cfg.to_provider_config(),
                    agent_cfg.use_cli,
                ),
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

    let runs = app.pipeline_runs.max(1);
    let concurrency = effective_concurrency(runs, app.pipeline_concurrency);

    if runs > 1 {
        start_multi_pipeline_execution(app, client, iterations, runs, concurrency, agent_configs);
        return;
    }

    // Output — use session name (not pipeline filename) for output dir
    let session_name = if app.pipeline_session_name.trim().is_empty() {
        None
    } else {
        Some(app.pipeline_session_name.trim())
    };
    let base_path = app.config.resolved_output_dir();
    let output = match OutputManager::new(&base_path, session_name) {
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
        KeyCode::Up | KeyCode::Char('k') if app.multi_run_total > 1 => {
            app.multi_run_cursor = app.multi_run_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') if app.multi_run_total > 1 => {
            if app.multi_run_cursor + 1 < app.multi_run_states.len() {
                app.multi_run_cursor += 1;
            }
        }
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
                app.consolidation_target = if app.multi_run_total > 1 {
                    ConsolidationTarget::PerRun
                } else {
                    ConsolidationTarget::Single
                };
                app.consolidation_phase = if app.config.agents.len() <= 1 {
                    ConsolidationPhase::Prompt
                } else {
                    ConsolidationPhase::Provider
                };
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                if app.multi_run_total > 1 {
                    app.consolidation_target = ConsolidationTarget::AcrossRuns;
                    app.consolidation_phase = ConsolidationPhase::CrossRunConfirm;
                    app.consolidation_prompt.clear();
                } else {
                    app.consolidation_active = false;
                    maybe_start_diagnostics(app);
                }
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
                app.consolidation_phase =
                    if app.consolidation_target == ConsolidationTarget::AcrossRuns {
                        ConsolidationPhase::CrossRunPrompt
                    } else {
                        ConsolidationPhase::Prompt
                    };
            }
            KeyCode::Esc => {
                app.consolidation_phase =
                    if app.consolidation_target == ConsolidationTarget::AcrossRuns {
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
        ConsolidationPhase::CrossRunConfirm => match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                app.consolidation_target = ConsolidationTarget::AcrossRuns;
                app.consolidation_phase = if app.config.agents.len() <= 1 {
                    ConsolidationPhase::CrossRunPrompt
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
        ConsolidationPhase::CrossRunPrompt => match key.code {
            KeyCode::Enter => start_consolidation(app),
            KeyCode::Backspace => {
                app.consolidation_prompt.pop();
            }
            KeyCode::Char(c) => app.consolidation_prompt.push(c),
            KeyCode::Esc => {
                app.consolidation_phase = if app.config.agents.len() <= 1 {
                    ConsolidationPhase::CrossRunConfirm
                } else {
                    ConsolidationPhase::Provider
                };
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
            if has_batch_result_tree(app) {
                let max = batch_result_visible_len(app).saturating_sub(1);
                if app.result_cursor < max {
                    app.result_cursor += 1;
                }
            } else if app.result_cursor < app.result_files.len().saturating_sub(1) {
                app.result_cursor += 1;
            }
            update_preview(app);
        }
        KeyCode::Enter | KeyCode::Char('l') if has_batch_result_tree(app) => {
            if let Some(BatchResultEntry::RunHeader(run_id)) =
                batch_result_entry_at(app, app.result_cursor)
            {
                if !app.batch_result_expanded.insert(run_id) {
                    app.batch_result_expanded.remove(&run_id);
                }
                update_preview(app);
            } else {
                reset_to_home(app);
            }
        }
        KeyCode::Esc => reset_to_home(app),
        KeyCode::Enter => reset_to_home(app),
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
    app.runs = 1;
    app.runs_buf = "1".into();
    app.concurrency = 0;
    app.concurrency_buf = "0".into();
    app.resume_previous = false;
    app.forward_prompt = false;
    app.consolidation_active = false;
    app.consolidation_phase = ConsolidationPhase::Confirm;
    app.consolidation_target = ConsolidationTarget::Single;
    app.consolidation_provider_cursor = 0;
    app.consolidation_prompt.clear();
    app.consolidation_running = false;
    app.consolidation_rx = None;
    app.diagnostic_running = false;
    app.diagnostic_rx = None;
    app.batch_stage1_done = false;
    app.batch_progress_rx = None;
    app.multi_run_total = 0;
    app.multi_run_concurrency = 0;
    app.multi_run_cursor = 0;
    app.multi_run_states.clear();
    app.multi_run_step_labels.clear();
    app.batch_result_runs.clear();
    app.batch_result_root_files.clear();
    app.batch_result_expanded.clear();
    app.progress_rx = None;
    app.run_dir = None;
    app.cancel_flag = Arc::new(AtomicBool::new(false));

    // Pipeline state
    app.pipeline_def = pipeline_mod::PipelineDefinition::default();
    app.pipeline_next_id = 1;
    app.pipeline_block_cursor = None;
    app.pipeline_focus = PipelineFocus::InitialPrompt;
    app.pipeline_canvas_offset = (0, 0);
    app.pipeline_prompt_cursor = 0;
    app.pipeline_session_name.clear();
    app.pipeline_iterations_buf = "1".into();
    app.pipeline_runs = 1;
    app.pipeline_runs_buf = "1".into();
    app.pipeline_concurrency = 0;
    app.pipeline_concurrency_buf = "0".into();
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
                let mut config =
                    effective_section_config(app).unwrap_or_else(empty_provider_config);
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
                let mut config =
                    effective_section_config(app).unwrap_or_else(empty_provider_config);
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
    let mut config = effective_section_config(app).unwrap_or_else(empty_provider_config);

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
    app.edit_popup_cursor = app
        .edit_popup_cursor
        .min(app.config.agents.len().saturating_sub(1));
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
    app.config
        .agents
        .get(app.edit_popup_cursor)
        .map(|a| a.provider)
}

/// Returns a ProviderConfig view for the current edit selection (Providers section).
fn effective_section_config(app: &App) -> Option<ProviderConfig> {
    let name = app
        .config
        .agents
        .get(app.edit_popup_cursor)
        .map(|a| a.name.as_str())?;
    app.effective_agent_config(name)
        .map(|a| a.to_provider_config())
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

fn resume_allowed_in_prompt(app: &App) -> bool {
    !(app.runs > 1
        && matches!(
            app.selected_mode,
            ExecutionMode::Relay | ExecutionMode::Swarm
        ))
}

fn enforce_prompt_resume_constraints(app: &mut App) {
    if !resume_allowed_in_prompt(app) {
        app.resume_previous = false;
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

fn sync_runs_buf(app: &mut App) {
    if app.runs_buf.is_empty() {
        app.runs = 1;
    } else {
        app.runs = app.runs_buf.parse().unwrap_or(1).clamp(1, 99);
    }
    app.runs_buf = app.runs.to_string();
}

fn sync_concurrency_buf(app: &mut App) {
    if app.concurrency_buf.is_empty() {
        app.concurrency = 0;
    } else {
        app.concurrency = app.concurrency_buf.parse().unwrap_or(0).min(99);
    }
    app.concurrency_buf = app.concurrency.to_string();
}

fn sync_pipeline_iterations_buf(app: &mut App) {
    if app.pipeline_iterations_buf.is_empty() {
        app.pipeline_def.iterations = 1;
    } else {
        app.pipeline_def.iterations = app
            .pipeline_iterations_buf
            .parse()
            .unwrap_or(1)
            .clamp(1, 99);
    }
    app.pipeline_iterations_buf = app.pipeline_def.iterations.to_string();
}

fn sync_pipeline_runs_buf(app: &mut App) {
    if app.pipeline_runs_buf.is_empty() {
        app.pipeline_runs = 1;
    } else {
        app.pipeline_runs = app.pipeline_runs_buf.parse().unwrap_or(1).clamp(1, 99);
    }
    app.pipeline_runs_buf = app.pipeline_runs.to_string();
}

fn sync_pipeline_concurrency_buf(app: &mut App) {
    if app.pipeline_concurrency_buf.is_empty() {
        app.pipeline_concurrency = 0;
    } else {
        app.pipeline_concurrency = app.pipeline_concurrency_buf.parse().unwrap_or(0).min(99);
    }
    app.pipeline_concurrency_buf = app.pipeline_concurrency.to_string();
}

fn effective_concurrency(runs: u32, concurrency: u32) -> u32 {
    match concurrency {
        0 => runs.max(1),
        value => value.min(runs).max(1),
    }
}

fn reset_running_state(app: &mut App) {
    app.screen = Screen::Running;
    app.progress_events.clear();
    app.is_running = true;
    app.run_error = None;
    app.consolidation_active = false;
    app.consolidation_phase = ConsolidationPhase::Confirm;
    app.consolidation_target = ConsolidationTarget::Single;
    app.consolidation_provider_cursor = 0;
    app.consolidation_prompt.clear();
    app.consolidation_running = false;
    app.consolidation_rx = None;
    app.diagnostic_running = false;
    app.diagnostic_rx = None;
    app.batch_stage1_done = false;
    app.result_files.clear();
    app.result_preview.clear();
    app.batch_result_runs.clear();
    app.batch_result_root_files.clear();
    app.batch_result_expanded.clear();
    app.result_cursor = 0;
    app.batch_progress_rx = None;
    app.multi_run_total = 0;
    app.multi_run_concurrency = 0;
    app.multi_run_cursor = 0;
    app.multi_run_states.clear();
    app.multi_run_step_labels.clear();
}

fn resolve_selected_agent_configs(
    app: &App,
    agent_names: &[String],
) -> Result<Vec<AgentConfig>, String> {
    let mut resolved = Vec::with_capacity(agent_names.len());
    for name in agent_names {
        let agent_config = app
            .effective_agent_config(name)
            .cloned()
            .ok_or_else(|| format!("{name} is not configured"))?;
        validate_agent_runtime(app, name, &agent_config)?;
        resolved.push(agent_config);
    }
    Ok(resolved)
}

fn pipeline_step_labels(def: &pipeline_mod::PipelineDefinition) -> Vec<String> {
    def.blocks
        .iter()
        .map(|block| {
            if block.name.trim().is_empty() {
                format!("Block {} ({})", block.id, block.agent)
            } else {
                format!("{} ({})", block.name, block.agent)
            }
        })
        .collect()
}

struct MultiExecutionParams {
    config: AppConfig,
    client: reqwest::Client,
    raw_prompt: String,
    prompt_context: PromptRuntimeContext,
    agent_names: Vec<String>,
    mode: ExecutionMode,
    iterations: u32,
    forward_prompt: bool,
    cli_timeout_secs: u64,
    runs: u32,
    concurrency: u32,
}

fn start_multi_execution(app: &mut App, params: MultiExecutionParams) {
    let MultiExecutionParams {
        config,
        client,
        raw_prompt,
        prompt_context,
        agent_names,
        mode,
        iterations,
        forward_prompt,
        cli_timeout_secs,
        runs,
        concurrency,
    } = params;

    let resolved_agents = match resolve_selected_agent_configs(app, &agent_names) {
        Ok(resolved) => resolved,
        Err(message) => {
            app.error_modal = Some(message);
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }
    };

    let output_dir = config.resolved_output_dir();
    let session_name = if app.session_name.trim().is_empty() {
        None
    } else {
        Some(app.session_name.trim().to_string())
    };
    let batch_root = match OutputManager::new_batch_parent(&output_dir, session_name.as_deref()) {
        Ok(output) => output,
        Err(e) => {
            app.error_modal = Some(format!("Failed to create batch output dir: {e}"));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }
    };

    if let Err(e) = batch_root.write_batch_info(runs, concurrency, &mode, &agent_names, iterations)
    {
        app.error_modal = Some(format!("Failed to write batch metadata: {e}"));
        app.screen = Screen::Prompt;
        app.is_running = false;
        return;
    }

    app.run_dir = Some(batch_root.run_dir().clone());
    app.iterations = iterations;
    app.runs = runs;
    app.concurrency = concurrency;
    app.init_multi_run_state(runs, concurrency, agent_names.clone());

    let (batch_tx, batch_rx) = mpsc::unbounded_channel();
    app.batch_progress_rx = Some(batch_rx);
    let cancel = Arc::new(AtomicBool::new(false));
    app.cancel_flag = cancel.clone();

    let batch_root_dir = batch_root.run_dir().clone();
    let default_max_tokens = config.default_max_tokens;
    let max_history_messages = config.max_history_messages;

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
                                cli_timeout_secs,
                            ),
                        ));
                    }

                    let result = match mode {
                        ExecutionMode::Solo => {
                            run_solo(
                                &prompt_context,
                                agents,
                                use_cli_by_agent,
                                &output,
                                progress_tx,
                                cancel.clone(),
                            )
                            .await
                        }
                        ExecutionMode::Relay => {
                            run_relay(
                                &prompt_context,
                                agents,
                                iterations,
                                1,
                                None,
                                forward_prompt,
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

fn start_multi_pipeline_execution(
    app: &mut App,
    client: reqwest::Client,
    iterations: u32,
    runs: u32,
    concurrency: u32,
    agent_configs: HashMap<String, (ProviderKind, ProviderConfig, bool)>,
) {
    let output_dir = app.config.resolved_output_dir();
    let session_name = if app.pipeline_session_name.trim().is_empty() {
        None
    } else {
        Some(app.pipeline_session_name.trim().to_string())
    };
    let batch_root = match OutputManager::new_batch_parent(&output_dir, session_name.as_deref()) {
        Ok(output) => output,
        Err(e) => {
            app.error_modal = Some(format!("Cannot create batch output dir: {e}"));
            app.screen = Screen::Pipeline;
            app.is_running = false;
            return;
        }
    };

    let step_labels = pipeline_step_labels(&app.pipeline_def);
    if let Err(e) = batch_root.write_batch_info(
        runs,
        concurrency,
        &ExecutionMode::Pipeline,
        &step_labels,
        iterations,
    ) {
        app.error_modal = Some(format!("Failed to write batch metadata: {e}"));
        app.screen = Screen::Pipeline;
        app.is_running = false;
        return;
    }

    app.run_dir = Some(batch_root.run_dir().clone());
    app.init_multi_run_state(runs, concurrency, step_labels);

    let config = app.config.clone();
    let cli_timeout = app.effective_cli_timeout_seconds();
    let pipeline_def = app.pipeline_def.clone();
    let prompt_context = PromptRuntimeContext::new(
        pipeline_def.initial_prompt.clone(),
        app.config.diagnostic_provider.is_some(),
    );
    let pipeline_source = app
        .pipeline_save_path
        .as_ref()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .map(str::to_string);

    let (batch_tx, batch_rx) = mpsc::unbounded_channel();
    app.batch_progress_rx = Some(batch_rx);
    let cancel = Arc::new(AtomicBool::new(false));
    app.cancel_flag = cancel.clone();

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
                    if let Err(e) = output.write_pipeline_session_info(
                        pipeline_def.blocks.len(),
                        pipeline_def.connections.len(),
                        iterations,
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

fn start_execution(app: &mut App) {
    reset_running_state(app);

    let config = app.config.clone();
    let http_timeout_secs = app.effective_http_timeout_seconds().max(1);
    let cli_timeout_secs = app.effective_cli_timeout_seconds().max(1);
    let runs = app.runs.max(1);
    let concurrency = effective_concurrency(runs, app.concurrency);
    let raw_prompt = app.prompt_text.clone();
    let prompt_context =
        PromptRuntimeContext::new(raw_prompt.clone(), app.config.diagnostic_provider.is_some());
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

    if runs > 1 {
        if app.resume_previous && matches!(mode, ExecutionMode::Relay | ExecutionMode::Swarm) {
            app.error_modal = Some("Resume is only supported for single-run execution".into());
            app.screen = Screen::Prompt;
            app.is_running = false;
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
                forward_prompt: forward_prompt_flag,
                cli_timeout_secs,
                runs,
                concurrency,
            },
        );
        return;
    }

    let mut agents: Vec<(String, Box<dyn provider::Provider>)> = Vec::new();
    let mut use_cli_by_agent: std::collections::HashMap<String, bool> =
        std::collections::HashMap::new();
    let mut run_models: Vec<(String, String)> = Vec::new();
    let mut agent_info: Vec<(String, String)> = Vec::new();
    let mut fallback_agent_kind = None;
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

        if let Err(message) = validate_agent_runtime(app, name, &agent_config) {
            app.error_modal = Some(message);
            app.screen = Screen::Prompt;
            app.is_running = false;
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

        if let Err(message) = validate_resume_run(&run_dir, mode, &agent_names) {
            app.error_modal = Some(message);
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }

        let last_iteration = match find_last_complete_iteration_for_agents(&run_dir, &agent_names) {
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
                let prev_path = run_dir.join(format!("{}_iter{}.md", file_key, last_iteration));
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
                    let prev_path = run_dir.join(format!("{}_iter{}.md", file_key, last_iteration));
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
        if let Err(e) = output.write_prompt(&raw_prompt) {
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
    let fallback_agent_kind = fallback_agent_kind.unwrap_or(ProviderKind::Anthropic);

    tokio::spawn(async move {
        let result = match mode {
            ExecutionMode::Solo => {
                run_solo(
                    &prompt_context,
                    agents,
                    use_cli_by_agent,
                    &output,
                    tx.clone(),
                    cancel,
                )
                .await
            }
            ExecutionMode::Relay => {
                run_relay(
                    &prompt_context,
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
                    &prompt_context,
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
        handle_execution_task_result(
            &tx,
            result,
            agent_names.first().cloned().unwrap_or_default(),
            fallback_agent_kind,
        );
    });
}

fn handle_execution_task_result(
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

fn validate_agent_runtime(
    app: &App,
    agent_label: &str,
    agent_config: &AgentConfig,
) -> Result<(), String> {
    if agent_config.use_cli
        && !app
            .cli_available
            .get(&agent_config.provider)
            .copied()
            .unwrap_or(false)
    {
        return Err(format!(
            "{agent_label}: {} CLI is not installed",
            agent_config.provider.display_name()
        ));
    }

    if !agent_config.use_cli && agent_config.api_key.trim().is_empty() {
        return Err(format!("{agent_label} API key is missing"));
    }

    provider::validate_effort_config(
        agent_config.provider,
        agent_config.use_cli,
        agent_config.reasoning_effort.as_deref(),
        agent_config.thinking_effort.as_deref(),
    )
    .map_err(|message| format!("{agent_label}: {message}"))
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

fn handle_batch_progress(app: &mut App, event: BatchProgressEvent) {
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
                app.progress_events.push(event);
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
            }
        }
        BatchProgressEvent::AllRunsDone => {
            app.is_running = false;
            app.batch_progress_rx = None;
            if should_offer_consolidation(app) {
                app.consolidation_active = true;
                app.consolidation_phase = ConsolidationPhase::Confirm;
                app.consolidation_target = ConsolidationTarget::PerRun;
                app.consolidation_provider_cursor = 0;
                app.consolidation_prompt.clear();
                app.consolidation_running = false;
                app.consolidation_rx = None;
            } else {
                maybe_start_diagnostics(app);
            }
        }
    }
}

fn multi_run_state_mut(app: &mut App, run_id: u32) -> Option<&mut RunState> {
    let index = run_id.checked_sub(1)? as usize;
    if app
        .multi_run_states
        .get(index)
        .is_some_and(|state| state.run_id == run_id)
    {
        return app.multi_run_states.get_mut(index);
    }

    app.multi_run_states
        .iter_mut()
        .find(|state| state.run_id == run_id)
}

fn update_multi_run_state(app: &mut App, run_id: u32, event: &ProgressEvent) {
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
            ..
        } => {
            let step = format_block_step_label(*block_id, label, agent_name);
            state.status = RunStatus::Running;
            update_step_status(state, &step, RunStepStatus::Running);
            state.push_log(format!("{step}: started"));
        }
        ProgressEvent::BlockLog {
            block_id,
            agent_name,
            message,
            ..
        } => {
            let step = format_block_step_label(*block_id, "", agent_name);
            state.push_log(format!("{step}: {message}"));
        }
        ProgressEvent::BlockFinished {
            block_id,
            agent_name,
            label,
            ..
        } => {
            let step = format_block_step_label(*block_id, label, agent_name);
            update_step_status(state, &step, RunStepStatus::Done);
            state.push_log(format!("{step}: finished"));
        }
        ProgressEvent::BlockError {
            block_id,
            agent_name,
            label,
            error,
            ..
        }
        | ProgressEvent::BlockSkipped {
            block_id,
            agent_name,
            label,
            reason: error,
            ..
        } => {
            let step = format_block_step_label(*block_id, label, agent_name);
            state.status = RunStatus::Failed;
            state.error = Some(error.clone());
            update_step_status(state, &step, RunStepStatus::Error);
            state.push_log(format!("{step}: {error}"));
        }
        ProgressEvent::AllDone => {}
    }
}

fn update_step_status(state: &mut RunState, label: &str, status: RunStepStatus) {
    if let Some(step) = state.steps.iter_mut().find(|step| step.label == label) {
        step.status = status;
    }
}

fn format_block_step_label(block_id: u32, label: &str, agent_name: &str) -> String {
    if label.trim().is_empty() {
        format!("Block {block_id} ({agent_name})")
    } else {
        format!("{label} ({agent_name})")
    }
}

fn should_offer_consolidation(app: &App) -> bool {
    if app.cancel_flag.load(Ordering::Relaxed) {
        return false;
    }
    if app.multi_run_total > 1 {
        return !successful_run_ids(app).is_empty();
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
    discover_final_outputs(run_dir, app.selected_mode, &app.selected_agents).len() > 1
}

fn successful_run_ids(app: &App) -> Vec<u32> {
    app.multi_run_states
        .iter()
        .filter(|state| state.status == RunStatus::Done)
        .map(|state| state.run_id)
        .collect()
}

fn discover_final_outputs(
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
        files.sort_by(|a, b| a.0.cmp(&b.0));
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

async fn discover_final_outputs_async(
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

        files.sort_by(|a, b| a.0.cmp(&b.0));
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

async fn find_last_iteration_async(
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
            Ok(None) => break,
            Err(_) => break,
        }
    }

    max_iter
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
                iter_to_agents.entry(iter).or_default().insert(key.clone());
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

fn session_matches_resume(
    session: &AgentSessionInfo,
    mode: ExecutionMode,
    agents: &[String],
) -> bool {
    if session.mode != mode {
        return false;
    }

    match mode {
        ExecutionMode::Relay => session.agents == agents,
        ExecutionMode::Swarm => {
            if session.agents.len() != agents.len() {
                return false;
            }
            let stored: std::collections::HashSet<&String> = session.agents.iter().collect();
            let requested: std::collections::HashSet<&String> = agents.iter().collect();
            stored == requested
        }
        ExecutionMode::Solo | ExecutionMode::Pipeline => session.agents == agents,
    }
}

fn validate_resume_run(
    run_dir: &std::path::Path,
    mode: ExecutionMode,
    agents: &[String],
) -> Result<(), String> {
    let session = OutputManager::read_agent_session_info(run_dir)
        .map_err(|e| format!("Failed to read session metadata: {e}"))?;
    if session_matches_resume(&session, mode, agents) {
        Ok(())
    } else {
        Err(format!(
            "Previous run at {} does not exactly match the selected {} configuration",
            run_dir.display(),
            mode
        ))
    }
}

fn run_dir_matches_mode_and_agents(
    run_dir: &std::path::Path,
    mode: ExecutionMode,
    agents: &[String],
) -> bool {
    OutputManager::read_agent_session_info(run_dir)
        .map(|session| session_matches_resume(&session, mode, agents))
        .unwrap_or(false)
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

fn selected_consolidation_agent(app: &App) -> Result<(String, AgentConfig), String> {
    let agent_name = app
        .config
        .agents
        .get(app.consolidation_provider_cursor)
        .map(|a| a.name.clone())
        .ok_or_else(|| "Select an agent for consolidation".to_string())?;
    let agent_config = app
        .effective_agent_config(&agent_name)
        .cloned()
        .ok_or_else(|| format!("{agent_name} is not configured"))?;
    validate_agent_runtime(app, &agent_name, &agent_config)?;
    Ok((agent_name, agent_config))
}

async fn build_file_consolidation_prompt(
    files: &[(String, std::path::PathBuf)],
    additional: &str,
    use_cli: bool,
) -> Result<String, String> {
    let mut prompt = String::from("Consolidate these outputs into one final markdown answer.\n\n");

    if use_cli {
        prompt.push_str("Files to read:\n");
        for (label, path) in files {
            prompt.push_str(&format!("- {label}: {}\n", path.display()));
        }
        prompt.push_str("\nRead each file before writing.\n");
    } else {
        prompt.push_str("Input reports:\n");
        for (label, path) in files {
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

fn build_cross_run_consolidation_prompt(
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

const CROSS_RUN_MAX_INPUT_BYTES: u64 = 200 * 1024;

struct ConsolidationRequest {
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

async fn run_consolidation_with_provider_factory<F>(
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

fn start_consolidation(app: &mut App) {
    if app.consolidation_running {
        return;
    }

    let run_dir = match app.run_dir.clone() {
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
    let additional = app.consolidation_prompt.trim().to_string();

    app.progress_events.push(ProgressEvent::AgentStarted {
        agent: agent_name.clone(),
        kind: provider_kind,
        iteration: 0,
    });
    app.progress_events.push(ProgressEvent::AgentLog {
        agent: agent_name.clone(),
        kind: provider_kind,
        iteration: 0,
        message: "consolidating reports".into(),
    });

    let (tx, rx) = mpsc::unbounded_channel();
    app.consolidation_rx = Some(rx);
    app.consolidation_running = true;
    app.consolidation_active = false;
    app.is_running = true;

    let mode = app.selected_mode;
    let selected_agents = app.selected_agents.clone();
    let successful_runs = successful_run_ids(app);
    let target = app.consolidation_target;
    let batch_stage1_done = app.batch_stage1_done;
    let default_max_tokens = app.config.default_max_tokens;
    let max_history_messages = app.config.max_history_messages;
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
                    cli_timeout,
                )
            },
        )
        .await;
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

    match result {
        Ok(path) => {
            app.progress_events.push(ProgressEvent::AgentFinished {
                agent: agent_name.clone(),
                kind,
                iteration: 0,
            });
            app.progress_events.push(ProgressEvent::AgentLog {
                agent: agent_name,
                kind,
                iteration: 0,
                message: format!("Consolidation saved to {path}"),
            });

            if app.consolidation_target == ConsolidationTarget::PerRun {
                app.batch_stage1_done = true;
                app.consolidation_active = true;
                app.consolidation_phase = ConsolidationPhase::CrossRunConfirm;
                app.consolidation_target = ConsolidationTarget::AcrossRuns;
                app.consolidation_prompt.clear();
            } else {
                app.consolidation_active = false;
                maybe_start_diagnostics(app);
            }
        }
        Err(e) => {
            app.progress_events.push(ProgressEvent::AgentError {
                agent: agent_name,
                kind,
                iteration: 0,
                error: e.clone(),
                details: Some(e),
            });
            app.consolidation_active = false;
            maybe_start_diagnostics(app);
        }
    }
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
    if let Err(message) = validate_agent_runtime(
        app,
        &format!("Diagnostic agent '{}'", diag_agent_name),
        &agent_config,
    ) {
        app.error_modal = Some(message);
        return;
    }
    let pconfig = agent_config.to_provider_config();

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

fn collect_application_errors(app: &App, run_dir: &std::path::Path) -> Vec<String> {
    let mut errors = Vec::new();
    for event in &app.progress_events {
        match event {
            ProgressEvent::AgentError {
                agent,
                iteration,
                error,
                details,
                ..
            } => {
                let body = details.as_deref().unwrap_or(error);
                errors.push(format!("[{agent} iter {iteration}] {body}"));
            }
            ProgressEvent::BlockError {
                block_id,
                agent_name,
                iteration,
                error,
                details,
                ..
            } => {
                let body = details.as_deref().unwrap_or(error);
                errors.push(format!(
                    "[block {block_id} {agent_name} iter {iteration}] {body}"
                ));
            }
            _ => {}
        }
    }

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

fn build_diagnostic_prompt(
    report_files: &[std::path::PathBuf],
    app_errors: &[String],
    use_cli: bool,
) -> String {
    let mut prompt = String::from(
        "Analyze all reports for OPERATIONAL errors only and produce a markdown report.\n",
    );
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
        if OutputManager::is_batch_root(run_dir) {
            let mut root_files = Vec::new();
            let mut run_groups = Vec::new();

            if let Ok(entries) = std::fs::read_dir(run_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        root_files.push(path);
                    } else if path.is_dir() {
                        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                            continue;
                        };
                        let Some(run_id) = name
                            .strip_prefix("run_")
                            .and_then(|n| n.parse::<u32>().ok())
                        else {
                            continue;
                        };
                        let mut files = std::fs::read_dir(&path)
                            .ok()
                            .into_iter()
                            .flat_map(|entries| entries.flatten())
                            .filter_map(|entry| {
                                let path = entry.path();
                                if path.is_file() {
                                    Some(path)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>();
                        files.sort();
                        run_groups.push(BatchRunGroup { run_id, files });
                    }
                }
            }

            root_files.sort();
            run_groups.sort_by_key(|group| group.run_id);
            app.result_files.clear();
            app.batch_result_runs = run_groups;
            app.batch_result_root_files = root_files;
            app.batch_result_expanded = app
                .batch_result_runs
                .iter()
                .map(|group| group.run_id)
                .collect();
        } else {
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
            app.batch_result_runs.clear();
            app.batch_result_root_files.clear();
            app.batch_result_expanded.clear();
        }
    }
    app.result_cursor = 0;
    update_preview(app);
}

fn update_preview(app: &mut App) {
    let preview = if has_batch_result_tree(app) {
        match batch_result_entry_at(app, app.result_cursor) {
            Some(BatchResultEntry::File(path)) => {
                std::fs::read_to_string(path).unwrap_or_else(|e| format!("Error: {e}"))
            }
            _ => String::new(),
        }
    } else if let Some(path) = app.result_files.get(app.result_cursor) {
        std::fs::read_to_string(path).unwrap_or_else(|e| format!("Error: {e}"))
    } else {
        String::new()
    };
    app.result_preview = preview;
}

fn batch_run_directories(run_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
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

fn has_batch_result_tree(app: &App) -> bool {
    !app.batch_result_runs.is_empty() || !app.batch_result_root_files.is_empty()
}

enum BatchResultEntry<'a> {
    RunHeader(u32),
    File(&'a std::path::PathBuf),
}

fn batch_result_visible_len(app: &App) -> usize {
    let mut len = app.batch_result_root_files.len();
    for group in &app.batch_result_runs {
        len += 1;
        if app.batch_result_expanded.contains(&group.run_id) {
            len += group.files.len();
        }
    }
    len
}

fn batch_result_entry_at(app: &App, mut index: usize) -> Option<BatchResultEntry<'_>> {
    for group in &app.batch_result_runs {
        if index == 0 {
            return Some(BatchResultEntry::RunHeader(group.run_id));
        }
        index -= 1;

        if app.batch_result_expanded.contains(&group.run_id) {
            if index < group.files.len() {
                return Some(BatchResultEntry::File(&group.files[index]));
            }
            index = index.saturating_sub(group.files.len());
        }
    }

    if index < app.batch_result_root_files.len() {
        Some(BatchResultEntry::File(&app.batch_result_root_files[index]))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::error::AppError;
    use crate::execution::ProgressEvent;
    use crate::provider::{CompletionResponse, Provider};
    use crossterm::event::KeyEvent;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;
    use tokio::sync::mpsc;

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

    fn test_agent(
        name: &str,
        provider: ProviderKind,
        model: &str,
        use_cli: bool,
        thinking_effort: Option<&str>,
    ) -> AgentConfig {
        AgentConfig {
            name: name.to_string(),
            provider,
            api_key: if use_cli {
                String::new()
            } else {
                "k".to_string()
            },
            model: model.to_string(),
            reasoning_effort: None,
            thinking_effort: thinking_effort.map(str::to_string),
            use_cli,
            cli_print_mode: true,
            extra_cli_args: String::new(),
        }
    }

    struct HistoryEchoProvider {
        kind: ProviderKind,
        calls: usize,
    }

    #[async_trait::async_trait]
    impl Provider for HistoryEchoProvider {
        fn kind(&self) -> ProviderKind {
            self.kind
        }

        async fn send(&mut self, _message: &str) -> Result<CompletionResponse, AppError> {
            self.calls += 1;
            Ok(CompletionResponse {
                content: format!("call {}", self.calls),
                debug_logs: Vec::new(),
            })
        }
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
    fn validate_agent_runtime_rejects_anthropic_max_in_api_mode() {
        let app = test_app();
        let agent = test_agent(
            "Claude",
            ProviderKind::Anthropic,
            "claude-opus-4-6",
            false,
            Some("max"),
        );

        let err = validate_agent_runtime(&app, &agent.name, &agent).expect_err("should reject");
        assert!(err.contains("\"max\" thinking effort requires CLI mode"));
    }

    #[test]
    fn validate_agent_runtime_allows_anthropic_max_in_cli_mode() {
        let mut app = test_app();
        app.cli_available.insert(ProviderKind::Anthropic, true);
        let agent = test_agent(
            "Claude",
            ProviderKind::Anthropic,
            "claude-sonnet-4-5",
            true,
            Some("max"),
        );

        validate_agent_runtime(&app, &agent.name, &agent)
            .expect("cli mode should be allowed through to the provider");
    }

    #[test]
    fn handle_execution_task_result_uses_selected_provider_kind() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        handle_execution_task_result(
            &tx,
            Err(AppError::Config("boom".to_string())),
            "OpenAI".to_string(),
            ProviderKind::OpenAI,
        );

        match rx.try_recv().expect("agent error event") {
            ProgressEvent::AgentError {
                agent, kind, error, ..
            } => {
                assert_eq!(agent, "OpenAI");
                assert_eq!(kind, ProviderKind::OpenAI);
                assert!(error.contains("Config error: boom"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(matches!(
            rx.try_recv().expect("all done"),
            ProgressEvent::AllDone
        ));
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
        assert_eq!(parse_pipeline_iteration_filename("Claude_iter2.md"), None);
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

    #[tokio::test]
    async fn find_last_iteration_async_matches_sync_for_pipeline_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Analyzer_b1_Claude_iter2.md"), "test").unwrap();
        fs::write(dir.path().join("Reviewer_b2_Gemini_iter5.md"), "test").unwrap();

        let sync = find_last_iteration(dir.path(), &[]);
        let async_found = find_last_iteration_async(dir.path(), &[]).await;
        assert_eq!(async_found, sync);
    }

    #[test]
    fn find_last_iteration_multiple_files() {
        let dir = tempdir().unwrap();
        write_agent_iter(dir.path(), "Claude", 1);
        write_agent_iter(dir.path(), "OpenAI", 3);
        write_agent_iter(dir.path(), "Gemini", 2);

        let keys: Vec<String> = ["Claude", "OpenAI", "Gemini"]
            .iter()
            .map(|n| App::agent_file_key(n))
            .collect();
        assert_eq!(find_last_iteration(dir.path(), &keys), Some(3));
    }

    #[tokio::test]
    async fn find_last_iteration_async_matches_sync_for_agent_files() {
        let dir = tempdir().unwrap();
        write_agent_iter(dir.path(), "Claude", 1);
        write_agent_iter(dir.path(), "OpenAI", 4);
        write_agent_iter(dir.path(), "Gemini", 2);

        let keys: Vec<String> = ["Claude", "OpenAI", "Gemini"]
            .iter()
            .map(|n| App::agent_file_key(n))
            .collect();
        let sync = find_last_iteration(dir.path(), &keys);
        let async_found = find_last_iteration_async(dir.path(), &keys).await;
        assert_eq!(async_found, sync);
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
    fn run_dir_matches_relay_requires_exact_agent_order() {
        let dir = tempdir().unwrap();
        write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

        assert!(!run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["anthropic".to_string()]
        ));
    }

    #[test]
    fn run_dir_matches_relay_rejects_different_agent_order() {
        let dir = tempdir().unwrap();
        write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

        assert!(!run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Relay,
            &["openai".to_string(), "anthropic".to_string()]
        ));
    }

    #[test]
    fn run_dir_matches_swarm_accepts_different_agent_order_for_same_set() {
        let dir = tempdir().unwrap();
        write_session_toml(dir.path(), "swarm", &["anthropic", "openai"]);

        assert!(run_dir_matches_mode_and_agents(
            dir.path(),
            ExecutionMode::Swarm,
            &["openai".to_string(), "anthropic".to_string()]
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
    fn validate_resume_run_rejects_named_session_with_wrong_relay_order() {
        let dir = tempdir().unwrap();
        write_session_toml(dir.path(), "relay", &["anthropic", "openai"]);

        let err = validate_resume_run(
            dir.path(),
            ExecutionMode::Relay,
            &["openai".to_string(), "anthropic".to_string()],
        )
        .expect_err("should reject");
        assert!(err.contains("does not exactly match"));
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
            agent: "OpenAI".to_string(),
            kind: ProviderKind::OpenAI,
            iteration: 2,
            error: "api failed".to_string(),
            details: Some("rate limited".to_string()),
        });
        fs::write(dir.path().join("_errors.log"), "tool timeout\n\n").unwrap();

        let errors = collect_application_errors(&app, dir.path());
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().any(|e| e == "[OpenAI iter 2] rate limited"));
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

    #[tokio::test]
    async fn per_run_consolidation_recreates_provider_each_run() {
        let dir = tempdir().unwrap();
        let batch_root = OutputManager::new_batch_parent(dir.path(), Some("batch")).unwrap();
        let run1 = batch_root.new_run_subdir(1).unwrap();
        let run2 = batch_root.new_run_subdir(2).unwrap();

        for run_dir in [run1.run_dir(), run2.run_dir()] {
            fs::write(run_dir.join("Claude_iter1.md"), "claude").unwrap();
            fs::write(run_dir.join("OpenAI_iter1.md"), "openai").unwrap();
        }

        let result = run_consolidation_with_provider_factory(
            ConsolidationRequest {
                run_dir: batch_root.run_dir().clone(),
                target: ConsolidationTarget::PerRun,
                mode: ExecutionMode::Solo,
                selected_agents: vec!["Claude".to_string(), "OpenAI".to_string()],
                successful_runs: vec![1, 2],
                batch_stage1_done: false,
                additional: String::new(),
                agent_name: "Claude".to_string(),
                agent_use_cli: false,
            },
            || {
                Box::new(HistoryEchoProvider {
                    kind: ProviderKind::Anthropic,
                    calls: 0,
                })
            },
        )
        .await
        .expect("consolidation");

        assert_eq!(result, "Per-run consolidation completed");
        assert_eq!(
            fs::read_to_string(run1.run_dir().join("consolidation.md")).unwrap(),
            "call 1"
        );
        assert_eq!(
            fs::read_to_string(run2.run_dir().join("consolidation.md")).unwrap(),
            "call 1"
        );
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
    fn enforce_prompt_resume_constraints_disables_resume_in_multi_run_relay() {
        let mut app = test_app();
        app.selected_mode = ExecutionMode::Relay;
        app.runs = 3;
        app.resume_previous = true;

        enforce_prompt_resume_constraints(&mut app);

        assert!(!app.resume_previous);
    }

    #[test]
    fn handle_prompt_resume_toggle_shows_error_when_disallowed() {
        let mut app = test_app();
        app.selected_mode = ExecutionMode::Swarm;
        app.prompt_focus = PromptFocus::Resume;
        app.runs = 2;
        app.resume_previous = false;

        handle_prompt_key(&mut app, key(KeyCode::Char(' ')));

        assert!(!app.resume_previous);
        assert_eq!(
            app.error_modal,
            Some("Resume is only supported for single-run execution".to_string())
        );
    }

    #[test]
    fn batch_result_entry_helpers_respect_expansion_state() {
        let mut app = test_app();
        app.batch_result_runs = vec![
            BatchRunGroup {
                run_id: 1,
                files: vec![PathBuf::from("run1/a.md"), PathBuf::from("run1/b.md")],
            },
            BatchRunGroup {
                run_id: 2,
                files: vec![PathBuf::from("run2/c.md")],
            },
        ];
        app.batch_result_root_files = vec![PathBuf::from("root.md")];
        app.batch_result_expanded.insert(1);

        assert_eq!(batch_result_visible_len(&app), 5);
        assert!(matches!(
            batch_result_entry_at(&app, 0),
            Some(BatchResultEntry::RunHeader(1))
        ));
        assert!(matches!(
            batch_result_entry_at(&app, 1),
            Some(BatchResultEntry::File(path)) if path.ends_with("run1/a.md")
        ));
        assert!(matches!(
            batch_result_entry_at(&app, 2),
            Some(BatchResultEntry::File(path)) if path.ends_with("run1/b.md")
        ));
        assert!(matches!(
            batch_result_entry_at(&app, 3),
            Some(BatchResultEntry::RunHeader(2))
        ));
        assert!(matches!(
            batch_result_entry_at(&app, 4),
            Some(BatchResultEntry::File(path)) if path.ends_with("root.md")
        ));
        assert!(batch_result_entry_at(&app, 5).is_none());
    }

    #[test]
    fn prompt_focus_cycle_solo_tab_and_backtab() {
        let mut app = test_app();
        app.selected_mode = ExecutionMode::Solo;
        app.prompt_focus = PromptFocus::Text;

        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::SessionName);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Iterations);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Runs);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Concurrency);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Text);

        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Concurrency);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Runs);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Iterations);
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
        assert_eq!(app.prompt_focus, PromptFocus::Runs);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Concurrency);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Resume);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Text);

        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Resume);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Concurrency);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Runs);
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
        assert_eq!(app.prompt_focus, PromptFocus::Runs);
        handle_prompt_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.prompt_focus, PromptFocus::Concurrency);
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
        assert_eq!(app.prompt_focus, PromptFocus::Concurrency);
        handle_prompt_key(&mut app, key(KeyCode::BackTab));
        assert_eq!(app.prompt_focus, PromptFocus::Runs);
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
        assert_eq!(
            after_nav.position,
            (2, 2),
            "unmodified arrow should not move the block"
        );
        assert_eq!(
            app.pipeline_block_cursor,
            Some(2),
            "cursor should navigate to block 2"
        );

        // Shift+arrow moves the selected block
        handle_pipeline_builder_key(&mut app, KeyEvent::new(KeyCode::Right, KeyModifiers::SHIFT));
        let moved = app.pipeline_def.blocks.iter().find(|b| b.id == 2).unwrap();
        assert_eq!(moved.position, (4, 2), "Shift+arrow should move the block");
        assert_eq!(
            app.pipeline_block_cursor,
            Some(2),
            "cursor stays on the moved block"
        );
    }
}
