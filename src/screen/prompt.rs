use crate::app::{App, PromptFocus};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Min(0),    // Prompt text area
            Constraint::Length(3), // Session name
            Constraint::Length(3), // Iterations
            Constraint::Length(3), // Resume previous
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title
    let agents_str: Vec<&str> = app
        .selected_agents
        .iter()
        .map(|a| a.display_name())
        .collect();
    let title = Paragraph::new(format!(
        "Prompt — {} mode with {}",
        app.selected_mode,
        agents_str.join(", ")
    ))
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
    .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Prompt text area
    let prompt_border = if app.prompt_focus == PromptFocus::Text {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let prompt_block = Block::default()
        .title(" Prompt ")
        .borders(Borders::ALL)
        .border_style(prompt_border);
    let prompt_inner = prompt_block.inner(chunks[1]);

    let display_text = if app.prompt_text.is_empty() && app.prompt_focus != PromptFocus::Text {
        "Enter your prompt here..."
    } else {
        app.prompt_text.as_str()
    };

    let text_style = if app.prompt_text.is_empty() && app.prompt_focus != PromptFocus::Text {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    let (scroll_y, cursor_col, cursor_row) = if app.prompt_focus == PromptFocus::Text {
        prompt_cursor_layout(
            app.prompt_text.as_str(),
            app.prompt_cursor,
            prompt_inner.width as usize,
            prompt_inner.height as usize,
        )
    } else {
        (0, 0, 0)
    };

    let prompt_area = Paragraph::new(display_text)
        .style(text_style)
        .wrap(Wrap { trim: false })
        .scroll((scroll_y, 0))
        .block(prompt_block);
    f.render_widget(prompt_area, chunks[1]);

    if app.prompt_focus == PromptFocus::Text && prompt_inner.width > 0 && prompt_inner.height > 0 {
        let visible_row = cursor_row.saturating_sub(scroll_y as usize);
        let x =
            prompt_inner.x + (cursor_col.min(prompt_inner.width.saturating_sub(1) as usize) as u16);
        let y = prompt_inner.y
            + (visible_row.min(prompt_inner.height.saturating_sub(1) as usize) as u16);
        f.set_cursor_position((x, y));
    }

    // Session name
    let name_border = if app.prompt_focus == PromptFocus::SessionName {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let name_display = if app.session_name.is_empty() {
        if app.prompt_focus == PromptFocus::SessionName {
            "_".to_string()
        } else {
            "(optional)".to_string()
        }
    } else if app.prompt_focus == PromptFocus::SessionName {
        format!("{}_", app.session_name)
    } else {
        app.session_name.clone()
    };

    let name_style = if app.session_name.is_empty() && app.prompt_focus != PromptFocus::SessionName
    {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    let session_name = Paragraph::new(name_display).style(name_style).block(
        Block::default()
            .title(" Session Name ")
            .borders(Borders::ALL)
            .border_style(name_border),
    );
    f.render_widget(session_name, chunks[2]);

    // Iterations
    let iter_border = if app.prompt_focus == PromptFocus::Iterations {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let iter_info = if app.selected_mode == crate::execution::ExecutionMode::Solo {
        "(Solo mode: always 1 iteration)".to_string()
    } else if app.prompt_focus == PromptFocus::Iterations {
        format!("{}_", app.iterations_buf)
    } else {
        format!("{}", app.iterations)
    };

    let iterations = Paragraph::new(iter_info).block(
        Block::default()
            .title(" Iterations ")
            .borders(Borders::ALL)
            .border_style(iter_border),
    );
    f.render_widget(iterations, chunks[3]);

    // Resume previous session (relay/swarm only)
    let resume_available = app.selected_mode != crate::execution::ExecutionMode::Solo;
    let resume_text = if resume_available {
        if app.resume_previous {
            "on (uses session name, or latest compatible run if empty)"
        } else {
            "off"
        }
    } else {
        "(not available in Solo mode)"
    };
    let resume_style = if resume_available && app.resume_previous {
        Style::default().fg(Color::Green)
    } else if !resume_available {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };
    let resume = Paragraph::new(resume_text).style(resume_style).block(
        Block::default()
            .title(" Resume Previous [r] ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(resume, chunks[4]);

    // Help bar
    let help_spans: Vec<Span> = match app.prompt_focus {
        PromptFocus::Iterations if app.selected_mode != crate::execution::ExecutionMode::Solo => {
            vec![
                Span::styled("Type", Style::default().fg(Color::Yellow)),
                Span::raw(": set value  "),
                Span::styled("↑/+", Style::default().fg(Color::Yellow)),
                Span::raw(": increase  "),
                Span::styled("↓/-", Style::default().fg(Color::Yellow)),
                Span::raw(": decrease  "),
                Span::styled("Tab", Style::default().fg(Color::Yellow)),
                Span::raw(": next field  "),
                Span::styled("r", Style::default().fg(Color::Yellow)),
                Span::raw(": toggle resume  "),
                Span::styled("Enter/F5", Style::default().fg(Color::Yellow)),
                Span::raw(": run  "),
                Span::styled("Esc", Style::default().fg(Color::Yellow)),
                Span::raw(": back"),
            ]
        }
        PromptFocus::Text => {
            vec![
                Span::styled("←/→", Style::default().fg(Color::Yellow)),
                Span::raw(": move  "),
                Span::styled("Alt+←/→", Style::default().fg(Color::Yellow)),
                Span::raw(": move word  "),
                Span::styled("Alt+Backspace", Style::default().fg(Color::Yellow)),
                Span::raw(": delete word  "),
                Span::styled("Tab", Style::default().fg(Color::Yellow)),
                Span::raw(": next field  "),
                Span::styled("r", Style::default().fg(Color::Yellow)),
                Span::raw(": toggle resume  "),
                Span::styled("F5", Style::default().fg(Color::Yellow)),
                Span::raw(": run  "),
                Span::styled("Esc", Style::default().fg(Color::Yellow)),
                Span::raw(": back"),
            ]
        }
        _ => {
            vec![
                Span::styled("Tab", Style::default().fg(Color::Yellow)),
                Span::raw(": next field  "),
                Span::styled("r", Style::default().fg(Color::Yellow)),
                Span::raw(": toggle resume  "),
                Span::styled("Enter/F5", Style::default().fg(Color::Yellow)),
                Span::raw(": run  "),
                Span::styled("Esc", Style::default().fg(Color::Yellow)),
                Span::raw(": back"),
            ]
        }
    };
    let help = Paragraph::new(Line::from(help_spans)).block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[5]);
}

fn prompt_cursor_layout(
    text: &str,
    cursor: usize,
    width: usize,
    height: usize,
) -> (u16, usize, usize) {
    if width == 0 || height == 0 {
        return (0, 0, 0);
    }

    let mut cursor = cursor.min(text.len());
    while cursor > 0 && !text.is_char_boundary(cursor) {
        cursor -= 1;
    }

    let mut row = 0usize;
    let mut col = 0usize;
    for (idx, ch) in text.char_indices() {
        if idx >= cursor {
            break;
        }
        if ch == '\n' {
            row += 1;
            col = 0;
            continue;
        }

        col += 1;
        if col >= width {
            row += 1;
            col = 0;
        }
    }

    let scroll = row.saturating_sub(height - 1) as u16;
    (scroll, col, row)
}
