use crate::app::{App, PromptFocus};
use crate::execution::ExecutionMode;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;
use unicode_width::UnicodeWidthChar;

pub fn draw(f: &mut Frame, app: &App) {
    let is_solo = app.selected_mode == ExecutionMode::Solo;
    let options_height = if is_solo { 0 } else { 3 };
    let iterations_height = if is_solo { 0 } else { 3 };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),                // Title
            Constraint::Min(0),                   // Prompt text area
            Constraint::Length(3),                // Session name
            Constraint::Length(iterations_height), // Iterations
            Constraint::Length(options_height),    // Options row (Resume / Forward Prompt)
            Constraint::Length(3),                // Help bar
        ])
        .split(f.area());

    // Title
    let agents_str: Vec<&str> = app
        .selected_agents
        .iter()
        .map(|a| a.as_str())
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

    let wrapped = char_wrap_text(display_text, prompt_inner.width as usize);
    let prompt_area = Paragraph::new(wrapped.as_str())
        .style(text_style)
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

    let iter_info = if is_solo {
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

    // Options row: Resume + Forward Prompt (hidden for Solo)
    if !is_solo {
        let is_relay = app.selected_mode == ExecutionMode::Relay;
        let option_cols = if is_relay {
            Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[4])
        } else {
            // Swarm: only Resume, full width
            Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(100), Constraint::Min(0)])
                .split(chunks[4])
        };

        // Resume box
        let resume_border = if app.prompt_focus == PromptFocus::Resume {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let resume_text = if app.resume_previous {
            "on (uses session name, or latest compatible run if empty)"
        } else {
            "off"
        };
        let resume_style = if app.resume_previous {
            Style::default().fg(Color::Green)
        } else {
            Style::default()
        };
        let resume = Paragraph::new(resume_text).style(resume_style).block(
            Block::default()
                .title(" Resume ")
                .borders(Borders::ALL)
                .border_style(resume_border),
        );
        f.render_widget(resume, option_cols[0]);

        // Forward Prompt box (Relay only)
        if is_relay {
            let fp_border = if app.prompt_focus == PromptFocus::ForwardPrompt {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let fp_text = if app.forward_prompt { "on" } else { "off" };
            let fp_style = if app.forward_prompt {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };
            let fp = Paragraph::new(fp_text).style(fp_style).block(
                Block::default()
                    .title(" Forward Prompt ")
                    .borders(Borders::ALL)
                    .border_style(fp_border),
            );
            f.render_widget(fp, option_cols[1]);
        }
    }

    // Help bar
    let help_spans: Vec<Span> = match app.prompt_focus {
        PromptFocus::Iterations if !is_solo => {
            vec![
                Span::styled("Type", Style::default().fg(Color::Yellow)),
                Span::raw(": set value  "),
                Span::styled("↑/+", Style::default().fg(Color::Yellow)),
                Span::raw(": increase  "),
                Span::styled("↓/-", Style::default().fg(Color::Yellow)),
                Span::raw(": decrease  "),
                Span::styled("Tab", Style::default().fg(Color::Yellow)),
                Span::raw(": next field  "),
                Span::styled("Enter/F5", Style::default().fg(Color::Yellow)),
                Span::raw(": run  "),
                Span::styled("Esc", Style::default().fg(Color::Yellow)),
                Span::raw(": back"),
            ]
        }
        PromptFocus::Text => {
            vec![
                Span::styled("↑/↓", Style::default().fg(Color::Yellow)),
                Span::raw(": line  "),
                Span::styled("←/→", Style::default().fg(Color::Yellow)),
                Span::raw(": move  "),
                Span::styled("Alt+←/→", Style::default().fg(Color::Yellow)),
                Span::raw(": move word  "),
                Span::styled("Alt+Backspace", Style::default().fg(Color::Yellow)),
                Span::raw(": delete word  "),
                Span::styled("Tab", Style::default().fg(Color::Yellow)),
                Span::raw(": next field  "),
                Span::styled("F5", Style::default().fg(Color::Yellow)),
                Span::raw(": run  "),
                Span::styled("Esc", Style::default().fg(Color::Yellow)),
                Span::raw(": back"),
            ]
        }
        PromptFocus::Resume | PromptFocus::ForwardPrompt => {
            vec![
                Span::styled("Space", Style::default().fg(Color::Yellow)),
                Span::raw(": toggle  "),
                Span::styled("Tab", Style::default().fg(Color::Yellow)),
                Span::raw(": next field  "),
                Span::styled("Enter/F5", Style::default().fg(Color::Yellow)),
                Span::raw(": run  "),
                Span::styled("Esc", Style::default().fg(Color::Yellow)),
                Span::raw(": back"),
            ]
        }
        _ => {
            vec![
                Span::styled("Tab", Style::default().fg(Color::Yellow)),
                Span::raw(": next field  "),
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

/// Character-wrap text into visual lines matching `prompt_cursor_layout` wrapping.
/// Returns a new string with newlines inserted at wrap points.
pub(crate) fn char_wrap_text(text: &str, width: usize) -> String {
    if width == 0 {
        return text.to_string();
    }
    let mut result = String::with_capacity(text.len() + text.len() / width.max(1));
    let mut col = 0usize;
    for ch in text.chars() {
        if ch == '\n' {
            result.push('\n');
            col = 0;
            continue;
        }
        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0).min(width);
        if ch_width == 0 {
            result.push(ch);
            continue;
        }
        if col + ch_width > width {
            result.push('\n');
            col = 0;
        }
        result.push(ch);
        col += ch_width;
    }
    result
}

pub(crate) fn prompt_cursor_layout(
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

        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0).min(width);
        if ch_width == 0 {
            continue;
        }

        if col >= width || col + ch_width > width {
            row += 1;
            col = 0;
        }

        col += ch_width;
    }

    if col >= width {
        row += col / width;
        col %= width;
    }

    let scroll = row.saturating_sub(height - 1) as u16;
    (scroll, col, row)
}

#[cfg(test)]
mod tests {
    use super::{char_wrap_text, prompt_cursor_layout};

    #[test]
    fn prompt_cursor_layout_zero_size_returns_origin() {
        assert_eq!(prompt_cursor_layout("hello", 3, 0, 2), (0, 0, 0));
        assert_eq!(prompt_cursor_layout("hello", 3, 4, 0), (0, 0, 0));
    }

    #[test]
    fn prompt_cursor_layout_single_line_no_wrap() {
        assert_eq!(prompt_cursor_layout("hello", 2, 10, 5), (0, 2, 0));
    }

    #[test]
    fn prompt_cursor_layout_wraps_when_width_exceeded() {
        assert_eq!(prompt_cursor_layout("abcdef", 6, 3, 5), (0, 0, 2));
    }

    #[test]
    fn prompt_cursor_layout_handles_newline() {
        assert_eq!(prompt_cursor_layout("ab\ncd", 4, 10, 5), (0, 1, 1));
    }

    #[test]
    fn prompt_cursor_layout_handles_wide_characters() {
        assert_eq!(prompt_cursor_layout("中a", "中".len(), 2, 5), (0, 0, 1));
    }

    #[test]
    fn prompt_cursor_layout_wide_char_before_newline_no_extra_row() {
        assert_eq!(prompt_cursor_layout("中\na", "中\n".len(), 2, 5), (0, 0, 1));
    }

    #[test]
    fn prompt_cursor_layout_newline_after_exact_wrap_does_not_double_advance() {
        assert_eq!(prompt_cursor_layout("abc\ndef", 4, 3, 5), (0, 0, 1));
    }

    #[test]
    fn prompt_cursor_layout_trailing_newline_after_exact_wrap() {
        assert_eq!(prompt_cursor_layout("abc\n", 4, 3, 5), (0, 0, 1));
    }

    #[test]
    fn prompt_cursor_layout_clamps_non_boundary_cursor() {
        // "é" occupies bytes [0..2], index 1 is not a char boundary and should clamp to 0
        assert_eq!(prompt_cursor_layout("éx", 1, 10, 5), (0, 0, 0));
    }

    #[test]
    fn prompt_cursor_layout_scrolls_when_cursor_below_visible_height() {
        let text = "a\nb\nc\nd\ne";
        let (scroll, col, row) = prompt_cursor_layout(text, text.len(), 10, 2);
        assert_eq!(col, 1);
        assert_eq!(row, 4);
        assert_eq!(scroll, 3);
    }

    #[test]
    fn char_wrap_text_no_wrap_needed() {
        assert_eq!(char_wrap_text("hello", 10), "hello");
    }

    #[test]
    fn char_wrap_text_wraps_at_width() {
        assert_eq!(char_wrap_text("abcdef", 3), "abc\ndef");
    }

    #[test]
    fn char_wrap_text_preserves_newlines() {
        assert_eq!(char_wrap_text("ab\ncd", 5), "ab\ncd");
    }

    #[test]
    fn char_wrap_text_zero_width_returns_original() {
        assert_eq!(char_wrap_text("abc", 0), "abc");
    }

    #[test]
    fn char_wrap_text_wide_chars() {
        // CJK char "中" has width 2; width=3 fits one CJK + one ASCII
        assert_eq!(char_wrap_text("中a中", 3), "中a\n中");
    }
}
