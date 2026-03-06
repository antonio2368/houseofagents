use crate::app::{App, PipelineDialogMode, PipelineEditField, PipelineFocus};
use crate::execution::pipeline::BlockId;
use crate::execution::truncate_chars;
use crate::screen::centered_rect;
use crate::screen::prompt::prompt_cursor_layout;

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

pub(crate) const BLOCK_W: u16 = 18;
pub(crate) const BLOCK_H: u16 = 5;
pub(crate) const CELL_W: u16 = 20; // block width + 2 gap
pub(crate) const CELL_H: u16 = 6; // block height + 1 gap

pub fn draw(f: &mut Frame, app: &App) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // title
            Constraint::Length(6), // prompt + iterations
            Constraint::Min(0),   // builder canvas
            Constraint::Length(2), // help + status
        ])
        .split(area);

    draw_title(f, chunks[0]);
    draw_prompt_area(f, app, chunks[1]);
    draw_canvas(f, app, chunks[2]);
    draw_help_bar(f, app, chunks[3]);

    // Overlays
    if app.pipeline_show_edit {
        draw_edit_popup(f, app, area);
    }
    if app.pipeline_file_dialog.is_some() {
        draw_file_dialog(f, app, area);
    }
    if let Some(ref msg) = app.error_modal {
        draw_error_modal(f, msg);
    }
}

fn draw_title(f: &mut Frame, area: Rect) {
    let block = Block::default()
        .title(" Custom Pipeline ")
        .title_alignment(ratatui::layout::Alignment::Center)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));
    f.render_widget(block, area);
}

fn draw_prompt_area(f: &mut Frame, app: &App, area: Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(80), Constraint::Percentage(20)])
        .split(area);

    // Initial prompt textarea
    let prompt_focus = app.pipeline_focus == PipelineFocus::InitialPrompt;
    let prompt_style = if prompt_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let prompt_block = Block::default()
        .title(" Initial Prompt ")
        .borders(Borders::ALL)
        .border_style(prompt_style);
    let inner = prompt_block.inner(cols[0]);

    let display_text = if app.pipeline_def.initial_prompt.is_empty() && !prompt_focus {
        "Enter initial prompt..."
    } else {
        app.pipeline_def.initial_prompt.as_str()
    };

    let text_style = if app.pipeline_def.initial_prompt.is_empty() && !prompt_focus {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    let (scroll_y, cursor_col, cursor_row) = if prompt_focus {
        prompt_cursor_layout(
            app.pipeline_def.initial_prompt.as_str(),
            app.pipeline_prompt_cursor,
            inner.width as usize,
            inner.height as usize,
        )
    } else {
        (0, 0, 0)
    };

    let prompt_p = Paragraph::new(display_text)
        .style(text_style)
        .wrap(Wrap { trim: false })
        .scroll((scroll_y, 0));
    f.render_widget(prompt_block, cols[0]);
    f.render_widget(prompt_p, inner);

    let has_overlay =
        app.pipeline_show_edit || app.pipeline_file_dialog.is_some() || app.error_modal.is_some();
    if prompt_focus && !has_overlay && inner.width > 0 && inner.height > 0 {
        let visible_row = cursor_row.saturating_sub(scroll_y as usize);
        let x =
            inner.x + (cursor_col.min(inner.width.saturating_sub(1) as usize) as u16);
        let y = inner.y
            + (visible_row.min(inner.height.saturating_sub(1) as usize) as u16);
        f.set_cursor_position((x, y));
    }

    // Iterations field
    let iter_focus = app.pipeline_focus == PipelineFocus::Iterations;
    let iter_style = if iter_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let iter_block = Block::default()
        .title(" Iterations ")
        .borders(Borders::ALL)
        .border_style(iter_style);
    let iter_inner = iter_block.inner(cols[1]);
    f.render_widget(iter_block, cols[1]);
    let iter_text = Paragraph::new(app.pipeline_iterations_buf.as_str())
        .style(if iter_focus {
            Style::default().fg(Color::White)
        } else {
            Style::default()
        });
    f.render_widget(iter_text, iter_inner);
}

fn draw_canvas(f: &mut Frame, app: &App, area: Rect) {
    let builder_focus = app.pipeline_focus == PipelineFocus::Builder;
    let canvas_style = if builder_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let canvas_block = Block::default()
        .title(" Builder ")
        .borders(Borders::ALL)
        .border_style(canvas_style);
    let canvas_inner = canvas_block.inner(area);
    f.render_widget(canvas_block, area);

    if canvas_inner.width == 0 || canvas_inner.height == 0 {
        return;
    }

    if app.pipeline_def.blocks.is_empty() {
        let msg = Paragraph::new("Press 'a' to add a block")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(ratatui::layout::Alignment::Center);
        let y = canvas_inner.y + canvas_inner.height / 2;
        let hint_area = Rect::new(canvas_inner.x, y, canvas_inner.width, 1);
        f.render_widget(msg, hint_area);
        return;
    }

    let ox = app.pipeline_canvas_offset.0;
    let oy = app.pipeline_canvas_offset.1;

    // Draw blocks first
    for block in &app.pipeline_def.blocks {
        let sx = block.position.0 as i16 * CELL_W as i16 - ox;
        let sy = block.position.1 as i16 * CELL_H as i16 - oy;

        if sx + BLOCK_W as i16 <= 0 || sy + BLOCK_H as i16 <= 0 {
            continue;
        }
        let rx = canvas_inner.x as i16 + sx;
        let ry = canvas_inner.y as i16 + sy;
        if rx < canvas_inner.x as i16
            || ry < canvas_inner.y as i16
            || rx as u16 + BLOCK_W > canvas_inner.x + canvas_inner.width
            || ry as u16 + BLOCK_H > canvas_inner.y + canvas_inner.height
        {
            continue;
        }
        let block_area = Rect::new(rx as u16, ry as u16, BLOCK_W, BLOCK_H);

        let is_selected = app.pipeline_block_cursor == Some(block.id);
        let is_connect_src = app.pipeline_connecting_from == Some(block.id);

        let border_type = if is_selected {
            BorderType::Double
        } else {
            BorderType::Plain
        };
        let border_color = if is_connect_src {
            Color::Green
        } else if is_selected {
            Color::Yellow
        } else {
            Color::DarkGray
        };

        let title = if block.name.is_empty() {
            format!(" {} ", block.agent)
        } else {
            format!(" {} ", block.name)
        };
        let block_widget = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_type(border_type)
            .border_style(Style::default().fg(border_color));
        let inner = block_widget.inner(block_area);
        f.render_widget(block_widget, block_area);

        // Line 1: Agent name
        if inner.height > 0 && inner.width > 0 {
            let agent_display = truncate_chars(&block.agent, inner.width as usize);
            let agent_p = Paragraph::new(Span::styled(
                agent_display,
                Style::default().fg(Color::White),
            ));
            f.render_widget(agent_p, Rect::new(inner.x, inner.y, inner.width, 1));
        }

        // Line 2: Prompt status
        if inner.height > 1 && inner.width > 0 {
            let prompt_label = if block.prompt.is_empty() {
                "Prompt: no"
            } else {
                "Prompt: yes"
            };
            let prompt_p = Paragraph::new(Span::styled(
                prompt_label,
                Style::default().fg(Color::DarkGray),
            ));
            f.render_widget(prompt_p, Rect::new(inner.x, inner.y + 1, inner.width, 1));
        }

        // Line 3: Session ID (if present)
        if inner.height > 2 && inner.width > 0 {
            if let Some(ref sid) = block.session_id {
                let sess_display = truncate_chars(sid, inner.width.saturating_sub(6) as usize);
                let sess_p = Paragraph::new(Span::styled(
                    format!("sess: {sess_display}"),
                    Style::default().fg(Color::DarkGray),
                ));
                f.render_widget(
                    sess_p,
                    Rect::new(inner.x, inner.y + 2, inner.width, 1),
                );
            }
        }
    }

    // Draw connections AFTER blocks (on top)
    for (ci, conn) in app.pipeline_def.connections.iter().enumerate() {
        let from_block = app.pipeline_def.blocks.iter().find(|b| b.id == conn.from);
        let to_block = app.pipeline_def.blocks.iter().find(|b| b.id == conn.to);
        if let (Some(fb), Some(tb)) = (from_block, to_block) {
            let removing = app.pipeline_removing_conn
                && is_conn_for_selected(app, conn.from, conn.to);
            let highlighted = removing && ci == app.pipeline_conn_cursor;
            let conn_color = if highlighted {
                Color::Red
            } else if removing {
                Color::Yellow
            } else {
                Color::DarkGray
            };
            draw_connection(f, canvas_inner, fb.position, tb.position, ox, oy, conn_color);
        }
    }

    // Status line for connect/remove modes
    if app.pipeline_connecting_from.is_some() {
        let status = Paragraph::new("Select target block (Enter=connect, Esc=cancel)")
            .style(Style::default().fg(Color::Yellow));
        let sy = canvas_inner.y + canvas_inner.height.saturating_sub(1);
        f.render_widget(
            status,
            Rect::new(canvas_inner.x, sy, canvas_inner.width, 1),
        );
    } else if app.pipeline_removing_conn {
        let status =
            Paragraph::new("\u{2191}\u{2193} cycle connections, Enter=remove, Esc=cancel")
                .style(Style::default().fg(Color::Yellow));
        let sy = canvas_inner.y + canvas_inner.height.saturating_sub(1);
        f.render_widget(
            status,
            Rect::new(canvas_inner.x, sy, canvas_inner.width, 1),
        );
    }
}

fn is_conn_for_selected(app: &App, from: BlockId, to: BlockId) -> bool {
    if let Some(sel) = app.pipeline_block_cursor {
        from == sel || to == sel
    } else {
        false
    }
}

fn draw_connection(
    f: &mut Frame,
    canvas: Rect,
    from_pos: (u16, u16),
    to_pos: (u16, u16),
    ox: i16,
    oy: i16,
    color: Color,
) {
    // Source: right-center of from-block
    let fx = from_pos.0 as i16 * CELL_W as i16 + BLOCK_W as i16 - ox;
    let fy = from_pos.1 as i16 * CELL_H as i16 + (BLOCK_H as i16 / 2) - oy;
    // Target: left-center of to-block
    let tx = to_pos.0 as i16 * CELL_W as i16 - ox;
    let ty = to_pos.1 as i16 * CELL_H as i16 + (BLOCK_H as i16 / 2) - oy;

    let style = Style::default().fg(color);

    // Simple horizontal line when same row
    if fy == ty && fx < tx {
        for x in fx..tx {
            put_char(f, canvas, x, fy, '\u{2500}', style); // ─
        }
        put_char(f, canvas, tx, ty, '\u{25b6}', style); // ▶
    } else if fx < tx {
        // Route: right from source, then down/up, then right to target
        let mid_x = fx + (tx - fx) / 2;
        // Horizontal from source
        for x in fx..mid_x {
            put_char(f, canvas, x, fy, '\u{2500}', style);
        }
        // Corner
        if ty > fy {
            put_char(f, canvas, mid_x, fy, '\u{2510}', style); // ┐
            for y in (fy + 1)..ty {
                put_char(f, canvas, mid_x, y, '\u{2502}', style); // │
            }
            put_char(f, canvas, mid_x, ty, '\u{2514}', style); // └
        } else {
            put_char(f, canvas, mid_x, fy, '\u{2518}', style); // ┘
            for y in (ty + 1)..fy {
                put_char(f, canvas, mid_x, y, '\u{2502}', style);
            }
            put_char(f, canvas, mid_x, ty, '\u{250c}', style); // ┌
        }
        // Horizontal to target
        for x in (mid_x + 1)..tx {
            put_char(f, canvas, x, ty, '\u{2500}', style);
        }
        put_char(f, canvas, tx, ty, '\u{25b6}', style);
    }
}

fn put_char(f: &mut Frame, canvas: Rect, x: i16, y: i16, ch: char, style: Style) {
    let ax = canvas.x as i16 + x;
    let ay = canvas.y as i16 + y;
    if ax >= canvas.x as i16
        && ay >= canvas.y as i16
        && (ax as u16) < canvas.x + canvas.width
        && (ay as u16) < canvas.y + canvas.height
    {
        let buf = f.buffer_mut();
        buf[(ax as u16, ay as u16)].set_char(ch).set_style(style);
    }
}

fn draw_help_bar(f: &mut Frame, app: &App, area: Rect) {
    let help = if app.pipeline_connecting_from.is_some() {
        "Arrows: navigate | Enter: connect | Esc: cancel"
    } else if app.pipeline_removing_conn {
        "j/k: cycle | Enter: remove | Esc: cancel"
    } else {
        "Tab: cycle focus | a: add | d: delete | e: edit | c: connect | x: disconnect | Ctrl+S: save | Ctrl+L: load | F5: run | ?: help | Esc: back"
    };
    let help_p = Paragraph::new(help)
        .style(Style::default().fg(Color::DarkGray))
        .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(help_p, area);
}

fn draw_edit_popup(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(60, 55, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .title(" Edit Block ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(popup);
    f.render_widget(block, popup);

    if inner.width < 10 || inner.height < 10 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Name
            Constraint::Length(1), // spacer
            Constraint::Length(2), // Provider
            Constraint::Length(1), // spacer
            Constraint::Min(6),   // Prompt
            Constraint::Length(1), // spacer
            Constraint::Length(2), // Session ID
            Constraint::Length(1), // spacer
            Constraint::Length(1), // hint
        ])
        .split(inner);

    // Name field
    let name_focus = app.pipeline_edit_field == PipelineEditField::Name;
    let name_style = if name_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let name_line = Line::from(vec![
        Span::styled("Name: ", Style::default().fg(Color::White)),
        Span::styled("[", name_style),
        Span::raw(&app.pipeline_edit_name_buf),
        Span::styled("]", name_style),
    ]);
    f.render_widget(Paragraph::new(name_line), chunks[0]);

    // Agent selector
    let agent_focus = app.pipeline_edit_field == PipelineEditField::Agent;
    let agent_name = app.config.agents
        .get(app.pipeline_edit_agent_idx)
        .map(|a| a.name.as_str())
        .unwrap_or("(none)");
    let avail_agents: std::collections::HashMap<&str, bool> =
        app.available_agents().into_iter().map(|(a, avail)| (a.name.as_str(), avail)).collect();
    let is_avail = avail_agents.get(agent_name).copied().unwrap_or(false);
    let agent_color = if is_avail { Color::Green } else { Color::Red };
    let arrow_style = Style::default().fg(if agent_focus {
        Color::White
    } else {
        Color::DarkGray
    });
    let agent_line = Line::from(vec![
        Span::styled("Agent: ", Style::default().fg(Color::White)),
        Span::styled("\u{25c4} ", arrow_style),
        Span::styled(
            agent_name,
            Style::default().fg(agent_color).add_modifier(Modifier::BOLD),
        ),
        Span::styled(" \u{25ba}", arrow_style),
    ]);
    let agent_p = Paragraph::new(agent_line);
    f.render_widget(agent_p, chunks[2]);

    // Prompt textarea
    let prompt_focus = app.pipeline_edit_field == PipelineEditField::Prompt;
    let prompt_style = if prompt_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let prompt_block = Block::default()
        .title(" Prompt ")
        .borders(Borders::ALL)
        .border_style(prompt_style);
    let prompt_inner = prompt_block.inner(chunks[4]);
    f.render_widget(prompt_block, chunks[4]);
    let prompt_p = Paragraph::new(app.pipeline_edit_prompt_buf.as_str()).wrap(Wrap { trim: false });
    f.render_widget(prompt_p, prompt_inner);

    // Session ID
    let sess_focus = app.pipeline_edit_field == PipelineEditField::SessionId;
    let sess_style = if sess_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let sess_line = Line::from(vec![
        Span::styled("Session ID: ", Style::default().fg(Color::White)),
        Span::styled("[", sess_style),
        Span::raw(&app.pipeline_edit_session_buf),
        Span::styled("]", sess_style),
    ]);
    f.render_widget(Paragraph::new(sess_line), chunks[6]);

    // Hint
    let hint = Paragraph::new("  Tab: next  Enter: save  Esc: back")
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(hint, chunks[8]);
}

fn draw_file_dialog(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(50, 40, area);
    f.render_widget(Clear, popup);

    match app.pipeline_file_dialog {
        Some(PipelineDialogMode::Save) => {
            let block = Block::default()
                .title(" Save Pipeline ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan));
            let inner = block.inner(popup);
            f.render_widget(block, popup);

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(2),
                    Constraint::Length(1),
                    Constraint::Length(1),
                ])
                .split(inner);

            let input_line = Line::from(vec![
                Span::styled("Filename: ", Style::default().fg(Color::White)),
                Span::raw(&app.pipeline_file_input),
                Span::styled(".toml", Style::default().fg(Color::DarkGray)),
            ]);
            f.render_widget(Paragraph::new(input_line), chunks[0]);
            f.render_widget(
                Paragraph::new("Enter: save  Esc: cancel")
                    .style(Style::default().fg(Color::DarkGray)),
                chunks[2],
            );
        }
        Some(PipelineDialogMode::Load) => {
            let block = Block::default()
                .title(" Load Pipeline ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan));
            let inner = block.inner(popup);
            f.render_widget(block, popup);

            if app.pipeline_file_list.is_empty() {
                let msg = Paragraph::new("No pipeline files found")
                    .style(Style::default().fg(Color::DarkGray));
                f.render_widget(msg, inner);
            } else {
                let items: Vec<Line> = app
                    .pipeline_file_list
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let style = if i == app.pipeline_file_cursor {
                            Style::default()
                                .fg(Color::Cyan)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default()
                        };
                        Line::from(Span::styled(name.as_str(), style))
                    })
                    .collect();
                let list = Paragraph::new(items);
                f.render_widget(list, inner);
            }
        }
        None => {}
    }
}

fn draw_error_modal(f: &mut Frame, message: &str) {
    let area = centered_rect(60, 20, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .title(" Error ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Red));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let msg = Paragraph::new(message)
        .style(Style::default().fg(Color::Red))
        .wrap(Wrap { trim: false });
    f.render_widget(msg, inner);
}
