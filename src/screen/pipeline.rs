use crate::app::{App, PipelineDialogMode, PipelineEditField, PipelineFocus, PipelineLoopEditField};
use crate::execution::pipeline::BlockId;
use crate::execution::truncate_chars;
use crate::screen::centered_rect;
use crate::screen::help;
use crate::screen::prompt::{char_wrap_text, prompt_cursor_layout};

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

pub(crate) const BLOCK_W: u16 = 18;
pub(crate) const BLOCK_H: u16 = 5;
pub(crate) const CELL_W: u16 = 24; // block width + 6 gap (3 vertical wire lanes)
pub(crate) const CELL_H: u16 = 8; // block height + 3 gap (2 horizontal wire lanes)

use std::collections::{HashMap, HashSet};

/// Orthogonal wire segment in world pixel coords.
struct WireSeg {
    x1: i16,
    y1: i16,
    x2: i16,
    y2: i16,
}

/// Direction bitmask constants for glyph merging.
const DIR_N: u8 = 1;
const DIR_E: u8 = 2;
const DIR_S: u8 = 4;
const DIR_W: u8 = 8;

/// Accumulated wire state at one pixel.
struct WireCell {
    dirs: u8,
    color: Color,
    is_arrow: bool,
    arrow_char: char,
    is_loop: bool,
}

type WirePoint = (i16, i16);
type ConnectionRaster = HashMap<WirePoint, WireCell>;

/// Deterministic color palette for normal (non-highlighted) wires.
const WIRE_PALETTE: [Color; 6] = [
    Color::Blue,
    Color::Green,
    Color::Magenta,
    Color::Cyan,
    Color::LightBlue,
    Color::LightGreen,
];

/// Choose arrow character based on the direction of a wire segment's endpoint.
fn arrow_for_seg(seg: &WireSeg) -> char {
    let dx = seg.x2 - seg.x1;
    let dy = seg.y2 - seg.y1;
    if dx > 0 {
        '▶'
    } else if dx < 0 {
        '◀'
    } else if dy > 0 {
        '▼'
    } else if dy < 0 {
        '▲'
    } else {
        '▶'
    }
}

pub fn draw(f: &mut Frame, app: &App) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // title
            Constraint::Length(12), // prompt + session/iterations/runs/concurrency
            Constraint::Min(0),     // builder canvas
            Constraint::Length(2),  // help + status
        ])
        .split(area);

    draw_title(f, chunks[0]);
    draw_prompt_area(f, app, chunks[1]);
    draw_canvas(f, app, chunks[2]);
    draw_help_bar(f, app, chunks[3]);

    // Overlays
    if app.pipeline.pipeline_show_edit {
        draw_edit_popup(f, app, area);
    }
    if app.pipeline.pipeline_file_dialog.is_some() {
        draw_file_dialog(f, app, area);
    }
    if app.pipeline.pipeline_show_session_config {
        draw_session_config_popup(f, app, area);
    }
    if app.pipeline.pipeline_show_loop_edit {
        draw_loop_edit_popup(f, app, area);
    }
    if app.help_popup.active {
        let tab = app.help_popup.tab;
        let name = help::PIPELINE_TAB_NAMES[tab];
        let title = format!(
            " Pipeline Builder — {} ({}/{}) — Tab: next section ",
            name,
            tab + 1,
            help::PIPELINE_TAB_COUNT,
        );
        help::draw_help_overlay(f, &app.help_popup, help::pipeline_help_lines(tab), &title);
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
        .border_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(block, area);
}

fn draw_prompt_area(f: &mut Frame, app: &App, area: Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
        .split(area);

    // Initial prompt textarea
    let prompt_focus = app.pipeline.pipeline_focus == PipelineFocus::InitialPrompt;
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

    let display_text = if app.pipeline.pipeline_def.initial_prompt.is_empty() && !prompt_focus {
        "Enter initial prompt..."
    } else {
        app.pipeline.pipeline_def.initial_prompt.as_str()
    };

    let text_style = if app.pipeline.pipeline_def.initial_prompt.is_empty() && !prompt_focus {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    let (scroll_y, cursor_col, cursor_row) = if prompt_focus {
        prompt_cursor_layout(
            app.pipeline.pipeline_def.initial_prompt.as_str(),
            app.pipeline.pipeline_prompt_cursor,
            inner.width as usize,
            inner.height as usize,
        )
    } else {
        (0, 0, 0)
    };

    let wrapped = char_wrap_text(display_text, inner.width as usize);
    let prompt_p = Paragraph::new(wrapped.as_str())
        .style(text_style)
        .scroll((scroll_y, 0));
    f.render_widget(prompt_block, cols[0]);
    f.render_widget(prompt_p, inner);

    let has_overlay = app.pipeline.pipeline_show_edit
        || app.pipeline.pipeline_file_dialog.is_some()
        || app.pipeline.pipeline_show_session_config
        || app.pipeline.pipeline_show_loop_edit
        || app.error_modal.is_some()
        || app.help_popup.active;
    if prompt_focus && !has_overlay && inner.width > 0 && inner.height > 0 {
        let visible_row = cursor_row.saturating_sub(scroll_y as usize);
        let x = inner.x + (cursor_col.min(inner.width.saturating_sub(1) as usize) as u16);
        let y = inner.y + (visible_row.min(inner.height.saturating_sub(1) as usize) as u16);
        f.set_cursor_position((x, y));
    }

    // Right column: session name + iterations + runs + concurrency
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(cols[1]);

    // Session name field
    let name_focus = app.pipeline.pipeline_focus == PipelineFocus::SessionName;
    let name_border = if name_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let name_display = if app.pipeline.pipeline_session_name.is_empty() {
        if name_focus {
            "_".to_string()
        } else {
            "(optional)".to_string()
        }
    } else if name_focus {
        format!("{}_", app.pipeline.pipeline_session_name)
    } else {
        app.pipeline.pipeline_session_name.clone()
    };
    let name_style = if app.pipeline.pipeline_session_name.is_empty() && !name_focus {
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
    f.render_widget(session_name, right_chunks[0]);

    // Iterations field
    let iter_focus = app.pipeline.pipeline_focus == PipelineFocus::Iterations;
    let iter_border = if iter_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let iter_display = if iter_focus {
        format!("{}_", app.pipeline.pipeline_iterations_buf)
    } else {
        app.pipeline.pipeline_iterations_buf.clone()
    };
    let iter_widget = Paragraph::new(iter_display).block(
        Block::default()
            .title(" Iterations ")
            .borders(Borders::ALL)
            .border_style(iter_border),
    );
    f.render_widget(iter_widget, right_chunks[1]);

    let runs_focus = app.pipeline.pipeline_focus == PipelineFocus::Runs;
    let runs_border = if runs_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let runs_display = if runs_focus {
        format!("{}_", app.pipeline.pipeline_runs_buf)
    } else {
        app.pipeline.pipeline_runs.to_string()
    };
    let runs_widget = Paragraph::new(runs_display).block(
        Block::default()
            .title(" Runs ")
            .borders(Borders::ALL)
            .border_style(runs_border),
    );
    f.render_widget(runs_widget, right_chunks[2]);

    let concurrency_focus = app.pipeline.pipeline_focus == PipelineFocus::Concurrency;
    let concurrency_border = if concurrency_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let concurrency_display = if concurrency_focus {
        format!("{}_", app.pipeline.pipeline_concurrency_buf)
    } else if app.pipeline.pipeline_concurrency == 0 {
        "0 (unlimited)".to_string()
    } else {
        app.pipeline.pipeline_concurrency.to_string()
    };
    let concurrency_widget = Paragraph::new(concurrency_display).block(
        Block::default()
            .title(" Concurrency ")
            .borders(Borders::ALL)
            .border_style(concurrency_border),
    );
    f.render_widget(concurrency_widget, right_chunks[3]);
}

fn draw_canvas(f: &mut Frame, app: &App, area: Rect) {
    let builder_focus = app.pipeline.pipeline_focus == PipelineFocus::Builder;
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

    if app.pipeline.pipeline_def.blocks.is_empty() {
        let msg = Paragraph::new("Press 'a' to add a block")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(ratatui::layout::Alignment::Center);
        let y = canvas_inner.y + canvas_inner.height / 2;
        let hint_area = Rect::new(canvas_inner.x, y, canvas_inner.width, 1);
        f.render_widget(msg, hint_area);
        return;
    }

    let ox = app.pipeline.pipeline_canvas_offset.0;
    let oy = app.pipeline.pipeline_canvas_offset.1;

    // Draw blocks first
    for block in &app.pipeline.pipeline_def.blocks {
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

        let is_selected = app.pipeline.pipeline_block_cursor == Some(block.id);
        let is_connect_src = app.pipeline.pipeline_connecting_from == Some(block.id);
        let is_loop_connect_src = app.pipeline.pipeline_loop_connecting_from == Some(block.id);

        let border_type = if is_selected {
            BorderType::Double
        } else {
            BorderType::Plain
        };
        let border_color = if is_connect_src {
            Color::Green
        } else if is_loop_connect_src || is_selected {
            Color::Yellow
        } else {
            Color::DarkGray
        };

        let base_title = if block.name.is_empty() {
            block.agent.clone()
        } else {
            block.name.clone()
        };
        let title = if block.replicas > 1 {
            format!(" {} \u{00d7}{} ", base_title, block.replicas)
        } else {
            format!(" {} ", base_title)
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
                f.render_widget(sess_p, Rect::new(inner.x, inner.y + 2, inner.width, 1));
            }
        }
    }

    // ── Connection rendering (two-phase: route then paint) ──
    let grid_occ = grid_occupancy(&app.pipeline.pipeline_def.blocks);
    let lanes = assign_lanes(
        &app.pipeline.pipeline_def.connections,
        &app.pipeline.pipeline_def.blocks,
    );
    let ports = assign_ports(
        &app.pipeline.pipeline_def.connections,
        &app.pipeline.pipeline_def.blocks,
    );

    // Resolve pipeline_conn_cursor (index into filtered subset) to global connection index
    let highlighted_global_idx = if app.pipeline.pipeline_removing_conn {
        let sel = app.pipeline.pipeline_block_cursor.unwrap_or(0);
        app.pipeline
            .pipeline_def
            .connections
            .iter()
            .enumerate()
            .filter(|(_, c)| c.from == sel || c.to == sel)
            .nth(app.pipeline.pipeline_conn_cursor)
            .map(|(i, _)| i)
    } else {
        None
    };

    let mut rendered_connections: Vec<(u8, ConnectionRaster)> = Vec::new();

    for (ci, conn) in app.pipeline.pipeline_def.connections.iter().enumerate() {
        let fb = app
            .pipeline
            .pipeline_def
            .blocks
            .iter()
            .find(|b| b.id == conn.from);
        let tb = app
            .pipeline
            .pipeline_def
            .blocks
            .iter()
            .find(|b| b.id == conn.to);
        let (Some(fb), Some(tb)) = (fb, tb) else {
            continue;
        };

        let removing =
            app.pipeline.pipeline_removing_conn && is_conn_for_selected(app, conn.from, conn.to);
        let highlighted = highlighted_global_idx == Some(ci);
        let color = if highlighted {
            Color::Red
        } else if removing {
            Color::Yellow
        } else {
            WIRE_PALETTE[ci % WIRE_PALETTE.len()]
        };

        let (exit_y_off, entry_y_off) = ports[ci];
        let segs = route_wire(
            fb.position,
            tb.position,
            &grid_occ,
            lanes[ci],
            exit_y_off,
            entry_y_off,
        );
        let mut conn_map: ConnectionRaster = HashMap::new();
        for seg in &segs {
            rasterize_seg(seg, color, &mut conn_map);
        }
        if let Some(last) = segs.last() {
            let arrow_ch = arrow_for_seg(last);
            if let Some(cell) = conn_map.get_mut(&(last.x2, last.y2)) {
                cell.is_arrow = true;
                cell.arrow_char = arrow_ch;
            } else {
                conn_map.insert(
                    (last.x2, last.y2),
                    WireCell {
                        dirs: 0,
                        color,
                        is_arrow: true,
                        arrow_char: arrow_ch,
                        is_loop: false,
                    },
                );
            }
        }
        rendered_connections.push((color_rank(color), conn_map));
    }

    // Draw lower-priority wires first so highlighted wires remain visible.
    rendered_connections.sort_by_key(|(rank, _)| *rank);

    // Paint each connection independently; do not merge glyphs across connections.
    for (_, conn_map) in rendered_connections {
        for (&(wx, wy), cell) in &conn_map {
            if pixel_hits_block(wx, wy, &app.pipeline.pipeline_def.blocks) {
                continue;
            }
            let ch = if cell.is_arrow {
                cell.arrow_char
            } else if cell.is_loop {
                dirs_to_double_char(cell.dirs)
            } else {
                dirs_to_char(cell.dirs)
            };
            put_char(
                f,
                canvas_inner,
                wx - ox,
                wy - oy,
                ch,
                Style::default().fg(cell.color),
            );
        }
    }

    // ── Loop connection rendering ──
    {
        let loop_conns_as_regular: Vec<crate::execution::pipeline::PipelineConnection> =
            app.pipeline.pipeline_def.loop_connections.iter()
                .map(|lc| crate::execution::pipeline::PipelineConnection { from: lc.from, to: lc.to })
                .collect();
        if !loop_conns_as_regular.is_empty() {
            let loop_lanes = assign_lanes(&loop_conns_as_regular, &app.pipeline.pipeline_def.blocks);
            let loop_ports = assign_ports(&loop_conns_as_regular, &app.pipeline.pipeline_def.blocks);

            for (ci, lc) in app.pipeline.pipeline_def.loop_connections.iter().enumerate() {
                let fb = app.pipeline.pipeline_def.blocks.iter().find(|b| b.id == lc.from);
                let tb = app.pipeline.pipeline_def.blocks.iter().find(|b| b.id == lc.to);
                let (Some(fb), Some(tb)) = (fb, tb) else { continue; };

                let _removing = app.pipeline.pipeline_removing_conn
                    && is_conn_for_selected(app, lc.from, lc.to);
                let highlighted = if app.pipeline.pipeline_removing_conn {
                    let sel = app.pipeline.pipeline_block_cursor.unwrap_or(0);
                    let regular_count = app.pipeline.pipeline_def.connections.iter()
                        .filter(|c| c.from == sel || c.to == sel).count();
                    let loop_offset = app.pipeline.pipeline_def.loop_connections.iter()
                        .enumerate()
                        .filter(|(_, l)| l.from == sel || l.to == sel)
                        .position(|(i, _)| i == ci);
                    loop_offset.map(|off| app.pipeline.pipeline_conn_cursor == regular_count + off)
                        .unwrap_or(false)
                } else {
                    false
                };
                let color = if highlighted {
                    Color::Red
                } else {
                    // Loop wires are always yellow (both when removing and normally)
                    Color::Yellow
                };

                let (exit_y_off, entry_y_off) = loop_ports[ci];
                let segs = route_wire(
                    fb.position, tb.position, &grid_occ,
                    loop_lanes[ci], exit_y_off, entry_y_off,
                );
                let mut conn_map: ConnectionRaster = HashMap::new();
                for seg in &segs {
                    rasterize_seg(seg, color, &mut conn_map);
                }
                // Mark all cells as loop
                for cell in conn_map.values_mut() {
                    cell.is_loop = true;
                }
                // Arrow
                if let Some(last) = segs.last() {
                    let arrow_ch = arrow_for_seg(last);
                    if let Some(cell) = conn_map.get_mut(&(last.x2, last.y2)) {
                        cell.is_arrow = true;
                        cell.arrow_char = arrow_ch;
                    } else {
                        conn_map.insert(
                            (last.x2, last.y2),
                            WireCell { dirs: 0, color, is_arrow: true, arrow_char: arrow_ch, is_loop: true },
                        );
                    }
                }

                // Count label at midpoint of longest horizontal segment
                let mut best_h: Option<(i16, i16, i16)> = None; // (y, x_start, x_end)
                for seg in &segs {
                    if seg.y1 == seg.y2 {
                        let len = (seg.x2 - seg.x1).abs();
                        if best_h.is_none() || len > (best_h.unwrap().2 - best_h.unwrap().1).abs() {
                            let (lo, hi) = if seg.x1 <= seg.x2 { (seg.x1, seg.x2) } else { (seg.x2, seg.x1) };
                            best_h = Some((seg.y1, lo, hi));
                        }
                    }
                }
                let label = format!("\u{00d7}{}", lc.count);

                // Paint the wire
                for (&(wx, wy), cell) in &conn_map {
                    if pixel_hits_block(wx, wy, &app.pipeline.pipeline_def.blocks) {
                        continue;
                    }
                    let ch = if cell.is_arrow {
                        cell.arrow_char
                    } else {
                        dirs_to_double_char(cell.dirs)
                    };
                    put_char(f, canvas_inner, wx - ox, wy - oy, ch,
                        Style::default().fg(cell.color));
                }

                // Paint the label
                if let Some((ly, lx_start, lx_end)) = best_h {
                    let mid_x = (lx_start + lx_end) / 2;
                    let label_style = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
                    for (i, ch) in label.chars().enumerate() {
                        put_char(f, canvas_inner, mid_x + i as i16 - ox, ly - oy, ch, label_style);
                    }
                }
            }
        }
    }

    // Status line for connect/remove modes
    if app.pipeline.pipeline_loop_connecting_from.is_some() {
        let status = Paragraph::new("Select target block (Enter=loop, Esc=cancel)")
            .style(Style::default().fg(Color::Yellow));
        let sy = canvas_inner.y + canvas_inner.height.saturating_sub(1);
        f.render_widget(status, Rect::new(canvas_inner.x, sy, canvas_inner.width, 1));
    } else if app.pipeline.pipeline_connecting_from.is_some() {
        let status = Paragraph::new("Select target block (Enter=connect, Esc=cancel)")
            .style(Style::default().fg(Color::Yellow));
        let sy = canvas_inner.y + canvas_inner.height.saturating_sub(1);
        f.render_widget(status, Rect::new(canvas_inner.x, sy, canvas_inner.width, 1));
    } else if app.pipeline.pipeline_removing_conn {
        let status = Paragraph::new("\u{2191}\u{2193} cycle connections, Enter=remove, Esc=cancel")
            .style(Style::default().fg(Color::Yellow));
        let sy = canvas_inner.y + canvas_inner.height.saturating_sub(1);
        f.render_widget(status, Rect::new(canvas_inner.x, sy, canvas_inner.width, 1));
    }
}

fn is_conn_for_selected(app: &App, from: BlockId, to: BlockId) -> bool {
    if let Some(sel) = app.pipeline.pipeline_block_cursor {
        from == sel || to == sel
    } else {
        false
    }
}

// ── Routing infrastructure ──

fn grid_occupancy(blocks: &[crate::execution::pipeline::PipelineBlock]) -> HashSet<(u16, u16)> {
    blocks.iter().map(|b| b.position).collect()
}

fn pixel_hits_block(
    wx: i16,
    wy: i16,
    blocks: &[crate::execution::pipeline::PipelineBlock],
) -> bool {
    blocks.iter().any(|b| {
        let bx = b.position.0 as i16 * CELL_W as i16;
        let by = b.position.1 as i16 * CELL_H as i16;
        wx >= bx && wx < bx + BLOCK_W as i16 && wy >= by && wy < by + BLOCK_H as i16
    })
}

fn assign_lanes(
    conns: &[crate::execution::pipeline::PipelineConnection],
    blocks: &[crate::execution::pipeline::PipelineBlock],
) -> Vec<usize> {
    let bmap: HashMap<BlockId, (u16, u16)> = blocks.iter().map(|b| (b.id, b.position)).collect();

    let mut order: Vec<usize> = (0..conns.len()).collect();
    order.sort_by(|&a, &b| {
        let (ca, cb) = (&conns[a], &conns[b]);
        let fa = bmap.get(&ca.from).copied().unwrap_or((0, 0));
        let ta = bmap.get(&ca.to).copied().unwrap_or((0, 0));
        let fb = bmap.get(&cb.from).copied().unwrap_or((0, 0));
        let tb = bmap.get(&cb.to).copied().unwrap_or((0, 0));
        (fa.0, ta.0, fa.1, ta.1, ca.from, ca.to).cmp(&(fb.0, tb.0, fb.1, tb.1, cb.from, cb.to))
    });

    let mut lanes = vec![0usize; conns.len()];
    for (lane, &ci) in order.iter().enumerate() {
        lanes[ci] = lane;
    }
    lanes
}

/// Assign per-connection exit/entry Y offsets so fan-in/fan-out wires don't stack.
fn assign_ports(
    conns: &[crate::execution::pipeline::PipelineConnection],
    blocks: &[crate::execution::pipeline::PipelineBlock],
) -> Vec<(i16, i16)> {
    let bmap: HashMap<BlockId, (u16, u16)> = blocks.iter().map(|b| (b.id, b.position)).collect();

    let mut outgoing: HashMap<BlockId, Vec<usize>> = HashMap::new();
    let mut incoming: HashMap<BlockId, Vec<usize>> = HashMap::new();
    for (ci, conn) in conns.iter().enumerate() {
        outgoing.entry(conn.from).or_default().push(ci);
        incoming.entry(conn.to).or_default().push(ci);
    }

    // Linear offset sequence centered at 0 so ports follow spatial order:
    // n=1: [0], n=2: [-1, 0], n=3: [-1, 0, 1], n=4: [-2, -1, 0, 1]
    let offset_seq = |n: usize| -> Vec<i16> {
        let half = n as i16 / 2;
        (0..n as i16).map(|i| i - half).collect()
    };

    let max_off = (BLOCK_H as i16 / 2).saturating_sub(1).max(1);

    let mut ports = vec![(0i16, 0i16); conns.len()];

    for group in outgoing.values_mut() {
        group.sort_by(|&a, &b| {
            let ta = bmap.get(&conns[a].to).copied().unwrap_or((0, 0));
            let tb = bmap.get(&conns[b].to).copied().unwrap_or((0, 0));
            (ta.1, ta.0, a).cmp(&(tb.1, tb.0, b))
        });
        let offsets = offset_seq(group.len());
        for (i, &ci) in group.iter().enumerate() {
            ports[ci].0 = offsets[i].clamp(-max_off, max_off);
        }
    }

    for group in incoming.values_mut() {
        group.sort_by(|&a, &b| {
            let fa = bmap.get(&conns[a].from).copied().unwrap_or((0, 0));
            let fb = bmap.get(&conns[b].from).copied().unwrap_or((0, 0));
            (fa.1, fa.0, a).cmp(&(fb.1, fb.0, b))
        });
        let offsets = offset_seq(group.len());
        for (i, &ci) in group.iter().enumerate() {
            ports[ci].1 = offsets[i].clamp(-max_off, max_off);
        }
    }

    ports
}

fn route_wire(
    from_grid: (u16, u16),
    to_grid: (u16, u16),
    grid_occ: &HashSet<(u16, u16)>,
    lane: usize,
    exit_y_off: i16,
    entry_y_off: i16,
) -> Vec<WireSeg> {
    let (fc, fr) = from_grid;
    let (tc, tr) = to_grid;

    let ex = fc as i16 * CELL_W as i16 + BLOCK_W as i16;
    let ey = fr as i16 * CELL_H as i16 + BLOCK_H as i16 / 2 + exit_y_off;
    let nx = tc as i16 * CELL_W as i16 - 1;
    let ny = tr as i16 * CELL_H as i16 + BLOCK_H as i16 / 2 + entry_y_off;

    // Case A: Forward same row with clear corridor
    if fc < tc && fr == tr {
        let corridor_clear = (fc + 1..tc).all(|c| !grid_occ.contains(&(c, fr)));
        if corridor_clear {
            if ey == ny {
                return vec![WireSeg {
                    x1: ex,
                    y1: ey,
                    x2: nx,
                    y2: ny,
                }];
            }
            let mid_x = (ex + nx) / 2;
            return vec![
                WireSeg {
                    x1: ex,
                    y1: ey,
                    x2: mid_x,
                    y2: ey,
                },
                WireSeg {
                    x1: mid_x,
                    y1: ey,
                    x2: mid_x,
                    y2: ny,
                },
                WireSeg {
                    x1: mid_x,
                    y1: ny,
                    x2: nx,
                    y2: ny,
                },
            ];
        }
    }

    // ── shared lane offsets for non-straight routes ──
    let gap = (CELL_W as i16 - BLOCK_W as i16).max(2);
    let side_lane = ((lane % gap as usize) as i16) * 2 + 1;
    let side_clamp = gap - 1;

    // Case B: Forward different row — prefer a direct 3-segment L-path
    //   Exit-L:  exit → short right into source-column gap → vertical to target row → horizontal to entry
    //   Entry-L: exit → horizontal on source row → vertical in target-column gap → short right to entry
    if fc < tc && fr != tr {
        let target_row_clear = (fc + 1..tc).all(|c| !grid_occ.contains(&(c, tr)));
        if target_row_clear {
            let v1 = ex + side_lane.min(side_clamp);
            return vec![
                WireSeg { x1: ex, y1: ey, x2: v1, y2: ey },
                WireSeg { x1: v1, y1: ey, x2: v1, y2: ny },
                WireSeg { x1: v1, y1: ny, x2: nx, y2: ny },
            ];
        }
        let source_row_clear = (fc + 1..tc).all(|c| !grid_occ.contains(&(c, fr)));
        if source_row_clear {
            let v2 = nx - side_lane.min(side_clamp);
            return vec![
                WireSeg { x1: ex, y1: ey, x2: v2, y2: ey },
                WireSeg { x1: v2, y1: ey, x2: v2, y2: ny },
                WireSeg { x1: v2, y1: ny, x2: nx, y2: ny },
            ];
        }
    }

    // Case C: Backward, same column, or fully blocked forward — 5-segment U-route.
    // Forward: route through the inter-row gap nearest to the higher row (between rows).
    // Backward/same-col: route below both rows to avoid crossing forward wires.
    let hy = if fc < tc {
        fr.min(tr) as i16 * CELL_H as i16 + BLOCK_H as i16 + 1
    } else {
        fr.max(tr) as i16 * CELL_H as i16 + BLOCK_H as i16 + 1
    };
    let v1 = ex + side_lane.min(side_clamp);
    let v2 = nx - side_lane.min(side_clamp);

    vec![
        WireSeg {
            x1: ex,
            y1: ey,
            x2: v1,
            y2: ey,
        },
        WireSeg {
            x1: v1,
            y1: ey,
            x2: v1,
            y2: hy,
        },
        WireSeg {
            x1: v1,
            y1: hy,
            x2: v2,
            y2: hy,
        },
        WireSeg {
            x1: v2,
            y1: hy,
            x2: v2,
            y2: ny,
        },
        WireSeg {
            x1: v2,
            y1: ny,
            x2: nx,
            y2: ny,
        },
    ]
}

fn color_rank(c: Color) -> u8 {
    match c {
        Color::Red => 3,
        Color::Yellow => 2,
        _ => 1,
    }
}

fn rasterize_seg(seg: &WireSeg, color: Color, map: &mut HashMap<(i16, i16), WireCell>) {
    if seg.y1 == seg.y2 {
        let y = seg.y1;
        let (lo, hi) = if seg.x1 <= seg.x2 {
            (seg.x1, seg.x2)
        } else {
            (seg.x2, seg.x1)
        };
        for x in lo..=hi {
            let c = map.entry((x, y)).or_insert(WireCell {
                dirs: 0,
                color,
                is_arrow: false,
                arrow_char: ' ',
                is_loop: false,
            });
            if x > lo {
                c.dirs |= DIR_W;
            }
            if x < hi {
                c.dirs |= DIR_E;
            }
            if color_rank(color) > color_rank(c.color) {
                c.color = color;
            }
        }
    } else {
        let x = seg.x1;
        let (lo, hi) = if seg.y1 <= seg.y2 {
            (seg.y1, seg.y2)
        } else {
            (seg.y2, seg.y1)
        };
        for y in lo..=hi {
            let c = map.entry((x, y)).or_insert(WireCell {
                dirs: 0,
                color,
                is_arrow: false,
                arrow_char: ' ',
                is_loop: false,
            });
            if y > lo {
                c.dirs |= DIR_N;
            }
            if y < hi {
                c.dirs |= DIR_S;
            }
            if color_rank(color) > color_rank(c.color) {
                c.color = color;
            }
        }
    }
}

fn dirs_to_char(d: u8) -> char {
    match d {
        d if d == DIR_E | DIR_W => '─',
        d if d == DIR_N | DIR_S => '│',
        d if d == DIR_S | DIR_E => '┌',
        d if d == DIR_S | DIR_W => '┐',
        d if d == DIR_N | DIR_E => '└',
        d if d == DIR_N | DIR_W => '┘',
        d if d == DIR_E | DIR_W | DIR_S => '┬',
        d if d == DIR_E | DIR_W | DIR_N => '┴',
        d if d == DIR_N | DIR_S | DIR_E => '├',
        d if d == DIR_N | DIR_S | DIR_W => '┤',
        d if d == DIR_N | DIR_S | DIR_E | DIR_W => '┼',
        d if d & (DIR_E | DIR_W) != 0 => '─',
        d if d & (DIR_N | DIR_S) != 0 => '│',
        _ => '·',
    }
}

fn dirs_to_double_char(d: u8) -> char {
    match d {
        d if d == DIR_E | DIR_W => '═',
        d if d == DIR_N | DIR_S => '║',
        d if d == DIR_S | DIR_E => '╔',
        d if d == DIR_S | DIR_W => '╗',
        d if d == DIR_N | DIR_E => '╚',
        d if d == DIR_N | DIR_W => '╝',
        d if d == DIR_E | DIR_W | DIR_S => '╦',
        d if d == DIR_E | DIR_W | DIR_N => '╩',
        d if d == DIR_N | DIR_S | DIR_E => '╠',
        d if d == DIR_N | DIR_S | DIR_W => '╣',
        d if d == DIR_N | DIR_S | DIR_E | DIR_W => '╬',
        d if d & (DIR_E | DIR_W) != 0 => '═',
        d if d & (DIR_N | DIR_S) != 0 => '║',
        _ => '·',
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

/// Read-only DAG renderer for the running screen.
/// Renders pipeline blocks and wires with status-based coloring and auto-centered viewport.
pub(crate) fn render_dag_readonly(
    f: &mut Frame,
    area: Rect,
    blocks: &[crate::execution::pipeline::PipelineBlock],
    connections: &[crate::execution::pipeline::PipelineConnection],
    loop_connections: &[crate::execution::pipeline::LoopConnection],
    block_style_fn: &dyn Fn(crate::execution::pipeline::BlockId) -> (Color, BorderType),
) {
    if area.width == 0 || area.height == 0 || blocks.is_empty() {
        return;
    }

    // Auto-center viewport based on bounding box of all blocks
    let (min_col, max_col, min_row, max_row) = blocks.iter().fold(
        (u16::MAX, 0u16, u16::MAX, 0u16),
        |(mnc, mxc, mnr, mxr), b| {
            (
                mnc.min(b.position.0),
                mxc.max(b.position.0),
                mnr.min(b.position.1),
                mxr.max(b.position.1),
            )
        },
    );

    let world_left = min_col as i16 * CELL_W as i16;
    let world_right = max_col as i16 * CELL_W as i16 + BLOCK_W as i16;
    let world_top = min_row as i16 * CELL_H as i16;
    let world_bottom = max_row as i16 * CELL_H as i16 + BLOCK_H as i16;
    let world_w = world_right - world_left;
    let world_h = world_bottom - world_top;

    let ox = world_left - (area.width as i16 - world_w) / 2;
    let oy = world_top - (area.height as i16 - world_h) / 2;

    // Draw blocks
    for block in blocks {
        let sx = block.position.0 as i16 * CELL_W as i16 - ox;
        let sy = block.position.1 as i16 * CELL_H as i16 - oy;

        if sx + BLOCK_W as i16 <= 0 || sy + BLOCK_H as i16 <= 0 {
            continue;
        }
        let rx = area.x as i16 + sx;
        let ry = area.y as i16 + sy;
        if rx < area.x as i16
            || ry < area.y as i16
            || rx as u16 + BLOCK_W > area.x + area.width
            || ry as u16 + BLOCK_H > area.y + area.height
        {
            continue;
        }
        let block_area = Rect::new(rx as u16, ry as u16, BLOCK_W, BLOCK_H);

        let (border_color, border_type) = block_style_fn(block.id);
        let base_title = if block.name.is_empty() {
            block.agent.clone()
        } else {
            block.name.clone()
        };
        let title = if block.replicas > 1 {
            format!(" {} \u{00d7}{} ", base_title, block.replicas)
        } else {
            format!(" {} ", base_title)
        };
        let block_widget = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_type(border_type)
            .border_style(Style::default().fg(border_color));
        let inner = block_widget.inner(block_area);
        f.render_widget(block_widget, block_area);

        if inner.height > 0 && inner.width > 0 {
            let agent_display = truncate_chars(&block.agent, inner.width as usize);
            let agent_p = Paragraph::new(Span::styled(
                agent_display,
                Style::default().fg(Color::White),
            ));
            f.render_widget(agent_p, Rect::new(inner.x, inner.y, inner.width, 1));
        }

        if inner.height > 1 && inner.width > 0 {
            let prompt_label = if block.prompt.is_empty() {
                "no prompt"
            } else {
                "has prompt"
            };
            let prompt_p = Paragraph::new(Span::styled(
                prompt_label,
                Style::default().fg(Color::DarkGray),
            ));
            f.render_widget(
                prompt_p,
                Rect::new(inner.x, inner.y + 1, inner.width, 1),
            );
        }
    }

    // Wire routing and painting
    let grid_occ = grid_occupancy(blocks);
    let lanes = assign_lanes(connections, blocks);
    let ports = assign_ports(connections, blocks);

    let mut rendered_connections: Vec<(u8, ConnectionRaster)> = Vec::new();

    for (ci, conn) in connections.iter().enumerate() {
        let fb = blocks.iter().find(|b| b.id == conn.from);
        let tb = blocks.iter().find(|b| b.id == conn.to);
        let (Some(fb), Some(tb)) = (fb, tb) else {
            continue;
        };

        let color = WIRE_PALETTE[ci % WIRE_PALETTE.len()];
        let (exit_y_off, entry_y_off) = ports[ci];
        let segs = route_wire(
            fb.position,
            tb.position,
            &grid_occ,
            lanes[ci],
            exit_y_off,
            entry_y_off,
        );
        let mut conn_map: ConnectionRaster = HashMap::new();
        for seg in &segs {
            rasterize_seg(seg, color, &mut conn_map);
        }
        if let Some(last) = segs.last() {
            let arrow_ch = arrow_for_seg(last);
            if let Some(cell) = conn_map.get_mut(&(last.x2, last.y2)) {
                cell.is_arrow = true;
                cell.arrow_char = arrow_ch;
            } else {
                conn_map.insert(
                    (last.x2, last.y2),
                    WireCell {
                        dirs: 0,
                        color,
                        is_arrow: true,
                        arrow_char: arrow_ch,
                        is_loop: false,
                    },
                );
            }
        }
        rendered_connections.push((color_rank(color), conn_map));
    }

    rendered_connections.sort_by_key(|(rank, _)| *rank);

    for (_, conn_map) in rendered_connections {
        for (&(wx, wy), cell) in &conn_map {
            if pixel_hits_block(wx, wy, blocks) {
                continue;
            }
            let ch = if cell.is_arrow {
                cell.arrow_char
            } else if cell.is_loop {
                dirs_to_double_char(cell.dirs)
            } else {
                dirs_to_char(cell.dirs)
            };
            put_char(
                f,
                area,
                wx - ox,
                wy - oy,
                ch,
                Style::default().fg(cell.color),
            );
        }
    }

    // ── Loop connection rendering (readonly) ──
    {
        let loop_conns_as_regular: Vec<crate::execution::pipeline::PipelineConnection> =
            loop_connections.iter()
                .map(|lc| crate::execution::pipeline::PipelineConnection { from: lc.from, to: lc.to })
                .collect();
        if !loop_conns_as_regular.is_empty() {
            let loop_lanes = assign_lanes(&loop_conns_as_regular, blocks);
            let loop_ports = assign_ports(&loop_conns_as_regular, blocks);

            for (ci, lc) in loop_connections.iter().enumerate() {
                let fb = blocks.iter().find(|b| b.id == lc.from);
                let tb = blocks.iter().find(|b| b.id == lc.to);
                let (Some(fb), Some(tb)) = (fb, tb) else { continue; };

                let color = Color::Yellow;

                let (exit_y_off, entry_y_off) = loop_ports[ci];
                let segs = route_wire(
                    fb.position, tb.position, &grid_occ,
                    loop_lanes[ci], exit_y_off, entry_y_off,
                );
                let mut conn_map: ConnectionRaster = HashMap::new();
                for seg in &segs {
                    rasterize_seg(seg, color, &mut conn_map);
                }
                for cell in conn_map.values_mut() {
                    cell.is_loop = true;
                }
                if let Some(last) = segs.last() {
                    let arrow_ch = arrow_for_seg(last);
                    if let Some(cell) = conn_map.get_mut(&(last.x2, last.y2)) {
                        cell.is_arrow = true;
                        cell.arrow_char = arrow_ch;
                    } else {
                        conn_map.insert(
                            (last.x2, last.y2),
                            WireCell { dirs: 0, color, is_arrow: true, arrow_char: arrow_ch, is_loop: true },
                        );
                    }
                }

                // Count label at midpoint of longest horizontal segment
                let mut best_h: Option<(i16, i16, i16)> = None;
                for seg in &segs {
                    if seg.y1 == seg.y2 {
                        let len = (seg.x2 - seg.x1).abs();
                        if best_h.is_none() || len > (best_h.unwrap().2 - best_h.unwrap().1).abs() {
                            let (lo, hi) = if seg.x1 <= seg.x2 { (seg.x1, seg.x2) } else { (seg.x2, seg.x1) };
                            best_h = Some((seg.y1, lo, hi));
                        }
                    }
                }
                let label = format!("\u{00d7}{}", lc.count);

                for (&(wx, wy), cell) in &conn_map {
                    if pixel_hits_block(wx, wy, blocks) {
                        continue;
                    }
                    let ch = if cell.is_arrow {
                        cell.arrow_char
                    } else {
                        dirs_to_double_char(cell.dirs)
                    };
                    put_char(f, area, wx - ox, wy - oy, ch,
                        Style::default().fg(cell.color));
                }

                if let Some((ly, lx_start, lx_end)) = best_h {
                    let mid_x = (lx_start + lx_end) / 2;
                    let label_style = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
                    for (i, ch) in label.chars().enumerate() {
                        put_char(f, area, mid_x + i as i16 - ox, ly - oy, ch, label_style);
                    }
                }
            }
        }
    }
}

fn draw_help_bar(f: &mut Frame, app: &App, area: Rect) {
    let help = if app.pipeline.pipeline_loop_connecting_from.is_some() {
        "Arrows: navigate | Enter: loop | Esc: cancel"
    } else if app.pipeline.pipeline_connecting_from.is_some() {
        "Arrows: navigate | Enter: connect | Esc: cancel"
    } else if app.pipeline.pipeline_removing_conn {
        "j/k: cycle | Enter: remove | Esc: cancel"
    } else if matches!(
        app.pipeline.pipeline_focus,
        PipelineFocus::InitialPrompt | PipelineFocus::SessionName
    ) {
        "Tab: next field | F5: run | Esc: back"
    } else {
        "Arrows/hjkl: select | Shift+Arrows/HJKL: move block | Ctrl+Arrows: scroll | a: add | d: delete | e: edit | c: connect | x: disconnect | o: loop | s: sessions | Ctrl+S: save | Ctrl+L: load | F5: run | ?: help | Esc: back"
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
            Constraint::Length(2), // Name          [0]
            Constraint::Length(1), // spacer        [1]
            Constraint::Length(2), // Provider      [2]
            Constraint::Length(1), // spacer        [3]
            Constraint::Min(6),    // Prompt        [4]
            Constraint::Length(1), // spacer        [5]
            Constraint::Length(2), // Session ID    [6]
            Constraint::Length(1), // spacer        [7]
            Constraint::Length(2), // Replicas      [8]
            Constraint::Length(1), // spacer        [9]
            Constraint::Length(1), // hint          [10]
        ])
        .split(inner);

    // Name field
    let name_focus = app.pipeline.pipeline_edit_field == PipelineEditField::Name;
    let name_style = if name_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let name_line = Line::from(vec![
        Span::styled("Name: ", Style::default().fg(Color::White)),
        Span::styled("[", name_style),
        Span::raw(&app.pipeline.pipeline_edit_name_buf),
        Span::styled("]", name_style),
    ]);
    f.render_widget(Paragraph::new(name_line), chunks[0]);

    // Agent selector
    let agent_focus = app.pipeline.pipeline_edit_field == PipelineEditField::Agent;
    let agent_name = app
        .config
        .agents
        .get(app.pipeline.pipeline_edit_agent_idx)
        .map(|a| a.name.as_str())
        .unwrap_or("(none)");
    let avail_agents: std::collections::HashMap<&str, bool> = app
        .available_agents()
        .into_iter()
        .map(|(a, avail)| (a.name.as_str(), avail))
        .collect();
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
            Style::default()
                .fg(agent_color)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" \u{25ba}", arrow_style),
    ]);
    let agent_p = Paragraph::new(agent_line);
    f.render_widget(agent_p, chunks[2]);

    // Prompt textarea
    let prompt_focus = app.pipeline.pipeline_edit_field == PipelineEditField::Prompt;
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

    let (prompt_scroll, prompt_cursor_col, prompt_cursor_row) = if prompt_focus {
        prompt_cursor_layout(
            &app.pipeline.pipeline_edit_prompt_buf,
            app.pipeline.pipeline_edit_prompt_cursor,
            prompt_inner.width as usize,
            prompt_inner.height as usize,
        )
    } else {
        (0, 0, 0)
    };

    let edit_wrapped = char_wrap_text(
        &app.pipeline.pipeline_edit_prompt_buf,
        prompt_inner.width as usize,
    );
    let prompt_p = Paragraph::new(edit_wrapped.as_str()).scroll((prompt_scroll, 0));
    f.render_widget(prompt_p, prompt_inner);

    if prompt_focus && prompt_inner.width > 0 && prompt_inner.height > 0 {
        let visible_row = prompt_cursor_row.saturating_sub(prompt_scroll as usize);
        let cx = prompt_inner.x
            + (prompt_cursor_col.min(prompt_inner.width.saturating_sub(1) as usize) as u16);
        let cy = prompt_inner.y
            + (visible_row.min(prompt_inner.height.saturating_sub(1) as usize) as u16);
        f.set_cursor_position((cx, cy));
    }

    // Session ID
    let sess_focus = app.pipeline.pipeline_edit_field == PipelineEditField::SessionId;
    let sess_style = if sess_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let sess_line = Line::from(vec![
        Span::styled("Session ID: ", Style::default().fg(Color::White)),
        Span::styled("[", sess_style),
        Span::raw(&app.pipeline.pipeline_edit_session_buf),
        Span::styled("]", sess_style),
    ]);
    f.render_widget(Paragraph::new(sess_line), chunks[6]);

    // Replicas
    let rep_focus = app.pipeline.pipeline_edit_field == PipelineEditField::Replicas;
    let rep_style = if rep_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let rep_line = Line::from(vec![
        Span::styled("Replicas: ", Style::default().fg(Color::White)),
        Span::styled("[", rep_style),
        Span::raw(&app.pipeline.pipeline_edit_replicas_buf),
        Span::styled("]", rep_style),
        Span::styled(" (1-32)", Style::default().fg(Color::DarkGray)),
    ]);
    f.render_widget(Paragraph::new(rep_line), chunks[8]);

    // Hint
    let hint = Paragraph::new("  Tab: next  Enter: save  Esc: back")
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(hint, chunks[10]);
}

fn draw_file_dialog(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(50, 40, area);
    f.render_widget(Clear, popup);

    match app.pipeline.pipeline_file_dialog {
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
                Span::raw(format!("{}_", &app.pipeline.pipeline_file_input)),
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

            if app.pipeline.pipeline_file_list.is_empty() {
                let msg = Paragraph::new("No pipeline files found")
                    .style(Style::default().fg(Color::DarkGray));
                f.render_widget(msg, inner);
            } else {
                let items: Vec<Line> = app
                    .pipeline
                    .pipeline_file_list
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let style = if i == app.pipeline.pipeline_file_cursor {
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

fn draw_session_config_popup(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(55, 50, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .title(" Session Config ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(popup);
    f.render_widget(block, popup);

    if inner.width < 10 || inner.height < 3 {
        return;
    }

    let sessions = app.pipeline.pipeline_def.effective_sessions();

    if sessions.is_empty() {
        // Empty state
        let msg = Paragraph::new("No sessions defined")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(ratatui::layout::Alignment::Center);
        let cy = inner.y + inner.height / 2;
        f.render_widget(msg, Rect::new(inner.x, cy, inner.width, 1));

        if inner.height > 2 {
            let hint = Paragraph::new("Add blocks to the pipeline")
                .style(Style::default().fg(Color::DarkGray))
                .alignment(ratatui::layout::Alignment::Center);
            f.render_widget(hint, Rect::new(inner.x, cy + 1, inner.width, 1));
        }

        let footer = Paragraph::new("Esc: close")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(ratatui::layout::Alignment::Center);
        let fy = inner.y + inner.height.saturating_sub(1);
        f.render_widget(footer, Rect::new(inner.x, fy, inner.width, 1));
        return;
    }

    // Header
    let header = Line::from(vec![
        Span::styled(
            format!("{:<15}", "Agent"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:<17}", "Session"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:<6}", "Keep"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "\u{00d7}N",
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ]);
    f.render_widget(Paragraph::new(header), Rect::new(inner.x, inner.y, inner.width, 1));

    // Footer
    let footer = Paragraph::new("j/k: navigate | Space/Enter: toggle | Esc: close")
        .style(Style::default().fg(Color::DarkGray))
        .alignment(ratatui::layout::Alignment::Center);
    let footer_y = inner.y + inner.height.saturating_sub(1);
    f.render_widget(footer, Rect::new(inner.x, footer_y, inner.width, 1));

    // Rows
    let visible_rows = inner.height.saturating_sub(2) as usize; // minus header and footer
    if visible_rows == 0 {
        return;
    }

    let cursor = app
        .pipeline
        .pipeline_session_config_cursor
        .min(sessions.len().saturating_sub(1));
    let scroll_offset = cursor.saturating_sub(visible_rows.saturating_sub(1));

    for (vi, si) in (scroll_offset..sessions.len()).take(visible_rows).enumerate() {
        let session = &sessions[si];
        let is_selected = si == cursor;

        let agent_col = truncate_chars(&session.agent, 14);
        let label_col = truncate_chars(&session.display_label, 16);
        let keep_col = if session.keep_across_iterations {
            "[x]"
        } else {
            "[ ]"
        };
        let replicas_col = if session.total_replicas > 1 {
            format!("\u{00d7}{}", session.total_replicas)
        } else {
            String::new()
        };

        let style = if is_selected {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
        } else {
            Style::default()
        };

        let row = Line::from(vec![
            Span::styled(format!("{:<15}", agent_col), style),
            Span::styled(format!("{:<17}", label_col), style),
            Span::styled(
                format!("{:<6}", keep_col),
                if session.keep_across_iterations {
                    style.fg(if is_selected { Color::Black } else { Color::Green })
                } else {
                    style.fg(if is_selected { Color::Black } else { Color::Red })
                },
            ),
            Span::styled(replicas_col, style),
        ]);

        let row_y = inner.y + 1 + vi as u16; // +1 for header
        f.render_widget(Paragraph::new(row), Rect::new(inner.x, row_y, inner.width, 1));
    }
}

fn draw_loop_edit_popup(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(50, 40, area);
    f.render_widget(Clear, popup);

    let (from_name, to_name) = if let Some((from_id, to_id)) = app.pipeline.pipeline_loop_edit_target {
        let fname = app.pipeline.pipeline_def.blocks.iter()
            .find(|b| b.id == from_id)
            .map(|b| if b.name.is_empty() { format!("Block {}", b.id) } else { b.name.clone() })
            .unwrap_or_else(|| format!("#{from_id}"));
        let tname = app.pipeline.pipeline_def.blocks.iter()
            .find(|b| b.id == to_id)
            .map(|b| if b.name.is_empty() { format!("Block {}", b.id) } else { b.name.clone() })
            .unwrap_or_else(|| format!("#{to_id}"));
        (fname, tname)
    } else {
        ("?".into(), "?".into())
    };

    let title = format!(" Loop: {} \u{2192} {} ", from_name, to_name);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(popup);
    f.render_widget(block, popup);

    if inner.width < 10 || inner.height < 6 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Count
            Constraint::Length(1), // spacer
            Constraint::Min(4),    // Prompt
            Constraint::Length(1), // hint
        ])
        .split(inner);

    // Count field
    let count_focus = app.pipeline.pipeline_loop_edit_field == PipelineLoopEditField::Count;
    let count_style = if count_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let count_line = Line::from(vec![
        Span::styled("Count: ", Style::default().fg(Color::White)),
        Span::styled("[", count_style),
        Span::raw(&app.pipeline.pipeline_loop_edit_count_buf),
        Span::styled("]", count_style),
        Span::styled(" (1-99, returns from target to source)", Style::default().fg(Color::DarkGray)),
    ]);
    f.render_widget(Paragraph::new(count_line), chunks[0]);

    // Prompt textarea
    let prompt_focus = app.pipeline.pipeline_loop_edit_field == PipelineLoopEditField::Prompt;
    let prompt_style = if prompt_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let prompt_block = Block::default()
        .title(" Loop Prompt ")
        .borders(Borders::ALL)
        .border_style(prompt_style);
    let prompt_inner = prompt_block.inner(chunks[2]);
    f.render_widget(prompt_block, chunks[2]);

    let (prompt_scroll, prompt_cursor_col, prompt_cursor_row) = if prompt_focus {
        prompt_cursor_layout(
            &app.pipeline.pipeline_loop_edit_prompt_buf,
            app.pipeline.pipeline_loop_edit_prompt_cursor,
            prompt_inner.width as usize,
            prompt_inner.height as usize,
        )
    } else {
        (0, 0, 0)
    };

    let edit_wrapped = char_wrap_text(
        &app.pipeline.pipeline_loop_edit_prompt_buf,
        prompt_inner.width as usize,
    );
    let prompt_p = Paragraph::new(edit_wrapped.as_str()).scroll((prompt_scroll, 0));
    f.render_widget(prompt_p, prompt_inner);

    if prompt_focus && prompt_inner.width > 0 && prompt_inner.height > 0 {
        let visible_row = prompt_cursor_row.saturating_sub(prompt_scroll as usize);
        let cx = prompt_inner.x
            + (prompt_cursor_col.min(prompt_inner.width.saturating_sub(1) as usize) as u16);
        let cy = prompt_inner.y
            + (visible_row.min(prompt_inner.height.saturating_sub(1) as usize) as u16);
        f.set_cursor_position((cx, cy));
    }

    // Hint
    let hint = Paragraph::new("  Tab: switch field  Enter: save (from Count) / newline (from Prompt)  Esc: cancel")
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(hint, chunks[3]);
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

#[cfg(test)]
mod routing_tests {
    use super::*;
    use crate::execution::pipeline::{PipelineBlock, PipelineConnection};

    fn block_at(id: u32, col: u16, row: u16) -> PipelineBlock {
        PipelineBlock {
            id,
            name: String::new(),
            agent: "test".into(),
            prompt: String::new(),
            session_id: None,
            position: (col, row),
            replicas: 1,
        }
    }

    fn no_seg_hits_block(segs: &[WireSeg], blocks: &[PipelineBlock]) {
        for seg in segs {
            if seg.y1 == seg.y2 {
                let y = seg.y1;
                let (lo, hi) = if seg.x1 <= seg.x2 {
                    (seg.x1, seg.x2)
                } else {
                    (seg.x2, seg.x1)
                };
                for x in lo..=hi {
                    assert!(
                        !pixel_hits_block(x, y, blocks),
                        "wire pixel ({x}, {y}) hits a block"
                    );
                }
            } else {
                let x = seg.x1;
                let (lo, hi) = if seg.y1 <= seg.y2 {
                    (seg.y1, seg.y2)
                } else {
                    (seg.y2, seg.y1)
                };
                for y in lo..=hi {
                    assert!(
                        !pixel_hits_block(x, y, blocks),
                        "wire pixel ({x}, {y}) hits a block"
                    );
                }
            }
        }
    }

    #[test]
    fn route_same_row_clear() {
        let blocks = vec![block_at(1, 0, 0), block_at(2, 1, 0)];
        let occ = grid_occupancy(&blocks);
        let segs = route_wire((0, 0), (1, 0), &occ, 0, 0, 0);
        assert_eq!(segs.len(), 1, "direct forward same-row clear = 1 segment");
        no_seg_hits_block(&segs, &blocks);
    }

    #[test]
    fn route_same_row_clear_with_port_offsets() {
        let blocks = vec![block_at(1, 0, 0), block_at(2, 1, 0)];
        let occ = grid_occupancy(&blocks);
        let segs = route_wire((0, 0), (1, 0), &occ, 0, -1, 1);
        assert_eq!(
            segs.len(),
            3,
            "same-row clear with different port offsets = 3 segments"
        );
        no_seg_hits_block(&segs, &blocks);
    }

    #[test]
    fn route_same_row_blocked() {
        let blocks = vec![block_at(1, 0, 0), block_at(2, 1, 0), block_at(3, 2, 0)];
        let occ = grid_occupancy(&blocks);
        let segs = route_wire((0, 0), (2, 0), &occ, 0, 0, 0);
        assert_eq!(segs.len(), 5, "detour around obstacle = 5 segments");
        no_seg_hits_block(&segs, &blocks);
    }

    #[test]
    fn route_forward_diff_row() {
        let blocks = vec![block_at(1, 0, 0), block_at(2, 2, 2)];
        let occ = grid_occupancy(&blocks);
        let segs = route_wire((0, 0), (2, 2), &occ, 0, 0, 0);
        // Unobstructed forward different-row uses a 3-segment L-path
        assert_eq!(segs.len(), 3, "forward different row (clear) = 3-segment L-path");
        no_seg_hits_block(&segs, &blocks);
    }

    #[test]
    fn route_backward() {
        let blocks = vec![block_at(1, 0, 0), block_at(2, 2, 0)];
        let occ = grid_occupancy(&blocks);
        let segs = route_wire((2, 0), (0, 0), &occ, 0, 0, 0);
        assert_eq!(segs.len(), 5, "backward U-route = 5 segments");
        no_seg_hits_block(&segs, &blocks);
    }

    #[test]
    fn route_same_col_diff_row() {
        let blocks = vec![block_at(1, 1, 0), block_at(2, 1, 2)];
        let occ = grid_occupancy(&blocks);
        let segs = route_wire((1, 0), (1, 2), &occ, 0, 0, 0);
        assert_eq!(
            segs.len(),
            5,
            "same column different row = backward U-route = 5 segments"
        );
        no_seg_hits_block(&segs, &blocks);
    }

    #[test]
    fn lane_assignment_spreads() {
        let blocks = vec![
            block_at(1, 0, 0),
            block_at(2, 0, 1),
            block_at(3, 0, 2),
            block_at(4, 1, 0),
        ];
        let conns = vec![
            PipelineConnection { from: 1, to: 4 },
            PipelineConnection { from: 2, to: 4 },
            PipelineConnection { from: 3, to: 4 },
        ];
        let lanes = assign_lanes(&conns, &blocks);
        let mut sorted = lanes.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2], "3 connections get lanes 0, 1, 2");
    }

    #[test]
    fn glyph_merge_crossing() {
        let mut map: HashMap<(i16, i16), WireCell> = HashMap::new();
        // Horizontal segment through (5,5)
        rasterize_seg(
            &WireSeg {
                x1: 3,
                y1: 5,
                x2: 7,
                y2: 5,
            },
            Color::DarkGray,
            &mut map,
        );
        // Vertical segment through (5,5)
        rasterize_seg(
            &WireSeg {
                x1: 5,
                y1: 3,
                x2: 5,
                y2: 7,
            },
            Color::DarkGray,
            &mut map,
        );
        let cell = map.get(&(5, 5)).unwrap();
        assert_eq!(dirs_to_char(cell.dirs), '┼');
    }

    #[test]
    fn glyph_merge_corner() {
        let mut map: HashMap<(i16, i16), WireCell> = HashMap::new();
        // Horizontal ending at (5,5) from the left
        rasterize_seg(
            &WireSeg {
                x1: 3,
                y1: 5,
                x2: 5,
                y2: 5,
            },
            Color::DarkGray,
            &mut map,
        );
        // Vertical starting at (5,5) going down
        rasterize_seg(
            &WireSeg {
                x1: 5,
                y1: 5,
                x2: 5,
                y2: 8,
            },
            Color::DarkGray,
            &mut map,
        );
        let cell = map.get(&(5, 5)).unwrap();
        assert_eq!(dirs_to_char(cell.dirs), '┐');
    }

    #[test]
    fn color_priority_order() {
        let mut map: HashMap<(i16, i16), WireCell> = HashMap::new();
        rasterize_seg(
            &WireSeg {
                x1: 0,
                y1: 0,
                x2: 2,
                y2: 0,
            },
            Color::DarkGray,
            &mut map,
        );
        rasterize_seg(
            &WireSeg {
                x1: 0,
                y1: 0,
                x2: 2,
                y2: 0,
            },
            Color::Yellow,
            &mut map,
        );
        rasterize_seg(
            &WireSeg {
                x1: 0,
                y1: 0,
                x2: 2,
                y2: 0,
            },
            Color::Red,
            &mut map,
        );
        let cell = map.get(&(1, 0)).unwrap();
        assert_eq!(cell.color, Color::Red, "Red > Yellow > DarkGray");
    }

    #[test]
    fn arrow_does_not_downgrade_color_at_shared_endpoint() {
        // Two connections share an endpoint: first Red, then DarkGray arrow.
        // Arrow marking must not overwrite Red with DarkGray.
        let mut wire_map: HashMap<(i16, i16), WireCell> = HashMap::new();
        let mut arrows: Vec<((i16, i16), Color)> = Vec::new();

        // Simulate a Red connection ending at (10, 5)
        rasterize_seg(
            &WireSeg {
                x1: 5,
                y1: 5,
                x2: 10,
                y2: 5,
            },
            Color::Red,
            &mut wire_map,
        );
        arrows.push(((10, 5), Color::Red));

        // Simulate a DarkGray connection also ending at (10, 5)
        rasterize_seg(
            &WireSeg {
                x1: 5,
                y1: 3,
                x2: 10,
                y2: 3,
            },
            Color::DarkGray,
            &mut wire_map,
        );
        rasterize_seg(
            &WireSeg {
                x1: 10,
                y1: 3,
                x2: 10,
                y2: 5,
            },
            Color::DarkGray,
            &mut wire_map,
        );
        arrows.push(((10, 5), Color::DarkGray));

        // Apply arrow marking (same logic as draw_canvas)
        for &((ax, ay), color) in &arrows {
            if let Some(cell) = wire_map.get_mut(&(ax, ay)) {
                cell.is_arrow = true;
                if color_rank(color) > color_rank(cell.color) {
                    cell.color = color;
                }
            }
        }

        let cell = wire_map.get(&(10, 5)).unwrap();
        assert!(cell.is_arrow);
        assert_eq!(
            cell.color,
            Color::Red,
            "DarkGray arrow must not downgrade Red"
        );
    }

    #[test]
    fn highlight_mapping_uses_filtered_index() {
        // pipeline_conn_cursor indexes a filtered subset; the drawing code must
        // map it to the correct global connection index.
        let blocks = vec![block_at(1, 0, 0), block_at(2, 1, 0), block_at(3, 2, 0)];
        let conns = vec![
            PipelineConnection { from: 1, to: 2 }, // global 0
            PipelineConnection { from: 2, to: 3 }, // global 1
            PipelineConnection { from: 1, to: 3 }, // global 2
        ];

        // Simulate selecting block 2 (sel=2) in remove mode.
        // Filtered connections touching block 2: global indices [0, 1].
        let sel: BlockId = 2;
        let filtered: Vec<usize> = conns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.from == sel || c.to == sel)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(filtered, vec![0, 1]);

        // pipeline_conn_cursor=1 should map to global index 1 (conn 2→3),
        // not global index 1 by coincidence — test with cursor=0 too.
        let cursor = 1;
        let highlighted_global = filtered.get(cursor).copied();
        assert_eq!(highlighted_global, Some(1));

        // Verify global index 0 is NOT highlighted
        assert_ne!(highlighted_global, Some(0));

        // Also verify the same logic as in draw_canvas (nth-based)
        let nth_result = conns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.from == sel || c.to == sel)
            .nth(cursor)
            .map(|(i, _)| i);
        assert_eq!(nth_result, highlighted_global);

        // Now test cursor=0 → global index 0 (conn 1→2)
        let cursor_0 = 0;
        let highlighted_0 = conns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.from == sel || c.to == sel)
            .nth(cursor_0)
            .map(|(i, _)| i);
        assert_eq!(highlighted_0, Some(0));

        // Ensure the blocks are valid for route_wire (no panics)
        let occ = grid_occupancy(&blocks);
        let lanes = assign_lanes(&conns, &blocks);
        let ports = assign_ports(&conns, &blocks);
        for (ci, conn) in conns.iter().enumerate() {
            let fb = blocks.iter().find(|b| b.id == conn.from).unwrap();
            let tb = blocks.iter().find(|b| b.id == conn.to).unwrap();
            let (ey, ny) = ports[ci];
            let segs = route_wire(fb.position, tb.position, &occ, lanes[ci], ey, ny);
            assert!(!segs.is_empty());
        }
    }

    #[test]
    fn arrow_direction_matches_segment() {
        assert_eq!(
            arrow_for_seg(&WireSeg {
                x1: 0,
                y1: 0,
                x2: 5,
                y2: 0
            }),
            '▶'
        );
        assert_eq!(
            arrow_for_seg(&WireSeg {
                x1: 5,
                y1: 0,
                x2: 0,
                y2: 0
            }),
            '◀'
        );
        assert_eq!(
            arrow_for_seg(&WireSeg {
                x1: 0,
                y1: 0,
                x2: 0,
                y2: 5
            }),
            '▼'
        );
        assert_eq!(
            arrow_for_seg(&WireSeg {
                x1: 0,
                y1: 5,
                x2: 0,
                y2: 0
            }),
            '▲'
        );
        assert_eq!(
            arrow_for_seg(&WireSeg {
                x1: 0,
                y1: 0,
                x2: 0,
                y2: 0
            }),
            '▶'
        );
    }

    #[test]
    fn ports_produce_distinct_offsets() {
        // Fan-out: one block with 3 outgoing connections
        let blocks = vec![
            block_at(1, 0, 0),
            block_at(2, 1, 0),
            block_at(3, 1, 1),
            block_at(4, 1, 2),
        ];
        let conns = vec![
            PipelineConnection { from: 1, to: 2 },
            PipelineConnection { from: 1, to: 3 },
            PipelineConnection { from: 1, to: 4 },
        ];
        let ports = assign_ports(&conns, &blocks);
        let exit_offsets: Vec<i16> = ports.iter().map(|p| p.0).collect();
        let mut unique_exits: Vec<i16> = exit_offsets.clone();
        unique_exits.sort();
        unique_exits.dedup();
        assert_eq!(
            unique_exits.len(),
            exit_offsets.len(),
            "exit offsets must be distinct"
        );

        // Fan-in: 3 blocks into one
        let conns_in = vec![
            PipelineConnection { from: 2, to: 1 },
            PipelineConnection { from: 3, to: 1 },
            PipelineConnection { from: 4, to: 1 },
        ];
        let ports_in = assign_ports(&conns_in, &blocks);
        let entry_offsets: Vec<i16> = ports_in.iter().map(|p| p.1).collect();
        let mut unique_entries: Vec<i16> = entry_offsets.clone();
        unique_entries.sort();
        unique_entries.dedup();
        assert_eq!(
            unique_entries.len(),
            entry_offsets.len(),
            "entry offsets must be distinct"
        );
    }

    #[test]
    fn fan_in_routes_are_distinct() {
        // Three connections arriving at the same block should have mostly distinct routes.
        // Crossing overlaps are expected (different colors handle visual distinction).
        // Each route must have unique pixels not shared by any other route.
        let blocks = vec![
            block_at(1, 0, 0),
            block_at(2, 0, 2),
            block_at(3, 0, 4),
            block_at(4, 2, 1),
        ];
        let conns = vec![
            PipelineConnection { from: 1, to: 4 },
            PipelineConnection { from: 2, to: 4 },
            PipelineConnection { from: 3, to: 4 },
        ];
        let occ = grid_occupancy(&blocks);
        let lanes = assign_lanes(&conns, &blocks);
        let ports = assign_ports(&conns, &blocks);

        let mut all_pixels: Vec<HashSet<(i16, i16)>> = Vec::new();
        for (ci, conn) in conns.iter().enumerate() {
            let fb = blocks.iter().find(|b| b.id == conn.from).unwrap();
            let tb = blocks.iter().find(|b| b.id == conn.to).unwrap();
            let (ey, ny) = ports[ci];
            let segs = route_wire(fb.position, tb.position, &occ, lanes[ci], ey, ny);
            let mut pixels = HashSet::new();
            for seg in &segs {
                if seg.y1 == seg.y2 {
                    let y = seg.y1;
                    let (lo, hi) = if seg.x1 <= seg.x2 {
                        (seg.x1, seg.x2)
                    } else {
                        (seg.x2, seg.x1)
                    };
                    for x in lo..=hi {
                        pixels.insert((x, y));
                    }
                } else {
                    let x = seg.x1;
                    let (lo, hi) = if seg.y1 <= seg.y2 {
                        (seg.y1, seg.y2)
                    } else {
                        (seg.y2, seg.y1)
                    };
                    for y in lo..=hi {
                        pixels.insert((x, y));
                    }
                }
            }
            all_pixels.push(pixels);
        }

        // Each route must have unique pixels that no other route shares
        for i in 0..all_pixels.len() {
            let unique_count = all_pixels[i]
                .iter()
                .filter(|p| (0..all_pixels.len()).all(|j| j == i || !all_pixels[j].contains(p)))
                .count();
            assert!(
                unique_count > all_pixels[i].len() / 2,
                "connection {i} shares too many pixels: only {unique_count}/{} unique",
                all_pixels[i].len()
            );
        }

        // Routes must not be identical
        for i in 0..all_pixels.len() {
            for j in (i + 1)..all_pixels.len() {
                assert_ne!(
                    all_pixels[i], all_pixels[j],
                    "connections {i} and {j} have identical pixel sets"
                );
            }
        }
    }

    #[test]
    fn grid_occ_blocks_direct_same_row() {
        // With an obstacle at column 1, route from col 0 to col 2 on same row
        // should NOT use the short direct path.
        let blocks = vec![block_at(1, 0, 0), block_at(2, 1, 0), block_at(3, 2, 0)];
        let occ = grid_occupancy(&blocks);
        let segs = route_wire((0, 0), (2, 0), &occ, 0, 0, 0);
        assert_eq!(segs.len(), 5, "blocked corridor forces 5-segment detour");
        // Verify we don't get a 1-segment or 3-segment direct path
        assert!(segs.len() > 3);
    }

    #[test]
    fn higher_lane_u_route_stays_out_of_blocks() {
        // Regression: higher lanes must not place the horizontal U-route segment
        // inside the next row of blocks.
        let blocks = vec![
            block_at(1, 0, 0),
            block_at(4, 1, 0), // force same-row corridor to be blocked -> U-route
            block_at(2, 3, 0),
            block_at(3, 1, 1), // was intersected by lane=1 with too-tight tier spacing
        ];
        let occ = grid_occupancy(&blocks);
        let segs = route_wire((0, 0), (3, 0), &occ, 1, 0, 0);
        assert_eq!(segs.len(), 5);
        no_seg_hits_block(&segs, &blocks);
    }
}
