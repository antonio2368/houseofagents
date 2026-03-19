use crate::app::{
    App, PipelineDialogMode, PipelineEditField, PipelineFeedEditField, PipelineFocus,
    PipelineLoopEditField,
};
use crate::execution::pipeline::BlockId;
use crate::execution::{fit_display_width, truncate_chars};
use crate::screen::centered_rect;
use crate::screen::help;
use crate::screen::prompt::{char_wrap_text, prompt_cursor_layout};

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph};
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

/// Given the current pipeline state and connection cursor, resolve which finalization
/// connection (by global index) is highlighted. Returns `None` if the cursor does not
/// point at a finalization connection.
fn resolve_fin_conn_highlight(ps: &crate::app::PipelineState, removing: bool) -> Option<usize> {
    if !removing {
        return None;
    }
    let sel = ps.pipeline_block_cursor?;
    let regular_count = ps
        .pipeline_def
        .connections
        .iter()
        .filter(|c| c.from == sel || c.to == sel)
        .count();
    let loop_count = ps
        .pipeline_def
        .loop_connections
        .iter()
        .filter(|lc| lc.from == sel || lc.to == sel)
        .count();
    let prefix = regular_count + loop_count;
    if ps.pipeline_conn_cursor < prefix {
        return None;
    }
    let fin_local = ps.pipeline_conn_cursor - prefix;
    ps.pipeline_def
        .finalization_connections
        .iter()
        .enumerate()
        .filter(|(_, c)| c.from == sel || c.to == sel)
        .nth(fin_local)
        .map(|(i, _)| i)
}

/// Compute the pixel Y offset of the separator line between execution and finalization blocks.
/// Returns the separator Y in world coordinates (pixels).
/// Finalization blocks' local Y coordinates are offset by this value + CELL_H.
pub(crate) fn separator_y_offset(blocks: &[crate::execution::pipeline::PipelineBlock]) -> i16 {
    let max_row = blocks.iter().map(|b| b.position.1).max().unwrap_or(0);
    (max_row as i16 + 1) * CELL_H as i16
}

/// Prompt/profile label for block cell rendering.
fn block_prompt_profile_label(
    block: &crate::execution::pipeline::PipelineBlock,
    max_width: usize,
) -> String {
    if block.profiles.is_empty() {
        if block.prompt.is_empty() {
            "Prompt: no".into()
        } else {
            "Prompt: yes".into()
        }
    } else {
        let p = if block.prompt.is_empty() {
            "P:no"
        } else {
            "P:yes"
        };
        let full = format!("{p} Prof:{}", block.profiles.len());
        if full.len() <= max_width {
            full
        } else {
            p.into()
        }
    }
}

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
    if app.pipeline.pipeline_show_feed_list {
        draw_feed_list_popup(f, app, area);
    }
    if app.pipeline.pipeline_show_feed_edit {
        draw_feed_edit_popup(f, app, area);
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
    if app.setup_analysis.active {
        help::draw_setup_analysis_popup(f, &app.setup_analysis);
    }
    // Error/info modals are rendered globally by screen::draw()
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
        || app.pipeline.pipeline_show_feed_edit
        || app.pipeline.pipeline_show_feed_list
        || app.error_modal.is_some()
        || app.info_modal.is_some()
        || app.help_popup.active
        || app.setup_analysis.active;
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

    if app.pipeline.pipeline_def.blocks.is_empty()
        && app.pipeline.pipeline_def.finalization_blocks.is_empty()
    {
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
            block.primary_agent().to_string()
        } else {
            block.name.clone()
        };
        let total_tasks = block.agents.len() as u32 * block.replicas;
        let title = if total_tasks > 1 {
            format!(" {base_title} \u{00d7}{total_tasks} ")
        } else {
            format!(" {base_title} ")
        };
        let block_widget = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_type(border_type)
            .border_style(Style::default().fg(border_color));
        let inner = block_widget.inner(block_area);
        f.render_widget(block_widget, block_area);

        // Line 1: Agent name(s)
        if inner.height > 0 && inner.width > 0 {
            let agent_text = if block.agents.len() > 1 {
                format!("Multiple ({})", block.agents.len())
            } else {
                block.primary_agent().to_string()
            };
            let agent_display = truncate_chars(&agent_text, inner.width as usize);
            let agent_p = Paragraph::new(Span::styled(
                agent_display,
                Style::default().fg(Color::White),
            ));
            f.render_widget(agent_p, Rect::new(inner.x, inner.y, inner.width, 1));
        }

        // Line 2: Prompt/profile status
        if inner.height > 1 && inner.width > 0 {
            let prompt_label = block_prompt_profile_label(block, inner.width as usize);
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
        let loop_conns_as_regular: Vec<crate::execution::pipeline::PipelineConnection> = app
            .pipeline
            .pipeline_def
            .loop_connections
            .iter()
            .map(|lc| crate::execution::pipeline::PipelineConnection {
                from: lc.from,
                to: lc.to,
            })
            .collect();
        if !loop_conns_as_regular.is_empty() {
            let loop_lanes =
                assign_lanes(&loop_conns_as_regular, &app.pipeline.pipeline_def.blocks);
            let loop_ports =
                assign_ports(&loop_conns_as_regular, &app.pipeline.pipeline_def.blocks);

            for (ci, lc) in app
                .pipeline
                .pipeline_def
                .loop_connections
                .iter()
                .enumerate()
            {
                let fb = app
                    .pipeline
                    .pipeline_def
                    .blocks
                    .iter()
                    .find(|b| b.id == lc.from);
                let tb = app
                    .pipeline
                    .pipeline_def
                    .blocks
                    .iter()
                    .find(|b| b.id == lc.to);
                let (Some(fb), Some(tb)) = (fb, tb) else {
                    continue;
                };

                let _removing = app.pipeline.pipeline_removing_conn
                    && is_conn_for_selected(app, lc.from, lc.to);
                let highlighted = if app.pipeline.pipeline_removing_conn {
                    let sel = app.pipeline.pipeline_block_cursor.unwrap_or(0);
                    let regular_count = app
                        .pipeline
                        .pipeline_def
                        .connections
                        .iter()
                        .filter(|c| c.from == sel || c.to == sel)
                        .count();
                    let loop_offset = app
                        .pipeline
                        .pipeline_def
                        .loop_connections
                        .iter()
                        .enumerate()
                        .filter(|(_, l)| l.from == sel || l.to == sel)
                        .position(|(i, _)| i == ci);
                    loop_offset
                        .map(|off| app.pipeline.pipeline_conn_cursor == regular_count + off)
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
                    fb.position,
                    tb.position,
                    &grid_occ,
                    loop_lanes[ci],
                    exit_y_off,
                    entry_y_off,
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
                            WireCell {
                                dirs: 0,
                                color,
                                is_arrow: true,
                                arrow_char: arrow_ch,
                                is_loop: true,
                            },
                        );
                    }
                }

                // Count label at midpoint of longest horizontal segment
                let mut best_h: Option<(i16, i16, i16)> = None; // (y, x_start, x_end)
                for seg in &segs {
                    if seg.y1 == seg.y2 {
                        let len = (seg.x2 - seg.x1).abs();
                        if best_h.is_none() || len > (best_h.unwrap().2 - best_h.unwrap().1).abs() {
                            let (lo, hi) = if seg.x1 <= seg.x2 {
                                (seg.x1, seg.x2)
                            } else {
                                (seg.x2, seg.x1)
                            };
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
                    put_char(
                        f,
                        canvas_inner,
                        wx - ox,
                        wy - oy,
                        ch,
                        Style::default().fg(cell.color),
                    );
                }

                // Paint the label
                if let Some((ly, lx_start, lx_end)) = best_h {
                    let mid_x = (lx_start + lx_end) / 2;
                    let label_style = Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD);
                    for (i, ch) in label.chars().enumerate() {
                        put_char(
                            f,
                            canvas_inner,
                            mid_x + i as i16 - ox,
                            ly - oy,
                            ch,
                            label_style,
                        );
                    }
                }
            }
        }
    }

    // ── Finalization separator, blocks, feeds, and connections ──
    if !app.pipeline.pipeline_def.finalization_blocks.is_empty() {
        let sep_y = separator_y_offset(&app.pipeline.pipeline_def.blocks);
        // Render separator dashed line
        let screen_sep_y = canvas_inner.y as i16 + sep_y - oy;
        if screen_sep_y >= canvas_inner.y as i16
            && (screen_sep_y as u16) < canvas_inner.y + canvas_inner.height
        {
            let label = " Finalization ";
            let label_len = label.len() as u16;
            let dash_style = Style::default().fg(Color::Cyan);
            let label_style = Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD);
            let w = canvas_inner.width;
            // Center the label
            let label_start = if w > label_len {
                (w - label_len) / 2
            } else {
                0
            };
            for col in 0..w {
                let screen_x = canvas_inner.x + col;
                let screen_y = screen_sep_y as u16;
                if screen_y < canvas_inner.y + canvas_inner.height {
                    if col >= label_start && col < label_start + label_len {
                        let ch_idx = (col - label_start) as usize;
                        if let Some(ch) = label.chars().nth(ch_idx) {
                            let buf = f.buffer_mut();
                            buf[(screen_x, screen_y)]
                                .set_char(ch)
                                .set_style(label_style);
                        }
                    } else {
                        let buf = f.buffer_mut();
                        buf[(screen_x, screen_y)]
                            .set_char('\u{2500}')
                            .set_style(dash_style);
                    }
                }
            }
        }

        // Finalization block Y offset = sep_y + 1 row of gap
        let fin_y_off = sep_y + CELL_H as i16 / 2;

        // Draw finalization blocks
        for block in &app.pipeline.pipeline_def.finalization_blocks {
            let sx = block.position.0 as i16 * CELL_W as i16 - ox;
            let sy = block.position.1 as i16 * CELL_H as i16 + fin_y_off - oy;

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
            let is_feed_connect_src =
                app.pipeline.pipeline_feed_connecting_from.is_some() && is_selected;

            let border_type = if is_selected {
                BorderType::Double
            } else {
                BorderType::Rounded
            };
            let border_color = if is_feed_connect_src {
                Color::Green
            } else if is_selected {
                Color::Yellow
            } else {
                Color::Magenta
            };

            let base_title = if block.name.is_empty() {
                block.primary_agent().to_string()
            } else {
                block.name.clone()
            };
            let total_tasks = block.agents.len() as u32 * block.replicas;
            let title = if total_tasks > 1 {
                format!(" {base_title} \u{00d7}{total_tasks} ")
            } else {
                format!(" {base_title} ")
            };
            let block_widget = Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_type(border_type)
                .border_style(Style::default().fg(border_color));
            let inner = block_widget.inner(block_area);
            f.render_widget(block_widget, block_area);

            // Line 1: Agent name(s)
            if inner.height > 0 && inner.width > 0 {
                let agent_text = if block.agents.len() > 1 {
                    format!("Multiple ({})", block.agents.len())
                } else {
                    block.primary_agent().to_string()
                };
                let agent_display = truncate_chars(&agent_text, inner.width as usize);
                let agent_p = Paragraph::new(Span::styled(
                    agent_display,
                    Style::default().fg(Color::White),
                ));
                f.render_widget(agent_p, Rect::new(inner.x, inner.y, inner.width, 1));
            }

            // Line 2: Prompt/profile status
            if inner.height > 1 && inner.width > 0 {
                let prompt_label = block_prompt_profile_label(block, inner.width as usize);
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

        // ── Data feed wires (dashed yellow from execution blocks to finalization blocks) ──
        let feed_style = Style::default().fg(Color::Yellow);
        let exec_blocks = &app.pipeline.pipeline_def.blocks;
        let fin_blocks = &app.pipeline.pipeline_def.finalization_blocks;
        for feed in &app.pipeline.pipeline_def.data_feeds {
            // Determine source blocks
            let sources: Vec<&crate::execution::pipeline::PipelineBlock> =
                if feed.from == crate::execution::pipeline::WILDCARD_BLOCK_ID {
                    exec_blocks.iter().collect()
                } else {
                    exec_blocks.iter().filter(|b| b.id == feed.from).collect()
                };
            let target = fin_blocks.iter().find(|b| b.id == feed.to);
            let Some(target) = target else { continue };

            for src in &sources {
                // Source bottom center (execution block)
                let src_cx = src.position.0 as i16 * CELL_W as i16 + BLOCK_W as i16 / 2;
                let src_by = src.position.1 as i16 * CELL_H as i16 + BLOCK_H as i16;
                // Target top center (finalization block, offset by fin_y_off)
                let tgt_cx = target.position.0 as i16 * CELL_W as i16 + BLOCK_W as i16 / 2;
                let tgt_ty = target.position.1 as i16 * CELL_H as i16 + fin_y_off;

                // Draw dashed vertical-ish wire: go down from src, horizontal to align, then down to target
                let mid_y = (src_by + tgt_ty) / 2;
                // Vertical from src bottom to mid
                let (y_lo, y_hi) = (src_by, mid_y);
                for y in y_lo..=y_hi {
                    let dash = if (y - y_lo) % 2 == 0 { '\u{2506}' } else { ' ' };
                    if dash != ' ' {
                        put_char(f, canvas_inner, src_cx - ox, y - oy, dash, feed_style);
                    }
                }
                // Horizontal from src_cx to tgt_cx at mid_y
                if src_cx != tgt_cx {
                    let (x_lo, x_hi) = if src_cx < tgt_cx {
                        (src_cx, tgt_cx)
                    } else {
                        (tgt_cx, src_cx)
                    };
                    for x in x_lo..=x_hi {
                        let dash = if (x - x_lo) % 2 == 0 { '\u{2504}' } else { ' ' };
                        if dash != ' ' {
                            put_char(f, canvas_inner, x - ox, mid_y - oy, dash, feed_style);
                        }
                    }
                }
                // Vertical from mid_y to target top
                for y in mid_y..tgt_ty {
                    let dash = if (y - mid_y) % 2 == 0 {
                        '\u{2506}'
                    } else {
                        ' '
                    };
                    if dash != ' ' {
                        put_char(f, canvas_inner, tgt_cx - ox, y - oy, dash, feed_style);
                    }
                }
                // Arrow at target top
                put_char(
                    f,
                    canvas_inner,
                    tgt_cx - ox,
                    tgt_ty - oy,
                    '\u{25bc}',
                    feed_style,
                );
            }
        }

        // ── Finalization connections (within finalization phase) ──
        if !app
            .pipeline
            .pipeline_def
            .finalization_connections
            .is_empty()
        {
            // Build position map with offset applied for finalization blocks
            let fin_blocks_offset: Vec<crate::execution::pipeline::PipelineBlock> =
                app.pipeline.pipeline_def.finalization_blocks.to_vec();
            let fin_grid_occ = grid_occupancy(&fin_blocks_offset);
            let fin_lanes = assign_lanes(
                &app.pipeline.pipeline_def.finalization_connections,
                &fin_blocks_offset,
            );
            let fin_ports = assign_ports(
                &app.pipeline.pipeline_def.finalization_connections,
                &fin_blocks_offset,
            );

            // Resolve highlighted finalization connection
            let fin_highlighted_global_idx =
                resolve_fin_conn_highlight(&app.pipeline, app.pipeline.pipeline_removing_conn);

            for (ci, conn) in app
                .pipeline
                .pipeline_def
                .finalization_connections
                .iter()
                .enumerate()
            {
                let fb = fin_blocks_offset.iter().find(|b| b.id == conn.from);
                let tb = fin_blocks_offset.iter().find(|b| b.id == conn.to);
                let (Some(fb), Some(tb)) = (fb, tb) else {
                    continue;
                };

                let removing = app.pipeline.pipeline_removing_conn
                    && is_conn_for_selected(app, conn.from, conn.to);
                let highlighted = fin_highlighted_global_idx == Some(ci);
                let color = if highlighted {
                    Color::Red
                } else if removing {
                    Color::Yellow
                } else {
                    WIRE_PALETTE[ci % WIRE_PALETTE.len()]
                };

                let (exit_y_off, entry_y_off) = fin_ports[ci];
                let segs = route_wire(
                    fb.position,
                    tb.position,
                    &fin_grid_occ,
                    fin_lanes[ci],
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
                // Paint finalization connection wires (offset by fin_y_off)
                for (&(wx, wy), cell) in &conn_map {
                    if pixel_hits_block(wx, wy, &fin_blocks_offset) {
                        continue;
                    }
                    let ch = if cell.is_arrow {
                        cell.arrow_char
                    } else {
                        dirs_to_char(cell.dirs)
                    };
                    put_char(
                        f,
                        canvas_inner,
                        wx - ox,
                        wy + fin_y_off - oy,
                        ch,
                        Style::default().fg(cell.color),
                    );
                }
            }
        }
    }

    // Status line for connect/remove modes
    if app.pipeline.pipeline_feed_connecting_from.is_some() {
        let status = Paragraph::new("Select finalization block target (Enter=feed, Esc=cancel)")
            .style(Style::default().fg(Color::Yellow));
        let sy = canvas_inner.y + canvas_inner.height.saturating_sub(1);
        f.render_widget(status, Rect::new(canvas_inner.x, sy, canvas_inner.width, 1));
    } else if app.pipeline.pipeline_loop_connecting_from.is_some() {
        let status = Paragraph::new("Select upstream restart target (Enter=loop, Esc=cancel)")
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
                    y2: ny,
                },
                WireSeg {
                    x1: v1,
                    y1: ny,
                    x2: nx,
                    y2: ny,
                },
            ];
        }
        let source_row_clear = (fc + 1..tc).all(|c| !grid_occ.contains(&(c, fr)));
        if source_row_clear {
            let v2 = nx - side_lane.min(side_clamp);
            return vec![
                WireSeg {
                    x1: ex,
                    y1: ey,
                    x2: v2,
                    y2: ey,
                },
                WireSeg {
                    x1: v2,
                    y1: ey,
                    x2: v2,
                    y2: ny,
                },
                WireSeg {
                    x1: v2,
                    y1: ny,
                    x2: nx,
                    y2: ny,
                },
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
            block.primary_agent().to_string()
        } else {
            block.name.clone()
        };
        let total_tasks = block.agents.len() as u32 * block.replicas;
        let title = if total_tasks > 1 {
            format!(" {base_title} \u{00d7}{total_tasks} ")
        } else {
            format!(" {base_title} ")
        };
        let block_widget = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_type(border_type)
            .border_style(Style::default().fg(border_color));
        let inner = block_widget.inner(block_area);
        f.render_widget(block_widget, block_area);

        if inner.height > 0 && inner.width > 0 {
            let agent_text = if block.agents.len() > 1 {
                format!("Multiple ({})", block.agents.len())
            } else {
                block.primary_agent().to_string()
            };
            let agent_display = truncate_chars(&agent_text, inner.width as usize);
            let agent_p = Paragraph::new(Span::styled(
                agent_display,
                Style::default().fg(Color::White),
            ));
            f.render_widget(agent_p, Rect::new(inner.x, inner.y, inner.width, 1));
        }

        if inner.height > 1 && inner.width > 0 {
            let prompt_label = block_prompt_profile_label(block, inner.width as usize);
            let prompt_p = Paragraph::new(Span::styled(
                prompt_label,
                Style::default().fg(Color::DarkGray),
            ));
            f.render_widget(prompt_p, Rect::new(inner.x, inner.y + 1, inner.width, 1));
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
            loop_connections
                .iter()
                .map(|lc| crate::execution::pipeline::PipelineConnection {
                    from: lc.from,
                    to: lc.to,
                })
                .collect();
        if !loop_conns_as_regular.is_empty() {
            let loop_lanes = assign_lanes(&loop_conns_as_regular, blocks);
            let loop_ports = assign_ports(&loop_conns_as_regular, blocks);

            for (ci, lc) in loop_connections.iter().enumerate() {
                let fb = blocks.iter().find(|b| b.id == lc.from);
                let tb = blocks.iter().find(|b| b.id == lc.to);
                let (Some(fb), Some(tb)) = (fb, tb) else {
                    continue;
                };

                let color = Color::Yellow;

                let (exit_y_off, entry_y_off) = loop_ports[ci];
                let segs = route_wire(
                    fb.position,
                    tb.position,
                    &grid_occ,
                    loop_lanes[ci],
                    exit_y_off,
                    entry_y_off,
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
                            WireCell {
                                dirs: 0,
                                color,
                                is_arrow: true,
                                arrow_char: arrow_ch,
                                is_loop: true,
                            },
                        );
                    }
                }

                // Count label at midpoint of longest horizontal segment
                let mut best_h: Option<(i16, i16, i16)> = None;
                for seg in &segs {
                    if seg.y1 == seg.y2 {
                        let len = (seg.x2 - seg.x1).abs();
                        if best_h.is_none() || len > (best_h.unwrap().2 - best_h.unwrap().1).abs() {
                            let (lo, hi) = if seg.x1 <= seg.x2 {
                                (seg.x1, seg.x2)
                            } else {
                                (seg.x2, seg.x1)
                            };
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
                    put_char(
                        f,
                        area,
                        wx - ox,
                        wy - oy,
                        ch,
                        Style::default().fg(cell.color),
                    );
                }

                if let Some((ly, lx_start, lx_end)) = best_h {
                    let mid_x = (lx_start + lx_end) / 2;
                    let label_style = Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD);
                    for (i, ch) in label.chars().enumerate() {
                        put_char(f, area, mid_x + i as i16 - ox, ly - oy, ch, label_style);
                    }
                }
            }
        }
    }
}

fn draw_help_bar(f: &mut Frame, app: &App, area: Rect) {
    let help = if app.pipeline.pipeline_feed_connecting_from.is_some() {
        "Arrows: navigate to finalization block | Enter: create feed | Esc: cancel"
    } else if app.pipeline.pipeline_loop_connecting_from.is_some() {
        "Arrows: navigate | Enter: loop | Esc: cancel"
    } else if app.pipeline.pipeline_connecting_from.is_some() {
        "Arrows: navigate | Enter: connect | Esc: cancel"
    } else if app.pipeline.pipeline_removing_conn {
        "j/k: cycle | Enter: remove | Esc: cancel"
    } else if matches!(
        app.pipeline.pipeline_focus,
        PipelineFocus::InitialPrompt | PipelineFocus::SessionName
    ) {
        "Tab: next field | F5: run | Ctrl+E: analyze | Esc: back"
    } else {
        "a: add | A: add finalization | d: del | e: edit | c: connect | x: disconnect | o: loop | f: feed | F: rm feed | s: sessions | ?: help | Esc: back"
    };
    let help_p = Paragraph::new(help)
        .style(Style::default().fg(Color::DarkGray))
        .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(help_p, area);
}

/// Build a truncated comma-separated summary that fits within `max_width` display chars.
/// Shows `"(empty_label)"` when `names` is empty. Uses char count for width measurement.
fn collapse_summary(names: &[&str], max_width: usize, empty_label: &str) -> String {
    use crate::execution::truncate_chars;

    if names.is_empty() {
        return format!("({empty_label})");
    }
    let total = names.len();
    let mut buf = String::new();
    for (i, name) in names.iter().enumerate() {
        let remaining = total - i;
        let suffix_len = if remaining > 1 {
            // ", +N more" where N can be multi-digit
            ", +".len() + format!("{}", remaining - 1).len() + " more".len()
        } else {
            0
        };
        let candidate = if buf.is_empty() {
            name.to_string()
        } else {
            format!("{buf}, {name}")
        };
        let candidate_chars = candidate.chars().count();
        if candidate_chars + suffix_len > max_width {
            if i == 0 {
                // Single first name too long — truncate it
                let avail = max_width.saturating_sub(suffix_len);
                let truncated = truncate_chars(name, avail);
                return if total > 1 {
                    format!("{truncated}, +{} more", total - 1)
                } else {
                    truncated
                };
            }
            return format!("{buf}, +{} more", total - i);
        }
        buf = candidate;
    }
    buf
}

fn draw_edit_popup(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(60, 80, area);
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

    // Dynamic heights: full list when focused, 1-line summary when unfocused
    let agent_focus = app.pipeline.pipeline_edit_field == PipelineEditField::Agent;
    let profile_focus = app.pipeline.pipeline_edit_field == PipelineEditField::Profile;

    let agent_list_h = if agent_focus {
        1 + (app.config.agents.len() as u16).min(6)
    } else {
        1
    };
    let profile_count = app.pipeline.pipeline_edit_profile_list.len() as u16;
    let profile_list_h = if profile_focus {
        if profile_count == 0 {
            2
        } else {
            1 + profile_count.min(4)
        }
    } else {
        1
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),              // [0]  Name
            Constraint::Length(1),              // [1]  spacer
            Constraint::Length(agent_list_h),   // [2]  Agents list
            Constraint::Length(profile_list_h), // [3]  Profiles list
            Constraint::Min(6),                 // [4]  Prompt
            Constraint::Length(1),              // [5]  spacer
            Constraint::Length(2),              // [6]  Session ID
            Constraint::Length(1),              // [7]  spacer
            Constraint::Length(2),              // [8]  Replicas
            Constraint::Length(1),              // [9]  spacer
            Constraint::Length(1),              // [10] hint
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

    // Agent multiselect list (expanded when focused, summary when unfocused)
    if agent_focus {
        let avail_agents: std::collections::HashMap<&str, bool> = app
            .available_agents()
            .into_iter()
            .map(|(a, avail)| (a.name.as_str(), avail))
            .collect();
        let agent_lines: Vec<Line> = app
            .config
            .agents
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let selected = app
                    .pipeline
                    .pipeline_edit_agent_selection
                    .get(i)
                    .copied()
                    .unwrap_or(false);
                let is_avail = avail_agents.get(a.name.as_str()).copied().unwrap_or(false);
                let checkbox = if selected { "[x] " } else { "[ ] " };
                let agent_color = if is_avail { Color::Green } else { Color::Red };
                let is_cursor = i == app.pipeline.pipeline_edit_agent_cursor;
                let style = if is_cursor {
                    Style::default()
                        .fg(agent_color)
                        .add_modifier(Modifier::BOLD | Modifier::REVERSED)
                } else {
                    Style::default().fg(agent_color)
                };
                Line::from(Span::styled(format!("{checkbox}{}", a.name), style))
            })
            .collect();
        let agent_label = Line::from(Span::styled("Agents:", Style::default().fg(Color::Cyan)));
        let mut all_agent_lines = vec![agent_label];
        all_agent_lines.extend(agent_lines);
        let agent_visible_rows = (chunks[2].height as usize).saturating_sub(1);
        app.pipeline
            .pipeline_edit_agent_visible
            .set(agent_visible_rows);
        let scroll_offset = app.pipeline.pipeline_edit_agent_scroll as u16;
        let agent_p = Paragraph::new(all_agent_lines).scroll((scroll_offset, 0));
        f.render_widget(agent_p, chunks[2]);
    } else {
        // Collapsed summary line
        let selected_names: Vec<&str> = app
            .config
            .agents
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                app.pipeline
                    .pipeline_edit_agent_selection
                    .get(*i)
                    .copied()
                    .unwrap_or(false)
            })
            .map(|(_, a)| a.name.as_str())
            .collect();
        let max_w = chunks[2].width.saturating_sub(9) as usize; // "Agents: " = 8 chars + margin
        let summary = collapse_summary(&selected_names, max_w, "none selected");
        let line = Line::from(vec![
            Span::styled("Agents: ", Style::default().fg(Color::White)),
            Span::styled(summary, Style::default().fg(Color::DarkGray)),
        ]);
        f.render_widget(Paragraph::new(line), chunks[2]);
    }

    // Profile multiselect list (expanded when focused, summary when unfocused)
    if profile_focus {
        if app.pipeline.pipeline_edit_profile_list.is_empty() {
            let label = Line::from(Span::styled("Profiles:", Style::default().fg(Color::Cyan)));
            let none_line = Line::from(Span::styled(
                "(none found)",
                Style::default().fg(Color::DarkGray),
            ));
            let profile_p = Paragraph::new(vec![label, none_line]);
            f.render_widget(profile_p, chunks[3]);
        } else {
            let profile_lines: Vec<Line> = app
                .pipeline
                .pipeline_edit_profile_list
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let selected = app
                        .pipeline
                        .pipeline_edit_profile_selection
                        .get(i)
                        .copied()
                        .unwrap_or(false);
                    let is_orphan = app.pipeline.pipeline_edit_profile_orphaned.contains(name);
                    let checkbox = if selected { "[x] " } else { "[ ] " };
                    let color = if is_orphan {
                        Color::Yellow
                    } else if selected {
                        Color::Green
                    } else {
                        Color::White
                    };
                    let label = if is_orphan {
                        format!("{checkbox}{name} [!]")
                    } else {
                        format!("{checkbox}{name}")
                    };
                    let is_cursor = i == app.pipeline.pipeline_edit_profile_cursor;
                    let style = if is_cursor {
                        Style::default()
                            .fg(color)
                            .add_modifier(Modifier::BOLD | Modifier::REVERSED)
                    } else {
                        Style::default().fg(color)
                    };
                    Line::from(Span::styled(label, style))
                })
                .collect();
            let profile_label =
                Line::from(Span::styled("Profiles:", Style::default().fg(Color::Cyan)));
            let mut all_profile_lines = vec![profile_label];
            all_profile_lines.extend(profile_lines);
            let profile_visible_rows = (chunks[3].height as usize).saturating_sub(1);
            app.pipeline
                .pipeline_edit_profile_visible
                .set(profile_visible_rows);
            let profile_scroll_offset = app.pipeline.pipeline_edit_profile_scroll as u16;
            let profile_p = Paragraph::new(all_profile_lines).scroll((profile_scroll_offset, 0));
            f.render_widget(profile_p, chunks[3]);
        }
    } else {
        // Collapsed summary line — append [!] to orphaned profile names
        let selected_names: Vec<String> = app
            .pipeline
            .pipeline_edit_profile_list
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                app.pipeline
                    .pipeline_edit_profile_selection
                    .get(*i)
                    .copied()
                    .unwrap_or(false)
            })
            .map(|(_, name)| {
                if app.pipeline.pipeline_edit_profile_orphaned.contains(name) {
                    format!("{name} [!]")
                } else {
                    name.clone()
                }
            })
            .collect();
        let name_refs: Vec<&str> = selected_names.iter().map(|s| s.as_str()).collect();
        let max_w = chunks[3].width.saturating_sub(11) as usize; // "Profiles: " = 10 chars + margin
        let summary = if app.pipeline.pipeline_edit_profile_list.is_empty() {
            "(none found)".to_string()
        } else {
            collapse_summary(&name_refs, max_w, "none")
        };
        let has_orphans = selected_names.iter().any(|s| s.ends_with("[!]"));
        let summary_color = if has_orphans {
            Color::Yellow
        } else {
            Color::DarkGray
        };
        let line = Line::from(vec![
            Span::styled("Profiles: ", Style::default().fg(Color::White)),
            Span::styled(summary, Style::default().fg(summary_color)),
        ]);
        f.render_widget(Paragraph::new(line), chunks[3]);
    }

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

    if prompt_focus
        && prompt_inner.width > 0
        && prompt_inner.height > 0
        && app.error_modal.is_none()
        && app.info_modal.is_none()
    {
        let visible_row = prompt_cursor_row.saturating_sub(prompt_scroll as usize);
        let cx = prompt_inner.x
            + (prompt_cursor_col.min(prompt_inner.width.saturating_sub(1) as usize) as u16);
        let cy = prompt_inner.y
            + (visible_row.min(prompt_inner.height.saturating_sub(1) as usize) as u16);
        f.set_cursor_position((cx, cy));
    }

    // Session ID (hidden for finalization blocks — they use runtime-managed sessions)
    let is_fin_block = app
        .pipeline
        .pipeline_block_cursor
        .map(|sel| app.pipeline.pipeline_def.is_finalization_block(sel))
        .unwrap_or(false);
    if is_fin_block {
        let sess_line = Line::from(vec![
            Span::styled("Session ID: ", Style::default().fg(Color::White)),
            Span::styled("(managed by runtime)", Style::default().fg(Color::DarkGray)),
        ]);
        f.render_widget(Paragraph::new(sess_line), chunks[6]);
    } else {
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
    }

    // Replicas
    let rep_focus = app.pipeline.pipeline_edit_field == PipelineEditField::Replicas;
    let rep_style = if rep_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let selected_agents = app
        .pipeline
        .pipeline_edit_agent_selection
        .iter()
        .filter(|&&s| s)
        .count()
        .max(1) as u32;
    let too_many_agents = selected_agents > 32;
    let max_replicas = (32 / selected_agents).max(1);
    let rep_line = if too_many_agents {
        Line::from(vec![
            Span::styled("Replicas: ", Style::default().fg(Color::White)),
            Span::styled("[", rep_style),
            Span::raw(&app.pipeline.pipeline_edit_replicas_buf),
            Span::styled("]", rep_style),
            Span::styled(" too many agents (max 32)", Style::default().fg(Color::Red)),
        ])
    } else {
        let rep_hint = format!(" (1-{max_replicas})");
        Line::from(vec![
            Span::styled("Replicas: ", Style::default().fg(Color::White)),
            Span::styled("[", rep_style),
            Span::raw(&app.pipeline.pipeline_edit_replicas_buf),
            Span::styled("]", rep_style),
            Span::styled(rep_hint, Style::default().fg(Color::DarkGray)),
        ])
    };
    f.render_widget(Paragraph::new(rep_line), chunks[8]);

    // Hint
    let hint = Paragraph::new("  Tab/S-Tab: nav  Space: toggle  Enter: save  Esc: back")
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

            let sections = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1),
                    Constraint::Length(1),
                    Constraint::Min(1),
                ])
                .split(inner);

            // Search input row
            let search_style = if app.pipeline.pipeline_file_search_focus {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let cursor = if app.pipeline.pipeline_file_search_focus {
                "_"
            } else {
                ""
            };
            let search_line = Line::from(vec![
                Span::styled("Search: ", search_style),
                Span::raw(format!("{}{}", &app.pipeline.pipeline_file_search, cursor)),
            ]);
            f.render_widget(Paragraph::new(search_line), sections[0]);

            // Match count hint
            let total = app.pipeline.pipeline_file_list.len();
            let matched = app.pipeline.pipeline_file_filtered.len();
            let hint = if app.pipeline.pipeline_file_search.is_empty() {
                format!("{total} pipelines")
            } else {
                format!("{matched}/{total} matches")
            };
            f.render_widget(
                Paragraph::new(hint).style(Style::default().fg(Color::DarkGray)),
                sections[1],
            );

            // Publish actual viewport height so the input handler can scroll correctly.
            let file_list_rows = sections[2].height as usize;
            app.pipeline.pipeline_file_visible.set(file_list_rows);

            // File list from filtered indices
            if app.pipeline.pipeline_file_filtered.is_empty() {
                let msg = if app.pipeline.pipeline_file_list.is_empty() {
                    "No pipeline files found"
                } else {
                    "No matches"
                };
                f.render_widget(
                    Paragraph::new(msg).style(Style::default().fg(Color::DarkGray)),
                    sections[2],
                );
            } else {
                let scroll = app.pipeline.pipeline_file_scroll;
                let items: Vec<Line> = app
                    .pipeline
                    .pipeline_file_filtered
                    .iter()
                    .enumerate()
                    .filter_map(|(vi, &orig_i)| {
                        let name = app.pipeline.pipeline_file_list.get(orig_i)?;
                        let style = if vi == app.pipeline.pipeline_file_cursor {
                            Style::default()
                                .fg(Color::Cyan)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default()
                        };
                        Some(Line::from(Span::styled(name.as_str(), style)))
                    })
                    .collect();
                let list = Paragraph::new(items).scroll((scroll as u16, 0));
                f.render_widget(list, sections[2]);
            }
        }
        None => {}
    }
}

fn draw_session_config_popup(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(60, 50, area);
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
    let col = app.pipeline.pipeline_session_config_col;
    let iter_header_style = if col == 0 {
        Style::default()
            .add_modifier(Modifier::BOLD)
            .add_modifier(Modifier::UNDERLINED)
    } else {
        Style::default().add_modifier(Modifier::BOLD)
    };
    let loop_header_style = if col == 1 {
        Style::default()
            .add_modifier(Modifier::BOLD)
            .add_modifier(Modifier::UNDERLINED)
    } else {
        Style::default().add_modifier(Modifier::BOLD)
    };
    let header = Line::from(vec![
        Span::styled(
            format!("{:<15}", "Agent"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:<15}", "Session"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(format!("{:<6}", "Iter"), iter_header_style),
        Span::styled(format!("{:<6}", "Loop"), loop_header_style),
        Span::styled("\u{00d7}N", Style::default().add_modifier(Modifier::BOLD)),
    ]);
    f.render_widget(
        Paragraph::new(header),
        Rect::new(inner.x, inner.y, inner.width, 1),
    );

    // Footer
    let footer = Paragraph::new("j/k: navigate | h/l: column | Space/Enter: toggle | Esc: close")
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

    for (vi, si) in (scroll_offset..sessions.len())
        .take(visible_rows)
        .enumerate()
    {
        let session = &sessions[si];
        let is_selected = si == cursor;

        let agent_col = fit_display_width(&session.agent, 15);
        let label_col = fit_display_width(&session.display_label, 15);
        let iter_col = if session.keep_across_iterations {
            "[x]"
        } else {
            "[ ]"
        };
        let loop_col = if session.keep_across_loop_passes {
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
            Style::default().fg(Color::Black).bg(Color::Cyan)
        } else {
            Style::default()
        };

        let iter_style = {
            let base = if is_selected && col == 0 {
                style.bg(Color::White)
            } else {
                style
            };
            if session.keep_across_iterations {
                base.fg(if is_selected {
                    Color::Black
                } else {
                    Color::Green
                })
            } else {
                base.fg(if is_selected {
                    Color::Black
                } else {
                    Color::Red
                })
            }
        };

        let loop_style = {
            let base = if is_selected && col == 1 {
                style.bg(Color::White)
            } else {
                style
            };
            if session.keep_across_loop_passes {
                base.fg(if is_selected {
                    Color::Black
                } else {
                    Color::Green
                })
            } else {
                base.fg(if is_selected {
                    Color::Black
                } else {
                    Color::Red
                })
            }
        };

        let row = Line::from(vec![
            Span::styled(agent_col, style),
            Span::styled(label_col, style),
            Span::styled(format!("{iter_col:<6}"), iter_style),
            Span::styled(format!("{loop_col:<6}"), loop_style),
            Span::styled(replicas_col, style),
        ]);

        let row_y = inner.y + 1 + vi as u16; // +1 for header
        f.render_widget(
            Paragraph::new(row),
            Rect::new(inner.x, row_y, inner.width, 1),
        );
    }
}

fn draw_loop_edit_popup(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(50, 40, area);
    f.render_widget(Clear, popup);

    let (from_name, to_name) =
        if let Some((from_id, to_id)) = app.pipeline.pipeline_loop_edit_target {
            let fname = app
                .pipeline
                .pipeline_def
                .blocks
                .iter()
                .find(|b| b.id == from_id)
                .map(|b| {
                    if b.name.is_empty() {
                        format!("Block {}", b.id)
                    } else {
                        b.name.clone()
                    }
                })
                .unwrap_or_else(|| format!("#{from_id}"));
            let tname = app
                .pipeline
                .pipeline_def
                .blocks
                .iter()
                .find(|b| b.id == to_id)
                .map(|b| {
                    if b.name.is_empty() {
                        format!("Block {}", b.id)
                    } else {
                        b.name.clone()
                    }
                })
                .unwrap_or_else(|| format!("#{to_id}"));
            (fname, tname)
        } else {
            ("?".into(), "?".into())
        };

    let title = format!(" Loop: {from_name} \u{2192} {to_name} ");
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
        Span::styled(
            " (1-99, returns from target to source)",
            Style::default().fg(Color::DarkGray),
        ),
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

    if prompt_focus
        && prompt_inner.width > 0
        && prompt_inner.height > 0
        && app.error_modal.is_none()
        && app.info_modal.is_none()
    {
        let visible_row = prompt_cursor_row.saturating_sub(prompt_scroll as usize);
        let cx = prompt_inner.x
            + (prompt_cursor_col.min(prompt_inner.width.saturating_sub(1) as usize) as u16);
        let cy = prompt_inner.y
            + (visible_row.min(prompt_inner.height.saturating_sub(1) as usize) as u16);
        f.set_cursor_position((cx, cy));
    }

    // Hint
    let hint = Paragraph::new(
        "  Tab: switch field  Enter: save (from Count) / newline (from Prompt)  Esc: cancel",
    )
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(hint, chunks[3]);
}

fn draw_feed_edit_popup(f: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(50, 30, area);
    f.render_widget(Clear, popup);

    let (from_name, to_name) =
        if let Some((from_id, to_id)) = app.pipeline.pipeline_feed_edit_target {
            let fname = if from_id == 0 {
                "All execution blocks".to_string()
            } else {
                app.pipeline
                    .pipeline_def
                    .blocks
                    .iter()
                    .find(|b| b.id == from_id)
                    .map(|b| {
                        if b.name.is_empty() {
                            format!("Block {}", b.id)
                        } else {
                            b.name.clone()
                        }
                    })
                    .unwrap_or_else(|| format!("#{from_id}"))
            };
            let tname = app
                .pipeline
                .pipeline_def
                .finalization_blocks
                .iter()
                .find(|b| b.id == to_id)
                .map(|b| {
                    if b.name.is_empty() {
                        format!("Block {}", b.id)
                    } else {
                        b.name.clone()
                    }
                })
                .unwrap_or_else(|| format!("#{to_id}"));
            (fname, tname)
        } else {
            ("?".into(), "?".into())
        };

    let title = format!(" Feed: {from_name} \u{2192} {to_name} ");
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(popup);
    f.render_widget(block, popup);

    if inner.width < 10 || inner.height < 5 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Collection
            Constraint::Length(1), // spacer
            Constraint::Length(2), // Granularity
            Constraint::Length(1), // spacer
            Constraint::Length(1), // hint
        ])
        .split(inner);

    // Find the current feed values
    let (cur_collection, cur_granularity) = app
        .pipeline
        .pipeline_feed_edit_target
        .and_then(|(from_id, to_id)| {
            app.pipeline
                .pipeline_def
                .data_feeds
                .iter()
                .find(|f| f.from == from_id && f.to == to_id)
                .map(|f| (f.collection, f.granularity))
        })
        .unwrap_or((
            crate::execution::pipeline::FeedCollection::LastIteration,
            crate::execution::pipeline::FeedGranularity::PerRun,
        ));

    // Collection field
    let coll_focus = app.pipeline.pipeline_feed_edit_field == PipelineFeedEditField::Collection;
    let coll_style = if coll_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let coll_value = cur_collection.as_str();
    let coll_line = Line::from(vec![
        Span::styled("Collection: ", Style::default().fg(Color::White)),
        Span::styled(format!("< {coll_value} >"), coll_style),
        Span::styled(
            "  (Space/Enter to toggle)",
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(coll_line), chunks[0]);

    // Granularity field
    let gran_focus = app.pipeline.pipeline_feed_edit_field == PipelineFeedEditField::Granularity;
    let gran_style = if gran_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let gran_value = cur_granularity.as_str();
    let gran_line = Line::from(vec![
        Span::styled("Granularity: ", Style::default().fg(Color::White)),
        Span::styled(format!("< {gran_value} >"), gran_style),
        Span::styled(
            "  (Space/Enter to toggle)",
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(gran_line), chunks[2]);

    // Hint
    let hint = Paragraph::new("  Tab: switch field  Space/Enter: toggle value  Esc: close")
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(hint, chunks[4]);
}

fn draw_feed_list_popup(f: &mut Frame, app: &App, area: Rect) {
    let fin_id = match app.pipeline.pipeline_feed_list_target {
        Some(id) => id,
        None => return,
    };

    let popup = centered_rect(50, 30, area);
    f.render_widget(Clear, popup);

    let fin_name = app
        .pipeline
        .pipeline_def
        .finalization_blocks
        .iter()
        .find(|b| b.id == fin_id)
        .map(|b| {
            if b.name.is_empty() {
                format!("Block {}", b.id)
            } else {
                b.name.clone()
            }
        })
        .unwrap_or_else(|| format!("#{fin_id}"));

    let title = format!(" Feeds \u{2192} {fin_name} ");
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(popup);
    f.render_widget(block, popup);

    if inner.width < 10 || inner.height < 3 {
        return;
    }

    let feeds: Vec<&crate::execution::pipeline::DataFeed> = app
        .pipeline
        .pipeline_def
        .data_feeds
        .iter()
        .filter(|df| df.to == fin_id)
        .collect();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),    // feed list (scrollable)
            Constraint::Length(1), // hint
        ])
        .split(inner);

    let cursor = app.pipeline.pipeline_feed_list_cursor;
    let lines: Vec<Line> = feeds
        .iter()
        .enumerate()
        .map(|(i, feed)| {
            let source_name = if feed.from == 0 {
                "All execution blocks".to_string()
            } else {
                app.pipeline
                    .pipeline_def
                    .blocks
                    .iter()
                    .find(|b| b.id == feed.from)
                    .map(|b| {
                        if b.name.is_empty() {
                            format!("Block {}", b.id)
                        } else {
                            b.name.clone()
                        }
                    })
                    .unwrap_or_else(|| format!("#{}", feed.from))
            };
            let marker = if i == cursor { "\u{25b8} " } else { "  " };
            let style = if i == cursor {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Line::from(Span::styled(
                format!(
                    "{marker}{source_name}  [{}] [{}]",
                    feed.collection.as_str(),
                    feed.granularity.as_str()
                ),
                style,
            ))
        })
        .collect();

    let visible_rows = chunks[0].height as usize;
    let scroll = if cursor >= visible_rows {
        (cursor - visible_rows + 1) as u16
    } else {
        0
    };
    let list_p = Paragraph::new(lines).scroll((scroll, 0));
    f.render_widget(list_p, chunks[0]);

    let hint = Paragraph::new("  Up/Down: navigate | Enter: edit | F: delete | Esc: close")
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(hint, chunks[1]);
}

#[cfg(test)]
mod routing_tests {
    use super::*;
    use crate::execution::pipeline::{PipelineBlock, PipelineConnection};

    fn block_at(id: u32, col: u16, row: u16) -> PipelineBlock {
        PipelineBlock {
            id,
            name: String::new(),
            agents: vec!["test".into()],
            prompt: String::new(),
            profiles: vec![],
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
        assert_eq!(
            segs.len(),
            3,
            "forward different row (clear) = 3-segment L-path"
        );
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
