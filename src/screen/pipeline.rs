use crate::app::{App, PipelineDialogMode, PipelineEditField, PipelineFocus};
use crate::execution::pipeline::BlockId;
use crate::execution::truncate_chars;
use crate::screen::centered_rect;
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
            Constraint::Length(3), // title
            Constraint::Length(6), // prompt + session/iterations
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
        .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
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

    let wrapped = char_wrap_text(display_text, inner.width as usize);
    let prompt_p = Paragraph::new(wrapped.as_str())
        .style(text_style)
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

    // Right column: session name + iterations
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(cols[1]);

    // Session name field
    let name_focus = app.pipeline_focus == PipelineFocus::SessionName;
    let name_border = if name_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let name_display = if app.pipeline_session_name.is_empty() {
        if name_focus { "_".to_string() } else { "(optional)".to_string() }
    } else if name_focus {
        format!("{}_", app.pipeline_session_name)
    } else {
        app.pipeline_session_name.clone()
    };
    let name_style = if app.pipeline_session_name.is_empty() && !name_focus {
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
    let iter_focus = app.pipeline_focus == PipelineFocus::Iterations;
    let iter_border = if iter_focus {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let iter_display = if iter_focus {
        format!("{}_", app.pipeline_iterations_buf)
    } else {
        app.pipeline_iterations_buf.clone()
    };
    let iter_widget = Paragraph::new(iter_display).block(
        Block::default()
            .title(" Iterations ")
            .borders(Borders::ALL)
            .border_style(iter_border),
    );
    f.render_widget(iter_widget, right_chunks[1]);
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

    // ── Connection rendering (two-phase: route then paint) ──
    let grid_occ = grid_occupancy(&app.pipeline_def.blocks);
    let lanes = assign_lanes(&app.pipeline_def.connections, &app.pipeline_def.blocks);
    let ports = assign_ports(&app.pipeline_def.connections, &app.pipeline_def.blocks);

    // Resolve pipeline_conn_cursor (index into filtered subset) to global connection index
    let highlighted_global_idx = if app.pipeline_removing_conn {
        let sel = app.pipeline_block_cursor.unwrap_or(0);
        app.pipeline_def
            .connections
            .iter()
            .enumerate()
            .filter(|(_, c)| c.from == sel || c.to == sel)
            .nth(app.pipeline_conn_cursor)
            .map(|(i, _)| i)
    } else {
        None
    };

    let mut rendered_connections: Vec<(u8, ConnectionRaster)> = Vec::new();

    for (ci, conn) in app.pipeline_def.connections.iter().enumerate() {
        let fb = app.pipeline_def.blocks.iter().find(|b| b.id == conn.from);
        let tb = app.pipeline_def.blocks.iter().find(|b| b.id == conn.to);
        let (Some(fb), Some(tb)) = (fb, tb) else { continue };

        let removing = app.pipeline_removing_conn
            && is_conn_for_selected(app, conn.from, conn.to);
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
                    WireCell { dirs: 0, color, is_arrow: true, arrow_char: arrow_ch },
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
            if pixel_hits_block(wx, wy, &app.pipeline_def.blocks) {
                continue;
            }
            let ch = if cell.is_arrow { cell.arrow_char } else { dirs_to_char(cell.dirs) };
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
        (fa.0, ta.0, fa.1, ta.1, ca.from, ca.to)
            .cmp(&(fb.0, tb.0, fb.1, tb.1, cb.from, cb.to))
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
    let bmap: HashMap<BlockId, (u16, u16)> =
        blocks.iter().map(|b| (b.id, b.position)).collect();

    let mut outgoing: HashMap<BlockId, Vec<usize>> = HashMap::new();
    let mut incoming: HashMap<BlockId, Vec<usize>> = HashMap::new();
    for (ci, conn) in conns.iter().enumerate() {
        outgoing.entry(conn.from).or_default().push(ci);
        incoming.entry(conn.to).or_default().push(ci);
    }

    // Symmetric offset sequence: 0, -1, +1, -2, +2, ...
    let offset_seq = |n: usize| -> Vec<i16> {
        let mut offsets = Vec::with_capacity(n);
        offsets.push(0);
        let mut d = 1i16;
        while offsets.len() < n {
            offsets.push(-d);
            if offsets.len() < n {
                offsets.push(d);
            }
            d += 1;
        }
        offsets
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
                return vec![WireSeg { x1: ex, y1: ey, x2: nx, y2: ny }];
            }
            let mid_x = (ex + nx) / 2;
            return vec![
                WireSeg { x1: ex, y1: ey, x2: mid_x, y2: ey },
                WireSeg { x1: mid_x, y1: ey, x2: mid_x, y2: ny },
                WireSeg { x1: mid_x, y1: ny, x2: nx, y2: ny },
            ];
        }
    }

    // Case B: Forward different row (or blocked same-row forward)
    // Case C: Backward or same column
    // Both use 5-segment U-route with monotonic lane/tier separation.
    let gap = (CELL_W as i16 - BLOCK_W as i16).max(2);
    let side_lane = ((lane % gap as usize) as i16) * 2 + 1;
    let side_clamp = gap - 1;
    let tier = lane as i16;
    // Keep the horizontal corridor in row gaps (between block rows) for all tiers.
    // Advancing by CELL_H preserves the same in-gap offset and avoids cutting through blocks.
    let hy = fr.max(tr) as i16 * CELL_H as i16
        + BLOCK_H as i16
        + 1
        + tier * CELL_H as i16;
    let v1 = ex + side_lane.min(side_clamp);
    let v2 = nx - side_lane.min(side_clamp);

    vec![
        WireSeg { x1: ex, y1: ey, x2: v1, y2: ey },
        WireSeg { x1: v1, y1: ey, x2: v1, y2: hy },
        WireSeg { x1: v1, y1: hy, x2: v2, y2: hy },
        WireSeg { x1: v2, y1: hy, x2: v2, y2: ny },
        WireSeg { x1: v2, y1: ny, x2: nx, y2: ny },
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
        let (lo, hi) = if seg.x1 <= seg.x2 { (seg.x1, seg.x2) } else { (seg.x2, seg.x1) };
        for x in lo..=hi {
            let c = map
                .entry((x, y))
                .or_insert(WireCell { dirs: 0, color, is_arrow: false, arrow_char: ' ' });
            if x > lo { c.dirs |= DIR_W; }
            if x < hi { c.dirs |= DIR_E; }
            if color_rank(color) > color_rank(c.color) { c.color = color; }
        }
    } else {
        let x = seg.x1;
        let (lo, hi) = if seg.y1 <= seg.y2 { (seg.y1, seg.y2) } else { (seg.y2, seg.y1) };
        for y in lo..=hi {
            let c = map
                .entry((x, y))
                .or_insert(WireCell { dirs: 0, color, is_arrow: false, arrow_char: ' ' });
            if y > lo { c.dirs |= DIR_N; }
            if y < hi { c.dirs |= DIR_S; }
            if color_rank(color) > color_rank(c.color) { c.color = color; }
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
        "Arrows/hjkl: select | Shift+Arrows/HJKL: move block | Ctrl+Arrows: scroll | a: add | d: delete | e: edit | c: connect | x: disconnect | Ctrl+S: save | Ctrl+L: load | F5: run | ?: help | Esc: back"
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

    let (prompt_scroll, prompt_cursor_col, prompt_cursor_row) = if prompt_focus {
        prompt_cursor_layout(
            &app.pipeline_edit_prompt_buf,
            app.pipeline_edit_prompt_cursor,
            prompt_inner.width as usize,
            prompt_inner.height as usize,
        )
    } else {
        (0, 0, 0)
    };

    let edit_wrapped = char_wrap_text(&app.pipeline_edit_prompt_buf, prompt_inner.width as usize);
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
                Span::raw(format!("{}_", &app.pipeline_file_input)),
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
        }
    }

    fn no_seg_hits_block(segs: &[WireSeg], blocks: &[PipelineBlock]) {
        for seg in segs {
            if seg.y1 == seg.y2 {
                let y = seg.y1;
                let (lo, hi) = if seg.x1 <= seg.x2 { (seg.x1, seg.x2) } else { (seg.x2, seg.x1) };
                for x in lo..=hi {
                    assert!(
                        !pixel_hits_block(x, y, blocks),
                        "wire pixel ({x}, {y}) hits a block"
                    );
                }
            } else {
                let x = seg.x1;
                let (lo, hi) = if seg.y1 <= seg.y2 { (seg.y1, seg.y2) } else { (seg.y2, seg.y1) };
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
        assert_eq!(segs.len(), 3, "same-row clear with different port offsets = 3 segments");
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
        assert_eq!(segs.len(), 5, "forward different row = 5 segments");
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
        assert_eq!(segs.len(), 5, "same column different row = backward U-route = 5 segments");
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
            &WireSeg { x1: 3, y1: 5, x2: 7, y2: 5 },
            Color::DarkGray,
            &mut map,
        );
        // Vertical segment through (5,5)
        rasterize_seg(
            &WireSeg { x1: 5, y1: 3, x2: 5, y2: 7 },
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
            &WireSeg { x1: 3, y1: 5, x2: 5, y2: 5 },
            Color::DarkGray,
            &mut map,
        );
        // Vertical starting at (5,5) going down
        rasterize_seg(
            &WireSeg { x1: 5, y1: 5, x2: 5, y2: 8 },
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
            &WireSeg { x1: 0, y1: 0, x2: 2, y2: 0 },
            Color::DarkGray,
            &mut map,
        );
        rasterize_seg(
            &WireSeg { x1: 0, y1: 0, x2: 2, y2: 0 },
            Color::Yellow,
            &mut map,
        );
        rasterize_seg(
            &WireSeg { x1: 0, y1: 0, x2: 2, y2: 0 },
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
            &WireSeg { x1: 5, y1: 5, x2: 10, y2: 5 },
            Color::Red,
            &mut wire_map,
        );
        arrows.push(((10, 5), Color::Red));

        // Simulate a DarkGray connection also ending at (10, 5)
        rasterize_seg(
            &WireSeg { x1: 5, y1: 3, x2: 10, y2: 3 },
            Color::DarkGray,
            &mut wire_map,
        );
        rasterize_seg(
            &WireSeg { x1: 10, y1: 3, x2: 10, y2: 5 },
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
        assert_eq!(cell.color, Color::Red, "DarkGray arrow must not downgrade Red");
    }

    #[test]
    fn highlight_mapping_uses_filtered_index() {
        // pipeline_conn_cursor indexes a filtered subset; the drawing code must
        // map it to the correct global connection index.
        let blocks = vec![
            block_at(1, 0, 0),
            block_at(2, 1, 0),
            block_at(3, 2, 0),
        ];
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
        assert_eq!(arrow_for_seg(&WireSeg { x1: 0, y1: 0, x2: 5, y2: 0 }), '▶');
        assert_eq!(arrow_for_seg(&WireSeg { x1: 5, y1: 0, x2: 0, y2: 0 }), '◀');
        assert_eq!(arrow_for_seg(&WireSeg { x1: 0, y1: 0, x2: 0, y2: 5 }), '▼');
        assert_eq!(arrow_for_seg(&WireSeg { x1: 0, y1: 5, x2: 0, y2: 0 }), '▲');
        assert_eq!(arrow_for_seg(&WireSeg { x1: 0, y1: 0, x2: 0, y2: 0 }), '▶');
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
        assert_eq!(unique_exits.len(), exit_offsets.len(), "exit offsets must be distinct");

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
        assert_eq!(unique_entries.len(), entry_offsets.len(), "entry offsets must be distinct");
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
                    let (lo, hi) = if seg.x1 <= seg.x2 { (seg.x1, seg.x2) } else { (seg.x2, seg.x1) };
                    for x in lo..=hi { pixels.insert((x, y)); }
                } else {
                    let x = seg.x1;
                    let (lo, hi) = if seg.y1 <= seg.y2 { (seg.y1, seg.y2) } else { (seg.y2, seg.y1) };
                    for y in lo..=hi { pixels.insert((x, y)); }
                }
            }
            all_pixels.push(pixels);
        }

        // Each route must have unique pixels that no other route shares
        for i in 0..all_pixels.len() {
            let unique_count = all_pixels[i]
                .iter()
                .filter(|p| {
                    (0..all_pixels.len()).all(|j| j == i || !all_pixels[j].contains(p))
                })
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
