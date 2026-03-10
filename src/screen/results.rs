use crate::app::App;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use ratatui::Frame;
use std::path::{Path, PathBuf};
use unicode_width::UnicodeWidthStr;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4), // Title + output path
            Constraint::Min(0),    // Main content
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title
    let run_dir_display = app
        .running
        .run_dir
        .as_ref()
        .map(|p| absolute_path(p).display().to_string())
        .unwrap_or_else(|| "(unknown)".to_string());

    let title = Paragraph::new(vec![
        Line::from(Span::styled(
            "Results",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled("Stored at: ", Style::default().fg(Color::DarkGray)),
            Span::styled(run_dir_display, Style::default().fg(Color::Yellow)),
        ]),
    ])
    .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Main content: file list + preview
    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(chunks[1]);

    let batch_items = has_batch_results(app).then(|| visible_batch_items(app));

    // File list
    let items: Vec<ListItem> = if let Some(items) = batch_items.as_ref() {
        items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let style = if i == app.results.result_cursor {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };

                let label = match item {
                    VisibleBatchItem::RunHeader { run_id, expanded } => {
                        format!("{} Run {run_id}", if *expanded { "▾" } else { "▸" })
                    }
                    VisibleBatchItem::File { depth, path } => {
                        let name = path
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_default();
                        format!("{}{}", "  ".repeat(*depth), name)
                    }
                };

                ListItem::new(label).style(style)
            })
            .collect()
    } else {
        app.results
            .result_files
            .iter()
            .enumerate()
            .map(|(i, path)| {
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();

                let style = if i == app.results.result_cursor {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };

                ListItem::new(name).style(style)
            })
            .collect()
    };

    let list = List::new(items).block(
        Block::default()
            .title(" Files ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(list, main_chunks[0]);

    // Preview pane
    let selected = selected_file_ref(app, batch_items.as_deref());
    let preview = Paragraph::new(render_preview(&app.results.result_preview, selected))
        .wrap(Wrap { trim: false })
        .block(Block::default().title(" Preview ").borders(Borders::ALL));
    f.render_widget(preview, main_chunks[1]);

    // Help bar
    let help = Paragraph::new(Line::from(if batch_items.is_some() {
        vec![
            Span::styled("j/k", Style::default().fg(Color::Yellow)),
            Span::raw(": navigate  "),
            Span::styled("Enter/l", Style::default().fg(Color::Yellow)),
            Span::raw(": expand/collapse  "),
            Span::styled("Esc", Style::default().fg(Color::Yellow)),
            Span::raw(": new run  "),
            Span::styled("q", Style::default().fg(Color::Yellow)),
            Span::raw(": quit"),
        ]
    } else {
        vec![
            Span::styled("j/k", Style::default().fg(Color::Yellow)),
            Span::raw(": navigate  "),
            Span::styled("Enter", Style::default().fg(Color::Yellow)),
            Span::raw(": new run  "),
            Span::styled("q", Style::default().fg(Color::Yellow)),
            Span::raw(": quit"),
        ]
    }))
    .block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[2]);
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path.to_path_buf(),
    }
}

fn selected_file_ref<'a>(
    app: &'a App,
    batch_items: Option<&'a [VisibleBatchItem<'a>]>,
) -> Option<&'a PathBuf> {
    if let Some(items) = batch_items {
        match items.get(app.results.result_cursor) {
            Some(VisibleBatchItem::File { path, .. }) => Some(*path),
            _ => None,
        }
    } else {
        app.results.result_files.get(app.results.result_cursor)
    }
}

fn has_batch_results(app: &App) -> bool {
    !app.results.batch_result_runs.is_empty() || !app.results.batch_result_root_files.is_empty()
}

enum VisibleBatchItem<'a> {
    RunHeader { run_id: u32, expanded: bool },
    File { depth: usize, path: &'a PathBuf },
}

fn visible_batch_items(app: &App) -> Vec<VisibleBatchItem<'_>> {
    let mut items = Vec::new();
    for run in &app.results.batch_result_runs {
        let expanded = app.results.batch_result_expanded.contains(&run.run_id);
        items.push(VisibleBatchItem::RunHeader {
            run_id: run.run_id,
            expanded,
        });
        if expanded {
            for path in &run.files {
                items.push(VisibleBatchItem::File { depth: 1, path });
            }
        }
    }
    for path in &app.results.batch_result_root_files {
        items.push(VisibleBatchItem::File { depth: 0, path });
    }
    items
}

fn render_preview(content: &str, selected_file: Option<&PathBuf>) -> Text<'static> {
    if is_markdown_file(selected_file) {
        render_markdown(content)
    } else {
        Text::from(content.to_string())
    }
}

fn is_markdown_file(path: Option<&PathBuf>) -> bool {
    let ext = path
        .and_then(|p| p.extension())
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    matches!(ext.as_deref(), Some("md") | Some("markdown") | Some("mdx"))
}

#[derive(Default)]
struct MarkdownState {
    in_code_block: bool,
    in_heading: bool,
    in_strong: usize,
    in_emphasis: usize,
    list_depth: usize,
    quote_depth: usize,
    item_prefix_needed: bool,
    // Table state — rows buffered until End(Table) for correct column alignment
    in_table: bool,
    in_table_head: bool,
    table_head_count: usize,
    table_rows: Vec<Vec<Vec<Span<'static>>>>,
    current_table_row: Vec<Vec<Span<'static>>>,
    current_cell: Vec<Span<'static>>,
}

pub(crate) fn render_markdown(markdown: &str) -> Text<'static> {
    let mut state = MarkdownState::default();
    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut current: Vec<Span<'static>> = Vec::new();

    let mut parser_opts = Options::empty();
    parser_opts.insert(Options::ENABLE_STRIKETHROUGH);
    parser_opts.insert(Options::ENABLE_TABLES);
    parser_opts.insert(Options::ENABLE_TASKLISTS);

    for event in Parser::new_ext(markdown, parser_opts) {
        match event {
            Event::Start(tag) => match tag {
                Tag::Paragraph => {}
                Tag::Heading { .. } => {
                    flush_line(&mut lines, &mut current, false);
                    state.in_heading = true;
                }
                Tag::List(_) => {
                    state.list_depth += 1;
                    flush_line(&mut lines, &mut current, false);
                }
                Tag::Item => {
                    flush_line(&mut lines, &mut current, false);
                    state.item_prefix_needed = true;
                }
                Tag::BlockQuote => {
                    flush_line(&mut lines, &mut current, false);
                    state.quote_depth += 1;
                }
                Tag::CodeBlock(_) => {
                    flush_line(&mut lines, &mut current, false);
                    state.in_code_block = true;
                }
                Tag::Table(_) => {
                    flush_line(&mut lines, &mut current, false);
                    state.in_table = true;
                    state.table_rows.clear();
                    state.table_head_count = 0;
                }
                Tag::TableHead => {
                    state.in_table_head = true;
                    state.current_table_row.clear();
                }
                Tag::TableRow => {
                    state.current_table_row.clear();
                }
                Tag::TableCell => {
                    state.current_cell.clear();
                }
                Tag::Strong => state.in_strong += 1,
                Tag::Emphasis => state.in_emphasis += 1,
                _ => {}
            },
            Event::End(tag) => match tag {
                TagEnd::Paragraph => flush_line(&mut lines, &mut current, true),
                TagEnd::Heading { .. } => {
                    state.in_heading = false;
                    flush_line(&mut lines, &mut current, true);
                }
                TagEnd::List(_) => {
                    state.list_depth = state.list_depth.saturating_sub(1);
                    flush_line(&mut lines, &mut current, true);
                }
                TagEnd::Item => flush_line(&mut lines, &mut current, false),
                TagEnd::BlockQuote => {
                    state.quote_depth = state.quote_depth.saturating_sub(1);
                    flush_line(&mut lines, &mut current, true);
                }
                TagEnd::CodeBlock => {
                    state.in_code_block = false;
                    flush_line(&mut lines, &mut current, true);
                }
                TagEnd::TableCell => {
                    state
                        .current_table_row
                        .push(std::mem::take(&mut state.current_cell));
                }
                TagEnd::TableHead => {
                    if !state.current_table_row.is_empty() {
                        state
                            .table_rows
                            .push(std::mem::take(&mut state.current_table_row));
                        state.table_head_count = state.table_rows.len();
                    }
                    state.in_table_head = false;
                }
                TagEnd::TableRow => {
                    if !state.current_table_row.is_empty() {
                        state
                            .table_rows
                            .push(std::mem::take(&mut state.current_table_row));
                    }
                }
                TagEnd::Table => {
                    flush_table(&mut lines, &mut state);
                    state.in_table = false;
                }
                TagEnd::Strong => state.in_strong = state.in_strong.saturating_sub(1),
                TagEnd::Emphasis => state.in_emphasis = state.in_emphasis.saturating_sub(1),
                _ => {}
            },
            Event::Text(text) => {
                if state.in_table {
                    state
                        .current_cell
                        .push(Span::styled(text.to_string(), current_style(&state)));
                } else {
                    ensure_line_prefix(&state, &mut current);
                    current.push(Span::styled(text.to_string(), current_style(&state)));
                    state.item_prefix_needed = false;
                }
            }
            Event::Code(text) => {
                let code_style = Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD);
                if state.in_table {
                    state
                        .current_cell
                        .push(Span::styled(text.to_string(), code_style));
                } else {
                    ensure_line_prefix(&state, &mut current);
                    current.push(Span::styled(text.to_string(), code_style));
                    state.item_prefix_needed = false;
                }
            }
            Event::Rule => {
                flush_line(&mut lines, &mut current, false);
                lines.push(Line::from(Span::styled(
                    "────────────────────────",
                    Style::default().fg(Color::DarkGray),
                )));
            }
            Event::SoftBreak | Event::HardBreak => flush_line(&mut lines, &mut current, false),
            Event::Html(raw) => {
                if state.in_table {
                    state.current_cell.push(Span::styled(
                        raw.to_string(),
                        Style::default().fg(Color::DarkGray),
                    ));
                } else {
                    ensure_line_prefix(&state, &mut current);
                    current.push(Span::styled(
                        raw.to_string(),
                        Style::default().fg(Color::DarkGray),
                    ));
                    state.item_prefix_needed = false;
                }
            }
            _ => {}
        }
    }

    flush_line(&mut lines, &mut current, false);
    if lines.is_empty() {
        lines.push(Line::from(""));
    }
    Text::from(lines)
}

fn ensure_line_prefix(state: &MarkdownState, current: &mut Vec<Span<'static>>) {
    if !current.is_empty() {
        return;
    }

    if state.quote_depth > 0 {
        current.push(Span::styled(
            format!("{} ", ">".repeat(state.quote_depth)),
            Style::default().fg(Color::DarkGray),
        ));
    }

    if state.item_prefix_needed {
        let indent = "  ".repeat(state.list_depth.saturating_sub(1));
        current.push(Span::styled(
            format!("{indent}• "),
            Style::default().fg(Color::Magenta),
        ));
    }
}

fn current_style(state: &MarkdownState) -> Style {
    let mut style = if state.in_code_block {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };

    if state.in_heading {
        style = style
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
    }
    if state.in_strong > 0 {
        style = style.add_modifier(Modifier::BOLD);
    }
    if state.in_emphasis > 0 {
        style = style.add_modifier(Modifier::ITALIC);
    }

    style
}

fn flush_line(lines: &mut Vec<Line<'static>>, current: &mut Vec<Span<'static>>, force: bool) {
    if force || !current.is_empty() {
        lines.push(Line::from(std::mem::take(current)));
    }
}

/// Compute the display width of a cell's spans.
fn cell_display_width(cell: &[Span<'_>]) -> usize {
    cell.iter()
        .map(|s| UnicodeWidthStr::width(s.content.as_ref()))
        .sum()
}

/// Build prefix spans for a table line (quote markers, list indentation, optional bullet).
fn table_line_prefix(state: &MarkdownState, with_bullet: bool) -> Vec<Span<'static>> {
    let mut prefix = Vec::new();
    if state.quote_depth > 0 {
        prefix.push(Span::styled(
            format!("{} ", ">".repeat(state.quote_depth)),
            Style::default().fg(Color::DarkGray),
        ));
    }
    if with_bullet && state.list_depth > 0 {
        let indent = "  ".repeat(state.list_depth.saturating_sub(1));
        prefix.push(Span::styled(
            format!("{indent}• "),
            Style::default().fg(Color::Magenta),
        ));
    } else if state.list_depth > 0 {
        let indent = "  ".repeat(state.list_depth);
        prefix.push(Span::raw(indent));
    }
    prefix
}

fn flush_table(lines: &mut Vec<Line<'static>>, state: &mut MarkdownState) {
    let rows = std::mem::take(&mut state.table_rows);
    if rows.is_empty() {
        return;
    }

    // Compute column widths from ALL rows (header + body)
    let col_count = rows.iter().map(|r| r.len()).max().unwrap_or(0);
    let mut widths = vec![0usize; col_count];
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell_display_width(cell));
        }
    }

    let sep_style = Style::default().fg(Color::DarkGray);
    let head_extra = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);

    for (row_idx, row) in rows.iter().enumerate() {
        let is_head = row_idx < state.table_head_count;
        let with_bullet = row_idx == 0 && state.item_prefix_needed;
        let mut spans = table_line_prefix(state, with_bullet);

        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                spans.push(Span::styled(" │ ", sep_style));
            }
            for span in cell {
                let style = if is_head {
                    span.style.patch(head_extra)
                } else {
                    span.style
                };
                spans.push(Span::styled(span.content.to_string(), style));
            }
            let pad = widths[i].saturating_sub(cell_display_width(cell));
            if pad > 0 {
                spans.push(Span::raw(" ".repeat(pad)));
            }
        }
        lines.push(Line::from(spans));

        // Separator after header
        if row_idx + 1 == state.table_head_count {
            let mut sep_spans = table_line_prefix(state, false);
            for (i, &w) in widths.iter().enumerate() {
                if i > 0 {
                    sep_spans.push(Span::styled("─┼─", sep_style));
                }
                sep_spans.push(Span::styled("─".repeat(w), sep_style));
            }
            lines.push(Line::from(sep_spans));
        }
    }

    state.item_prefix_needed = false;
    state.table_head_count = 0;
    lines.push(Line::from(""));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::config::AppConfig;
    use std::collections::HashMap;

    fn app_with_files(files: Vec<PathBuf>, cursor: usize) -> App {
        let cfg = AppConfig {
            output_dir: "/tmp".to_string(),
            default_max_tokens: 4096,
            max_history_messages: 50,
            http_timeout_seconds: 120,
            model_fetch_timeout_seconds: 30,
            cli_timeout_seconds: 600,
            max_history_bytes: 102400,
            pipeline_block_concurrency: 0,
            diagnostic_provider: None,
            agents: Vec::new(),
            providers: HashMap::new(),
        };
        let mut app = App::new(cfg);
        app.results.result_files = files;
        app.results.result_cursor = cursor;
        app
    }

    #[test]
    fn absolute_path_keeps_absolute() {
        let p = PathBuf::from("/tmp/test.md");
        assert_eq!(absolute_path(&p), p);
    }

    #[test]
    fn absolute_path_converts_relative() {
        let p = PathBuf::from("test.md");
        let out = absolute_path(&p);
        assert!(out.is_absolute());
        assert!(out.ends_with("test.md"));
    }

    #[test]
    fn selected_file_returns_none_when_out_of_bounds() {
        let app = app_with_files(vec![PathBuf::from("a.md")], 5);
        assert!(selected_file_ref(&app, None).is_none());
    }

    #[test]
    fn selected_file_returns_current_file() {
        let app = app_with_files(vec![PathBuf::from("a.md"), PathBuf::from("b.md")], 1);
        assert_eq!(
            selected_file_ref(&app, None).map(|path| path.as_path()),
            Some(Path::new("b.md"))
        );
    }

    #[test]
    fn is_markdown_file_recognizes_extensions() {
        assert!(is_markdown_file(Some(&PathBuf::from("a.md"))));
        assert!(is_markdown_file(Some(&PathBuf::from("a.markdown"))));
        assert!(is_markdown_file(Some(&PathBuf::from("a.mdx"))));
    }

    #[test]
    fn is_markdown_file_rejects_non_markdown() {
        assert!(!is_markdown_file(Some(&PathBuf::from("a.txt"))));
        assert!(!is_markdown_file(None));
    }

    #[test]
    fn render_preview_plain_text_for_non_markdown() {
        let out = render_preview("hello", Some(&PathBuf::from("a.txt")));
        assert_eq!(out.lines.len(), 1);
        assert_eq!(out.lines[0].spans[0].content, "hello");
    }

    #[test]
    fn render_preview_uses_markdown_renderer() {
        let out = render_preview("# Title", Some(&PathBuf::from("a.md")));
        assert!(!out.lines.is_empty());
    }

    #[test]
    fn render_markdown_handles_heading_and_text() {
        let out = render_markdown("# Title\nBody");
        assert!(out.lines.len() >= 2);
    }

    #[test]
    fn render_markdown_handles_list_and_quote_prefixes() {
        let out = render_markdown("> quote\n- item");
        let joined = out
            .lines
            .iter()
            .flat_map(|l| l.spans.iter().map(|s| s.content.to_string()))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(joined.contains(">"));
        assert!(joined.contains("•"));
    }

    #[test]
    fn current_style_applies_heading_and_emphasis_modifiers() {
        let state = MarkdownState {
            in_code_block: false,
            in_heading: true,
            in_strong: 1,
            in_emphasis: 1,
            list_depth: 0,
            quote_depth: 0,
            item_prefix_needed: false,
            ..Default::default()
        };
        let style = current_style(&state);
        assert_eq!(style.fg, Some(Color::Cyan));
        assert!(style.add_modifier.contains(Modifier::BOLD));
        assert!(style.add_modifier.contains(Modifier::ITALIC));
    }

    #[test]
    fn flush_line_pushes_when_forced_or_non_empty() {
        let mut lines = Vec::new();
        let mut current = Vec::new();
        flush_line(&mut lines, &mut current, false);
        assert!(lines.is_empty());

        current.push(Span::raw("x"));
        flush_line(&mut lines, &mut current, false);
        assert_eq!(lines.len(), 1);
        assert!(current.is_empty());

        flush_line(&mut lines, &mut current, true);
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn render_markdown_handles_table() {
        let md = "| Name | Value |\n|------|-------|\n| a    | 1     |\n| bb   | 22    |";
        let out = render_markdown(md);
        let text: Vec<String> = out
            .lines
            .iter()
            .map(|l| {
                l.spans
                    .iter()
                    .map(|s| s.content.to_string())
                    .collect::<String>()
            })
            .collect();
        // Header row, separator, two data rows, trailing blank
        assert!(text.len() >= 4);
        // Header contains column names
        assert!(text[0].contains("Name"));
        assert!(text[0].contains("Value"));
        // Separator uses box-drawing
        assert!(text[1].contains("─"));
        // Data rows
        assert!(text[2].contains("a"));
        assert!(text[3].contains("bb"));
    }
}
