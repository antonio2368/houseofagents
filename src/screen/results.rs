use crate::app::App;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use ratatui::Frame;
use std::path::{Path, PathBuf};

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

    // File list
    let items: Vec<ListItem> = app
        .result_files
        .iter()
        .enumerate()
        .map(|(i, path)| {
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let style = if i == app.result_cursor {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };

            ListItem::new(name).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Files ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(list, main_chunks[0]);

    // Preview pane
    let preview = Paragraph::new(render_preview(&app.result_preview, selected_file(app)))
        .wrap(Wrap { trim: false })
        .block(Block::default().title(" Preview ").borders(Borders::ALL));
    f.render_widget(preview, main_chunks[1]);

    // Help bar
    let help = Paragraph::new(Line::from(vec![
        Span::styled("j/k", Style::default().fg(Color::Yellow)),
        Span::raw(": navigate  "),
        Span::styled("Enter", Style::default().fg(Color::Yellow)),
        Span::raw(": new run  "),
        Span::styled("q", Style::default().fg(Color::Yellow)),
        Span::raw(": quit"),
    ]))
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

fn selected_file(app: &App) -> Option<&PathBuf> {
    app.result_files.get(app.result_cursor)
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
}

fn render_markdown(markdown: &str) -> Text<'static> {
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
                TagEnd::Strong => state.in_strong = state.in_strong.saturating_sub(1),
                TagEnd::Emphasis => state.in_emphasis = state.in_emphasis.saturating_sub(1),
                _ => {}
            },
            Event::Text(text) => {
                ensure_line_prefix(&state, &mut current);
                current.push(Span::styled(text.to_string(), current_style(&state)));
                state.item_prefix_needed = false;
            }
            Event::Code(text) => {
                ensure_line_prefix(&state, &mut current);
                current.push(Span::styled(
                    text.to_string(),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ));
                state.item_prefix_needed = false;
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
                ensure_line_prefix(&state, &mut current);
                current.push(Span::styled(
                    raw.to_string(),
                    Style::default().fg(Color::DarkGray),
                ));
                state.item_prefix_needed = false;
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
