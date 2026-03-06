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
            diagnostic_provider: None,
            agents: Vec::new(),
            providers: HashMap::new(),
        };
        let mut app = App::new(cfg);
        app.result_files = files;
        app.result_cursor = cursor;
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
        assert!(selected_file(&app).is_none());
    }

    #[test]
    fn selected_file_returns_current_file() {
        let app = app_with_files(vec![PathBuf::from("a.md"), PathBuf::from("b.md")], 1);
        assert_eq!(selected_file(&app), Some(&PathBuf::from("b.md")));
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
}
