use crate::app::App;
use crate::memory::types::MemoryKind;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

fn kind_color(kind: MemoryKind) -> Color {
    match kind {
        MemoryKind::Decision => Color::Green,
        MemoryKind::Observation => Color::Blue,
        MemoryKind::Summary => Color::Yellow,
        MemoryKind::Principle => Color::Magenta,
    }
}

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Min(0),    // Content
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title
    let mut filter_parts = Vec::new();
    if let Some(kind) = app.memory.management_kind_filter {
        filter_parts.push(kind.as_str().to_string());
    }
    if app.memory.management_never_recalled_filter {
        filter_parts.push("never recalled".to_string());
    }
    if app.memory.management_show_archived {
        filter_parts.push("archived".to_string());
    }
    let filter_text = if filter_parts.is_empty() {
        String::new()
    } else {
        format!(" [filter: {}]", filter_parts.join(", "))
    };
    let db_size_text = app
        .memory
        .cached_db_size
        .map(|b| {
            if b < 1024 {
                format!(", {b} B")
            } else if b < 1024 * 1024 {
                format!(", {} KB", b / 1024)
            } else {
                format!(", {:.1} MB", b as f64 / (1024.0 * 1024.0))
            }
        })
        .unwrap_or_default();
    let mut title_spans = vec![
        Span::styled(
            " Memory Management ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            {
                let loaded = app.memory.management_memories.len() as u64;
                let total = app.memory.management_total_count;
                if total > loaded {
                    format!("(showing {loaded} of {total} memories{db_size_text}{filter_text})")
                } else {
                    format!("({loaded} memories{db_size_text}{filter_text})")
                }
            },
            Style::default().fg(Color::DarkGray),
        ),
    ];
    if app.memory.pending_bulk_delete {
        title_spans.push(Span::styled(
            "  Press D again to confirm bulk delete",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    }
    let title =
        Paragraph::new(Line::from(title_spans)).block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Content: split into list + detail
    let content_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(chunks[1]);

    // Left: memory list
    let items: Vec<ListItem> = app
        .memory
        .management_memories
        .iter()
        .map(|mem| {
            let prefix = format!(
                "[{}] ",
                mem.kind
                    .as_str()
                    .chars()
                    .next()
                    .unwrap_or('?')
                    .to_uppercase()
                    .next()
                    .unwrap_or('?')
            );
            let content_preview = if mem.content.chars().count() > 60 {
                let truncated: String = mem.content.chars().take(57).collect();
                format!("{truncated}...")
            } else {
                mem.content.clone()
            };
            let style = if app.memory.management_show_archived {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(vec![
                Span::styled(
                    prefix,
                    if app.memory.management_show_archived {
                        Style::default().fg(Color::DarkGray)
                    } else {
                        Style::default().fg(kind_color(mem.kind))
                    },
                ),
                Span::styled(content_preview, style),
            ]))
        })
        .collect();

    let list = List::new(items)
        .block(
            Block::default()
                .title(" Memories ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );

    let mut list_state = ListState::default();
    if !app.memory.management_memories.is_empty() {
        list_state.select(Some(app.memory.management_cursor));
    }
    f.render_stateful_widget(list, content_chunks[0], &mut list_state);

    // Right: detail panel
    let detail = if let Some(mem) = app
        .memory
        .management_memories
        .get(app.memory.management_cursor)
    {
        let mut lines = vec![
            Line::from(vec![
                Span::styled("Kind: ", Style::default().fg(Color::Yellow)),
                Span::styled(mem.kind.as_str(), Style::default().fg(kind_color(mem.kind))),
            ]),
            Line::from(""),
        ];
        if mem.archived {
            lines.push(Line::from(Span::styled(
                "ARCHIVED",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(""));
        }
        lines.extend([
            Line::from(Span::styled("Content:", Style::default().fg(Color::Yellow))),
            Line::from(mem.content.as_str()),
            Line::from(""),
        ]);
        if !mem.reasoning.is_empty() {
            lines.push(Line::from(Span::styled(
                "Reasoning:",
                Style::default().fg(Color::Yellow),
            )));
            lines.push(Line::from(mem.reasoning.as_str()));
            lines.push(Line::from(""));
        }
        if !mem.tags.is_empty() {
            lines.push(Line::from(vec![
                Span::styled("Tags: ", Style::default().fg(Color::Yellow)),
                Span::raw(&mem.tags),
            ]));
        }
        if !mem.source_agent.is_empty() {
            lines.push(Line::from(vec![
                Span::styled("Agent: ", Style::default().fg(Color::Yellow)),
                Span::raw(&mem.source_agent),
            ]));
        }
        if mem.evidence_count > 1 {
            lines.push(Line::from(vec![
                Span::styled("Evidence: ", Style::default().fg(Color::Yellow)),
                Span::raw(format!("{}", mem.evidence_count)),
            ]));
        }
        if mem.recall_count > 0 {
            lines.push(Line::from(vec![
                Span::styled("Recalled: ", Style::default().fg(Color::Yellow)),
                Span::raw(format!("{} times", mem.recall_count)),
            ]));
        }
        if let Some(ref recalled_at) = mem.last_recalled_at {
            lines.push(Line::from(vec![
                Span::styled("Last recalled: ", Style::default().fg(Color::DarkGray)),
                Span::raw(recalled_at.chars().take(19).collect::<String>()),
            ]));
        }
        lines.push(Line::from(vec![
            Span::styled("Created: ", Style::default().fg(Color::DarkGray)),
            Span::raw(mem.created_at.chars().take(19).collect::<String>()),
        ]));
        if let Some(ref exp) = mem.expires_at {
            lines.push(Line::from(vec![
                Span::styled("Expires: ", Style::default().fg(Color::DarkGray)),
                Span::raw(exp.chars().take(19).collect::<String>()),
            ]));
        }
        Paragraph::new(lines).wrap(Wrap { trim: false })
    } else {
        Paragraph::new("No memories")
    };

    let detail_block = Block::default()
        .title(" Detail ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    f.render_widget(detail.block(detail_block), content_chunks[1]);

    // Help bar
    let mut help_spans = vec![
        Span::styled("j/k", Style::default().fg(Color::Yellow)),
        Span::raw(": navigate  "),
        Span::styled("d", Style::default().fg(Color::Yellow)),
        Span::raw(": delete  "),
        Span::styled("D", Style::default().fg(Color::Yellow)),
        Span::raw(": bulk delete  "),
        Span::styled("f", Style::default().fg(Color::Yellow)),
        Span::raw(": filter kind  "),
        Span::styled("r", Style::default().fg(Color::Yellow)),
        Span::raw(": never recalled  "),
        Span::styled("a", Style::default().fg(Color::Yellow)),
        Span::raw(": archived  "),
    ];
    if app.memory.management_show_archived {
        help_spans.push(Span::styled("u", Style::default().fg(Color::Yellow)));
        help_spans.push(Span::raw(": unarchive  "));
    }
    help_spans.push(Span::styled("q/Esc", Style::default().fg(Color::Yellow)));
    help_spans.push(Span::raw(": back"));
    let help = Paragraph::new(Line::from(help_spans)).block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[2]);
}
