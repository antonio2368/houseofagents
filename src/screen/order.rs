use crate::app::App;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Min(0),    // Order list
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title
    let title = Paragraph::new("Relay Order — arrange agent execution sequence")
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Agent order list
    let items: Vec<ListItem> = app
        .selected_agents
        .iter()
        .enumerate()
        .map(|(i, kind)| {
            let is_cursor = i == app.order_cursor;
            let is_grabbed = app.order_grabbed == Some(i);
            let prefix = if is_grabbed {
                ">> ".to_string()
            } else {
                format!("{}. ", i + 1)
            };

            let style = if is_grabbed {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else if is_cursor {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };

            ListItem::new(format!("{prefix}{}", kind.display_name())).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Agent Order ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(list, chunks[1]);

    // Help bar
    let help = Paragraph::new(Line::from(vec![
        Span::styled("j/k", Style::default().fg(Color::Yellow)),
        Span::raw(": move  "),
        Span::styled("Space", Style::default().fg(Color::Yellow)),
        Span::raw(": grab/release  "),
        Span::styled("Enter", Style::default().fg(Color::Yellow)),
        Span::raw(": start (cursor can be first)  "),
        Span::styled("Esc", Style::default().fg(Color::Yellow)),
        Span::raw(": back"),
    ]))
    .block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[2]);
}
