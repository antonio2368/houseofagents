use super::centered_rect;
use crate::app::{App, ConsolidationPhase};
use crate::execution::ProgressEvent;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title + current status
            Constraint::Length(3), // Progress gauge
            Constraint::Min(0),    // Event log + detail panel
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title with current status and output dir
    let status_text = current_status(app);
    let out_dir = app
        .run_dir
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let title = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                format!("Running — {} mode", app.selected_mode),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(status_text, Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::styled("Output: ", Style::default().fg(Color::DarkGray)),
            Span::styled(out_dir, Style::default().fg(Color::DarkGray)),
        ]),
    ])
    .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Progress gauge
    let total_steps = compute_total_steps(app);
    let completed = count_completed_steps(app);
    let ratio = if total_steps > 0 {
        (completed as f64 / total_steps as f64).min(1.0)
    } else {
        0.0
    };

    let gauge = Gauge::default()
        .block(Block::default().title(" Progress ").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Cyan))
        .ratio(ratio)
        .label(format!("{completed}/{total_steps}"));
    f.render_widget(gauge, chunks[1]);

    // Check if there's an error to show details for
    let failed_error = find_last_error(app);

    if let Some((kind, error_text)) = &failed_error {
        // Split: event list on top, error detail on bottom
        let log_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(6), Constraint::Percentage(50)])
            .split(chunks[2]);

        render_activity_list(f, app, log_chunks[0]);

        // Error detail panel
        let error_title = format!(" {} — Error Details ", kind.display_name());
        let error_para = Paragraph::new(error_text.as_str())
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(error_title)
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Red)),
            )
            .style(Style::default().fg(Color::Red));
        f.render_widget(error_para, log_chunks[1]);
    } else {
        // Split: event list on top, agent detail on bottom (if agent is active)
        let active_logs = collect_active_agent_logs(app);

        if active_logs.is_empty() {
            render_activity_list(f, app, chunks[2]);
        } else {
            let log_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(6), Constraint::Length(12)])
                .split(chunks[2]);

            render_activity_list(f, app, log_chunks[0]);

            // Active agent detail panel — last 10 logs
            let detail_items: Vec<ListItem> = active_logs
                .iter()
                .map(|(kind, msg)| {
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!("{}: ", kind.display_name()),
                            Style::default().fg(Color::Cyan),
                        ),
                        Span::raw(msg.as_str()),
                    ]))
                })
                .collect();

            let detail_list = List::new(detail_items).block(
                Block::default()
                    .title(" Agent Logs ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Yellow)),
            );
            f.render_widget(detail_list, log_chunks[1]);
        }
    }

    // Help bar
    let help_text = if app.diagnostic_running {
        "Analyzing reports for errors..."
    } else if app.consolidation_running {
        "Consolidating reports..."
    } else if app.is_running {
        "Esc: cancel run"
    } else if app.consolidation_active {
        match app.consolidation_phase {
            ConsolidationPhase::Confirm => "Consolidate final outputs? y/n",
            ConsolidationPhase::Provider => "j/k: choose provider  Enter: next  Esc: back",
            ConsolidationPhase::Prompt => {
                if app.consolidation_running {
                    "Consolidating..."
                } else {
                    "Type extra prompt  Enter: run  Esc: back"
                }
            }
        }
    } else {
        "Enter: view results  q: quit"
    };
    let help = Paragraph::new(help_text).block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[3]);

    if app.consolidation_active {
        draw_consolidation_modal(f, app);
    }
}

fn draw_consolidation_modal(f: &mut Frame, app: &App) {
    let area = centered_rect(70, 50, f.area());
    let block = Block::default()
        .title(" Consolidation ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    f.render_widget(ratatui::widgets::Clear, area);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let content = if app.consolidation_running {
        vec![
            Line::from("Running consolidation in a fresh provider session..."),
            Line::from(""),
            Line::from("Please wait."),
        ]
    } else {
        match app.consolidation_phase {
            ConsolidationPhase::Confirm => vec![
                Line::from("Consolidate last-iteration outputs now?"),
                Line::from(""),
                Line::from(vec![
                    Span::styled("y", Style::default().fg(Color::Green)),
                    Span::raw(": yes  "),
                    Span::styled("n", Style::default().fg(Color::Red)),
                    Span::raw(": no"),
                ]),
            ],
            ConsolidationPhase::Provider => {
                let mut lines = vec![Line::from("Select consolidation provider:"), Line::from("")];
                for (i, kind) in app.selected_agents.iter().enumerate() {
                    let style = if i == app.consolidation_provider_cursor {
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    lines.push(Line::from(Span::styled(
                        kind.display_name().to_string(),
                        style,
                    )));
                }
                lines
            }
            ConsolidationPhase::Prompt => vec![
                Line::from("Additional prompt to append (optional):"),
                Line::from(""),
                Line::from(Span::styled(
                    format!("{}_ ", app.consolidation_prompt),
                    Style::default().fg(Color::Yellow),
                )),
                Line::from(""),
                Line::from("Press Enter to run consolidation."),
            ],
        }
    };

    let p = Paragraph::new(content).wrap(Wrap { trim: false });
    f.render_widget(p, inner);
}

fn build_event_items(app: &App) -> Vec<ListItem<'_>> {
    let spinner = spinner_frame();

    // Show prompt snippet first
    let prompt_preview: String = app
        .prompt_text
        .chars()
        .take(120)
        .collect::<String>()
        .replace('\n', " ");
    let prompt_suffix = if app.prompt_text.chars().count() > 120 {
        "..."
    } else {
        ""
    };

    let mut items: Vec<ListItem> = vec![
        ListItem::new(Line::from(vec![
            Span::styled("Prompt: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{prompt_preview}{prompt_suffix}"),
                Style::default().fg(Color::DarkGray),
            ),
        ])),
        ListItem::new(""),
    ];

    #[derive(Clone)]
    enum AgentStatus {
        Thinking,
        Finished,
        Error(String),
    }

    enum Row {
        Agent {
            kind: crate::provider::ProviderKind,
            iteration: u32,
            status: AgentStatus,
        },
        Log {
            kind: crate::provider::ProviderKind,
            message: String,
        },
        IterationComplete(u32),
        AllDone,
    }

    let mut rows: Vec<Row> = Vec::new();
    let mut agent_row_idx: HashMap<(crate::provider::ProviderKind, u32), usize> = HashMap::new();

    for evt in &app.progress_events {
        match evt {
            ProgressEvent::AgentStarted { kind, iteration } => {
                let key = (*kind, *iteration);
                if let Some(idx) = agent_row_idx.get(&key).copied() {
                    if let Row::Agent { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Thinking;
                    }
                } else {
                    agent_row_idx.insert(key, rows.len());
                    rows.push(Row::Agent {
                        kind: *kind,
                        iteration: *iteration,
                        status: AgentStatus::Thinking,
                    });
                }
            }
            ProgressEvent::AgentFinished { kind, iteration } => {
                let key = (*kind, *iteration);
                if let Some(idx) = agent_row_idx.get(&key).copied() {
                    if let Row::Agent { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Finished;
                    }
                } else {
                    agent_row_idx.insert(key, rows.len());
                    rows.push(Row::Agent {
                        kind: *kind,
                        iteration: *iteration,
                        status: AgentStatus::Finished,
                    });
                }
            }
            ProgressEvent::AgentError {
                kind,
                iteration,
                error,
                ..
            } => {
                let key = (*kind, *iteration);
                let err = truncate_line(error, 100);
                if let Some(idx) = agent_row_idx.get(&key).copied() {
                    if let Row::Agent { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Error(err);
                    }
                } else {
                    agent_row_idx.insert(key, rows.len());
                    rows.push(Row::Agent {
                        kind: *kind,
                        iteration: *iteration,
                        status: AgentStatus::Error(err),
                    });
                }
            }
            ProgressEvent::IterationComplete { iteration } => {
                rows.push(Row::IterationComplete(*iteration));
            }
            ProgressEvent::AllDone => rows.push(Row::AllDone),
            ProgressEvent::AgentLog { kind, message, .. } => {
                if message.contains("consolidating reports")
                    || message.contains("analyzing reports for errors")
                    || message.contains("Diagnostic report saved to")
                {
                    rows.push(Row::Log {
                        kind: *kind,
                        message: message.clone(),
                    });
                }
            }
        }
    }

    for row in rows {
        match row {
            Row::Agent {
                kind,
                iteration,
                status,
            } => match status {
                AgentStatus::Thinking => items.push(ListItem::new(Line::from(vec![
                    Span::styled(format!("{spinner} "), Style::default().fg(Color::Yellow)),
                    Span::raw(format!(
                        "{} thinking (iter {})",
                        kind.display_name(),
                        iteration
                    )),
                ]))),
                AgentStatus::Finished => items.push(ListItem::new(Line::from(vec![
                    Span::styled("✓ ", Style::default().fg(Color::Green)),
                    Span::raw(format!(
                        "{} finished (iter {})",
                        kind.display_name(),
                        iteration
                    )),
                ]))),
                AgentStatus::Error(err) => items.push(ListItem::new(Line::from(vec![
                    Span::styled("✗ ", Style::default().fg(Color::Red)),
                    Span::raw(format!(
                        "{} FAILED (iter {}): {}",
                        kind.display_name(),
                        iteration,
                        err
                    )),
                ]))),
            },
            Row::IterationComplete(iteration) => {
                items.push(ListItem::new(Line::from(vec![Span::styled(
                    format!("── Iteration {iteration} complete ──"),
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::ITALIC),
                )])))
            }
            Row::Log { kind, message } => items.push(ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{} ", kind.display_name()),
                    Style::default().fg(Color::Yellow),
                ),
                Span::styled(message, Style::default().fg(Color::Yellow)),
            ]))),
            Row::AllDone => items.push(ListItem::new(Line::from(vec![Span::styled(
                "All done!",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )]))),
        }
    }

    items
}

fn render_activity_list(f: &mut Frame, app: &App, area: Rect) {
    let items = build_event_items(app);
    let selected = if items.is_empty() {
        None
    } else {
        Some(items.len().saturating_sub(1))
    };
    let mut state = ListState::default();
    state.select(selected);

    let list = List::new(items)
        .block(Block::default().title(" Activity ").borders(Borders::ALL))
        .highlight_style(Style::default());

    f.render_stateful_widget(list, area, &mut state);
}

/// Collect the last 10 AgentLog events for currently active agents
fn collect_active_agent_logs(app: &App) -> Vec<(crate::provider::ProviderKind, String)> {
    app.progress_events
        .iter()
        .rev()
        .filter_map(|evt| match evt {
            ProgressEvent::AgentLog {
                kind,
                iteration,
                message,
            } => Some((*kind, format!("[iter {iteration}] {message}"))),
            _ => None,
        })
        .take(10)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

/// Find the last error event (if any) to show full details
fn find_last_error(app: &App) -> Option<(crate::provider::ProviderKind, String)> {
    app.progress_events.iter().rev().find_map(|evt| match evt {
        ProgressEvent::AgentError {
            kind,
            details: Some(details),
            ..
        } => Some((*kind, details.clone())),
        _ => None,
    })
}

fn truncate_line(s: &str, max: usize) -> String {
    let first_line = s.lines().next().unwrap_or(s);
    let out: String = first_line.chars().take(max).collect();
    if first_line.chars().count() > max {
        format!("{out}...")
    } else {
        out
    }
}

fn spinner_frame() -> char {
    let frames = ['|', '/', '-', '\\'];
    let idx = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| ((d.as_millis() / 120) % frames.len() as u128) as usize)
        .unwrap_or(0);
    frames[idx]
}

fn compute_total_steps(app: &App) -> usize {
    let agents = app.selected_agents.len();
    match app.selected_mode {
        crate::execution::ExecutionMode::Solo => agents,
        crate::execution::ExecutionMode::Relay => agents * app.iterations as usize,
        crate::execution::ExecutionMode::Swarm => agents * app.iterations as usize,
    }
}

fn count_completed_steps(app: &App) -> usize {
    app.progress_events
        .iter()
        .filter(|e| {
            matches!(
                e,
                ProgressEvent::AgentFinished { .. } | ProgressEvent::AgentError { .. }
            )
        })
        .count()
}

fn current_status(app: &App) -> String {
    if app.diagnostic_running {
        return "Analyzing reports for errors...".into();
    }
    if app.consolidation_running {
        return "Consolidating reports...".into();
    }
    if !app.is_running {
        return "Done".into();
    }
    let mut active_agents: Vec<&str> = Vec::new();
    for kind in &app.selected_agents {
        let name = kind.display_name();
        let last = app.progress_events.iter().rev().find(|e| match e {
            ProgressEvent::AgentStarted { kind: k, .. }
            | ProgressEvent::AgentFinished { kind: k, .. }
            | ProgressEvent::AgentError { kind: k, .. } => k == kind,
            _ => false,
        });
        if matches!(last, Some(ProgressEvent::AgentStarted { .. })) {
            active_agents.push(name);
        }
    }

    if active_agents.is_empty() {
        "Waiting...".into()
    } else {
        format!("{} thinking...", active_agents.join(", "))
    }
}
