use super::centered_rect;
use crate::app::{App, ConsolidationPhase};
use crate::execution::{ExecutionMode, ProgressEvent};
use std::collections::HashSet;
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

    if let Some((agent_name, error_text)) = &failed_error {
        // Split: event list on top, error detail on bottom
        let log_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(6), Constraint::Percentage(50)])
            .split(chunks[2]);

        render_activity_list(f, app, log_chunks[0]);

        // Error detail panel
        let error_title = format!(" {agent_name} — Error Details ");
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
                .map(|(agent, msg)| {
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!("{agent}: "),
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
                let mut lines = vec![Line::from("Select consolidation agent:"), Line::from("")];
                for (i, agent) in app.config.agents.iter().enumerate() {
                    let style = if i == app.consolidation_provider_cursor {
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    lines.push(Line::from(Span::styled(
                        format!("{} ({})", agent.name, agent.provider.display_name()),
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
        Cancelled,
    }

    enum Row {
        Agent {
            name: String,
            iteration: u32,
            status: AgentStatus,
        },
        Block {
            label: String,
            iteration: u32,
            status: AgentStatus,
        },
        Log {
            name: String,
            message: String,
        },
        IterationComplete(u32),
        AllDone,
    }

    let mut rows: Vec<Row> = Vec::new();
    let mut agent_row_idx: HashMap<(String, u32), usize> = HashMap::new();
    let mut block_row_idx: HashMap<(u32, u32), usize> = HashMap::new();

    for evt in &app.progress_events {
        match evt {
            ProgressEvent::AgentStarted { agent, iteration, .. } => {
                let key = (agent.clone(), *iteration);
                if let Some(idx) = agent_row_idx.get(&key).copied() {
                    if let Row::Agent { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Thinking;
                    }
                } else {
                    agent_row_idx.insert(key, rows.len());
                    rows.push(Row::Agent {
                        name: agent.clone(),
                        iteration: *iteration,
                        status: AgentStatus::Thinking,
                    });
                }
            }
            ProgressEvent::AgentFinished { agent, iteration, .. } => {
                let key = (agent.clone(), *iteration);
                if let Some(idx) = agent_row_idx.get(&key).copied() {
                    if let Row::Agent { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Finished;
                    }
                } else {
                    agent_row_idx.insert(key, rows.len());
                    rows.push(Row::Agent {
                        name: agent.clone(),
                        iteration: *iteration,
                        status: AgentStatus::Finished,
                    });
                }
            }
            ProgressEvent::AgentError {
                agent,
                iteration,
                error,
                ..
            } => {
                let key = (agent.clone(), *iteration);
                let err = truncate_line(error, 100);
                if let Some(idx) = agent_row_idx.get(&key).copied() {
                    if let Row::Agent { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Error(err);
                    }
                } else {
                    agent_row_idx.insert(key, rows.len());
                    rows.push(Row::Agent {
                        name: agent.clone(),
                        iteration: *iteration,
                        status: AgentStatus::Error(err),
                    });
                }
            }
            ProgressEvent::IterationComplete { iteration } => {
                rows.push(Row::IterationComplete(*iteration));
            }
            ProgressEvent::AllDone => rows.push(Row::AllDone),
            ProgressEvent::AgentLog { agent, message, .. } => {
                if message.contains("consolidating reports")
                    || message.contains("analyzing reports for errors")
                    || message.contains("Diagnostic report saved to")
                {
                    rows.push(Row::Log {
                        name: agent.clone(),
                        message: message.clone(),
                    });
                }
            }
            ProgressEvent::BlockStarted {
                block_id,
                label,
                iteration,
                ..
            } => {
                let key = (*block_id, *iteration);
                if let Some(idx) = block_row_idx.get(&key).copied() {
                    if let Row::Block { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Thinking;
                    }
                } else {
                    block_row_idx.insert(key, rows.len());
                    rows.push(Row::Block {
                        label: label.clone(),
                        iteration: *iteration,
                        status: AgentStatus::Thinking,
                    });
                }
            }
            ProgressEvent::BlockFinished {
                block_id,
                label,
                iteration,
                ..
            } => {
                let key = (*block_id, *iteration);
                if let Some(idx) = block_row_idx.get(&key).copied() {
                    if let Row::Block { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Finished;
                    }
                } else {
                    block_row_idx.insert(key, rows.len());
                    rows.push(Row::Block {
                        label: label.clone(),
                        iteration: *iteration,
                        status: AgentStatus::Finished,
                    });
                }
            }
            ProgressEvent::BlockError {
                block_id,
                label,
                iteration,
                error,
                ..
            } => {
                let key = (*block_id, *iteration);
                let err = truncate_line(error, 100);
                if let Some(idx) = block_row_idx.get(&key).copied() {
                    if let Row::Block { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Error(err);
                    }
                } else {
                    block_row_idx.insert(key, rows.len());
                    rows.push(Row::Block {
                        label: label.clone(),
                        iteration: *iteration,
                        status: AgentStatus::Error(err),
                    });
                }
            }
            ProgressEvent::BlockSkipped {
                block_id,
                label,
                iteration,
                reason,
                ..
            } => {
                let key = (*block_id, *iteration);
                let err = format!("SKIPPED: {reason}");
                if let Some(idx) = block_row_idx.get(&key).copied() {
                    if let Row::Block { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Error(err);
                    }
                } else {
                    block_row_idx.insert(key, rows.len());
                    rows.push(Row::Block {
                        label: label.clone(),
                        iteration: *iteration,
                        status: AgentStatus::Error(err),
                    });
                }
            }
            ProgressEvent::BlockLog { agent_name, message, .. } => {
                if message.contains("consolidating reports")
                    || message.contains("analyzing reports for errors")
                    || message.contains("Diagnostic report saved to")
                {
                    rows.push(Row::Log {
                        name: agent_name.clone(),
                        message: message.clone(),
                    });
                }
            }
        }
    }

    // If run finished due to cancellation, mark still-thinking items as cancelled
    if !app.is_running && app.cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
        for row in &mut rows {
            match row {
                Row::Agent { status, .. } | Row::Block { status, .. } => {
                    if matches!(status, AgentStatus::Thinking) {
                        *status = AgentStatus::Cancelled;
                    }
                }
                _ => {}
            }
        }
    }

    for row in rows {
        match row {
            Row::Agent {
                name,
                iteration,
                status,
            } => match status {
                AgentStatus::Thinking => items.push(ListItem::new(Line::from(vec![
                    Span::styled(format!("{spinner} "), Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{name} thinking (iter {iteration})")),
                ]))),
                AgentStatus::Finished => items.push(ListItem::new(Line::from(vec![
                    Span::styled("✓ ", Style::default().fg(Color::Green)),
                    Span::raw(format!("{name} finished (iter {iteration})")),
                ]))),
                AgentStatus::Error(err) => items.push(ListItem::new(Line::from(vec![
                    Span::styled("✗ ", Style::default().fg(Color::Red)),
                    Span::raw(format!("{name} FAILED (iter {iteration}): {err}")),
                ]))),
                AgentStatus::Cancelled => items.push(ListItem::new(Line::from(vec![
                    Span::styled("✗ ", Style::default().fg(Color::Yellow)),
                    Span::styled(
                        format!("{name} cancelled (iter {iteration})"),
                        Style::default().fg(Color::Yellow),
                    ),
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
            Row::Log { name, message } => items.push(ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{name} "),
                    Style::default().fg(Color::Yellow),
                ),
                Span::styled(message, Style::default().fg(Color::Yellow)),
            ]))),
            Row::Block {
                label,
                iteration,
                status,
            } => match status {
                AgentStatus::Thinking => items.push(ListItem::new(Line::from(vec![
                    Span::styled(format!("{spinner} "), Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{label} thinking (iter {iteration})")),
                ]))),
                AgentStatus::Finished => items.push(ListItem::new(Line::from(vec![
                    Span::styled("\u{2713} ", Style::default().fg(Color::Green)),
                    Span::raw(format!("{label} finished (iter {iteration})")),
                ]))),
                AgentStatus::Error(err) => items.push(ListItem::new(Line::from(vec![
                    Span::styled("\u{2717} ", Style::default().fg(Color::Red)),
                    Span::raw(format!("{label} (iter {iteration}): {err}")),
                ]))),
                AgentStatus::Cancelled => items.push(ListItem::new(Line::from(vec![
                    Span::styled("\u{2717} ", Style::default().fg(Color::Yellow)),
                    Span::styled(
                        format!("{label} cancelled (iter {iteration})"),
                        Style::default().fg(Color::Yellow),
                    ),
                ]))),
            },
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

/// Collect the last 10 AgentLog/BlockLog events for currently active agents
fn collect_active_agent_logs(app: &App) -> Vec<(String, String)> {
    app.progress_events
        .iter()
        .rev()
        .filter_map(|evt| match evt {
            ProgressEvent::AgentLog {
                agent,
                iteration,
                message,
                ..
            } => Some((agent.clone(), format!("[iter {iteration}] {message}"))),
            ProgressEvent::BlockLog {
                agent_name,
                iteration,
                message,
                ..
            } => Some((agent_name.clone(), format!("[iter {iteration}] {message}"))),
            _ => None,
        })
        .take(10)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

/// Find the last error event (if any) to show full details
fn find_last_error(app: &App) -> Option<(String, String)> {
    app.progress_events.iter().rev().find_map(|evt| match evt {
        ProgressEvent::AgentError {
            agent,
            details: Some(details),
            ..
        } => Some((agent.clone(), details.clone())),
        ProgressEvent::BlockError {
            label,
            details: Some(details),
            ..
        } => Some((label.clone(), details.clone())),
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
        crate::execution::ExecutionMode::Pipeline => {
            app.pipeline_def.blocks.len() * app.pipeline_def.iterations as usize
        }
    }
}

fn count_completed_steps(app: &App) -> usize {
    app.progress_events
        .iter()
        .filter(|e| {
            matches!(
                e,
                ProgressEvent::AgentFinished { .. }
                    | ProgressEvent::AgentError { .. }
                    | ProgressEvent::BlockFinished { .. }
                    | ProgressEvent::BlockError { .. }
                    | ProgressEvent::BlockSkipped { .. }
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
        if app.cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
            return "Cancelled".into();
        }
        return "Done".into();
    }

    if app.selected_mode == ExecutionMode::Pipeline {
        // Collect block labels whose most recent event is BlockStarted (still running)
        let mut active_blocks: Vec<String> = Vec::new();
        let mut seen_block_ids = HashSet::new();
        for event in app.progress_events.iter().rev() {
            match event {
                ProgressEvent::BlockStarted {
                    block_id, label, ..
                } => {
                    if seen_block_ids.insert(*block_id) {
                        active_blocks.push(label.clone());
                    }
                }
                ProgressEvent::BlockFinished { block_id, .. }
                | ProgressEvent::BlockError { block_id, .. }
                | ProgressEvent::BlockSkipped { block_id, .. } => {
                    seen_block_ids.insert(*block_id);
                }
                _ => {}
            }
        }
        return if active_blocks.is_empty() {
            "Waiting...".into()
        } else {
            active_blocks.reverse();
            format!("{} thinking...", active_blocks.join(", "))
        };
    }

    let mut active_agents: Vec<&str> = Vec::new();
    for name in &app.selected_agents {
        let last = app.progress_events.iter().rev().find(|e| match e {
            ProgressEvent::AgentStarted { agent, .. }
            | ProgressEvent::AgentFinished { agent, .. }
            | ProgressEvent::AgentError { agent, .. } => agent == name,
            _ => false,
        });
        if matches!(last, Some(ProgressEvent::AgentStarted { .. })) {
            active_agents.push(name.as_str());
        }
    }

    if active_agents.is_empty() {
        "Waiting...".into()
    } else {
        format!("{} thinking...", active_agents.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::config::AppConfig;
    use crate::execution::{ExecutionMode, ProgressEvent};
    use crate::provider::ProviderKind;

    fn app() -> App {
        App::new(AppConfig {
            output_dir: "/tmp".to_string(),
            default_max_tokens: 4096,
            max_history_messages: 50,
            http_timeout_seconds: 120,
            model_fetch_timeout_seconds: 30,
            cli_timeout_seconds: 600,
            diagnostic_provider: None,
            agents: Vec::new(),
            providers: std::collections::HashMap::new(),
        })
    }

    #[test]
    fn truncate_line_uses_first_line_only() {
        assert_eq!(truncate_line("first\nsecond", 20), "first");
    }

    #[test]
    fn truncate_line_adds_ellipsis_when_truncated() {
        assert_eq!(truncate_line("abcdef", 3), "abc...");
    }

    #[test]
    fn spinner_frame_returns_valid_character() {
        let c = spinner_frame();
        assert!(matches!(c, '|' | '/' | '-' | '\\'));
    }

    #[test]
    fn compute_total_steps_solo_is_agent_count() {
        let mut a = app();
        a.selected_mode = ExecutionMode::Solo;
        a.selected_agents = vec!["Claude".into(), "Codex".into()];
        a.iterations = 9;
        assert_eq!(compute_total_steps(&a), 2);
    }

    #[test]
    fn compute_total_steps_relay_uses_iterations() {
        let mut a = app();
        a.selected_mode = ExecutionMode::Relay;
        a.selected_agents = vec!["Claude".into(), "Codex".into()];
        a.iterations = 3;
        assert_eq!(compute_total_steps(&a), 6);
    }

    #[test]
    fn compute_total_steps_swarm_uses_iterations() {
        let mut a = app();
        a.selected_mode = ExecutionMode::Swarm;
        a.selected_agents = vec!["Claude".into()];
        a.iterations = 4;
        assert_eq!(compute_total_steps(&a), 4);
    }

    #[test]
    fn count_completed_steps_counts_finished_and_error_only() {
        let mut a = app();
        a.progress_events = vec![
            ProgressEvent::AgentStarted {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration: 1,
            },
            ProgressEvent::AgentFinished {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration: 1,
            },
            ProgressEvent::AgentError {
                agent: "Codex".into(),
                kind: ProviderKind::OpenAI,
                iteration: 1,
                error: "x".to_string(),
                details: Some("x".to_string()),
            },
        ];
        assert_eq!(count_completed_steps(&a), 2);
    }

    #[test]
    fn collect_active_agent_logs_returns_last_10_in_order() {
        let mut a = app();
        for i in 0..12 {
            a.progress_events.push(ProgressEvent::AgentLog {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration: i,
                message: format!("m{i}"),
            });
        }
        let logs = collect_active_agent_logs(&a);
        assert_eq!(logs.len(), 10);
        assert_eq!(logs[0].1, "[iter 2] m2");
        assert_eq!(logs[9].1, "[iter 11] m11");
    }

    #[test]
    fn find_last_error_prefers_most_recent_with_details() {
        let mut a = app();
        a.progress_events = vec![
            ProgressEvent::AgentError {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration: 1,
                error: "old".to_string(),
                details: Some("old details".to_string()),
            },
            ProgressEvent::AgentError {
                agent: "Codex".into(),
                kind: ProviderKind::OpenAI,
                iteration: 2,
                error: "new".to_string(),
                details: Some("new details".to_string()),
            },
        ];
        assert_eq!(
            find_last_error(&a),
            Some(("Codex".to_string(), "new details".to_string()))
        );
    }

    #[test]
    fn find_last_error_ignores_missing_details() {
        let mut a = app();
        a.progress_events.push(ProgressEvent::AgentError {
            agent: "Codex".into(),
            kind: ProviderKind::OpenAI,
            iteration: 1,
            error: "x".to_string(),
            details: None,
        });
        assert!(find_last_error(&a).is_none());
    }

    #[test]
    fn current_status_reports_done_when_not_running() {
        let mut a = app();
        a.is_running = false;
        assert_eq!(current_status(&a), "Done");
    }

    #[test]
    fn current_status_reports_diagnostic_and_consolidation() {
        let mut a = app();
        a.is_running = true;
        a.diagnostic_running = true;
        assert_eq!(current_status(&a), "Analyzing reports for errors...");
        a.diagnostic_running = false;
        a.consolidation_running = true;
        assert_eq!(current_status(&a), "Consolidating reports...");
    }

    #[test]
    fn current_status_reports_waiting_when_no_active_agents() {
        let mut a = app();
        a.is_running = true;
        a.selected_agents = vec!["Claude".into()];
        a.progress_events = vec![ProgressEvent::AgentFinished {
            agent: "Claude".into(),
            kind: ProviderKind::Anthropic,
            iteration: 1,
        }];
        assert_eq!(current_status(&a), "Waiting...");
    }

    #[test]
    fn current_status_reports_active_agent_names() {
        let mut a = app();
        a.is_running = true;
        a.selected_agents = vec!["Claude".into(), "Codex".into()];
        a.progress_events = vec![
            ProgressEvent::AgentStarted {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration: 1,
            },
            ProgressEvent::AgentStarted {
                agent: "Codex".into(),
                kind: ProviderKind::OpenAI,
                iteration: 1,
            },
        ];
        let s = current_status(&a);
        assert!(s.contains("Claude"));
        assert!(s.contains("Codex"));
        assert!(s.contains("thinking"));
    }

    #[test]
    fn current_status_pipeline_shows_active_blocks() {
        let mut a = app();
        a.is_running = true;
        a.selected_mode = ExecutionMode::Pipeline;
        a.progress_events = vec![
            ProgressEvent::BlockStarted {
                block_id: 1,
                agent_name: "Claude".into(),
                label: "Block 1 (Claude)".into(),
                iteration: 1,
            },
            ProgressEvent::BlockStarted {
                block_id: 2,
                agent_name: "Codex".into(),
                label: "Block 2 (Codex)".into(),
                iteration: 1,
            },
        ];
        let s = current_status(&a);
        assert!(s.contains("Block 1 (Claude)"));
        assert!(s.contains("Block 2 (Codex)"));
        assert!(s.contains("thinking"));
    }

    #[test]
    fn current_status_pipeline_excludes_finished_blocks() {
        let mut a = app();
        a.is_running = true;
        a.selected_mode = ExecutionMode::Pipeline;
        a.progress_events = vec![
            ProgressEvent::BlockStarted {
                block_id: 1,
                agent_name: "Claude".into(),
                label: "Block 1 (Claude)".into(),
                iteration: 1,
            },
            ProgressEvent::BlockFinished {
                block_id: 1,
                agent_name: "Claude".into(),
                label: "Block 1 (Claude)".into(),
                iteration: 1,
            },
            ProgressEvent::BlockStarted {
                block_id: 2,
                agent_name: "Codex".into(),
                label: "Block 2 (Codex)".into(),
                iteration: 1,
            },
        ];
        let s = current_status(&a);
        assert!(!s.contains("Block 1"));
        assert!(s.contains("Block 2 (Codex)"));
    }

    #[test]
    fn current_status_pipeline_waiting_when_no_active_blocks() {
        let mut a = app();
        a.is_running = true;
        a.selected_mode = ExecutionMode::Pipeline;
        a.progress_events = vec![ProgressEvent::BlockFinished {
            block_id: 1,
            agent_name: "Claude".into(),
            label: "Block 1 (Claude)".into(),
            iteration: 1,
        }];
        assert_eq!(current_status(&a), "Waiting...");
    }

    #[test]
    fn current_status_shows_cancelled_when_flag_set() {
        let mut a = app();
        a.is_running = false;
        a.cancel_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(current_status(&a), "Cancelled");
    }

    #[test]
    fn build_event_items_marks_thinking_as_cancelled() {
        let mut a = app();
        a.is_running = false;
        a.cancel_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        a.progress_events = vec![
            ProgressEvent::AgentStarted {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration: 1,
            },
            ProgressEvent::AgentFinished {
                agent: "Codex".into(),
                kind: ProviderKind::OpenAI,
                iteration: 1,
            },
        ];
        let items = build_event_items(&a);
        let texts: Vec<String> = items.iter().map(|i| format!("{:?}", i)).collect();
        let joined = texts.join("\n");
        // Thinking agent should be cancelled, finished agent stays finished
        assert!(joined.contains("cancelled"), "expected cancelled in: {joined}");
        assert!(joined.contains("finished"), "expected finished in: {joined}");
        assert!(
            !joined.contains("thinking"),
            "expected no thinking in: {joined}"
        );
    }

    #[test]
    fn build_event_items_keeps_thinking_when_not_cancelled() {
        let mut a = app();
        a.is_running = true;
        a.progress_events = vec![ProgressEvent::AgentStarted {
            agent: "Claude".into(),
            kind: ProviderKind::Anthropic,
            iteration: 1,
        }];
        let items = build_event_items(&a);
        let joined: String = items.iter().map(|i| format!("{:?}", i)).collect();
        assert!(
            joined.contains("thinking"),
            "expected thinking in: {joined}"
        );
    }

    #[test]
    fn build_event_items_marks_thinking_blocks_as_cancelled() {
        let mut a = app();
        a.is_running = false;
        a.cancel_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        a.progress_events = vec![ProgressEvent::BlockStarted {
            block_id: 1,
            agent_name: "Claude".into(),
            label: "Block 1 (Claude)".into(),
            iteration: 1,
        }];
        let items = build_event_items(&a);
        let joined: String = items.iter().map(|i| format!("{:?}", i)).collect();
        assert!(joined.contains("cancelled"), "expected cancelled in: {joined}");
        assert!(
            !joined.contains("thinking"),
            "expected no thinking in: {joined}"
        );
    }
}
