use super::centered_rect;
use crate::app::{AgentRowStatus, App, ConsolidationPhase, RunStatus, RunStepStatus};
use crate::execution::{ExecutionMode, ProgressEvent};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn is_notable_log(message: &str) -> bool {
    message.contains("consolidating reports")
        || message.contains("analyzing reports for errors")
        || message.contains("Diagnostic report saved to")
}

fn format_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    if hours > 0 {
        format!("{hours}:{mins:02}:{secs:02}")
    } else {
        format!("{mins}:{secs:02}")
    }
}

fn preview_paragraph<'a>(preview_text: &'a str, target_label: &str) -> Paragraph<'a> {
    Paragraph::new(preview_text)
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title(format!(" {target_label} — Preview "))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
}

pub fn draw(f: &mut Frame, app: &App) {
    if app.running.multi_run_total > 1 {
        draw_multi_run(f, app);
        if app.running.consolidation_active {
            draw_consolidation_modal(f, app);
        }
        return;
    }
    match app.selected_mode {
        ExecutionMode::Relay => draw_relay(f, app),
        ExecutionMode::Swarm => draw_swarm(f, app),
        ExecutionMode::Pipeline => draw_pipeline_running(f, app),
    }
    if app.running.consolidation_active {
        draw_consolidation_modal(f, app);
    }
}

// ── Mode-specific layouts ──

fn draw_relay(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Length(3), // Gauge
            Constraint::Length(3), // Chain
            Constraint::Min(0),    // Main panel
            Constraint::Length(3), // Help
        ])
        .split(f.area());

    draw_title_bar(f, app, chunks[0]);
    draw_progress_gauge(f, app, chunks[1]);
    draw_relay_chain(f, app, chunks[2]);
    draw_main_panel(f, app, chunks[3]);
    draw_running_help_bar(f, app, chunks[4]);
}

fn draw_swarm(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Length(3), // Gauge
            Constraint::Length(5), // Round + cards
            Constraint::Min(0),    // Main panel
            Constraint::Length(3), // Help
        ])
        .split(f.area());

    draw_title_bar(f, app, chunks[0]);
    draw_progress_gauge(f, app, chunks[1]);
    draw_swarm_round(f, app, chunks[2]);
    draw_main_panel(f, app, chunks[3]);
    draw_running_help_bar(f, app, chunks[4]);
}

fn draw_pipeline_running(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Title
            Constraint::Length(3),  // Gauge
            Constraint::Length(12), // Mini DAG
            Constraint::Min(0),     // Main panel
            Constraint::Length(3),  // Help
        ])
        .split(f.area());

    draw_title_bar(f, app, chunks[0]);
    draw_progress_gauge(f, app, chunks[1]);
    draw_pipeline_dag(f, app, chunks[2]);
    draw_main_panel(f, app, chunks[3]);
    draw_running_help_bar(f, app, chunks[4]);
}

// ── Shared helpers ──

fn draw_title_bar(f: &mut Frame, app: &App, area: Rect) {
    let status_text = current_status(app);
    let out_dir = app
        .running
        .run_dir
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let elapsed = format_duration(app.run_elapsed());
    let iter_display = if app.final_iteration() > 0 {
        format!(
            "  iter {}/{}",
            app.current_iteration(),
            app.final_iteration()
        )
    } else {
        String::new()
    };
    let title = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                format!("Running — {} mode", app.selected_mode),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("  [{elapsed}]"),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(iter_display, Style::default().fg(Color::Magenta)),
            if app.memory.last_recalled_count > 0 {
                Span::styled(
                    format!("  {} memories", app.memory.last_recalled_count),
                    Style::default().fg(Color::DarkGray),
                )
            } else {
                Span::raw("")
            },
            match (
                &app.memory.last_extraction_error,
                app.memory.last_extraction_count,
            ) {
                (Some(_), _) => {
                    Span::styled("  | extraction failed", Style::default().fg(Color::Red))
                }
                (None, Some(n)) if n > 0 => Span::styled(
                    format!("  | +{n} extracted"),
                    Style::default().fg(Color::DarkGray),
                ),
                _ => Span::raw(""),
            },
            Span::raw("  "),
            Span::styled(status_text, Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::styled("Output: ", Style::default().fg(Color::DarkGray)),
            Span::styled(out_dir, Style::default().fg(Color::DarkGray)),
        ]),
    ])
    .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, area);
}

fn draw_progress_gauge(f: &mut Frame, app: &App, area: Rect) {
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
    f.render_widget(gauge, area);
}

fn draw_main_panel(f: &mut Frame, app: &App, area: Rect) {
    let failed_error = find_last_error(app);

    if let Some((agent_name, error_text)) = &failed_error {
        let log_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(6), Constraint::Percentage(50)])
            .split(area);

        render_activity_list(f, app, log_chunks[0]);

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
    } else if let Some(target) = app.preview_target() {
        let preview_text = app.stream_buffer(target).map(|b| b.text()).unwrap_or("");
        let target_label = resolve_stream_target_label(app, target);

        if app.show_activity_log() {
            let log_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(6), Constraint::Percentage(50)])
                .split(area);

            render_activity_list(f, app, log_chunks[0]);
            let preview = preview_paragraph(preview_text, &target_label);
            f.render_widget(preview, log_chunks[1]);
        } else {
            let preview = preview_paragraph(preview_text, &target_label);
            f.render_widget(preview, area);
        }
    } else {
        let active_logs = collect_active_agent_logs(app);

        if active_logs.is_empty() || !app.show_activity_log() {
            render_activity_list(f, app, area);
        } else {
            let log_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(6), Constraint::Length(12)])
                .split(area);

            render_activity_list(f, app, log_chunks[0]);

            let detail_items: Vec<ListItem> = active_logs
                .iter()
                .map(|(agent, msg)| {
                    ListItem::new(Line::from(vec![
                        Span::styled(format!("{agent}: "), Style::default().fg(Color::Cyan)),
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
}

fn draw_running_help_bar(f: &mut Frame, app: &App, area: Rect) {
    let help_text = if app.running.diagnostic_running {
        "Analyzing reports for errors..."
    } else if app.running.consolidation_running {
        "Consolidating reports..."
    } else if app.running.is_running {
        "Esc: cancel run"
    } else if app.running.consolidation_active {
        match app.running.consolidation_phase {
            ConsolidationPhase::Confirm => "Consolidate final outputs? y/n",
            ConsolidationPhase::Provider => "j/k: choose provider  Enter: next  Esc: back",
            ConsolidationPhase::Prompt => {
                if app.running.consolidation_running {
                    "Consolidating..."
                } else {
                    "Type extra prompt  Enter: run  Esc: back"
                }
            }
            ConsolidationPhase::CrossRunConfirm => "Consolidate across runs? y/n",
            ConsolidationPhase::CrossRunPrompt => "Type extra prompt  Enter: run  Esc: back",
        }
    } else {
        "Enter: view results  q: quit"
    };
    let toggle_hint = if app.running.multi_run_total <= 1 {
        if app.running.is_running {
            "  [l] Log  [p] Preview  [Tab] Cycle"
        } else if app.preview_target().is_some() {
            "  [l] Log  [p] Close preview  [Tab] Cycle"
        } else {
            "  [l] Log  [p] Preview"
        }
    } else {
        ""
    };
    let help = Paragraph::new(format!("{help_text}{toggle_hint}"))
        .block(Block::default().borders(Borders::TOP));
    f.render_widget(help, area);
}

// ── Mode visualizations ──

fn agent_status_icon(app: &App, name: &str) -> (char, Color) {
    let row = app.agent_rows().iter().find(|r| r.name == name);
    match row.map(|r| &r.status) {
        Some(AgentRowStatus::Running) => (spinner_frame(), Color::Yellow),
        Some(AgentRowStatus::Finished) => ('\u{2713}', Color::Green),
        Some(AgentRowStatus::Error(_)) => ('\u{2717}', Color::Red),
        Some(AgentRowStatus::Skipped(_)) => ('\u{25cb}', Color::Gray),
        _ => ('\u{00b7}', Color::DarkGray),
    }
}

fn draw_relay_chain(f: &mut Frame, app: &App, area: Rect) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let agents = &app.selected_agents;
    if agents.is_empty() {
        return;
    }

    // Check if there's enough width for horizontal layout (~14 per card, ~4 per arrow)
    let min_width = agents.len() * 14 + agents.len().saturating_sub(1) * 4;

    if (area.width as usize) < min_width {
        // Vertical fallback
        let lines: Vec<Line> = agents
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let (icon, color) = agent_status_icon(app, name);
                let timer = app
                    .agent_timer(name)
                    .map(|t| format_duration(t.elapsed()))
                    .unwrap_or_else(|| "--".into());
                Line::from(vec![
                    Span::styled(format!("{}. ", i + 1), Style::default().fg(Color::DarkGray)),
                    Span::styled(format!("{icon} "), Style::default().fg(color)),
                    Span::raw(name.clone()),
                    Span::styled(format!("  {timer}"), Style::default().fg(Color::DarkGray)),
                ])
            })
            .collect();
        f.render_widget(Paragraph::new(lines), area);
        return;
    }

    // Horizontal chain
    let mut spans: Vec<Span> = Vec::new();
    for (i, name) in agents.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(
                " \u{2500}\u{2192} ",
                Style::default().fg(Color::DarkGray),
            ));
        }
        let (icon, color) = agent_status_icon(app, name);
        let timer = app
            .agent_timer(name)
            .map(|t| format!(" {}", format_duration(t.elapsed())))
            .unwrap_or_default();
        spans.push(Span::styled("[ ", Style::default().fg(color)));
        spans.push(Span::styled(format!("{icon} "), Style::default().fg(color)));
        spans.push(Span::styled(
            name.clone(),
            Style::default().fg(Color::White),
        ));
        spans.push(Span::styled(timer, Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(" ]", Style::default().fg(color)));
    }

    let p = Paragraph::new(Line::from(spans)).alignment(ratatui::layout::Alignment::Center);
    let y = area.y + area.height / 2;
    f.render_widget(p, Rect::new(area.x, y, area.width, 1));
}

fn draw_swarm_round(f: &mut Frame, app: &App, area: Rect) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let round = app.current_iteration();
    let total = app.final_iteration();
    let header_text = if total > 0 {
        format!("  Round {round}/{total}")
    } else {
        "  Round:".to_string()
    };
    let header = Paragraph::new(Span::styled(
        header_text,
        Style::default().fg(Color::Magenta),
    ));
    f.render_widget(header, Rect::new(area.x, area.y, area.width, 1));

    if area.height > 1 {
        let card_area = Rect::new(area.x, area.y + 1, area.width, area.height - 1);
        draw_agent_cards(f, app, card_area);
    }
}

fn draw_agent_cards(f: &mut Frame, app: &App, area: Rect) {
    let agents = &app.selected_agents;
    if agents.is_empty() || area.width == 0 || area.height == 0 {
        return;
    }

    let n = agents.len() as u32;
    let constraints: Vec<Constraint> = (0..n).map(|_| Constraint::Ratio(1, n)).collect();
    let card_areas = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(area);

    for (i, name) in agents.iter().enumerate() {
        let (icon, color) = agent_status_icon(app, name);
        let timer_text = app
            .agent_timer(name)
            .map(|t| format_duration(t.elapsed()))
            .unwrap_or_else(|| "--".into());
        let content = format!("{icon} {timer_text}");
        let card = Paragraph::new(content)
            .alignment(ratatui::layout::Alignment::Center)
            .block(
                Block::default()
                    .title(format!(" {name} "))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(color)),
            );
        f.render_widget(card, card_areas[i]);
    }
}

fn draw_pipeline_dag(f: &mut Frame, app: &App, area: Rect) {
    use crate::execution::pipeline::BlockId;
    use ratatui::widgets::BorderType;

    let blocks = &app.pipeline.pipeline_def.blocks;
    let connections = &app.pipeline.pipeline_def.connections;

    if blocks.is_empty() {
        let msg = Paragraph::new("No pipeline blocks defined")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(ratatui::layout::Alignment::Center);
        f.render_widget(msg, area);
        return;
    }

    let dag_block = Block::default()
        .title(" Pipeline ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = dag_block.inner(area);
    f.render_widget(dag_block, area);

    let block_style_fn = |block_id: BlockId| -> (Color, BorderType) {
        // Aggregate status across all replicas of this logical block
        let replicas: Vec<_> = app
            .block_rows()
            .iter()
            .filter(|r| r.source_block_id == block_id)
            .collect();
        if replicas.is_empty() {
            return (Color::DarkGray, BorderType::Plain);
        }
        let any_running = replicas
            .iter()
            .any(|r| matches!(r.status, AgentRowStatus::Running));
        let any_error = replicas
            .iter()
            .any(|r| matches!(r.status, AgentRowStatus::Error(_)));
        let all_finished = replicas
            .iter()
            .all(|r| matches!(r.status, AgentRowStatus::Finished));
        let all_skipped = replicas
            .iter()
            .all(|r| matches!(r.status, AgentRowStatus::Skipped(_)));
        let color = if any_running {
            Color::Yellow
        } else if all_finished {
            Color::Green
        } else if any_error {
            Color::Red
        } else if all_skipped {
            Color::Gray
        } else {
            Color::DarkGray
        };
        (color, BorderType::Plain)
    };

    super::pipeline::render_dag_readonly(
        f,
        inner,
        blocks,
        connections,
        &app.pipeline.pipeline_def.loop_connections,
        &block_style_fn,
    );
}

fn draw_multi_run(f: &mut Frame, app: &App) {
    let has_stage = app.running.batch_stage.is_some() || app.running.batch_stage_error.is_some();
    let stage_height = if has_stage { 2 } else { 0 };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),            // title
            Constraint::Length(3),            // progress gauge
            Constraint::Min(8),               // runs list
            Constraint::Length(stage_height), // batch stage indicator (0 when hidden)
            Constraint::Length(10),           // selected run logs
            Constraint::Length(3),            // help bar
        ])
        .split(f.area());

    let out_dir = app
        .running
        .run_dir
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let title = Paragraph::new(vec![
        Line::from(Span::styled(
            format!(
                "Running — {} mode ({} runs, concurrency {})",
                app.selected_mode, app.running.multi_run_total, app.running.multi_run_concurrency
            ),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled("Output: ", Style::default().fg(Color::DarkGray)),
            Span::styled(out_dir, Style::default().fg(Color::DarkGray)),
        ]),
    ])
    .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    let completed = app
        .running
        .multi_run_states
        .iter()
        .filter(|state| {
            matches!(
                state.status,
                RunStatus::Done | RunStatus::Failed | RunStatus::Cancelled
            )
        })
        .count();
    let total = app.running.multi_run_states.len();
    let ratio = if total > 0 {
        completed as f64 / total as f64
    } else {
        0.0
    };
    let gauge = Gauge::default()
        .block(Block::default().title(" Progress ").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Cyan))
        .ratio(ratio)
        .label(format!("{completed}/{total}"));
    f.render_widget(gauge, chunks[1]);

    let items = app
        .running
        .multi_run_states
        .iter()
        .enumerate()
        .map(|(idx, state)| {
            let mut spans = vec![Span::styled(
                format!("Run {:>2}  ", state.run_id),
                if idx == app.running.multi_run_cursor {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                },
            )];

            for step in &state.steps {
                let (symbol, color) = step_symbol(step.status);
                spans.push(Span::styled(symbol.to_string(), Style::default().fg(color)));
                spans.push(Span::raw(" "));
                spans.push(Span::raw(step.label.as_str()));
                spans.push(Span::raw("  "));
            }

            spans.push(Span::styled(
                run_status_label(state.status),
                Style::default().fg(run_status_color(state.status)),
            ));

            ListItem::new(Line::from(spans))
        })
        .collect::<Vec<_>>();
    let list = List::new(items).block(
        Block::default()
            .title(" Runs ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    let mut list_state = ListState::default();
    if !app.running.multi_run_states.is_empty() {
        list_state.select(Some(
            app.running
                .multi_run_cursor
                .min(app.running.multi_run_states.len() - 1),
        ));
    }
    f.render_stateful_widget(list, chunks[2], &mut list_state);

    // Batch stage indicator (finalization, etc.)
    if has_stage {
        let stage_line = if let Some(ref label) = app.running.batch_stage {
            Line::from(vec![
                Span::styled("  Stage: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    label.clone(),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" ...", Style::default().fg(Color::DarkGray)),
            ])
        } else if let Some(ref err) = app.running.batch_stage_error {
            Line::from(vec![
                Span::styled("  Stage error: ", Style::default().fg(Color::Red)),
                Span::styled(err.clone(), Style::default().fg(Color::Red)),
            ])
        } else {
            Line::default()
        };
        let stage_widget = Paragraph::new(stage_line);
        f.render_widget(stage_widget, chunks[3]);
    }

    let detail_lines = if let Some(state) = app
        .running
        .multi_run_states
        .get(app.running.multi_run_cursor)
    {
        if state.recent_logs.is_empty() {
            vec![Line::from("No logs yet.")]
        } else {
            state
                .recent_logs
                .iter()
                .map(|line| Line::from(line.clone()))
                .collect::<Vec<_>>()
        }
    } else {
        vec![Line::from("No run selected.")]
    };
    let detail = Paragraph::new(detail_lines)
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title(" Selected Run Logs ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow)),
        );
    f.render_widget(detail, chunks[4]);

    let help_text = if app.running.diagnostic_running {
        "Analyzing reports for errors..."
    } else if app.running.consolidation_running {
        "Consolidating reports..."
    } else if app.running.is_running {
        "j/k: select run  Esc: cancel batch"
    } else if app.running.consolidation_active {
        "Enter consolidation choice"
    } else {
        "j/k: select run  Enter: view results  q: quit"
    };
    let help = Paragraph::new(help_text).block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[5]);
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

    let content = if app.running.consolidation_running {
        vec![
            Line::from("Running consolidation in a fresh provider session..."),
            Line::from(""),
            Line::from("Please wait."),
        ]
    } else {
        match app.running.consolidation_phase {
            ConsolidationPhase::Confirm => vec![
                Line::from(if app.running.multi_run_total > 1 {
                    "Consolidate each successful run individually?"
                } else {
                    "Consolidate last-iteration outputs now?"
                }),
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
                    let style = if i == app.running.consolidation_provider_cursor {
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
                    format!("{}_ ", app.running.consolidation_prompt),
                    Style::default().fg(Color::Yellow),
                )),
                Line::from(""),
                Line::from("Press Enter to run consolidation."),
            ],
            ConsolidationPhase::CrossRunConfirm => vec![
                Line::from("Consolidate across successful runs?"),
                Line::from(""),
                Line::from(vec![
                    Span::styled("y", Style::default().fg(Color::Green)),
                    Span::raw(": yes  "),
                    Span::styled("n", Style::default().fg(Color::Red)),
                    Span::raw(": no"),
                ]),
            ],
            ConsolidationPhase::CrossRunPrompt => vec![
                Line::from("Additional cross-run prompt to append (optional):"),
                Line::from(""),
                Line::from(Span::styled(
                    format!("{}_ ", app.running.consolidation_prompt),
                    Style::default().fg(Color::Yellow),
                )),
                Line::from(""),
                Line::from("Press Enter to run cross-run consolidation."),
            ],
        }
    };

    let p = Paragraph::new(content).wrap(Wrap { trim: false });
    f.render_widget(p, inner);
}

fn step_symbol(status: RunStepStatus) -> (char, Color) {
    match status {
        RunStepStatus::Queued => ('·', Color::DarkGray),
        RunStepStatus::Pending => ('-', Color::Gray),
        RunStepStatus::Running => (spinner_frame(), Color::Yellow),
        RunStepStatus::Done => ('✓', Color::Green),
        RunStepStatus::Error => ('✗', Color::Red),
    }
}

fn run_status_label(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::Done => "done",
        RunStatus::Failed => "error",
        RunStatus::Cancelled => "cancelled",
    }
}

fn run_status_color(status: RunStatus) -> Color {
    match status {
        RunStatus::Queued => Color::DarkGray,
        RunStatus::Running => Color::Yellow,
        RunStatus::Done => Color::Green,
        RunStatus::Failed => Color::Red,
        RunStatus::Cancelled => Color::Gray,
    }
}

fn build_event_items(app: &App) -> Vec<ListItem<'_>> {
    let spinner = spinner_frame();

    // Show prompt snippet first
    let prompt_preview: String = app
        .prompt
        .prompt_text
        .chars()
        .take(120)
        .collect::<String>()
        .replace('\n', " ");
    let prompt_suffix = if app.prompt.prompt_text.chars().count() > 120 {
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
            loop_pass: u32,
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
    let mut block_row_idx: HashMap<(u32, u32, u32), usize> = HashMap::new();

    for evt in app.activity_log() {
        match evt {
            ProgressEvent::AgentStarted {
                agent, iteration, ..
            } => {
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
            ProgressEvent::AgentFinished {
                agent, iteration, ..
            } => {
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
                if is_notable_log(message) {
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
                loop_pass,
                ..
            } => {
                let key = (*block_id, *iteration, *loop_pass);
                if let Some(idx) = block_row_idx.get(&key).copied() {
                    if let Row::Block { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Thinking;
                    }
                } else {
                    block_row_idx.insert(key, rows.len());
                    rows.push(Row::Block {
                        label: label.clone(),
                        iteration: *iteration,
                        loop_pass: *loop_pass,
                        status: AgentStatus::Thinking,
                    });
                }
            }
            ProgressEvent::BlockFinished {
                block_id,
                label,
                iteration,
                loop_pass,
                ..
            } => {
                let key = (*block_id, *iteration, *loop_pass);
                if let Some(idx) = block_row_idx.get(&key).copied() {
                    if let Row::Block { status, .. } = &mut rows[idx] {
                        *status = AgentStatus::Finished;
                    }
                } else {
                    block_row_idx.insert(key, rows.len());
                    rows.push(Row::Block {
                        label: label.clone(),
                        iteration: *iteration,
                        loop_pass: *loop_pass,
                        status: AgentStatus::Finished,
                    });
                }
            }
            ProgressEvent::BlockError {
                block_id,
                label,
                iteration,
                loop_pass,
                error,
                ..
            } => {
                let key = (*block_id, *iteration, *loop_pass);
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
                        loop_pass: *loop_pass,
                        status: AgentStatus::Error(err),
                    });
                }
            }
            ProgressEvent::BlockSkipped {
                block_id,
                label,
                iteration,
                loop_pass,
                reason,
                ..
            } => {
                let key = (*block_id, *iteration, *loop_pass);
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
                        loop_pass: *loop_pass,
                        status: AgentStatus::Error(err),
                    });
                }
            }
            ProgressEvent::BlockLog {
                agent_name,
                message,
                ..
            } => {
                if is_notable_log(message) {
                    rows.push(Row::Log {
                        name: agent_name.clone(),
                        message: message.clone(),
                    });
                }
            }
            ProgressEvent::AgentStreamChunk { .. } | ProgressEvent::BlockStreamChunk { .. } => {}
        }
    }

    // If run finished due to cancellation, mark still-thinking items as cancelled
    if !app.running.is_running
        && app
            .running
            .cancel_flag
            .load(std::sync::atomic::Ordering::Relaxed)
    {
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
                Span::styled(format!("{name} "), Style::default().fg(Color::Yellow)),
                Span::styled(message, Style::default().fg(Color::Yellow)),
            ]))),
            Row::Block {
                label,
                iteration,
                loop_pass,
                status,
            } => {
                let suffix = if loop_pass > 0 {
                    format!(" (iter {iteration} pass {loop_pass})")
                } else {
                    format!(" (iter {iteration})")
                };
                match status {
                    AgentStatus::Thinking => items.push(ListItem::new(Line::from(vec![
                        Span::styled(format!("{spinner} "), Style::default().fg(Color::Yellow)),
                        Span::raw(format!("{label} thinking{suffix}")),
                    ]))),
                    AgentStatus::Finished => items.push(ListItem::new(Line::from(vec![
                        Span::styled("\u{2713} ", Style::default().fg(Color::Green)),
                        Span::raw(format!("{label} finished{suffix}")),
                    ]))),
                    AgentStatus::Error(err) => items.push(ListItem::new(Line::from(vec![
                        Span::styled("\u{2717} ", Style::default().fg(Color::Red)),
                        Span::raw(format!("{label}{suffix}: {err}")),
                    ]))),
                    AgentStatus::Cancelled => items.push(ListItem::new(Line::from(vec![
                        Span::styled("\u{2717} ", Style::default().fg(Color::Yellow)),
                        Span::styled(
                            format!("{label} cancelled{suffix}"),
                            Style::default().fg(Color::Yellow),
                        ),
                    ]))),
                }
            }
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
    app.recent_activity_logs().iter().cloned().collect()
}

/// Find the last error event (if any) to show full details
fn find_last_error(app: &App) -> Option<(String, String)> {
    app.last_error().cloned()
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

fn resolve_stream_target_label(app: &App, target: &crate::app::StreamTarget) -> String {
    match target {
        crate::app::StreamTarget::Agent(name) => name.clone(),
        crate::app::StreamTarget::Block(id) => app
            .running
            .block_rows
            .iter()
            .find(|r| r.block_id == *id)
            .map(|r| r.label.clone())
            .unwrap_or_else(|| format!("Block {id}")),
    }
}

fn compute_total_steps(app: &App) -> usize {
    let agents = app.selected_agents.len();
    match app.selected_mode {
        crate::execution::ExecutionMode::Relay => agents * app.prompt.iterations as usize,
        crate::execution::ExecutionMode::Swarm => agents * app.prompt.iterations as usize,
        crate::execution::ExecutionMode::Pipeline => {
            if app.running.expected_total_steps > 0 {
                app.running.expected_total_steps
            } else {
                app.running.block_rows.len() * app.pipeline.pipeline_def.iterations as usize
            }
        }
    }
}

fn count_completed_steps(app: &App) -> usize {
    app.completed_steps()
}

fn current_status(app: &App) -> String {
    if app.running.diagnostic_running {
        return "Analyzing reports for errors...".into();
    }
    if app.running.consolidation_running {
        return "Consolidating reports...".into();
    }
    if app
        .running
        .cancel_flag
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return if app.running.is_running {
            "Cancelling...".into()
        } else {
            "Cancelled".into()
        };
    }
    if !app.running.is_running {
        return "Done".into();
    }

    if app.selected_mode == ExecutionMode::Pipeline {
        let active_blocks = app
            .active_block_labels()
            .map(str::to_string)
            .collect::<Vec<_>>();
        return if active_blocks.is_empty() {
            "Waiting...".into()
        } else {
            format!("{} thinking...", active_blocks.join(", "))
        };
    }

    let mut active_agents: Vec<&str> = Vec::new();
    for name in &app.selected_agents {
        if app.is_agent_active(name) {
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
            max_history_bytes: 102400,
            pipeline_block_concurrency: 0,
            diagnostic_provider: None,
            memory: crate::config::MemoryConfig::default(),
            agents: Vec::new(),
            providers: std::collections::HashMap::new(),
        })
    }

    fn push_events(app: &mut App, events: impl IntoIterator<Item = ProgressEvent>) {
        for event in events {
            app.record_progress(event);
        }
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
    fn compute_total_steps_relay_uses_iterations() {
        let mut a = app();
        a.selected_mode = ExecutionMode::Relay;
        a.selected_agents = vec!["Claude".into(), "OpenAI".into()];
        a.prompt.iterations = 3;
        assert_eq!(compute_total_steps(&a), 6);
    }

    #[test]
    fn compute_total_steps_swarm_uses_iterations() {
        let mut a = app();
        a.selected_mode = ExecutionMode::Swarm;
        a.selected_agents = vec!["Claude".into()];
        a.prompt.iterations = 4;
        assert_eq!(compute_total_steps(&a), 4);
    }

    #[test]
    fn count_completed_steps_counts_finished_and_error_only() {
        let mut a = app();
        push_events(
            &mut a,
            vec![
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
                    agent: "OpenAI".into(),
                    kind: ProviderKind::OpenAI,
                    iteration: 1,
                    error: "x".to_string(),
                    details: Some("x".to_string()),
                },
            ],
        );
        assert_eq!(count_completed_steps(&a), 2);
    }

    #[test]
    fn collect_active_agent_logs_returns_last_10_in_order() {
        let mut a = app();
        for i in 0..12 {
            a.record_progress(ProgressEvent::AgentLog {
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
        push_events(
            &mut a,
            vec![
                ProgressEvent::AgentError {
                    agent: "Claude".into(),
                    kind: ProviderKind::Anthropic,
                    iteration: 1,
                    error: "old".to_string(),
                    details: Some("old details".to_string()),
                },
                ProgressEvent::AgentError {
                    agent: "OpenAI".into(),
                    kind: ProviderKind::OpenAI,
                    iteration: 2,
                    error: "new".to_string(),
                    details: Some("new details".to_string()),
                },
            ],
        );
        assert_eq!(
            find_last_error(&a),
            Some(("OpenAI".to_string(), "new details".to_string()))
        );
    }

    #[test]
    fn find_last_error_ignores_missing_details() {
        let mut a = app();
        a.record_progress(ProgressEvent::AgentError {
            agent: "OpenAI".into(),
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
        a.running.is_running = false;
        assert_eq!(current_status(&a), "Done");
    }

    #[test]
    fn current_status_reports_diagnostic_and_consolidation() {
        let mut a = app();
        a.running.is_running = true;
        a.running.diagnostic_running = true;
        assert_eq!(current_status(&a), "Analyzing reports for errors...");
        a.running.diagnostic_running = false;
        a.running.consolidation_running = true;
        assert_eq!(current_status(&a), "Consolidating reports...");
    }

    #[test]
    fn current_status_reports_waiting_when_no_active_agents() {
        let mut a = app();
        a.running.is_running = true;
        a.selected_agents = vec!["Claude".into()];
        push_events(
            &mut a,
            vec![ProgressEvent::AgentFinished {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration: 1,
            }],
        );
        assert_eq!(current_status(&a), "Waiting...");
    }

    #[test]
    fn current_status_reports_active_agent_names() {
        let mut a = app();
        a.running.is_running = true;
        a.selected_agents = vec!["Claude".into(), "OpenAI".into()];
        push_events(
            &mut a,
            vec![
                ProgressEvent::AgentStarted {
                    agent: "Claude".into(),
                    kind: ProviderKind::Anthropic,
                    iteration: 1,
                },
                ProgressEvent::AgentStarted {
                    agent: "OpenAI".into(),
                    kind: ProviderKind::OpenAI,
                    iteration: 1,
                },
            ],
        );
        let s = current_status(&a);
        assert!(s.contains("Claude"));
        assert!(s.contains("OpenAI"));
        assert!(s.contains("thinking"));
    }

    #[test]
    fn current_status_pipeline_shows_active_blocks() {
        let mut a = app();
        a.running.is_running = true;
        a.selected_mode = ExecutionMode::Pipeline;
        push_events(
            &mut a,
            vec![
                ProgressEvent::BlockStarted {
                    block_id: 1,
                    agent_name: "Claude".into(),
                    label: "Block 1".into(),
                    iteration: 1,
                    loop_pass: 0,
                },
                ProgressEvent::BlockStarted {
                    block_id: 2,
                    agent_name: "OpenAI".into(),
                    label: "Block 2".into(),
                    iteration: 1,
                    loop_pass: 0,
                },
            ],
        );
        let s = current_status(&a);
        assert!(s.contains("Block 1"));
        assert!(s.contains("Block 2"));
        assert!(s.contains("thinking"));
    }

    #[test]
    fn current_status_pipeline_excludes_finished_blocks() {
        let mut a = app();
        a.running.is_running = true;
        a.selected_mode = ExecutionMode::Pipeline;
        push_events(
            &mut a,
            vec![
                ProgressEvent::BlockStarted {
                    block_id: 1,
                    agent_name: "Claude".into(),
                    label: "Block 1".into(),
                    iteration: 1,
                    loop_pass: 0,
                },
                ProgressEvent::BlockFinished {
                    block_id: 1,
                    agent_name: "Claude".into(),
                    label: "Block 1".into(),
                    iteration: 1,
                    loop_pass: 0,
                },
                ProgressEvent::BlockStarted {
                    block_id: 2,
                    agent_name: "OpenAI".into(),
                    label: "Block 2".into(),
                    iteration: 1,
                    loop_pass: 0,
                },
            ],
        );
        let s = current_status(&a);
        assert!(!s.contains("Block 1"));
        assert!(s.contains("Block 2"));
    }

    #[test]
    fn current_status_pipeline_waiting_when_no_active_blocks() {
        let mut a = app();
        a.running.is_running = true;
        a.selected_mode = ExecutionMode::Pipeline;
        push_events(
            &mut a,
            vec![ProgressEvent::BlockFinished {
                block_id: 1,
                agent_name: "Claude".into(),
                label: "Block 1".into(),
                iteration: 1,
                loop_pass: 0,
            }],
        );
        assert_eq!(current_status(&a), "Waiting...");
    }

    #[test]
    fn current_status_shows_cancelled_when_flag_set() {
        let mut a = app();
        a.running.is_running = false;
        a.running
            .cancel_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(current_status(&a), "Cancelled");
    }

    #[test]
    fn current_status_shows_cancelling_while_run_is_still_active() {
        let mut a = app();
        a.running.is_running = true;
        a.running
            .cancel_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(current_status(&a), "Cancelling...");
    }

    #[test]
    fn build_event_items_marks_thinking_as_cancelled() {
        let mut a = app();
        a.running.is_running = false;
        a.running
            .cancel_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        push_events(
            &mut a,
            vec![
                ProgressEvent::AgentStarted {
                    agent: "Claude".into(),
                    kind: ProviderKind::Anthropic,
                    iteration: 1,
                },
                ProgressEvent::AgentFinished {
                    agent: "OpenAI".into(),
                    kind: ProviderKind::OpenAI,
                    iteration: 1,
                },
            ],
        );
        let items = build_event_items(&a);
        let texts: Vec<String> = items.iter().map(|i| format!("{i:?}")).collect();
        let joined = texts.join("\n");
        // Thinking agent should be cancelled, finished agent stays finished
        assert!(
            joined.contains("cancelled"),
            "expected cancelled in: {joined}"
        );
        assert!(
            joined.contains("finished"),
            "expected finished in: {joined}"
        );
        assert!(
            !joined.contains("thinking"),
            "expected no thinking in: {joined}"
        );
    }

    #[test]
    fn build_event_items_keeps_thinking_when_not_cancelled() {
        let mut a = app();
        a.running.is_running = true;
        push_events(
            &mut a,
            vec![ProgressEvent::AgentStarted {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration: 1,
            }],
        );
        let items = build_event_items(&a);
        let joined: String = items.iter().map(|i| format!("{i:?}")).collect();
        assert!(
            joined.contains("thinking"),
            "expected thinking in: {joined}"
        );
    }

    #[test]
    fn build_event_items_marks_thinking_blocks_as_cancelled() {
        let mut a = app();
        a.running.is_running = false;
        a.running
            .cancel_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        push_events(
            &mut a,
            vec![ProgressEvent::BlockStarted {
                block_id: 1,
                agent_name: "Claude".into(),
                label: "Block 1".into(),
                iteration: 1,
                loop_pass: 0,
            }],
        );
        let items = build_event_items(&a);
        let joined: String = items.iter().map(|i| format!("{i:?}")).collect();
        assert!(
            joined.contains("cancelled"),
            "expected cancelled in: {joined}"
        );
        assert!(
            !joined.contains("thinking"),
            "expected no thinking in: {joined}"
        );
    }

    #[test]
    fn resolve_stream_target_label_uses_block_row_label() {
        let mut a = app();
        a.running.block_rows.push(crate::app::BlockStatusRow {
            block_id: 0,
            source_block_id: 1,
            replica_index: 0,
            label: "Writer (r1)".into(),
            agent_name: "Claude".into(),
            provider: ProviderKind::Anthropic,
            status: crate::app::AgentRowStatus::Running,
        });
        let target = crate::app::StreamTarget::Block(0);
        assert_eq!(resolve_stream_target_label(&a, &target), "Writer (r1)");
    }

    #[test]
    fn resolve_stream_target_label_falls_back_for_unknown_id() {
        let a = app();
        let target = crate::app::StreamTarget::Block(99);
        assert_eq!(resolve_stream_target_label(&a, &target), "Block 99");
    }
}
