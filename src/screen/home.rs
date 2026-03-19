use super::centered_rect;
use super::help;
use crate::app::{App, EditPopupSection, HomeSection};
use crate::execution::ExecutionMode;
use crate::provider::ProviderKind;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Min(0),    // Main content
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title
    let title = Paragraph::new("House of Agents")
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Main content: agents list + mode picker side by side
    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    draw_agents_panel(f, app, main_chunks[0]);
    draw_mode_panel(f, app, main_chunks[1]);

    // Help bar
    let mut help_spans = vec![
        Span::styled("Space", Style::default().fg(Color::Yellow)),
        Span::raw(": toggle  "),
        Span::styled("Tab", Style::default().fg(Color::Yellow)),
        Span::raw(": switch panel  "),
        Span::styled("Enter", Style::default().fg(Color::Yellow)),
        Span::raw(": proceed  "),
        Span::styled("e", Style::default().fg(Color::Yellow)),
        Span::raw(": edit config  "),
    ];
    if app.memory.store.is_some() {
        help_spans.push(Span::styled("M", Style::default().fg(Color::Yellow)));
        help_spans.push(Span::raw(": memory  "));
    }
    help_spans.extend([
        Span::styled("?", Style::default().fg(Color::Yellow)),
        Span::raw(": help  "),
        Span::styled("q", Style::default().fg(Color::Yellow)),
        Span::raw(": quit"),
    ]);
    let help = Paragraph::new(Line::from(help_spans)).block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[2]);

    // Edit popup overlay
    if app.edit_popup.visible {
        draw_edit_popup(f, app);
    }

    // Help popup overlay
    if app.help_popup.active {
        help::draw_help_overlay(
            f,
            &app.help_popup,
            help::home_help_lines(),
            " Execution Modes ",
        );
    }

    // Error/info modals are rendered globally by screen::draw()
}

fn draw_agents_panel(f: &mut Frame, app: &App, area: Rect) {
    let agents = app.available_agents();
    let is_focused = app.home_section == HomeSection::Agents;

    let items: Vec<ListItem> = agents
        .iter()
        .enumerate()
        .map(|(i, (agent, available))| {
            let selected = app.selected_agents.contains(&agent.name);
            let marker = if selected { "[x]" } else { "[ ]" };
            let status = if *available { "" } else { " (unavailable)" };
            let name = &agent.name;

            let style = if !available {
                Style::default().fg(Color::DarkGray)
            } else if is_focused && i == app.home_cursor {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if selected {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };

            ListItem::new(format!("{marker} {name}{status}")).style(style)
        })
        .collect();

    let border_style = if is_focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let list = List::new(items).block(
        Block::default()
            .title(" Agents ")
            .borders(Borders::ALL)
            .border_style(border_style),
    );
    f.render_widget(list, area);
}

fn draw_mode_panel(f: &mut Frame, app: &App, area: Rect) {
    let is_focused = app.home_section == HomeSection::Mode;
    let modes = ExecutionMode::all();

    let items: Vec<ListItem> = modes
        .iter()
        .enumerate()
        .map(|(i, mode)| {
            let selected = app.selected_mode == *mode;
            let marker = if selected { "(o)" } else { "( )" };

            let style = if is_focused && i == app.home_cursor {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if selected {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };

            ListItem::new(Line::from(vec![
                Span::styled(format!("{marker} {mode}"), style),
                Span::styled(
                    format!("  {}", mode.description()),
                    Style::default().fg(Color::DarkGray),
                ),
            ]))
        })
        .collect();

    let border_style = if is_focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let list = List::new(items).block(
        Block::default()
            .title(" Mode ")
            .borders(Borders::ALL)
            .border_style(border_style),
    );
    f.render_widget(list, area);
}

fn draw_edit_popup(f: &mut Frame, app: &App) {
    let area = centered_rect(70, 60, f.area());
    let mut selected_line_start = 0usize;
    let diag_agent = app.config.diagnostic_provider.as_deref();
    let selected_cursor = match app.edit_popup.section {
        EditPopupSection::Providers => app.edit_popup.cursor,
        EditPopupSection::Timeouts => app.edit_popup.timeout_cursor,
        EditPopupSection::Memory => app.edit_popup.memory_cursor,
    };

    let mut header_lines = vec![
        Line::from(Span::styled(
            "j/k: navigate  Tab: section  [o]: output dir  [s]: save  Esc: keep for session  [n]: new  [Del]: remove  [p]: provider  [r]: rename  [d]: diagnostic  [c]: CLI/API  [b]: print/agent  [a]: key  [m]: model  [l]: list  [t]: effort  [x]: extra CLI",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(vec![
            Span::raw("Section: "),
            Span::styled(
                match app.edit_popup.section {
                    EditPopupSection::Providers => "Agents",
                    EditPopupSection::Timeouts => "Timeouts",
                    EditPopupSection::Memory => "Memory",
                },
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::raw("Output Dir: "),
            Span::styled(
                app.config.output_dir.clone(),
                Style::default().fg(Color::Yellow),
            ),
            Span::styled("  [o]", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::raw("Diagnostic Agent: "),
            Span::styled(
                diag_agent.unwrap_or("off"),
                if diag_agent.is_some() {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                },
            ),
            Span::styled("  [d]", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(if app.edit_popup.config_save_in_progress {
            Span::styled(
                "Saving config to disk...",
                Style::default().fg(Color::Yellow),
            )
        } else {
            Span::raw("")
        }),
        Line::from(""),
    ];

    if app.edit_popup.editing && matches!(app.edit_popup.field, crate::app::EditField::OutputDir) {
        header_lines.push(Line::from(Span::styled(
            format!(
                "New output dir: {}_ (Enter: save, Esc: cancel)",
                app.edit_popup.edit_buffer
            ),
            Style::default().fg(Color::Yellow),
        )));
        header_lines.push(Line::from(""));
    }

    let mut body_lines: Vec<Line> = Vec::new();
    match app.edit_popup.section {
        EditPopupSection::Providers => {
            let agents = app.available_agents();
            for (i, (agent_cfg, _available)) in agents.iter().enumerate() {
                let config = app.effective_agent_config(&agent_cfg.name);
                let provider = config.map(|c| c.provider).unwrap_or(agent_cfg.provider);
                let cli_installed = app.cli_available.get(&provider).copied().unwrap_or(false);
                let use_cli = config.map(|c| c.use_cli).unwrap_or(false);
                let (key, model, extra_cli_args, thinking_label, has_api_key) = match config {
                    Some(c) => {
                        let thinking = match c.provider {
                            ProviderKind::OpenAI => match c.reasoning_effort.as_deref() {
                                Some(e) => format!("reasoning: {e}"),
                                None => "off".into(),
                            },
                            _ => match c.thinking_effort.as_deref() {
                                Some(e) => format!("effort: {e}"),
                                None => "off".into(),
                            },
                        };
                        (
                            mask_key(&c.api_key),
                            c.model.clone(),
                            if c.extra_cli_args.trim().is_empty() {
                                "(none)".into()
                            } else {
                                c.extra_cli_args.clone()
                            },
                            thinking,
                            !c.api_key.trim().is_empty(),
                        )
                    }
                    None => (
                        "(not set)".into(),
                        "(not set)".into(),
                        "(none)".into(),
                        "off".into(),
                        false,
                    ),
                };

                let (mode_text, mode_style) = if use_cli {
                    ("CLI", Style::default().fg(Color::Green))
                } else if !cli_installed {
                    ("API (no CLI)", Style::default().fg(Color::DarkGray))
                } else {
                    ("API", Style::default())
                };
                let effort_title = match provider {
                    ProviderKind::OpenAI => "Reasoning",
                    ProviderKind::Anthropic | ProviderKind::Gemini => "Thinking",
                };

                let is_selected = i == selected_cursor;
                let style = if is_selected {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                if is_selected {
                    selected_line_start = body_lines.len();
                }

                let is_diag = diag_agent == Some(agent_cfg.name.as_str());
                let mut name_spans = vec![Span::styled(
                    format!("{} ({})", agent_cfg.name, provider.display_name()),
                    style,
                )];
                if is_diag {
                    name_spans.push(Span::styled(" [diag]", Style::default().fg(Color::Green)));
                }
                body_lines.push(Line::from(name_spans));
                let cli_print_mode = config.map(|c| c.cli_print_mode).unwrap_or(true);
                if is_selected && !app.edit_popup.editing {
                    body_lines.push(Line::from(vec![
                        Span::raw("  Mode:     "),
                        Span::styled(mode_text, mode_style),
                        Span::styled("  [c]", Style::default().fg(Color::DarkGray)),
                    ]));
                    if provider == ProviderKind::Anthropic && use_cli {
                        let (cm_text, cm_style) = if cli_print_mode {
                            ("print (-p)", Style::default())
                        } else {
                            ("agent", Style::default().fg(Color::Green))
                        };
                        body_lines.push(Line::from(vec![
                            Span::raw("  CLI Mode: "),
                            Span::styled(cm_text, cm_style),
                            Span::styled("  [b]", Style::default().fg(Color::DarkGray)),
                        ]));
                    }
                    let key_style = if use_cli {
                        Style::default().fg(Color::DarkGray)
                    } else {
                        Style::default()
                    };
                    body_lines.push(Line::from(vec![
                        Span::styled("  Key:      ", key_style),
                        Span::styled(key.clone(), key_style),
                        Span::styled("  [a]", Style::default().fg(Color::DarkGray)),
                    ]));
                    body_lines.push(Line::from(vec![
                        Span::raw("  Model:    "),
                        Span::raw(model.clone()),
                        Span::styled("  [m] [l]", Style::default().fg(Color::DarkGray)),
                    ]));
                    let extra_cli_style = cli_dependent_style(use_cli);
                    body_lines.push(Line::from(vec![
                        Span::styled("  Extra CLI:", extra_cli_style),
                        Span::styled(format!(" {extra_cli_args}"), extra_cli_style),
                        Span::styled("  [x]", Style::default().fg(Color::DarkGray)),
                    ]));
                    if !has_api_key {
                        body_lines.push(Line::from(Span::styled(
                            "            Add API key to fetch model list",
                            Style::default().fg(Color::DarkGray),
                        )));
                    }
                    let thinking_style = if thinking_label == "off" {
                        Style::default().fg(Color::DarkGray)
                    } else {
                        Style::default().fg(Color::Magenta)
                    };
                    body_lines.push(Line::from(vec![
                        Span::raw(format!("  {effort_title}: ").to_string()),
                        Span::styled(thinking_label.clone(), thinking_style),
                        Span::styled("  [t]", Style::default().fg(Color::DarkGray)),
                    ]));
                } else {
                    let thinking_style = if thinking_label == "off" {
                        Style::default().fg(Color::DarkGray)
                    } else {
                        Style::default().fg(Color::Magenta)
                    };
                    body_lines.push(Line::from(vec![
                        Span::raw("  Mode:     "),
                        Span::styled(mode_text, mode_style),
                    ]));
                    let key_style = if use_cli {
                        Style::default().fg(Color::DarkGray)
                    } else {
                        Style::default()
                    };
                    body_lines.push(Line::from(vec![
                        Span::styled("  Key:      ", key_style),
                        Span::styled(key, key_style),
                    ]));
                    body_lines.push(Line::from(format!("  Model:    {model}")));
                    let extra_cli_style = cli_dependent_style(use_cli);
                    body_lines.push(Line::from(vec![
                        Span::styled("  Extra CLI:", extra_cli_style),
                        Span::styled(format!(" {extra_cli_args}"), extra_cli_style),
                    ]));
                    body_lines.push(Line::from(vec![
                        Span::raw(format!("  {effort_title}: ").to_string()),
                        Span::styled(thinking_label, thinking_style),
                    ]));
                }

                if is_selected
                    && app.edit_popup.editing
                    && !matches!(app.edit_popup.field, crate::app::EditField::OutputDir)
                {
                    let field_name = match app.edit_popup.field {
                        crate::app::EditField::ApiKey => "key",
                        crate::app::EditField::Model => "model",
                        crate::app::EditField::ExtraCliArgs => "extra cli args",
                        crate::app::EditField::OutputDir => "output dir",
                        crate::app::EditField::TimeoutSeconds => "timeout",
                        crate::app::EditField::AgentName => "name",
                        crate::app::EditField::MemoryValue => "value",
                    };
                    selected_line_start = body_lines.len();
                    body_lines.push(Line::from(Span::styled(
                        format!(
                            "  New {}: {}_ (Enter: save, Esc: cancel)",
                            field_name, app.edit_popup.edit_buffer
                        ),
                        Style::default().fg(Color::Yellow),
                    )));
                }

                body_lines.push(Line::from(""));
            }
        }
        EditPopupSection::Timeouts => {
            let timeout_rows = [
                (
                    "HTTP/API timeout",
                    "Applied to provider API calls when CLI mode is off.",
                    app.effective_http_timeout_seconds(),
                    app.session_http_timeout_seconds,
                ),
                (
                    "Model fetch timeout",
                    "Applied when fetching model lists from the config popup.",
                    app.effective_model_fetch_timeout_seconds(),
                    app.session_model_fetch_timeout_seconds,
                ),
                (
                    "CLI timeout",
                    "Applied to provider CLI calls when CLI mode is on.",
                    app.effective_cli_timeout_seconds(),
                    app.session_cli_timeout_seconds,
                ),
            ];

            body_lines.push(Line::from(Span::styled(
                "Press Enter or [e] to edit selected timeout.",
                Style::default().fg(Color::DarkGray),
            )));
            body_lines.push(Line::from(""));

            for (i, (label, description, value, session_override)) in
                timeout_rows.iter().enumerate()
            {
                let is_selected = i == selected_cursor;
                let style = if is_selected {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                if is_selected {
                    selected_line_start = body_lines.len();
                }

                body_lines.push(Line::from(vec![
                    Span::styled(if is_selected { "▸ " } else { "  " }, style),
                    Span::styled(format!("{label}: {value}s"), style),
                    Span::styled(
                        if session_override.is_some() {
                            "  (session override)"
                        } else {
                            "  (global default)"
                        },
                        if session_override.is_some() {
                            Style::default().fg(Color::Yellow)
                        } else {
                            Style::default().fg(Color::DarkGray)
                        },
                    ),
                ]));
                body_lines.push(Line::from(vec![
                    Span::raw("   "),
                    Span::styled(*description, Style::default().fg(Color::DarkGray)),
                ]));

                if is_selected
                    && app.edit_popup.editing
                    && matches!(app.edit_popup.field, crate::app::EditField::TimeoutSeconds)
                {
                    selected_line_start = body_lines.len();
                    body_lines.push(Line::from(Span::styled(
                        format!(
                            "   New timeout: {}_ (seconds, Enter: save, Esc: cancel)",
                            app.edit_popup.edit_buffer
                        ),
                        Style::default().fg(Color::Yellow),
                    )));
                }

                body_lines.push(Line::from(""));
            }
        }
        EditPopupSection::Memory => {
            body_lines.push(Line::from(Span::styled(
                "Space: toggle  Enter/[e]: edit  [s]: save to disk",
                Style::default().fg(Color::DarkGray),
            )));
            body_lines.push(Line::from(""));

            // Row order must match MEM_* constants in tui/input.rs
            let memory_rows: Vec<MemoryRow> = vec![
                MemoryRow::Toggle {
                    label: "Enabled",
                    description: "Enable cross-run memory system (SQLite+FTS5).",
                    value: app.effective_memory_enabled(),
                    has_override: app.session_memory_enabled.is_some(),
                },
                MemoryRow::Value {
                    label: "Max Recall",
                    description: "Maximum number of memories injected per run.",
                    value: app.effective_memory_max_recall().to_string(),
                    has_override: app.session_memory_max_recall.is_some(),
                },
                MemoryRow::Value {
                    label: "Max Recall Bytes",
                    description: "Maximum total bytes of recalled memory context.",
                    value: app.effective_memory_max_recall_bytes().to_string(),
                    has_override: app.session_memory_max_recall_bytes.is_some(),
                },
                MemoryRow::Value {
                    label: "Observation TTL",
                    description: "Days before observation memories expire.",
                    value: format!("{} days", app.effective_memory_observation_ttl_days()),
                    has_override: app.session_memory_observation_ttl_days.is_some(),
                },
                MemoryRow::Value {
                    label: "Summary TTL",
                    description: "Days before summary memories expire.",
                    value: format!("{} days", app.effective_memory_summary_ttl_days()),
                    has_override: app.session_memory_summary_ttl_days.is_some(),
                },
                MemoryRow::Value {
                    label: "Stale Permanent Days",
                    description:
                        "Archive permanent memories after N days without recall (0 = disabled).",
                    value: format!("{} days", app.effective_memory_stale_permanent_days()),
                    has_override: app.session_memory_stale_permanent_days.is_some(),
                },
                MemoryRow::Value {
                    label: "Extraction Agent",
                    description: "Agent used for post-run memory extraction (empty = auto).",
                    value: {
                        let explicit = app.effective_memory_extraction_agent();
                        if explicit.is_empty() {
                            match app.resolved_extraction_agent() {
                                Some(name) => format!("(auto: {name})"),
                                None => "(auto: none)".to_string(),
                            }
                        } else {
                            explicit.to_string()
                        }
                    },
                    has_override: app.session_memory_extraction_agent.is_some(),
                },
                MemoryRow::Toggle {
                    label: "Disable Extraction",
                    description: "Skip post-run memory extraction entirely.",
                    value: app.effective_memory_disable_extraction(),
                    has_override: app.session_memory_disable_extraction.is_some(),
                },
            ];

            for (i, row) in memory_rows.iter().enumerate() {
                let is_selected = i == selected_cursor;
                let style = if is_selected {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                if is_selected {
                    selected_line_start = body_lines.len();
                }

                match row {
                    MemoryRow::Toggle {
                        label,
                        description: _,
                        value,
                        has_override,
                    } => {
                        let (val_text, val_style) = if *value {
                            ("on", Style::default().fg(Color::Green))
                        } else {
                            ("off", Style::default().fg(Color::Red))
                        };
                        body_lines.push(Line::from(vec![
                            Span::styled(if is_selected { "▸ " } else { "  " }, style),
                            Span::styled(format!("{label}: "), style),
                            Span::styled(val_text, val_style),
                            Span::styled(
                                if *has_override {
                                    "  (session override)"
                                } else {
                                    "  (global default)"
                                },
                                if *has_override {
                                    Style::default().fg(Color::Yellow)
                                } else {
                                    Style::default().fg(Color::DarkGray)
                                },
                            ),
                        ]));
                    }
                    MemoryRow::Value {
                        label,
                        description: _,
                        value,
                        has_override,
                    } => {
                        body_lines.push(Line::from(vec![
                            Span::styled(if is_selected { "▸ " } else { "  " }, style),
                            Span::styled(format!("{label}: {value}"), style),
                            Span::styled(
                                if *has_override {
                                    "  (session override)"
                                } else {
                                    "  (global default)"
                                },
                                if *has_override {
                                    Style::default().fg(Color::Yellow)
                                } else {
                                    Style::default().fg(Color::DarkGray)
                                },
                            ),
                        ]));
                    }
                }

                body_lines.push(Line::from(vec![
                    Span::raw("   "),
                    Span::styled(row.description(), Style::default().fg(Color::DarkGray)),
                ]));

                if is_selected
                    && app.edit_popup.editing
                    && matches!(app.edit_popup.field, crate::app::EditField::MemoryValue)
                {
                    // Indices match MEM_* constants in tui/input.rs
                    let unit_hint = match app.edit_popup.memory_cursor {
                        1 => " (count)",    // MEM_MAX_RECALL
                        2 => " (bytes)",    // MEM_MAX_RECALL_BYTES
                        3..=5 => " (days)", // MEM_OBSERVATION_TTL | MEM_SUMMARY_TTL | MEM_STALE_PERMANENT_DAYS
                        _ => "",
                    };
                    selected_line_start = body_lines.len();
                    body_lines.push(Line::from(Span::styled(
                        format!(
                            "   New value{}: {}_ (Enter: save, Esc: cancel)",
                            unit_hint, app.edit_popup.edit_buffer
                        ),
                        Style::default().fg(Color::Yellow),
                    )));
                }

                body_lines.push(Line::from(""));
            }
        }
    }

    let block = Block::default()
        .title(" Configuration ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    f.render_widget(ratatui::widgets::Clear, area);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width > 0 && inner.height > 0 {
        let header_height = if inner.width > 0 {
            header_lines
                .iter()
                .map(|l| {
                    let w: usize = l.spans.iter().map(|s| s.content.len()).sum();
                    1usize.max(w.div_ceil(inner.width as usize))
                })
                .sum::<usize>()
                .min(inner.height as usize)
        } else {
            header_lines.len().min(inner.height as usize)
        } as u16;
        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(header_height), Constraint::Min(0)])
            .split(inner);

        let header = Paragraph::new(header_lines).wrap(Wrap { trim: false });
        f.render_widget(header, sections[0]);

        if sections[1].height > 0 {
            if body_lines.is_empty() {
                body_lines.push(Line::from(""));
            }
            let lines_len = body_lines.len();
            let mut body = Paragraph::new(body_lines).wrap(Wrap { trim: false });
            let body_height = sections[1].height as usize;
            if body_height > 0 && lines_len > body_height {
                let max_scroll = lines_len - body_height;
                let scroll = selected_line_start.min(max_scroll) as u16;
                body = body.scroll((scroll, 0));
            }
            f.render_widget(body, sections[1]);
        }
    }

    // Model picker overlay
    if app.edit_popup.model_picker_active {
        draw_model_picker(f, app);
    }
}

fn draw_model_picker(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 70, f.area());

    let provider_name: &str = app
        .config
        .agents
        .get(app.edit_popup.cursor)
        .map(|a| a.name.as_str())
        .unwrap_or("?");

    if app.edit_popup.model_picker_loading {
        let block = Block::default()
            .title(format!(" {provider_name} — Loading models... "))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));
        let text = Paragraph::new("Fetching available models from API...\n\nEsc: cancel")
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(ratatui::widgets::Clear, area);
        f.render_widget(text.block(block), area);
        return;
    }

    // Search bar
    let filter_display = if app.edit_popup.model_picker_filter.is_empty() {
        "Type to filter...".to_string()
    } else {
        app.edit_popup.model_picker_filter.clone()
    };
    let filter_style = if app.edit_popup.model_picker_filter.is_empty() {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default().fg(Color::Yellow)
    };

    // Build filtered view on demand from the single model_picker_list source
    let filter_lc = app.edit_popup.model_picker_filter.to_lowercase();
    let filtered: Vec<&str> = if filter_lc.is_empty() {
        app.edit_popup
            .model_picker_list
            .iter()
            .map(|s| s.as_str())
            .collect()
    } else {
        app.edit_popup
            .model_picker_list
            .iter()
            .filter(|m| m.to_lowercase().contains(&filter_lc))
            .map(|s| s.as_str())
            .collect()
    };

    let inner_height = area.height.saturating_sub(2) as usize;
    let reserved_rows = 4usize; // filter, spacer, spacer, help
    let visible_model_rows = inner_height.saturating_sub(reserved_rows).max(1);
    let total_models = filtered.len();
    let max_start = total_models.saturating_sub(visible_model_rows);
    let start = app
        .edit_popup
        .model_picker_cursor
        .saturating_sub(visible_model_rows / 2)
        .min(max_start);
    let end = (start + visible_model_rows).min(total_models);

    let items: Vec<ListItem> = std::iter::once(ListItem::new(Line::from(vec![
        Span::styled("⌕ ", Style::default().fg(Color::Yellow)),
        Span::styled(format!("{filter_display}_"), filter_style),
    ])))
    .chain(std::iter::once(ListItem::new("")))
    .chain(
        filtered[start..end]
            .iter()
            .enumerate()
            .map(|(offset, model)| {
                let i = start + offset;
                let style = if i == app.edit_popup.model_picker_cursor {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                let marker = if i == app.edit_popup.model_picker_cursor {
                    "▸ "
                } else {
                    "  "
                };
                ListItem::new(format!("{marker}{model}")).style(style)
            }),
    )
    .collect();

    let shown = filtered.len();
    let total = app.edit_popup.model_picker_list.len();
    let count_label = if shown == total {
        format!("{total}")
    } else {
        format!("{shown}/{total}")
    };

    let block = Block::default()
        .title(format!(" {provider_name} — Select Model ({count_label}) ",))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    // Help line at bottom
    let mut all_lines: Vec<ListItem> = items;
    all_lines.push(ListItem::new(""));
    all_lines.push(ListItem::new(Line::from(Span::styled(
        "↑/↓: navigate  Enter: select  Esc: cancel  Type: filter",
        Style::default().fg(Color::DarkGray),
    ))));

    let list = List::new(all_lines).block(block);
    f.render_widget(ratatui::widgets::Clear, area);
    f.render_widget(list, area);
}

enum MemoryRow {
    Toggle {
        label: &'static str,
        description: &'static str,
        value: bool,
        has_override: bool,
    },
    Value {
        label: &'static str,
        description: &'static str,
        value: String,
        has_override: bool,
    },
}

impl MemoryRow {
    fn description(&self) -> &'static str {
        match self {
            MemoryRow::Toggle { description, .. } | MemoryRow::Value { description, .. } => {
                description
            }
        }
    }
}

fn cli_dependent_style(use_cli: bool) -> Style {
    if use_cli {
        Style::default()
    } else {
        Style::default().fg(Color::DarkGray)
    }
}

fn mask_key(key: &str) -> String {
    let total_chars = key.chars().count();
    if total_chars <= 8 {
        "****".into()
    } else {
        let prefix: String = key.chars().take(4).collect();
        let suffix: String = key
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("{prefix}...{suffix}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_dependent_style_for_cli_mode_is_default() {
        let style = cli_dependent_style(true);
        assert_eq!(style.fg, None);
    }

    #[test]
    fn cli_dependent_style_for_api_mode_is_dimmed() {
        let style = cli_dependent_style(false);
        assert_eq!(style.fg, Some(Color::DarkGray));
    }

    #[test]
    fn mask_key_short_values() {
        assert_eq!(mask_key("abcd"), "****");
    }

    #[test]
    fn mask_key_long_values() {
        assert_eq!(mask_key("abcdefghijkl"), "abcd...ijkl");
    }

    #[test]
    fn mask_key_long_values_unicode_safe() {
        assert_eq!(mask_key("ééééabcdéééé"), "éééé...éééé");
    }
}
