use super::centered_rect;
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
    let help = Paragraph::new(Line::from(vec![
        Span::styled("Space", Style::default().fg(Color::Yellow)),
        Span::raw(": toggle  "),
        Span::styled("Tab", Style::default().fg(Color::Yellow)),
        Span::raw(": switch panel  "),
        Span::styled("Enter", Style::default().fg(Color::Yellow)),
        Span::raw(": proceed  "),
        Span::styled("e", Style::default().fg(Color::Yellow)),
        Span::raw(": edit config  "),
        Span::styled("q", Style::default().fg(Color::Yellow)),
        Span::raw(": quit"),
    ]))
    .block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[2]);

    // Error modal overlay
    if let Some(ref err) = app.error_modal {
        draw_error_modal(f, err);
    }

    // Edit popup overlay
    if app.show_edit_popup {
        draw_edit_popup(f, app);
    }
}

fn draw_agents_panel(f: &mut Frame, app: &App, area: Rect) {
    let providers = app.available_providers();
    let is_focused = app.home_section == HomeSection::Agents;

    let items: Vec<ListItem> = providers
        .iter()
        .enumerate()
        .map(|(i, (kind, available))| {
            let selected = app.selected_agents.contains(kind);
            let marker = if selected { "[x]" } else { "[ ]" };
            let status = if *available { "" } else { " (unavailable)" };
            let name = kind.display_name();

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

fn draw_error_modal(f: &mut Frame, message: &str) {
    let area = centered_rect(60, 20, f.area());
    let block = Block::default()
        .title(" Error ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Red));
    let text = Paragraph::new(message)
        .style(Style::default().fg(Color::Red))
        .block(block);
    f.render_widget(ratatui::widgets::Clear, area);
    f.render_widget(text, area);
}

fn draw_edit_popup(f: &mut Frame, app: &App) {
    let area = centered_rect(70, 60, f.area());
    let mut selected_line_idx = 0usize;
    let diag_kind = diagnostic_provider_kind(app);
    let selected_cursor = match app.edit_popup_section {
        EditPopupSection::Providers => app.edit_popup_cursor,
        EditPopupSection::Diagnostics => app.edit_popup_diagnostic_cursor,
    };

    let mut header_lines = vec![
        Line::from(Span::styled(
            "j/k: navigate providers  Tab: switch section  [o]: output dir  [s]: save to disk",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "[c]: CLI/API  [a]: key  [m]: model  [l]: list models  [t]: effort/reasoning  [d]: set/clear diagnostic provider  Esc: keep for session",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(vec![
            Span::raw("Section: "),
            Span::styled(
                match app.edit_popup_section {
                    EditPopupSection::Providers => "Run Providers",
                    EditPopupSection::Diagnostics => "Diagnostics",
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
            Span::raw("Diagnostics Provider: "),
            Span::styled(
                diag_kind
                    .map(|k| k.display_name().to_string())
                    .unwrap_or_else(|| "off".into()),
                if diag_kind.is_some() {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                },
            ),
            Span::styled("  [d]", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(if app.config_save_in_progress {
            Span::styled(
                "Saving config to disk...",
                Style::default().fg(Color::Yellow),
            )
        } else {
            Span::raw("")
        }),
        Line::from(""),
    ];

    if app.edit_popup_editing && matches!(app.edit_popup_field, crate::app::EditField::OutputDir) {
        header_lines.push(Line::from(Span::styled(
            format!(
                "New output dir: {}_ (Enter: save, Esc: cancel)",
                app.edit_buffer
            ),
            Style::default().fg(Color::Yellow),
        )));
        header_lines.push(Line::from(""));
    }

    let mut body_lines: Vec<Line> = Vec::new();
    match app.edit_popup_section {
        EditPopupSection::Providers => {
            let providers = app.available_providers();
            for (i, (kind, _available)) in providers.iter().enumerate() {
                let config = app.effective_provider_config(*kind);
                let cli_installed = app.cli_available.get(kind).copied().unwrap_or(false);
                let use_cli = config.as_ref().map(|c| c.use_cli).unwrap_or(false);
                let (key, model, thinking_label, has_api_key) = match &config {
                    Some(c) => {
                        let thinking = match kind {
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
                            thinking,
                            !c.api_key.trim().is_empty(),
                        )
                    }
                    None => ("(not set)".into(), "(not set)".into(), "off".into(), false),
                };

                let (mode_text, mode_style) = if use_cli {
                    ("CLI", Style::default().fg(Color::Green))
                } else if !cli_installed {
                    ("API (no CLI)", Style::default().fg(Color::DarkGray))
                } else {
                    ("API", Style::default())
                };
                let effort_title = match kind {
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
                    selected_line_idx = body_lines.len();
                }

                body_lines.push(Line::from(Span::styled(
                    kind.display_name().to_string(),
                    style,
                )));
                if is_selected && !app.edit_popup_editing {
                    body_lines.push(Line::from(vec![
                        Span::raw("  Mode:     "),
                        Span::styled(mode_text, mode_style),
                        Span::styled("  [c]", Style::default().fg(Color::DarkGray)),
                    ]));
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
                    body_lines.push(Line::from(vec![
                        Span::raw(format!("  {effort_title}: ").to_string()),
                        Span::styled(thinking_label, thinking_style),
                    ]));
                }

                if is_selected
                    && app.edit_popup_editing
                    && !matches!(app.edit_popup_field, crate::app::EditField::OutputDir)
                {
                    let field_name = match app.edit_popup_field {
                        crate::app::EditField::ApiKey => "key",
                        crate::app::EditField::Model => "model",
                        crate::app::EditField::OutputDir => "output dir",
                    };
                    selected_line_idx = body_lines.len();
                    body_lines.push(Line::from(Span::styled(
                        format!(
                            "  New {}: {}_ (Enter: save, Esc: cancel)",
                            field_name, app.edit_buffer
                        ),
                        Style::default().fg(Color::Yellow),
                    )));
                }

                body_lines.push(Line::from(""));
            }
        }
        EditPopupSection::Diagnostics => {
            let kind = ProviderKind::all()
                .get(selected_cursor)
                .copied()
                .unwrap_or(ProviderKind::Anthropic);
            let config = app.effective_diagnostic_config(kind);
            let cli_installed = app.cli_available.get(&kind).copied().unwrap_or(false);
            let use_cli = config.as_ref().map(|c| c.use_cli).unwrap_or(false);
            let (key, model, thinking_label, has_api_key) = match &config {
                Some(c) => {
                    let thinking = match kind {
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
                        thinking,
                        !c.api_key.trim().is_empty(),
                    )
                }
                None => ("(not set)".into(), "(not set)".into(), "off".into(), false),
            };
            let (mode_text, mode_style) = if use_cli {
                ("CLI", Style::default().fg(Color::Green))
            } else if !cli_installed {
                ("API (no CLI)", Style::default().fg(Color::DarkGray))
            } else {
                ("API", Style::default())
            };
            let effort_title = match kind {
                ProviderKind::OpenAI => "Reasoning",
                ProviderKind::Anthropic | ProviderKind::Gemini => "Thinking",
            };
            let active = is_diagnostic_provider(app, kind);
            body_lines.push(Line::from(vec![
                Span::raw("Provider: "),
                Span::styled(
                    kind.display_name(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("  (j/k)", Style::default().fg(Color::DarkGray)),
            ]));
            body_lines.push(Line::from(vec![
                Span::raw("  Active:   "),
                Span::styled(
                    if active { "yes" } else { "no" },
                    if active {
                        Style::default().fg(Color::Green)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    },
                ),
                Span::styled("  [d]", Style::default().fg(Color::DarkGray)),
            ]));
            body_lines.push(Line::from(vec![
                Span::raw("  Mode:     "),
                Span::styled(mode_text, mode_style),
                Span::styled("  [c]", Style::default().fg(Color::DarkGray)),
            ]));
            let key_style = if use_cli {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default()
            };
            body_lines.push(Line::from(vec![
                Span::styled("  Key:      ", key_style),
                Span::styled(key, key_style),
                Span::styled("  [a]", Style::default().fg(Color::DarkGray)),
            ]));
            if !active {
                body_lines.push(Line::from(Span::styled(
                    "  Diagnostics are currently off for this provider",
                    Style::default().fg(Color::DarkGray),
                )));
            }
            if active {
                body_lines.push(Line::from(vec![
                    Span::raw("  Model:    "),
                    Span::raw(model.clone()),
                    Span::styled("  [m] [l]", Style::default().fg(Color::DarkGray)),
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
                    Span::styled(thinking_label, thinking_style),
                    Span::styled("  [t]", Style::default().fg(Color::DarkGray)),
                ]));
            }

            if app.edit_popup_editing
                && !matches!(app.edit_popup_field, crate::app::EditField::OutputDir)
            {
                let field_name = match app.edit_popup_field {
                    crate::app::EditField::ApiKey => "key",
                    crate::app::EditField::Model => "model",
                    crate::app::EditField::OutputDir => "output dir",
                };
                selected_line_idx = body_lines.len();
                body_lines.push(Line::from(Span::styled(
                    format!(
                        "  New {}: {}_ (Enter: save, Esc: cancel)",
                        field_name, app.edit_buffer
                    ),
                    Style::default().fg(Color::Yellow),
                )));
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
        let header_height = header_lines.len().min(inner.height as usize) as u16;
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
                let scroll = selected_line_idx
                    .saturating_sub(body_height / 2)
                    .min(max_scroll) as u16;
                body = body.scroll((scroll, 0));
            }
            f.render_widget(body, sections[1]);
        }
    }

    // Model picker overlay
    if app.model_picker_active {
        draw_model_picker(f, app);
    }
}

fn draw_model_picker(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 70, f.area());

    let cursor = match app.edit_popup_section {
        EditPopupSection::Providers => app.edit_popup_cursor,
        EditPopupSection::Diagnostics => app.edit_popup_diagnostic_cursor,
    };
    let provider_name = ProviderKind::all()
        .get(cursor)
        .map(|k| k.display_name())
        .unwrap_or("?");

    if app.model_picker_loading {
        let block = Block::default()
            .title(format!(" {} — Loading models... ", provider_name))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));
        let text = Paragraph::new("Fetching available models from API...\n\nEsc: cancel")
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(ratatui::widgets::Clear, area);
        f.render_widget(text.block(block), area);
        return;
    }

    // Search bar
    let filter_display = if app.model_picker_filter.is_empty() {
        "Type to filter...".to_string()
    } else {
        app.model_picker_filter.clone()
    };
    let filter_style = if app.model_picker_filter.is_empty() {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default().fg(Color::Yellow)
    };

    let inner_height = area.height.saturating_sub(2) as usize;
    let reserved_rows = 4usize; // filter, spacer, spacer, help
    let visible_model_rows = inner_height.saturating_sub(reserved_rows).max(1);
    let total_models = app.model_picker_list.len();
    let max_start = total_models.saturating_sub(visible_model_rows);
    let start = app
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
        app.model_picker_list[start..end]
            .iter()
            .enumerate()
            .map(|(offset, model)| {
                let i = start + offset;
                let style = if i == app.model_picker_cursor {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                let marker = if i == app.model_picker_cursor {
                    "▸ "
                } else {
                    "  "
                };
                ListItem::new(format!("{marker}{model}")).style(style)
            }),
    )
    .collect();

    let shown = app.model_picker_list.len();
    let total = app.model_picker_all_models.len();
    let count_label = if shown == total {
        format!("{total}")
    } else {
        format!("{shown}/{total}")
    };

    let block = Block::default()
        .title(format!(
            " {} — Select Model ({}) ",
            provider_name, count_label,
        ))
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

fn mask_key(key: &str) -> String {
    if key.len() <= 8 {
        "****".into()
    } else {
        format!("{}...{}", &key[..4], &key[key.len() - 4..])
    }
}

fn is_diagnostic_provider(app: &App, kind: ProviderKind) -> bool {
    app.config.diagnostic_provider.as_deref() == Some(kind.config_key())
}

fn diagnostic_provider_kind(app: &App) -> Option<ProviderKind> {
    ProviderKind::all()
        .iter()
        .copied()
        .find(|k| app.config.diagnostic_provider.as_deref() == Some(k.config_key()))
}
