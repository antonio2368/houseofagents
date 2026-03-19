use super::centered_rect;
use super::results::render_markdown;
use crate::app::{HelpPopupState, SetupAnalysisState};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;
use std::sync::LazyLock;

pub const PIPELINE_TAB_COUNT: usize = 7;
pub const PIPELINE_TAB_NAMES: [&str; PIPELINE_TAB_COUNT] = [
    "Overview",
    "Canvas Navigation",
    "Block Operations",
    "Wiring",
    "Sessions",
    "Prompt & Settings",
    "Finalization",
];

// ---------------------------------------------------------------------------
// Content functions
// ---------------------------------------------------------------------------

pub fn home_help_lines() -> &'static [Line<'static>] {
    static LINES: LazyLock<Vec<Line<'static>>> = LazyLock::new(|| {
        vec![
            Line::from(Span::styled(
                "Relay",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from("  Agents run one after another in sequence. Each agent"),
            Line::from("  receives the previous agent's full output and builds"),
            Line::from("  on it. Over multiple iterations the baton passes around"),
            Line::from("  the ring, progressively refining the result."),
            Line::from(""),
            Line::from("  Best for: deep refinement, iterative improvement,"),
            Line::from("  tasks where each step should build on the last."),
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                "Swarm",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from("  All agents run in parallel each round. After a round"),
            Line::from("  completes, every agent receives all other agents'"),
            Line::from("  outputs and produces an updated analysis. This repeats"),
            Line::from("  for the configured number of iterations."),
            Line::from(""),
            Line::from("  Best for: multi-perspective analysis, debates,"),
            Line::from("  cross-checking, consensus-building."),
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                "Pipeline",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from("  Build a custom DAG of agent blocks with explicit connections."),
            Line::from(
                "  Each block has one or more agents, optional profiles, a prompt, and an optional session ID.",
            ),
            Line::from("  Blocks execute as soon as their dependencies are satisfied,"),
            Line::from("  maximizing parallelism. Root blocks receive the initial prompt;"),
            Line::from("  on subsequent iterations they receive terminal block outputs."),
            Line::from(""),
            Line::from("  Best for: complex multi-step pipelines, hierarchical analysis,"),
            Line::from("  workflows requiring specific agent ordering and data flow."),
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                "Headless Mode",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from("  All modes can also run noninteractively from the CLI."),
            Line::from("  Pass --prompt/--prompt-file or --pipeline to skip the TUI."),
            Line::from("  Run houseofagents --help for full flag reference."),
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                "Memory",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from("  Cross-run memory powered by SQLite+FTS5. Memories from"),
            Line::from("  past runs are recalled and injected into prompts."),
            Line::from("  Post-run extraction stores new memories automatically."),
            Line::from(""),
            Line::from(vec![
                Span::styled("  M", Style::default().fg(Color::Yellow)),
                Span::raw(": Open memory management (on home screen)"),
            ]),
            Line::from(""),
            Line::from("  In memory management:"),
            Line::from(vec![
                Span::styled("  j/k", Style::default().fg(Color::Yellow)),
                Span::raw(": navigate  "),
                Span::styled("d", Style::default().fg(Color::Yellow)),
                Span::raw(": delete  "),
                Span::styled("D", Style::default().fg(Color::Yellow)),
                Span::raw(": bulk delete visible"),
            ]),
            Line::from(vec![
                Span::styled("  f", Style::default().fg(Color::Yellow)),
                Span::raw(": filter by kind  "),
                Span::styled("r", Style::default().fg(Color::Yellow)),
                Span::raw(": toggle never-recalled  "),
                Span::styled("a", Style::default().fg(Color::Yellow)),
                Span::raw(": toggle archived"),
            ]),
            Line::from(vec![
                Span::styled("  u", Style::default().fg(Color::Yellow)),
                Span::raw(": unarchive  "),
                Span::styled("q/Esc", Style::default().fg(Color::Yellow)),
                Span::raw(": back"),
            ]),
            Line::from(""),
            Line::from("  Memory settings are configurable in the config popup"),
            Line::from("  (press e on home screen, then Tab to the Memory tab)."),
            Line::from("  Stronger extraction models produce higher-quality memories."),
            Line::from(vec![
                Span::styled("  Space", Style::default().fg(Color::Yellow)),
                Span::raw(": toggle  "),
                Span::styled("Enter/e", Style::default().fg(Color::Yellow)),
                Span::raw(": edit value  "),
                Span::styled("s", Style::default().fg(Color::Yellow)),
                Span::raw(": save to disk"),
            ]),
            Line::from(""),
        ]
    });
    &LINES
}

pub fn prompt_relay_help_lines() -> &'static [Line<'static>] {
    static LINES: LazyLock<Vec<Line<'static>>> = LazyLock::new(|| {
        let h = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);
        let k = Style::default().fg(Color::Yellow);
        vec![
            Line::from(Span::styled("Prompt", h)),
            Line::from("  The task all agents will work on. Each agent receives"),
            Line::from("  the previous agent's output and refines it."),
            Line::from(""),
            Line::from(Span::styled("Session Name", h)),
            Line::from("  Optional label for the output directory."),
            Line::from("  If empty, a timestamp is used."),
            Line::from(""),
            Line::from(Span::styled("Iterations", h)),
            Line::from("  Number of full cycles through the agent ring."),
            Line::from("  Each iteration passes through all agents in order."),
            Line::from(""),
            Line::from(Span::styled("Runs / Concurrency", h)),
            Line::from("  Runs = independent repetitions of the full execution."),
            Line::from("  Concurrency = how many runs execute in parallel"),
            Line::from("  (0 = unlimited)."),
            Line::from(""),
            Line::from(Span::styled("Resume", h)),
            Line::from("  When on, looks for a previous run with the same session"),
            Line::from("  name (or the latest run) and continues from where it"),
            Line::from("  left off. Requires matching Keep Session setting."),
            Line::from(""),
            Line::from(Span::styled("Forward Prompt", h)),
            Line::from("  (Relay only) When on, the original prompt is prepended"),
            Line::from("  to every handoff after the first agent, preventing"),
            Line::from("  context loss in long chains. Without this, downstream"),
            Line::from("  agents only see the previous agent's output."),
            Line::from(""),
            Line::from(Span::styled("Keep Session", h)),
            Line::from("  Controls whether provider conversation history persists"),
            Line::from("  across iterations. When on (default), each agent"),
            Line::from("  remembers all prior exchanges. When off, history is"),
            Line::from("  cleared between iterations — only the handoff context"),
            Line::from("  (previous agent's output) is preserved."),
            Line::from(""),
            Line::from(vec![
                Span::styled("Tab/Shift+Tab", k),
                Span::raw(": cycle fields  "),
                Span::styled("Space", k),
                Span::raw(": toggle option  "),
                Span::styled("Enter/F5", k),
                Span::raw(": run  "),
                Span::styled("Ctrl+E", k),
                Span::raw(": analyze setup"),
            ]),
        ]
    });
    &LINES
}

pub fn prompt_swarm_help_lines() -> &'static [Line<'static>] {
    static LINES: LazyLock<Vec<Line<'static>>> = LazyLock::new(|| {
        let h = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);
        let k = Style::default().fg(Color::Yellow);
        vec![
            Line::from(Span::styled("Prompt", h)),
            Line::from("  The task all agents will work on. Every agent runs in"),
            Line::from("  parallel and sees all other agents' outputs after each"),
            Line::from("  round."),
            Line::from(""),
            Line::from(Span::styled("Session Name", h)),
            Line::from("  Optional label for the output directory."),
            Line::from("  If empty, a timestamp is used."),
            Line::from(""),
            Line::from(Span::styled("Iterations", h)),
            Line::from("  Number of rounds. After each round, all agents receive"),
            Line::from("  all peers' outputs and produce an updated response."),
            Line::from(""),
            Line::from(Span::styled("Runs / Concurrency", h)),
            Line::from("  Runs = independent repetitions of the full execution."),
            Line::from("  Concurrency = how many runs execute in parallel"),
            Line::from("  (0 = unlimited)."),
            Line::from(""),
            Line::from(Span::styled("Resume", h)),
            Line::from("  When on, looks for a previous run with the same session"),
            Line::from("  name (or the latest run) and continues from where it"),
            Line::from("  left off. Requires matching Keep Session setting."),
            Line::from(""),
            Line::from(Span::styled("Keep Session", h)),
            Line::from("  Controls whether provider conversation history persists"),
            Line::from("  across iterations. When on, agents accumulate context"),
            Line::from("  round over round. When off, each round starts fresh —"),
            Line::from("  only the cross-agent outputs from the previous round"),
            Line::from("  are injected."),
            Line::from(""),
            Line::from(vec![
                Span::styled("Tab/Shift+Tab", k),
                Span::raw(": cycle fields  "),
                Span::styled("Space", k),
                Span::raw(": toggle option  "),
                Span::styled("Enter/F5", k),
                Span::raw(": run  "),
                Span::styled("Ctrl+E", k),
                Span::raw(": analyze setup"),
            ]),
        ]
    });
    &LINES
}

pub fn order_help_lines() -> &'static [Line<'static>] {
    static LINES: LazyLock<Vec<Line<'static>>> = LazyLock::new(|| {
        let h = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);
        let k = Style::default().fg(Color::Yellow);
        vec![
            Line::from(Span::styled("Relay Order", h)),
            Line::from(""),
            Line::from("  Arrange the order in which agents execute in Relay mode."),
            Line::from(""),
            Line::from(vec![
                Span::styled("  j/k", k),
                Span::raw(": Move cursor up/down"),
            ]),
            Line::from(vec![
                Span::styled("  Space", k),
                Span::raw(": Grab the agent at cursor position. Move it"),
            ]),
            Line::from("    with j/k, then Space again to release."),
            Line::from(vec![
                Span::styled("  Enter", k),
                Span::raw(": Confirm order and proceed. The agent under"),
            ]),
            Line::from("    the cursor becomes the starting agent."),
            Line::from(""),
            Line::from("  The order determines the sequence of handoffs:"),
            Line::from("  Agent 1's output goes to Agent 2, Agent 2's to"),
            Line::from("  Agent 3, and so on in a ring."),
            Line::from(""),
            Line::from(vec![
                Span::styled("  Ctrl+E", k),
                Span::raw(": Analyze setup — sends config to diagnostic_provider"),
            ]),
            Line::from(""),
        ]
    });
    &LINES
}

pub fn pipeline_help_lines(tab: usize) -> &'static [Line<'static>] {
    static SECTIONS: LazyLock<[Vec<Line<'static>>; PIPELINE_TAB_COUNT]> = LazyLock::new(|| {
        let h = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);
        let k = Style::default().fg(Color::Yellow);
        [
            // Tab 0: Overview
            vec![
                Line::from(Span::styled("Overview", h)),
                Line::from(""),
                Line::from("  Pipeline mode lets you build a custom DAG (directed"),
                Line::from("  acyclic graph) of agent blocks with explicit connections."),
                Line::from(""),
                Line::from("  Blocks execute as soon as their dependencies are"),
                Line::from("  satisfied, maximizing parallelism."),
                Line::from(""),
                Line::from("  Root blocks (no incoming connections) receive the"),
                Line::from("  initial prompt. On subsequent iterations, they receive"),
                Line::from("  outputs from terminal blocks (no outgoing connections)."),
                Line::from(""),
                Line::from("  Each block has one or more agents, optional profiles,"),
                Line::from("  a prompt, and optional session ID for grouping history."),
                Line::from("  Multiple agents multiply with replicas (e.g. 2 agents"),
                Line::from("  \u{00d7} 3 replicas = 6 runtime tasks)."),
                Line::from(""),
            ],
            // Tab 1: Canvas Navigation
            vec![
                Line::from(Span::styled("Canvas Navigation", h)),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Arrow keys / hjkl", k),
                    Span::raw(": Move block cursor between blocks"),
                ]),
                Line::from(vec![
                    Span::styled("  Shift+Arrow", k),
                    Span::raw(": Move selected block to a new grid position"),
                ]),
                Line::from(vec![
                    Span::styled("  Ctrl+Arrow", k),
                    Span::raw(": Scroll the canvas viewport"),
                ]),
                Line::from(""),
            ],
            // Tab 2: Block Operations
            vec![
                Line::from(Span::styled("Block Operations", h)),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  a", k),
                    Span::raw(": Add a new block at the next available grid position"),
                ]),
                Line::from(vec![
                    Span::styled("  d", k),
                    Span::raw(": Delete the selected block and all its connections"),
                ]),
                Line::from(vec![
                    Span::styled("  e / Enter", k),
                    Span::raw(": Edit the selected block (name, agents, profiles,"),
                ]),
                Line::from("    prompt, session ID, replicas). Space toggles selection."),
                Line::from(""),
                Line::from("  Profiles are reusable .md instruction files in"),
                Line::from("  ~/.config/houseofagents/profiles/ — assign per block"),
                Line::from("  via the edit dialog (Tab to Profiles, Space to toggle)."),
                Line::from("  Missing profiles show as yellow [!] in the dialog and"),
                Line::from("  [missing] in setup analysis; they are skipped at runtime."),
                Line::from(vec![
                    Span::styled("  F5", k),
                    Span::raw(": Start pipeline execution"),
                ]),
                Line::from(""),
            ],
            // Tab 3: Wiring
            vec![
                Line::from(Span::styled("Wiring", h)),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  c", k),
                    Span::raw(": Enter connect mode — select source block,"),
                ]),
                Line::from("    navigate to target, press Enter to create a connection"),
                Line::from(vec![
                    Span::styled("  x", k),
                    Span::raw(": Enter remove-connection mode — cycle through"),
                ]),
                Line::from("    connections on the selected block and press Enter"),
                Line::from("    to remove"),
                Line::from(""),
                Line::from("  Connections must form a DAG (no cycles allowed)."),
                Line::from("  The system validates this automatically."),
                Line::from(""),
                Line::from("  Wires are drawn as orthogonal lines with directional"),
                Line::from("  arrows showing data flow."),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  o", k),
                    Span::raw(": Create loop-back connection"),
                ]),
                Line::from("    Press o on the downstream feedback block, then"),
                Line::from("    navigate to the upstream restart target and Enter."),
                Line::from("    All blocks between target and source re-run each pass."),
                Line::from("    Press o on a loop endpoint to edit count and prompt."),
                Line::from(""),
                Line::from("  Loop wires are drawn as double-line (\u{2550}\u{2551}) in yellow"),
                Line::from("  with \u{00d7}N count label. Delete with x (same as regular)."),
                Line::from(""),
                Line::from("  Count = additional passes beyond the initial run."),
                Line::from(""),
            ],
            // Tab 4: Sessions
            vec![
                Line::from(Span::styled("Sessions", h)),
                Line::from(""),
                Line::from("  Session ID groups blocks that share conversation"),
                Line::from("  history. Blocks with the same agent and session ID"),
                Line::from("  share a single conversation thread."),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  s", k),
                    Span::raw(": Open session config popup."),
                ]),
                Line::from(""),
                Line::from("  Two toggle columns (h/l to switch, Space to toggle):"),
                Line::from(""),
                Line::from("  Iter — Keep Across Iterations (default: on)."),
                Line::from("    When off, history is cleared before each iteration."),
                Line::from(""),
                Line::from("  Loop — Keep Across Loop Passes (default: on)."),
                Line::from("    When off, history is cleared between loop passes."),
                Line::from(""),
            ],
            // Tab 5: Prompt & Settings
            vec![
                Line::from(Span::styled("Prompt & Settings", h)),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Initial Prompt", Style::default().fg(Color::Cyan)),
                    Span::raw(": The text sent to root blocks"),
                ]),
                Line::from("  (blocks with no incoming connections)."),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Session Name", Style::default().fg(Color::Cyan)),
                    Span::raw(": Optional label for the output directory."),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Iterations", Style::default().fg(Color::Cyan)),
                    Span::raw(": Number of times the full DAG executes."),
                ]),
                Line::from("  After each iteration, terminal block outputs feed"),
                Line::from("  back into root blocks."),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Runs / Concurrency", Style::default().fg(Color::Cyan)),
                    Span::raw(": Independent repetitions"),
                ]),
                Line::from("  and parallelism (same as other modes)."),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Ctrl+S", k),
                    Span::raw(": Save pipeline definition to a TOML file."),
                ]),
                Line::from(vec![
                    Span::styled("  Ctrl+L", k),
                    Span::raw(": Load a previously saved pipeline definition."),
                ]),
                Line::from("    Type to filter by name. Tab toggles between search"),
                Line::from("    box and file list. j/k or arrows navigate the list."),
                Line::from("    Typing in the list switches back to search (j/k are"),
                Line::from("    navigation-only; Tab to search first for j/k names)."),
                Line::from("    In search: Esc clears query, then closes dialog."),
                Line::from(vec![
                    Span::styled("  Ctrl+E", k),
                    Span::raw(": Analyze setup — sends config to diagnostic_provider."),
                ]),
                Line::from(""),
            ],
            // Tab 6: Finalization
            vec![
                Line::from(Span::styled("Finalization", h)),
                Line::from(""),
                Line::from("  Finalization is a second pipeline phase that runs after"),
                Line::from("  the execution DAG completes. Finalization blocks receive"),
                Line::from("  execution outputs via data feeds."),
                Line::from(""),
                Line::from(Span::styled("  Keys", h)),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  A (Shift+A)", k),
                    Span::raw(": Add a finalization block. These appear below"),
                ]),
                Line::from("    the separator line and have magenta borders."),
                Line::from(vec![
                    Span::styled("  f", k),
                    Span::raw(": Create or edit data feed. On an execution block:"),
                ]),
                Line::from("    enter feed-connect mode — navigate to a finalization"),
                Line::from("    block and press Enter to create the feed."),
                Line::from("    On a finalization block: open feed list for selecting"),
                Line::from("    and editing incoming feeds (opens directly if only one)."),
                Line::from(vec![
                    Span::styled("  F", k),
                    Span::raw(": Remove a data feed. On a finalization block with"),
                ]),
                Line::from("    multiple feeds: opens feed list to pick which one."),
                Line::from(""),
                Line::from(Span::styled("  Data Feed Settings", h)),
                Line::from(""),
                Line::from("  Collection: controls which iteration outputs are fed."),
                Line::from("    last_iteration — only the final iteration output"),
                Line::from("    all_iterations  — every iteration output"),
                Line::from(""),
                Line::from("  Granularity: controls run scope for batch execution."),
                Line::from("    per_run  — finalization runs once per successful run"),
                Line::from("    all_runs — finalization runs once over all run outputs"),
                Line::from(""),
                Line::from(Span::styled("  Finalization Connections", h)),
                Line::from(""),
                Line::from("  Use c to connect finalization blocks to each other"),
                Line::from("  (same as regular DAG wiring, but within the"),
                Line::from("  finalization phase). Cross-phase connections are not"),
                Line::from("  allowed — use data feeds instead."),
                Line::from(""),
                Line::from("  Loops (o) are only available for execution blocks."),
                Line::from(""),
            ],
        ]
    });
    &SECTIONS[tab]
}

// ---------------------------------------------------------------------------
// Shared overlay renderer
// ---------------------------------------------------------------------------

fn draw_scrollable_popup(
    f: &mut Frame,
    title: &str,
    border_color: Color,
    lines: Vec<Line<'_>>,
    scroll: u16,
    hints: Line<'_>,
) {
    let area = centered_rect(70, 70, f.area());
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color));

    f.render_widget(ratatui::widgets::Clear, area);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(inner);

    let content_height = sections[0].height as usize;
    let content_width = sections[0].width as usize;

    let visual_lines: usize = if content_width > 0 {
        lines
            .iter()
            .map(|line| {
                let w = line.width();
                if w <= content_width {
                    1
                } else {
                    w.div_ceil(content_width)
                }
            })
            .sum()
    } else {
        lines.len()
    };
    let max_scroll = visual_lines.saturating_sub(content_height);
    let scroll = (scroll as usize).min(max_scroll) as u16;

    let text = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    f.render_widget(text, sections[0]);

    f.render_widget(Paragraph::new(hints), sections[1]);
}

pub fn draw_help_overlay(f: &mut Frame, state: &HelpPopupState, lines: &[Line<'_>], title: &str) {
    let mut hints = vec![
        Span::styled("j/k, arrows, PgUp/PgDn", Style::default().fg(Color::Yellow)),
        Span::raw(": scroll  "),
    ];
    if state.tab_count > 1 {
        hints.push(Span::styled(
            "Tab/Shift+Tab",
            Style::default().fg(Color::Yellow),
        ));
        hints.push(Span::raw(": section  "));
    }
    hints.push(Span::styled("Esc/q/?", Style::default().fg(Color::Yellow)));
    hints.push(Span::raw(": close"));

    draw_scrollable_popup(
        f,
        title,
        Color::Cyan,
        lines.to_vec(),
        state.scroll,
        Line::from(hints),
    );
}

// ---------------------------------------------------------------------------
// Setup analysis popup
// ---------------------------------------------------------------------------

pub fn draw_setup_analysis_popup(f: &mut Frame, state: &SetupAnalysisState) {
    if state.loading {
        let loading_line = Line::from(Span::styled(
            "Analyzing setup...",
            Style::default().fg(Color::Yellow),
        ));
        let hints = Line::from(vec![
            Span::styled("Esc/q", Style::default().fg(Color::Yellow)),
            Span::raw(": close"),
        ]);
        draw_scrollable_popup(
            f,
            " Setup Analysis ",
            Color::Yellow,
            vec![loading_line],
            0,
            hints,
        );
        return;
    }

    let rendered = render_markdown(&state.content);
    let hints = Line::from(vec![
        Span::styled("j/k, arrows, PgUp/PgDn", Style::default().fg(Color::Yellow)),
        Span::raw(": scroll  "),
        Span::styled("Esc/q", Style::default().fg(Color::Yellow)),
        Span::raw(": close"),
    ]);
    draw_scrollable_popup(
        f,
        " Setup Analysis ",
        Color::Yellow,
        rendered.lines,
        state.scroll,
        hints,
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn lines_to_text(lines: &[Line<'_>]) -> String {
        lines
            .iter()
            .flat_map(|l| l.spans.iter().map(|s| s.content.to_string()))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn home_help_contains_mode_headers() {
        let text = lines_to_text(home_help_lines());
        assert!(text.contains("Relay"));
        assert!(text.contains("Swarm"));
        assert!(text.contains("Pipeline"));
    }

    #[test]
    fn pipeline_help_has_six_sections() {
        for tab in 0..PIPELINE_TAB_COUNT {
            assert!(
                !pipeline_help_lines(tab).is_empty(),
                "Pipeline help tab {tab} is empty"
            );
        }
    }

    #[test]
    fn relay_prompt_help_mentions_forward_prompt() {
        let text = lines_to_text(prompt_relay_help_lines());
        assert!(text.contains("Forward Prompt"));
    }

    #[test]
    fn swarm_prompt_help_omits_forward_prompt() {
        let text = lines_to_text(prompt_swarm_help_lines());
        assert!(!text.contains("Forward Prompt"));
    }
}
