# House of Agents

Multi-agent prompt runner with a terminal UI. Runs Claude, OpenAI, and Gemini in collaborative execution modes (Relay, Swarm, Pipeline) and saves all artifacts to disk.

## Tech Stack

- **Language:** Rust (MSRV 1.88)
- **UI:** ratatui (terminal TUI)
- **Config:** TOML (`~/.config/houseofagents/config.toml`)
- **Build:** Cargo

## Project Structure

```
src/
  main.rs              — Entry point, CLI arg parsing, config init
  app.rs               — App state machine and main event loop
  config.rs            — Config types and TOML (de)serialization
  error.rs             — Error types
  event.rs             — Terminal event handling (key, mouse, resize)
  output.rs            — Run artifact writing (markdown files, session.toml)

  tui/                 — TUI rendering, event dispatch, and run orchestration
    mod.rs             — Top-level imports, draw loop, and run() entry point
    input.rs           — Keyboard/mouse event handlers for all screens
    execution.rs       — Execution launch, progress polling, and completion logic
    consolidation.rs   — Post-run consolidation (multi-agent summary) workflow
    diagnostics.rs     — Provider diagnostics and setup validation UI
    results.rs         — Result loading and batch-result tree navigation
    resume.rs          — Iteration resume and checkpoint discovery
    setup_analysis.rs  — Pipeline setup analysis and validation rendering
    text_edit.rs       — In-place text editing helpers (cursor, keys)
    tests.rs           — Unit tests for TUI subsystems

  provider/
    mod.rs             — Provider trait and dispatch
    anthropic.rs       — Anthropic (Claude) API integration
    openai.rs          — OpenAI API integration
    gemini.rs          — Google (Gemini) API integration
    cli.rs             — CLI-mode provider wrapper
    sse.rs             — SSE (Server-Sent Events) stream parser

  execution/
    mod.rs             — Execution engine trait and shared logic
    relay.rs           — Relay mode (sequential handoff)
    swarm.rs           — Swarm mode (parallel rounds)
    pipeline.rs        — Pipeline mode (DAG builder + dependency-driven executor)
    multi.rs           — Multi-run (batch) execution coordinator
    test_utils.rs      — Shared test utilities

  screen/
    mod.rs             — Screen trait and screen registry
    home.rs            — Home screen (agent/mode selection)
    prompt.rs          — Prompt input screen
    order.rs           — Agent ordering screen (relay mode)
    pipeline.rs        — Pipeline builder screen (DAG canvas)
    running.rs         — Execution progress screen
    results.rs         — Results display screen
    help.rs            — Help overlay and keybinding reference
```

## Development

```bash
# Build
cargo build

# Run
cargo run

# Test
cargo test

# Clippy
cargo clippy -- -D warnings
```

## Key Conventions

- **Always run `cargo fmt` and `cargo clippy --all-targets --all-features -- -D warnings` before committing** to avoid CI formatting and lint failures
- Provider implementations live in `src/provider/` and implement a common trait
- Execution modes live in `src/execution/` and implement a common trait
- Screens live in `src/screen/` and implement a common trait
- All run output goes to timestamped directories under `output_dir`
- Config supports both API-key and CLI-binary modes per provider
- Always update `README.md` when user-facing parts change (CLI flags, config options, execution modes, screens, keybindings, setup instructions, etc.)
- The tool should always stay lightweight when it comes to memory usage and performant
- Always update help pages with new changes
- Always keep in mind how changes affect memory usage and performance. Keeping the tool lightweight is mandatory.
