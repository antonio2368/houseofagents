<div align="center">

# House of Agents

**Multi-agent prompt runner with a terminal UI**

Run Claude, Codex, and Gemini in collaborative execution modes and save all artifacts to disk.

[![Rust](https://img.shields.io/badge/Rust-1.88%2B-orange?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</div>

---

## Execution Modes

| Mode | Description |
|------|-------------|
| **Relay** | Sequential handoff — each agent builds on the previous agent's output |
| **Swarm** | Parallel rounds with cross-agent context injected between rounds |
| **Solo** | Independent one-shot runs per selected agent (forced to 1 iteration) |
| **Pipeline** | Custom DAG builder — wire arbitrary blocks of agents into a dependency graph |

## Supported Providers

| Provider | API | CLI |
|----------|:---:|:---:|
| **Anthropic** (Claude) | `api_key` | `claude` binary |
| **OpenAI** (Codex) | `api_key` | `codex` binary |
| **Google** (Gemini) | `api_key` | `gemini` binary |

Each agent can run in API mode or CLI mode (`use_cli = true`). Mix and match freely.

## Features

- **Terminal UI** — select agents, mode, prompt, and iteration count from an interactive TUI
- **Named agents** — define multiple agents per provider with independent configs
- **Pipeline builder** — visual DAG editor for wiring arbitrary agent blocks with dependency-driven execution and independent per-connection routing
- **Resume runs** — pick up where you left off in relay or swarm sessions
- **Forward Prompt** — relay mode option to include the original prompt in every handoff
- **Consolidation** — merge multi-agent output into a single final markdown file (any configured agent can consolidate)
- **Diagnostics** — optional post-run analysis pass that writes `errors.md`
- **Config editor** — add/remove/rename agents, edit settings, timeouts, and models live with a popup (`e`)
- **Model picker** — browse available models from the API directly inside the config editor (`l`)
- **CLI print mode** — Anthropic agents in CLI mode can toggle between print (`-p`) and agent mode

## Requirements

- **Rust 1.88+** and Cargo
- A terminal with TUI support
- At least one agent configured via API key **or** locally-installed CLI with auth set up

## Install

```bash
git clone https://github.com/antonio2368/houseofagents.git
cd houseofagents
./install.sh
```

The install script runs `cargo install --path .` and writes a starter config via `houseofagents --init-config`.

<details>
<summary>Custom config path / overwrite existing</summary>

```bash
# Custom config location
./install.sh /absolute/path/config.toml

# Overwrite existing config
./install.sh /absolute/path/config.toml --force
```

</details>

## Quick Start

```bash
# 1. Initialize config (skip if you used install.sh)
houseofagents --init-config

# 2. Edit config — for at least one agent:
#    - API mode: set `api_key` and `use_cli = false`
#    - CLI mode: set `use_cli = true` and ensure the CLI is installed/authenticated
#    Default location: ~/.config/houseofagents/config.toml
#    Optional: set diagnostic_provider to an agent name to enable diagnostics

# 3. Launch
houseofagents
```

In the TUI: select agents with `Space`, pick a mode, enter your prompt, and hit `Enter` or `F5` to run.

## CLI Options

```
houseofagents [OPTIONS]

Options:
  -c, --config <PATH>   Config file path [default: ~/.config/houseofagents/config.toml]
      --init-config      Write a starter config file and exit
      --force            Overwrite config when used with --init-config
  -h, --help             Print help
```

## Configuration

The starter config lives at `~/.config/houseofagents/config.toml`:

```toml
output_dir = "~/houseofagents-output"
default_max_tokens = 4096
max_history_messages = 50
http_timeout_seconds = 120
model_fetch_timeout_seconds = 30
cli_timeout_seconds = 600

# Optional: set to an agent name to enable diagnostics
# diagnostic_provider = "Claude"

# Named agents — you can have multiple agents per provider
[[agents]]
name = "Codex"
provider = "openai"
api_key = ""
model = "gpt-5.3-codex"
reasoning_effort = "high"
use_cli = true
extra_cli_args = ""

[[agents]]
name = "Claude"
provider = "anthropic"
api_key = ""
model = "claude-opus-4-6"
thinking_effort = "high"
use_cli = true
cli_print_mode = true
extra_cli_args = ""

[[agents]]
name = "Gemini"
provider = "gemini"
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "medium"
use_cli = true
extra_cli_args = ""
```

### Config Reference

**General settings** (top-level):

| Field | Description |
|-------|-------------|
| `output_dir` | Base directory for run output folders |
| `default_max_tokens` | Token budget sent to providers |
| `max_history_messages` | Max chat history kept per provider session |
| `http_timeout_seconds` | Timeout for API calls (`use_cli = false`) |
| `model_fetch_timeout_seconds` | Timeout for model list fetch in config editor |
| `cli_timeout_seconds` | Timeout for CLI calls (`use_cli = true`) |
| `diagnostic_provider` | Agent name for the automatic diagnostics pass (disabled when unset) |

**Agent settings** (`[[agents]]`):

| Field | Description |
|-------|-------------|
| `name` | Display name for the agent (must be unique) |
| `provider` | Provider type — `anthropic`, `openai`, or `gemini` |
| `api_key` | API key (required when `use_cli = false`, leave empty for CLI mode) |
| `model` | Model identifier to use |
| `use_cli` | Use local CLI binary instead of HTTP API |
| `cli_print_mode` | Anthropic only: use print mode (`-p`) instead of agent mode (default: `true`) |
| `extra_cli_args` | Raw string appended as one extra CLI argument |
| `reasoning_effort` | OpenAI effort setting — `low` / `medium` / `high` |
| `thinking_effort` | Anthropic & Gemini effort setting — `low` / `medium` / `high` |

## Keyboard Shortcuts

### Home Screen

| Key | Action |
|-----|--------|
| `Space` | Toggle agent / mode selection |
| `Tab` | Switch panels |
| `e` | Open config editor |
| `Enter` | Continue to prompt |

### Config Editor

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate agents or timeouts |
| `Tab` | Switch section (Agents / Timeouts) |
| `n` | Add new agent |
| `Del` / `Backspace` | Remove agent |
| `r` | Rename agent |
| `p` | Cycle provider (Anthropic / OpenAI / Gemini) |
| `c` | Toggle CLI / API mode |
| `b` | Toggle print / agent mode (Anthropic CLI) |
| `a` | Edit API key |
| `m` | Edit model |
| `l` | Open model picker |
| `t` | Cycle thinking / reasoning effort |
| `x` | Edit extra CLI args |
| `d` | Toggle diagnostic agent |
| `o` | Edit output directory |
| `e` | Edit selected timeout (Timeouts section) |
| `s` | Save config to disk |
| `Esc` | Close (keep changes for session) |

### Prompt Screen

| Key | Action |
|-----|--------|
| `Tab` | Cycle input fields |
| `Space` | Toggle focused option (Resume / Forward Prompt) |
| `Enter` / `F5` | Start run |
| `Esc` | Back |

Fields vary by mode: Solo shows only Prompt and Session Name; Swarm adds Iterations and Resume; Relay adds Forward Prompt alongside Resume.

### Pipeline Builder Screen

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Cycle focus: Initial Prompt → Session Name → Iterations → Builder |
| `a` | Add a new block |
| `d` | Delete selected block |
| `e` | Edit selected block (name, agent, prompt, session ID) |
| `c` | Enter connect mode — select a second block to create a connection |
| `x` | Enter remove-connection mode — pick a connection to delete |
| `Arrow keys` / `h j k l` | Navigate/select blocks spatially without moving |
| `Shift+Arrow keys` / `Shift+H J K L` | Move selected block (swap with occupied target cell, otherwise move) |
| `Ctrl+Arrow keys` | Scroll the builder canvas |
| `↑`/`+` `↓`/`-` | Increment / decrement iterations (when Iterations focused) |
| `Ctrl+S` | Save pipeline (always prompts for filename, prefills current name) |
| `Ctrl+L` | Load pipeline from file |
| `F5` | Validate and run the pipeline |
| `Esc` | Cancel current action / back to home |

Inside the **edit popup**: `Tab` cycles between Name, Agent (use `Left`/`Right`), Prompt (text area), and Session ID fields. `Esc` closes the popup.

### Order Screen (relay with 2+ agents)

| Key | Action |
|-----|--------|
| `j` / `k` | Move cursor |
| `Space` | Grab / reorder agent |
| `Enter` | Confirm and start |

### Running Screen

| Key | Action |
|-----|--------|
| `Esc` | Cancel in-flight run |
| `Enter` | Open results after completion |

### Results Screen

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate files |
| `Enter` | Return to home |
| `q` | Quit |

> `Ctrl+C` exits from any screen (cancels an active run first).

## Run Artifacts

Each run creates a directory inside `output_dir`:

```
output_dir/
  20260305_143022_a1b2/
    prompt.md                    # Original prompt
    session.toml                 # Run metadata
    Claude_iter1.md              # Agent output per iteration
    Codex_iter2.md
    consolidated_Claude.md       # Optional: merged output
    errors.md                    # Optional: diagnostics report
    _errors.log                  # Application-level error log
```

Pipeline runs produce per-block output files named using the block's name (sanitized) and a unique block id suffix:

```
output_dir/
  20260305_143022_234_my_session/
    session.toml                         # mode = "pipeline", block/connection counts
    prompt.md                            # Pipeline-level prompt (shared across blocks)
    pipeline.toml                        # Pipeline definition snapshot
    Analyzer_b1_Claude_iter1.md          # Block "Analyzer" (id 1), agent Claude, iteration 1
    Reviewer_b2_Gemini_iter1.md          # Block "Reviewer" (id 2), agent Gemini, iteration 1
    _errors.log
```

Directory names follow the pattern `YYYYMMDD_HHMMSS_<rand>` (with optional `_<session_name>` suffix).

## Resume, Consolidation & Diagnostics

- **Resume** (toggle with `Space` on Prompt screen) — available for relay and swarm modes
  - With a session name: resumes the latest run matching that name
  - Without: resumes the latest compatible run (matching mode + agents)
- **Forward Prompt** (toggle with `Space` on Prompt screen) — relay mode only; when enabled, downstream agents receive the original prompt alongside the previous agent's output, preventing context loss in the handoff chain
- **Consolidation** — offered after non-cancelled swarm/solo runs with 2+ agents; any configured agent can perform the consolidation (not limited to agents from the run)
- **Diagnostics** — when `diagnostic_provider` is set to an agent name, a final analysis pass writes `errors.md`
