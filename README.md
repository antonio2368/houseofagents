<div align="center">

# House of Agents

**Multi-agent prompt runner with a terminal UI**

Run Claude, OpenAI, and Gemini in collaborative execution modes and save all artifacts to disk.

[![Rust](https://img.shields.io/badge/Rust-1.88%2B-orange?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</div>

---

## Execution Modes

| Mode | Description |
|------|-------------|
| **Relay** | Sequential handoff — each agent builds on the previous agent's output |
| **Swarm** | Parallel rounds with cross-agent context injected between rounds |
| **Pipeline** | Custom DAG builder — wire arbitrary blocks of agents into a dependency graph |

## Supported Providers

| Provider | API | CLI |
|----------|:---:|:---:|
| **Anthropic** (Claude) | `api_key` | `claude` binary |
| **OpenAI** | `api_key` | `codex` binary |
| **Gemini** | `api_key` | `gemini` binary |

Each agent can run in API mode or CLI mode (`use_cli = true`). Mix and match freely.

## Features

- **Terminal UI** — select agents, mode, prompt, iterations, run count, and concurrency from an interactive TUI
- **Named agents** — define multiple agents per provider with independent configs
- **Pipeline builder** — visual DAG editor for wiring arbitrary agent blocks with dependency-driven execution, independent per-connection routing, and loop connections for iterative refinement between blocks
- **Multiple runs** — launch N independent copies of the same setup in parallel with bounded concurrency
- **Resume runs** — pick up where you left off in relay or swarm sessions
- **Forward Prompt** — relay mode option to include the original prompt in every handoff
- **Keep Session** — toggle per-provider conversation history persistence across iterations (on by default; turn off to clear provider memory between iterations while preserving inter-agent handoff context). Pipeline mode has its own per-session configuration popup in the Builder screen (`s`)
- **Consolidation** — merge multi-agent output within a run or across multiple runs (any configured agent can consolidate)
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
# For values 4+, providers keep the first exchange plus the newest messages.
# For values 1-3, providers keep only the most recent messages up to the cap.
max_history_messages = 50
http_timeout_seconds = 120
model_fetch_timeout_seconds = 30
cli_timeout_seconds = 600
max_history_bytes = 102400
# pipeline_block_concurrency = 0

# Optional: set to an agent name to enable diagnostics
# diagnostic_provider = "Claude"

# Named agents — you can have multiple agents per provider
[[agents]]
name = "OpenAI"
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
| `max_history_messages` | Max chat history kept per provider session. Values `4+` preserve the first exchange plus newer messages; values `1-3` keep only the most recent messages. |
| `http_timeout_seconds` | Timeout for API calls (`use_cli = false`) |
| `model_fetch_timeout_seconds` | Timeout for model list fetch in config editor |
| `cli_timeout_seconds` | Timeout for CLI calls (`use_cli = true`) |
| `max_history_bytes` | Max total bytes of conversation history per provider session (default 100 KB). Applied after message-count pruning. |
| `pipeline_block_concurrency` | Max concurrent pipeline blocks. `0` = unlimited (default). |
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
| `extra_cli_args` | Shell-style extra CLI args parsed at runtime, for example `--sandbox workspace-write --profile "fast mode"` |
| `reasoning_effort` | OpenAI effort setting — `low` / `medium` / `high` / `xhigh` |
| `thinking_effort` | Anthropic & Gemini effort setting — `low` / `medium` / `high`; Anthropic CLI also supports `max` for `claude-opus-4-6` |

Anthropic `thinking_effort = "max"` is rejected in API mode. In CLI mode, House of Agents passes it through and lets the `claude` CLI report any model-specific incompatibility.

## Keyboard Shortcuts

### Home Screen

| Key | Action |
|-----|--------|
| `j` / `k` / `Up` / `Down` | Navigate agents and modes |
| `Space` | Toggle agent / mode selection |
| `Tab` | Switch panels |
| `e` | Open config editor |
| `?` | Open help popup |
| `Enter` | Continue to prompt |
| `q` | Quit |

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
| `Tab` / `Shift+Tab` | Cycle input fields forward / backward |
| `Space` | Toggle focused option (Resume / Forward Prompt / Keep Session) |
| `Enter` / `F5` | Start run |
| `?` | Open help (unavailable while editing text fields: prompt, session name) |
| `Esc` | Back |

Fields vary by mode for options, but every prompt flow includes Prompt, Session Name, Iterations, Runs, and Concurrency.

### Pipeline Builder Screen

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Cycle focus: Initial Prompt → Session Name → Iterations → Runs → Concurrency → Builder |
| `a` | Add a new block |
| `d` | Delete selected block |
| `e` | Edit selected block (name, agent, prompt, session ID, replicas) |
| `c` | Enter connect mode — select a second block to create a connection |
| `x` | Enter remove-connection mode — pick a connection to delete |
| `o` | Create loop connection — select target block, set count and prompt; press on existing loop to edit |
| `s` | Open session configuration popup — toggle per-session history persistence |
| `Arrow keys` / `h j k l` | Navigate/select blocks spatially without moving |
| `Shift+Arrow keys` / `Shift+H J K L` | Move selected block (swap with occupied target cell, otherwise move) |
| `Ctrl+Arrow keys` | Scroll the builder canvas |
| `↑`/`+` `↓`/`-` | Increment / decrement iterations, runs, or concurrency on the focused numeric field |
| `Ctrl+S` | Save pipeline (always prompts for filename, prefills current name) |
| `Ctrl+L` | Load pipeline from file |
| `F5` | Validate and run the pipeline |
| `?` | Open help popup (6 tabbed sections; Tab/Shift+Tab to cycle). Only when focus is not on a text field (initial prompt / session name). |
| `Esc` | Cancel current action / back to home |

Inside the **edit popup**: `Tab` cycles between Name, Agent (use `Left`/`Right`), Prompt (text area), Session ID, and Replicas (1-32) fields. `Esc` closes the popup. Setting Replicas > 1 spawns that many independent copies of the block at execution time, each with its own provider session.

### Order Screen (relay with 2+ agents)

| Key | Action |
|-----|--------|
| `j` / `k` | Move cursor |
| `Space` | Grab / reorder agent |
| `Enter` | Confirm and start |
| `?` | Open help |
| `Esc` | Back to prompt |

### Running Screen

| Key | Action |
|-----|--------|
| `j` / `k` | Select run row in batch mode |
| `Esc` | Cancel in-flight run or batch |
| `Enter` | Open results after completion |
| `q` | Quit after run completes |
| `l` | Toggle activity log detail panel |
| `p` | Toggle stream preview on/off |
| `Tab` / `Shift+Tab` | Cycle preview target between agents/blocks |

### Results Screen

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate files |
| `Enter` / `l` | Expand/collapse run groups in batch results |
| `Esc` | Return to home |
| `q` | Quit |

> `Ctrl+C` exits from any screen (cancels an active run first).

## Run Artifacts

Each run creates a directory inside `output_dir`:

```
output_dir/
  latest -> 2026-03-05/my_session  # symlink to most recent run (unix)
  runs.toml                        # append-only run index
  2026-03-05/
    my_session/                    # user-defined session name
      prompt.md                    # Original prompt
      session.toml                 # Run metadata
      Claude_iter1.md              # Agent output per iteration
      OpenAI_iter2.md
      consolidated_Claude.md       # Optional: merged output
      errors.md                    # Optional: diagnostics report
      _errors.log                  # Application-level error log
    swift-falcon/                  # auto-generated name (no session provided)
      ...
```

Pipeline runs produce per-block output files named using the block's name (sanitized) and a unique block id suffix. When a block has `replicas > 1`, each replica gets an `_rN` suffix:

```
output_dir/
  2026-03-05/
    my_session/
      session.toml                         # mode = "pipeline", block/connection counts, total_runtime_tasks
      prompt.md                            # Pipeline-level prompt (shared across blocks)
      pipeline.toml                        # Pipeline definition snapshot (may include [[session_configs]])
      Analyzer_b1_Claude_iter1.md          # Block "Analyzer" (id 1), agent Claude, iteration 1
      Reviewer_b2_Gemini_iter1.md          # Block "Reviewer" (id 2), replicas=1 — no _r suffix
      Worker_b3_Claude_r1_iter1.md         # Block "Worker" (id 3), replicas=3, replica 1
      Worker_b3_Claude_r2_iter1.md         # replica 2
      Worker_b3_Claude_r3_iter1.md         # replica 3
      Analyzer_b1_Claude_iter1_loop1.md   # loop pass 1 (from loop connection)
      Analyzer_b1_Claude_iter1_loop2.md   # loop pass 2
      _errors.log
```

Loop connections create iterative feedback loops between two blocks. In saved pipeline TOML files they appear as:

```toml
[[loop_connections]]
from = 2
to = 1
count = 3
prompt = "Refine based on feedback"
```

`count` is the number of returns from `to` back to `from`. Each block in the loop runs `count + 1` times total. Loop wires are drawn as double-line in yellow on the canvas.

Runs are grouped by date: `YYYY-MM-DD/<session_name>`. When no session name is provided, a random two-word name (`adjective-noun`) is generated. Duplicate user-defined session names within the same date are rejected.

When `runs > 1`, House of Agents creates a batch root and one subdirectory per independent run:

```
output_dir/
  2026-03-08/
    my_session/
      batch.toml
      cross_run_consolidation.md   # Optional: synthesis across successful runs
      run_1/
        prompt.md
        session.toml
        Claude_iter1.md
        consolidation.md           # Optional: per-run synthesis
      run_2/
        ...
```

Legacy directories (`YYYYMMDD_HHMMSS_NNN[_session]` and `YYYY-MM-DD/HH-MM-SS[_session]`) from older versions are preserved and remain searchable.

## Resume, Consolidation & Diagnostics

- **Resume** (toggle with `Space` on Prompt screen) — available for relay and swarm modes
  - With a session name: resolves the latest run with that name, then validates it against the current run configuration before resuming
  - Without: resumes the latest compatible run with an exact mode match
  - Relay resume requires the exact same agent order
  - Swarm resume requires the exact same agent set
  - Resume also requires the same Keep Session setting — a run started with `keep_session = true` cannot be resumed with it off, and vice versa
  - Batch roots are excluded from resume lookup; resume is currently single-run only
  - Note: resuming a `keep_session = true` run across app restarts does not restore provider conversation history — providers are recreated fresh, though inter-agent handoff context is preserved
- **Forward Prompt** (toggle with `Space` on Prompt screen) — relay mode only; when enabled, downstream agents receive the original prompt alongside the previous agent's output, preventing context loss in the handoff chain
- **Keep Session** (toggle with `Space` on Prompt screen) — on by default; controls whether providers retain their conversation history across iterations. When turned off, each provider's history is cleared before every iteration after the first, so agents treat each round as a fresh conversation. Inter-agent handoff context (relay's previous output, swarm's round outputs) is always preserved regardless of this setting. Pipeline mode has its own per-session session configuration popup accessible via `s` in the Builder screen — each effective session (shared or isolated) can independently toggle whether provider history persists across iterations. Non-default settings are stored in `pipeline.toml` as `[[session_configs]]` entries
- **Consolidation**
  - Single-run: offered after non-cancelled swarm/pipeline runs with 2+ final outputs
  - Batch: first offers per-run consolidation, then optional cross-run consolidation across successful runs
- **Diagnostics** — when `diagnostic_provider` is set to an agent name, a final analysis pass writes `errors.md`
