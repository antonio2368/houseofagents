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

## Supported Providers

| Provider | API | CLI |
|----------|:---:|:---:|
| **Anthropic** (Claude) | `api_key` | `claude` binary |
| **OpenAI** (Codex) | `api_key` | `codex` binary |
| **Google** (Gemini) | `api_key` | `gemini` binary |

Each provider can run in API mode or CLI mode (`use_cli = true`). Mix and match freely.

## Features

- **Terminal UI** — select agents, mode, prompt, and iteration count from an interactive TUI
- **Resume runs** — pick up where you left off in relay or swarm sessions
- **Forward Prompt** — relay mode option to include the original prompt in every handoff
- **Consolidation** — merge multi-agent output into a single final markdown file
- **Diagnostics** — optional post-run analysis pass that writes `errors.md`
- **Config editor** — edit provider settings, timeouts, and models live with a popup (`e`)
- **Model picker** — browse available models from the API directly inside the config editor (`l`)

## Requirements

- **Rust 1.88+** and Cargo
- A terminal with TUI support
- At least one provider configured via API key **or** locally-installed CLI with auth set up

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

# 2. Edit config — for at least one provider:
#    - API mode: set `api_key` and `use_cli = false`
#    - CLI mode: set `use_cli = true` and ensure the CLI is installed/authenticated
#    Default location: ~/.config/houseofagents/config.toml
#    Optional: uncomment diagnostic_provider to enable diagnostics

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

# Optional: set one diagnostics provider ("anthropic", "openai", "gemini")
# diagnostic_provider = "openai"

[providers.openai]
api_key = ""
model = "gpt-5.3-codex"
reasoning_effort = "high"
use_cli = true
extra_cli_args = ""

[providers.anthropic]
api_key = ""
model = "claude-opus-4-6"
thinking_effort = "high"
use_cli = true
extra_cli_args = ""

[providers.gemini]
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "medium"
use_cli = true
extra_cli_args = ""
```

<details>
<summary>Diagnostics providers (separate config block)</summary>

```toml
[diagnostics.anthropic]
api_key = ""
model = "claude-opus-4-6"
thinking_effort = "low"
use_cli = true
extra_cli_args = ""

[diagnostics.openai]
api_key = ""
model = "gpt-5.3-codex"
reasoning_effort = "low"
use_cli = true
extra_cli_args = ""

[diagnostics.gemini]
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "low"
use_cli = true
extra_cli_args = ""
```

</details>

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
| `diagnostic_provider` | Optional provider key for the automatic diagnostics pass (disabled when unset) |

**Provider settings** (`[providers.*]` and `[diagnostics.*]`):

| Field | Description |
|-------|-------------|
| `api_key` | API key for the provider (required when `use_cli = false`, leave empty for CLI mode) |
| `model` | Model identifier to use |
| `use_cli` | Use local CLI binary instead of HTTP API |
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

### Prompt Screen

| Key | Action |
|-----|--------|
| `Tab` | Cycle input fields |
| `Space` | Toggle focused option (Resume / Forward Prompt) |
| `Enter` / `F5` | Start run |
| `Esc` | Back |

Fields vary by mode: Solo shows only Prompt and Session Name; Swarm adds Iterations and Resume; Relay adds Forward Prompt alongside Resume.

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
    anthropic_iter1.md           # Provider output per iteration
    openai_iter2.md
    consolidated_anthropic.md    # Optional: merged output
    errors.md                    # Optional: diagnostics report
    _errors.log                  # Application-level error log
```

Directory names follow the pattern `YYYYMMDD_HHMMSS_<rand>` (with optional `_<session_name>` suffix).

## Resume, Consolidation & Diagnostics

- **Resume** (toggle with `Space` on Prompt screen) — available for relay and swarm modes
  - With a session name: resumes the latest run matching that name
  - Without: resumes the latest compatible run (matching mode + agents)
- **Forward Prompt** (toggle with `Space` on Prompt screen) — relay mode only; when enabled, downstream agents receive the original prompt alongside the previous agent's output, preventing context loss in the handoff chain
- **Consolidation** — offered after non-cancelled swarm/solo runs; produces a single merged markdown
- **Diagnostics** — when `diagnostic_provider` is set, a final analysis pass writes `errors.md`
