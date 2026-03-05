# House of Agents

Multi-agent prompt runner with a terminal UI.

Run Claude, Codex, and Gemini in collaborative execution modes (`relay`, `swarm`, `solo`), save all artifacts to disk, and optionally generate a diagnostics report.

## Features

- Terminal UI for selecting agents, mode, prompt, and iteration count.
- Three execution modes:
  - `relay`: sequential handoff between agents.
  - `swarm`: parallel rounds with cross-agent context between rounds.
  - `solo`: independent one-shot runs per selected agent.
- Provider support for:
  - Anthropic (`claude` / API)
  - OpenAI Codex (`codex` / API)
  - Gemini (`gemini` / API)
- Per-provider API or CLI mode (`use_cli = true/false`).
- Optional resume for previous `relay`/`swarm` runs.
- Optional post-run consolidation into a single final markdown file.
- Optional diagnostics pass that writes `errors.md`.

## Requirements

- Rust and Cargo installed.
- A terminal that supports TUI apps.
- At least one configured provider using either:
  - API key in config, or
  - installed CLI binary (`claude`, `codex`, or `gemini`) with local auth already set up.

## Install

```bash
git clone git@github.com:antonio2368/houseofagents.git
cd houseofagents
./install.sh
```

Install script behavior:

- Runs `cargo install --path .`
- Writes starter config via `houseofagents --init-config`

Optional custom config path:

```bash
./install.sh /absolute/path/config.toml
```

Overwrite existing config template:

```bash
./install.sh /absolute/path/config.toml --force
```

## Quick Start

1. Initialize config (if you did not use `install.sh`):

```bash
houseofagents --init-config
```

2. Edit config file (default path: `~/.config/houseofagents/config.toml`).

3. Start the app:

```bash
houseofagents
```

4. In the TUI:

- Select one or more agents.
- Pick execution mode.
- Enter prompt (+ optional session name and iterations).
- Run with `Enter`/`F5`.

## CLI Options

```text
Usage: houseofagents [OPTIONS]

Options:
  -c, --config <CONFIG>  Path to config file (default: ~/.config/houseofagents/config.toml)
      --init-config      Write a starter config file and exit
      --force            Overwrite config when used with --init-config
  -h, --help             Print help
```

## Configuration

Starter template:

```toml
# House of Agents config
output_dir = "~/houseofagents-output"
default_max_tokens = 4096
max_history_messages = 50

# Optional: set one diagnostics provider ("anthropic", "openai", "gemini")
# diagnostic_provider = "openai"

[providers.anthropic]
api_key = ""
model = "claude-sonnet-4-5"
thinking_effort = "medium"
use_cli = false
extra_cli_args = ""

[providers.openai]
api_key = ""
model = "gpt-5"
reasoning_effort = "medium"
use_cli = false
extra_cli_args = ""

[providers.gemini]
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "medium"
use_cli = false
extra_cli_args = ""

# Diagnostics are configured separately from run providers.
[diagnostics.anthropic]
api_key = ""
model = "claude-sonnet-4-5"
thinking_effort = "low"
use_cli = false
extra_cli_args = ""

[diagnostics.openai]
api_key = ""
model = "gpt-5-mini"
reasoning_effort = "low"
use_cli = false
extra_cli_args = ""

[diagnostics.gemini]
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "low"
use_cli = false
extra_cli_args = ""
```

Field notes:

- `output_dir`: base directory for run folders.
- `default_max_tokens`: request token budget sent to providers.
- `max_history_messages`: max chat history kept per provider session.
- `use_cli = true`: use local CLI binary instead of HTTP API.
- `extra_cli_args`: single raw string appended as one extra CLI argument.
- `reasoning_effort`: OpenAI-only effort setting (`low|medium|high`).
- `thinking_effort`: Anthropic/Gemini effort setting (`low|medium|high`).
- `diagnostic_provider`: optional provider key used for automatic diagnostics pass.

Diagnostics configuration is separate from main run providers (`[diagnostics.*]` vs `[providers.*]`).

## Modes

- `relay`: agents run one after another; each agent builds on the previous agent output.
- `swarm`: agents run in parallel each round, then use prior round outputs in the next round.
- `solo`: selected agents run independently in parallel (forced to 1 iteration).

## Run Artifacts

Each run creates (or resumes) a directory inside `output_dir`:

- New run dir format: `YYYYMMDD_HHMMSS_<rand>` or `YYYYMMDD_HHMMSS_<rand>_<session_name>`
- Common files:
  - `prompt.md`
  - `session.toml`
  - `<provider>_iter<N>.md` (for example `openai_iter2.md`)
  - `_errors.log` (application-level logged errors)
- Optional files:
  - `consolidated_<provider>.md`
  - `errors.md` (diagnostic report)

## Resume, Consolidation, Diagnostics

- Resume (`Prompt` screen, `r`): available for `relay` and `swarm`.
  - With session name: resumes latest run matching that name suffix.
  - Without session name: resumes latest compatible run (mode + agents).
- Consolidation: offered after non-cancelled `swarm`/`solo` runs; writes one consolidated markdown output.
- Diagnostics: if `diagnostic_provider` is set, app runs a final diagnostics pass and writes `errors.md`.

## Keyboard Flow

Primary keys by screen:

- Home:
  - `Space` toggle agent/mode
  - `Tab` switch panels
  - `e` edit config popup
  - `Enter` continue
- Prompt:
  - `Tab` cycle fields
  - `r` toggle resume (`relay`/`swarm`)
  - `Enter`/`F5` run
  - `Esc` back
- Order (`relay` with >1 agent):
  - `j/k` move cursor
  - `Space` grab/reorder
  - `Enter` start run
- Running:
  - `Esc` cancel in-flight run
  - `Enter` open results after completion
- Results:
  - `j/k` navigate files
  - `Enter` return to home for a new run
  - `q` quit

Global:

- `Ctrl+C` exits from any screen (cancels active run first).

## Notes

- When any selected agent uses CLI mode, the app prepends working directory context to the prompt.
- API model list picker is available from config edit popup (`l`) when API key is present.
- Session edits in popup are in-memory until you save with `s`.
