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
| **Pipeline** | Custom DAG builder — wire arbitrary blocks of agents into a dependency graph, with optional finalization DAG for post-execution analysis |

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
- **Pipeline builder** — visual DAG editor for wiring arbitrary agent blocks with dependency-driven execution, independent per-connection routing, loop-back connections for iterative refinement of sub-DAGs, and an optional finalization DAG for post-execution analysis and summarization
- **Multiple runs** — launch N independent copies of the same setup in parallel with bounded concurrency
- **Resume runs** — pick up where you left off in relay or swarm sessions
- **Forward Prompt** — relay mode option to include the original prompt in every handoff
- **Keep Session** — toggle per-provider conversation history persistence across iterations (on by default; turn off to clear provider memory between iterations while preserving inter-agent handoff context). Pipeline mode has its own per-session configuration popup in the Builder screen (`s`)
- **Consolidation** — merge multi-agent output within a run or across multiple runs (any configured agent can consolidate)
- **Diagnostics** — optional post-run analysis pass that writes `errors.md`
- **Config editor** — add/remove/rename agents, edit settings, timeouts, and models live with a popup (`e`)
- **Model picker** — browse available models from the API directly inside the config editor (`l`)
- **CLI print mode** — Anthropic agents in CLI mode can toggle between print (`-p`) and agent mode
- **Cross-run memory** — SQLite-backed memory system that recalls relevant context from previous runs and extracts new memories post-run (decision, observation, summary, principle)

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

Headless mode (noninteractive):
      --prompt <TEXT>              Prompt text (activates headless mode)
      --prompt-file <PATH>        Read prompt from file (activates headless mode)
      --pipeline <PATH>           Pipeline TOML file (activates headless mode)
      --mode <MODE>               Execution mode: relay, swarm, pipeline [default: swarm]
      --agents <A,B,...>          Comma-separated agent names
      --order <A,B,...>           Explicit relay order (defaults to --agents order)
      --iterations <N>            Number of iterations
      --runs <N>                  Independent runs [default: 1]
      --concurrency <N>           Batch concurrency, 0 = unlimited [default: 0]
      --session-name <NAME>       Output directory label
      --forward-prompt            Forward prompt to all relay agents
      --no-keep-session           Disable session history keeping
      --consolidate <AGENT>       Agent for post-run consolidation
      --consolidation-prompt <S>  Extra consolidation instructions
      --output-format <FMT>       Output format: text, json [default: text]
      --quiet                     Suppress stderr progress output
      --no-memory                 Disable cross-run memory
```

## Noninteractive (Headless) Mode

Run agents from scripts and CI pipelines without the TUI. Activate headless mode by passing `--prompt`, `--prompt-file`, or `--pipeline`.

### Relay / Swarm Examples

```bash
# Swarm with two agents, 3 iterations
houseofagents --prompt "Analyze this codebase" --agents Claude,OpenAI --iterations 3

# Relay with explicit order
houseofagents --prompt "Review and fix" --mode relay --agents Claude,OpenAI --order OpenAI,Claude

# Batch: 5 independent runs, 3 at a time
houseofagents --prompt "Generate tests" --agents Claude --runs 5 --concurrency 3

# With consolidation
houseofagents --prompt "Debate pros and cons" --agents Claude,Gemini --iterations 2 \
  --consolidate Claude --consolidation-prompt "Summarize the debate"

# JSON output for scripting
houseofagents --prompt "Analyze" --agents Claude --output-format json --quiet
```

### Pipeline Examples

```bash
# Run a saved pipeline
houseofagents --pipeline my_pipeline.toml

# Override iterations and run in batch
houseofagents --pipeline my_pipeline.toml --iterations 5 --runs 3

# Pipeline with consolidation
houseofagents --pipeline my_pipeline.toml --consolidate Claude
```

### Output Behavior

- **stdout**: In text mode, prints the run directory path on success. In JSON mode, prints a structured result object.
- **stderr**: Progress events (agent starts, iteration completions, errors). Suppressed with `--quiet`. In JSON output format, progress events are NDJSON on stderr.
- Headless mode never enters alternate-screen mode — safe for piping and CI.

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Validation error (bad flags, missing agents, etc.) |
| `2` | Execution error (provider failure, partial failure) |
| `130` | Cancelled (Ctrl+C) |

### Limitations

- Resume is not supported in headless mode (planned for a future version).
- Prompt is required for relay/swarm (`--prompt` or `--prompt-file`); for pipeline, the pipeline TOML's `initial_prompt` is used if `--prompt` is not given.
- `--consolidate` is not supported for single-run relay (relay produces a single output chain with no branches to merge). Batch relay consolidation works normally.

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

**Memory settings** (`[memory]`):

| Field | Description |
|-------|-------------|
| `enabled` | Enable cross-run memory (`true` by default) |
| `db_path` | Custom SQLite database path (default: `{output_dir}/memory.db`) |
| `project_id` | Override automatic project detection (default: derived from git remote or cwd) |
| `max_recall` | Max memories to recall per run (default: 20) |
| `max_recall_bytes` | Byte budget for recalled memory context (default: 16384) |
| `extraction_agent` | Agent to use for post-run memory extraction (default: first participating agent, then first configured). Stronger models produce higher-quality memories. |
| `disable_extraction` | Disable post-run memory extraction (default: false) |
| `observation_ttl_days` | Days before observations expire (default: 120) |
| `summary_ttl_days` | Days before summaries expire (default: 180) |
| `stale_permanent_days` | Archive permanent memories (decisions, principles) after N days without recall (default: 365, 0 to disable) |

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
| `M` | Open memory management (when memory enabled) |
| `?` | Open help popup |
| `Enter` | Continue to prompt |
| `q` | Quit |

### Memory Management

| Key | Action |
|-----|--------|
| `j` / `k` / `Up` / `Down` | Navigate memories |
| `d` | Delete selected memory |
| `D` | Bulk delete all visible memories (press twice to confirm) |
| `f` | Cycle kind filter (all → decision → observation → summary → principle) |
| `r` | Toggle "never recalled" filter |
| `a` | Toggle archived view |
| `u` | Unarchive selected memory (in archived view) |
| `q` / `Esc` | Back to home |

### Config Editor

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate agents, timeouts, or memory settings |
| `Tab` / `Shift+Tab` | Switch section (Agents / Timeouts / Memory) |
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
| `e` / `Enter` | Edit selected value (Timeouts / Memory sections) |
| `Space` | Toggle boolean setting (Memory section) |
| `s` | Save config to disk |
| `Esc` | Close (keep changes for session) |

### Prompt Screen

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Cycle input fields forward / backward |
| `Space` | Toggle focused option (Resume / Forward Prompt / Keep Session) |
| `Enter` / `F5` | Start run |
| `Ctrl+E` | Analyze setup — sends current configuration to `diagnostic_provider` for a plain-language explanation |
| `?` | Open help (unavailable while editing text fields: prompt, session name) |
| `Esc` | Back |

Fields vary by mode for options, but every prompt flow includes Prompt, Session Name, Iterations, Runs, and Concurrency.

### Pipeline Builder Screen

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Cycle focus: Initial Prompt → Session Name → Iterations → Runs → Concurrency → Builder |
| `a` | Add a new block |
| `d` | Delete selected block |
| `e` | Edit selected block (name, agents, prompt, session ID, replicas) |
| `c` | Enter connect mode — select a second block to create a connection |
| `x` | Enter remove-connection mode — pick a connection to delete |
| `o` | Create loop-back connection — press on the downstream feedback block, then select the upstream restart target; set count and prompt; press on existing loop to edit |
| `A` | Add a finalization block (placed below the separator in the finalization region) |
| `f` | Create or edit data feed — on an execution block, enters feed-connect mode (navigate to a finalization block and press Enter to create the feed); on a finalization block with multiple feeds, opens a feed list picker for selecting and editing individual feeds (opens edit directly if only one feed) |
| `F` | Remove a data feed — removes directly if only one feed on the block; on a finalization block with multiple feeds, opens the feed list picker to select which one; on an execution block with multiple feeds, prompts to use the finalization block instead |
| `s` | Open session configuration popup — toggle per-session history persistence |
| `Arrow keys` / `h j k l` | Navigate/select blocks spatially without moving |
| `Shift+Arrow keys` / `Shift+H J K L` | Move selected block (swap with occupied target cell, otherwise move) |
| `Ctrl+Arrow keys` | Scroll the builder canvas |
| `↑`/`+` `↓`/`-` | Increment / decrement iterations, runs, or concurrency on the focused numeric field |
| `Ctrl+S` | Save pipeline (always prompts for filename, prefills current name) |
| `Ctrl+L` | Load pipeline from file (type to search, Tab toggles search/list focus, j/k navigates list) |
| `Ctrl+E` | Analyze setup — sends current pipeline to `diagnostic_provider` for a plain-language explanation |
| `F5` | Validate and run the pipeline |
| `?` | Open help popup (7 tabbed sections; Tab/Shift+Tab to cycle). Only when focus is not on a text field (initial prompt / session name). |
| `Esc` | Cancel current action / back to home |

Inside the **edit popup**: `Tab` cycles between Name, Agents (multiselect list — `Up`/`Down` to navigate, `Space` to toggle), Profiles (multiselect list of reusable instruction files), Prompt (text area), Session ID, and Replicas fields. `Esc` closes the popup. Each block can have one or more agents selected. Setting Replicas > 1 spawns that many copies per agent. Total tasks per block = agents × replicas (max 32).

### Order Screen (relay with 2+ agents)

| Key | Action |
|-----|--------|
| `j` / `k` | Move cursor |
| `Space` | Grab / reorder agent |
| `Enter` | Confirm and start |
| `Ctrl+E` | Analyze setup — sends current configuration to `diagnostic_provider` for a plain-language explanation |
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
      _memories.json               # Optional: extracted memories from this run
      _errors.log                  # Application-level error log
    swift-falcon/                  # auto-generated name (no session provided)
      ...
```

Pipeline runs produce per-block output files named using the block's name (sanitized) and a unique block id suffix. Blocks with multiple agents produce one file per agent. When `replicas > 1`, each replica gets an `_rN` suffix. Total runtime tasks per block = agents × replicas:

```
output_dir/
  2026-03-05/
    my_session/
      session.toml                         # mode = "pipeline", block/connection counts, total_runtime_tasks
      prompt.md                            # Pipeline-level prompt (shared across blocks)
      pipeline.toml                        # Pipeline definition snapshot (may include [[session_configs]])
      Analyzer_b1_Claude_iter1.md          # Block "Analyzer" (id 1), agent Claude, iteration 1
      Reviewer_b2_Gemini_iter1.md          # Block "Reviewer" (id 2), single agent, replicas=1
      Critic_b4_Claude_iter1.md            # Block "Critic" (id 4), agents=[Claude, GPT] — one file per agent
      Critic_b4_GPT_iter1.md
      Worker_b3_Claude_r1_iter1.md         # Block "Worker" (id 3), replicas=3, replica 1
      Worker_b3_Claude_r2_iter1.md         # replica 2
      Worker_b3_Claude_r3_iter1.md         # replica 3
      Analyzer_b1_Claude_iter1_loop1.md   # loop pass 1 (from loop connection)
      Analyzer_b1_Claude_iter1_loop2.md   # loop pass 2
      _errors.log
```

Loop-back connections create iterative refinement cycles. `from` is the downstream feedback source and `to` is the upstream restart target. All blocks on regular-graph paths between the two endpoints form the loop sub-DAG and re-run on each pass. In saved pipeline TOML files they appear as:

```toml
[[loop_connections]]
from = 2
to = 1
count = 3
prompt = "Refine based on feedback"
```

`count` is the number of additional passes beyond the initial run. Each block in the sub-DAG runs `count + 1` times total. Loop wires are drawn as double-line in yellow on the canvas.

### Profiles

Profiles are reusable system instruction files (Markdown) stored in
`~/.config/houseofagents/profiles/`. Assign them to pipeline blocks to
inject instructions into every message sent by that block.

**Create a profile:**
```bash
mkdir -p ~/.config/houseofagents/profiles
cat > ~/.config/houseofagents/profiles/reviewer.md << 'EOF'
You are a senior code reviewer. Focus on:
- Security vulnerabilities
- Performance implications
- API contract violations
EOF
```

**Assign profiles:** Open the pipeline builder, select a block, press `e` to edit,
Tab to the Profiles field, and Space to toggle profiles on/off.

**CLI vs API behavior:**
- **API agents:** Profile content is read and inlined into the message.
- **CLI agents:** Profile file paths are passed (the CLI tool reads them).

Profiles persist in the pipeline TOML under each block's `profiles` key.
Values are file stems without the `.md` extension:
```toml
[[blocks]]
id = 1
agents = ["Claude"]
profiles = ["reviewer", "security"]
prompt = "Review this PR"
position = [0, 0]
```

Finalization blocks support profiles too since they share the same block model.

**Missing profiles:** If a profile file is deleted or renamed after assignment, the
edit dialog shows it in yellow with `[!]` and setup analysis marks it `[missing]`.
Missing profiles are silently skipped at runtime — no instructions are injected for them.

### Finalization DAG

Pipeline mode supports an optional **finalization phase** that runs after the execution DAG completes. Finalization blocks receive execution outputs via **data feeds** and can be wired into their own dependency DAG via finalization connections. When finalization is defined, it replaces consolidation for pipeline mode.

**Data feeds** connect execution blocks to finalization blocks with two configurable dimensions:

| Setting | Options | Description |
|---------|---------|-------------|
| **Collection** | `last_iteration` (default), `all_iterations` | Which iteration outputs to collect from execution blocks |
| **Granularity** | `per_run` (default), `all_runs` | Whether the finalization block runs once per successful run or once across all runs |

A **per-run** finalization block runs independently for each successful run, receiving only that run's execution outputs. An **all-runs** finalization block runs once, receiving outputs from all successful runs. Per-run blocks can feed into all-runs blocks via finalization connections, enabling patterns like "summarize each run, then synthesize across summaries."

Wildcard feeds (`from = 0`) collect outputs from all execution blocks. Block-specific feeds target a single execution block.

Single-run finalization outputs are stored in `run_dir/finalization/`:

```
my_session/
  session.toml
  pipeline.toml
  Analyzer_b1_Claude_iter1.md
  Reviewer_b2_Gemini_iter1.md
  finalization/
    finalization.toml
    Summary_b3_Claude.md
    Report_b4_GPT.md
```

Batch finalization outputs are stored in `batch_root/finalization/`:

```
my_session/
  batch.toml
  run_1/
    ...
  run_3/
    ...
  finalization/
    finalization.toml
    per_run_summary_Claude_run1.md
    per_run_summary_Claude_run3.md
    meta_report_Claude.md
```

Per-run finalization filenames include the real run ID (only successful runs). The `finalization.toml` metadata file records the successful run IDs, finalization block count, and feed count.

Example pipeline TOML with finalization:

```toml
initial_prompt = "Analyze the codebase"
iterations = 2

[[blocks]]
id = 1
name = "Analyzer"
agents = ["Claude"]
prompt = "Analyze the architecture"
row = 0
col = 0

[[blocks]]
id = 2
name = "Reviewer"
agents = ["Gemini"]
prompt = "Review the analysis"
row = 0
col = 1

[[connections]]
from = 1
to = 2

[[finalization_blocks]]
id = 3
name = "Summary"
agents = ["Claude"]
prompt = "Synthesize all findings into a final report"
row = 0
col = 0

[[data_feeds]]
from = 0
to = 3
collection = "last_iteration"
granularity = "per_run"
```

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
- **Keep Session** (toggle with `Space` on Prompt screen) — on by default; controls whether providers retain their conversation history across iterations. When turned off, each provider's history is cleared before every iteration after the first, so agents treat each round as a fresh conversation. Inter-agent handoff context (relay's previous output, swarm's round outputs) is always preserved regardless of this setting. Pipeline mode has its own per-session session configuration popup accessible via `s` in the Builder screen — each effective session (shared or isolated) can independently toggle two settings: **Iter** (keep across iterations, default on) and **Loop** (keep across loop passes, default on). When Loop is off, provider history is cleared between loop pass advances within a single iteration. Use `h`/`l` to switch columns and `Space` to toggle. Non-default settings are stored in `pipeline.toml` as `[[session_configs]]` entries
- **Consolidation**
  - Single-run: offered after non-cancelled swarm/pipeline runs with 2+ final outputs
  - Batch: first offers per-run consolidation, then optional cross-run consolidation across successful runs
  - Skipped automatically for pipeline runs that have finalization defined (finalization replaces consolidation)
- **Setup Analysis** — press `Ctrl+E` on the Prompt, Order, or Pipeline screen to send the current run configuration to the `diagnostic_provider` for a plain-language explanation of what the run will do. Requires `diagnostic_provider` to be set in config. The popup shows loading state, then a scrollable analysis result (scroll with `j`/`k`/arrows/PgUp/PgDn, close with `Esc`/`q`). Pre-flight checks catch invalid setups locally before making the provider call. Errors are shown inside the popup itself.
- **Diagnostics** — when `diagnostic_provider` is set to an agent name, a final analysis pass writes `errors.md`
