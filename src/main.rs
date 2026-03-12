mod app;
mod config;
mod error;
mod event;
mod execution;
mod headless;
mod output;
mod post_run;
mod provider;
mod runtime_support;
mod screen;
mod tui;

use clap::{Parser, ValueEnum};
use execution::ExecutionMode;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliMode {
    Relay,
    Swarm,
    Pipeline,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliOutputFormat {
    Text,
    Json,
}

#[derive(Parser, Debug)]
#[command(name = "houseofagents", about = "Multi-agent prompt runner with TUI")]
struct Cli {
    /// Path to config file (default: ~/.config/houseofagents/config.toml)
    #[arg(short, long)]
    config: Option<String>,

    /// Write a starter config file and exit
    #[arg(long)]
    init_config: bool,

    /// Overwrite config when used with --init-config
    #[arg(long)]
    force: bool,

    // -- Headless flags --
    /// Prompt text for relay/swarm (activates headless mode)
    #[arg(long, conflicts_with = "prompt_file")]
    prompt: Option<String>,

    /// Read prompt from a file (activates headless mode)
    #[arg(long, conflicts_with = "prompt")]
    prompt_file: Option<String>,

    /// Execution mode (default: swarm for prompt runs, pipeline for --pipeline)
    #[arg(long, value_enum)]
    mode: Option<CliMode>,

    /// Comma-separated agent names for relay/swarm
    #[arg(long, value_delimiter = ',')]
    agents: Vec<String>,

    /// Explicit relay order (comma-separated); if absent, uses --agents order
    #[arg(long, value_delimiter = ',')]
    order: Vec<String>,

    /// Number of iterations
    #[arg(long)]
    iterations: Option<u32>,

    /// Number of independent runs
    #[arg(long, default_value_t = 1)]
    runs: u32,

    /// Batch concurrency (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    concurrency: u32,

    /// Session name for output directory
    #[arg(long)]
    session_name: Option<String>,

    /// Forward prompt to all agents in relay mode
    #[arg(long)]
    forward_prompt: bool,

    /// Disable session history keeping
    #[arg(long)]
    no_keep_session: bool,

    /// Pipeline TOML file (activates headless mode)
    #[arg(long)]
    pipeline: Option<String>,

    /// Agent to use for post-run consolidation
    #[arg(long)]
    consolidate: Option<String>,

    /// Extra instructions for consolidation
    #[arg(long, default_value = "")]
    consolidation_prompt: String,

    /// Output format for headless mode
    #[arg(long, value_enum, default_value_t = CliOutputFormat::Text)]
    output_format: CliOutputFormat,

    /// Suppress stderr progress output
    #[arg(long)]
    quiet: bool,
}

impl Cli {
    fn is_headless(&self) -> bool {
        self.prompt.is_some() || self.prompt_file.is_some() || self.pipeline.is_some()
    }

    fn has_headless_only_flags(&self) -> bool {
        !self.agents.is_empty()
            || !self.order.is_empty()
            || self.iterations.is_some()
            || self.runs != 1
            || self.concurrency != 0
            || self.session_name.is_some()
            || self.forward_prompt
            || self.no_keep_session
            || self.consolidate.is_some()
            || !self.consolidation_prompt.is_empty()
            || self.quiet
            || !matches!(self.output_format, CliOutputFormat::Text)
            || self.mode.is_some()
    }
}

fn resolve_pipeline_path(raw: &str) -> Result<std::path::PathBuf, String> {
    let path = std::path::Path::new(raw);
    // Absolute or explicit relative (./foo, ../foo, dir/foo) → validate existence
    if path.is_absolute() || path != std::path::Path::new(path.file_name().unwrap_or_default()) {
        if !path.exists() {
            return Err(format!("Pipeline file not found: '{raw}'"));
        }
        if !path.is_file() {
            return Err(format!("Pipeline path is not a file: '{raw}'"));
        }
        return Ok(path.to_path_buf());
    }
    // Bare filename: check CWD first, then fall back to pipelines dir
    if path.is_file() {
        return Ok(path.to_path_buf());
    }
    if path.exists() {
        return Err(format!("Pipeline path is not a file: '{raw}'"));
    }
    let pipelines = execution::pipeline::pipelines_dir();
    let resolved = pipelines.join(path);
    if resolved.is_file() {
        return Ok(resolved);
    }
    Err(format!(
        "Pipeline '{}' not found in current directory or {}",
        raw,
        pipelines.display()
    ))
}

fn cli_to_headless_args(cli: &Cli) -> Result<headless::HeadlessArgs, String> {
    // Read prompt
    let prompt = if let Some(ref text) = cli.prompt {
        Some(text.clone())
    } else if let Some(ref path) = cli.prompt_file {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read prompt file '{}': {e}", path))?;
        if content.trim().is_empty() {
            return Err(format!("Prompt file '{}' is empty", path));
        }
        Some(content)
    } else {
        None
    };

    let mode = match (cli.mode, cli.pipeline.is_some()) {
        (Some(CliMode::Pipeline), _) | (None, true) => ExecutionMode::Pipeline,
        (Some(CliMode::Relay), _) => ExecutionMode::Relay,
        (Some(CliMode::Swarm), false) | (None, false) => ExecutionMode::Swarm,
        (Some(CliMode::Swarm), true) => {
            return Err(
                "--mode swarm is not valid with --pipeline; omit --mode or use --mode pipeline"
                    .into(),
            );
        }
    };

    let output_format = match cli.output_format {
        CliOutputFormat::Text => headless::OutputFormat::Text,
        CliOutputFormat::Json => headless::OutputFormat::Json,
    };

    Ok(headless::HeadlessArgs {
        prompt,
        mode,
        agents: cli.agents.clone(),
        relay_order: cli.order.clone(),
        iterations: cli.iterations,
        runs: cli.runs,
        concurrency: cli.concurrency,
        session_name: cli.session_name.clone(),
        forward_prompt: cli.forward_prompt,
        keep_session: !cli.no_keep_session,
        pipeline_path: cli
            .pipeline
            .as_ref()
            .map(|p| resolve_pipeline_path(p))
            .transpose()?,
        consolidate_agent: cli.consolidate.clone(),
        consolidation_prompt: cli.consolidation_prompt.clone(),
        output_format,
        quiet: cli.quiet,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.init_config {
        let path =
            config::AppConfig::write_template_with_override(cli.config.as_deref(), cli.force)?;
        println!("Wrote config template to {}", path.display());
        return Ok(());
    }

    // Early reject: headless-only flags without a headless primary flag.
    // Checked before config loading so a missing/broken config doesn't mask
    // the more relevant validation error.
    if !cli.is_headless() && cli.has_headless_only_flags() {
        let msg = "headless flags (--agents, --order, --runs, etc.) require --prompt, --prompt-file, or --pipeline";
        if matches!(cli.output_format, CliOutputFormat::Json) {
            let obj = serde_json::json!({"event": "result", "status": "error", "error": msg});
            println!("{}", serde_json::to_string(&obj).unwrap_or_default());
        } else {
            eprintln!("Error: {msg}");
        }
        std::process::exit(1);
    }

    let config = match cli.config.as_deref() {
        Some(path) => config::AppConfig::load_with_override(Some(path)),
        None => config::AppConfig::load(),
    };
    let config = match config {
        Ok(c) => c,
        Err(e) => {
            if cli.is_headless() && matches!(cli.output_format, CliOutputFormat::Json) {
                let obj = serde_json::json!({"event": "result", "status": "error", "error": e.to_string()});
                println!("{}", serde_json::to_string(&obj).unwrap_or_default());
                std::process::exit(1);
            }
            return Err(e.into());
        }
    };

    if cli.is_headless() {
        let args = match cli_to_headless_args(&cli) {
            Ok(args) => args,
            Err(e) => {
                if matches!(cli.output_format, CliOutputFormat::Json) {
                    let obj = serde_json::json!({"event": "result", "status": "error", "error": e});
                    println!("{}", serde_json::to_string(&obj).unwrap_or_default());
                } else {
                    eprintln!("Error: {e}");
                }
                std::process::exit(1);
            }
        };
        let exit_code = headless::run(args, config).await;
        std::process::exit(exit_code);
    }

    // TUI path: install panic hook that restores terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = tui::restore_terminal();
        original_hook(panic_info);
    }));

    let config_path_override = cli.config.clone();
    let mut app = app::App::new(config);
    app.config_path_override = config_path_override;
    tui::run(&mut app).await?;

    Ok(())
}
