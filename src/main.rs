mod app;
mod config;
mod error;
mod event;
mod execution;
mod output;
mod provider;
mod screen;
mod tui;

use clap::Parser;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install panic hook that restores terminal before printing panic
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = tui::restore_terminal();
        original_hook(panic_info);
    }));

    let cli = Cli::parse();
    let config_path_override = cli.config.clone();

    if cli.init_config {
        let path =
            config::AppConfig::write_template_with_override(cli.config.as_deref(), cli.force)?;
        println!("Wrote config template to {}", path.display());
        return Ok(());
    }

    let config = match cli.config.as_deref() {
        Some(path) => config::AppConfig::load_with_override(Some(path))?,
        None => config::AppConfig::load()?,
    };

    let mut app = app::App::new(config);
    app.config_path_override = config_path_override;
    tui::run(&mut app).await?;

    Ok(())
}
