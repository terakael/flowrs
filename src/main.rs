use std::path::PathBuf;
use std::sync::LazyLock;

use clap::Parser;
use ui::constants::ASCII_LOGO;

mod airflow;
mod app;
mod commands;
mod editor;
mod ui;

use anyhow::Result;
use commands::config::model::ConfigCommand;
use commands::run::RunCommand;
use dirs::{config_dir, home_dir, state_dir};

/// Get the configuration file path using XDG Base Directory specification
/// Prefers XDG config location (~/.config/flowrs/config.toml) but falls back to
/// legacy location (~/.flowrs) if it exists and XDG doesn't
static CONFIG_FILE: LazyLock<PathBuf> = LazyLock::new(|| {
    let xdg_config = config_dir()
        .unwrap_or_else(|| {
            home_dir()
                .expect("HOME directory must be set to run flowrs")
                .join(".config")
        })
        .join("flowrs")
        .join("config.toml");
    
    // Use XDG location if it exists
    if xdg_config.exists() {
        return xdg_config;
    }
    
    // Otherwise check for legacy location
    let legacy_config = home_dir()
        .expect("HOME directory must be set to run flowrs")
        .join(".flowrs");
    
    if legacy_config.exists() {
        legacy_config
    } else {
        // Neither exists - use XDG for new installations
        xdg_config
    }
});

/// Get the state directory path using XDG Base Directory specification
/// Used for logs and other state files
pub fn get_state_dir() -> PathBuf {
    state_dir()
        .unwrap_or_else(|| {
            home_dir()
                .expect("HOME directory must be set to run flowrs")
                .join(".local")
                .join("state")
        })
        .join("flowrs")
}

#[derive(Parser)]
#[clap(name="flowrs", bin_name="flowrs", version, about, before_help=ASCII_LOGO)]
struct FlowrsApp {
    #[clap(subcommand)]
    command: Option<FlowrsCommand>,
}

#[derive(Parser)]
enum FlowrsCommand {
    Run(RunCommand),
    #[clap(subcommand)]
    Config(ConfigCommand),
}

impl FlowrsApp {
    pub async fn run(&self) -> Result<()> {
        match &self.command {
            Some(FlowrsCommand::Run(cmd)) => cmd.run().await,
            Some(FlowrsCommand::Config(cmd)) => cmd.run(),
            None => RunCommand { file: None }.run().await,
        }
    }
}

// Initialize rustls crypto provider for gcp_auth
// This uses ring as the crypto backend
fn init_crypto_provider() {
    use std::sync::Once;
    static CRYPTO_PROVIDER_INIT: Once = Once::new();
    
    CRYPTO_PROVIDER_INIT.call_once(|| {
        // Try to install the default crypto provider (ring)
        // This may fail if already installed, which is fine
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize crypto provider before any async operations
    init_crypto_provider();
    
    let app = FlowrsApp::parse();
    app.run().await?;
    std::process::exit(0);
}
