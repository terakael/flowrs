use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Parser;
use log::{info, LevelFilter};
use simplelog::{Config, WriteLogger};

use crate::airflow::config::FlowrsConfig;
use crate::app::run_app;
use crate::app::state::App;
use anyhow::Result;

#[derive(Parser, Debug)]
pub struct RunCommand {
    #[clap(short, long)]
    pub file: Option<String>,
}

impl RunCommand {
    pub async fn run(&self) -> Result<()> {
        // setup logging
        if let Ok(log_level) = std::env::var("FLOWRS_LOG") {
            setup_logging(&log_level)?;
        }

        // Read config file
        let path = self.file.as_ref().map(PathBuf::from);
        let (config, mut errors) = match FlowrsConfig::from_file(path.as_ref()) {
            Ok(config) => {
                // Config loaded successfully, expand managed services
                config.expand_managed_services().await?
            }
            Err(e) => {
                // Config validation failed - show error popup but continue with defaults
                let default_config = FlowrsConfig::new();
                let validation_error = format!("Configuration Error:\n\n{}", e);
                (default_config, vec![validation_error])
            }
        };

        // setup terminal (includes panic hooks) and run app
        let mut terminal = ratatui::init();
        let app = App::new_with_errors(config, errors);
        run_app(&mut terminal, Arc::new(Mutex::new(app))).await?;

        info!("Shutting down the terminal...");
        ratatui::restore();
        Ok(())
    }
}

fn setup_logging(log_level: &str) -> Result<()> {
    // Get the XDG state directory for logs
    let log_dir = crate::get_state_dir().join("logs");
    
    // Create the log directory if it doesn't exist
    std::fs::create_dir_all(&log_dir)?;
    
    let log_file_path = log_dir.join(format!(
        "flowrs-debug-{}.log",
        chrono::Local::now().format("%Y%m%d%H%M%S")
    ));
    
    let log_level = match log_level.to_lowercase().as_str() {
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    WriteLogger::init(log_level, Config::default(), File::create(&log_file_path)?)?;
    
    // Log the file location so users know where to find it
    info!("Logging to: {}", log_file_path.display());
    
    Ok(())
}
