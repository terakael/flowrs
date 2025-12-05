use crate::airflow::config::ManagedService;
use anyhow::Result;
use clap::Parser;
use inquire::validator::Validation;
use strum::Display;
use strum::EnumIter;
use url::Url;

#[derive(Parser, Debug)]
pub enum ConfigCommand {
    Add(AddCommand),
    #[clap(alias = "rm")]
    Remove(RemoveCommand),
    Update(UpdateCommand),
    #[clap(alias = "ls")]
    List(ListCommand),
    Enable(ManagedServiceCommand),
    Disable(ManagedServiceCommand),
}

impl ConfigCommand {
    pub fn run(&self) -> Result<()> {
        match self {
            ConfigCommand::Add(cmd) => cmd.run(),
            ConfigCommand::Remove(cmd) => cmd.run(),
            ConfigCommand::Update(cmd) => cmd.run(),
            ConfigCommand::List(cmd) => cmd.run(),
            ConfigCommand::Enable(cmd) => cmd.run(),
            ConfigCommand::Disable(cmd) => cmd.disable(),
        }
    }
}

#[derive(Parser, Debug)]
pub struct AddCommand {
    #[clap(short, long)]
    pub file: Option<String>,
}

#[derive(Parser, Debug)]
pub struct RemoveCommand {
    pub name: Option<String>,
    #[clap(short, long)]
    pub file: Option<String>,
}

#[derive(Parser, Debug)]
pub struct ListCommand {
    #[clap(short, long)]
    pub file: Option<String>,
}

#[derive(Parser, Debug)]
pub struct UpdateCommand {
    pub name: Option<String>,
    #[clap(short, long)]
    pub file: Option<String>,
}

#[derive(EnumIter, Debug, Display)]
pub enum ConfigOption {
    BasicAuth,
    Token(Command),
    #[strum(serialize = "Google Cloud Composer")]
    Composer,
}

#[derive(Parser, Debug)]
pub struct ManagedServiceCommand {
    #[clap(short, long)]
    pub managed_service: Option<ManagedService>,
    #[clap(short, long)]
    pub file: Option<String>,
}

type Command = Option<String>;

#[allow(clippy::unnecessary_wraps)]
pub fn validate_endpoint(
    endpoint: &str,
) -> Result<Validation, Box<dyn std::error::Error + Send + Sync>> {
    match Url::parse(endpoint) {
        Ok(url) => {
            // Check if URL contains api/v1 or api/v2 which shouldn't be in the base endpoint
            if url.path().contains("/api/v1") || url.path().contains("/api/v2") {
                Ok(Validation::Invalid(
                    "⚠️ Endpoint should not include '/api/v1' or '/api/v2' - these are added automatically.\nExample: Use 'https://airflow.example.com/subpath' instead of 'https://airflow.example.com/subpath/api/v1'".into()
                ))
            } else {
                Ok(Validation::Valid)
            }
        }
        Err(error) => Ok(Validation::Invalid(error.into())),
    }
}

/// Validates a keyfile path for service account authentication.
/// Checks that the path exists, is a file, and contains valid JSON.
#[allow(clippy::unnecessary_wraps)]
pub fn validate_keyfile_path(
    path: &str,
) -> Result<Validation, Box<dyn std::error::Error + Send + Sync>> {
    // Expand environment variables first
    let expanded = match crate::airflow::config::expand_env_vars(path) {
        Ok(p) => p,
        Err(_) => {
            return Ok(Validation::Invalid(
                "⚠️ Invalid environment variable in path".into()
            ));
        }
    };
    
    // Check if file exists
    let path_obj = std::path::Path::new(&expanded);
    if !path_obj.exists() {
        return Ok(Validation::Invalid(
            format!("⚠️ File does not exist: {}", expanded).into()
        ));
    }
    
    // Check if it's a file (not a directory)
    if !path_obj.is_file() {
        return Ok(Validation::Invalid(
            "⚠️ Path must point to a file, not a directory".into()
        ));
    }
    
    // Check if it's valid JSON (basic validation)
    if let Ok(contents) = std::fs::read_to_string(path_obj) {
        if serde_json::from_str::<serde_json::Value>(&contents).is_err() {
            return Ok(Validation::Invalid(
                "⚠️ File is not valid JSON".into()
            ));
        }
    } else {
        return Ok(Validation::Invalid(
            "⚠️ Cannot read file".into()
        ));
    }
    
    Ok(Validation::Valid)
}

/// Prompts the user for proxy configuration.
/// 
/// # Arguments
/// * `default_proxy` - Optional default proxy URL to pre-fill in the prompt
/// 
/// # Returns
/// * `Ok(Some(String))` - User wants to configure a proxy and provided a URL
/// * `Ok(None)` - User chose not to configure a proxy
/// * `Err` - An error occurred during prompting
pub fn prompt_proxy_config(default_proxy: Option<&str>) -> Result<Option<String>> {
    let use_proxy = inquire::Confirm::new("Configure a proxy?")
        .with_default(default_proxy.is_some())
        .prompt()?;
    
    if use_proxy {
        let default = default_proxy.unwrap_or("");
        let proxy_url = inquire::Text::new("proxy URL")
            .with_default(default)
            .with_placeholder("http://proxy.example.com:8080")
            .with_help_message("Supports environment variables like ${PROXY_URL}")
            .prompt()?;
        Ok(Some(proxy_url))
    } else {
        Ok(None)
    }
}
