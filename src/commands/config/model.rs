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
