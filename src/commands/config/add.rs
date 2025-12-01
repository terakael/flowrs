use std::path::PathBuf;

use inquire::Select;
use strum::IntoEnumIterator;

use super::model::AddCommand;
use crate::{
    airflow::config::{
        AirflowAuth, AirflowConfig, AirflowVersion, BasicAuth, FlowrsConfig, TokenCmd,
    },
    commands::config::model::{validate_endpoint, ConfigOption},
};
use anyhow::Result;

impl AddCommand {
    pub fn run(&self) -> Result<()> {
        let name = inquire::Text::new("name").prompt()?;
        let endpoint = inquire::Text::new("endpoint")
            .with_validator(validate_endpoint)
            .prompt()?;

        let version_str = inquire::Select::new("Airflow version", vec!["v2", "v3"])
            .with_help_message("Select the Airflow API version")
            .prompt()?;

        let version = match version_str {
            "v3" => AirflowVersion::V3,
            _ => AirflowVersion::V2,
        };

        let auth_type =
            Select::new("authentication type", ConfigOption::iter().collect()).prompt()?;

        let new_config = match auth_type {
            ConfigOption::BasicAuth => {
                println!("\n📝 Enter environment variable names for credentials.");
                println!("   These will be expanded at runtime (e.g., AIRFLOW_USERNAME, AIRFLOW_PASSWORD)");
                println!("   You can use ${{VAR}} or $VAR syntax, or just the variable name.\n");
                
                let username = inquire::Text::new("username environment variable")
                    .with_placeholder("AIRFLOW_USERNAME")
                    .prompt()?;
                let password = inquire::Text::new("password environment variable")
                    .with_placeholder("AIRFLOW_PASSWORD")
                    .prompt()?;

                AirflowConfig {
                    name,
                    endpoint,
                    auth: AirflowAuth::Basic(BasicAuth { 
                        username: format!("${{{}}}", username.trim_start_matches('$').trim_start_matches('{').trim_end_matches('}')),
                        password: format!("${{{}}}", password.trim_start_matches('$').trim_start_matches('{').trim_end_matches('}')),
                    }),
                    managed: None,
                    version,
                }
            }
            ConfigOption::Token(_) => {
                println!("\n📝 Choose token authentication method:");
                println!("   1. Command: Execute a shell command to get the token (e.g., 'echo $TOKEN')");
                println!("   2. Environment variable: Reference an environment variable (e.g., AIRFLOW_TOKEN)\n");
                
                let token_method = inquire::Select::new(
                    "token method",
                    vec!["Command", "Environment Variable"]
                ).prompt()?;

                let (cmd, token) = match token_method {
                    "Command" => {
                        let cmd = inquire::Text::new("command")
                            .with_placeholder("echo $AIRFLOW_TOKEN")
                            .prompt()?;
                        (Some(cmd), None)
                    }
                    _ => {
                        let var_name = inquire::Text::new("token environment variable")
                            .with_placeholder("AIRFLOW_TOKEN")
                            .prompt()?;
                        let formatted_var = format!("${{{}}}", var_name.trim_start_matches('$').trim_start_matches('{').trim_end_matches('}'));
                        (None, Some(formatted_var))
                    }
                };

                AirflowConfig {
                    name,
                    endpoint,
                    auth: AirflowAuth::Token(TokenCmd { cmd, token }),
                    managed: None,
                    version,
                }
            }
        };

        let path = self.file.as_ref().map(PathBuf::from);
        let mut config = FlowrsConfig::from_file(path.as_ref())?;

        // If the user provided a custom path, override the config path so write_to_file
        // uses the user-specified location even if it didn't exist during from_file
        if let Some(user_path) = path {
            config.path = Some(user_path);
        }

        if let Some(mut servers) = config.servers.clone() {
            servers.retain(|server| server.name != new_config.name && server.managed.is_none());
            servers.push(new_config);
            config.servers = Some(servers);
        } else {
            config.servers = Some(vec![new_config]);
        }

        config.write_to_file()?;

        println!("✅ Config added successfully!");
        Ok(())
    }
}
