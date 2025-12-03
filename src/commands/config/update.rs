use std::path::PathBuf;

use inquire::Select;
use strum::IntoEnumIterator;

use super::model::UpdateCommand;
use crate::{
    airflow::config::{AirflowAuth, AirflowConfig, BasicAuth, FlowrsConfig, TokenCmd},
    commands::config::model::{prompt_proxy_config, validate_endpoint, ConfigOption},
};

use anyhow::Result;

impl UpdateCommand {
    pub fn run(&self) -> Result<()> {
        let path = self.file.as_ref().map(PathBuf::from);
        let mut config = FlowrsConfig::from_file(path.as_ref())?;

        if config.servers.is_none() {
            println!("❌ No servers found in config file");
            return Ok(());
        }

        let mut servers = config.servers.unwrap();

        let name: String = if self.name.is_none() {
            Select::new(
                "name",
                servers.iter().map(|server| server.name.clone()).collect(),
            )
            .prompt()?
        } else {
            self.name.clone().unwrap()
        };

        let airflow_config: &mut AirflowConfig = servers
            .iter_mut()
            .find(|server| server.name == name)
            .expect("🤔 Airflow config not found ...");

        let name = inquire::Text::new("name")
            .with_default(&airflow_config.name)
            .prompt()?;
        let endpoint = inquire::Text::new("endpoint")
            .with_default(&airflow_config.endpoint)
            .with_validator(validate_endpoint)
            .prompt()?;

        // Proxy configuration
        let proxy = prompt_proxy_config(airflow_config.proxy.as_deref())?;

        let auth_type =
            Select::new("authentication type", ConfigOption::iter().collect()).prompt()?;

        airflow_config.name = name;
        airflow_config.endpoint = endpoint;
        airflow_config.proxy = proxy;
        match auth_type {
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

                airflow_config.auth = AirflowAuth::Basic(BasicAuth { 
                    username: format!("${{{}}}", username.trim_start_matches('$').trim_start_matches('{').trim_end_matches('}')),
                    password: format!("${{{}}}", password.trim_start_matches('$').trim_start_matches('{').trim_end_matches('}')),
                });
            }
            ConfigOption::Composer => {
                println!("⚠️  Composer authentication cannot be updated directly.");
                println!("   Please remove the config and add it again.");
                return Err(anyhow::anyhow!("Composer configs cannot be updated"));
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

                airflow_config.auth = AirflowAuth::Token(TokenCmd { cmd, token });
            }
        }

        config.servers = Some(servers);
        config.write_to_file()?;

        println!("✅ Config updated successfully!");
        Ok(())
    }
}
