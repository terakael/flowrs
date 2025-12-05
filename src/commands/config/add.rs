use std::path::PathBuf;

use inquire::Select;
use strum::IntoEnumIterator;

use super::model::AddCommand;
use crate::{
    airflow::config::{
        AirflowAuth, AirflowConfig, AirflowVersion, BasicAuth, FlowrsConfig, TokenCmd,
    },
    airflow::managed_services::composer,
    commands::config::model::{prompt_proxy_config, validate_endpoint, validate_keyfile_path, ConfigOption},
};
use anyhow::Result;

impl AddCommand {
    pub fn run(&self) -> Result<()> {
        let auth_type =
            Select::new("authentication type", ConfigOption::iter().collect()).prompt()?;

        // For Composer, handle separately as it uses async
        if matches!(auth_type, ConfigOption::Composer) {
            return self.run_composer_add();
        }

        let name = inquire::Text::new("name").prompt()?;
        let endpoint = inquire::Text::new("endpoint")
            .with_validator(validate_endpoint)
            .prompt()?;

        // Optional proxy configuration
        let proxy = prompt_proxy_config(None)?;

        let version_str = inquire::Select::new("Airflow version", vec!["v2", "v3"])
            .with_help_message("Select the Airflow API version")
            .prompt()?;

        let version = match version_str {
            "v3" => AirflowVersion::V3,
            _ => AirflowVersion::V2,
        };

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
                    proxy,
                }
            }
            ConfigOption::Composer => {
                // This case is already handled at the top of the function
                unreachable!("Composer auth is handled separately")
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
                    proxy,
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

    fn run_composer_add(&self) -> Result<()> {
        println!("\n🌩️  Google Cloud Composer Configuration");
        println!("   Choose your authentication method:");
        println!("     1. Service account keyfile (recommended - no session expiration)");
        println!("     2. Application Default Credentials (ADC)\n");

        let auth_method = inquire::Select::new(
            "authentication method",
            vec!["Service account keyfile", "Application Default Credentials (ADC)"]
        ).prompt()?;

        let keyfile_path = if auth_method == "Service account keyfile" {
            println!("\n📝 Enter the path to your service account keyfile:");
            println!("   You can use environment variables (e.g., $GOOGLE_APPLICATION_CREDENTIALS)\n");
            
            let path = inquire::Text::new("keyfile path")
                .with_placeholder("$GOOGLE_APPLICATION_CREDENTIALS or /path/to/keyfile.json")
                .with_validator(validate_keyfile_path)
                .prompt()?;
            Some(path)
        } else {
            println!("\n   Make sure you have set up GCP credentials:");
            println!("     • Run: gcloud auth application-default login");
            println!("     • Or set GOOGLE_APPLICATION_CREDENTIALS environment variable\n");
            None
        };

        let name = inquire::Text::new("name")
            .with_help_message("A friendly name for this Composer environment")
            .prompt()?;

        let endpoint = inquire::Text::new("Airflow web server URL")
            .with_validator(validate_endpoint)
            .with_help_message("The Composer Airflow UI URL (e.g., https://abc123.composer.googleusercontent.com)")
            .prompt()?;

        let version_str = inquire::Select::new("Airflow version", vec!["v2", "v3"])
            .with_help_message("Composer 2 uses Airflow v2, Composer 3 uses Airflow v3")
            .prompt()?;

        let version = match version_str {
            "v3" => AirflowVersion::V3,
            _ => AirflowVersion::V2,
        };

        // Create the Composer config using async runtime
        let rt = tokio::runtime::Runtime::new()?;
        let new_config = rt.block_on(async {
            composer::create_composer_config(name, endpoint, version, keyfile_path).await
        })?;

        let path = self.file.as_ref().map(PathBuf::from);
        let mut config = FlowrsConfig::from_file(path.as_ref())?;

        // If the user provided a custom path, override the config path
        if let Some(user_path) = path {
            config.path = Some(user_path);
        }

        let uses_keyfile = new_config.auth.is_composer_with_keyfile();
        
        if let Some(mut servers) = config.servers.clone() {
            servers.retain(|server| server.name != new_config.name && server.managed.is_none());
            servers.push(new_config);
            config.servers = Some(servers);
        } else {
            config.servers = Some(vec![new_config]);
        }

        config.write_to_file()?;

        println!("✅ Composer config added successfully!");
        if uses_keyfile {
            println!("   Using service account keyfile for authentication.");
        } else {
            println!("   Using Application Default Credentials (ADC) for authentication.");
            println!("   Note: ADC may require periodic reauthentication.");
        }
        Ok(())
    }
}
