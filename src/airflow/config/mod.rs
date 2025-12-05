use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use clap::ValueEnum;
use log::info;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

use super::managed_services::astronomer::get_astronomer_environment_servers;
use super::managed_services::conveyor::get_conveyor_environment_servers;
use super::managed_services::mwaa::get_mwaa_environment_servers;
use crate::CONFIG_FILE;
use anyhow::Result;

/// Expands environment variables in a string value.
/// Supports ${VAR} and $VAR syntax.
pub fn expand_env_vars(value: &str) -> Result<String> {
    shellexpand::env(value)
        .map(|s| s.into_owned())
        .map_err(|e| anyhow::anyhow!("Failed to expand environment variable in '{}': {}", value, e))
}

/// Normalizes an endpoint URL by ensuring it has a scheme and trailing slash.
/// 
/// # Arguments
/// * `endpoint` - The endpoint URL to normalize
/// 
/// # Returns
/// A normalized URL with `https://` prefix (if no scheme) and trailing slash
/// 
/// # Examples
/// ```
/// use flowrs_tui::airflow::config::normalize_endpoint;
/// 
/// assert_eq!(
///     normalize_endpoint("example.com".to_string()),
///     "https://example.com/"
/// );
/// assert_eq!(
///     normalize_endpoint("https://example.com".to_string()),
///     "https://example.com/"
/// );
/// ```
pub fn normalize_endpoint(endpoint: String) -> String {
    let mut normalized = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint
    } else {
        format!("https://{}", endpoint)
    };

    if !normalized.ends_with('/') {
        normalized.push('/');
    }

    normalized
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Default)]
pub enum AirflowVersion {
    #[default]
    V2,
    V3,
}

impl AirflowVersion {
    pub fn api_path(&self) -> &str {
        match self {
            AirflowVersion::V2 => "api/v1",
            AirflowVersion::V3 => "api/v2",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, ValueEnum, EnumIter)]
pub enum ManagedService {
    Conveyor,
    Mwaa,
    Astronomer,
    Gcc,
}

impl Display for ManagedService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagedService::Conveyor => write!(f, "Conveyor"),
            ManagedService::Mwaa => write!(f, "MWAA"),
            ManagedService::Astronomer => write!(f, "Astronomer"),
            ManagedService::Gcc => write!(f, "Google Cloud Composer"),
        }
    }
}

/// Configuration for the Flowrs TUI application.
///
/// This configuration is persisted to disk at `~/.flowrs` (or custom path).
///
/// # Date/Time Display
/// The `timezone_offset` field controls how dates are displayed. Airflow API returns
/// timestamps in UTC, and this setting converts them to your preferred timezone for display.
///
/// Supported values: UTC offset in format "+HH:MM" or "-HH:MM"
/// Examples: "+09:00" (JST), "-05:00" (EST), "+00:00" (UTC)
///
/// Note: DST is not automatically handled. You may need to adjust the offset manually
/// when daylight saving time changes (e.g., EST "-05:00" vs EDT "-04:00").
///
/// # Note on Active Environment
/// The active environment is not persisted. Users must select an environment
/// on each startup. This design prevents confusion from stale cached data and ensures
/// explicit environment awareness when working with multiple Airflow instances.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FlowrsConfig {
    pub servers: Option<Vec<AirflowConfig>>,
    pub managed_services: Option<Vec<ManagedService>>,
    #[serde(default = "default_show_init_screen")]
    pub show_init_screen: bool,
    #[serde(default = "default_timezone_offset")]
    pub timezone_offset: String,
    #[serde(skip_serializing)]
    pub path: Option<PathBuf>,
}

fn default_show_init_screen() -> bool {
    true
}

fn default_timezone_offset() -> String {
    "+00:00".to_string() // UTC by default
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AirflowConfig {
    pub name: String,
    pub endpoint: String,
    pub auth: crate::airflow::config::AirflowAuth,
    pub managed: Option<ManagedService>,
    #[serde(default)]
    pub version: AirflowVersion,
    pub proxy: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum AirflowAuth {
    Basic(BasicAuth),
    Token(TokenCmd),
    Conveyor,
    Mwaa(super::managed_services::mwaa::MwaaAuth),
    Astronomer(super::managed_services::astronomer::AstronomerAuth),
    Composer(super::managed_services::composer::ComposerAuth),
}

impl AirflowAuth {
    /// Checks if this is Composer auth with a keyfile path
    pub fn is_composer_with_keyfile(&self) -> bool {
        matches!(self, AirflowAuth::Composer(auth) if auth.uses_keyfile())
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for BasicAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicAuth")
            .field("username", &self.username)
            .field("password", &"***redacted***")
            .finish()
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TokenCmd {
    pub cmd: Option<String>,
    pub token: Option<String>,
}

impl std::fmt::Debug for TokenCmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenCmd")
            .field("cmd", &self.cmd)
            .field("token", &self.token.as_ref().map(|_| "***redacted***"))
            .finish()
    }
}

impl Default for FlowrsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowrsConfig {
    /// Creates a new `FlowrsConfig` with default values.
    ///
    /// Returns a `FlowrsConfig` with:
    /// - No servers configured
    /// - No managed services configured
    /// - Init screen enabled by default
    /// - Default config file path (`~/.flowrs`)
    pub fn new() -> Self {
        Self {
            servers: None,
            managed_services: None,
            show_init_screen: true,
            timezone_offset: "+00:00".to_string(),
            path: Some(CONFIG_FILE.as_path().to_path_buf()),
        }
    }

    pub fn from_file(config_path: Option<&PathBuf>) -> Result<Self> {
        let path = config_path
            .filter(|p| p.exists())
            .cloned()
            .unwrap_or_else(|| {
                // No valid path was provided by the user, use the default path
                let default_path = CONFIG_FILE.as_path().to_path_buf();
                info!("Using configuration path: {}", default_path.display());
                default_path
            });

        // If no config at the default path, return an empty (default) config
        let toml_config = std::fs::read_to_string(&path).unwrap_or_default();
        let mut config = Self::from_str(&toml_config)?;
        config.path = Some(path.clone());
        Ok(config)
    }

    pub fn from_str(config: &str) -> Result<Self> {
        let config: FlowrsConfig = toml::from_str(config)?;
        
        // Validate the configuration
        config.validate()?;
        
        let num_serves = config.servers.as_ref().map_or(0, std::vec::Vec::len);
        let num_managed = config
            .managed_services
            .as_ref()
            .map_or(0, std::vec::Vec::len);
        info!("Loaded config: servers={num_serves}, managed_services={num_managed}");
        Ok(config)
    }
    
    /// Validate the configuration and return detailed errors
    pub fn validate(&self) -> Result<()> {
        // Validate timezone offset format
        Self::validate_timezone_offset(&self.timezone_offset)?;
        
        // Validate servers if present
        if let Some(servers) = &self.servers {
            for (idx, server) in servers.iter().enumerate() {
                if server.name.trim().is_empty() {
                    return Err(anyhow::anyhow!(
                        "Server #{} has empty name",
                        idx + 1
                    ));
                }
                if server.endpoint.trim().is_empty() {
                    return Err(anyhow::anyhow!(
                        "Server '{}' has empty endpoint",
                        server.name
                    ));
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate timezone offset format
    fn validate_timezone_offset(offset: &str) -> Result<()> {
        // Check basic format
        if !offset.starts_with('+') && !offset.starts_with('-') {
            return Err(anyhow::anyhow!(
                "Invalid timezone offset format: '{}'. Must start with + or -. Examples: '+09:00', '-05:00', '+00:00'",
                offset
            ));
        }
        
        // Parse components
        let parts: Vec<&str> = offset[1..].split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!(
                "Invalid timezone offset format: '{}'. Expected format: +HH:MM or -HH:MM. Examples: '+09:00', '-05:00'",
                offset
            ));
        }
        
        // Validate hours
        let hours: i8 = parts[0].parse()
            .map_err(|_| anyhow::anyhow!(
                "Invalid hours in timezone offset: '{}'. Hours must be a number between -14 and +14",
                offset
            ))?;
        
        // Validate minutes
        let minutes: i8 = parts[1].parse()
            .map_err(|_| anyhow::anyhow!(
                "Invalid minutes in timezone offset: '{}'. Minutes must be a number between 00 and 59",
                offset
            ))?;
        
        // Check ranges
        if hours.abs() > 14 {
            return Err(anyhow::anyhow!(
                "Timezone offset hours out of range: '{}'. Hours must be between -14 and +14",
                offset
            ));
        }
        
        if minutes.abs() > 59 {
            return Err(anyhow::anyhow!(
                "Timezone offset minutes out of range: '{}'. Minutes must be between 00 and 59",
                offset
            ));
        }
        
        // Validate with time crate
        let is_negative = offset.starts_with('-');
        let hours = if is_negative { -hours } else { hours };
        let minutes = if is_negative { -minutes } else { minutes };
        
        time::UtcOffset::from_hms(hours, minutes, 0)
            .map_err(|_| anyhow::anyhow!(
                "Invalid timezone offset: '{}'. The time crate rejected this offset",
                offset
            ))?;
        
        Ok(())
    }

    fn extend_servers<I>(&mut self, new_servers: I)
    where
        I: IntoIterator<Item = AirflowConfig>,
    {
        match &mut self.servers {
            Some(existing) => existing.extend(new_servers),
            None => self.servers = Some(new_servers.into_iter().collect()),
        }
    }

    /// Expands the config by resolving managed services and adding their servers.
    /// This is an async convenience function that should be called after `from_file`/`from_str`
    /// when you need to resolve managed service environments.
    /// Returns a tuple of (config, errors) where errors contains any non-fatal errors encountered.
    pub async fn expand_managed_services(mut self) -> Result<(Self, Vec<String>)> {
        let mut all_errors = Vec::new();

        if self.managed_services.is_none() {
            return Ok((self, all_errors));
        }

        let services = self.managed_services.clone().unwrap();
        for service in services {
            match service {
                ManagedService::Conveyor => {
                    let conveyor_servers = get_conveyor_environment_servers()?;
                    self.extend_servers(conveyor_servers);
                }
                ManagedService::Mwaa => {
                    let mwaa_servers = get_mwaa_environment_servers().await?;
                    self.extend_servers(mwaa_servers);
                }
                ManagedService::Astronomer => {
                    let (astronomer_servers, errors) = get_astronomer_environment_servers().await;
                    all_errors.extend(errors);
                    self.extend_servers(astronomer_servers);
                }
                ManagedService::Gcc => {
                    log::warn!("ManagedService::Gcc (Google Cloud Composer) expansion not implemented; skipping");
                }
            }
        }
        let total = self.servers.as_ref().map_or(0, std::vec::Vec::len);
        info!(
            "Expanded config: servers={total}, errors={}",
            all_errors.len()
        );
        Ok((self, all_errors))
    }

    pub fn to_str(&self) -> Result<String> {
        toml::to_string(self).map_err(std::convert::Into::into)
    }

    pub fn write_to_file(&mut self) -> Result<()> {
        let path = self
            .path
            .clone()
            .unwrap_or(CONFIG_FILE.as_path().to_path_buf());
        
        // Set restrictive file permissions on Unix systems (0600 = rw-------)
        #[cfg(unix)]
        let mut file = {
            use std::os::unix::fs::OpenOptionsExt;
            OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(true)
                .create(true)
                .mode(0o600)
                .open(&path)?
        };
        
        #[cfg(not(unix))]
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)?;

        // Only write non-managed servers to the config file
        if let Some(servers) = &mut self.servers {
            *servers = servers
                .iter()
                .filter(|server| server.managed.is_none())
                .cloned()
                .collect();
        }
        file.write_all(Self::to_str(self)?.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CONFIG: &str = r#"[[servers]]
        name = "test"
        endpoint = "http://localhost:8080"

        [servers.auth.Basic]
        username = "airflow"
        password = "airflow"
        "#;

    #[test]
    fn test_get_config() {
        let result = FlowrsConfig::from_str(TEST_CONFIG).unwrap();
        let servers = result.servers.unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "test");
    }

    const TEST_CONFIG_CONVEYOR: &str = r#"
managed_services = ["Conveyor"]
show_init_screen = true

[[servers]]
name = "bla"
endpoint = "http://localhost:8080"
version = "V2"

[servers.auth.Basic]
username = "airflow"
password = "airflow"
    "#;
    #[test]
    fn test_get_config_conveyor() {
        let result = FlowrsConfig::from_str(TEST_CONFIG_CONVEYOR.trim()).unwrap();
        let services = result.managed_services.unwrap();
        assert_eq!(services.len(), 1);
        assert_eq!(services[0], ManagedService::Conveyor);
    }

    #[test]
    fn test_write_config_conveyor() {
        let config = FlowrsConfig {
            servers: Some(vec![AirflowConfig {
                name: "bla".to_string(),
                endpoint: "http://localhost:8080".to_string(),
                auth: AirflowAuth::Basic(BasicAuth {
                    username: "airflow".to_string(),
                    password: "airflow".to_string(),
                }),
                managed: None,
                version: AirflowVersion::V2,
                proxy: None,
            }]),
            managed_services: Some(vec![ManagedService::Conveyor]),
            show_init_screen: true,
            timezone_offset: "+00:00".to_string(),
            path: None,
        };

        let serialized_config = config.to_str().unwrap();
        assert_eq!(serialized_config.trim(), TEST_CONFIG_CONVEYOR.trim());
    }

    #[test]
    fn non_existing_path() {
        let path = PathBuf::from("non-existing.toml");
        let config = FlowrsConfig::from_file(Some(&path));
        assert!(config.is_ok());

        let config = config.unwrap();
        assert!(config.path.is_some());
    }

    #[test]
    fn none_path() {
        let config = FlowrsConfig::from_file(None);
        assert!(config.is_ok());

        let config = config.unwrap();
        assert_eq!(config.path.unwrap(), CONFIG_FILE.as_path().to_path_buf());
    }

    const TEST_CONFIG_WITH_PROXY: &str = r#"[[servers]]
name = "test-proxy"
endpoint = "http://localhost:8080"
proxy = "http://proxy.example.com:8080"

[servers.auth.Basic]
username = "airflow"
password = "airflow"
"#;

    #[test]
    fn test_config_with_proxy() {
        let result = FlowrsConfig::from_str(TEST_CONFIG_WITH_PROXY).unwrap();
        let servers = result.servers.unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "test-proxy");
        assert_eq!(servers[0].proxy, Some("http://proxy.example.com:8080".to_string()));
    }

    #[test]
    fn test_config_with_proxy_env_var() {
        let config_str = r#"[[servers]]
name = "test-proxy-env"
endpoint = "http://localhost:8080"
proxy = "${PROXY_URL}"

[servers.auth.Basic]
username = "airflow"
password = "airflow"
"#;
        let result = FlowrsConfig::from_str(config_str).unwrap();
        let servers = result.servers.unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].proxy, Some("${PROXY_URL}".to_string()));
    }

    #[test]
    fn test_serialize_config_with_proxy() {
        let config = FlowrsConfig {
            servers: Some(vec![AirflowConfig {
                name: "test-proxy".to_string(),
                endpoint: "http://localhost:8080".to_string(),
                auth: AirflowAuth::Basic(BasicAuth {
                    username: "airflow".to_string(),
                    password: "airflow".to_string(),
                }),
                managed: None,
                version: AirflowVersion::V2,
                proxy: Some("http://proxy.example.com:8080".to_string()),
            }]),
            managed_services: None,
            show_init_screen: true,
            timezone_offset: "+00:00".to_string(),
            path: None,
        };

        let serialized = config.to_str().unwrap();
        assert!(serialized.contains("proxy = \"http://proxy.example.com:8080\""));
    }

    #[test]
    fn test_config_without_proxy() {
        let result = FlowrsConfig::from_str(TEST_CONFIG).unwrap();
        let servers = result.servers.unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].proxy, None);
    }

    #[test]
    fn test_multiple_servers_different_auth() {
        let config_str = r#"
[[servers]]
name = "server-one"
endpoint = "http://airflow1.example.com:8080"
version = "V2"

[servers.auth.Basic]
username = "user1"
password = "${PASSWORD_1}"

[[servers]]
name = "server-two"
endpoint = "http://airflow2.example.com:8080"
version = "V3"

[servers.auth.Basic]
username = "user2"
password = "${PASSWORD_2}"
"#;

        let config = FlowrsConfig::from_str(config_str).unwrap();
        let servers = config.servers.unwrap();
        
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].name, "server-one");
        assert_eq!(servers[1].name, "server-two");
        
        // Check that each has different auth
        match &servers[0].auth {
            AirflowAuth::Basic(auth) => {
                assert_eq!(auth.username, "user1");
                assert_eq!(auth.password, "${PASSWORD_1}");
            },
            _ => panic!("Expected Basic auth for server-one"),
        }
        
        match &servers[1].auth {
            AirflowAuth::Basic(auth) => {
                assert_eq!(auth.username, "user2");
                assert_eq!(auth.password, "${PASSWORD_2}");
            },
            _ => panic!("Expected Basic auth for server-two"),
        }
    }
}
