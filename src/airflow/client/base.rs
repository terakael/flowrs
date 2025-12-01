use anyhow::{Context, Result};
use log::{debug, info};
use reqwest::{Method, Url};
use std::convert::TryFrom;
use std::time::Duration;

use crate::airflow::config::{AirflowAuth, AirflowConfig};
use crate::airflow::managed_services::conveyor::ConveyorClient;

/// Base HTTP client for Airflow API communication.
/// Handles authentication and provides base request building functionality.
#[derive(Debug, Clone)]
pub struct BaseClient {
    pub client: reqwest::Client,
    pub config: AirflowConfig,
}

impl BaseClient {
    pub fn new(config: AirflowConfig) -> Result<Self> {
        let mut client_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .use_rustls_tls();
        
        // Configure proxy if specified in config (takes priority)
        if let Some(proxy_url) = &config.proxy {
            let proxy_url = crate::airflow::config::expand_env_vars(proxy_url)?;
            let proxy = reqwest::Proxy::all(&proxy_url)
                .with_context(|| format!("Invalid proxy URL: {}", proxy_url))?;
            client_builder = client_builder.proxy(proxy);
            info!("🔀 Using proxy from config: {}", proxy_url);
        } else {
            // Fall back to standard environment variables if no config proxy
            if let Ok(http_proxy) = std::env::var("HTTP_PROXY").or_else(|_| std::env::var("http_proxy")) {
                let proxy = reqwest::Proxy::http(&http_proxy)
                    .with_context(|| format!("Invalid HTTP_PROXY: {}", http_proxy))?;
                client_builder = client_builder.proxy(proxy);
                info!("🔀 Using proxy from HTTP_PROXY: {}", http_proxy);
            }
            if let Ok(https_proxy) = std::env::var("HTTPS_PROXY").or_else(|_| std::env::var("https_proxy")) {
                let proxy = reqwest::Proxy::https(&https_proxy)
                    .with_context(|| format!("Invalid HTTPS_PROXY: {}", https_proxy))?;
                client_builder = client_builder.proxy(proxy);
                info!("🔀 Using proxy from HTTPS_PROXY: {}", https_proxy);
            }
        }
        
        let client = client_builder.build()?;
        Ok(Self { client, config })
    }

    /// Build a base request with authentication for the specified API version
    pub fn base_api(
        &self,
        method: Method,
        endpoint: &str,
        api_version: &str,
    ) -> Result<reqwest::RequestBuilder> {
        // Ensure base URL ends with a trailing slash for proper path joining
        let mut base_endpoint = self.config.endpoint.clone();
        if !base_endpoint.ends_with('/') {
            base_endpoint.push('/');
        }
        
        let base_url = Url::parse(&base_endpoint)?;
        let url = base_url.join(format!("{api_version}/{endpoint}").as_str())?;
        debug!("🔗 Request URL: {url}");

        match &self.config.auth {
            AirflowAuth::Basic(auth) => {
                let username = crate::airflow::config::expand_env_vars(&auth.username)?;
                let password = crate::airflow::config::expand_env_vars(&auth.password)?;
                info!("🔑 Basic Auth: {}", username);
                Ok(self
                    .client
                    .request(method, url)
                    .basic_auth(&username, Some(&password)))
            }
            AirflowAuth::Token(token) => {
                info!("🔑 Token Auth: {:?}", token.cmd);
                if let Some(cmd) = &token.cmd {
                    let output = std::process::Command::new("sh")
                        .arg("-c")
                        .arg(cmd)
                        .output()
                        .context("Failed to run token helper command")?;

                    if !output.status.success() {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        return Err(anyhow::anyhow!(
                            "Token helper command failed with exit code {:?}\nstdout: {}\nstderr: {}",
                            output.status.code(),
                            stdout,
                            stderr
                        ));
                    }

                    let token = String::from_utf8(output.stdout)
                        .context("Token helper returned invalid UTF-8")?
                        .trim()
                        .replace('"', "");
                    Ok(self.client.request(method, url).bearer_auth(token))
                } else {
                    if let Some(token) = &token.token {
                        let expanded_token = crate::airflow::config::expand_env_vars(token.trim())?;
                        return Ok(self.client.request(method, url).bearer_auth(expanded_token));
                    }
                    Err(anyhow::anyhow!("Token not found"))
                }
            }
            AirflowAuth::Conveyor => {
                info!("🔑 Conveyor Auth");
                let token: String = ConveyorClient::get_token()?;
                Ok(self.client.request(method, url).bearer_auth(token))
            }
            AirflowAuth::Mwaa(auth) => {
                info!("🔑 MWAA Auth: {}", auth.environment_name);
                Ok(self
                    .client
                    .request(method, url)
                    .header("Cookie", format!("session={}", auth.session_cookie)))
            }
            AirflowAuth::Astronomer(auth) => {
                info!("🔑 Astronomer Auth");
                Ok(self
                    .client
                    .request(method, url)
                    .bearer_auth(&auth.api_token))
            }
        }
    }
}

impl TryFrom<&AirflowConfig> for BaseClient {
    type Error = anyhow::Error;

    fn try_from(config: &AirflowConfig) -> Result<Self, Self::Error> {
        Self::new(config.clone())
    }
}
