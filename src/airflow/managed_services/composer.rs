use crate::airflow::config::{AirflowAuth, AirflowConfig, AirflowVersion, ManagedService};
use anyhow::{Context, Result};
use gcp_auth::TokenProvider;
use log::info;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Google Cloud Composer client for managing authentication
#[derive(Clone)]
pub struct ComposerClient {
    token_provider: Arc<dyn TokenProvider>,
}

impl fmt::Debug for ComposerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ComposerClient")
            .field("token_provider", &"<TokenProvider>")
            .finish()
    }
}

impl ComposerClient {
    /// Creates a new Composer client using Application Default Credentials (ADC)
    /// This supports:
    /// - Service account key files (via GOOGLE_APPLICATION_CREDENTIALS)
    /// - gcloud auth application-default login
    /// - Default service accounts on GCE/Cloud Run/Cloud Functions
    pub async fn new() -> Result<Self> {
        let token_provider = gcp_auth::provider()
            .await
            .context("Failed to create GCP token provider")?;

        Ok(Self {
            token_provider,
        })
    }

    /// Gets a fresh access token for authenticating to Cloud Composer
    /// The token is automatically refreshed if expired
    pub async fn get_token(&self) -> Result<String> {
        // Get token with cloud-platform scope
        let scopes = &["https://www.googleapis.com/auth/cloud-platform"];
        let token = self.token_provider
            .token(scopes)
            .await
            .context("Failed to get GCP access token")?;

        Ok(token.as_str().to_string())
    }
}

/// Composer authentication data including the client for token refresh
/// The client is lazily initialized on first use using OnceCell for interior mutability
#[derive(Clone)]
pub struct ComposerAuth {
    pub client: Arc<OnceCell<ComposerClient>>,
}

impl fmt::Debug for ComposerAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ComposerAuth")
            .field("client", &"<OnceCell<ComposerClient>>")
            .finish()
    }
}

impl ComposerAuth {
    /// Creates a new ComposerAuth with a client
    pub fn new(client: ComposerClient) -> Self {
        let cell = OnceCell::new();
        let _ = cell.set(client);
        Self {
            client: Arc::new(cell),
        }
    }

    /// Gets the client, initializing it if necessary
    pub async fn get_client(&self) -> Result<&ComposerClient> {
        self.client
            .get_or_try_init(|| async { ComposerClient::new().await })
            .await
    }
}

// Custom serialization/deserialization for ComposerAuth
// We serialize as an empty object and recreate the client on demand
impl Serialize for ComposerAuth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let state = serializer.serialize_struct("ComposerAuth", 0)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ComposerAuth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{MapAccess, Visitor};
        
        struct ComposerAuthVisitor;
        
        impl<'de> Visitor<'de> for ComposerAuthVisitor {
            type Value = ComposerAuth;
            
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ComposerAuth")
            }
            
            fn visit_map<V>(self, _map: V) -> Result<ComposerAuth, V::Error>
            where
                V: MapAccess<'de>,
            {
                // Don't create the client during deserialization - it will be created on first use
                Ok(ComposerAuth {
                    client: Arc::new(OnceCell::new()),
                })
            }
        }
        
        deserializer.deserialize_struct("ComposerAuth", &[], ComposerAuthVisitor)
    }
}

/// Composer environment metadata from manual configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComposerEnvironment {
    pub name: String,
    pub endpoint: String,
    pub airflow_version: AirflowVersion,
}

/// Creates a Composer environment configuration from user-provided details
pub async fn create_composer_config(
    name: String,
    endpoint: String,
    airflow_version: AirflowVersion,
) -> Result<AirflowConfig> {
    let client = ComposerClient::new().await?;

    // Normalize the endpoint URL
    let normalized_endpoint = crate::airflow::config::normalize_endpoint(endpoint);

    info!(
        "Created Composer configuration: {} ({})",
        name, normalized_endpoint
    );

    Ok(AirflowConfig {
        name,
        endpoint: normalized_endpoint,
        auth: AirflowAuth::Composer(ComposerAuth::new(client)),
        managed: Some(ManagedService::Gcc),
        version: airflow_version,
        proxy: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    
    // Initialize crypto provider once for all tests
    static INIT: Once = Once::new();
    
    fn init_crypto() {
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    #[tokio::test]
    async fn test_composer_client_new() {
        init_crypto();
        let result = ComposerClient::new().await;
        // This test will only work if you have GCP credentials configured
        match result {
            Ok(client) => {
                println!("✓ Successfully created ComposerClient");
                // Try to get a token
                match client.get_token().await {
                    Ok(token) => {
                        println!("✓ Successfully obtained token (length: {})", token.len());
                        assert!(!token.is_empty());
                    }
                    Err(e) => {
                        println!("⚠ Could not obtain token: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("⚠ Expected error if GCP credentials not configured: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_create_composer_config() {
        init_crypto();
        
        let result = create_composer_config(
            "test-composer".to_string(),
            "https://example-airflow-ui.composer.googleusercontent.com".to_string(),
            AirflowVersion::V2,
        )
        .await;

        match result {
            Ok(config) => {
                println!("✓ Successfully created Composer config");
                assert_eq!(config.name, "test-composer");
                assert!(config.endpoint.starts_with("https://"));
                assert!(config.endpoint.ends_with('/'));
                assert_eq!(config.version, AirflowVersion::V2);
                assert_eq!(config.managed, Some(ManagedService::Gcc));
            }
            Err(e) => {
                println!("⚠ Expected error if GCP credentials not configured: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_endpoint_normalization() {
        init_crypto();
        
        // Test without https://
        let result = create_composer_config(
            "test".to_string(),
            "example.composer.googleusercontent.com".to_string(),
            AirflowVersion::V2,
        )
        .await;

        if let Ok(config) = result {
            assert!(config.endpoint.starts_with("https://"));
            assert!(config.endpoint.ends_with('/'));
        }

        // Test without trailing slash
        let result2 = create_composer_config(
            "test2".to_string(),
            "https://example.composer.googleusercontent.com".to_string(),
            AirflowVersion::V3,
        )
        .await;

        if let Ok(config) = result2 {
            assert!(config.endpoint.ends_with('/'));
            assert_eq!(config.version, AirflowVersion::V3);
        }
    }
}
