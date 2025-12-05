use crate::airflow::config::{AirflowAuth, AirflowConfig, AirflowVersion, ManagedService};
use anyhow::{Context, Result};
use gcp_auth::TokenProvider;
use log::info;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Creates a detailed error message for GCP session expiration issues
///
/// This helper provides consistent, actionable guidance when GCP authentication fails,
/// typically due to Google Workspace session control policies requiring periodic reauthentication.
fn create_session_expired_error(context: &str, original_error: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!(
        "{}\n\
        \n\
        This usually happens when your Google Workspace session has expired.\n\
        Your organization's administrator has configured session length policies\n\
        that require periodic reauthentication.\n\
        \n\
        To fix this issue, try one of the following:\n\
        \n\
        1. Re-authenticate (recommended for local development):\n\
           gcloud auth application-default login\n\
        \n\
        2. Use a service account (recommended for production/frequent use):\n\
           - Request a service account key from your GCP administrator\n\
           - Set: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json\n\
           - Service accounts are not subject to session expiration\n\
        \n\
        3. Request longer session duration from your Google Workspace admin:\n\
           - Ask them to increase the session length in Google Workspace settings\n\
           - This may still require daily login depending on policy\n\
        \n\
        Original error: {}", context, original_error
    )
}

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
    ///
    /// # Session Expiration
    /// When using user credentials (from `gcloud auth application-default login`),
    /// your Google Workspace administrator may have configured session length policies
    /// that require periodic reauthentication (typically daily). This is expected
    /// security behavior for corporate accounts.
    pub async fn new() -> Result<Self> {
        let token_provider = gcp_auth::provider()
            .await
            .map_err(|e| create_session_expired_error("Failed to create GCP token provider.", e))?;

        Ok(Self {
            token_provider,
        })
    }

    /// Creates a new Composer client using a service account keyfile
    /// This is the recommended approach for production use as it avoids session expiration issues.
    ///
    /// # Arguments
    /// * `keyfile_path` - Path to the service account JSON keyfile
    pub async fn from_keyfile(keyfile_path: &str) -> Result<Self> {
        let expanded_path = crate::airflow::config::expand_env_vars(keyfile_path)?;
        let token_provider = gcp_auth::CustomServiceAccount::from_file(&expanded_path)
            .with_context(|| format!("Failed to load service account from keyfile: {}", expanded_path))?;

        Ok(Self {
            token_provider: Arc::new(token_provider),
        })
    }

    /// Gets a fresh access token for authenticating to Cloud Composer
    /// The token is automatically refreshed if expired
    ///
    /// # Session Expiration
    /// If your Google Workspace session has expired, this will fail and require
    /// reauthentication. This typically happens daily for corporate accounts with
    /// session control policies enabled.
    pub async fn get_token(&self) -> Result<String> {
        // Get token with cloud-platform scope
        let scopes = &["https://www.googleapis.com/auth/cloud-platform"];
        let token = self.token_provider
            .token(scopes)
            .await
            .map_err(|e| {
                // TODO: Inspect error type/message to distinguish session expiration from other failures
                // (e.g., network issues, permission problems, malformed credentials).
                // The gcp_auth crate doesn't expose structured error types, so we'd need to parse
                // error messages, which is brittle. For now, we assume session expiration as it's
                // the most common case for corporate users using ADC.
                // Consider contributing to gcp_auth to expose structured error types if this becomes
                // a frequent issue. With keyfile auth, session expiration is not a concern.
                create_session_expired_error("Your GCP session has expired.", e)
            })?;

        Ok(token.as_str().to_string())
    }
}

/// Composer authentication data including the client for token refresh
/// The client is lazily initialized on first use using OnceCell for interior mutability
#[derive(Clone)]
pub struct ComposerAuth {
    pub client: Arc<OnceCell<ComposerClient>>,
    /// Optional path to service account keyfile (if not using ADC)
    pub keyfile_path: Option<String>,
}

impl fmt::Debug for ComposerAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ComposerAuth")
            .field("client", &"<OnceCell<ComposerClient>>")
            .field("keyfile_path", &self.keyfile_path)
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
            keyfile_path: None,
        }
    }

    /// Creates a new ComposerAuth with deferred initialization using ADC
    /// The client will be created on first use with Application Default Credentials
    pub fn new_deferred() -> Self {
        Self {
            client: Arc::new(OnceCell::new()),
            keyfile_path: None,
        }
    }

    /// Creates a new ComposerAuth from a keyfile path
    /// The client will be created on first use with the specified keyfile
    pub fn from_keyfile(keyfile_path: String) -> Self {
        Self {
            client: Arc::new(OnceCell::new()),
            keyfile_path: Some(keyfile_path),
        }
    }

    /// Checks if this auth uses a keyfile path (vs ADC)
    pub fn uses_keyfile(&self) -> bool {
        self.keyfile_path.is_some()
    }

    /// Gets the client, initializing it if necessary
    ///
    /// # Lazy Initialization
    /// The client is created on first use to avoid authentication during config
    /// deserialization. If credentials have expired since the last use, this will
    /// fail and require reauthentication.
    pub async fn get_client(&self) -> Result<&ComposerClient> {
        self.client
            .get_or_try_init(|| async {
                if let Some(keyfile_path) = &self.keyfile_path {
                    ComposerClient::from_keyfile(keyfile_path).await
                } else {
                    ComposerClient::new().await
                }
            })
            .await
    }
}

// Custom serialization/deserialization for ComposerAuth
// We serialize the keyfile path if present, and recreate the client on demand
impl Serialize for ComposerAuth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        // Field count: keyfile_path (1 field)
        const SERIALIZED_FIELD_COUNT: usize = 1;
        let mut state = serializer.serialize_struct("ComposerAuth", SERIALIZED_FIELD_COUNT)?;
        state.serialize_field("keyfile_path", &self.keyfile_path)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ComposerAuth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{MapAccess, Visitor};
        
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            KeyfilePath,
        }
        
        struct ComposerAuthVisitor;
        
        impl<'de> Visitor<'de> for ComposerAuthVisitor {
            type Value = ComposerAuth;
            
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ComposerAuth")
            }
            
            fn visit_map<V>(self, mut map: V) -> Result<ComposerAuth, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut keyfile_path = None;
                
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::KeyfilePath => {
                            keyfile_path = map.next_value()?;
                        }
                    }
                }
                
                // Don't create the client during deserialization - it will be created on first use
                Ok(ComposerAuth {
                    client: Arc::new(OnceCell::new()),
                    keyfile_path,
                })
            }
        }
        
        deserializer.deserialize_struct("ComposerAuth", &["keyfile_path"], ComposerAuthVisitor)
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
    keyfile_path: Option<String>,
) -> Result<AirflowConfig> {
    // Normalize the endpoint URL
    let normalized_endpoint = crate::airflow::config::normalize_endpoint(endpoint);

    // Both paths use lazy initialization for consistency
    // Authentication will be validated on first use rather than during config creation
    let auth = if let Some(keyfile) = keyfile_path {
        info!(
            "Created Composer configuration: {} ({}) with keyfile: {}",
            name, normalized_endpoint, keyfile
        );
        AirflowAuth::Composer(ComposerAuth::from_keyfile(keyfile))
    } else {
        info!(
            "Created Composer configuration: {} ({}) with ADC",
            name, normalized_endpoint
        );
        AirflowAuth::Composer(ComposerAuth::new_deferred())
    };

    Ok(AirflowConfig {
        name,
        endpoint: normalized_endpoint,
        auth,
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
            None,
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
            None,
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
            None,
        )
        .await;

        if let Ok(config) = result2 {
            assert!(config.endpoint.ends_with('/'));
            assert_eq!(config.version, AirflowVersion::V3);
        }
    }
}
