use anyhow::Result;
use async_trait::async_trait;

use crate::airflow::model::common::{Connection, ConnectionCollection};

#[async_trait]
pub trait ConnectionOperations: Send + Sync {
    async fn list_connections(&self) -> Result<ConnectionCollection>;
    async fn get_connection(&self, connection_id: &str) -> Result<Connection>;
}
