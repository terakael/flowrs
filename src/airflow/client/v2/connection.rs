use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use reqwest::Method;

use crate::airflow::{
    model::common::{Connection, ConnectionCollection},
    traits::ConnectionOperations,
};

use super::model::connection::{ConnectionCollectionResponse, ConnectionResponse};
use super::V2Client;

#[async_trait]
impl ConnectionOperations for V2Client {
    async fn list_connections(&self) -> Result<ConnectionCollection> {
        debug!("list_connections called");
        
        let response = self
            .base_api(Method::GET, "connections")?
            .query(&[("limit", "1000")]) // Get up to 1000 connections
            .send()
            .await?
            .error_for_status()?;

        let response_text = response.text().await?;
        
        let collection: ConnectionCollectionResponse = match serde_json::from_str(&response_text) {
            Ok(collection) => collection,
            Err(e) => {
                log::error!("Failed to decode connection list response. Error: {}", e);
                log::error!("Response body (first 500 chars): {}", &response_text.chars().take(500).collect::<String>());
                return Err(anyhow::anyhow!("Failed to decode response: {}. Check debug log for response body.", e));
            }
        };
        
        debug!("Fetched {} connections", collection.connections.len());
        
        Ok(ConnectionCollection {
            connections: collection.connections.into_iter().map(|c| c.into()).collect(),
            total_entries: collection.total_entries,
        })
    }

    async fn get_connection(&self, connection_id: &str) -> Result<Connection> {
        debug!("get_connection called for connection_id: {}", connection_id);
        
        let response = self
            .base_api(Method::GET, &format!("connections/{}", connection_id))?
            .send()
            .await?
            .error_for_status()?;

        let response_text = response.text().await?;
        
        let connection: ConnectionResponse = match serde_json::from_str(&response_text) {
            Ok(connection) => connection,
            Err(e) => {
                log::error!("Failed to decode connection response. Error: {}", e);
                log::error!("Response body (first 500 chars): {}", &response_text.chars().take(500).collect::<String>());
                return Err(anyhow::anyhow!("Failed to decode response: {}. Check debug log for response body.", e));
            }
        };
        
        debug!("Fetched connection: {}", connection_id);
        
        Ok(connection.into())
    }
}
