use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use reqwest::Method;

use crate::airflow::{
    model::common::{Variable, VariableCollection},
    traits::VariableOperations,
};

use super::model::variable::{VariableCollectionResponse, VariableResponse};
use super::V1Client;

#[async_trait]
impl VariableOperations for V1Client {
    async fn list_variables(&self) -> Result<VariableCollection> {
        debug!("list_variables called");
        
        let response = self
            .base_api(Method::GET, "variables")?
            .query(&[("limit", "1000")]) // Get up to 1000 variables
            .send()
            .await?
            .error_for_status()?;

        let response_text = response.text().await?;
        
        let collection: VariableCollectionResponse = match serde_json::from_str(&response_text) {
            Ok(collection) => collection,
            Err(e) => {
                log::error!("Failed to decode variable list response. Error: {}", e);
                log::error!("Response body (first 500 chars): {}", &response_text.chars().take(500).collect::<String>());
                return Err(anyhow::anyhow!("Failed to decode response: {}. Check debug log for response body.", e));
            }
        };
        
        debug!("Fetched {} variables", collection.variables.len());
        
        Ok(VariableCollection {
            variables: collection.variables.into_iter().map(|v| v.into()).collect(),
            total_entries: collection.total_entries,
        })
    }

    async fn get_variable(&self, key: &str) -> Result<Variable> {
        debug!("get_variable called for key: {}", key);
        
        let response = self
            .base_api(Method::GET, &format!("variables/{}", key))?
            .send()
            .await?
            .error_for_status()?;

        let response_text = response.text().await?;
        
        let variable: VariableResponse = match serde_json::from_str(&response_text) {
            Ok(variable) => variable,
            Err(e) => {
                log::error!("Failed to decode variable response. Error: {}", e);
                log::error!("Response body (first 500 chars): {}", &response_text.chars().take(500).collect::<String>());
                return Err(anyhow::anyhow!("Failed to decode response: {}. Check debug log for response body.", e));
            }
        };
        
        debug!("Fetched variable: {}", key);
        
        Ok(variable.into())
    }
}
