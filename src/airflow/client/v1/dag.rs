use anyhow::Result;
use async_trait::async_trait;
use log::{debug, info};
use reqwest::Method;

use crate::airflow::{
    model::common::DagList,
    traits::DagOperations,
};

use super::model::dag::{DagCollectionResponse, DagResponse};

use super::V1Client;

#[async_trait]
impl DagOperations for V1Client {
    async fn list_dags_paginated(&self, offset: i64, limit: i64, only_active: bool) -> Result<DagList> {
        debug!("list_dags_paginated called with offset={}, limit={}, only_active={}", offset, limit, only_active);
        
        let response = self
            .base_api(Method::GET, "dags")?
            .query(&[
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
                ("order_by", "dag_id".to_string()),
                ("only_active", "true".to_string())  // Always fetch only is_active=true DAGs
            ])
            .send()
            .await?
            .error_for_status()?;

        // Try to get the response text first for better error messages
        let response_text = response.text().await?;
        
        let page: DagCollectionResponse = match serde_json::from_str(&response_text) {
            Ok(page) => page,
            Err(e) => {
                log::error!("Failed to decode DAG list response. Error: {}", e);
                log::error!("Response body (first 500 chars): {}", &response_text.chars().take(500).collect::<String>());
                return Err(anyhow::anyhow!("Failed to decode response: {}. Check debug log for response body.", e));
            }
        };
        
        let total_entries = page.total_entries;
        let fetched_count = page.dags.len();
        
        debug!("Fetched {} DAGs at offset {}, total in system: {}", fetched_count, offset, total_entries);
        
        Ok(DagList { 
            dags: page.dags.into_iter().map(|d| d.into()).collect(),
            total_entries,
        })
    }

    async fn list_dags(&self, only_active: bool) -> Result<DagList> {
        // Fetch all DAGs using pagination
        // Note: only_active filters by is_active (scheduler visibility), not is_paused
        // Since we want to filter by pause state, we fetch all DAGs and filter locally
        debug!("list_dags called with only_active={}, fetching all DAGs with pagination", only_active);
        
        let mut all_dags = Vec::new();
        let mut offset = 0;
        let limit = 100; // API seems to have a max limit of 100
        let mut total_entries = 0;
        
        loop {
            let page = self.list_dags_paginated(offset, limit, only_active).await?;
            
            total_entries = page.total_entries;
            let fetched_count = page.dags.len();
            all_dags.extend(page.dags);
            
            debug!("Fetched {} DAGs at offset {}, total so far: {}/{}", fetched_count, offset, all_dags.len(), total_entries);
            
            // Break if we've fetched all DAGs or got fewer than limit (last page)
            if all_dags.len() >= total_entries as usize || fetched_count < limit as usize {
                break;
            }
            
            offset += limit;
        }
        
        info!("DAGs fetched: {} out of {} total (only_active: {})", all_dags.len(), total_entries, only_active);
        
        Ok(DagList { 
            dags: all_dags,
            total_entries,
        })
    }

    async fn toggle_dag(&self, dag_id: &str, is_paused: bool) -> Result<()> {
        self
            .base_api(Method::PATCH, &format!("dags/{dag_id}"))?
            .query(&[("update_mask", "is_paused")])
            .json(&serde_json::json!({"is_paused": !is_paused}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn get_dag_code(&self, dag: &crate::airflow::model::common::Dag) -> Result<String> {
        let r = self
            .base_api(Method::GET, &format!("dagSources/{}", dag.file_token))?
            .build()?;
        let response = self.base.client.execute(r).await?.error_for_status()?;
        let code = response.text().await?;
        Ok(code)
    }

    async fn get_dag_details(&self, dag_id: &str) -> Result<crate::airflow::model::common::Dag> {
        let r = self
            .base_api(Method::GET, &format!("dags/{}/details", dag_id))?
            .build()?;
        let response = self.base.client.execute(r).await?.error_for_status()?;

        // Try to get the response text first for better error messages
        let response_text = response.text().await?;
        
        match serde_json::from_str::<DagResponse>(&response_text) {
            Ok(dag_response) => {
                info!("DAG details fetched: {dag_id}");
                Ok(dag_response.into())
            }
            Err(e) => {
                log::error!("Failed to decode DAG details response. Error: {}", e);
                log::error!("Response body (first 500 chars): {}", &response_text.chars().take(500).collect::<String>());
                Err(anyhow::anyhow!("Failed to decode DAG details: {}. Check debug log for response body.", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::airflow::client::base::BaseClient;

    const TEST_CONFIG: &str = r#"[[servers]]
        name = "test"
        endpoint = "http://localhost:8080"

        [servers.auth.Basic]
        username = "airflow"
        password = "airflow"
        "#;

    fn get_test_client() -> V1Client {
        let config: crate::airflow::config::FlowrsConfig =
            toml::from_str(TEST_CONFIG.trim()).unwrap();
        let base = BaseClient::new(config.servers.unwrap()[0].clone()).unwrap();
        V1Client::new(base)
    }

    #[tokio::test]
    async fn test_list_dags() {
        let client = get_test_client();
        let daglist: DagList = client.list_dags(false).await.unwrap();
        assert_eq!(daglist.dags[0].owners, vec!["airflow"]);
    }

    #[tokio::test]
    async fn test_get_dag_code() {
        let client = get_test_client();
        let dag = client.list_dags(false).await.unwrap().dags[0].clone();
        let code = client.get_dag_code(&dag).await.unwrap();
        assert!(code.contains("with DAG"));
    }
}
