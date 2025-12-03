use anyhow::Result;
use async_trait::async_trait;
use log::{debug, info};
use reqwest::Method;

use super::model;
use crate::airflow::{model::common::DagList, traits::DagOperations};

use super::V2Client;

#[async_trait]
impl DagOperations for V2Client {
    async fn list_dags_paginated(&self, offset: i64, limit: i64) -> Result<DagList> {
        debug!("list_dags_paginated called with offset={}, limit={}", offset, limit);
        
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

        let page: model::dag::DagList = response.json().await?;
        
        let total_entries = page.total_entries;
        let fetched_count = page.dags.len();
        
        debug!("Fetched {} DAGs at offset {}, total in system: {}", fetched_count, offset, total_entries);
        
        Ok(DagList { 
            dags: page.dags.into_iter().map(|d| d.into()).collect(),
            total_entries,
        })
    }

    async fn list_dags(&self) -> Result<DagList> {
        // Fetch all active DAGs using pagination
        // Returns only DAGs with is_active=true (valid/schedulable DAGs)
        // Paused vs unpaused filtering happens in the UI layer
        debug!("list_dags called, fetching all active DAGs with pagination");
        
        let mut all_dags = Vec::new();
        let mut offset = 0;
        let limit = 100; // API seems to have a max limit of 100
        let mut total_entries = 0;
        
        loop {
            let page = self.list_dags_paginated(offset, limit).await?;
            
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
        
        info!("Active DAGs fetched: {} out of {} total", all_dags.len(), total_entries);
        
        Ok(DagList { 
            dags: all_dags,
            total_entries,
        })
    }

    async fn toggle_dag(&self, dag_id: &str, is_paused: bool) -> Result<()> {
        self
            .base_api(Method::PATCH, &format!("dags/{dag_id}"))?
            .json(&serde_json::json!({"is_paused": !is_paused}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn get_dag_code(&self, dag: &crate::airflow::model::common::Dag) -> Result<String> {
        let r = self
            .base_api(Method::GET, &format!("dagSources/{}", dag.dag_id))?
            .build()?;
        let response = self.base.client.execute(r).await?.error_for_status()?;
        let dag_source: model::dag::DagSource = response.json().await?;
        Ok(dag_source.content)
    }

    async fn get_dag_details(&self, dag_id: &str) -> Result<crate::airflow::model::common::Dag> {
        let r = self
            .base_api(Method::GET, &format!("dags/{}/details", dag_id))?
            .build()?;
        let response = self.base.client.execute(r).await?.error_for_status()?;
        
        response
            .json::<model::dag::Dag>()
            .await
            .map(std::convert::Into::into)
            .map_err(std::convert::Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::airflow::{
        client::base::BaseClient,
        config::AirflowVersion,
        managed_services::conveyor::get_conveyor_environment_servers,
    };

    fn get_test_client() -> V2Client {
        let servers = get_conveyor_environment_servers().unwrap();
        let server = servers
            .into_iter()
            .find(|s| s.version == AirflowVersion::V3)
            .unwrap();
        let base = BaseClient::new(server).unwrap();
        V2Client::new(base)
    }

    #[tokio::test]
    // TODO: use a docker-compose Airflow v3 setup for testing instead
    async fn test_list_dags() {
        // Skip test if CONVEYOR_TESTS is not set to avoid panicking when credentials are unavailable
        if std::env::var("CONVEYOR_TESTS").unwrap_or_default() != "true" {
            eprintln!("Skipping test_list_dags: Set CONVEYOR_TESTS=true to run Conveyor integration tests");
            return;
        }

        let client = get_test_client();
        let daglist: DagList = client.list_dags(false).await.unwrap();
        assert!(!daglist.dags.is_empty());
    }

    #[tokio::test]
    async fn test_get_dag_code() {
        // Skip test if CONVEYOR_TESTS is not set to avoid panicking when credentials are unavailable
        if std::env::var("CONVEYOR_TESTS").unwrap_or_default() != "true" {
            eprintln!("Skipping test_get_dag_code: Set CONVEYOR_TESTS=true to run Conveyor integration tests");
            return;
        }

        let client = get_test_client();
        let daglist: DagList = client.list_dags(false).await.unwrap();
        let dag = &daglist.dags[0];
        let code = client.get_dag_code(dag).await.unwrap();
        assert!(code.contains(&dag.dag_id));
    }
}
