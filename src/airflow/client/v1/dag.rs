use anyhow::Result;
use async_trait::async_trait;
use log::info;
use reqwest::Method;

use crate::airflow::{
    model::common::DagList,
    traits::DagOperations,
};

use super::model::dag::{DagCollectionResponse, DagResponse};

use super::V1Client;

#[async_trait]
impl DagOperations for V1Client {
    async fn list_dags(&self) -> Result<DagList> {
        let r = self.base_api(Method::GET, "dags")?.build()?;
        let response = self.base.client.execute(r).await?.error_for_status()?;

        // Try to get the response text first for better error messages
        let response_text = response.text().await?;
        
        match serde_json::from_str::<DagCollectionResponse>(&response_text) {
            Ok(daglist) => {
                info!("DAGs: {daglist:?}");
                Ok(daglist.into())
            }
            Err(e) => {
                log::error!("Failed to decode DAG list response. Error: {}", e);
                log::error!("Response body (first 500 chars): {}", &response_text.chars().take(500).collect::<String>());
                Err(anyhow::anyhow!("Failed to decode response: {}. Check debug log for response body.", e))
            }
        }
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
        let daglist: DagList = client.list_dags().await.unwrap();
        assert_eq!(daglist.dags[0].owners, vec!["airflow"]);
    }

    #[tokio::test]
    async fn test_get_dag_code() {
        let client = get_test_client();
        let dag = client.list_dags().await.unwrap().dags[0].clone();
        let code = client.get_dag_code(&dag).await.unwrap();
        assert!(code.contains("with DAG"));
    }
}
