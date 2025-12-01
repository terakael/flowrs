use anyhow::Result;
use async_trait::async_trait;
use log::info;
use reqwest::Method;

use super::model;
use crate::airflow::traits::TaskOperations;

use super::V2Client;

#[async_trait]
impl TaskOperations for V2Client {
    async fn list_tasks(&self, dag_id: &str) -> Result<Vec<(String, Vec<String>)>> {
        let response = self
            .base_api(Method::GET, &format!("dags/{dag_id}/tasks"))?
            .send()
            .await?
            .error_for_status()?;

        let task_collection: model::task::TaskCollection = response.json().await?;
        
        info!("Fetched {} tasks for DAG {}", task_collection.tasks.len(), dag_id);
        
        // Return (task_id, downstream_task_ids) pairs
        Ok(task_collection
            .tasks
            .into_iter()
            .map(|t| (t.task_id, t.downstream_task_ids))
            .collect())
    }
}
