use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait TaskOperations: Send + Sync {
    /// List all tasks for a DAG with their downstream dependencies
    /// Returns Vec<(task_id, downstream_task_ids)>
    async fn list_tasks(&self, dag_id: &str) -> Result<Vec<(String, Vec<String>)>>;
}
