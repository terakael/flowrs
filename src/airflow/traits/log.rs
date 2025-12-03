use anyhow::Result;
use async_trait::async_trait;

use crate::airflow::model::common::Log;

/// Trait for Log operations
#[async_trait]
pub trait LogOperations: Send + Sync {
    /// Get task logs for a specific task instance and try number
    async fn get_task_logs(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u16,
    ) -> Result<Log>;

    /// Get task logs with optional continuation token for pagination
    /// When continuation_token is None, fetches the first chunk without full_content=true
    /// When continuation_token is Some, fetches the next chunk from that position
    async fn get_task_logs_paginated(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u16,
        continuation_token: Option<&str>,
    ) -> Result<Log>;
}
