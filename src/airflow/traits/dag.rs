use anyhow::Result;
use async_trait::async_trait;

use crate::airflow::model::common::{Dag, DagList};

/// Trait for DAG operations
#[async_trait]
pub trait DagOperations: Send + Sync {
    /// List all DAGs
    /// 
    /// Returns only DAGs with is_active=true (valid/schedulable DAGs).
    /// Paused vs unpaused filtering happens in the UI layer.
    async fn list_dags(&self) -> Result<DagList>;

    /// List DAGs with pagination support
    /// 
    /// Returns only DAGs with is_active=true (valid/schedulable DAGs).
    /// Paused vs unpaused filtering happens in the UI layer.
    /// 
    /// # Arguments
    /// * `offset` - Number of DAGs to skip
    /// * `limit` - Maximum number of DAGs to return
    async fn list_dags_paginated(&self, offset: i64, limit: i64) -> Result<DagList>;

    /// Toggle a DAG's paused state
    async fn toggle_dag(&self, dag_id: &str, is_paused: bool) -> Result<()>;

    /// Get DAG source code (uses `file_token` in v1, `dag_id` in v2)
    async fn get_dag_code(&self, dag: &Dag) -> Result<String>;

    /// Get detailed DAG information including doc_md
    async fn get_dag_details(&self, dag_id: &str) -> Result<Dag>;
}
