use anyhow::Result;
use async_trait::async_trait;

use crate::airflow::model::common::{Variable, VariableCollection};

#[async_trait]
pub trait VariableOperations: Send + Sync {
    async fn list_variables(&self) -> Result<VariableCollection>;
    async fn get_variable(&self, key: &str) -> Result<Variable>;
}
