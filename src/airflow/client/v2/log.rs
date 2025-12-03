use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use reqwest::Method;

use crate::airflow::{model::common::Log, traits::LogOperations};
use super::model;

use super::V2Client;

#[async_trait]
impl LogOperations for V2Client {
    async fn get_task_logs(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u16,
    ) -> Result<Log> {
        let response = self
            .base_api(
                Method::GET,
                &format!(
                    "dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try}"
                ),
            )?
            .query(&[("full_content", "true")])
            .header("Accept", "application/json")
            .send()
            .await?
            .error_for_status()?;

        debug!("Response: {response:?}");
        let log = response.json::<model::log::Log>().await?;
        debug!("Parsed Log: {log:?}");
        Ok(log.into())
    }

    async fn get_task_logs_paginated(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u16,
        continuation_token: Option<&str>,
    ) -> Result<Log> {
        let mut request = self
            .base_api(
                Method::GET,
                &format!(
                    "dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try}"
                ),
            )?
            .header("Accept", "application/json");
        
        // Do NOT set full_content=true - we want chunks
        
        if let Some(token) = continuation_token {
            request = request.query(&[("token", token)]);
        }
        
        let response = request.send().await?.error_for_status()?;
        debug!("Paginated Response: {response:?}");
        let log = response.json::<model::log::Log>().await?;
        debug!("Parsed Paginated Log: {log:?}");
        Ok(log.into())
    }
}
