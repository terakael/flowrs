pub mod model;

mod connection;
mod dag;
mod dagrun;
mod dagstats;
mod log;
mod task;
mod taskinstance;
mod variable;

use anyhow::Result;
use async_trait::async_trait;
use reqwest::Method;
use url::{form_urlencoded, Url};

use super::base::BaseClient;
use crate::airflow::{config::AirflowVersion, traits::AirflowClient};
use crate::app::worker::OpenItem;

/// API v1 client implementation (for Airflow v2, uses /api/v1 endpoint)
#[derive(Debug, Clone)]
pub struct V1Client {
    base: BaseClient,
}

impl V1Client {
    const API_VERSION: &'static str = "api/v1";

    pub fn new(base: BaseClient) -> Self {
        Self { base }
    }

    fn base_api(&self, method: Method, endpoint: &str) -> Result<reqwest::RequestBuilder> {
        self.base.base_api(method, endpoint, Self::API_VERSION)
    }
}

#[async_trait]
impl AirflowClient for V1Client {
    fn get_version(&self) -> AirflowVersion {
        AirflowVersion::V2
    }
    
    async fn get_import_error_count(&self) -> Result<usize> {
        let response = self
            .base_api(Method::GET, "importErrors")?
            .query(&[("limit", "1")])
            .send()
            .await?
            .error_for_status()?;
            
        let result: model::importerror::ImportErrorCollection = response.json().await?;
        Ok(result.total_entries as usize)
    }
    
    async fn list_import_errors(&self) -> Result<crate::airflow::model::common::ImportErrorList> {
        let response = self
            .base_api(Method::GET, "importErrors")?
            .send()
            .await?
            .error_for_status()?;
            
        let result: model::importerror::ImportErrorCollection = response.json().await?;
        Ok(result.into())
    }

    fn build_open_url(&self, item: &OpenItem) -> Result<String> {
        // Ensure base URL ends with a trailing slash for proper path joining
        let mut base_endpoint = self.base.config.endpoint.clone();
        if !base_endpoint.ends_with('/') {
            base_endpoint.push('/');
        }
        
        let mut base_url = Url::parse(&base_endpoint)?;

        match item {
            OpenItem::Config(config_endpoint) => {
                base_url = config_endpoint.parse()?;
            }
            OpenItem::Dag { dag_id } => {
                base_url = base_url.join(&format!("dags/{dag_id}"))?;
            }
            OpenItem::DagRun { dag_id, dag_run_id } => {
                let escaped_dag_run_id: String =
                    form_urlencoded::byte_serialize(dag_run_id.as_bytes()).collect();
                base_url = base_url.join(&format!("dags/{dag_id}/grid"))?;
                base_url.set_query(Some(&format!("dag_run_id={escaped_dag_run_id}")));
            }
            OpenItem::TaskInstance {
                dag_id,
                dag_run_id,
                task_id,
            } => {
                let escaped_dag_run_id: String =
                    form_urlencoded::byte_serialize(dag_run_id.as_bytes()).collect();
                base_url = base_url.join(&format!("dags/{dag_id}/grid"))?;
                base_url.set_query(Some(&format!(
                    "dag_run_id={escaped_dag_run_id}&task_id={task_id}"
                )));
            }
            OpenItem::Log {
                dag_id,
                dag_run_id,
                task_id,
                task_try: _,
            } => {
                let escaped_dag_run_id: String =
                    form_urlencoded::byte_serialize(dag_run_id.as_bytes()).collect();
                base_url = base_url.join(&format!("dags/{dag_id}/grid"))?;
                base_url.set_query(Some(&format!(
                    "dag_run_id={escaped_dag_run_id}&task_id={task_id}&tab=logs"
                )));
            }
        }

        Ok(base_url.to_string())
    }
}
