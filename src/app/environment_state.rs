use std::collections::HashMap;
use std::sync::Arc;

use crate::airflow::{
    model::common::{Dag, DagRun, Log, TaskInstance},
    traits::AirflowClient as AirflowClientTrait,
};

/// Key identifying an environment (Airflow server configuration)
pub type EnvironmentKey = String;
pub type DagId = String;
pub type DagRunId = String;
pub type TaskId = String;

/// Represents a single chunk of log content
#[derive(Debug, Clone)]
pub struct LogChunk {
    pub content: String,
    pub continuation_token: Option<String>,
}

/// Represents all chunks for a single task attempt
#[derive(Debug, Clone)]
pub struct TaskLog {
    pub chunks: Vec<LogChunk>,
    pub current_continuation_token: Option<String>,
    pub is_complete: bool,
}

impl TaskLog {
    pub fn new() -> Self {
        Self {
            chunks: Vec::new(),
            current_continuation_token: None,
            is_complete: false,
        }
    }
    
    pub fn add_chunk(&mut self, log: Log) {
        self.chunks.push(LogChunk {
            content: log.content.clone(),
            continuation_token: log.continuation_token.clone(),
        });
        self.current_continuation_token = log.continuation_token.clone();
        self.is_complete = log.continuation_token.is_none();
    }
    
    /// Get all content as a single string
    pub fn full_content(&self) -> String {
        self.chunks
            .iter()
            .map(|chunk| chunk.content.as_str())
            .collect::<Vec<_>>()
            .join("")
    }
    
    /// Get total line count across all loaded chunks
    pub fn total_lines(&self) -> usize {
        self.chunks.iter().map(|c| c.content.lines().count()).sum()
    }
    
    /// Check if we can load more chunks
    pub fn has_more(&self) -> bool {
        !self.is_complete
    }
}

/// State for a specific task instance's logs
#[derive(Debug, Clone)]
pub struct TaskInstanceData {
    pub task_instance: TaskInstance,
    pub logs: HashMap<u16, TaskLog>,  // Key = try_number
}

impl TaskInstanceData {
    pub fn new(task_instance: TaskInstance) -> Self {
        Self {
            task_instance,
            logs: HashMap::new(),
        }
    }
}

/// State for a specific DAG run
#[derive(Debug, Clone)]
pub struct DagRunData {
    pub dag_run: DagRun,
    pub task_instances: HashMap<TaskId, TaskInstanceData>,
}

impl DagRunData {
    pub fn new(dag_run: DagRun) -> Self {
        Self {
            dag_run,
            task_instances: HashMap::new(),
        }
    }
    pub fn get_task_instance(&self, task_id: &str) -> Option<&TaskInstanceData> {
        self.task_instances.get(task_id)
    }
}

/// State for a specific DAG
#[derive(Debug, Clone)]
pub struct DagData {
    pub dag: Dag,
    pub dag_runs: HashMap<DagRunId, DagRunData>,
    pub total_dag_runs: i64,  // Total DAG runs available from API
}

impl DagData {
    pub fn new(dag: Dag) -> Self {
        Self {
            dag,
            dag_runs: HashMap::new(),
            total_dag_runs: 0,
        }
    }

    pub fn get_dag_run(&self, dag_run_id: &str) -> Option<&DagRunData> {
        self.dag_runs.get(dag_run_id)
    }
}

/// State for a specific environment (Airflow server)
#[derive(Clone)]
pub struct EnvironmentData {
    pub client: Arc<dyn AirflowClientTrait>,
    pub dags: HashMap<DagId, DagData>,
    pub dag_details: HashMap<DagId, Dag>,
    pub task_order: HashMap<DagId, Vec<String>>,
    pub task_dependencies: HashMap<DagId, HashMap<String, Vec<String>>>,
}

impl EnvironmentData {
    pub fn new(client: Arc<dyn AirflowClientTrait>) -> Self {
        Self {
            client,
            dags: HashMap::new(),
            dag_details: HashMap::new(),
            task_order: HashMap::new(),
            task_dependencies: HashMap::new(),
        }
    }

    pub fn get_dag(&self, dag_id: &str) -> Option<&DagData> {
        self.dags.get(dag_id)
    }

    /// Update or create a DAG in the environment
    pub fn upsert_dag(&mut self, dag: Dag) {
        let dag_id = dag.dag_id.clone();
        if let Some(existing_dag_data) = self.dags.get_mut(&dag_id) {
            existing_dag_data.dag = dag;
        } else {
            self.dags.insert(dag_id, DagData::new(dag));
        }
    }

    /// Update or create a DAG run in the environment
    pub fn upsert_dag_run(&mut self, dag_run: DagRun) {
        let dag_id = dag_run.dag_id.clone();
        let dag_run_id = dag_run.dag_run_id.clone();

        if let Some(dag_data) = self.dags.get_mut(&dag_id) {
            if let Some(existing_run) = dag_data.dag_runs.get_mut(&dag_run_id) {
                existing_run.dag_run = dag_run;
            } else {
                dag_data
                    .dag_runs
                    .insert(dag_run_id, DagRunData::new(dag_run));
            }
        }
    }

    /// Set the total DAG runs count for a DAG
    pub fn set_total_dag_runs(&mut self, dag_id: &str, total: i64) {
        if let Some(dag_data) = self.dags.get_mut(dag_id) {
            dag_data.total_dag_runs = total;
        }
    }

    /// Update or create a task instance in the environment
    pub fn upsert_task_instance(&mut self, task_instance: TaskInstance) {
        let dag_id = task_instance.dag_id.clone();
        let dag_run_id = task_instance.dag_run_id.clone();
        let task_id = task_instance.task_id.clone();

        if let Some(dag_data) = self.dags.get_mut(&dag_id) {
            if let Some(dag_run_data) = dag_data.dag_runs.get_mut(&dag_run_id) {
                if let Some(existing_task) = dag_run_data.task_instances.get_mut(&task_id) {
                    existing_task.task_instance = task_instance;
                } else {
                    dag_run_data
                        .task_instances
                        .insert(task_id, TaskInstanceData::new(task_instance));
                }
            }
        }
    }

    /// Add a log chunk for a specific task attempt
    pub fn add_task_log_chunk(
        &mut self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u16,
        log: Log,
    ) {
        if let Some(dag_data) = self.dags.get_mut(dag_id) {
            if let Some(dag_run_data) = dag_data.dag_runs.get_mut(dag_run_id) {
                if let Some(task_data) = dag_run_data.task_instances.get_mut(task_id) {
                    let task_log = task_data.logs
                        .entry(task_try)
                        .or_insert_with(TaskLog::new);
                    task_log.add_chunk(log);
                }
            }
        }
    }

    /// Get a specific task attempt's log
    pub fn get_task_log(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u16,
    ) -> Option<&TaskLog> {
        self.dags
            .get(dag_id)?
            .dag_runs
            .get(dag_run_id)?
            .task_instances
            .get(task_id)?
            .logs
            .get(&task_try)
    }

    /// Clear log chunks for a specific task attempt
    pub fn clear_task_log(
        &mut self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u16,
    ) {
        if let Some(dag_data) = self.dags.get_mut(dag_id) {
            if let Some(dag_run_data) = dag_data.dag_runs.get_mut(dag_run_id) {
                if let Some(task_data) = dag_run_data.task_instances.get_mut(task_id) {
                    task_data.logs.remove(&task_try);
                }
            }
        }
    }

    /// Evict log attempts not in the LRU cache
    pub fn evict_task_logs_not_in_cache(
        &mut self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        keep_attempts: &[u16],
    ) {
        if let Some(dag_data) = self.dags.get_mut(dag_id) {
            if let Some(dag_run_data) = dag_data.dag_runs.get_mut(dag_run_id) {
                if let Some(task_data) = dag_run_data.task_instances.get_mut(task_id) {
                    task_data.logs.retain(|&try_num, _| keep_attempts.contains(&try_num));
                }
            }
        }
    }

    /// Get detailed DAG information
    pub fn get_dag_details(&self, dag_id: &str) -> Option<&Dag> {
        self.dag_details.get(dag_id)
    }

    /// Set detailed DAG information
    pub fn set_dag_details(&mut self, dag_id: String, dag: Dag) {
        self.dag_details.insert(dag_id, dag);
    }
    
    /// Get task order for a DAG
    pub fn get_task_order(&self, dag_id: &str) -> Option<&Vec<String>> {
        self.task_order.get(dag_id)
    }
    
    /// Set task order for a DAG
    pub fn set_task_order(&mut self, dag_id: String, order: Vec<String>) {
        self.task_order.insert(dag_id, order);
    }
    
    /// Get task dependencies for a DAG (task_id -> list of upstream dependencies)
    pub fn get_task_dependencies(&self, dag_id: &str) -> Option<&HashMap<String, Vec<String>>> {
        self.task_dependencies.get(dag_id)
    }
    
    /// Set task dependencies for a DAG
    pub fn set_task_dependencies(&mut self, dag_id: String, dependencies: HashMap<String, Vec<String>>) {
        self.task_dependencies.insert(dag_id, dependencies);
    }
}

/// Container for all environment states
#[derive(Clone)]
pub struct EnvironmentStateContainer {
    pub environments: HashMap<EnvironmentKey, EnvironmentData>,
    pub active_environment: Option<EnvironmentKey>,
}

impl EnvironmentStateContainer {
    pub fn new() -> Self {
        Self {
            environments: HashMap::new(),
            active_environment: None,
        }
    }

    pub fn add_environment(&mut self, key: EnvironmentKey, data: EnvironmentData) {
        self.environments.insert(key, data);
    }

    pub fn get_active_environment(&self) -> Option<&EnvironmentData> {
        self.active_environment
            .as_ref()
            .and_then(|key| self.environments.get(key))
    }

    pub fn get_active_environment_mut(&mut self) -> Option<&mut EnvironmentData> {
        self.active_environment
            .as_ref()
            .and_then(|key| self.environments.get_mut(key))
    }

    pub fn set_active_environment(&mut self, key: EnvironmentKey) {
        if self.environments.contains_key(&key) {
            self.active_environment = Some(key);
        }
    }

    pub fn get_active_client(&self) -> Option<Arc<dyn AirflowClientTrait>> {
        self.get_active_environment().map(|env| env.client.clone())
    }

    /// Get all DAGs for the active environment
    pub fn get_active_dags(&self) -> Vec<Dag> {
        self.get_active_environment()
            .map(|env| {
                env.dags
                    .values()
                    .map(|dag_data| dag_data.dag.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all DAG runs for a specific DAG in the active environment
    pub fn get_active_dag_runs(&self, dag_id: &str) -> Vec<DagRun> {
        self.get_active_environment()
            .and_then(|env| env.get_dag(dag_id))
            .map(|dag_data| {
                dag_data
                    .dag_runs
                    .values()
                    .map(|run_data| run_data.dag_run.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the total DAG runs count for a specific DAG in the active environment
    pub fn get_active_dag_runs_total(&self, dag_id: &str) -> i64 {
        self.get_active_environment()
            .and_then(|env| env.get_dag(dag_id))
            .map(|dag_data| dag_data.total_dag_runs)
            .unwrap_or(0)
    }

    /// Get all task instances for a specific DAG run in the active environment
    pub fn get_active_task_instances(&self, dag_id: &str, dag_run_id: &str) -> Vec<TaskInstance> {
        self.get_active_environment()
            .and_then(|env| env.get_dag(dag_id))
            .and_then(|dag_data| dag_data.get_dag_run(dag_run_id))
            .map(|run_data| {
                run_data
                    .task_instances
                    .values()
                    .map(|task_data| task_data.task_instance.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get logs for a specific task instance attempt in the active environment
    pub fn get_active_task_log(&self, dag_id: &str, dag_run_id: &str, task_id: &str, task_try: u16) -> Option<TaskLog> {
        self.get_active_environment()
            .and_then(|env| env.get_task_log(dag_id, dag_run_id, task_id, task_try))
            .cloned()
    }

    /// Get a specific DAG by ID from the active environment
    pub fn get_active_dag(&self, dag_id: &str) -> Option<Dag> {
        self.get_active_environment()
            .and_then(|env| env.get_dag(dag_id))
            .map(|dag_data| dag_data.dag.clone())
    }

    /// Get detailed DAG information from the active environment
    pub fn get_active_dag_details(&self, dag_id: &str) -> Option<Dag> {
        self.get_active_environment()
            .and_then(|env| env.get_dag_details(dag_id))
            .cloned()
    }

    /// Set detailed DAG information in the active environment
    pub fn set_dag_details(&mut self, dag_id: String, dag: Dag) {
        if let Some(env) = self.get_active_environment_mut() {
            env.set_dag_details(dag_id, dag);
        }
    }
    
    /// Check if task order exists for a DAG in the active environment
    pub fn has_task_order(&self, dag_id: &str) -> bool {
        self.get_active_environment()
            .and_then(|env| env.get_task_order(dag_id))
            .is_some()
    }
    
    /// Set task order for a DAG in the active environment
    pub fn set_task_order(&mut self, dag_id: String, order: Vec<String>) {
        if let Some(env) = self.get_active_environment_mut() {
            env.set_task_order(dag_id, order);
        }
    }
    
    /// Get task order for a DAG in the active environment
    pub fn get_task_order(&self, dag_id: &str) -> Option<Vec<String>> {
        self.get_active_environment()
            .and_then(|env| env.get_task_order(dag_id))
            .cloned()
    }
    
    /// Set task dependencies for a DAG in the active environment
    pub fn set_task_dependencies(&mut self, dag_id: String, dependencies: HashMap<String, Vec<String>>) {
        if let Some(env) = self.get_active_environment_mut() {
            env.set_task_dependencies(dag_id, dependencies);
        }
    }
    
    /// Get task dependencies for a DAG in the active environment
    pub fn get_task_dependencies(&self, dag_id: &str) -> Option<&HashMap<String, Vec<String>>> {
        self.get_active_environment()
            .and_then(|env| env.get_task_dependencies(dag_id))
    }
}

impl Default for EnvironmentStateContainer {
    fn default() -> Self {
        Self::new()
    }
}
