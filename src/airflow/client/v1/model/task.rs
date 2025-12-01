use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskCollection {
    pub tasks: Vec<TaskResponse>,
    pub total_entries: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskResponse {
    pub task_id: String,
    pub owner: Option<String>,
    pub downstream_task_ids: Vec<String>,
    pub pool: Option<String>,
    pub retries: Option<f64>,
}
