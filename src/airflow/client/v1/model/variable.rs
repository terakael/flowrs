use serde::{Deserialize, Serialize};

use crate::airflow::model::common::Variable;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableResponse {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

impl From<VariableResponse> for Variable {
    fn from(v: VariableResponse) -> Self {
        Variable {
            key: v.key,
            value: v.value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableCollectionResponse {
    pub variables: Vec<VariableResponse>,
    pub total_entries: i64,
}
