use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportErrorCollection {
    pub import_errors: Vec<ImportError>,
    pub total_entries: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportError {
    pub import_error_id: Option<i64>,
    pub timestamp: Option<String>,
    pub filename: Option<String>,
    pub stack_trace: Option<String>,
}
