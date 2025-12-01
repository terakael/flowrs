use serde::{Deserialize, Serialize};

use crate::airflow::client::{v1, v2};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportError {
    pub import_error_id: Option<i64>,
    pub timestamp: Option<String>,
    pub filename: Option<String>,
    pub stack_trace: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportErrorList {
    pub import_errors: Vec<ImportError>,
    pub total_entries: i64,
}

// From trait implementations for v1 models
impl From<v1::model::importerror::ImportError> for ImportError {
    fn from(value: v1::model::importerror::ImportError) -> Self {
        ImportError {
            import_error_id: value.import_error_id,
            timestamp: value.timestamp,
            filename: value.filename,
            stack_trace: value.stack_trace,
        }
    }
}

impl From<v1::model::importerror::ImportErrorCollection> for ImportErrorList {
    fn from(value: v1::model::importerror::ImportErrorCollection) -> Self {
        ImportErrorList {
            import_errors: value.import_errors.into_iter().map(|e| e.into()).collect(),
            total_entries: value.total_entries,
        }
    }
}

// From trait implementations for v2 models
impl From<v2::model::importerror::ImportError> for ImportError {
    fn from(value: v2::model::importerror::ImportError) -> Self {
        ImportError {
            import_error_id: value.import_error_id,
            timestamp: value.timestamp,
            filename: value.filename,
            stack_trace: value.stack_trace,
        }
    }
}

impl From<v2::model::importerror::ImportErrorCollection> for ImportErrorList {
    fn from(value: v2::model::importerror::ImportErrorCollection) -> Self {
        ImportErrorList {
            import_errors: value.import_errors.into_iter().map(|e| e.into()).collect(),
            total_entries: value.total_entries,
        }
    }
}
