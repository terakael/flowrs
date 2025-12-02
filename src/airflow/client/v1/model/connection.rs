use serde::{Deserialize, Serialize};

use crate::airflow::model::common::Connection;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionResponse {
    pub connection_id: String,
    pub conn_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub login: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<String>,
}

impl From<ConnectionResponse> for Connection {
    fn from(c: ConnectionResponse) -> Self {
        Connection {
            connection_id: c.connection_id,
            conn_type: c.conn_type,
            host: c.host,
            login: c.login,
            schema: c.schema,
            port: c.port,
            password: c.password,
            extra: c.extra,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionCollectionResponse {
    pub connections: Vec<ConnectionResponse>,
    pub total_entries: i64,
}
