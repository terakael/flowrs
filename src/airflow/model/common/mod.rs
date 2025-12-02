pub mod connection;
pub mod dag;
pub mod dagrun;
pub mod dagstats;
pub mod importerror;
pub mod log;
pub mod taskinstance;
pub mod variable;

// Re-export common types for easier access
pub use connection::{Connection, ConnectionCollection};
pub use dag::{Dag, DagList};
pub use dagrun::{DagRun, DagRunList};
pub use dagstats::{DagStatistic, DagStatsResponse};
pub use importerror::{ImportError, ImportErrorList};
pub use log::Log;
pub use taskinstance::{TaskInstance, TaskInstanceList};
pub use variable::{Variable, VariableCollection};
