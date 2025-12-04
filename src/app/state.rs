use crate::airflow::config::FlowrsConfig;
use crate::app::environment_state::EnvironmentStateContainer;
use crate::app::model::dagruns::DagRunModel;
use crate::app::model::dags::DagModel;
use throbber_widgets_tui::ThrobberState;
use log::debug;

use super::model::{
    config::ConfigModel,
    detail::{ConnectionDetailModel, ImportErrorDetailModel, VariableDetailModel},
    logs::LogModel, 
    taskinstances::TaskInstanceModel,
};

pub struct App {
    pub config: FlowrsConfig,
    pub environment_state: EnvironmentStateContainer,
    pub dags: DagModel,
    pub configs: ConfigModel,
    pub dagruns: DagRunModel,
    pub task_instances: TaskInstanceModel,
    pub logs: LogModel,
    pub variable_detail: VariableDetailModel,
    pub connection_detail: ConnectionDetailModel,
    pub import_error_detail: ImportErrorDetailModel,
    pub ticks: u32,
    pub active_panel: Panel,
    pub loading: bool,
    pub startup: bool,
    pub throbber_state: ThrobberState,
}

#[derive(Clone, PartialEq)]
pub enum Panel {
    Config,
    Dag,
    DAGRun,
    TaskInstance,
    Logs,
    VariableDetail,
    ConnectionDetail,
    ImportErrorDetail,
}

impl App {
    #[allow(dead_code)]
    pub fn new(config: FlowrsConfig) -> Self {
        Self::new_with_errors(config, vec![])
    }

    pub fn new_with_errors(config: FlowrsConfig, errors: Vec<String>) -> Self {
        let servers = &config.clone().servers.unwrap_or_default();
        let active_server = if let Some(active_server) = &config.active_server {
            servers.iter().find(|server| server.name == *active_server)
        } else {
            None
        };
        App {
            config,
            environment_state: EnvironmentStateContainer::new(),
            dags: DagModel::new(),
            configs: ConfigModel::new_with_errors(servers.clone(), errors),
            dagruns: DagRunModel::new(),
            task_instances: TaskInstanceModel::new(),
            logs: LogModel::new(),
            variable_detail: VariableDetailModel::new(),
            connection_detail: ConnectionDetailModel::new(),
            import_error_detail: ImportErrorDetailModel::new(),
            active_panel: match active_server {
                Some(_) => Panel::Dag,
                None => Panel::Config,
            },
            ticks: 0,
            loading: true,
            startup: true,
            throbber_state: ThrobberState::default(),
        }
    }

    pub fn next_panel(&mut self) {
        match self.active_panel {
            Panel::Config => self.active_panel = Panel::Dag,
            Panel::Dag => self.active_panel = Panel::DAGRun,
            Panel::DAGRun => self.active_panel = Panel::TaskInstance,
            Panel::TaskInstance => self.active_panel = Panel::Logs,
            Panel::Logs => (),
            // Detail panels go back to DAG panel (they're not in the main flow)
            Panel::VariableDetail | Panel::ConnectionDetail | Panel::ImportErrorDetail => self.active_panel = Panel::Dag,
        }
    }

    pub fn previous_panel(&mut self) {
        match self.active_panel {
            Panel::Config => (),
            Panel::Dag => self.active_panel = Panel::Config,
            Panel::DAGRun => self.active_panel = Panel::Dag,
            Panel::TaskInstance => self.active_panel = Panel::DAGRun,
            Panel::Logs => self.active_panel = Panel::TaskInstance,
            // Detail panels go back to DAG panel
            Panel::VariableDetail | Panel::ConnectionDetail | Panel::ImportErrorDetail => self.active_panel = Panel::Dag,
        }
    }

    pub fn clear_state(&mut self) {
        self.ticks = 0;
        self.loading = true;
        // Clear view models but not environment_state
        // This clears UI state (filters, selections) but data persists in environment_state
        self.dags.all.clear();
        self.dags.loading_status = crate::app::model::dags::LoadingStatus::NotStarted;
        self.dagruns.all.clear();
        self.task_instances.all.clear();
        self.logs.current_log_data = None;
    }

    /// Sync panel data from `environment_state`
    /// This should be called when switching panels or environments
    pub fn sync_panel_data(&mut self) {
        match self.active_panel {
            Panel::Dag => {
                let dag_count = self.environment_state.get_active_dags().len();
                self.dags.all = self.environment_state.get_active_dags();
                self.dags.filter_dags();
                debug!("sync_panel_data: Synced {} DAGs to panel, recent_runs has {} entries", dag_count, self.dags.recent_runs.len());
                // Restore tab and selection state when returning from detail views
                self.dags.restore_state_from_detail_view();
            }
            Panel::DAGRun => {
                if let Some(dag_id) = &self.dagruns.dag_id.clone() {
                    self.dagruns.all = self.environment_state.get_active_dag_runs(dag_id);
                    self.dagruns.total_entries = self.environment_state.get_active_dag_runs_total(dag_id);
                    self.dagruns.filter_dag_runs();
                    
                    // Sync cached DAG details
                    if let Some(dag_details) = self.environment_state.get_active_dag_details(dag_id) {
                        self.dagruns.dag_details = Some(dag_details);
                        self.dagruns.init_info_scroll();
                    }
                } else {
                    self.dagruns.all.clear();
                    self.dagruns.total_entries = 0;
                }
            }
            Panel::TaskInstance => {
                // Clone the IDs to avoid borrow checker issues
                let dag_id = self.task_instances.dag_id.clone();
                let dag_run_id = self.task_instances.dag_run_id.clone();
                
                if let (Some(dag_id), Some(dag_run_id)) = (dag_id, dag_run_id) {
                    self.task_instances.all = self
                        .environment_state
                        .get_active_task_instances(&dag_id, &dag_run_id);
                    self.task_instances.filter_task_instances();
                    
                    // Build graph layout and apply tree-based ordering if dependencies available
                    let dependencies_opt = self.environment_state.get_task_dependencies(&dag_id).cloned();
                    if let Some(dependencies) = dependencies_opt {
                        // Build tree-ordered layout
                        let tree_ordered = crate::airflow::graph_layout::build_graph_layout_ordered(&dependencies);
                        
                        // Extract task order from tree traversal (first occurrence of each task)
                        let mut tree_order: Vec<String> = Vec::new();
                        let mut seen = std::collections::HashSet::new();
                        for (task_id, _) in &tree_ordered {
                            if seen.insert(task_id.clone()) {
                                tree_order.push(task_id.clone());
                            }
                        }
                        
                        // Apply tree ordering
                        self.apply_task_order(&tree_order);
                        
                        // Build graph layout HashMap
                        let graph_layout = crate::airflow::graph_layout::build_graph_layout(
                            &tree_order,
                            &dependencies
                        );
                        self.task_instances.graph_layout = graph_layout;
                    } else if let Some(task_order) = self.environment_state.get_task_order(&dag_id) {
                        // Fallback to topological ordering if no dependencies
                        self.apply_task_order(&task_order);
                    }
                } else {
                    self.task_instances.all.clear();
                }
            }
            Panel::Logs => {
                if let (Some(dag_id), Some(dag_run_id), Some(task_id)) =
                    (&self.logs.dag_id, &self.logs.dag_run_id, &self.logs.task_id)
                {
                    // Copy current attempt's log data to panel
                    self.logs.current_log_data = self
                        .environment_state
                        .get_active_task_log(dag_id, dag_run_id, task_id, self.logs.current_attempt as u16);
                } else {
                    self.logs.current_log_data = None;
                }
            }
            Panel::Config => {
                // Config panel doesn't need syncing
            }
            Panel::VariableDetail | Panel::ConnectionDetail | Panel::ImportErrorDetail => {
                // Detail panels don't sync from environment_state
                // They're populated by worker messages when navigating to them
            }
        }
    }
    
    /// Apply topological ordering to task instances
    fn apply_task_order(&mut self, task_order: &[String]) {
        // Create a map of task_id -> position in the topological order
        let position_map: std::collections::HashMap<&str, usize> = task_order
            .iter()
            .enumerate()
            .map(|(idx, task_id)| (task_id.as_str(), idx))
            .collect();
        
        // Sort filtered items by position in topological order
        self.task_instances.filtered.items.sort_by_key(|task| {
            position_map
                .get(task.task_id.as_str())
                .copied()
                .unwrap_or(usize::MAX) // Tasks not in order go to the end
        });
    }
}
