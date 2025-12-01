use crate::airflow::config::FlowrsConfig;
use crate::app::environment_state::EnvironmentStateContainer;
use crate::app::model::dagruns::DagRunModel;
use crate::app::model::dags::DagModel;
use throbber_widgets_tui::ThrobberState;

use super::model::{config::ConfigModel, logs::LogModel, taskinstances::TaskInstanceModel};

pub struct App {
    pub config: FlowrsConfig,
    pub environment_state: EnvironmentStateContainer,
    pub dags: DagModel,
    pub configs: ConfigModel,
    pub dagruns: DagRunModel,
    pub task_instances: TaskInstanceModel,
    pub logs: LogModel,
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
        }
    }

    pub fn previous_panel(&mut self) {
        match self.active_panel {
            Panel::Config => (),
            Panel::Dag => self.active_panel = Panel::Config,
            Panel::DAGRun => self.active_panel = Panel::Dag,
            Panel::TaskInstance => self.active_panel = Panel::DAGRun,
            Panel::Logs => self.active_panel = Panel::TaskInstance,
        }
    }

    pub fn clear_state(&mut self) {
        self.ticks = 0;
        self.loading = true;
        // Clear view models but not environment_state
        // This clears UI state (filters, selections) but data persists in environment_state
        self.dags.all.clear();
        self.dagruns.all.clear();
        self.task_instances.all.clear();
        self.logs.all.clear();
    }

    /// Sync panel data from `environment_state`
    /// This should be called when switching panels or environments
    pub fn sync_panel_data(&mut self) {
        match self.active_panel {
            Panel::Dag => {
                self.dags.all = self.environment_state.get_active_dags();
                self.dags.filter_dags();
            }
            Panel::DAGRun => {
                if let Some(dag_id) = &self.dagruns.dag_id.clone() {
                    self.dagruns.all = self.environment_state.get_active_dag_runs(dag_id);
                    self.dagruns.filter_dag_runs();
                    
                    // Sync cached DAG details
                    if let Some(dag_details) = self.environment_state.get_active_dag_details(dag_id) {
                        self.dagruns.dag_details = Some(dag_details);
                        self.dagruns.init_info_scroll();
                    }
                } else {
                    self.dagruns.all.clear();
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
                    self.logs.all = self
                        .environment_state
                        .get_active_task_logs(dag_id, dag_run_id, task_id);
                } else {
                    self.logs.all.clear();
                }
            }
            Panel::Config => {
                // Config panel doesn't need syncing
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
