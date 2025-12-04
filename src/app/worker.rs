use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::airflow::model::common::Dag;

use super::model::popup::error::ErrorPopup;
use super::model::popup::taskinstances::mark::MarkState as taskMarkState;
use super::{model::popup::dagruns::mark::MarkState, state::{App, Panel}};
use anyhow::Result;
use futures::future::join_all;
use log::debug;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct Worker {
    app: Arc<Mutex<App>>,
    rx: Receiver<WorkerMessage>,
    tx: Sender<WorkerMessage>,
}

#[derive(Debug)]
pub enum WorkerMessage {
    ConfigSelected(usize),
    UpdateDags,
    FetchMoreDags {
        offset: i64,
        limit: i64,
    },
    ToggleDag {
        dag_id: String,
        is_paused: bool,
    },
    UpdateDagRuns {
        dag_id: String,
        clear: bool,
    },
    FetchMoreDagRuns {
        dag_id: String,
        offset: i64,
        limit: i64,
    },
    UpdateTaskInstances {
        dag_id: String,
        dag_run_id: String,
        clear: bool,
    },
    FetchTaskOrder {
        dag_id: String,
    },
    GetDagCode {
        dag_id: String,
    },
    FetchAndOpenDagCodeInEditor {
        dag_id: String,
    },
    GetDagDetails {
        dag_id: String,
    },
    UpdateRecentDagRuns,  // Fetch recent runs for all DAGs
    UpdateImportErrors,
    ClearDagRun {
        dag_run_id: String,
        dag_id: String,
    },
    UpdateTaskLogs {
        dag_id: String,
        dag_run_id: String,
        task_id: String,
        task_try: u16,
        clear: bool,
    },
    /// Ensure a specific attempt's log is loaded (checks cache first)
    EnsureTaskLogLoaded {
        dag_id: String,
        dag_run_id: String,
        task_id: String,
        task_try: u16,
    },
    /// Load next chunk for current log (auto-triggered on scroll)
    LoadMoreTaskLogChunk {
        dag_id: String,
        dag_run_id: String,
        task_id: String,
        task_try: u16,
        continuation_token: String,
    },
    MarkDagRun {
        dag_run_id: String,
        dag_id: String,
        status: MarkState,
    },
    ClearTaskInstance {
        task_id: String,
        dag_id: String,
        dag_run_id: String,
    },
    MarkTaskInstance {
        task_id: String,
        dag_id: String,
        dag_run_id: String,
        status: taskMarkState,
    },
    TriggerDagRun {
        dag_id: String,
    },
    OpenItem(OpenItem),
    OpenInEditor {
        filepath: std::path::PathBuf,
    },
    // Variables and Connections
    UpdateVariables,
    GetVariableDetail {
        key: String,
    },
    UpdateConnections,
    GetConnectionDetail {
        connection_id: String,
    },
    // Import Errors
    GetImportErrorDetail {
        import_error_id: i64,
    },
}

#[derive(Debug)]
pub enum OpenItem {
    Config(String),
    Dag {
        dag_id: String,
    },
    DagRun {
        dag_id: String,
        dag_run_id: String,
    },
    TaskInstance {
        dag_id: String,
        dag_run_id: String,
        task_id: String,
    },
    Log {
        dag_id: String,
        dag_run_id: String,
        task_id: String,
        #[allow(dead_code)]
        task_try: u16,
    },
}

impl Worker {
    pub fn new(app: Arc<Mutex<App>>, rx_worker: Receiver<WorkerMessage>, tx_worker: Sender<WorkerMessage>) -> Self {
        Worker { app, rx: rx_worker, tx: tx_worker }
    }
    
    /// Helper function to persist logs to disk after adding a chunk
    /// This is called every time a log chunk is added (incremental persistence)
    fn persist_log_to_disk(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u16,
    ) {
        use crate::app::environment_state::{get_log_filepath, save_log_to_disk};
        
        let app = self.app.lock().unwrap();
        
        // Get environment name
        let env_name = match app.environment_state.get_active_environment_name() {
            Some(name) => name,
            None => {
                log::warn!("No active environment when trying to persist log");
                return;
            }
        };
        
        // Get the current log data
        let log_data = match app.environment_state.get_active_task_log(dag_id, dag_run_id, task_id, task_try) {
            Some(log) => log,
            None => {
                log::warn!("Could not find log data when trying to persist");
                return;
            }
        };
        
        // Get filepath
        let filepath = match get_log_filepath(env_name, dag_id, dag_run_id, task_id, task_try) {
            Ok(path) => path,
            Err(e) => {
                log::warn!("Failed to get log filepath: {}", e);
                return;
            }
        };
        
        // Save to disk
        let content = log_data.full_content();
        if let Err(e) = save_log_to_disk(&filepath, &content) {
            log::warn!("Failed to persist log to disk: {}", e);
            return;
        }
        
        // Update TaskLog with file path (need to drop lock and re-acquire with mut)
        drop(app); // Drop the read lock
        let mut app = self.app.lock().unwrap();
        if let Some(env) = app.environment_state.get_active_environment_mut() {
            if let Some(dag_data) = env.dags.get_mut(dag_id) {
                if let Some(dag_run_data) = dag_data.dag_runs.get_mut(dag_run_id) {
                    if let Some(task_data) = dag_run_data.task_instances.get_mut(task_id) {
                        if let Some(task_log) = task_data.logs.get_mut(&task_try) {
                            task_log.set_file_path(filepath.clone());
                            log::debug!("Set file path for log: {}", filepath.display());
                        }
                    }
                }
            }
        }
        
        // Sync panel data to update current_log_data with the new file_path
        app.sync_panel_data();
    }

    pub async fn process_message(&mut self, message: WorkerMessage) -> Result<()> {
        // Set loading state at the start
        {
            let mut app = self.app.lock().unwrap();
            app.loading = true;
        }

        // Handle ConfigSelected BEFORE checking for client (since it creates the client)
        if let WorkerMessage::ConfigSelected(idx) = message {
            self.switch_airflow_client(idx);
            let mut app = self.app.lock().unwrap();
            app.loading = false;
            return Ok(());
        }

        // Get the active client from the environment state
        let client = {
            let app = self.app.lock().unwrap();
            app.environment_state.get_active_client()
        };

        if client.is_none() {
            // Reset loading state before returning
            let mut app = self.app.lock().unwrap();
            app.dags.error_popup = Some(ErrorPopup::from_strings(vec![
                "No active environment selected".into(),
            ]));
            app.loading = false;
            return Ok(());
        }
        let client = client.unwrap();
        match message {
            WorkerMessage::UpdateDags => {
                // Always clear backend first (instant if empty on initial load)
                // This is a hard refresh - backend and frontend both get cleared
                {
                    let mut app = self.app.lock().unwrap();
                    app.environment_state.clear_active_environment_dags();
                    app.dags.recent_runs.clear();
                }
                
                // Fetch initial 10 DAGs for immediate display
                let start = std::time::Instant::now();
                debug!("[PERF] Starting UpdateDags - fetching first 10 DAGs");
                let dag_list = client.list_dags_paginated(0, 10).await;
                debug!("[PERF] UpdateDags: list_dags_paginated took {:?}", start.elapsed());
                match dag_list {
                    Ok(dag_list) => {
                        let total = dag_list.total_entries;
                        debug!("Received {} DAGs from API, total: {}", dag_list.dags.len(), total);
                        let active_count = dag_list.dags.iter().filter(|d| !d.is_paused).count();
                        let paused_count = dag_list.dags.iter().filter(|d| d.is_paused).count();
                        debug!("  Active: {}, Paused: {}", active_count, paused_count);
                        
                        // Extract unpaused DAG IDs from this batch for stats fetching
                        let unpaused_dag_ids: Vec<String> = dag_list.dags
                            .iter()
                            .filter(|dag| !dag.is_paused)
                            .map(|dag| dag.dag_id.clone())
                            .collect();
                        
                        // Store initial DAGs in the environment state and check if we need more
                        let (needs_more, current_count) = {
                            let mut app = self.app.lock().unwrap();
                            if let Some(env) = app.environment_state.get_active_environment_mut() {
                                for dag in &dag_list.dags {
                                    env.upsert_dag(dag.clone());
                                }
                            }
                            
                            // Set loading status
                            let needs_more = dag_list.dags.len() < total as usize;
                            app.dags.loading_status = if needs_more {
                                crate::app::model::dags::LoadingStatus::LoadingMore {
                                    current: dag_list.dags.len(),
                                    total: total as usize,
                                }
                            } else {
                                crate::app::model::dags::LoadingStatus::Complete
                            };
                            
                            // Sync panel data from environment state (hard refresh - replaces frontend)
                            app.sync_panel_data();
                            
                            (needs_more, dag_list.dags.len())
                        }; // Lock is dropped here
                        
                        // If we need more DAGs, automatically trigger the next fetch
                        if needs_more {
                            debug!("Auto-triggering next batch after initial load: offset={}, total={}", current_count, total);
                            let _ = self.tx.send(WorkerMessage::FetchMoreDags {
                                offset: current_count as i64,
                                limit: 10, // Same batch size as initial load
                            }).await;
                        }
                        
                        // Spawn recent runs fetching in background - don't block next DAG batch
                        if !unpaused_dag_ids.is_empty() {
                            let app_clone = self.app.clone();
                            let client_clone = client.clone();
                            tokio::spawn(async move {
                                // Fetch recent runs using batch API with intelligent follow-up for missing DAGs
                                let mut all_runs: std::collections::HashMap<String, Vec<_>> = std::collections::HashMap::new();
                                let mut remaining_dag_ids = unpaused_dag_ids.clone();
                                
                                // Keep calling batch API until all DAGs have been retrieved
                                while !remaining_dag_ids.is_empty() {
                                    match client_clone.list_dagruns_batch(
                                        remaining_dag_ids.clone(),
                                        crate::app::model::dags::RECENT_RUNS_HEALTH_WINDOW as i64
                                    ).await {
                                        Ok(dag_runs) => {
                                            let run_count = dag_runs.dag_runs.len();
                                            debug!("[UpdateDags] Batch API returned {} runs for {} DAGs", run_count, remaining_dag_ids.len());
                                            
                                            // Group runs by DAG ID
                                            let mut runs_in_batch: std::collections::HashSet<String> = std::collections::HashSet::new();
                                            for run in dag_runs.dag_runs {
                                                runs_in_batch.insert(run.dag_id.clone());
                                                all_runs.entry(run.dag_id.clone()).or_default().push(run);
                                            }
                                            
                                            debug!("[UpdateDags] Got results for {} unique DAGs out of {} requested", runs_in_batch.len(), remaining_dag_ids.len());
                                            
                                            // Remove DAGs we got results for
                                            let before_count = remaining_dag_ids.len();
                                            remaining_dag_ids.retain(|id| !runs_in_batch.contains(id));
                                            let after_count = remaining_dag_ids.len();
                                            
                                            // If no DAGs were removed, that means remaining DAGs have no runs
                                            // Mark them as checked and stop to avoid infinite loop
                                            if before_count == after_count {
                                                debug!("[UpdateDags] No new DAGs returned runs - remaining {} DAGs likely have no runs", after_count);
                                                for dag_id in &remaining_dag_ids {
                                                    all_runs.insert(dag_id.clone(), vec![]);
                                                }
                                                break;
                                            }
                                            
                                            if after_count > 0 {
                                                debug!("[UpdateDags] {} DAGs still need results. Retrying (removed {})", after_count, before_count - after_count);
                                            } else {
                                                debug!("[UpdateDags] All DAGs retrieved successfully");
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            debug!("[UpdateDags] Batch API error: {}", e);
                                            break;
                                        }
                                    }
                                }
                                
                                // Store results
                                let mut app = app_clone.lock().unwrap();
                                let mut stored_with_runs = 0;
                                let mut stored_without_runs = 0;
                                for dag_id in &unpaused_dag_ids {
                                    if let Some(mut runs) = all_runs.remove(dag_id) {
                                        runs.sort_by(|a, b| b.logical_date.cmp(&a.logical_date));
                                        runs.truncate(crate::app::model::dags::RECENT_RUNS_HEALTH_WINDOW);
                                        app.dags.recent_runs.insert(dag_id.clone(), runs);
                                        stored_with_runs += 1;
                                    } else {
                                        app.dags.recent_runs.insert(dag_id.clone(), vec![]);
                                        stored_without_runs += 1;
                                    }
                                }
                                debug!("[UpdateDags] Stored {} DAGs with runs, {} without runs, recent_runs now has {} total entries", 
                                    stored_with_runs, stored_without_runs, app.dags.recent_runs.len());
                                
                                // Trigger UI refresh now that runs are available (only if on DAG panel)
                                if app.active_panel == crate::app::state::Panel::Dag {
                                    app.sync_panel_data();
                                    debug!("[UpdateDags] Synced panel data after storing runs");
                                } else {
                                    debug!("[UpdateDags] Skipping sync - user switched to different panel");
                                }
                            });
                        }
                        
                        // Also fetch import errors on initial load (spawn in background too)
                        let app_clone = self.app.clone();
                        let client_clone = client.clone();
                        tokio::spawn(async move {
                            if let Ok(error_list) = client_clone.list_import_errors().await {
                                let mut app = app_clone.lock().unwrap();
                                app.dags.import_error_list = error_list.import_errors.clone();
                                app.dags.filter_import_errors();
                            }
                        });
                    }
                    Err(e) => {
                        let mut app = self.app.lock().unwrap();
                        app.dags.error_popup = Some(ErrorPopup::from_strings(vec![e.to_string()]));
                        app.dags.loading_status = crate::app::model::dags::LoadingStatus::Complete;
                    }
                }
            }
            WorkerMessage::FetchMoreDags { offset, limit } => {
                let start = std::time::Instant::now();
                debug!("[PERF] FetchMoreDags: offset={}, limit={}", offset, limit);
                let dag_list = client.list_dags_paginated(offset, limit).await;
                debug!("[PERF] FetchMoreDags: list_dags_paginated took {:?}", start.elapsed());
                match dag_list {
                    Ok(dag_list) => {
                        let total = dag_list.total_entries;
                        debug!("Fetched {} more DAGs at offset {}, total: {}", dag_list.dags.len(), offset, total);
                        
                        // Extract unpaused DAG IDs from this batch for stats fetching
                        let unpaused_dag_ids: Vec<String> = dag_list.dags
                            .iter()
                            .filter(|dag| !dag.is_paused)
                            .map(|dag| dag.dag_id.clone())
                            .collect();
                        
                        // Append to existing DAGs in environment state and check if we need more
                        let (needs_more, current_count) = {
                            let mut app = self.app.lock().unwrap();
                            if let Some(env) = app.environment_state.get_active_environment_mut() {
                                for dag in &dag_list.dags {
                                    env.upsert_dag(dag.clone());
                                }
                            }
                            
                            // Calculate new current count
                            let current_count = app.environment_state
                                .get_active_dags()
                                .len();
                            
                            // Update loading status
                            let needs_more = current_count < total as usize;
                            app.dags.loading_status = if needs_more {
                                crate::app::model::dags::LoadingStatus::LoadingMore {
                                    current: current_count,
                                    total: total as usize,
                                }
                            } else {
                                crate::app::model::dags::LoadingStatus::Complete
                            };
                            
                            // Sync panel data from environment state
                            app.sync_panel_data();
                            
                            (needs_more, current_count)
                        }; // Lock is dropped here
                        
                        // If we need more DAGs, automatically trigger the next fetch
                        // This is done after dropping the lock to avoid holding it across await
                        if needs_more {
                            debug!("Auto-triggering next batch: offset={}, total={}", current_count, total);
                            let _ = self.tx.send(WorkerMessage::FetchMoreDags {
                                offset: current_count as i64,
                                limit,
                            }).await;
                        }
                        
                        // Spawn recent runs fetching in background - don't block next DAG batch
                        if !unpaused_dag_ids.is_empty() {
                            let app_clone = self.app.clone();
                            let client_clone = client.clone();
                            tokio::spawn(async move {
                                // Fetch recent runs using batch API with intelligent follow-up for missing DAGs
                                let mut all_runs: std::collections::HashMap<String, Vec<_>> = std::collections::HashMap::new();
                                let mut remaining_dag_ids = unpaused_dag_ids.clone();
                                
                                // Keep calling batch API until all DAGs have been retrieved
                                while !remaining_dag_ids.is_empty() {
                                    match client_clone.list_dagruns_batch(
                                        remaining_dag_ids.clone(),
                                        crate::app::model::dags::RECENT_RUNS_HEALTH_WINDOW as i64
                                    ).await {
                                        Ok(dag_runs) => {
                                            let run_count = dag_runs.dag_runs.len();
                                            debug!("[FetchMoreDags] Batch API returned {} runs for {} DAGs", run_count, remaining_dag_ids.len());
                                            
                                            // Group runs by DAG ID
                                            let mut runs_in_batch: std::collections::HashSet<String> = std::collections::HashSet::new();
                                            for run in dag_runs.dag_runs {
                                                runs_in_batch.insert(run.dag_id.clone());
                                                all_runs.entry(run.dag_id.clone()).or_default().push(run);
                                            }
                                            
                                            debug!("[FetchMoreDags] Got results for {} unique DAGs out of {} requested", runs_in_batch.len(), remaining_dag_ids.len());
                                            
                                            // Remove DAGs we got results for
                                            let before_count = remaining_dag_ids.len();
                                            remaining_dag_ids.retain(|id| !runs_in_batch.contains(id));
                                            let after_count = remaining_dag_ids.len();
                                            
                                            // If no DAGs were removed, that means remaining DAGs have no runs
                                            // Mark them as checked and stop to avoid infinite loop
                                            if before_count == after_count {
                                                debug!("[FetchMoreDags] No new DAGs returned runs - remaining {} DAGs likely have no runs", after_count);
                                                for dag_id in &remaining_dag_ids {
                                                    all_runs.insert(dag_id.clone(), vec![]);
                                                }
                                                break;
                                            }
                                            
                                            if after_count > 0 {
                                                debug!("[FetchMoreDags] {} DAGs still need results. Retrying (removed {})", after_count, before_count - after_count);
                                            } else {
                                                debug!("[FetchMoreDags] All DAGs retrieved successfully");
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            debug!("[FetchMoreDags] Batch API error: {}", e);
                                            break;
                                        }
                                    }
                                }
                                
                                // Store results
                                let mut app = app_clone.lock().unwrap();
                                let mut stored_with_runs = 0;
                                let mut stored_without_runs = 0;
                                for dag_id in &unpaused_dag_ids {
                                    if let Some(mut runs) = all_runs.remove(dag_id) {
                                        runs.sort_by(|a, b| b.logical_date.cmp(&a.logical_date));
                                        runs.truncate(crate::app::model::dags::RECENT_RUNS_HEALTH_WINDOW);
                                        app.dags.recent_runs.insert(dag_id.clone(), runs);
                                        stored_with_runs += 1;
                                    } else {
                                        app.dags.recent_runs.insert(dag_id.clone(), vec![]);
                                        stored_without_runs += 1;
                                    }
                                }
                                debug!("[FetchMoreDags] Stored {} DAGs with runs, {} without runs", stored_with_runs, stored_without_runs);
                                
                                // Trigger UI refresh now that runs are available (only if on DAG panel)
                                if app.active_panel == crate::app::state::Panel::Dag {
                                    app.sync_panel_data();
                                    debug!("[FetchMoreDags] Synced panel data after storing runs");
                                } else {
                                    debug!("[FetchMoreDags] Skipping sync - user switched to different panel");
                                }
                            });
                        }
                    }
                    Err(e) => {
                        // Retry logic: keep current loading status, error will be logged
                        log::error!("Failed to fetch more DAGs at offset {}: {}", offset, e);
                        // Don't show popup for background fetches to avoid disrupting user
                        // The tick handler will retry on the next tick
                    }
                }
            }
            WorkerMessage::ToggleDag { dag_id, is_paused } => {
                let dag = client.toggle_dag(&dag_id, is_paused).await;
                if let Err(e) = dag {
                    let mut app = self.app.lock().unwrap();
                    app.dags.error_popup = Some(ErrorPopup::from_strings(vec![e.to_string()]));
                }
            }
            WorkerMessage::UpdateDagRuns { dag_id, clear: _ } => {
                let dag_runs = client.list_dagruns(&dag_id).await;
                let mut app = self.app.lock().unwrap();
                // Note: dag_id is already set in the event loop before this runs
                match dag_runs {
                    Ok(dag_runs) => {
                        // Store DAG runs in the environment state
                        if let Some(env) = app.environment_state.get_active_environment_mut() {
                            env.set_total_dag_runs(&dag_id, dag_runs.total_entries);
                            for dag_run in &dag_runs.dag_runs {
                                env.upsert_dag_run(dag_run.clone());
                            }
                        }
                        // Sync panel data from environment state to refresh with new API data
                        app.sync_panel_data();
                    }
                    Err(e) => {
                        app.dagruns.error_popup =
                            Some(ErrorPopup::from_strings(vec![e.to_string()]));
                    }
                }
            }
            WorkerMessage::FetchMoreDagRuns { dag_id, offset, limit } => {
                let dag_runs = client.list_dagruns_paginated(&dag_id, offset, limit).await;
                let mut app = self.app.lock().unwrap();
                match dag_runs {
                    Ok(dag_runs) => {
                        // Store additional DAG runs in the environment state
                        if let Some(env) = app.environment_state.get_active_environment_mut() {
                            env.set_total_dag_runs(&dag_id, dag_runs.total_entries);
                            for dag_run in &dag_runs.dag_runs {
                                env.upsert_dag_run(dag_run.clone());
                            }
                        }
                        // Sync panel data from environment state to refresh with new API data
                        app.sync_panel_data();
                    }
                    Err(e) => {
                        app.dagruns.error_popup =
                            Some(ErrorPopup::from_strings(vec![e.to_string()]));
                    }
                }
            }
            WorkerMessage::UpdateTaskInstances {
                dag_id,
                dag_run_id,
                clear: _,
            } => {
                let task_instances = client.list_task_instances(&dag_id, &dag_run_id).await;
                let mut app = self.app.lock().unwrap();
                // Note: dag_id and dag_run_id are already set in the event loop before this runs
                match task_instances {
                    Ok(task_instances) => {
                        // Store task instances in the environment state
                        if let Some(env) = app.environment_state.get_active_environment_mut() {
                            for task_instance in &task_instances.task_instances {
                                env.upsert_task_instance(task_instance.clone());
                            }
                        }
                        // Sync panel data from environment state to refresh with new API data
                        app.sync_panel_data();
                    }

                    Err(e) => {
                        log::error!("Error getting task instances: {e:?}");
                        app.task_instances.error_popup =
                            Some(ErrorPopup::from_strings(vec![e.to_string()]));
                    }
                }
            }
            WorkerMessage::FetchTaskOrder { dag_id } => {
                // Fetch tasks from API
                let tasks = client.list_tasks(&dag_id).await;
                match tasks {
                    Ok(tasks) => {
                        debug!("Fetched {} tasks for DAG {}", tasks.len(), dag_id);
                        
                        // Build upstream dependency map (task_id -> list of tasks it depends on)
                        let mut dependencies: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
                        
                        // Initialize all tasks in the map
                        for (task_id, _) in &tasks {
                            dependencies.entry(task_id.clone()).or_insert_with(Vec::new);
                        }
                        
                        // Convert downstream relationships to upstream dependencies
                        for (task_id, downstream_ids) in &tasks {
                            for downstream_id in downstream_ids {
                                dependencies
                                    .entry(downstream_id.clone())
                                    .or_insert_with(Vec::new)
                                    .push(task_id.clone());
                            }
                        }
                        
                        // Perform topological sort
                        let sorted_task_ids = crate::airflow::topological_sort::topological_sort(tasks);
                        
                        // Store both the sorted order and dependencies in environment state
                        let mut app = self.app.lock().unwrap();
                        app.environment_state.set_task_order(dag_id.clone(), sorted_task_ids);
                        app.environment_state.set_task_dependencies(dag_id, dependencies);
                    }
                    Err(e) => {
                        log::error!("Failed to fetch tasks for {}: {}", dag_id, e);
                        // Don't show error popup - task ordering is an enhancement, not critical
                        // Tasks will just appear in the order returned by the API
                    }
                }
            }
            WorkerMessage::GetDagCode { dag_id } => {
                let current_dag: Option<Dag>;
                let env_name: Option<String>;
                {
                    let app = self.app.lock().unwrap();
                    current_dag = app.environment_state.get_active_dag(&dag_id);
                    env_name = app.environment_state.get_active_environment_name().map(|s| s.to_string());
                }

                if let Some(current_dag) = current_dag {
                    let dag_code = client.get_dag_code(&current_dag).await;
                    let mut app = self.app.lock().unwrap();
                    match dag_code {
                        Ok(dag_code) => {
                            if let Some(env) = env_name {
                                app.dagruns.dag_code.set_code(&dag_code, &dag_id, &env);
                            } else {
                                // Fallback: use "default" if no environment name available
                                app.dagruns.dag_code.set_code(&dag_code, &dag_id, "default");
                            }
                        }
                        Err(e) => {
                            app.dags.error_popup =
                                Some(ErrorPopup::from_strings(vec![e.to_string()]));
                        }
                    }
                } else {
                    let mut app = self.app.lock().unwrap();
                    app.dags.error_popup =
                        Some(ErrorPopup::from_strings(vec!["DAG not found".to_string()]));
                }
            }
            WorkerMessage::FetchAndOpenDagCodeInEditor { dag_id } => {
                // Fetch DAG code and open in editor WITHOUT showing the popup
                use crate::app::environment_state::{get_dag_code_filepath, save_dag_code_to_disk};
                
                let (current_dag, env_name) = {
                    let app = self.app.lock().unwrap();
                    (
                        app.environment_state.get_active_dag(&dag_id),
                        app.environment_state.get_active_environment_name().map(|s| s.to_string())
                    )
                };

                // Check if DAG exists
                let Some(current_dag) = current_dag else {
                    let mut app = self.app.lock().unwrap();
                    app.dagruns.error_popup = Some(ErrorPopup::from_strings(vec!["DAG not found".into()]));
                    return Ok(());
                };

                // Fetch DAG code from API
                let dag_code = match client.get_dag_code(&current_dag).await {
                    Ok(code) => code,
                    Err(e) => {
                        log::error!("Failed to fetch DAG code: {}", e);
                        let mut app = self.app.lock().unwrap();
                        app.dagruns.error_popup = Some(ErrorPopup::from_strings(vec![
                            "Failed to fetch DAG code".into(),
                            e.to_string(),
                        ]));
                        return Ok(());
                    }
                };

                let env = env_name.as_deref().unwrap_or("default");
                
                // Get filepath and save to disk
                let filepath = match get_dag_code_filepath(env, &dag_id) {
                    Ok(path) => path,
                    Err(e) => {
                        log::error!("Failed to get DAG code filepath: {}", e);
                        let mut app = self.app.lock().unwrap();
                        app.dagruns.error_popup = Some(ErrorPopup::from_strings(vec![
                            "Failed to get file path for DAG code".into(),
                            e.to_string(),
                        ]));
                        return Ok(());
                    }
                };

                if let Err(e) = save_dag_code_to_disk(&filepath, &dag_code) {
                    log::error!("Failed to save DAG code to disk: {}", e);
                    let mut app = self.app.lock().unwrap();
                    app.dagruns.error_popup = Some(ErrorPopup::from_strings(vec![
                        "Failed to save DAG code to disk".into(),
                        e.to_string(),
                    ]));
                    return Ok(());
                }

                // Success - update file_path and set pending_editor_open flag
                log::info!("DAG code saved to disk, setting flag to open in editor: {}", filepath.display());
                let mut app = self.app.lock().unwrap();
                app.dagruns.dag_code.file_path = Some(filepath);
                app.dagruns.dag_code.pending_editor_open = true;
            }
            WorkerMessage::GetDagDetails { dag_id } => {
                match client.get_dag_details(&dag_id).await {
                    Ok(dag_details) => {
                        let mut app = self.app.lock().unwrap();
                        // Cache in environment state
                        app.environment_state.set_dag_details(dag_id.clone(), dag_details.clone());
                        // Update DAGRuns model
                        app.dagruns.dag_details = Some(dag_details);
                        app.dagruns.init_info_scroll();
                    }
                    Err(e) => {
                        log::error!("Failed to fetch DAG details for {}: {}", dag_id, e);
                        let mut app = self.app.lock().unwrap();
                        app.dagruns.dag_details = None;
                    }
                }
            }

            WorkerMessage::UpdateRecentDagRuns => {
                // Fetch recent runs for DAG health indicators in the list view.
                // Only fetches for unpaused DAGs to optimize bandwidth usage - paused DAGs
                // don't need health monitoring since they won't execute new runs.
                let dag_ids = {
                    let app = self.app.lock().unwrap();
                    app.environment_state
                        .get_active_dags()
                        .iter()
                        .filter(|dag| !dag.is_paused)
                        .map(|dag| dag.dag_id.clone())
                        .collect::<Vec<_>>()
                };
                
                // Skip if no unpaused DAGs are loaded yet
                if dag_ids.is_empty() {
                    return Ok(());
                }
                
                debug!("Fetching recent runs for {} unpaused DAGs", dag_ids.len());
                
                // Fetch last N runs for each unpaused DAG in parallel
                let recent_runs_futures = dag_ids.iter().map(|dag_id| {
                    let dag_id_clone = dag_id.clone();
                    let client_clone = client.clone();
                    async move {
                        let runs = client_clone.list_dagruns(&dag_id_clone).await;
                        (dag_id_clone, runs)
                    }
                });
                
                let results = join_all(recent_runs_futures).await;
                
                // Store results in app state
                let mut app = self.app.lock().unwrap();
                for (dag_id, result) in results {
                    match result {
                        Ok(dag_run_list) => {
                            // Take first RECENT_RUNS_HEALTH_WINDOW runs (API returns newest first)
                            let recent_runs: Vec<_> = dag_run_list.dag_runs
                                .into_iter()
                                .take(crate::app::model::dags::RECENT_RUNS_HEALTH_WINDOW)
                                .collect();
                            debug!("Fetched {} recent runs for DAG {}", recent_runs.len(), dag_id);
                            app.dags.recent_runs.insert(dag_id, recent_runs);
                        }
                        Err(e) => {
                            // Log but don't show error - this is an enhancement feature
                            log::debug!("Failed to fetch recent runs for {}: {}", dag_id, e);
                        }
                    }
                }
            }
            WorkerMessage::ClearDagRun { dag_run_id, dag_id } => {
                debug!("Clearing dag_run: {dag_run_id}");
                let dag_run = client.clear_dagrun(&dag_id, &dag_run_id).await;
                if let Err(e) = dag_run {
                    debug!("Error clearing dag_run: {e}");
                    let mut app = self.app.lock().unwrap();
                    app.dagruns.error_popup = Some(ErrorPopup::from_strings(vec![e.to_string()]));
                }
            }
            WorkerMessage::UpdateTaskLogs {
                dag_id,
                dag_run_id,
                task_id,
                task_try,
                clear,
            } => {
                debug!("Loading first chunk for task: {task_id}, try: {task_try} (highest)");
                
                // Clear if requested
                if clear {
                    let mut app = self.app.lock().unwrap();
                    if let Some(env) = app.environment_state.get_active_environment_mut() {
                        env.clear_task_log(&dag_id, &dag_run_id, &task_id, task_try);
                    }
                }
                
                // Fetch first chunk (no continuation token)
                let log_result = client.get_task_logs_paginated(
                    &dag_id,
                    &dag_run_id,
                    &task_id,
                    task_try,
                    None,  // No token = first chunk
                ).await;
                
                match log_result {
                    Ok(log) => {
                        debug!("Received log chunk: {} bytes, continuation_token: {:?}", 
                            log.content.len(), log.continuation_token);
                        
                        {
                            let mut app = self.app.lock().unwrap();
                            
                            if let Some(env) = app.environment_state.get_active_environment_mut() {
                                env.add_task_log_chunk(&dag_id, &dag_run_id, &task_id, task_try, log);
                            }
                            
                            app.logs.is_loading_initial = false;  // Clear loading flag
                            app.sync_panel_data();
                        }
                        
                        // Persist log to disk after adding chunk
                        self.persist_log_to_disk(&dag_id, &dag_run_id, &task_id, task_try);
                    }
                    Err(e) => {
                        let mut app = self.app.lock().unwrap();
                        app.logs.is_loading_initial = false;  // Clear loading flag on error too
                        app.logs.error_popup = Some(ErrorPopup::from_strings(vec![
                            format!("Failed to load logs: {}", e)
                        ]));
                    }
                }
            }
            WorkerMessage::EnsureTaskLogLoaded {
                dag_id,
                dag_run_id,
                task_id,
                task_try,
            } => {
                // Check if already cached
                let needs_fetch = {
                    let mut app = self.app.lock().unwrap();
                    if let Some(env) = app.environment_state.get_active_environment() {
                        if let Some(_task_log) = env.get_task_log(&dag_id, &dag_run_id, &task_id, task_try) {
                            false  // Cache hit
                        } else {
                            app.logs.is_loading_initial = true;  // Show loading for cache miss
                            true   // Cache miss
                        }
                    } else {
                        false
                    }
                };
                
                if needs_fetch {
                    debug!("Cache miss - fetching first chunk for try {task_try}");
                    
                    let log_result = client.get_task_logs_paginated(
                        &dag_id,
                        &dag_run_id,
                        &task_id,
                        task_try,
                        None,
                    ).await;
                    
                    if let Ok(log) = log_result {
                        {
                            let mut app = self.app.lock().unwrap();
                            if let Some(env) = app.environment_state.get_active_environment_mut() {
                                env.add_task_log_chunk(&dag_id, &dag_run_id, &task_id, task_try, log);
                            }
                            app.logs.is_loading_initial = false;  // Clear loading flag
                            app.sync_panel_data();
                        }
                        
                        // Persist log to disk after adding chunk
                        self.persist_log_to_disk(&dag_id, &dag_run_id, &task_id, task_try);
                        
                        // Evict old attempts from cache (keep last 5)
                        let mut app = self.app.lock().unwrap();
                        let keep_attempts: Vec<u16> = app.logs.lru_cache.iter().copied().collect();
                        if let Some(env) = app.environment_state.get_active_environment_mut() {
                            env.evict_task_logs_not_in_cache(&dag_id, &dag_run_id, &task_id, &keep_attempts);
                        }
                    } else {
                        let mut app = self.app.lock().unwrap();
                        app.logs.is_loading_initial = false;  // Clear on error
                    }
                } else {
                    debug!("Cache hit - using existing chunks for try {task_try}");
                    let mut app = self.app.lock().unwrap();
                    app.sync_panel_data();
                }
            }
            WorkerMessage::LoadMoreTaskLogChunk {
                dag_id,
                dag_run_id,
                task_id,
                task_try,
                continuation_token,
            } => {
                debug!("Loading next chunk with token: {continuation_token}");
                
                let log_result = client.get_task_logs_paginated(
                    &dag_id,
                    &dag_run_id,
                    &task_id,
                    task_try,
                    Some(&continuation_token),
                ).await;
                
                match log_result {
                    Ok(log) => {
                        debug!("LoadMore: Received chunk: {} bytes, continuation_token: {:?}", 
                            log.content.len(), log.continuation_token);
                        
                        {
                            let mut app = self.app.lock().unwrap();
                            if let Some(env) = app.environment_state.get_active_environment_mut() {
                                env.add_task_log_chunk(&dag_id, &dag_run_id, &task_id, task_try, log);
                            }
                            app.logs.is_loading_more = false;
                            app.sync_panel_data();
                        }
                        
                        // Persist log to disk after adding chunk
                        self.persist_log_to_disk(&dag_id, &dag_run_id, &task_id, task_try);
                    }
                    Err(e) => {
                        let mut app = self.app.lock().unwrap();
                        app.logs.is_loading_more = false;
                        app.logs.error_popup = Some(ErrorPopup::from_strings(vec![
                            format!("Failed to load more logs: {}", e),
                            "Existing content is still available.".to_string(),
                        ]));
                    }
                }
            }
            WorkerMessage::MarkDagRun {
                dag_run_id,
                dag_id,
                status,
            } => {
                debug!("Marking dag_run: {dag_run_id}");
                {
                    // Update the local state before sending the request; this way, the UI will update immediately
                    let mut app = self.app.lock().unwrap();
                    app.dagruns.mark_dag_run(&dag_run_id, &status.to_string());
                }
                let dag_run = client
                    .mark_dag_run(&dag_id, &dag_run_id, &status.to_string())
                    .await;
                if let Err(e) = dag_run {
                    debug!("Error marking dag_run: {e}");
                    let mut app = self.app.lock().unwrap();
                    app.dagruns.error_popup = Some(ErrorPopup::from_strings(vec![e.to_string()]));
                }
            }
            WorkerMessage::ClearTaskInstance {
                task_id,
                dag_id,
                dag_run_id,
            } => {
                debug!("Clearing task_instance: {task_id}");
                let task_instance = client
                    .clear_task_instance(&dag_id, &dag_run_id, &task_id)
                    .await;
                if let Err(e) = task_instance {
                    debug!("Error clearing task_instance: {e}");
                    let mut app = self.app.lock().unwrap();
                    app.task_instances.error_popup =
                        Some(ErrorPopup::from_strings(vec![e.to_string()]));
                }
            }
            WorkerMessage::MarkTaskInstance {
                task_id,
                dag_id,
                dag_run_id,
                status,
            } => {
                debug!("Marking task_instance: {task_id}");
                {
                    // Update the local state before sending the request; this way, the UI will update immediately
                    let mut app = self.app.lock().unwrap();
                    app.task_instances
                        .mark_task_instance(&task_id, &status.to_string());
                }
                let task_instance = client
                    .mark_task_instance(&dag_id, &dag_run_id, &task_id, &status.to_string())
                    .await;
                if let Err(e) = task_instance {
                    debug!("Error marking task_instance: {e}");
                    let mut app = self.app.lock().unwrap();
                    app.task_instances.error_popup =
                        Some(ErrorPopup::from_strings(vec![e.to_string()]));
                }
            }
            WorkerMessage::TriggerDagRun { dag_id } => {
                debug!("Triggering dag_run: {dag_id}");
                let dag_run = client.trigger_dag_run(&dag_id, None).await;
                if let Err(e) = dag_run {
                    debug!("Error triggering dag_run: {e}");
                    let mut app = self.app.lock().unwrap();
                    app.dagruns.error_popup = Some(ErrorPopup::from_strings(vec![e.to_string()]));
                }
            }
            WorkerMessage::UpdateImportErrors => {
                // Fetch full import error list (includes count via total_entries)
                let errors = client.list_import_errors().await;
                let mut app = self.app.lock().unwrap();
                match errors {
                    Ok(error_list) => {
                        let count = error_list.total_entries as usize;
                        debug!("Fetched {} import errors", count);
                        
                        // Update error list
                        app.dags.import_error_list = error_list.import_errors.clone();
                        app.dags.filter_import_errors();
                    }
                    Err(e) => {
                        log::debug!("Failed to fetch import errors: {}", e);
                        // Clear everything on failure
                        app.dags.import_error_list = vec![];
                        app.dags.filtered_import_errors.items.clear();
                    }
                }
            }

            WorkerMessage::OpenItem(item) => {
                let url = client.build_open_url(&item)?;
                webbrowser::open(&url).unwrap();
            }
            WorkerMessage::OpenInEditor { .. } => {
                // OpenInEditor is handled in the main event loop (app.rs)
                // where we have access to the terminal for proper suspension
                // This case should never be reached
                log::warn!("OpenInEditor message received in worker (should be handled in main loop)");
            }
            WorkerMessage::UpdateVariables => {
                use crate::airflow::traits::VariableOperations;
                match client.list_variables().await {
                    Ok(variable_collection) => {
                        debug!("Fetched {} variables", variable_collection.variables.len());
                        let mut app = self.app.lock().unwrap();
                        app.dags.all_variables = variable_collection.variables;
                        app.dags.filter_variables();
                    }
                    Err(e) => {
                        log::error!("Failed to fetch variables: {}", e);
                        let mut app = self.app.lock().unwrap();
                        app.dags.error_popup = Some(ErrorPopup::from_strings(vec![
                            format!("Failed to fetch variables: {}", e),
                        ]));
                    }
                }
            }
            WorkerMessage::GetVariableDetail { key } => {
                use crate::airflow::traits::VariableOperations;
                match client.get_variable(&key).await {
                    Ok(variable) => {
                        debug!("Fetched variable detail for key: {}", key);
                        let mut app = self.app.lock().unwrap();
                        app.dags.selected_variable = Some(variable.clone());
                        app.variable_detail.set_variable(variable);
                        app.active_panel = crate::app::state::Panel::VariableDetail;
                    }
                    Err(e) => {
                        log::error!("Failed to fetch variable detail: {}", e);
                        let mut app = self.app.lock().unwrap();
                        app.dags.error_popup = Some(ErrorPopup::from_strings(vec![
                            format!("Failed to fetch variable: {}", e),
                        ]));
                    }
                }
            }
            WorkerMessage::UpdateConnections => {
                use crate::airflow::traits::ConnectionOperations;
                match client.list_connections().await {
                    Ok(connection_collection) => {
                        debug!("Fetched {} connections", connection_collection.connections.len());
                        let mut app = self.app.lock().unwrap();
                        app.dags.all_connections = connection_collection.connections;
                        app.dags.filter_connections();
                    }
                    Err(e) => {
                        log::error!("Failed to fetch connections: {}", e);
                        let mut app = self.app.lock().unwrap();
                        app.dags.error_popup = Some(ErrorPopup::from_strings(vec![
                            format!("Failed to fetch connections: {}", e),
                        ]));
                    }
                }
            }
            WorkerMessage::GetConnectionDetail { connection_id } => {
                use crate::airflow::traits::ConnectionOperations;
                match client.get_connection(&connection_id).await {
                    Ok(connection) => {
                        debug!("Fetched connection detail for id: {}", connection_id);
                        let mut app = self.app.lock().unwrap();
                        app.dags.selected_connection = Some(connection.clone());
                        app.connection_detail.set_connection(connection);
                        app.active_panel = crate::app::state::Panel::ConnectionDetail;
                    }
                    Err(e) => {
                        log::error!("Failed to fetch connection detail: {}", e);
                        let mut app = self.app.lock().unwrap();
                        app.dags.error_popup = Some(ErrorPopup::from_strings(vec![
                            format!("Failed to fetch connection: {}", e),
                        ]));
                    }
                }
            }
            WorkerMessage::GetImportErrorDetail { import_error_id } => {
                // Import errors are already fetched in the list, so we just need to find it
                let mut app = self.app.lock().unwrap();
                if let Some(import_error) = app.dags.import_error_list
                    .iter()
                    .find(|err| err.import_error_id == Some(import_error_id))
                    .cloned()
                {
                    debug!("Found import error detail for id: {}", import_error_id);
                    app.import_error_detail.set_import_error(import_error);
                    app.active_panel = crate::app::state::Panel::ImportErrorDetail;
                } else {
                    log::error!("Import error not found: {}", import_error_id);
                    app.dags.error_popup = Some(ErrorPopup::from_strings(vec![
                        format!("Import error not found: {}", import_error_id),
                    ]));
                }
            }
            // ConfigSelected is handled before the client check above
            WorkerMessage::ConfigSelected(_) => {
                // This should never be reached as it's handled earlier
                unreachable!("ConfigSelected should be handled before client check")
            }
        }

        // Reset loading state at the end
        {
            let mut app = self.app.lock().unwrap();
            app.loading = false;
        }

        Ok(())
    }

    pub fn switch_airflow_client(&mut self, idx: usize) {
        let mut app = self.app.lock().unwrap();
        let selected_config = app.configs.filtered.items[idx].clone();
        let env_name = selected_config.name.clone();

        // Check if environment already exists, if not create it
        if !app.environment_state.environments.contains_key(&env_name) {
            match crate::airflow::client::create_client(&selected_config) {
                Ok(client) => {
                    let env_data = crate::app::environment_state::EnvironmentData::new(client);
                    app.environment_state
                        .add_environment(env_name.clone(), env_data);
                }
                Err(e) => {
                    log::error!("Failed to create client for '{}': {}", env_name, e);
                    app.configs.error_popup = Some(ErrorPopup::from_strings(vec![
                        format!("Failed to create client for '{}'", env_name),
                        format!("Error: {}", e),
                        String::new(),
                        "Common causes:".to_string(),
                        "- Missing environment variable (check ${VAR_NAME} in ~/.flowrs)".to_string(),
                        "- Invalid endpoint URL".to_string(),
                        "- Network connectivity issues".to_string(),
                    ]));
                    return;
                }
            }
        }

        // Set this as the active environment
        app.environment_state
            .set_active_environment(env_name.clone());

        // Reset to Dag panel when switching environments
        app.active_panel = Panel::Dag;

        // Clear the view state but NOT the environment data
        app.clear_state();

        // Sync panel data from the new environment
        app.sync_panel_data();
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            if let Some(message) = self.rx.recv().await {
                // tokio::spawn(async move {
                //     self.process_message(message).await;
                // }); //TODO: check how we can send messages to a pool of workers
                self.process_message(message).await?;
            }
        }
    }
}
