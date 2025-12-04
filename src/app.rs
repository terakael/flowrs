use std::sync::{Arc, Mutex};

use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use events::{custom::FlowrsEvent, generator::EventGenerator};
use log::debug;
use model::Model;
use ratatui::{prelude::Backend, Terminal};
use state::{App, Panel};
use worker::{Worker, WorkerMessage};

use crate::{airflow::client::create_client, ui::draw_ui};

pub mod environment_state;
pub mod events;
pub mod model;
pub mod state;
pub mod worker;

// Wait for in-flight event reads to complete before opening editor
const EVENT_DRAIN_DELAY_MS: u64 = 100;

pub async fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: Arc<Mutex<App>>) -> Result<()> {
    let mut events = EventGenerator::new(200);
    let ui_app = app.clone();
    let worker_app = app.clone();

    let (tx_worker, rx_worker) = tokio::sync::mpsc::channel::<WorkerMessage>(100);

    // Clean up old cached files (logs older than 7 days, DAG code older than 30 days)
    log::info!("Cleaning up old cached files");
    if let Err(e) = environment_state::cleanup_old_logs(7) {
        log::warn!("Failed to cleanup old logs: {}", e);
    }
    if let Err(e) = environment_state::cleanup_old_dag_code(30) {
        log::warn!("Failed to cleanup old DAG code files: {}", e);
    }

    log::info!("Initializing environment state");
    {
        let mut app = app.lock().unwrap();

        // Clone servers to avoid borrow checker issues
        let servers = app.config.servers.clone();

        // Initialize all environments with their clients
        if let Some(servers) = servers {
            for server_config in servers {
                if let Ok(client) = create_client(&server_config) {
                    let env_data = environment_state::EnvironmentData::new(client);
                    app.environment_state
                        .add_environment(server_config.name.clone(), env_data);
                } else {
                    log::error!(
                        "Failed to create client for server '{}'; skipping",
                        server_config.name
                    );
                }
            }
        }
    }

    log::info!("Spawning worker");
    let tx_worker_for_worker = tx_worker.clone();
    tokio::spawn(async move { Worker::new(worker_app, rx_worker, tx_worker_for_worker).run().await });

    loop {
        terminal.draw(|f| {
            debug!("Drawing UI");
            draw_ui(f, &ui_app);
        })?;

        if let Some(event) = events.next().await {
            // First handle panel specific events, and send messages to the event channel
            let (fall_through_event, messages) = {
                let mut app = app.lock().unwrap();
                match app.active_panel {
                    Panel::Config => app.configs.update(&event),
                    Panel::Dag => app.dags.update(&event),
                    Panel::DAGRun => app.dagruns.update(&event),
                    Panel::TaskInstance => app.task_instances.update(&event),
                    Panel::Logs => app.logs.update(&event),
                    Panel::VariableDetail => app.variable_detail.update(&event),
                    Panel::ConnectionDetail => app.connection_detail.update(&event),
                    Panel::ImportErrorDetail => app.import_error_detail.update(&event),
                }
            };

            // Process messages and sync cached data immediately
            let mut additional_messages = Vec::new();
            for message in &messages {
                // Set context IDs and sync cached data before worker processes the message
                {
                    let mut app = app.lock().unwrap();
                    match message {
                        WorkerMessage::UpdateDagRuns { dag_id, clear } => {
                            if *clear {
                                app.dagruns.dag_id = Some(dag_id.clone());
                                // Clear DAG code cache when switching DAGs
                                app.dagruns.dag_code = Default::default();
                                // Sync cached data immediately
                                app.dagruns.all = app.environment_state.get_active_dag_runs(dag_id);
                                app.dagruns.filter_dag_runs();
                                
                                // Pre-fetch task order for this DAG if not already cached
                                // This ensures task instances display in correct order immediately when user enters them
                                if !app.environment_state.has_task_order(dag_id) {
                                    additional_messages.push(WorkerMessage::FetchTaskOrder {
                                        dag_id: dag_id.clone(),
                                    });
                                }
                            }
                        }
                        WorkerMessage::UpdateTaskInstances {
                            dag_id,
                            dag_run_id,
                            clear,
                        } => {
                            if *clear {
                                app.task_instances.dag_id = Some(dag_id.clone());
                                app.task_instances.dag_run_id = Some(dag_run_id.clone());
                                // Sync cached data immediately
                                app.task_instances.all = app
                                    .environment_state
                                    .get_active_task_instances(dag_id, dag_run_id);
                                app.task_instances.filter_task_instances();
                                
                                // Task order should already be cached from when we entered DAGRun panel
                                // If not (e.g., direct navigation), fetch as fallback
                                if !app.environment_state.has_task_order(dag_id) {
                                    additional_messages.push(WorkerMessage::FetchTaskOrder {
                                        dag_id: dag_id.clone(),
                                    });
                                }
                            }
                        }
                        WorkerMessage::UpdateTaskLogs {
                            dag_id,
                            dag_run_id,
                            task_id,
                            task_try,
                            clear,
                        } => {
                            if *clear {
                                app.logs.reset_for_new_task(
                                    dag_id.clone(),
                                    dag_run_id.clone(),
                                    task_id.clone(),
                                    *task_try
                                );
                                // Current log data will be synced after worker completes
                            }
                        }
                        WorkerMessage::EnsureTaskLogLoaded {
                            dag_id,
                            dag_run_id,
                            task_id,
                            task_try,
                        } => {
                            // Sync cached log data immediately to prevent UI lag when switching tries
                            // The current_attempt was already updated in the logs panel before this message was sent
                            // We need to update current_log_data to match before the next render
                            app.logs.current_log_data = app
                                .environment_state
                                .get_active_task_log(dag_id, dag_run_id, task_id, *task_try);
                            
                            // If log is not cached, worker will fetch it and sync again
                            // If log is cached, this sync ensures the UI shows the correct log immediately
                        }
                        _ => {}
                    }
                }
            }

            // Now send messages to worker for async processing
            // Special case: OpenInEditor needs terminal access, handle it here
            for message in messages {
                match message {
                    WorkerMessage::OpenInEditor { filepath } => {
                        // Handle OpenInEditor in the main loop where we have terminal access
                        log::info!("Opening log file in editor: {}", filepath.display());
                        
                        // Pause event generator to stop consuming stdin events
                        events.pause();
                        
                        // Wait a bit for any in-flight event reads to complete
                        tokio::time::sleep(tokio::time::Duration::from_millis(EVENT_DRAIN_DELAY_MS)).await;
                        
                        if let Err(e) = crate::editor::open_in_editor_with_suspend(terminal, &filepath) {
                            log::error!("Failed to open editor: {}", e);
                            // Show error popup
                            let mut app = app.lock().unwrap();
                            app.logs.error_popup = Some(crate::app::model::popup::error::ErrorPopup::from_strings(vec![
                                "Failed to open editor:".into(),
                                e.to_string(),
                            ]));
                        }
                        
                        // Resume event generator
                        events.resume();
                        
                        // Drain any events that were captured while editor was open
                        // The EventGenerator background task may have polled and buffered events
                        // that were meant for the editor. We need to discard them.
                        log::debug!("Draining event queue after editor close");
                        while events.rx_event.try_recv().is_ok() {
                            // Discard buffered events
                        }
                    }
                    _ => {
                        // All other messages go to worker
                        if let Err(e) = tx_worker.send(message).await {
                            log::error!("Failed to send message to worker: {e}");
                        }
                    }
                }
            }
            
            // Send additional messages generated during message processing
            for message in additional_messages {
                if let Err(e) = tx_worker.send(message).await {
                    log::error!("Failed to send additional message to worker: {e}");
                }
            }
            if fall_through_event.is_none() {
                continue;
            }

            // We do this so that when a user switches config,
            // it does not show the previous DAGs (because the Enter event falls through before the existing DAGs are cleared).
            // Not very mindful, not very demure.
            if let Some(FlowrsEvent::Key(KeyEvent {
                code: KeyCode::Enter,
                ..
            })) = fall_through_event
            {
                let mut app = app.lock().unwrap();
                if let Panel::Config = app.active_panel {
                    app.ticks = 0;
                }
            }

            // then handle generic events
            let mut app = app.lock().unwrap();
            if let Some(FlowrsEvent::Tick) = fall_through_event {
                app.ticks += 1;
                app.throbber_state.calc_next();
            }
            if let FlowrsEvent::Key(key) = event {
                // Handle exit key events
                if key.modifiers == KeyModifiers::CONTROL {
                    if let KeyCode::Char('c') = key.code {
                        return Ok(());
                    }
                }
                // Handle other key events
                match key.code {
                    KeyCode::Char('q') => {
                        app.config.write_to_file()?;
                        return Ok(());
                    }
                    KeyCode::Enter | KeyCode::Right => {
                        app.next_panel();
                        app.sync_panel_data();
                    }
                    KeyCode::Esc | KeyCode::Left => {
                        app.previous_panel();
                        app.sync_panel_data();
                    }
                    _ => {}
                }
            }
        }
    }
}
