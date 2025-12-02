use std::collections::HashMap;

use crossterm::event::KeyCode;
use log::debug;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, StatefulWidget, Table, Widget, Wrap};
use time::OffsetDateTime;

use crate::airflow::model::common::{Connection, Dag, DagRun, DagStatistic, ImportError, Variable};
use crate::app::events::custom::FlowrsEvent;
use crate::app::model::popup::dags::commands::DAG_COMMAND_POP_UP;
use crate::ui::common::{create_headers, hash_to_color};
use crate::ui::constants::{ALTERNATING_ROW_COLOR, DEFAULT_STYLE};

use super::popup::commands_help::CommandPopUp;
use super::popup::error::ErrorPopup;
use super::{filter::Filter, Model, StatefulTable, handle_table_scroll_keys, handle_vertical_scroll_keys};
use crate::app::worker::{OpenItem, WorkerMessage};

// Constants for DAG health monitoring and UI layout
/// Number of recent runs to analyze for DAG health indicators
pub const RECENT_RUNS_HEALTH_WINDOW: usize = 7;
/// Width of the state column (for colored square indicator)
const STATE_COLUMN_WIDTH: u16 = 5;
/// Height of the import errors panel
const IMPORT_ERRORS_PANEL_HEIGHT: u16 = 15;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DagPanelTab {
    Dags,
    Variables,
    Connections,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LoadingStatus {
    NotStarted,
    LoadingInitial,
    LoadingMore { current: usize, total: usize },
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DagStatus {
    Failed,    // Highest priority (sort first)
    Running,
    Idle,
    Paused,    // Lowest priority (sort last)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DagFocusedSection {
    ImportErrors,
    DagTable,
}

#[derive(Default)]
pub struct ImportErrorWidget {
    pub cached_lines: Option<Vec<Line<'static>>>,
    pub vertical_scroll: usize,
    pub vertical_scroll_state: ScrollbarState,
}

impl ImportErrorWidget {
    pub fn set_errors(&mut self, errors: &[ImportError]) {
        let new_lines = format_import_errors(errors);
        let content_length = new_lines.len();
        
        // Only reset scroll position if this is the first time loading
        if self.cached_lines.is_none() {
            self.vertical_scroll = 0;
        } else {
            // Ensure scroll position doesn't exceed new content length
            self.vertical_scroll = self.vertical_scroll.min(content_length.saturating_sub(1));
        }
        
        self.cached_lines = Some(new_lines);
        self.vertical_scroll_state = ScrollbarState::default()
            .content_length(content_length)
            .position(self.vertical_scroll);
    }

    pub fn clear(&mut self) {
        self.cached_lines = None;
        self.vertical_scroll = 0;
        self.vertical_scroll_state = ScrollbarState::default();
    }
}

pub struct DagModel {
    // Tab state
    pub active_tab: DagPanelTab,
    
    // DAG tab data
    pub all: Vec<Dag>,
    pub dag_stats: HashMap<String, Vec<DagStatistic>>,
    pub recent_runs: HashMap<String, Vec<DagRun>>,  // Store recent runs for each DAG
    pub filtered: StatefulTable<Dag>,
    pub filter: Filter,
    pub show_paused: bool,
    pub import_error_count: usize,
    pub import_errors: ImportErrorWidget,
    pub import_error_list: Vec<ImportError>,
    pub focused_section: DagFocusedSection,
    
    // Variables tab data
    pub all_variables: Vec<Variable>,
    pub filtered_variables: StatefulTable<Variable>,
    pub selected_variable: Option<Variable>,
    
    // Connections tab data
    pub all_connections: Vec<Connection>,
    pub filtered_connections: StatefulTable<Connection>,
    pub selected_connection: Option<Connection>,
    
    // State preservation for detail views
    pub saved_tab: Option<DagPanelTab>,
    pub saved_variable_selection: Option<usize>,
    pub saved_connection_selection: Option<usize>,
    
    // Shared UI state
    commands: Option<&'static CommandPopUp<'static>>,
    pub error_popup: Option<ErrorPopup>,
    pub loading_status: LoadingStatus,
    ticks: u32,
    event_buffer: Vec<FlowrsEvent>,
}

impl DagModel {
    pub fn new() -> Self {
        DagModel {
            // Tab state - start on DAGs tab
            active_tab: DagPanelTab::Dags,
            
            // DAG tab data
            all: vec![],
            dag_stats: HashMap::new(),
            recent_runs: HashMap::new(),
            filtered: StatefulTable::new(vec![]),
            filter: Filter::new(),
            show_paused: false,
            import_error_count: 0,
            import_errors: ImportErrorWidget::default(),
            import_error_list: vec![],
            focused_section: DagFocusedSection::DagTable,
            
            // Variables tab data
            all_variables: vec![],
            filtered_variables: StatefulTable::new(vec![]),
            selected_variable: None,
            
            // Connections tab data
            all_connections: vec![],
            filtered_connections: StatefulTable::new(vec![]),
            selected_connection: None,
            
            // State preservation for detail views
            saved_tab: None,
            saved_variable_selection: None,
            saved_connection_selection: None,
            
            // Shared UI state
            loading_status: LoadingStatus::NotStarted,
            ticks: 0,
            commands: None,
            error_popup: None,
            event_buffer: vec![],
        }
    }

    pub fn filter_dags(&mut self) {
        let prefix = &self.filter.prefix;
        
        // Step 1: Filter by text search (DAG name or tags) and active status (case-insensitive)
        let mut filtered_dags: Vec<Dag> = match prefix {
            Some(prefix) => {
                let lower_prefix = prefix.to_lowercase();
                self.all
                    .iter()
                    .filter(|dag| {
                        let matches_name = dag.dag_id.to_lowercase().contains(&lower_prefix);
                        let matches_tag = dag.tags.iter().any(|tag| tag.name.to_lowercase().contains(&lower_prefix));
                        (matches_name || matches_tag) && dag.is_active.unwrap_or(false)
                    })
                    .cloned()
                    .collect()
            }
            None => self.all.iter().filter(|dag| dag.is_active.unwrap_or(false)).cloned().collect(),
        };
        
        // Step 2: Filter by pause state
        if !self.show_paused {
            filtered_dags.retain(|dag| !dag.is_paused);
        }
        
        // Step 3: Sort - UNPAUSED FIRST, then alphabetically
        filtered_dags.sort_by(|a, b| {
            // Primary: unpaused DAGs first
            match (a.is_paused, b.is_paused) {
                (false, true) => std::cmp::Ordering::Less,    // a unpaused, b paused -> a first
                (true, false) => std::cmp::Ordering::Greater, // a paused, b unpaused -> b first
                _ => a.dag_id.cmp(&b.dag_id),                 // Both same state -> alphabetical
            }
        });
        
        self.filtered.items = filtered_dags;
    }

    pub fn filter_variables(&mut self) {
        let prefix = &self.filter.prefix;
        
        let mut filtered_variables: Vec<Variable> = match prefix {
            Some(prefix) => {
                let lower_prefix = prefix.to_lowercase();
                self.all_variables
                    .iter()
                    .filter(|var| var.key.to_lowercase().contains(&lower_prefix))
                    .cloned()
                    .collect()
            }
            None => self.all_variables.clone(),
        };
        
        // Sort alphabetically by key
        filtered_variables.sort_by(|a, b| a.key.cmp(&b.key));
        
        self.filtered_variables.items = filtered_variables;
    }

    pub fn filter_connections(&mut self) {
        let prefix = &self.filter.prefix;
        
        let mut filtered_connections: Vec<Connection> = match prefix {
            Some(prefix) => {
                let lower_prefix = prefix.to_lowercase();
                self.all_connections
                    .iter()
                    .filter(|conn| {
                        let matches_id = conn.connection_id.to_lowercase().contains(&lower_prefix);
                        let matches_type = conn.conn_type.to_lowercase().contains(&lower_prefix);
                        matches_id || matches_type
                    })
                    .cloned()
                    .collect()
            }
            None => self.all_connections.clone(),
        };
        
        // Sort alphabetically by connection_id
        filtered_connections.sort_by(|a, b| a.connection_id.cmp(&b.connection_id));
        
        self.filtered_connections.items = filtered_connections;
    }

    pub fn current(&mut self) -> Option<&mut Dag> {
        self.filtered
            .state
            .selected()
            .map(|i| &mut self.filtered.items[i])
    }
    pub fn get_dag_by_id(&self, dag_id: &str) -> Option<&Dag> {
        self.all.iter().find(|dag| dag.dag_id == dag_id)
    }

    pub fn save_state_before_detail_view(&mut self) {
        self.saved_tab = Some(self.active_tab);
        match self.active_tab {
            DagPanelTab::Variables => {
                self.saved_variable_selection = self.filtered_variables.state.selected();
            }
            DagPanelTab::Connections => {
                self.saved_connection_selection = self.filtered_connections.state.selected();
            }
            DagPanelTab::Dags => {}
        }
    }

    pub fn restore_state_from_detail_view(&mut self) {
        if let Some(saved_tab) = self.saved_tab.take() {
            self.active_tab = saved_tab;
            match saved_tab {
                DagPanelTab::Variables => {
                    if let Some(selection) = self.saved_variable_selection.take() {
                        if selection < self.filtered_variables.items.len() {
                            self.filtered_variables.state.select(Some(selection));
                        }
                    }
                }
                DagPanelTab::Connections => {
                    if let Some(selection) = self.saved_connection_selection.take() {
                        if selection < self.filtered_connections.items.len() {
                            self.filtered_connections.state.select(Some(selection));
                        }
                    }
                }
                DagPanelTab::Dags => {}
            }
        }
    }

    fn get_dag_status(&self, dag: &Dag) -> DagStatus {
        if dag.is_paused {
            return DagStatus::Paused;
        }
        
        if let Some(stats) = self.dag_stats.get(&dag.dag_id) {
            let has_failed = stats.iter().any(|s| s.state == "failed" && s.count > 0);
            let has_running = stats.iter().any(|s| s.state == "running" && s.count > 0);
            
            if has_failed {
                return DagStatus::Failed;
            }
            if has_running {
                return DagStatus::Running;
            }
        }
        
        DagStatus::Idle
    }

    /// Get DAG state color based on recent runs (last 7)
    /// - Green: all tasks in all runs succeeded
    /// - Yellow: latest run succeeded, but one of past 7 failed  
    /// - Red: latest run failed
    /// - DarkGray: Paused
    /// - Reset: No data
    fn get_dag_color(&self, dag: &Dag) -> Color {
        if dag.is_paused {
            return Color::DarkGray;  // Gray square for paused DAGs
        }

        if let Some(runs) = self.recent_runs.get(&dag.dag_id) {
            Self::analyze_run_health(runs)
        } else {
            Color::Reset  // No run data
        }
    }

    /// Analyze recent runs to determine DAG health color
    fn analyze_run_health(runs: &[DagRun]) -> Color {
        if runs.is_empty() {
            return Color::Reset;  // No runs yet
        }

        // Runs should be sorted newest first
        let latest_run = &runs[0];
        
        if Self::is_failed_state(&latest_run.state) {
            return Color::Red;  // Latest run failed
        }

        // Treat "running", "queued", "scheduled", and "success" as success states
        if matches!(latest_run.state.as_str(), "success" | "running" | "queued" | "scheduled") {
            if Self::has_recent_failures(&runs[1..]) {
                crate::ui::constants::YELLOW  // Warning - recovered from failure
            } else {
                crate::ui::constants::GREEN  // All good
            }
        } else {
            Color::Reset  // Unknown state
        }
    }

    /// Check if a state represents a failed run
    fn is_failed_state(state: &str) -> bool {
        matches!(state, "failed" | "upstream_failed")
    }

    /// Check if any recent runs have failed
    fn has_recent_failures(runs: &[DagRun]) -> bool {
        runs.iter()
            .take(RECENT_RUNS_HEALTH_WINDOW - 1)  // Check past runs (excluding latest)
            .any(|run| Self::is_failed_state(&run.state))
    }
}

impl Default for DagModel {
    fn default() -> Self {
        Self::new()
    }
}

/// Highlight search term occurrences in text with yellow background
/// Returns a Vec of Spans with matching parts highlighted (case-insensitive search)
fn highlight_search_term<'a>(
    text: &'a str,
    search: Option<&str>,
    base_color: Color,
) -> Vec<Span<'a>> {
    let Some(search) = search else {
        return vec![Span::styled(text, Style::default().fg(base_color))];
    };
    
    if search.is_empty() {
        return vec![Span::styled(text, Style::default().fg(base_color))];
    }
    
    let mut spans = Vec::new();
    let lower_text = text.to_lowercase();
    let lower_search = search.to_lowercase();
    let mut last_end = 0;
    
    // Find all occurrences (case-insensitive)
    for (idx, _) in lower_text.match_indices(&lower_search) {
        // Add non-matching part
        if idx > last_end {
            spans.push(Span::styled(
                &text[last_end..idx],
                Style::default().fg(base_color),
            ));
        }
        
        // Add highlighted matching part with yellow background
        spans.push(Span::styled(
            &text[idx..idx + search.len()],
            Style::default()
                .fg(base_color)
                .bg(crate::ui::constants::BRIGHT_YELLOW),
        ));
        
        last_end = idx + search.len();
    }
    
    // Add remaining text
    if last_end < text.len() {
        spans.push(Span::styled(
            &text[last_end..],
            Style::default().fg(base_color),
        ));
    }
    
    spans
}

impl Model for DagModel {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>) {
        match event {
            FlowrsEvent::Tick => {
                self.ticks += 1;
                
                match &self.loading_status {
                    LoadingStatus::NotStarted => {
                        // Trigger initial load on first tick
                        self.loading_status = LoadingStatus::LoadingInitial;
                        return (
                            Some(FlowrsEvent::Tick),
                            vec![
                                WorkerMessage::UpdateDags {
                                    only_active: !self.show_paused,
                                },
                            ],
                        );
                    }
                    LoadingStatus::LoadingInitial => {
                        // Waiting for initial load to complete
                        return (Some(FlowrsEvent::Tick), vec![]);
                    }
                    LoadingStatus::LoadingMore { current: _, total: _ } => {
                        // Progressive loading is now handled by the worker via callbacks
                        // The worker automatically triggers the next batch when the previous completes
                        // No need to check ticks or trigger from here
                    }
                    LoadingStatus::Complete => {
                        // All DAGs loaded - no automatic refresh, use 'r' key to refresh manually
                    }
                }
                
                (Some(FlowrsEvent::Tick), vec![])
            }
            FlowrsEvent::Key(key_event) => {
                // Handle Escape key with multi-stage behavior
                if key_event.code == KeyCode::Esc {
                    if self.filter.is_enabled() {
                        // Filter dialogue is open: close it and clear any filter
                        self.filter.reset();  // Closes dialogue and clears prefix
                        match self.active_tab {
                            DagPanelTab::Dags => self.filter_dags(),
                            DagPanelTab::Variables => self.filter_variables(),
                            DagPanelTab::Connections => self.filter_connections(),
                        }
                        return (None, vec![]);
                    } else if self.filter.prefix.is_some() {
                        // Filter dialogue closed but filter is applied: clear the filter
                        self.filter.prefix = None;
                        match self.active_tab {
                            DagPanelTab::Dags => self.filter_dags(),
                            DagPanelTab::Variables => self.filter_variables(),
                            DagPanelTab::Connections => self.filter_connections(),
                        }
                        return (None, vec![]);
                    }
                    // else: no filter active, fall through to go back to environment page
                }
                
                if self.filter.is_enabled() {
                    self.filter.update(key_event);
                    // Apply filter based on active tab
                    match self.active_tab {
                        DagPanelTab::Dags => self.filter_dags(),
                        DagPanelTab::Variables => self.filter_variables(),
                        DagPanelTab::Connections => self.filter_connections(),
                    }
                    return (None, vec![]);
                } else if let Some(_error_popup) = &mut self.error_popup {
                    match key_event.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            self.error_popup = None;
                        }
                        _ => (),
                    }
                    return (None, vec![]);
                } else if let Some(_commands) = &mut self.commands {
                    match key_event.code {
                        KeyCode::Char('q' | '?') | KeyCode::Esc => {
                            self.commands = None;
                        }
                        _ => (),
                    }
                } else {
                    // Handle scrolling based on focused section and active tab
                    let handled = match (self.active_tab, self.focused_section) {
                        (DagPanelTab::Dags, DagFocusedSection::ImportErrors) => {
                            let max_lines = self.import_errors.cached_lines.as_ref().map(|lines| lines.len());
                            handle_vertical_scroll_keys(
                                &mut self.import_errors.vertical_scroll,
                                &mut self.import_errors.vertical_scroll_state,
                                key_event,
                                max_lines,
                            )
                        }
                        (DagPanelTab::Dags, DagFocusedSection::DagTable) => {
                            handle_table_scroll_keys(&mut self.filtered, key_event)
                        }
                        (DagPanelTab::Variables, _) => {
                            handle_table_scroll_keys(&mut self.filtered_variables, key_event)
                        }
                        (DagPanelTab::Connections, _) => {
                            handle_table_scroll_keys(&mut self.filtered_connections, key_event)
                        }
                    };
                    
                    if handled {
                        return (None, vec![]);
                    }
                    
                    match key_event.code {
                        KeyCode::Char('H') => {
                            // Shift+H - Previous tab
                            self.active_tab = match self.active_tab {
                                DagPanelTab::Dags => DagPanelTab::Dags, // Stay on first tab
                                DagPanelTab::Variables => {
                                    // When switching back to DAGs tab, reset focus to table
                                    self.focused_section = DagFocusedSection::DagTable;
                                    DagPanelTab::Dags
                                }
                                DagPanelTab::Connections => DagPanelTab::Variables,
                            };
                            // Lazy load: trigger data load if tab hasn't been loaded yet
                            let messages = match self.active_tab {
                                DagPanelTab::Variables if self.all_variables.is_empty() => {
                                    vec![WorkerMessage::UpdateVariables]
                                }
                                DagPanelTab::Connections if self.all_connections.is_empty() => {
                                    vec![WorkerMessage::UpdateConnections]
                                }
                                _ => vec![],
                            };
                            return (None, messages);
                        }
                        KeyCode::Char('L') => {
                            // Shift+L - Next tab
                            self.active_tab = match self.active_tab {
                                DagPanelTab::Dags => DagPanelTab::Variables,
                                DagPanelTab::Variables => DagPanelTab::Connections,
                                DagPanelTab::Connections => DagPanelTab::Connections, // Stay on last tab
                            };
                            // Lazy load: trigger data load if tab hasn't been loaded yet
                            let messages = match self.active_tab {
                                DagPanelTab::Variables if self.all_variables.is_empty() => {
                                    vec![WorkerMessage::UpdateVariables]
                                }
                                DagPanelTab::Connections if self.all_connections.is_empty() => {
                                    vec![WorkerMessage::UpdateConnections]
                                }
                                _ => vec![],
                            };
                            return (None, messages);
                        }
                        KeyCode::Char('J') => {
                            // Switch focus to DAG table (down) - only relevant for DAGs tab with import errors
                            if self.active_tab == DagPanelTab::Dags {
                                self.focused_section = DagFocusedSection::DagTable;
                            }
                        }
                        KeyCode::Char('K') => {
                            // Switch focus to ImportErrors panel (up) - only relevant for DAGs tab
                            if self.active_tab == DagPanelTab::Dags && self.import_errors.cached_lines.is_some() {
                                self.focused_section = DagFocusedSection::ImportErrors;
                            }
                        }
                        KeyCode::Char('G') => {
                            // Jump to bottom of active section
                            match (self.active_tab, self.focused_section) {
                                (DagPanelTab::Dags, DagFocusedSection::ImportErrors) => {
                                    if let Some(cached_lines) = &self.import_errors.cached_lines {
                                        self.import_errors.vertical_scroll = cached_lines.len().saturating_sub(1);
                                        self.import_errors.vertical_scroll_state = 
                                            self.import_errors.vertical_scroll_state.position(self.import_errors.vertical_scroll);
                                    }
                                }
                                (DagPanelTab::Dags, DagFocusedSection::DagTable) => {
                                    self.filtered.state.select_last();
                                }
                                (DagPanelTab::Variables, _) => {
                                    self.filtered_variables.state.select_last();
                                }
                                (DagPanelTab::Connections, _) => {
                                    self.filtered_connections.state.select_last();
                                }
                            }
                        }
                        KeyCode::Char('p') => {
                            // Toggle showing paused DAGs
                            self.show_paused = !self.show_paused;
                            self.filter_dags();
                            // Refresh from API with new only_active filter
                            return (
                                None,
                                vec![
                                    WorkerMessage::UpdateDags {
                                        only_active: !self.show_paused,
                                    },
                                    WorkerMessage::UpdateDagStats { clear: true },
                                ],
                            );
                        }
                        KeyCode::Char('P') => {
                            // Pause/unpause the selected DAG (Shift+P)
                            match self.current() {
                                Some(dag) => {
                                    let current_state = dag.is_paused;
                                    dag.is_paused = !current_state;
                                    return (
                                        None,
                                        vec![WorkerMessage::ToggleDag {
                                            dag_id: dag.dag_id.clone(),
                                            is_paused: current_state,
                                        }],
                                    );
                                }
                                None => {
                                    self.error_popup = Some(ErrorPopup::from_strings(vec![
                                        "No DAG selected to pause/resume".to_string(),
                                    ]));
                                }
                            }
                        }
                        KeyCode::Char('/') => {
                            self.filter.toggle();
                            // Apply filter based on active tab
                            match self.active_tab {
                                DagPanelTab::Dags => self.filter_dags(),
                                DagPanelTab::Variables => self.filter_variables(),
                                DagPanelTab::Connections => self.filter_connections(),
                            }
                        }
                        KeyCode::Char('?') => {
                            self.commands = Some(&*DAG_COMMAND_POP_UP);
                        }
                        KeyCode::Enter => {
                            match self.active_tab {
                                DagPanelTab::Dags => {
                                    if let Some(selected_dag) = self.current().map(|dag| dag.dag_id.clone())
                                    {
                                        debug!("Selected dag: {selected_dag}");
                                        return (
                                            Some(FlowrsEvent::Key(*key_event)),
                                            vec![
                                                WorkerMessage::UpdateDagRuns {
                                                    dag_id: selected_dag.clone(),
                                                    clear: true,
                                                },
                                                WorkerMessage::GetDagDetails {
                                                    dag_id: selected_dag,
                                                },
                                            ],
                                        );
                                    }
                                    self.error_popup = Some(ErrorPopup::from_strings(vec![
                                        "No DAG selected to view DAG Runs".to_string(),
                                    ]));
                                }
                                DagPanelTab::Variables => {
                                    if let Some(selected_idx) = self.filtered_variables.state.selected() {
                                        if let Some(variable) = self.filtered_variables.items.get(selected_idx) {
                                            let key = variable.key.clone();
                                            debug!("Selected variable: {}", key);
                                            // Save state before navigating to detail view
                                            self.save_state_before_detail_view();
                                            return (
                                                Some(FlowrsEvent::Key(*key_event)),
                                                vec![WorkerMessage::GetVariableDetail { key }],
                                            );
                                        }
                                    }
                                    self.error_popup = Some(ErrorPopup::from_strings(vec![
                                        "No variable selected to view details".to_string(),
                                    ]));
                                }
                                DagPanelTab::Connections => {
                                    if let Some(selected_idx) = self.filtered_connections.state.selected() {
                                        if let Some(connection) = self.filtered_connections.items.get(selected_idx) {
                                            let connection_id = connection.connection_id.clone();
                                            debug!("Selected connection: {}", connection_id);
                                            // Save state before navigating to detail view
                                            self.save_state_before_detail_view();
                                            return (
                                                Some(FlowrsEvent::Key(*key_event)),
                                                vec![WorkerMessage::GetConnectionDetail { connection_id }],
                                            );
                                        }
                                    }
                                    self.error_popup = Some(ErrorPopup::from_strings(vec![
                                        "No connection selected to view details".to_string(),
                                    ]));
                                }
                            }
                        }
                        KeyCode::Char('g') => {
                            if let Some(FlowrsEvent::Key(key_event)) = self.event_buffer.pop() {
                                if key_event.code == KeyCode::Char('g') {
                                    // Jump to top of active section
                                    match (self.active_tab, self.focused_section) {
                                        (DagPanelTab::Dags, DagFocusedSection::ImportErrors) => {
                                            self.import_errors.vertical_scroll = 0;
                                            self.import_errors.vertical_scroll_state = 
                                                self.import_errors.vertical_scroll_state.position(0);
                                        }
                                        (DagPanelTab::Dags, DagFocusedSection::DagTable) => {
                                            self.filtered.state.select_first();
                                        }
                                        (DagPanelTab::Variables, _) => {
                                            self.filtered_variables.state.select_first();
                                        }
                                        (DagPanelTab::Connections, _) => {
                                            self.filtered_connections.state.select_first();
                                        }
                                    }
                                } else {
                                    self.event_buffer.push(FlowrsEvent::Key(key_event));
                                }
                            } else {
                                self.event_buffer.push(FlowrsEvent::Key(*key_event));
                            }
                        }
                        KeyCode::Char('o') => {
                            if let Some(dag) = self.current() {
                                debug!("Selected dag: {}", dag.dag_id);
                                return (
                                    Some(FlowrsEvent::Key(*key_event)),
                                    vec![WorkerMessage::OpenItem(OpenItem::Dag {
                                        dag_id: dag.dag_id.clone(),
                                    })],
                                );
                            }
                            self.error_popup = Some(ErrorPopup::from_strings(vec![
                                "No DAG selected to open in the browser".to_string(),
                            ]));
                        }
                        KeyCode::Char('r') => {
                            // Manual refresh - trigger fresh data load for active tab
                            match self.active_tab {
                                DagPanelTab::Dags => {
                                    self.loading_status = LoadingStatus::NotStarted;
                                    return (
                                        None,
                                        vec![
                                            WorkerMessage::UpdateDags {
                                                only_active: !self.show_paused,
                                            },
                                        ],
                                    );
                                }
                                DagPanelTab::Variables => {
                                    return (None, vec![WorkerMessage::UpdateVariables]);
                                }
                                DagPanelTab::Connections => {
                                    return (None, vec![WorkerMessage::UpdateConnections]);
                                }
                            }
                        }
                        _ => return (Some(FlowrsEvent::Key(*key_event)), vec![]), // if no match, return the event
                    }
                    return (None, vec![]);
                }
                (None, vec![])
            }
            FlowrsEvent::Mouse => (Some(event.clone()), vec![]),
        }
    }
}

impl DagModel {
    fn create_tab_title(&self) -> Line<'static> {
        // Create tab labels with highlighting for active tab
        let tabs = vec![
            (DagPanelTab::Dags, "DAGs"),
            (DagPanelTab::Variables, "Variables"),
            (DagPanelTab::Connections, "Connections"),
        ];
        
        let mut spans = Vec::new();
        for (i, (tab, label)) in tabs.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw(" "));
            }
            
            if *tab == self.active_tab {
                // Active tab: highlighted with cyan and bold
                spans.push(Span::styled(
                    format!("[{}]", label),
                    Style::default()
                        .fg(crate::ui::constants::CYAN)
                        .add_modifier(Modifier::BOLD)
                ));
            } else {
                // Inactive tabs: gray and not bold
                spans.push(Span::styled(
                    format!("[{}]", label),
                    Style::default().fg(Color::DarkGray)
                ));
            }
        }
        
        Line::from(spans)
    }
    
    fn render_tabbed_container(&mut self, area: Rect, buf: &mut Buffer) {
        let selected_style = crate::ui::constants::SELECTED_STYLE;
        let border_style = if self.active_tab != DagPanelTab::Dags || self.focused_section == DagFocusedSection::DagTable {
            DEFAULT_STYLE.fg(Color::Cyan) // Highlight when focused (or when not on DAGs tab)
        } else {
            DEFAULT_STYLE // Not focused (only happens on DAGs tab when import errors are focused)
        };

        // Create tab title with highlighting
        let tab_title = self.create_tab_title();
        
        // Get showing/total counts based on active tab
        let (showing_count, total_count) = match self.active_tab {
            DagPanelTab::Dags => {
                let showing = self.filtered.items.len();
                let total = self.all.iter().filter(|d| d.is_active.unwrap_or(false)).count();
                (showing, total)
            }
            DagPanelTab::Variables => {
                (self.filtered_variables.items.len(), self.all_variables.len())
            }
            DagPanelTab::Connections => {
                (self.filtered_connections.items.len(), self.all_connections.len())
            }
        };
        
        let status_text = match &self.loading_status {
            LoadingStatus::LoadingInitial => " (loading...)".to_string(),
            LoadingStatus::LoadingMore { current, total } => 
                format!(" (loaded {}/{})", current, total),
            LoadingStatus::Complete | LoadingStatus::NotStarted => String::new(),
        };
        
        let count_text = format!("(showing {} of {}){}", showing_count, total_count, status_text);
        
        // Render appropriate table based on active tab
        match self.active_tab {
            DagPanelTab::Dags => {
                let headers = ["State", "Name", "Schedule", "Next Run", "Tags"];
                let header_row = create_headers(headers);
                let header = Row::new(header_row)
                    .style(crate::ui::constants::HEADER_STYLE);
                let search_term = self.filter.prefix.as_deref();
                let rows =
                    self.filtered.items.iter().enumerate().map(|(idx, item)| {
                        let color = self.get_dag_color(item);
                        let text_color = if item.is_paused {
                            Color::DarkGray
                        } else {
                            Color::Reset
                        };
                        
                        Row::new(vec![
                            Line::from(Span::styled("■", Style::default().fg(color))),
                            Line::from(highlight_search_term(&item.dag_id, search_term, text_color)),
                            {
                                let schedule = item.timetable_description.as_deref().unwrap_or("None");
                                let schedule_text = if schedule.starts_with("Never") {
                                    "Never"
                                } else {
                                    schedule
                                };
                                if schedule_text == "Never" || schedule_text == "None" {
                                    Line::from(Span::styled(schedule_text, Style::default().fg(Color::DarkGray)))
                                } else {
                                    Line::from(schedule_text)
                                }
                            },
                            {
                                if let Some(date) = item.next_dagrun_create_after {
                                    Line::from(convert_datetimeoffset_to_human_readable_remaining_time(date))
                                } else {
                                    Line::from(Span::styled("None", Style::default().fg(Color::DarkGray)))
                                }
                            },
                            {
                                if item.tags.is_empty() {
                                    Line::from("")
                                } else {
                                    let mut spans = Vec::new();
                                    for (i, tag) in item.tags.iter().enumerate() {
                                        if i > 0 {
                                            spans.push(Span::raw(", "));
                                        }
                                        let tag_color = hash_to_color(&tag.name);
                                        let highlighted_spans = highlight_search_term(&tag.name, search_term, tag_color);
                                        spans.extend(highlighted_spans);
                                    }
                                    Line::from(spans)
                                }
                            },
                        ])
                        .style({
                            let base_style = if (idx % 2) == 0 {
                                DEFAULT_STYLE
                            } else {
                                DEFAULT_STYLE.bg(ALTERNATING_ROW_COLOR)
                            };
                            if item.is_paused {
                                base_style.fg(Color::DarkGray)
                            } else {
                                base_style
                            }
                        })
                    });
                
                let t = Table::new(
                    rows,
                    &[
                        Constraint::Length(STATE_COLUMN_WIDTH),
                        Constraint::Fill(2),
                        Constraint::Length(10),
                        Constraint::Length(10),
                        Constraint::Fill(1),
                    ],
                )
                .header(header)
                .block(
                    Block::default()
                        .border_type(BorderType::Rounded)
                        .borders(Borders::ALL)
                        .title(tab_title)
                        .title_bottom(Line::from(vec![
                            Span::styled(count_text, Style::default()),
                            Span::raw(" "),
                            Span::styled("Press <?> for commands", Style::default().fg(Color::DarkGray)),
                        ]))
                        .border_style(border_style)
                        .style(DEFAULT_STYLE),
                )
                .row_highlight_style(selected_style);

                StatefulWidget::render(t, area, buf, &mut self.filtered.state);
            }
            DagPanelTab::Variables => {
                let headers = ["Key", "Value (Preview)"];
                let header_row = create_headers(headers);
                let header = Row::new(header_row)
                    .style(crate::ui::constants::HEADER_STYLE);
                let search_term = self.filter.prefix.as_deref();
                
                let rows = self.filtered_variables.items.iter().enumerate().map(|(idx, item)| {
                    // Note: Airflow API doesn't return values in the list endpoint for security
                    // Users need to press Enter to view the full value
                    let value_display = if item.value.is_some() {
                        // If we have a value (from detail fetch), show truncated version
                        let v = item.value.as_ref().unwrap();
                        let cleaned = v.replace('\n', " ").replace('\r', "");
                        if cleaned.len() > 80 {
                            format!("{}...", &cleaned[..80])
                        } else {
                            cleaned
                        }
                    } else {
                        // Value not loaded - show hint
                        "Press Enter to view".to_string()
                    };
                    
                    Row::new(vec![
                        Line::from(highlight_search_term(&item.key, search_term, Color::Reset)),
                        Line::from(Span::styled(value_display, Style::default().fg(Color::DarkGray))),
                    ])
                    .style(if (idx % 2) == 0 {
                        DEFAULT_STYLE
                    } else {
                        DEFAULT_STYLE.bg(ALTERNATING_ROW_COLOR)
                    })
                });
                
                let t = Table::new(
                    rows, 
                    &[
                        Constraint::Fill(1),
                        Constraint::Fill(2),
                    ]
                )
                    .header(header)
                    .block(
                        Block::default()
                            .border_type(BorderType::Rounded)
                            .borders(Borders::ALL)
                            .title(tab_title)
                            .title_bottom(Line::from(vec![
                                Span::styled(count_text, Style::default()),
                                Span::raw(" "),
                                Span::styled("Press <?> for commands", Style::default().fg(Color::DarkGray)),
                            ]))
                            .border_style(border_style)
                            .style(DEFAULT_STYLE),
                    )
                    .row_highlight_style(selected_style);
                
                StatefulWidget::render(t, area, buf, &mut self.filtered_variables.state);
            }
            DagPanelTab::Connections => {
                let headers = ["ID", "Type", "Host", "Login", "Schema", "Port"];
                let header_row = create_headers(headers);
                let header = Row::new(header_row)
                    .style(crate::ui::constants::HEADER_STYLE);
                let search_term = self.filter.prefix.as_deref();
                
                let rows = self.filtered_connections.items.iter().enumerate().map(|(idx, item)| {
                    // Use the same color mapping as tags for connection types
                    let type_color = hash_to_color(&item.conn_type);
                    
                    Row::new(vec![
                        Line::from(highlight_search_term(&item.connection_id, search_term, Color::Reset)),
                        Line::from(highlight_search_term(&item.conn_type, search_term, type_color)),
                        Line::from(item.host.as_deref().unwrap_or("-")),
                        Line::from(item.login.as_deref().unwrap_or("-")),
                        Line::from(item.schema.as_deref().unwrap_or("-")),
                        Line::from(item.port.map_or("-".to_string(), |p| p.to_string())),
                    ])
                    .style(if (idx % 2) == 0 {
                        DEFAULT_STYLE
                    } else {
                        DEFAULT_STYLE.bg(ALTERNATING_ROW_COLOR)
                    })
                });
                
                let t = Table::new(
                    rows,
                    &[
                        Constraint::Fill(2),
                        Constraint::Fill(1),
                        Constraint::Fill(2),
                        Constraint::Fill(1),
                        Constraint::Fill(1),
                        Constraint::Length(6),
                    ],
                )
                .header(header)
                .block(
                    Block::default()
                        .border_type(BorderType::Rounded)
                        .borders(Borders::ALL)
                        .title(tab_title)
                        .title_bottom(Line::from(vec![
                            Span::styled(count_text, Style::default()),
                            Span::raw(" "),
                            Span::styled("Press <?> for commands", Style::default().fg(Color::DarkGray)),
                        ]))
                        .border_style(border_style)
                        .style(DEFAULT_STYLE),
                )
                .row_highlight_style(selected_style);
                
                StatefulWidget::render(t, area, buf, &mut self.filtered_connections.state);
            }
        }
    }
}

impl Widget for &mut DagModel {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Handle filter at bottom if enabled
        let base_area = if self.filter.is_enabled() {
            let rects = Layout::default()
                .constraints([Constraint::Fill(90), Constraint::Max(3)].as_ref())
                .margin(0)
                .split(area);

            self.filter.render(rects[1], buf);
            rects[0]
        } else {
            area
        };

        // Split into ImportErrors section (always visible) and main tabbed container
        let (errors_area, main_area) = if self.import_errors.cached_lines.is_some() {
            let rects = Layout::default()
                .constraints([Constraint::Length(IMPORT_ERRORS_PANEL_HEIGHT), Constraint::Min(0)].as_ref())
                .split(base_area);
            (Some(rects[0]), rects[1])
        } else {
            (None, base_area)
        };

        // Render ImportErrors section if available (separate container, always visible)
        if let (Some(errors_area), Some(cached_lines)) = (errors_area, &self.import_errors.cached_lines) {
            let border_style = if self.active_tab == DagPanelTab::Dags && self.focused_section == DagFocusedSection::ImportErrors {
                DEFAULT_STYLE.fg(Color::Cyan) // Highlight when focused
            } else {
                DEFAULT_STYLE.fg(crate::ui::constants::RED) // Red border when not focused
            };

            let errors_block = Block::default()
                .border_type(BorderType::Rounded)
                .borders(Borders::ALL)
                .title(format!("Import Errors ({})", self.import_error_count))
                .border_style(border_style)
                .style(DEFAULT_STYLE)
                .title_style(DEFAULT_STYLE.fg(crate::ui::constants::RED).add_modifier(Modifier::BOLD));

            let errors_text = Paragraph::new(cached_lines.clone())
                .block(errors_block)
                .style(DEFAULT_STYLE)
                .wrap(Wrap { trim: false })
                .scroll((self.import_errors.vertical_scroll as u16, 0));

            errors_text.render(errors_area, buf);

            // Render scrollbar for errors section
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(Some("↑"))
                .end_symbol(Some("↓"));
            let mut scrollbar_state = self.import_errors.vertical_scroll_state.clone();
            scrollbar.render(errors_area, buf, &mut scrollbar_state);
        }
        
        // Render the main tabbed container (single container with tabs in title)
        self.render_tabbed_container(main_area, buf);

        // Render popups (they float over everything)
        if let Some(commands) = &self.commands {
            commands.render(area, buf);
        }

        if let Some(error_popup) = &self.error_popup {
            error_popup.render(area, buf);
        }
    }
}

/// helper function to create a centered rect using up certain percentage of the available rect `r`
#[allow(dead_code)]
fn popup_area(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let vertical = Layout::vertical([Constraint::Percentage(percent_y)]).flex(Flex::Center);
    let horizontal = Layout::horizontal([Constraint::Percentage(percent_x)]).flex(Flex::Center);
    let [area] = vertical.areas(area);
    let [area] = horizontal.areas(area);
    area
}

fn convert_datetimeoffset_to_human_readable_remaining_time(dt: OffsetDateTime) -> String {
    let now = OffsetDateTime::now_utc();
    let duration = dt.unix_timestamp() - now.unix_timestamp();
    #[allow(clippy::cast_sign_loss)]
    let duration = if duration < 0 { 0 } else { duration as u64 };
    let days = duration / (24 * 3600);
    let hours = (duration % (24 * 3600)) / 3600;
    let minutes = (duration % 3600) / 60;
    let seconds = duration % 60;

    match duration {
        0..=59 => format!("{seconds}s"),
        60..=3599 => format!("{minutes}m"),
        3600..=86_399 => format!("{hours}h {minutes:02}m"),
        _ => format!("{days}d {hours:02}h {minutes:02}m"),
    }
}

/// Format import errors for display in the ImportErrors panel
fn format_import_errors(errors: &[ImportError]) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    
    if errors.is_empty() {
        lines.push(Line::from(Span::styled(
            "No import errors",
            Style::default().fg(Color::DarkGray),
        )));
        return lines;
    }
    
    for (i, error) in errors.iter().enumerate() {
        // Error header with filename
        let filename = error.filename.as_deref().unwrap_or("Unknown file");
        lines.push(Line::from(Span::styled(
            format!("═══ Error {} of {} ═══", i + 1, errors.len()),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )));
        
        lines.push(Line::from(vec![
            Span::styled("File: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(filename.to_string()),
        ]));
        
        // Timestamp
        if let Some(ts) = &error.timestamp {
            lines.push(Line::from(vec![
                Span::styled("Time: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(ts.to_string()),
            ]));
        }
        
        lines.push(Line::from("")); // Separator
        
        // Stack trace
        lines.push(Line::from(Span::styled(
            "Stack Trace:",
            Style::default().add_modifier(Modifier::BOLD).add_modifier(Modifier::UNDERLINED),
        )));
        
        if let Some(stack) = &error.stack_trace {
            for line in stack.lines() {
                lines.push(Line::from(line.to_string()));
            }
        } else {
            lines.push(Line::from(Span::styled(
                "No stack trace available",
                Style::default().fg(Color::DarkGray),
            )));
        }
        
        // Add spacing between errors
        if i < errors.len() - 1 {
            lines.push(Line::from(""));
            lines.push(Line::from(""));
        }
    }
    
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // TODO: This is poor test... should make it deterministic
    fn test_convert_datetimeoffset_to_human_readable_remaining_time() {
        let now = OffsetDateTime::now_utc();
        let dt = now + time::Duration::seconds(60);
        assert_eq!(
            convert_datetimeoffset_to_human_readable_remaining_time(dt),
            "1m"
        );
        let dt = now + time::Duration::seconds(3600);
        assert_eq!(
            convert_datetimeoffset_to_human_readable_remaining_time(dt),
            "1h 00m"
        );
    }
}
