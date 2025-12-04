use std::collections::HashMap;

use crossterm::event::{KeyCode, KeyModifiers};
use log::debug;
use once_cell::sync::Lazy;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Row, StatefulWidget, Table, Widget};
use regex::Regex;
use time::OffsetDateTime;

use crate::airflow::model::common::{Connection, Dag, DagRun, ImportError, Variable};
use crate::app::events::custom::FlowrsEvent;
use crate::app::model::popup::dags::commands::create_dag_command_popup;
use crate::ui::common::{format_and_highlight_json, get_state_icon, hash_to_color, highlight_search_text};
use crate::ui::constants::{ALTERNATING_ROW_COLOR, DEFAULT_STYLE, HEADER_STYLE, RED};

use super::popup::commands_help::CommandPopUp;
use super::popup::error::ErrorPopup;
use super::sortable_table::{CustomSort, SortableTable};
use super::{filter::Filter, handle_command_popup_events, Model, HALF_PAGE_SIZE};
use crate::app::worker::{OpenItem, WorkerMessage};
use std::cmp::Ordering;

// Constants for DAG health monitoring and UI layout
/// Number of recent runs to analyze for DAG health indicators
pub const RECENT_RUNS_HEALTH_WINDOW: usize = 7;
/// Width of the state column (for colored square indicator)
const STATE_COLUMN_WIDTH: u16 = 5;

// Constants for schedule frequency calculations (in seconds)
const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 3_600;
const SECONDS_PER_DAY: u64 = 86_400;
const SECONDS_PER_WEEK: u64 = 604_800;
const SECONDS_PER_MONTH: u64 = 2_592_000;  // ~30 days (approximate)
const SECONDS_PER_YEAR: u64 = 31_536_000;  // 365 days (approximate)
const UNKNOWN_SCHEDULE_FREQUENCY: u64 = 999_999;  // Fallback for unparseable schedules

// Lazy-initialized regex pattern for parsing "every X unit" schedule descriptions
static SCHEDULE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"every\s+(\d+)\s+(minute|hour|day|week|month|year)s?").expect("Invalid regex pattern")
});

// State priority constants for sorting (lower = higher urgency)
const PRIORITY_FAILED: u8 = 0;      // Failed - requires immediate attention
const PRIORITY_RUNNING: u8 = 1;     // Currently running
const PRIORITY_RECOVERED: u8 = 2;   // Recently failed but recovered
const PRIORITY_SUCCESS: u8 = 3;     // All runs successful
const PRIORITY_UNKNOWN: u8 = 4;     // No run data available
const PRIORITY_PAUSED: u8 = 5;      // Paused DAGs (lowest priority)

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DagPanelTab {
    Dags,
    Variables,
    Connections,
    ImportErrors,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LoadingStatus {
    NotStarted,
    LoadingInitial,
    LoadingMore { current: usize, total: usize },
    Complete,
}

// CustomSort implementations for DAG panel tables

impl CustomSort for Dag {
    fn column_value(&self, column_index: usize) -> String {
        match column_index {
            0 => if self.is_paused { "paused" } else { "active" }.to_string(), // State
            1 => self.dag_id.clone(), // Name
            2 => self.timetable_description.clone().unwrap_or_default(), // Schedule
            3 => self.next_dagrun_create_after.map(|d| d.to_string()).unwrap_or_default(), // Next Run
            4 => self.tags.first().map(|t| t.name.clone()).unwrap_or_default(), // Tags
            _ => String::new(),
        }
    }
    
    fn comparator(column_index: usize) -> Option<fn(&Self, &Self) -> Ordering> {
        match column_index {
            0 => Some(|a: &Dag, b: &Dag| {
                // Sort by computed state priority (lower priority value = higher urgency)
                let priority_a = a.computed_state_priority.unwrap_or(255);
                let priority_b = b.computed_state_priority.unwrap_or(255);
                priority_a.cmp(&priority_b)
            }),
            2 => Some(|a: &Dag, b: &Dag| {
                // Sort by schedule frequency (lower value = more frequent)
                let freq_a = a.computed_schedule_frequency.unwrap_or(u64::MAX);
                let freq_b = b.computed_schedule_frequency.unwrap_or(u64::MAX);
                freq_a.cmp(&freq_b)
            }),
            3 => Some(|a: &Dag, b: &Dag| {
                // Sort by next run time - soonest runs first (ascending)
                match (&a.next_dagrun_create_after, &b.next_dagrun_create_after) {
                    (Some(date_a), Some(date_b)) => date_a.cmp(date_b),
                    (Some(_), None) => Ordering::Less,  // DAGs with next run come first
                    (None, Some(_)) => Ordering::Greater,  // DAGs without next run come last
                    (None, None) => Ordering::Equal,
                }
            }),
            _ => None,
        }
    }
}

impl CustomSort for Variable {
    fn column_value(&self, column_index: usize) -> String {
        match column_index {
            0 => self.key.clone(), // Key
            1 => self.value.clone().unwrap_or_default(), // Value
            _ => String::new(),
        }
    }
}

impl CustomSort for Connection {
    fn column_value(&self, column_index: usize) -> String {
        match column_index {
            0 => self.connection_id.clone(), // ID
            1 => self.conn_type.clone(), // Type
            2 => self.host.clone().unwrap_or_default(), // Host
            3 => self.login.clone().unwrap_or_default(), // Login
            4 => self.schema.clone().unwrap_or_default(), // Schema
            5 => self.port.map(|p| p.to_string()).unwrap_or_default(), // Port
            _ => String::new(),
        }
    }
    
    fn comparator(column_index: usize) -> Option<fn(&Self, &Self) -> Ordering> {
        match column_index {
            5 => Some(|a: &Connection, b: &Connection| {
                // Sort port numerically
                a.port.cmp(&b.port)
            }),
            _ => None,
        }
    }
}

impl CustomSort for ImportError {
    fn column_value(&self, column_index: usize) -> String {
        match column_index {
            0 => {
                // DAG Name from filename
                self.filename.as_ref().and_then(|f| {
                    std::path::Path::new(f)
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .map(|s| s.to_string())
                }).unwrap_or_default()
            }
            1 => self.stack_trace.clone().unwrap_or_default(), // Error
            _ => String::new(),
        }
    }
}

pub struct DagModel {
    // Tab state
    pub active_tab: DagPanelTab,
    
    // DAG tab data
    pub all: Vec<Dag>,
    pub recent_runs: HashMap<String, Vec<DagRun>>,  // Store recent runs for each DAG
    pub filtered: SortableTable<Dag>,
    pub filter: Filter,
    pub show_paused: bool,
    pub import_error_list: Vec<ImportError>,
    
    // Variables tab data
    pub all_variables: Vec<Variable>,
    pub filtered_variables: SortableTable<Variable>,
    pub selected_variable: Option<Variable>,
    
    // Connections tab data
    pub all_connections: Vec<Connection>,
    pub filtered_connections: SortableTable<Connection>,
    pub selected_connection: Option<Connection>,
    
    // Import errors tab data
    pub filtered_import_errors: SortableTable<ImportError>,
    
    // Display settings
    pub timezone_offset: String,  // Format: "+09:00" or "-05:00"
    
    // State preservation for detail views
    pub saved_tab: Option<DagPanelTab>,
    pub saved_variable_selection: Option<usize>,
    pub saved_connection_selection: Option<usize>,
    pub saved_import_error_selection: Option<usize>,
    
    // Shared UI state
    commands: Option<CommandPopUp<'static>>,
    pub error_popup: Option<ErrorPopup>,
    pub loading_status: LoadingStatus,
    ticks: u32,
    event_buffer: Vec<FlowrsEvent>,
}

impl DagModel {
    pub fn new() -> Self {
        // Reserved keys across all DAG panel tabs: j/k (scroll), g/G (jump), h/l (tab nav), 
        // p (pause toggle), o (open), r (refresh), ? (help), / (filter)
        let reserved = &['j', 'k', 'g', 'G', 'h', 'l', 'p', 'o', 'r', '?', '/'];
        
        let dag_headers = ["State", "Name", "Schedule", "Next Run", "Tags"];
        let var_headers = ["Key", "Value"];
        let conn_headers = ["ID", "Type", "Host", "Login", "Schema", "Port"];
        let import_error_headers = ["DAG Name", "Error"];
        
        DagModel {
            active_tab: DagPanelTab::Dags,
            all: vec![],
            recent_runs: HashMap::new(),
            filtered: SortableTable::new(&dag_headers, vec![], reserved),
            filter: Filter::new(),
            show_paused: true,
            import_error_list: vec![],
            all_variables: vec![],
            filtered_variables: SortableTable::new(&var_headers, vec![], reserved),
            selected_variable: None,
            all_connections: vec![],
            filtered_connections: SortableTable::new(&conn_headers, vec![], reserved),
            selected_connection: None,
            filtered_import_errors: SortableTable::new(&import_error_headers, vec![], reserved),
            timezone_offset: "+00:00".to_string(),
            saved_tab: None,
            saved_variable_selection: None,
            saved_connection_selection: None,
            saved_import_error_selection: None,
            loading_status: LoadingStatus::NotStarted,
            commands: None,
            error_popup: None,
            ticks: 0,
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
        
        // Step 3: Compute state priority and schedule frequency for each DAG (for sorting)
        for dag in &mut filtered_dags {
            dag.computed_state_priority = Some(self.compute_state_priority(dag));
            dag.computed_schedule_frequency = Some(Self::compute_schedule_frequency(dag));
        }
        
        // Step 4: Sort - Alphabetically by DAG name (paused and unpaused interleaved)
        // This is the default sort when no column sort is active
        filtered_dags.sort_by(|a, b| a.dag_id.cmp(&b.dag_id));
        
        self.filtered.items = filtered_dags;
        // Reapply current sort if any
        self.filtered.reapply_sort();
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
        
        // Sort alphabetically by key (default sort)
        filtered_variables.sort_by(|a, b| a.key.cmp(&b.key));
        
        self.filtered_variables.items = filtered_variables;
        // Reapply current sort if any
        self.filtered_variables.reapply_sort();
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
        
        // Sort alphabetically by connection_id (default sort)
        filtered_connections.sort_by(|a, b| a.connection_id.cmp(&b.connection_id));
        
        self.filtered_connections.items = filtered_connections;
        // Reapply current sort if any
        self.filtered_connections.reapply_sort();
    }

    pub fn filter_import_errors(&mut self) {
        let prefix = &self.filter.prefix;
        
        let mut filtered_import_errors: Vec<ImportError> = match prefix {
            Some(prefix) => {
                let lower_prefix = prefix.to_lowercase();
                self.import_error_list
                    .iter()
                    .filter(|err| {
                        // Extract filename stem for searching
                        let filename_stem = err.filename.as_ref().and_then(|f| {
                            std::path::Path::new(f)
                                .file_stem()
                                .and_then(|s| s.to_str())
                        });
                        
                        let matches_filename = filename_stem
                            .map(|stem| stem.to_lowercase().contains(&lower_prefix))
                            .unwrap_or(false);
                        
                        let matches_stacktrace = err.stack_trace
                            .as_ref()
                            .map(|st| st.to_lowercase().contains(&lower_prefix))
                            .unwrap_or(false);
                        
                        matches_filename || matches_stacktrace
                    })
                    .cloned()
                    .collect()
            }
            None => self.import_error_list.clone(),
        };
        
        // Sort by timestamp (newest first) - default sort
        filtered_import_errors.sort_by(|a, b| {
            b.timestamp.cmp(&a.timestamp)
        });
        
        self.filtered_import_errors.items = filtered_import_errors;
        // Reapply current sort if any
        self.filtered_import_errors.reapply_sort();
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
            DagPanelTab::ImportErrors => {
                self.saved_import_error_selection = self.filtered_import_errors.state.selected();
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
                DagPanelTab::ImportErrors => {
                    if let Some(selection) = self.saved_import_error_selection.take() {
                        if selection < self.filtered_import_errors.items.len() {
                            self.filtered_import_errors.state.select(Some(selection));
                        }
                    }
                }
                DagPanelTab::Dags => {}
            }
        }
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

    /// Get DAG state icon based on latest run state
    /// Uses shared utility to determine icon (▶ for running, ■ for others)
    fn get_dag_icon(&self, dag: &Dag) -> &'static str {
        if dag.is_paused {
            return get_state_icon(None);  // Paused DAGs get default icon
        }

        if let Some(runs) = self.recent_runs.get(&dag.dag_id) {
            if !runs.is_empty() {
                let latest_run = &runs[0];
                return get_state_icon(Some(&latest_run.state));
            }
        }
        
        get_state_icon(None)  // No run data
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

    /// Compute state priority for sorting
    /// Lower values = higher priority (more urgent to address)
    fn compute_state_priority(&self, dag: &Dag) -> u8 {
        if dag.is_paused {
            return PRIORITY_PAUSED;
        }

        if let Some(runs) = self.recent_runs.get(&dag.dag_id) {
            if runs.is_empty() {
                return PRIORITY_UNKNOWN;
            }

            let latest_run = &runs[0];
            
            if Self::is_failed_state(&latest_run.state) {
                return PRIORITY_FAILED;
            }

            if latest_run.state == "running" {
                return PRIORITY_RUNNING;
            }

            if matches!(latest_run.state.as_str(), "success" | "queued" | "scheduled") {
                if Self::has_recent_failures(&runs[1..]) {
                    return PRIORITY_RECOVERED;
                } else {
                    return PRIORITY_SUCCESS;
                }
            }

            return PRIORITY_UNKNOWN;
        }
        
        PRIORITY_UNKNOWN
    }
    
    /// Compute schedule frequency for sorting
    /// Returns frequency in seconds (lower = more frequent)
    fn compute_schedule_frequency(dag: &Dag) -> u64 {
        // Try structured schedule_interval first (V1 API)
        if let Some(interval) = &dag.schedule_interval {
            if let Some(obj) = interval.as_object() {
                if let Some(type_str) = obj.get("__type").and_then(|v| v.as_str()) {
                    match type_str {
                        "TimeDelta" => return calculate_timedelta_frequency(obj),
                        "RelativeDelta" => return calculate_relativedelta_frequency(obj),
                        "CronExpression" => {
                            // For cron expressions, fall through to text parsing
                            // We could extract the value field and parse it, but for now
                            // let's rely on timetable_description which should have a human-readable version
                        }
                        _ => {}
                    }
                }
            }
        }
        
        // Fall back to parsing timetable_description
        parse_timetable_description(dag.timetable_description.as_deref())
    }
}

impl Default for DagModel {
    fn default() -> Self {
        Self::new()
    }
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
                            vec![WorkerMessage::UpdateDags],
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
                            DagPanelTab::ImportErrors => self.filter_import_errors(),
                        }
                        return (None, vec![]);
                    } else if self.filter.prefix.is_some() {
                        // Filter dialogue closed but filter is applied: clear the filter
                        self.filter.prefix = None;
                        match self.active_tab {
                            DagPanelTab::Dags => self.filter_dags(),
                            DagPanelTab::Variables => self.filter_variables(),
                            DagPanelTab::Connections => self.filter_connections(),
                            DagPanelTab::ImportErrors => self.filter_import_errors(),
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
                        DagPanelTab::ImportErrors => self.filter_import_errors(),
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
                } else if self.commands.is_some() {
                    return handle_command_popup_events(&mut self.commands, key_event);
                } else {
                    // Handle Ctrl+D and Ctrl+U for half-page scrolling
                    if key_event.modifiers == KeyModifiers::CONTROL {
                        match key_event.code {
                            KeyCode::Char('d') => {
                                match self.active_tab {
                                    DagPanelTab::Dags => self.filtered.scroll_by(HALF_PAGE_SIZE as isize),
                                    DagPanelTab::Variables => self.filtered_variables.scroll_by(HALF_PAGE_SIZE as isize),
                                    DagPanelTab::Connections => self.filtered_connections.scroll_by(HALF_PAGE_SIZE as isize),
                                    DagPanelTab::ImportErrors => self.filtered_import_errors.scroll_by(HALF_PAGE_SIZE as isize),
                                }
                                return (None, vec![]);
                            }
                            KeyCode::Char('u') => {
                                match self.active_tab {
                                    DagPanelTab::Dags => self.filtered.scroll_by(-(HALF_PAGE_SIZE as isize)),
                                    DagPanelTab::Variables => self.filtered_variables.scroll_by(-(HALF_PAGE_SIZE as isize)),
                                    DagPanelTab::Connections => self.filtered_connections.scroll_by(-(HALF_PAGE_SIZE as isize)),
                                    DagPanelTab::ImportErrors => self.filtered_import_errors.scroll_by(-(HALF_PAGE_SIZE as isize)),
                                }
                                return (None, vec![]);
                            }
                            _ => {}
                        }
                    }
                    
                    // Handle scrolling based on active tab
                    let handled = match (key_event.code, &mut self.active_tab) {
                        (KeyCode::Down | KeyCode::Char('j'), DagPanelTab::Dags) => {
                            self.filtered.scroll_by(1);
                            true
                        }
                        (KeyCode::Up | KeyCode::Char('k'), DagPanelTab::Dags) => {
                            self.filtered.scroll_by(-1);
                            true
                        }
                        (KeyCode::Down | KeyCode::Char('j'), DagPanelTab::Variables) => {
                            self.filtered_variables.scroll_by(1);
                            true
                        }
                        (KeyCode::Up | KeyCode::Char('k'), DagPanelTab::Variables) => {
                            self.filtered_variables.scroll_by(-1);
                            true
                        }
                        (KeyCode::Down | KeyCode::Char('j'), DagPanelTab::Connections) => {
                            self.filtered_connections.scroll_by(1);
                            true
                        }
                        (KeyCode::Up | KeyCode::Char('k'), DagPanelTab::Connections) => {
                            self.filtered_connections.scroll_by(-1);
                            true
                        }
                        (KeyCode::Down | KeyCode::Char('j'), DagPanelTab::ImportErrors) => {
                            self.filtered_import_errors.scroll_by(1);
                            true
                        }
                        (KeyCode::Up | KeyCode::Char('k'), DagPanelTab::ImportErrors) => {
                            self.filtered_import_errors.scroll_by(-1);
                            true
                        }
                        _ => false,
                    };
                    
                    if handled {
                        return (None, vec![]);
                    }
                    
                    // Handle sort keys based on active tab (only if no modifiers pressed)
                    if key_event.modifiers == KeyModifiers::NONE {
                        if let KeyCode::Char(c) = key_event.code {
                            let sort_handled = match self.active_tab {
                                DagPanelTab::Dags => self.filtered.handle_key(c),
                                DagPanelTab::Variables => self.filtered_variables.handle_key(c),
                                DagPanelTab::Connections => self.filtered_connections.handle_key(c),
                                DagPanelTab::ImportErrors => self.filtered_import_errors.handle_key(c),
                            };
                            
                            if sort_handled {
                                // Re-filter to apply default sort if sort was cleared
                                match self.active_tab {
                                    DagPanelTab::Dags => self.filter_dags(),
                                    DagPanelTab::Variables => self.filter_variables(),
                                    DagPanelTab::Connections => self.filter_connections(),
                                    DagPanelTab::ImportErrors => self.filter_import_errors(),
                                }
                                return (None, vec![]);
                            }
                        }
                    }
                    
                    match key_event.code {
                        KeyCode::Char('h') => {
                            // h - Previous tab
                            self.active_tab = match self.active_tab {
                                DagPanelTab::Dags => DagPanelTab::Dags, // Stay on first tab
                                DagPanelTab::Variables => DagPanelTab::Dags,
                                DagPanelTab::Connections => DagPanelTab::Variables,
                                DagPanelTab::ImportErrors => DagPanelTab::Connections,
                            };
                            // Lazy load: trigger data load if tab hasn't been loaded yet
                            // Note: Import errors are always loaded with DAGs, no lazy loading needed
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
                        KeyCode::Char('l') => {
                            // l - Next tab
                            self.active_tab = match self.active_tab {
                                DagPanelTab::Dags => DagPanelTab::Variables,
                                DagPanelTab::Variables => DagPanelTab::Connections,
                                DagPanelTab::Connections => DagPanelTab::ImportErrors,
                                DagPanelTab::ImportErrors => DagPanelTab::ImportErrors, // Stay on last tab
                            };
                            // Lazy load: trigger data load if tab hasn't been loaded yet
                            // Note: Import errors are always loaded with DAGs, no lazy loading needed
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
                        KeyCode::Char('G') => {
                            // Jump to bottom of active tab
                            match self.active_tab {
                                DagPanelTab::Dags => {
                                    self.filtered.state.select_last();
                                }
                                DagPanelTab::Variables => {
                                    self.filtered_variables.state.select_last();
                                }
                                DagPanelTab::Connections => {
                                    self.filtered_connections.state.select_last();
                                }
                                DagPanelTab::ImportErrors => {
                                    self.filtered_import_errors.state.select_last();
                                }
                            }
                        }
                        KeyCode::Char('p') => {
                            // Toggle showing paused DAGs (frontend filter only)
                            self.show_paused = !self.show_paused;
                            self.filter_dags();
                            // No WorkerMessage - purely frontend filtering!
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
                                DagPanelTab::ImportErrors => self.filter_import_errors(),
                            }
                        }
                        KeyCode::Char('?') => {
                            self.commands = Some(create_dag_command_popup());
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
                                                None,
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
                                                None,
                                                vec![WorkerMessage::GetConnectionDetail { connection_id }],
                                            );
                                        }
                                    }
                                    self.error_popup = Some(ErrorPopup::from_strings(vec![
                                        "No connection selected to view details".to_string(),
                                    ]));
                                }
                                DagPanelTab::ImportErrors => {
                                    if let Some(selected_idx) = self.filtered_import_errors.state.selected() {
                                        if let Some(import_error) = self.filtered_import_errors.items.get(selected_idx) {
                                            if let Some(import_error_id) = import_error.import_error_id {
                                                debug!("Selected import error: {}", import_error_id);
                                                // Save state before navigating to detail view
                                                self.save_state_before_detail_view();
                                                return (
                                                    None,
                                                    vec![WorkerMessage::GetImportErrorDetail { import_error_id }],
                                                );
                                            }
                                        }
                                    }
                                    self.error_popup = Some(ErrorPopup::from_strings(vec![
                                        "No import error selected to view details".to_string(),
                                    ]));
                                }
                            }
                        }
                        KeyCode::Char('g') => {
                            if let Some(FlowrsEvent::Key(key_event)) = self.event_buffer.pop() {
                                if key_event.code == KeyCode::Char('g') {
                                    // Jump to top of active tab
                                    match self.active_tab {
                                        DagPanelTab::Dags => {
                                            self.filtered.state.select_first();
                                        }
                                        DagPanelTab::Variables => {
                                            self.filtered_variables.state.select_first();
                                        }
                                        DagPanelTab::Connections => {
                                            self.filtered_connections.state.select_first();
                                        }
                                        DagPanelTab::ImportErrors => {
                                            self.filtered_import_errors.state.select_first();
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
                                        vec![WorkerMessage::UpdateDags],
                                    );
                                }
                                DagPanelTab::Variables => {
                                    return (None, vec![WorkerMessage::UpdateVariables]);
                                }
                                DagPanelTab::Connections => {
                                    return (None, vec![WorkerMessage::UpdateConnections]);
                                }
                                DagPanelTab::ImportErrors => {
                                    return (None, vec![WorkerMessage::UpdateImportErrors]);
                                }
                            }
                        }
                        _ => return (Some(FlowrsEvent::Key(*key_event)), vec![]), // if no match, return the event
                    }
                    return (None, vec![]);
                }
            }
            FlowrsEvent::Mouse => (Some(event.clone()), vec![]),
        }
    }
}

impl DagModel {
    fn create_tab_title(&self) -> Line<'static> {
        // Create tab labels with highlighting for active tab
        let mut tabs = vec![
            (DagPanelTab::Dags, "DAGs"),
            (DagPanelTab::Variables, "Variables"),
            (DagPanelTab::Connections, "Connections"),
        ];
        
        // Only show ImportErrors tab if there are errors
        if !self.import_error_list.is_empty() {
            tabs.push((DagPanelTab::ImportErrors, "Import Errors"));
        }
        
        let mut spans = Vec::new();
        for (i, (tab, label)) in tabs.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw(" "));
            }
            
            if *tab == self.active_tab {
                // Active tab: highlighted with cyan and bold
                let style = if *tab == DagPanelTab::ImportErrors {
                    // Red for import errors tab
                    Style::default()
                        .fg(crate::ui::constants::RED)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                        .fg(crate::ui::constants::CYAN)
                        .add_modifier(Modifier::BOLD)
                };
                spans.push(Span::styled(format!("[{}]", label), style));
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
        let border_style = DEFAULT_STYLE.fg(Color::Cyan);

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
            DagPanelTab::ImportErrors => {
                (self.filtered_import_errors.items.len(), self.import_error_list.len())
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
                let header_row = self.filtered.render_headers(HEADER_STYLE, RED);
                let header = Row::new(header_row).style(HEADER_STYLE);
                let search_term = self.filter.prefix.as_deref();
                let rows =
                    self.filtered.items.iter().enumerate().map(|(idx, item)| {
                        let color = self.get_dag_color(item);
                        let icon = self.get_dag_icon(item);
                        let text_color = if item.is_paused {
                            Color::DarkGray
                        } else {
                            Color::Reset
                        };
                        
                        Row::new(vec![
                            Line::from(Span::styled(icon, DEFAULT_STYLE.fg(color))),
                            Line::from(highlight_search_text(&item.dag_id, search_term, text_color)),
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
                                        let highlighted_spans = highlight_search_text(&tag.name, search_term, tag_color);
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
                let header_row = self.filtered_variables.render_headers(HEADER_STYLE, RED);
                let header = Row::new(header_row).style(HEADER_STYLE);
                let search_term = self.filter.prefix.as_deref();
                
                let rows = self.filtered_variables.items.iter().enumerate().map(|(idx, item)| {
                    // Note: Airflow API doesn't return values in the list endpoint for security
                    // Users need to press Enter to view the full value
                    let value_line = if let Some(v) = &item.value {
                        let (mut lines, is_json) = format_and_highlight_json(v, true, Some(80));
                        
                        // Extract single line from result
                        let line = lines.pop().unwrap_or_else(|| Line::from(""));
                        
                        // If not JSON, use default foreground color
                        if !is_json {
                            Line::from(Span::styled(
                                line.spans.into_iter().map(|s| s.content.to_string()).collect::<String>(),
                                Style::default().fg(Color::Reset)
                            ))
                        } else {
                            line
                        }
                    } else {
                        // Value not loaded - show hint
                        Line::from(Span::styled("Press Enter to view", Style::default().fg(Color::DarkGray)))
                    };
                    
                    Row::new(vec![
                        Line::from(highlight_search_text(&item.key, search_term, Color::Reset)),
                        value_line,
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
                let header_row = self.filtered_connections.render_headers(HEADER_STYLE, RED);
                let header = Row::new(header_row).style(HEADER_STYLE);
                let search_term = self.filter.prefix.as_deref();
                
                let rows = self.filtered_connections.items.iter().enumerate().map(|(idx, item)| {
                    // Use the same color mapping as tags for connection types
                    let type_color = hash_to_color(&item.conn_type);
                    
                    Row::new(vec![
                        Line::from(highlight_search_text(&item.connection_id, search_term, Color::Reset)),
                        Line::from(highlight_search_text(&item.conn_type, search_term, type_color)),
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
            DagPanelTab::ImportErrors => {
                let header_row = self.filtered_import_errors.render_headers(HEADER_STYLE, RED);
                let header = Row::new(header_row).style(HEADER_STYLE);
                let search_term = self.filter.prefix.as_deref();
                
                let rows = self.filtered_import_errors.items.iter().enumerate().map(|(idx, item)| {
                    // Extract DAG name (filename stem without extension)
                    let dag_name = item.filename.as_ref().and_then(|f| {
                        std::path::Path::new(f)
                            .file_stem()
                            .and_then(|s| s.to_str())
                    }).unwrap_or("-");
                    
                    // Get last line of stack trace as error summary
                    let error_summary = item.stack_trace.as_ref()
                        .and_then(|st| st.lines().last())
                        .unwrap_or("-");
                    
                    Row::new(vec![
                        Line::from(highlight_search_text(dag_name, search_term, crate::ui::constants::RED)),
                        Line::from(error_summary.to_string()),
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
                        Constraint::Fill(3),
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
                        .border_style(DEFAULT_STYLE.fg(crate::ui::constants::RED))
                        .style(DEFAULT_STYLE),
                )
                .row_highlight_style(selected_style);
                
                StatefulWidget::render(t, area, buf, &mut self.filtered_import_errors.state);
            }
        }
    }
}

impl Widget for &mut DagModel {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Handle filter at bottom if enabled
        let main_area = if self.filter.is_enabled() {
            let rects = Layout::default()
                .constraints([Constraint::Fill(90), Constraint::Max(3)].as_ref())
                .margin(0)
                .split(area);

            self.filter.render(rects[1], buf);
            rects[0]
        } else {
            area
        };

        // Render the main tabbed container (single container with tabs in title)
        self.render_tabbed_container(main_area, buf);

        // Render popups (they float over everything)
        if let Some(commands) = &mut self.commands {
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

/// Calculate frequency in seconds from TimeDelta schedule_interval
///
/// # Arguments
/// * `obj` - JSON object containing "days" and "seconds" fields
///
/// # Returns
/// * Frequency in seconds between runs
/// * `u64::MAX` if the interval is invalid, negative, or represents "never"
///
/// # Examples
/// * `{"days": 1, "seconds": 0}` → 86400 (daily)
/// * `{"days": 0, "seconds": 3600}` → 3600 (hourly)
fn calculate_timedelta_frequency(obj: &serde_json::Map<String, serde_json::Value>) -> u64 {
    let days = obj.get("days").and_then(|v| v.as_i64()).unwrap_or(0);
    let seconds = obj.get("seconds").and_then(|v| v.as_i64()).unwrap_or(0);
    // microseconds are too small to matter for scheduling frequency sorting
    
    // Convert to total seconds (ensure non-negative)
    let total_seconds = (days * SECONDS_PER_DAY as i64) + seconds;
    if total_seconds <= 0 {
        debug!("Invalid TimeDelta interval: days={}, seconds={}, total={}", days, seconds, total_seconds);
        u64::MAX  // Invalid or negative interval, treat as "never"
    } else {
        total_seconds as u64
    }
}

/// Calculate frequency in seconds from RelativeDelta schedule_interval
///
/// # Arguments
/// * `obj` - JSON object containing "years", "months", "days", "hours", "minutes", "seconds" fields
///
/// # Returns
/// * Frequency in seconds between runs (approximate for months/years)
/// * `u64::MAX` if the interval is invalid, negative, or represents "never"
///
/// # Note
/// Months are approximated as 30 days, years as 365 days
fn calculate_relativedelta_frequency(obj: &serde_json::Map<String, serde_json::Value>) -> u64 {
    let years = obj.get("years").and_then(|v| v.as_i64()).unwrap_or(0);
    let months = obj.get("months").and_then(|v| v.as_i64()).unwrap_or(0);
    let days = obj.get("days").and_then(|v| v.as_i64()).unwrap_or(0);
    let hours = obj.get("hours").and_then(|v| v.as_i64()).unwrap_or(0);
    let minutes = obj.get("minutes").and_then(|v| v.as_i64()).unwrap_or(0);
    let seconds = obj.get("seconds").and_then(|v| v.as_i64()).unwrap_or(0);
    
    // Approximate seconds (using average month/year lengths)
    let total_seconds = (years * SECONDS_PER_YEAR as i64)
        + (months * SECONDS_PER_MONTH as i64)
        + (days * SECONDS_PER_DAY as i64)
        + (hours * SECONDS_PER_HOUR as i64)
        + (minutes * SECONDS_PER_MINUTE as i64)
        + seconds;
    
    if total_seconds <= 0 {
        debug!("Invalid RelativeDelta interval: years={}, months={}, days={}, hours={}, minutes={}, seconds={}, total={}", 
               years, months, days, hours, minutes, seconds, total_seconds);
        u64::MAX  // Invalid or negative interval, treat as "never"
    } else {
        total_seconds as u64
    }
}

/// Parse timetable_description text to estimate frequency in seconds
///
/// This is a fallback when structured schedule_interval is not available (e.g., V2 API).
/// Uses pattern matching and keyword detection to estimate schedule frequency.
///
/// # Arguments
/// * `description` - Optional timetable description string
///
/// # Returns
/// * Estimated frequency in seconds between runs
/// * `u64::MAX` for "never" or missing descriptions
/// * `UNKNOWN_SCHEDULE_FREQUENCY` for unparseable schedules
fn parse_timetable_description(description: Option<&str>) -> u64 {
    let desc = match description {
        Some(d) if !d.is_empty() => d.to_lowercase(),
        _ => {
            debug!("No timetable description provided, treating as 'never'");
            return u64::MAX;
        }
    };
    
    // Special cases - never scheduled or manual-only
    if desc.contains("never") || desc == "none" {
        return u64::MAX;
    }
    
    // Try to extract numeric patterns like "Every X minutes/hours/days"
    // Pattern: "every X minute(s)" or "every X hour(s)" etc.
    if let Some(captures) = SCHEDULE_PATTERN.captures(&desc) {
        if let Some(num_str) = captures.get(1) {
            if let Ok(num) = num_str.as_str().parse::<u64>() {
                let unit = captures.get(2).map(|m| m.as_str()).unwrap_or("");
                return match unit {
                    "minute" => num * SECONDS_PER_MINUTE,
                    "hour" => num * SECONDS_PER_HOUR,
                    "day" => num * SECONDS_PER_DAY,
                    "week" => num * SECONDS_PER_WEEK,
                    "month" => num * SECONDS_PER_MONTH,
                    "year" => num * SECONDS_PER_YEAR,
                    _ => {
                        debug!("Unrecognized time unit '{}' in description: {}", unit, desc);
                        u64::MAX
                    }
                };
            }
        }
    }
    
    // Common frequency keywords - use exact matching or specific patterns to avoid false positives
    // Check for "hourly" or "every hour" patterns
    if desc == "hourly" || desc == "every hour" || (desc.contains("every") && desc.contains("hour") && !desc.contains("day")) {
        return SECONDS_PER_HOUR;
    }
    
    // Check for "daily" or "at HH:MM" patterns (without day-of-week indicators)
    if desc == "daily" {
        return SECONDS_PER_DAY;
    }
    
    // "At HH:MM" typically means daily, but make sure it's not weekly (contains day names)
    if desc.starts_with("at ") && 
       !desc.contains("monday") && !desc.contains("tuesday") && !desc.contains("wednesday") &&
       !desc.contains("thursday") && !desc.contains("friday") && !desc.contains("saturday") && !desc.contains("sunday") {
        return SECONDS_PER_DAY;
    }
    
    // Check for weekly patterns
    if desc == "weekly" || 
       desc.contains("monday") || desc.contains("tuesday") || desc.contains("wednesday") ||
       desc.contains("thursday") || desc.contains("friday") || desc.contains("saturday") || desc.contains("sunday") {
        return SECONDS_PER_WEEK;
    }
    
    // Check for monthly patterns
    if desc == "monthly" || desc.contains("day 1") || desc.contains("first day") {
        return SECONDS_PER_MONTH;
    }
    
    // Check for yearly patterns
    if desc == "yearly" || desc == "annually" || desc.contains("annual") {
        return SECONDS_PER_YEAR;
    }
    
    // Cron shorthand patterns (e.g., @hourly, @daily, etc.)
    if desc == "@hourly" || desc.starts_with("0 * * * *") {
        return SECONDS_PER_HOUR;
    }
    if desc == "@daily" || desc == "@midnight" || desc.starts_with("0 0 * * *") {
        return SECONDS_PER_DAY;
    }
    if desc == "@weekly" || desc.starts_with("0 0 * * 0") {
        return SECONDS_PER_WEEK;
    }
    if desc == "@monthly" || desc.starts_with("0 0 1 * *") {
        return SECONDS_PER_MONTH;
    }
    if desc == "@yearly" || desc == "@annually" || desc.starts_with("0 0 1 1 *") {
        return SECONDS_PER_YEAR;
    }
    
    // Unknown/custom schedule - log for debugging
    debug!("Unable to parse schedule frequency from description: '{}'", desc);
    UNKNOWN_SCHEDULE_FREQUENCY
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

    // Tests for schedule frequency calculation

    #[test]
    fn test_calculate_timedelta_frequency_daily() {
        let mut obj = serde_json::Map::new();
        obj.insert("days".to_string(), serde_json::json!(1));
        obj.insert("seconds".to_string(), serde_json::json!(0));
        assert_eq!(calculate_timedelta_frequency(&obj), SECONDS_PER_DAY);
    }

    #[test]
    fn test_calculate_timedelta_frequency_hourly() {
        let mut obj = serde_json::Map::new();
        obj.insert("days".to_string(), serde_json::json!(0));
        obj.insert("seconds".to_string(), serde_json::json!(3600));
        assert_eq!(calculate_timedelta_frequency(&obj), SECONDS_PER_HOUR);
    }

    #[test]
    fn test_calculate_timedelta_frequency_invalid() {
        let mut obj = serde_json::Map::new();
        obj.insert("days".to_string(), serde_json::json!(-1));
        obj.insert("seconds".to_string(), serde_json::json!(0));
        assert_eq!(calculate_timedelta_frequency(&obj), u64::MAX);
    }

    #[test]
    fn test_calculate_relativedelta_frequency_monthly() {
        let mut obj = serde_json::Map::new();
        obj.insert("years".to_string(), serde_json::json!(0));
        obj.insert("months".to_string(), serde_json::json!(1));
        obj.insert("days".to_string(), serde_json::json!(0));
        obj.insert("hours".to_string(), serde_json::json!(0));
        obj.insert("minutes".to_string(), serde_json::json!(0));
        obj.insert("seconds".to_string(), serde_json::json!(0));
        assert_eq!(calculate_relativedelta_frequency(&obj), SECONDS_PER_MONTH);
    }

    #[test]
    fn test_calculate_relativedelta_frequency_yearly() {
        let mut obj = serde_json::Map::new();
        obj.insert("years".to_string(), serde_json::json!(1));
        obj.insert("months".to_string(), serde_json::json!(0));
        obj.insert("days".to_string(), serde_json::json!(0));
        obj.insert("hours".to_string(), serde_json::json!(0));
        obj.insert("minutes".to_string(), serde_json::json!(0));
        obj.insert("seconds".to_string(), serde_json::json!(0));
        assert_eq!(calculate_relativedelta_frequency(&obj), SECONDS_PER_YEAR);
    }

    #[test]
    fn test_parse_timetable_description_numeric_patterns() {
        assert_eq!(parse_timetable_description(Some("every 5 minutes")), 5 * SECONDS_PER_MINUTE);
        assert_eq!(parse_timetable_description(Some("every 2 hours")), 2 * SECONDS_PER_HOUR);
        assert_eq!(parse_timetable_description(Some("every 3 days")), 3 * SECONDS_PER_DAY);
        assert_eq!(parse_timetable_description(Some("every 1 week")), SECONDS_PER_WEEK);
        assert_eq!(parse_timetable_description(Some("every 2 months")), 2 * SECONDS_PER_MONTH);
        assert_eq!(parse_timetable_description(Some("every 1 year")), SECONDS_PER_YEAR);
    }

    #[test]
    fn test_parse_timetable_description_keywords() {
        assert_eq!(parse_timetable_description(Some("hourly")), SECONDS_PER_HOUR);
        assert_eq!(parse_timetable_description(Some("every hour")), SECONDS_PER_HOUR);
        assert_eq!(parse_timetable_description(Some("daily")), SECONDS_PER_DAY);
        assert_eq!(parse_timetable_description(Some("weekly")), SECONDS_PER_WEEK);
        assert_eq!(parse_timetable_description(Some("monthly")), SECONDS_PER_MONTH);
        assert_eq!(parse_timetable_description(Some("yearly")), SECONDS_PER_YEAR);
        assert_eq!(parse_timetable_description(Some("annually")), SECONDS_PER_YEAR);
    }

    #[test]
    fn test_parse_timetable_description_at_time() {
        // "At HH:MM" should be daily
        assert_eq!(parse_timetable_description(Some("at 09:00")), SECONDS_PER_DAY);
        assert_eq!(parse_timetable_description(Some("at 14:30")), SECONDS_PER_DAY);
        
        // But "At HH:MM on Monday" should be weekly
        assert_eq!(parse_timetable_description(Some("at 09:00 on monday")), SECONDS_PER_WEEK);
    }

    #[test]
    fn test_parse_timetable_description_day_names() {
        assert_eq!(parse_timetable_description(Some("monday")), SECONDS_PER_WEEK);
        assert_eq!(parse_timetable_description(Some("tuesday")), SECONDS_PER_WEEK);
        assert_eq!(parse_timetable_description(Some("on friday")), SECONDS_PER_WEEK);
    }

    #[test]
    fn test_parse_timetable_description_cron_shortcuts() {
        assert_eq!(parse_timetable_description(Some("@hourly")), SECONDS_PER_HOUR);
        assert_eq!(parse_timetable_description(Some("@daily")), SECONDS_PER_DAY);
        assert_eq!(parse_timetable_description(Some("@midnight")), SECONDS_PER_DAY);
        assert_eq!(parse_timetable_description(Some("@weekly")), SECONDS_PER_WEEK);
        assert_eq!(parse_timetable_description(Some("@monthly")), SECONDS_PER_MONTH);
        assert_eq!(parse_timetable_description(Some("@yearly")), SECONDS_PER_YEAR);
        assert_eq!(parse_timetable_description(Some("@annually")), SECONDS_PER_YEAR);
    }

    #[test]
    fn test_parse_timetable_description_cron_expressions() {
        assert_eq!(parse_timetable_description(Some("0 * * * *")), SECONDS_PER_HOUR);
        assert_eq!(parse_timetable_description(Some("0 0 * * *")), SECONDS_PER_DAY);
        assert_eq!(parse_timetable_description(Some("0 0 * * 0")), SECONDS_PER_WEEK);
        assert_eq!(parse_timetable_description(Some("0 0 1 * *")), SECONDS_PER_MONTH);
        assert_eq!(parse_timetable_description(Some("0 0 1 1 *")), SECONDS_PER_YEAR);
    }

    #[test]
    fn test_parse_timetable_description_never() {
        assert_eq!(parse_timetable_description(Some("never")), u64::MAX);
        assert_eq!(parse_timetable_description(Some("Never")), u64::MAX);
        assert_eq!(parse_timetable_description(Some("none")), u64::MAX);
        assert_eq!(parse_timetable_description(None), u64::MAX);
        assert_eq!(parse_timetable_description(Some("")), u64::MAX);
    }

    #[test]
    fn test_parse_timetable_description_unknown() {
        // Unknown patterns should return UNKNOWN_SCHEDULE_FREQUENCY
        assert_eq!(parse_timetable_description(Some("custom schedule")), UNKNOWN_SCHEDULE_FREQUENCY);
        assert_eq!(parse_timetable_description(Some("irregular")), UNKNOWN_SCHEDULE_FREQUENCY);
    }

    #[test]
    fn test_parse_timetable_description_plurals() {
        // Test that plurals work
        assert_eq!(parse_timetable_description(Some("every 5 minutes")), 5 * SECONDS_PER_MINUTE);
        assert_eq!(parse_timetable_description(Some("every 2 hours")), 2 * SECONDS_PER_HOUR);
        assert_eq!(parse_timetable_description(Some("every 3 days")), 3 * SECONDS_PER_DAY);
    }
}
