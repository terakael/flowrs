use crossterm::event::KeyCode;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, BorderType, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
        StatefulWidget, Widget, Wrap,
    },
};
use regex::Regex;
use std::sync::OnceLock;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::{
    app::{
        environment_state::TaskLog,
        events::custom::FlowrsEvent,
        worker::{OpenItem, WorkerMessage},
    },
    ui::common::hash_to_color,
    ui::constants::{
        BRIGHT_BLACK, CYAN, BLUE, GREEN, YELLOW, RED, FOREGROUND, MAGENTA, DEFAULT_STYLE,
    },
};

use super::popup::error::ErrorPopup;
use super::popup::commands_help::CommandPopUp;
use super::popup::logs::commands::create_log_command_popup;
use super::{Model, handle_vertical_scroll_keys, handle_command_popup_events};

// Constants for log viewer configuration
const LRU_CACHE_SIZE: usize = 5;          // Number of recently viewed attempts to keep in cache
const VIRTUAL_SCROLL_BUFFER: usize = 100; // Lines to render beyond visible viewport
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Log level enum for filtering logs by minimum severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug = 1,
    Info = 2,
    Warning = 3,
    Error = 4,
    Critical = 5,
}

impl std::str::FromStr for LogLevel {
    type Err = ();
    
    /// Parse log level from string (case-insensitive)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "DEBUG" => Ok(LogLevel::Debug),
            "INFO" => Ok(LogLevel::Info),
            "WARNING" => Ok(LogLevel::Warning),
            "ERROR" => Ok(LogLevel::Error),
            "CRITICAL" => Ok(LogLevel::Critical),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warning => write!(f, "WARNING"),
            LogLevel::Error => write!(f, "ERROR"),
            LogLevel::Critical => write!(f, "CRITICAL"),
        }
    }
}

impl LogLevel {
    /// Get number key for this level (1-5)
    fn key_number(&self) -> u8 {
        *self as u8
    }
}

/// Helper struct to map logical lines to visual line ranges
/// Enables accurate scrolling when text wrapping is enabled
#[derive(Debug, Clone)]
struct VisualLineMapping {
    logical_index: usize,   // Index in cached_filtered_lines
    visual_start: usize,    // First visual line for this logical line
    visual_end: usize,      // Last visual line + 1 (exclusive range)
    line_count: usize,      // Number of visual lines (visual_end - visual_start)
}

pub struct LogModel {
    pub dag_id: Option<String>,
    pub dag_run_id: Option<String>,
    pub task_id: Option<String>,
    pub tries: Option<u16>,
    pub current_attempt: usize,           // 1-indexed attempt number
    pub current_log_data: Option<TaskLog>, // Current attempt's chunks
    pub is_loading_more: bool,            // Loading next chunk
    pub is_loading_initial: bool,         // Loading initial chunk (show spinner)
    pub lru_cache: VecDeque<u16>,         // Last 5 viewed attempts
    commands: Option<CommandPopUp<'static>>, // Help popup
    pub error_popup: Option<ErrorPopup>,
    pub min_log_level: LogLevel,          // Minimum log level to display
    ticks: u32,
    vertical_scroll: usize,               // VISUAL line offset (not logical line)
    vertical_scroll_state: ScrollbarState,
    last_viewport_height: usize,          // Lines visible (excluding borders)
    cached_lines: Vec<String>,            // CACHE: Parsed lines to avoid reparsing every frame
    cached_content_hash: u64,             // Hash to detect when content changes
    event_buffer: Vec<FlowrsEvent>,       // Buffer for 'gg' detection
    cached_log_date: Option<String>,      // CACHE: Extracted date from first log line
    cached_log_timezone: Option<String>,  // CACHE: Extracted timezone from first log line
    cached_filtered_lines: Vec<(usize, String)>, // CACHE: Filtered lines with original indices
    cached_filter_level: LogLevel,        // CACHE: Log level used for filtering
    cached_viewport_width: u16,           // CACHE: Viewport width (detect resize)
    cached_visual_line_map: Vec<VisualLineMapping>, // CACHE: Logical→visual mapping
    cached_total_visual_lines: usize,     // CACHE: Total visual lines with wrapping
}

impl LogModel {
    pub fn new() -> Self {
        LogModel {
            dag_id: None,
            dag_run_id: None,
            task_id: None,
            tries: None,
            current_attempt: 1,
            current_log_data: None,
            is_loading_more: false,
            is_loading_initial: false,
            lru_cache: VecDeque::new(),
            commands: None,
            error_popup: None,
            min_log_level: LogLevel::Info,  // Default to INFO
            ticks: 0,
            vertical_scroll: 0,              // Start at top (visual line 0)
            vertical_scroll_state: ScrollbarState::default(),
            last_viewport_height: 20,        // Default viewport size
            cached_lines: Vec::new(),
            cached_content_hash: 0,
            event_buffer: Vec::new(),
            cached_log_date: None,
            cached_log_timezone: None,
            cached_filtered_lines: Vec::new(),
            cached_filter_level: LogLevel::Info,
            cached_viewport_width: 0,        // Will recalculate on first render
            cached_visual_line_map: Vec::new(),
            cached_total_visual_lines: 0,
        }
    }
    
    /// Update LRU cache when viewing an attempt
    pub fn update_lru(&mut self, attempt: u16) {
        // Remove if already in cache
        self.lru_cache.retain(|&x| x != attempt);
        
        // Add to front
        self.lru_cache.push_front(attempt);
        
        // Keep only last N attempts
        if self.lru_cache.len() > LRU_CACHE_SIZE {
            self.lru_cache.pop_back();
        }
    }
    
    /// Check if we need to load more chunks based on scroll position
    /// DISABLED: Auto-loading causes performance issues, use manual 'm' key instead
    fn check_auto_load(&mut self) -> Option<WorkerMessage> {
        // Temporarily disabled - auto-loading entire chunks causes freezing
        // User can manually load more with 'm' key
        None
    }
    
    /// Clear all cached rendering data (call when switching attempts or tasks)
    fn clear_render_cache(&mut self) {
        self.cached_log_date = None;
        self.cached_log_timezone = None;
        self.cached_lines.clear();
        self.cached_content_hash = 0;
        self.cached_filtered_lines.clear();
        self.cached_visual_line_map.clear();
        self.cached_total_visual_lines = 0;
    }
    
    /// Reset state when switching to a new task
    pub fn reset_for_new_task(&mut self, dag_id: String, dag_run_id: String, task_id: String, task_try: u16) {
        self.dag_id = Some(dag_id);
        self.dag_run_id = Some(dag_run_id);
        self.task_id = Some(task_id);
        self.tries = Some(task_try);
        self.current_attempt = task_try as usize;
        self.vertical_scroll = 0;
        self.clear_render_cache();
        self.update_lru(task_try);
        self.is_loading_initial = true;
        self.min_log_level = LogLevel::Info;  // Reset to INFO when switching tasks
    }
    
    /// Build the top title line with semantic colors for each component:
    /// - YELLOW: Panel name (primary identifier)
    /// - GREEN: Try info (success/progress indicator)
    /// - CYAN: Date/timezone (temporal context) and separators (matching border)
    /// - BLUE: DAG ID (entity identifier)
    /// - MAGENTA: Task ID (sub-entity identifier)
    fn build_title_line(&self, total_tries: usize) -> Line<'static> {
        let mut title_spans = Vec::new();
        
        // "Logs" label in yellow
        title_spans.push(Span::styled("Logs".to_string(), Style::default().fg(YELLOW)));
        
        // Try info (always show)
        title_spans.push(Span::styled(" - ".to_string(), Style::default().fg(CYAN)));
        title_spans.push(Span::styled(
            format!("Try {}/{}", self.current_attempt, total_tries),
            Style::default().fg(GREEN)
        ));
        
        // Date and timezone
        if let (Some(date), Some(tz)) = (&self.cached_log_date, &self.cached_log_timezone) {
            title_spans.push(Span::styled(" - ".to_string(), Style::default().fg(CYAN)));
            title_spans.push(Span::styled(date.clone(), Style::default().fg(CYAN)));
            title_spans.push(Span::raw(" "));
            title_spans.push(Span::styled(tz.clone(), Style::default().fg(CYAN)));
        }
        
        // DAG ID
        if let Some(dag_id) = &self.dag_id {
            title_spans.push(Span::styled(" - ".to_string(), Style::default().fg(CYAN)));
            title_spans.push(Span::styled(dag_id.clone(), Style::default().fg(BLUE)));
        }
        
        // Task ID
        if let Some(task_id) = &self.task_id {
            title_spans.push(Span::styled(" - ".to_string(), Style::default().fg(CYAN)));
            title_spans.push(Span::styled(task_id.clone(), Style::default().fg(MAGENTA)));
        }
        
        Line::from(title_spans)
    }
    
    /// Build the bottom title with line count, loading status, and log level selector
    fn build_bottom_title(&self, total_lines: usize, _log_data: &TaskLog) -> Line<'static> {
        let frame = SPINNER_FRAMES[self.ticks as usize % SPINNER_FRAMES.len()];
        
        let mut spans = Vec::new();
        
        // Line count and loading status
        let status_text = if self.is_loading_more {
            format!("{} lines ({} loading more...)", total_lines, frame)
        } else {
            format!("{} lines", total_lines)
        };
        
        spans.push(Span::raw(status_text));
        spans.push(Span::raw(" - "));
        
        // Add log level selectors with colors
        // Gray out levels below threshold, show full color for threshold and above
        let levels = [
            (LogLevel::Debug, BLUE),
            (LogLevel::Info, GREEN),
            (LogLevel::Warning, YELLOW),
            (LogLevel::Error, RED),
            (LogLevel::Critical, RED),
        ];
        
        for (idx, (level, color)) in levels.iter().enumerate() {
            if idx > 0 {
                spans.push(Span::raw(" "));
            }
            
            // Gray out if below threshold, show full color if at or above threshold
            let is_visible = *level >= self.min_log_level;
            let level_color = if is_visible { *color } else { Color::DarkGray };
            let level_style = if *level == LogLevel::Critical && is_visible {
                Style::default().fg(level_color).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(level_color)
            };
            
            spans.push(Span::styled(
                format!("[{}]{}", level.key_number(), level),
                level_style,
            ));
        }
        
        Line::from(spans)
    }
    
    /// Check if we need to recalculate the visual line map
    /// Recalculates when: no cached map, width changed, or filter changed
    fn should_recalculate_visual_map(&self, viewport_width: u16) -> bool {
        // 1. No cached map yet but we have filtered lines
        if self.cached_visual_line_map.is_empty() && !self.cached_filtered_lines.is_empty() {
            return true;
        }
        
        // 2. Width changed (terminal resize or first render)
        if self.cached_viewport_width != viewport_width {
            return true;
        }
        
        // 3. Map length doesn't match filtered lines (filter changed)
        if self.cached_visual_line_map.len() != self.cached_filtered_lines.len() {
            return true;
        }
        
        false
    }
    
    /// Calculate the visual line map for the current filtered lines
    /// This determines how many visual lines each logical line occupies at the given width
    /// Returns (visual_line_mappings, total_visual_lines)
    fn calculate_visual_line_map(&self, viewport_width: u16) -> (Vec<VisualLineMapping>, usize) {
        let mut mappings = Vec::with_capacity(self.cached_filtered_lines.len());
        let mut current_visual_line = 0;
        
        // Account for borders: 2 chars for left/right borders
        let content_width = viewport_width.saturating_sub(2);
        
        if content_width == 0 {
            // Terminal too narrow, can't wrap anything
            return (mappings, 0);
        }
        
        for (logical_idx, (_original_idx, line_content)) in 
            self.cached_filtered_lines.iter().enumerate() 
        {
            // Build a temporary Line to calculate wrapping
            // Don't skip date/timezone for accurate width calculation
            let colored_line = colorize_log_line_with_options(
                line_content,
                None,  // Don't skip date for accurate width calculation
                None   // Don't skip timezone for accurate width calculation
            );
            
            // Create a temporary Paragraph with wrapping to calculate line count
            let temp_paragraph = Paragraph::new(colored_line)
                .wrap(Wrap { trim: false });
            
            // Use ratatui's built-in line_count() - accounts for unicode, styles, etc.
            let wrapped_line_count = temp_paragraph.line_count(content_width).max(1);
            
            mappings.push(VisualLineMapping {
                logical_index: logical_idx,
                visual_start: current_visual_line,
                visual_end: current_visual_line + wrapped_line_count,
                line_count: wrapped_line_count,
            });
            
            current_visual_line += wrapped_line_count;
        }
        
        let total_visual_lines = current_visual_line;
        (mappings, total_visual_lines)
    }
    
    /// Find which logical line(s) are visible at a given visual scroll position
    /// Returns (logical_start_idx, visual_offset_in_first_line, logical_end_idx)
    fn visual_to_logical_range(
        &self,
        visual_scroll: usize,
        viewport_height: usize,
    ) -> (usize, usize, usize) {
        if self.cached_visual_line_map.is_empty() {
            return (0, 0, 0);
        }
        
        let visual_end = visual_scroll + viewport_height;
        
        // Binary search for the logical line containing visual_scroll
        let start_idx = self.cached_visual_line_map
            .binary_search_by(|mapping| {
                if visual_scroll < mapping.visual_start {
                    std::cmp::Ordering::Greater
                } else if visual_scroll >= mapping.visual_end {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .unwrap_or_else(|insert_pos| insert_pos.min(self.cached_visual_line_map.len().saturating_sub(1)));
        
        // Calculate offset within the first logical line
        let start_mapping = &self.cached_visual_line_map[start_idx];
        let visual_offset = visual_scroll.saturating_sub(start_mapping.visual_start);
        
        // Find last logical line that's visible
        let end_idx = self.cached_visual_line_map
            .iter()
            .position(|mapping| mapping.visual_start >= visual_end)
            .unwrap_or(self.cached_visual_line_map.len());
        
        (start_idx, visual_offset, end_idx)
    }
    
    /// Get the visual line range for a logical line index
    fn logical_to_visual_range(&self, logical_idx: usize) -> Option<(usize, usize)> {
        self.cached_visual_line_map
            .get(logical_idx)
            .map(|mapping| (mapping.visual_start, mapping.visual_end))
    }
}

impl Default for LogModel {
    fn default() -> Self {
        Self::new()
    }
}

impl Model for LogModel {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>) {
        match event {
            FlowrsEvent::Tick => {
                self.ticks += 1;
                // Check if we should auto-load on each tick
                if let Some(msg) = self.check_auto_load() {
                    return (None, vec![msg]);
                }
                return (Some(FlowrsEvent::Tick), vec![]);
            }
            FlowrsEvent::Key(key) => {
                // Handle command popup first
                if self.commands.is_some() {
                    return handle_command_popup_events(&mut self.commands, key);
                }
                
                if let Some(_error_popup) = &mut self.error_popup {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            self.error_popup = None;
                        }
                        _ => (),
                    }
                    return (None, vec![]);
                }
                
                // Handle standard scrolling keybinds (now operates on visual lines)
                if handle_vertical_scroll_keys(
                    &mut self.vertical_scroll,
                    &mut self.vertical_scroll_state,
                    key,
                    None,
                ) {
                    // After scrolling, check if we need more
                    if let Some(msg) = self.check_auto_load() {
                        return (None, vec![msg]);
                    }
                    return (None, vec![]);
                }
                
                match key.code {
                    KeyCode::Char('l') | KeyCode::Right => {
                        // Next attempt
                        let total_tries = self.tries.unwrap_or(1) as usize;
                        let next_attempt = if self.current_attempt == total_tries {
                            1
                        } else {
                            self.current_attempt + 1
                        };
                        
                        self.current_attempt = next_attempt;
                        self.vertical_scroll = 0;
                        self.clear_render_cache();
                        self.update_lru(next_attempt as u16);
                        
                        return (None, vec![WorkerMessage::EnsureTaskLogLoaded {
                            dag_id: self.dag_id.clone().unwrap(),
                            dag_run_id: self.dag_run_id.clone().unwrap(),
                            task_id: self.task_id.clone().unwrap(),
                            task_try: next_attempt as u16,
                        }]);
                    }
                    KeyCode::Char('h') | KeyCode::Left => {
                        // Previous attempt
                        let total_tries = self.tries.unwrap_or(1) as usize;
                        let prev_attempt = if self.current_attempt == 1 {
                            total_tries
                        } else {
                            self.current_attempt - 1
                        };
                        
                        self.current_attempt = prev_attempt;
                        self.vertical_scroll = 0;
                        self.clear_render_cache();
                        self.update_lru(prev_attempt as u16);
                        
                        return (None, vec![WorkerMessage::EnsureTaskLogLoaded {
                            dag_id: self.dag_id.clone().unwrap(),
                            dag_run_id: self.dag_run_id.clone().unwrap(),
                            task_id: self.task_id.clone().unwrap(),
                            task_try: prev_attempt as u16,
                        }]);
                    }
                    KeyCode::Char('G') => {
                        // Jump to bottom (in visual lines)
                        if !self.cached_visual_line_map.is_empty() {
                            let max_scroll = self.cached_total_visual_lines
                                .saturating_sub(self.last_viewport_height);
                            self.vertical_scroll = max_scroll;
                            self.vertical_scroll_state = self.vertical_scroll_state.position(max_scroll);
                        }
                    }
                    KeyCode::Char('g') => {
                        // Check for double 'g' (gg = jump to top)
                        if let Some(FlowrsEvent::Key(prev_key)) = self.event_buffer.pop() {
                            if prev_key.code == KeyCode::Char('g') {
                                // Double 'g' detected - jump to top
                                self.vertical_scroll = 0;
                                self.vertical_scroll_state = self.vertical_scroll_state.position(0);
                            } else {
                                // Not a double 'g', put it back
                                self.event_buffer.push(FlowrsEvent::Key(prev_key));
                                self.event_buffer.push(FlowrsEvent::Key(*key));
                            }
                        } else {
                            // First 'g', buffer it
                            self.event_buffer.push(FlowrsEvent::Key(*key));
                        }
                    }
                    KeyCode::Char('o') => {
                        if self.current_log_data.is_some() {
                            return (
                                Some(FlowrsEvent::Key(*key)),
                                vec![WorkerMessage::OpenItem(OpenItem::Log {
                                    dag_id: self.dag_id.clone().expect("DAG ID not set"),
                                    dag_run_id: self
                                        .dag_run_id
                                        .clone()
                                        .expect("DAG Run ID not set"),
                                    task_id: self.task_id.clone().expect("Task ID not set"),
                                    task_try: self.current_attempt as u16,
                                })],
                            );
                        }
                    }
                    KeyCode::Char('e') => {
                        // Open logs in external editor
                        if let Some(log_data) = &self.current_log_data {
                            log::debug!("Attempting to open log in editor, file_path: {:?}", log_data.get_file_path());
                            if let Some(filepath) = log_data.get_file_path() {
                                log::info!("Opening log file in editor: {}", filepath.display());
                                return (
                                    None,
                                    vec![WorkerMessage::OpenInEditor {
                                        filepath: filepath.to_path_buf(),
                                    }],
                                );
                            } else {
                                // Fallback: show error that logs aren't persisted yet
                                log::warn!("Log file_path not set in current_log_data");
                                self.error_popup = Some(ErrorPopup::from_strings(vec![
                                    "Log file not found on disk".into(),
                                    "Try refreshing logs with 'r'".into(),
                                ]));
                            }
                        } else {
                            log::warn!("No current_log_data available");
                        }
                    }
                    KeyCode::Char('r') => {
                        // Manual refresh - reload current attempt's logs
                        if let (Some(dag_id), Some(dag_run_id), Some(task_id)) = 
                            (&self.dag_id, &self.dag_run_id, &self.task_id) 
                        {
                            return (
                                None,
                                vec![WorkerMessage::UpdateTaskLogs {
                                    dag_id: dag_id.clone(),
                                    dag_run_id: dag_run_id.clone(),
                                    task_id: task_id.clone(),
                                    task_try: self.current_attempt as u16,
                                    clear: true,
                                }],
                            );
                        }
                    }
                    KeyCode::Char('m') => {
                        // Manual "load more" - fetch next chunk
                        if let Some(log_data) = &self.current_log_data {
                            if log_data.has_more() && !self.is_loading_more {
                                if let Some(token) = &log_data.current_continuation_token {
                                    self.is_loading_more = true;
                                    return (
                                        None,
                                        vec![WorkerMessage::LoadMoreTaskLogChunk {
                                            dag_id: self.dag_id.clone().unwrap(),
                                            dag_run_id: self.dag_run_id.clone().unwrap(),
                                            task_id: self.task_id.clone().unwrap(),
                                            task_try: self.current_attempt as u16,
                                            continuation_token: token.clone(),
                                        }],
                                    );
                                }
                            }
                        }
                    }
                    KeyCode::Char(c @ '1'..='5') => {
                        // Set log level based on key (1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL)
                        self.min_log_level = match c {
                            '1' => LogLevel::Debug,
                            '2' => LogLevel::Info,
                            '3' => LogLevel::Warning,
                            '4' => LogLevel::Error,
                            '5' => LogLevel::Critical,
                            _ => unreachable!(),
                        };
                        self.vertical_scroll = 0;  // Reset scroll when changing filter
                        return (None, vec![]);
                    }
                    KeyCode::Char('?') => {
                        self.commands = Some(create_log_command_popup());
                        return (None, vec![]);
                    }

                    _ => return (Some(FlowrsEvent::Key(*key)), vec![]), // if no match, return the event
                }
            }
            FlowrsEvent::Mouse => (),
        }

        (None, vec![])
    }
}

impl Widget for &mut LogModel {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        // Check if we have log data
        if self.current_log_data.is_none() || self.is_loading_initial {
            // Show loading spinner
            let frame = SPINNER_FRAMES[self.ticks as usize % SPINNER_FRAMES.len()];
            let loading_text = if self.is_loading_initial {
                format!("{} Loading logs...", frame)
            } else {
                "Loading logs...".to_string()
            };
            
            Paragraph::new(loading_text)
                .block(
                    Block::default()
                        .border_type(BorderType::Rounded)
                        .borders(Borders::ALL)
                        .title("Logs"),
                )
                .render(area, buffer);
            return;
        }
        
        let log_data = self.current_log_data.as_ref().unwrap();
        let total_tries = self.tries.unwrap_or(1) as usize;
        
        // Cache viewport dimensions
        // Subtract 2 for top and bottom borders (BorderType::Rounded with Borders::ALL)
        let viewport_width = area.width;
        self.last_viewport_height = (area.height as usize).saturating_sub(2);
        
        // Check if we need to reparse (content changed)
        let full_content = log_data.full_content();
        let mut hasher = DefaultHasher::new();
        full_content.hash(&mut hasher);
        let content_hash = hasher.finish();
        
        // Only parse if content changed (avoids reparsing every frame!)
        if self.cached_content_hash != content_hash {
            log::debug!("LOG CACHE MISS - Parsing {} bytes into lines", full_content.len());
            // Parse content once to get all lines (handle v1/v2 formats)
            let fragments = parse_content(&full_content);
            
            self.cached_lines = if fragments.is_empty() {
                // v2 format
                full_content.lines().map(|s| s.to_string()).collect()
            } else {
                // v1 format - unescape all Python escape sequences
                let mut lines = Vec::new();
                for (_, log_fragment) in fragments {
                    let unescaped = unescape_python_string(&log_fragment);
                    lines.extend(unescaped.lines().map(|s| s.to_string()));
                }
                lines
            };
            
            // Extract date and timezone from first matching log line
            self.cached_log_date = None;
            self.cached_log_timezone = None;
            // Find the first line that matches the log format (skip any header/metadata lines)
            for line in &self.cached_lines {
                if let Some((date, timezone)) = extract_date_and_timezone(line) {
                    self.cached_log_date = Some(date);
                    self.cached_log_timezone = Some(timezone);
                    break;
                }
            }
            
            log::debug!("LOG CACHE - Parsed into {} lines", self.cached_lines.len());
            self.cached_content_hash = content_hash;
        } else {
            log::debug!("LOG CACHE HIT - Using {} cached lines", self.cached_lines.len());
        }
        
        // Apply log level filtering if needed (with caching)
        if self.cached_filter_level != self.min_log_level || self.cached_filtered_lines.is_empty() {
            log::debug!("LOG FILTER - Filtering {} lines at level {:?}", self.cached_lines.len(), self.min_log_level);
            self.cached_filtered_lines = filter_lines_by_level(&self.cached_lines, self.min_log_level);
            self.cached_filter_level = self.min_log_level;
            log::debug!("LOG FILTER - Filtered to {} lines", self.cached_filtered_lines.len());
        }
        
        // Check if we need to recalculate visual line map (width change, content change, etc.)
        if self.should_recalculate_visual_map(viewport_width) {
            // Before recalculating, remember which logical line we're viewing
            // so we can restore the position after wrapping changes
            let old_logical_line = if !self.cached_visual_line_map.is_empty() {
                // Find the logical line at the current scroll position
                self.visual_to_logical_range(self.vertical_scroll, 1).0
            } else {
                0
            };
            
            log::debug!("VISUAL MAP - Recalculating for width {} (was at logical line {})", 
                viewport_width, old_logical_line);
            
            let (map, total) = self.calculate_visual_line_map(viewport_width);
            self.cached_visual_line_map = map;
            self.cached_total_visual_lines = total;
            self.cached_viewport_width = viewport_width;
            
            // Restore position: find the new visual line for the same logical line
            if old_logical_line < self.cached_visual_line_map.len() {
                let new_visual_pos = self.cached_visual_line_map[old_logical_line].visual_start;
                self.vertical_scroll = new_visual_pos;
                self.vertical_scroll_state = self.vertical_scroll_state.position(new_visual_pos);
                log::debug!("VISUAL MAP - Restored to visual line {} (logical line {})", 
                    new_visual_pos, old_logical_line);
            }
            
            log::debug!("VISUAL MAP - Calculated {} visual lines from {} logical lines", 
                total, self.cached_filtered_lines.len());
        }
        
        let total_visual_lines = self.cached_total_visual_lines;
        
        // VIRTUAL SCROLLING: Convert visual scroll to logical lines, then render with buffer
        let buffer_size = VIRTUAL_SCROLL_BUFFER;
        let viewport_height = self.last_viewport_height;
        
        // Convert visual scroll position to logical line range
        let visual_scroll_start = self.vertical_scroll;
        let visual_scroll_end = visual_scroll_start + viewport_height;
        
        // Add buffer in visual line space
        let visual_start_with_buffer = visual_scroll_start.saturating_sub(buffer_size);
        let visual_end_with_buffer = (visual_scroll_end + buffer_size).min(total_visual_lines);
        
        // Find which logical lines correspond to this visual range (WITH buffer)
        let (logical_start, _visual_offset_in_buffered, logical_end) = self.visual_to_logical_range(
            visual_start_with_buffer,
            visual_end_with_buffer.saturating_sub(visual_start_with_buffer)
        );
        
        // Now find the actual scroll position (WITHOUT buffer) to get the real offset
        let (logical_start_actual, visual_offset_actual, _) = self.visual_to_logical_range(
            visual_scroll_start,
            viewport_height
        );
        
        // We only use logical_start_actual to verify we got the same line
        // The paragraph_scroll_offset should be relative to the ACTUAL scroll position
        let paragraph_scroll_offset = if logical_start == logical_start_actual {
            visual_offset_actual
        } else {
            // Buffer caused us to start earlier, so we need to account for that
            // The offset is from the actual scroll position within the buffered content
            let first_logical_visual_start = self.cached_visual_line_map
                .get(logical_start)
                .map(|m| m.visual_start)
                .unwrap_or(0);
            visual_scroll_start.saturating_sub(first_logical_visual_start)
        };
        
        log::debug!("RENDER - Visual scroll: {}, Visual lines: {}, Logical range: {}-{}, Offset: {}", 
            self.vertical_scroll, total_visual_lines, logical_start, logical_end, paragraph_scroll_offset);
        
        // Build Text with only visible logical lines (wrapping will happen in Paragraph)
        let mut content = Text::default();
        if logical_start < logical_end && logical_end <= self.cached_filtered_lines.len() {
            let skip_date = self.cached_log_date.as_deref();
            let skip_timezone = self.cached_log_timezone.as_deref();
            let mut last_log_level: Option<String> = None;
            
            for (_original_idx, line) in &self.cached_filtered_lines[logical_start..logical_end] {
                let colored_line = colorize_log_line_with_context(
                    line, 
                    skip_date, 
                    skip_timezone, 
                    &mut last_log_level
                );
                content.push_line(colored_line);
            }
        }
        
        // Build titles using helper methods
        let title = self.build_title_line(total_tries);
        let bottom_title = self.build_bottom_title(total_visual_lines, log_data);
        
        #[allow(clippy::cast_possible_truncation)]
        let paragraph = Paragraph::new(content)
            .block(
                Block::default()
                    .border_type(BorderType::Rounded)
                    .borders(Borders::ALL)
                    .border_style(DEFAULT_STYLE.fg(CYAN))
                    .title(title)
                    .title_bottom(bottom_title),
            )
            // WRAPPING ENABLED - long lines wrap at screen edge for readability
            // Visual line mapping ensures accurate scrolling despite wrap
            .style(Style::default().fg(Color::White))
            .wrap(Wrap { trim: false })
            .scroll((paragraph_scroll_offset as u16, 0));
        
        paragraph.render(area, buffer);
        
        // Scrollbar - configure with total VISUAL line count for proper thumb sizing
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"));
        
        // Update scrollbar state with visual line counts (not logical lines)
        self.vertical_scroll_state = self.vertical_scroll_state
            .content_length(total_visual_lines)
            .viewport_content_length(viewport_height);
        
        scrollbar.render(area, buffer, &mut self.vertical_scroll_state);
        
        // Command popup
        if let Some(commands) = &mut self.commands {
            commands.render(area, buffer);
        }
        
        // Error popup
        if let Some(error_popup) = &self.error_popup {
            error_popup.render(area, buffer);
        }
    }
}

// Macro to create lazily-compiled regex functions (DRY pattern for OnceLock)
macro_rules! lazy_regex {
    ($fn_name:ident, $pattern:expr) => {
        fn $fn_name() -> &'static Regex {
            static REGEX: OnceLock<Regex> = OnceLock::new();
            REGEX.get_or_init(|| Regex::new($pattern).unwrap())
        }
    };
}

// Compiled regexes (compiled once, reused forever)
lazy_regex!(
    get_log_regex,
    r#"\(\s*'((?:\\.|[^'])*)'\s*,\s*(?:"((?:\\.|[^"])*)"|'((?:\\.|[^'])*)')\s*\)"#
);

lazy_regex!(
    get_log_line_regex,
    r"^\[([^\]]+)\]\s+\{([^}]+)\}\s+(\w+)\s+-\s+(.*)$"
);

/// Unescape Python string escape sequences using snailquote library.
///
/// Handles all Python escape sequences including:
/// - Basic: \n (newline), \t (tab), \r (carriage return), \\ (backslash)
/// - Quotes: \' (single quote), \" (double quote)
/// - Hex: \xNN (2-digit hex)
/// - Unicode: \uNNNN (4-digit hex), \UNNNNNNNN (8-digit hex)
///
/// # Implementation
/// This function wraps the input with double quotes before passing to snailquote,
/// which expects a quoted string. The input is assumed to follow Python string
/// escaping rules (quotes are already escaped), which is guaranteed since it comes
/// from Python's repr() output via the Airflow API.
///
/// # Error Handling
/// If unescaping fails (e.g., malformed escape sequence), logs a warning and
/// returns the original string unchanged.
fn unescape_python_string(s: &str) -> String {
    // snailquote expects quotes around the string, so we add them temporarily.
    // SAFETY: This is safe because the input comes from Python's repr() output via
    // the Airflow API, which always properly escapes quotes based on the chosen
    // delimiter. The regex pattern (?:\\.|[^'])* also ensures proper extraction.
    let quoted = format!("\"{}\"", s);
    match snailquote::unescape(&quoted) {
        Ok(unescaped) => unescaped,
        Err(e) => {
            log::warn!("Failed to unescape string: {}. Using original. Error: {}", s, e);
            s.to_string()
        }
    }
}

// Log content is a list of tuples of form ('element1', 'element2'), i.e. serialized python tuples
// The second element can be single or double quoted depending on content
pub(crate) fn parse_content(content: &str) -> Vec<(String, String)> {
    // Use pre-compiled regex
    let re = get_log_regex();

    // Use regex to extract tuples
    re.captures_iter(content)
        .map(|cap| {
            let first = cap[1].to_string();
            // Second element can be in group 2 (double quotes) or group 3 (single quotes)
            let second = cap
                .get(2)
                .or_else(|| cap.get(3))
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();
            (first, second)
        })
        .collect()
}

/// Parse and unescape log content for saving to disk
/// Handles both v1 (tuple format with escaped newlines) and v2 (plain text) formats
/// This is the shared implementation used by both rendering and disk persistence
pub(crate) fn parse_and_unescape_log_content(content: &str) -> String {
    let fragments = parse_content(content);
    
    if fragments.is_empty() {
        // v2 format - already plain text, no escaping needed
        content.to_string()
    } else {
        // v1 format - extract log fragments and unescape all Python escape sequences
        let mut result = String::new();
        for (_, log_fragment) in fragments {
            let unescaped = unescape_python_string(&log_fragment);
            result.push_str(&unescaped);
        }
        result
    }
}

// Parse source location into filename and line number
// Example: "taskinstance.py:1157" -> ("taskinstance.py", "1157")
fn parse_source_location(source: &str) -> (&str, &str) {
    if let Some(colon_pos) = source.rfind(':') {
        (&source[..colon_pos], &source[colon_pos + 1..])
    } else {
        // No colon found, treat entire string as filename
        (source, "")
    }
}

// Extract date and timezone from a log line (returns first occurrence)
// Example: "[2025-12-02T04:00:02.468+0900] ..." -> Some(("2025-12-02", "+0900"))
fn extract_date_and_timezone(line: &str) -> Option<(String, String)> {
    let re = get_log_line_regex();
    if let Some(captures) = re.captures(line) {
        let timestamp = &captures[1];
        let (date_part, _, _, _, timezone) = parse_timestamp(timestamp);
        if !date_part.is_empty() && !timezone.is_empty() {
            return Some((date_part.to_string(), timezone.to_string()));
        }
    }
    None
}

/// Extract log level from a log line
/// Example: "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - ..." -> Some(LogLevel::Info)
fn extract_log_level(line: &str) -> Option<LogLevel> {
    let re = get_log_line_regex();
    if let Some(captures) = re.captures(line) {
        let level_str = &captures[3];
        return level_str.parse().ok();
    }
    None
}

/// Check if a line is a log line start (begins with timestamp) vs a continuation line
fn is_log_line_start(line: &str) -> bool {
    line.starts_with('[')
}

/// Filter lines by minimum log level, keeping continuation lines with their parent
/// Returns vector of (original_index, line) tuples
fn filter_lines_by_level(lines: &[String], min_level: LogLevel) -> Vec<(usize, String)> {
    let mut filtered = Vec::new();
    let mut last_level_met_threshold = true;  // Default to true for lines before first log line
    
    for (idx, line) in lines.iter().enumerate() {
        if is_log_line_start(line) {
            // This is a log line start - check its level
            if let Some(level) = extract_log_level(line) {
                last_level_met_threshold = level >= min_level;
                if last_level_met_threshold {
                    filtered.push((idx, line.clone()));
                }
            } else {
                // Malformed log line or non-standard format - include by default
                last_level_met_threshold = true;
                filtered.push((idx, line.clone()));
            }
        } else {
            // Continuation line - include if parent log line met threshold
            if last_level_met_threshold {
                filtered.push((idx, line.clone()));
            }
        }
    }
    
    filtered
}

// Build timestamp spans with optional skipping of date/timezone components
fn build_timestamp_spans(
    timestamp: &str,
    skip_date: Option<&str>,
    skip_timezone: Option<&str>,
) -> Vec<Span<'static>> {
    let mut spans = vec![
        Span::styled("[".to_string(), Style::default().fg(BRIGHT_BLACK)),
    ];
    
    let (date_part, t_sep, time_part, millis, timezone) = parse_timestamp(timestamp);
    
    // Check if we should skip date/timezone (when they match cached values)
    let should_skip_date = skip_date.is_some() && skip_date == Some(date_part);
    let should_skip_timezone = skip_timezone.is_some() && skip_timezone == Some(timezone);
    
    // Add date part if present and not skipped (BLUE - calm, readable date color)
    if !date_part.is_empty() && !should_skip_date {
        spans.push(Span::styled(date_part.to_string(), Style::default().fg(BLUE)));
        // Add T separator if showing date
        if !t_sep.is_empty() {
            spans.push(Span::styled(t_sep.to_string(), Style::default().fg(BRIGHT_BLACK)));
        }
    }
    
    // Add time part (HH:MM:SS) in CYAN (distinct from log level colors)
    if !time_part.is_empty() {
        spans.push(Span::styled(time_part.to_string(), Style::default().fg(CYAN)));
    }
    
    // Add milliseconds in GRAY (de-emphasized as requested)
    if !millis.is_empty() {
        spans.push(Span::styled(millis.to_string(), Style::default().fg(BRIGHT_BLACK)));
    }
    
    // Add timezone with gray separator (only if not skipped)
    if !timezone.is_empty() && !should_skip_timezone {
        // Split timezone into separator (+/-) and offset
        if timezone.len() > 1 {
            let tz_sep = &timezone[..1];  // + or -
            let tz_offset = &timezone[1..];  // 0900
            spans.push(Span::styled(tz_sep.to_string(), Style::default().fg(BRIGHT_BLACK)));
            spans.push(Span::styled(tz_offset.to_string(), Style::default().fg(MAGENTA)));
        } else {
            spans.push(Span::styled(timezone.to_string(), Style::default().fg(MAGENTA)));
        }
    }
    
    // If timestamp parsing failed, just show the whole timestamp
    if date_part.is_empty() && time_part.is_empty() {
        spans.push(Span::styled(timestamp.to_string(), Style::default().fg(CYAN)));
    }
    
    spans.push(Span::styled("]".to_string(), Style::default().fg(BRIGHT_BLACK)));
    spans
}

// Parse timestamp to extract components for highlighting
// Example: "2025-12-02T04:00:02.468+0900" -> ("2025-12-02", "T", "04:00:02", ".468", "+0900")
// Returns: (date, t_sep, time, millis, timezone)
fn parse_timestamp(timestamp: &str) -> (&str, &str, &str, &str, &str) {
    // Find the 'T' separator between date and time
    if let Some(t_pos) = timestamp.find('T') {
        let date_part = &timestamp[..t_pos];  // Date without 'T'
        let t_sep = "T";
        let rest = &timestamp[t_pos + 1..];
        
        // Find where time ends (before milliseconds '.' or timezone '+'/'-')
        if let Some(ms_pos) = rest.find('.') {
            let time_part = &rest[..ms_pos];
            let after_time = &rest[ms_pos..];
            
            // Find timezone separator ('+' or '-')
            if let Some(tz_pos) = after_time.find(|c| c == '+' || c == '-') {
                let millis = &after_time[..tz_pos];
                (date_part, t_sep, time_part, millis, &after_time[tz_pos..])
            } else {
                // No timezone, just milliseconds
                (date_part, t_sep, time_part, after_time, "")
            }
        } else if let Some(tz_pos) = rest.find(|c| c == '+' || c == '-') {
            // No milliseconds, but has timezone
            let time_part = &rest[..tz_pos];
            (date_part, t_sep, time_part, "", &rest[tz_pos..])
        } else {
            // No milliseconds or timezone, entire rest is time
            (date_part, t_sep, rest, "", "")
        }
    } else {
        // No 'T' found, return entire timestamp as-is
        (timestamp, "", "", "", "")
    }
}

// Colorize a single log line based on Airflow log format
// If skip_date and skip_timezone are Some, those components will be omitted from the timestamp
// Tracks last_log_level to style continuation lines consistently
fn colorize_log_line_with_context(
    line: &str, 
    skip_date: Option<&str>, 
    skip_timezone: Option<&str>,
    last_log_level: &mut Option<String>
) -> Line<'static> {
    let re = get_log_line_regex();
    
    if let Some(captures) = re.captures(line) {
        let timestamp = &captures[1];
        let source = &captures[2];
        let level = &captures[3];
        let message = &captures[4];
        
        // Update last log level for continuation lines (store as String)
        *last_log_level = Some(level.to_string());
        
        // Parse source into filename and line number
        let (filename, line_num) = parse_source_location(source);
        let filename_color = hash_to_color(filename);
        
        // Get log level style for coordinating colors
        let level_style = get_level_style(level);
        
        // Build timestamp spans with optional skipping
        let mut spans = build_timestamp_spans(timestamp, skip_date, skip_timezone);
        spans.push(Span::raw(" "));
        
        // {filename:line} - braces gray, filename hashed color, line number matches log level
        spans.push(Span::styled("{".to_string(), Style::default().fg(BRIGHT_BLACK)));
        spans.push(Span::styled(filename.to_string(), Style::default().fg(filename_color)));
        
        // Add line number if present - CYAN to match timestamp
        if !line_num.is_empty() {
            spans.push(Span::styled(":".to_string(), Style::default().fg(BRIGHT_BLACK)));
            spans.push(Span::styled(line_num.to_string(), Style::default().fg(CYAN)));
        }
        
        spans.extend(vec![
            Span::styled("}".to_string(), Style::default().fg(BRIGHT_BLACK)),
            Span::raw(" "),
            // LEVEL - colored by severity
            Span::styled(level.to_string(), level_style),
            // - message (colored by log level)
            Span::styled(" - ".to_string(), Style::default().fg(BRIGHT_BLACK)),
            Span::styled(message.to_string(), level_style),
        ]);
        
        Line::from(spans)
    } else {
        // Continuation line - style based on the parent log line's level
        if let Some(level) = last_log_level {
            let level_style = get_level_style(&level);
            Line::from(vec![Span::styled(line.to_string(), level_style)])
        } else {
            // Fallback: unformatted line before any proper log line (e.g., headers)
            Line::raw(line.to_string())
        }
    }
}

// Colorize a single log line based on Airflow log format
// If skip_date and skip_timezone are Some, those components will be omitted from the timestamp
fn colorize_log_line_with_options(line: &str, skip_date: Option<&str>, skip_timezone: Option<&str>) -> Line<'static> {
    let mut dummy_context = None;
    colorize_log_line_with_context(line, skip_date, skip_timezone, &mut dummy_context)
}

// Wrapper function for backward compatibility (no skipping)
fn colorize_log_line(line: &str) -> Line<'static> {
    colorize_log_line_with_options(line, None, None)
}

// Get color style for log level
fn get_level_style(level: &str) -> Style {
    match level {
        "DEBUG" => Style::default().fg(BLUE),
        "INFO" => Style::default().fg(GREEN),
        "WARNING" => Style::default().fg(YELLOW),
        "ERROR" => Style::default().fg(RED),
        "CRITICAL" => Style::default().fg(RED).add_modifier(Modifier::BOLD),
        _ => Style::default().fg(FOREGROUND),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unescape_python_string_newlines() {
        assert_eq!(unescape_python_string("line1\\nline2"), "line1\nline2");
        assert_eq!(unescape_python_string("hello\\nworld\\n"), "hello\nworld\n");
    }
    
    #[test]
    fn test_unescape_python_string_tabs() {
        assert_eq!(unescape_python_string("col1\\tcol2"), "col1\tcol2");
        assert_eq!(unescape_python_string("\\thello"), "\thello");
    }
    
    #[test]
    fn test_unescape_python_string_carriage_return() {
        assert_eq!(unescape_python_string("line1\\rline2"), "line1\rline2");
    }
    
    #[test]
    fn test_unescape_python_string_quotes() {
        assert_eq!(unescape_python_string("it\\'s"), "it's");
        assert_eq!(unescape_python_string("say \\\"hello\\\""), "say \"hello\"");
    }
    
    #[test]
    fn test_unescape_python_string_backslash() {
        assert_eq!(unescape_python_string("path\\\\to\\\\file"), "path\\to\\file");
    }
    
    #[test]
    fn test_unescape_python_string_hex_escape() {
        assert_eq!(unescape_python_string("\\x41\\x42\\x43"), "ABC");
        assert_eq!(unescape_python_string("hello\\x20world"), "hello world");
    }
    
    #[test]
    fn test_unescape_python_string_unicode_escape() {
        assert_eq!(unescape_python_string("\\u2764"), "❤");
        assert_eq!(unescape_python_string("\\u03B1\\u03B2\\u03B3"), "αβγ");
    }
    
    #[test]
    fn test_unescape_python_string_mixed() {
        let input = "Line 1\\nTab:\\there\\nQuote: \\'test\\'\\nBackslash: \\\\";
        let expected = "Line 1\nTab:\there\nQuote: 'test'\nBackslash: \\";
        assert_eq!(unescape_python_string(input), expected);
    }
    
    #[test]
    fn test_unescape_python_string_real_world() {
        // Simulate real Airflow log with multiple escape types
        let input = "*** Found local files:\\n***   * /opt/airflow/logs/dag_id=test\\n[2025-12-02] INFO - Task \\'my_task\\' started";
        let expected = "*** Found local files:\n***   * /opt/airflow/logs/dag_id=test\n[2025-12-02] INFO - Task 'my_task' started";
        assert_eq!(unescape_python_string(input), expected);
    }
    
    #[test]
    fn test_unescape_python_string_malformed_incomplete_escape() {
        // Test that incomplete escape sequences fall back gracefully
        let input = "incomplete escape \\";
        let result = unescape_python_string(input);
        // Should return original string on error
        assert_eq!(result, input);
    }
    
    #[test]
    fn test_unescape_python_string_malformed_invalid_hex() {
        // Invalid hex escape should fall back
        let input = "invalid \\xZZ hex";
        let result = unescape_python_string(input);
        assert_eq!(result, input);
    }
    
    #[test]
    fn test_unescape_python_string_malformed_invalid_unicode() {
        // Invalid unicode escape should fall back
        let input = "invalid \\uGGGG unicode";
        let result = unescape_python_string(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_parse_content_single_quotes() {
        let content = "[('host1', 'log content here')]";
        let result = parse_content(content);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "host1");
        assert_eq!(result[0].1, "log content here");
    }

    #[test]
    fn test_parse_content_double_quotes_second_element() {
        let content = r#"[('cec849a302e3', "*** Found local files:\n***   * /opt/airflow/logs/")]"#;
        let result = parse_content(content);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "cec849a302e3");
        // Note: This just parses, doesn't unescape yet - that's done by unescape_python_string
        assert_eq!(
            result[0].1,
            r"*** Found local files:\n***   * /opt/airflow/logs/"
        );
    }
    
    #[test]
    fn test_parse_and_unescape_log_content_v1() {
        // Test v1 format with escape sequences
        let content = r#"[('host1', 'Line 1\nLine 2\tTabbed')]"#;
        let result = parse_and_unescape_log_content(content);
        assert_eq!(result, "Line 1\nLine 2\tTabbed");
    }
    
    #[test]
    fn test_parse_and_unescape_log_content_v2() {
        // Test v2 format (plain text)
        let content = "Plain text log\nNo tuples here";
        let result = parse_and_unescape_log_content(content);
        assert_eq!(result, content);
    }
    
    #[test]
    fn test_parse_and_unescape_log_content_with_quotes() {
        let content = r#"[('host', 'Message: \"Hello\"\nNext line')]"#;
        let result = parse_and_unescape_log_content(content);
        assert_eq!(result, "Message: \"Hello\"\nNext line");
    }

    #[test]
    fn test_parse_content_with_escaped_quotes() {
        let content = r"[('host', 'line with \' escaped quote')]";
        let result = parse_content(content);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "host");
        assert_eq!(result[0].1, r"line with \' escaped quote");
    }

    #[test]
    fn test_parse_content_multiple_tuples() {
        let content = r#"[('host1', 'log1'), ('host2', "log2 with special chars")]"#;
        let result = parse_content(content);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("host1".to_string(), "log1".to_string()));
        assert_eq!(
            result[1],
            ("host2".to_string(), "log2 with special chars".to_string())
        );
    }

    #[test]
    fn test_parse_source_location() {
        assert_eq!(parse_source_location("taskinstance.py:1157"), 
                   ("taskinstance.py", "1157"));
        assert_eq!(parse_source_location("my_module.py:42"), 
                   ("my_module.py", "42"));
        assert_eq!(parse_source_location("no_line_number.py"), 
                   ("no_line_number.py", ""));
    }

    #[test]
    fn test_parse_timestamp() {
        // Format: (date, "T", time, millis, timezone)
        assert_eq!(parse_timestamp("2025-12-02T04:00:02.468+0900"),
                   ("2025-12-02", "T", "04:00:02", ".468", "+0900"));
        assert_eq!(parse_timestamp("2025-12-02T14:30:45.123-0500"),
                   ("2025-12-02", "T", "14:30:45", ".123", "-0500"));
        assert_eq!(parse_timestamp("2025-12-02T23:59:59+0000"),
                   ("2025-12-02", "T", "23:59:59", "", "+0000"));
    }

    #[test]
    fn test_colorize_log_line_standard_format() {
        let line = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Dependencies all met for task";
        let colored = colorize_log_line(line);
        // Should have 18 spans now with fully split timestamp: 
        // [ + date + T + time + millis + + + tz + ] + space + { + filename + : + line + } + space + LEVEL + sep + message
        // Breakdown: [ (1) + date (2) + T (3) + time (4) + millis (5) + + (6) + tz (7) + ] (8) + space (9) 
        //            + { (10) + filename (11) + : (12) + line (13) + } (14) + space (15) + LEVEL (16) + sep (17) + message (18)
        assert_eq!(colored.spans.len(), 18);
    }

    #[test]
    fn test_colorize_log_line_different_levels() {
        let levels = vec!["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"];
        for level in levels {
            let line = format!("[2025-12-02T04:00:02.468+0900] {{taskinstance.py:1157}} {} - Test message", level);
            let colored = colorize_log_line(&line);
            assert_eq!(colored.spans.len(), 18);
        }
    }

    #[test]
    fn test_colorize_log_line_malformed() {
        let line = "This is not a standard log line format";
        let colored = colorize_log_line(line);
        // Should fall back to raw text (1 span)
        assert_eq!(colored.spans.len(), 1);
    }

    #[test]
    fn test_parse_content_real_airflow_log() {
        let content = r#"[('cec849a302e3', "*** Found local files:\\n***   * /opt/airflow/logs/dag_id=dataset_consumes_1/run_id=dataset_triggered__2025-10-12T01:24:15.313731+00:00/task_id=consuming_1/attempt=1.log\\n[2025-10-12T01:24:16.754+0000] {local_task_job_runner.py:123} INFO - ::group::Pre task execution logs")]"#;
        let result = parse_content(content);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "cec849a302e3");
        assert!(result[0].1.starts_with("*** Found local files:"));
    }
    
    #[test]
    fn test_extract_date_and_timezone() {
        // Standard format with positive timezone
        let line = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Dependencies all met";
        let result = extract_date_and_timezone(line);
        assert_eq!(result, Some(("2025-12-02".to_string(), "+0900".to_string())));
        
        // Different timezone (negative)
        let line2 = "[2025-12-02T14:30:45.123-0500] {local_task_job_runner.py:123} INFO - Test";
        let result2 = extract_date_and_timezone(line2);
        assert_eq!(result2, Some(("2025-12-02".to_string(), "-0500".to_string())));
        
        // UTC timezone
        let line3 = "[2025-10-12T01:24:16.754+0000] {local_task_job_runner.py:123} INFO - UTC test";
        let result3 = extract_date_and_timezone(line3);
        assert_eq!(result3, Some(("2025-10-12".to_string(), "+0000".to_string())));
        
        // Malformed line
        let line4 = "Not a log line";
        let result4 = extract_date_and_timezone(line4);
        assert_eq!(result4, None);
    }
    
    #[test]
    fn test_colorize_log_line_with_skip() {
        let line = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Test message";
        
        // Without skipping - should have more spans
        let full = colorize_log_line(line);
        
        // With skipping date and timezone - should have fewer spans
        let shortened = colorize_log_line_with_options(line, Some("2025-12-02"), Some("+0900"));
        
        // Shortened version should have fewer spans (no date, no T separator, no timezone)
        assert!(shortened.spans.len() < full.spans.len());
    }
    
    #[test]
    fn test_colorize_with_partial_skip_date_only() {
        // Test skipping only date, not timezone
        let line = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Test";
        
        let full = colorize_log_line(line);
        let partial = colorize_log_line_with_options(line, Some("2025-12-02"), None);
        
        // Should skip date but still show timezone
        // partial should have fewer spans than full but more than if both were skipped
        assert!(partial.spans.len() < full.spans.len());
        
        let both_skipped = colorize_log_line_with_options(line, Some("2025-12-02"), Some("+0900"));
        assert!(partial.spans.len() > both_skipped.spans.len());
    }
    
    #[test]
    fn test_colorize_with_partial_skip_timezone_only() {
        // Test skipping only timezone, not date
        let line = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Test";
        
        let full = colorize_log_line(line);
        let partial = colorize_log_line_with_options(line, None, Some("+0900"));
        
        // Should skip timezone but still show date
        assert!(partial.spans.len() < full.spans.len());
        
        let both_skipped = colorize_log_line_with_options(line, Some("2025-12-02"), Some("+0900"));
        assert!(partial.spans.len() > both_skipped.spans.len());
    }
    
    #[test]
    fn test_colorize_with_mismatched_skip_values() {
        // Test that skip values must match exactly to trigger skipping
        let line = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Test";
        
        let full = colorize_log_line(line);
        
        // Wrong date - should not skip
        let wrong_date = colorize_log_line_with_options(line, Some("2025-12-03"), Some("+0900"));
        assert_eq!(wrong_date.spans.len(), full.spans.len());
        
        // Wrong timezone - should not skip
        let wrong_tz = colorize_log_line_with_options(line, Some("2025-12-02"), Some("-0500"));
        assert_eq!(wrong_tz.spans.len(), full.spans.len());
    }
    
    #[test]
    fn test_extract_date_from_lines_with_headers() {
        // Test that extraction skips non-matching header lines
        let lines = vec![
            "*** Found local files:",
            "***   * /opt/airflow/logs/dag_id=test/run_id=manual__2025-12-04/task_id=my_task/attempt=1.log",
            "[2025-12-04T10:30:45.123+0900] {taskinstance.py:1157} INFO - Starting task execution",
            "[2025-12-04T10:30:46.456+0900] {taskinstance.py:1158} INFO - Task running",
        ];
        
        // Should extract from the first line that matches the log format (line 3)
        let result = extract_date_and_timezone(lines[2]);
        assert_eq!(result, Some(("2025-12-04".to_string(), "+0900".to_string())));
        
        // Header lines should return None
        assert_eq!(extract_date_and_timezone(lines[0]), None);
        assert_eq!(extract_date_and_timezone(lines[1]), None);
    }
    
    #[test]
    fn test_extract_date_from_empty_content() {
        // Test that empty content returns None gracefully
        let empty_line = "";
        let result = extract_date_and_timezone(empty_line);
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_colorize_empty_line() {
        // Test that empty lines are handled gracefully
        let empty = "";
        let colored = colorize_log_line(empty);
        // Should fall back to raw text (1 span)
        assert_eq!(colored.spans.len(), 1);
    }
    
    #[test]
    fn test_log_level_from_str() {
        assert_eq!("DEBUG".parse::<LogLevel>(), Ok(LogLevel::Debug));
        assert_eq!("debug".parse::<LogLevel>(), Ok(LogLevel::Debug));
        assert_eq!("INFO".parse::<LogLevel>(), Ok(LogLevel::Info));
        assert_eq!("WARNING".parse::<LogLevel>(), Ok(LogLevel::Warning));
        assert_eq!("ERROR".parse::<LogLevel>(), Ok(LogLevel::Error));
        assert_eq!("CRITICAL".parse::<LogLevel>(), Ok(LogLevel::Critical));
        assert!("UNKNOWN".parse::<LogLevel>().is_err());
    }
    
    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warning);
        assert!(LogLevel::Warning < LogLevel::Error);
        assert!(LogLevel::Error < LogLevel::Critical);
        assert!(LogLevel::Critical >= LogLevel::Error);
    }
    
    #[test]
    fn test_extract_log_level() {
        let line = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Test message";
        assert_eq!(extract_log_level(line), Some(LogLevel::Info));
        
        let line2 = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} ERROR - Test error";
        assert_eq!(extract_log_level(line2), Some(LogLevel::Error));
        
        let malformed = "Not a log line";
        assert_eq!(extract_log_level(malformed), None);
    }
    
    #[test]
    fn test_is_log_line_start() {
        assert!(is_log_line_start("[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Test"));
        assert!(!is_log_line_start("    This is a continuation line"));
        assert!(!is_log_line_start("Another continuation"));
    }
    
    #[test]
    fn test_filter_lines_by_level_basic() {
        let lines = vec![
            "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} DEBUG - Debug message".to_string(),
            "[2025-12-02T04:00:03.468+0900] {taskinstance.py:1158} INFO - Info message".to_string(),
            "[2025-12-02T04:00:04.468+0900] {taskinstance.py:1159} WARNING - Warning message".to_string(),
            "[2025-12-02T04:00:05.468+0900] {taskinstance.py:1160} ERROR - Error message".to_string(),
        ];
        
        // Filter at INFO level - should exclude DEBUG
        let filtered = filter_lines_by_level(&lines, LogLevel::Info);
        assert_eq!(filtered.len(), 3);
        assert!(filtered[0].1.contains("INFO"));
        assert!(filtered[1].1.contains("WARNING"));
        assert!(filtered[2].1.contains("ERROR"));
        
        // Filter at WARNING level - should exclude DEBUG and INFO
        let filtered = filter_lines_by_level(&lines, LogLevel::Warning);
        assert_eq!(filtered.len(), 2);
        assert!(filtered[0].1.contains("WARNING"));
        assert!(filtered[1].1.contains("ERROR"));
        
        // Filter at ERROR level - should only include ERROR
        let filtered = filter_lines_by_level(&lines, LogLevel::Error);
        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].1.contains("ERROR"));
    }
    
    #[test]
    fn test_filter_lines_with_continuations() {
        let lines = vec![
            "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} DEBUG - Debug message".to_string(),
            "    This is a continuation of debug".to_string(),
            "    Another continuation".to_string(),
            "[2025-12-02T04:00:03.468+0900] {taskinstance.py:1158} INFO - Info message".to_string(),
            "    Info continuation line 1".to_string(),
            "    Info continuation line 2".to_string(),
            "[2025-12-02T04:00:04.468+0900] {taskinstance.py:1159} ERROR - Error message".to_string(),
            "    Error continuation".to_string(),
        ];
        
        // Filter at INFO level - should exclude DEBUG and its continuations
        let filtered = filter_lines_by_level(&lines, LogLevel::Info);
        assert_eq!(filtered.len(), 5); // INFO + 2 continuations + ERROR + 1 continuation
        assert!(filtered[0].1.contains("INFO"));
        assert!(filtered[1].1.contains("Info continuation line 1"));
        assert!(filtered[2].1.contains("Info continuation line 2"));
        assert!(filtered[3].1.contains("ERROR"));
        assert!(filtered[4].1.contains("Error continuation"));
        
        // Filter at ERROR level - should only include ERROR and its continuation
        let filtered = filter_lines_by_level(&lines, LogLevel::Error);
        assert_eq!(filtered.len(), 2); // ERROR + 1 continuation
        assert!(filtered[0].1.contains("ERROR"));
        assert!(filtered[1].1.contains("Error continuation"));
    }
    
    #[test]
    fn test_filter_lines_with_malformed() {
        let lines = vec![
            "*** Found local files:".to_string(),
            "***   * /opt/airflow/logs/dag_id=test/".to_string(),
            "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Info message".to_string(),
            "    Continuation".to_string(),
        ];
        
        // Malformed lines at the start should be included
        let filtered = filter_lines_by_level(&lines, LogLevel::Info);
        assert_eq!(filtered.len(), 4); // All lines included (malformed treated as meeting threshold)
    }
    
    #[test]
    fn test_filter_lines_preserves_indices() {
        let lines = vec![
            "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} DEBUG - Debug".to_string(),
            "[2025-12-02T04:00:03.468+0900] {taskinstance.py:1158} INFO - Info".to_string(),
            "[2025-12-02T04:00:04.468+0900] {taskinstance.py:1159} ERROR - Error".to_string(),
        ];
        
        // Filter at INFO level
        let filtered = filter_lines_by_level(&lines, LogLevel::Info);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].0, 1); // Original index of INFO line
        assert_eq!(filtered[1].0, 2); // Original index of ERROR line
    }
    
    #[test]
    fn test_colorize_with_context_multiple_continuations() {
        use ratatui::style::Color;
        
        let mut context = None;
        
        // First log line - INFO level
        let line1 = "[2025-12-02T04:00:02.468+0900] {taskinstance.py:1157} INFO - Start";
        let _colored1 = colorize_log_line_with_context(line1, None, None, &mut context);
        assert!(context.is_some());
        assert_eq!(context.as_ref().unwrap(), "INFO");
        
        // First continuation - should use INFO style (GREEN)
        let line2 = "    Continuation 1";
        let colored2 = colorize_log_line_with_context(line2, None, None, &mut context);
        assert_eq!(colored2.spans.len(), 1);
        assert_eq!(colored2.spans[0].style.fg, Some(GREEN)); // INFO color
        
        // Second continuation - should still use INFO style
        let line3 = "    Continuation 2";
        let colored3 = colorize_log_line_with_context(line3, None, None, &mut context);
        assert_eq!(colored3.spans.len(), 1);
        assert_eq!(colored3.spans[0].style.fg, Some(GREEN));
        
        // New log line - ERROR level
        let line4 = "[2025-12-02T04:00:03.468+0900] {taskinstance.py:1158} ERROR - Error";
        let _colored4 = colorize_log_line_with_context(line4, None, None, &mut context);
        assert_eq!(context.as_ref().unwrap(), "ERROR");
        
        // Continuation of ERROR - should use ERROR style (RED)
        let line5 = "    Error continuation";
        let colored5 = colorize_log_line_with_context(line5, None, None, &mut context);
        assert_eq!(colored5.spans.len(), 1);
        assert_eq!(colored5.spans[0].style.fg, Some(RED)); // ERROR color
        
        // WARNING level
        let line6 = "[2025-12-02T04:00:04.468+0900] {taskinstance.py:1159} WARNING - Warning";
        let _colored6 = colorize_log_line_with_context(line6, None, None, &mut context);
        assert_eq!(context.as_ref().unwrap(), "WARNING");
        
        // Continuation of WARNING - should use WARNING style (YELLOW)
        let line7 = "    Warning continuation";
        let colored7 = colorize_log_line_with_context(line7, None, None, &mut context);
        assert_eq!(colored7.spans.len(), 1);
        assert_eq!(colored7.spans[0].style.fg, Some(YELLOW)); // WARNING color
    }
}
