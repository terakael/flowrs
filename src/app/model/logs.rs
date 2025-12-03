use crossterm::event::KeyCode;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, BorderType, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
        StatefulWidget, Tabs, Widget,
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
        DM_RGB, BRIGHT_BLACK, CYAN, BLUE, GREEN, YELLOW, RED, FOREGROUND, MAGENTA,
    },
};

use super::popup::error::ErrorPopup;
use super::{Model, handle_vertical_scroll_keys};

// Constants for log viewer configuration
const LRU_CACHE_SIZE: usize = 5;          // Number of recently viewed attempts to keep in cache
const MAX_VISIBLE_TABS: usize = 5;        // Maximum attempt tabs visible at once
const VIRTUAL_SCROLL_BUFFER: usize = 100; // Lines to render beyond visible viewport

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
    pub error_popup: Option<ErrorPopup>,
    ticks: u32,
    vertical_scroll: usize,
    vertical_scroll_state: ScrollbarState,
    last_viewport_height: usize,          // Cached from last render for auto-load
    cached_lines: Vec<String>,            // CACHE: Parsed lines to avoid reparsing every frame
    cached_content_hash: u64,             // Hash to detect when content changes
    event_buffer: Vec<FlowrsEvent>,       // Buffer for 'gg' detection
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
            error_popup: None,
            ticks: 0,
            vertical_scroll: 0,
            vertical_scroll_state: ScrollbarState::default(),
            last_viewport_height: 20,  // Default viewport size
            cached_lines: Vec::new(),
            cached_content_hash: 0,
            event_buffer: Vec::new(),
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
                if let Some(_error_popup) = &mut self.error_popup {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            self.error_popup = None;
                        }
                        _ => (),
                    }
                    return (None, vec![]);
                }
                
                // Handle standard scrolling keybinds
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
                        self.update_lru(prev_attempt as u16);
                        
                        return (None, vec![WorkerMessage::EnsureTaskLogLoaded {
                            dag_id: self.dag_id.clone().unwrap(),
                            dag_run_id: self.dag_run_id.clone().unwrap(),
                            task_id: self.task_id.clone().unwrap(),
                            task_try: prev_attempt as u16,
                        }]);
                    }
                    KeyCode::Char('G') => {
                        // Jump to bottom
                        if !self.cached_lines.is_empty() {
                            let total_lines = self.cached_lines.len();
                            let max_scroll = total_lines.saturating_sub(self.last_viewport_height);
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
            let spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
            let frame = spinner_frames[self.ticks as usize % spinner_frames.len()];
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
        
        // Scrollable tabs
        let (start_idx, end_idx, prefix, suffix) = if total_tries <= MAX_VISIBLE_TABS {
            (1, total_tries + 1, "", "")
        } else {
            let half_window = MAX_VISIBLE_TABS / 2;
            let mut start = self.current_attempt.saturating_sub(half_window);
            let mut end = start + MAX_VISIBLE_TABS;
            
            if end > total_tries {
                end = total_tries;
                start = end.saturating_sub(MAX_VISIBLE_TABS).max(1);
            }
            
            let prefix = if start > 1 { "← " } else { "" };
            let suffix = if end < total_tries { " →" } else { "" };
            
            (start, end + 1, prefix, suffix)
        };
        
        let tab_titles: Vec<String> = (start_idx..end_idx)
            .map(|i| {
                if i == self.current_attempt {
                    format!("[ Try {} ]", i)  // Highlight current
                } else {
                    format!("Try {}", i)
                }
            })
            .collect();
        
        let title = if total_tries > 1 {
            format!("{}Logs (Attempt {}/{}){}",  prefix, self.current_attempt, total_tries, suffix)
        } else {
            "Logs".to_string()
        };
        
        let tabs = Tabs::new(tab_titles)
            .block(
                Block::default()
                    .border_type(BorderType::Rounded)
                    .title(title)
                    .borders(Borders::ALL),
            )
            .select(self.current_attempt - start_idx)
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .style(Style::default().fg(DM_RGB));
        
        tabs.render(area, buffer);
        
        // Layout: tabs + content
        let chunks = Layout::default()
            .constraints([Constraint::Length(3), Constraint::Min(0)])
            .split(area);
        
        // Cache viewport height for auto-load check
        self.last_viewport_height = (chunks[1].height as usize).saturating_sub(2);
        
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
                // v1 format
                let mut lines = Vec::new();
                for (_, log_fragment) in fragments {
                    let replaced_log = log_fragment.replace("\\n", "\n");
                    lines.extend(replaced_log.lines().map(|s| s.to_string()));
                }
                lines
            };
            
            log::debug!("LOG CACHE - Parsed into {} lines", self.cached_lines.len());
            self.cached_content_hash = content_hash;
        } else {
            log::debug!("LOG CACHE HIT - Using {} cached lines", self.cached_lines.len());
        }
        
        let all_lines = &self.cached_lines;
        let total_lines = all_lines.len();
        
        // VIRTUAL SCROLLING: Only build Line objects for visible window + buffer
        // Calculate visible window with buffer for smoother scrolling
        let buffer_size = VIRTUAL_SCROLL_BUFFER;
        let viewport_height = self.last_viewport_height;
        
        let start_line = self.vertical_scroll.saturating_sub(buffer_size);
        let end_line = (self.vertical_scroll + viewport_height + buffer_size).min(total_lines);
        
        // Build Text with only visible lines (no padding), colorized
        let mut content = Text::default();
        if start_line < end_line && end_line <= all_lines.len() {
            for line in &all_lines[start_line..end_line] {
                content.push_line(colorize_log_line(line));
            }
        }
        
        // Calculate scroll offset relative to the windowed content
        // The Paragraph's scroll should be: (actual_scroll - window_start)
        let window_scroll = self.vertical_scroll.saturating_sub(start_line);
        
        log::debug!("RENDER - Total: {}, Window: {}-{}, Scroll: {} -> {}", 
            total_lines, start_line, end_line, self.vertical_scroll, window_scroll);
        
        // Build content title with status
        let spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        let frame = spinner_frames[self.ticks as usize % spinner_frames.len()];
        
        let content_title = if self.is_loading_more {
            format!("Content ({} lines, {} loading more...)", total_lines, frame)
        } else if log_data.has_more() {
            format!("Content ({} lines, press 'm' for more)", total_lines)
        } else {
            format!("Content ({} lines, complete)", total_lines)
        };
        
        #[allow(clippy::cast_possible_truncation)]
        let paragraph = Paragraph::new(content)
            .block(
                Block::default()
                    .border_type(BorderType::Rounded)
                    .borders(Borders::ALL)
                    .title(content_title),
            )
            // NO WRAPPING - long lines truncate at screen edge (like vim/less)
            // This ensures 1 logical line = 1 visual line for accurate scrolling
            .style(Style::default().fg(Color::White))
            .scroll((window_scroll as u16, 0));
        
        paragraph.render(chunks[1], buffer);
        
        // Scrollbar - configure with total content length for proper thumb sizing
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"));
        
        // Update scrollbar state with total content size
        self.vertical_scroll_state = self.vertical_scroll_state
            .content_length(total_lines)
            .viewport_content_length(viewport_height);
        
        scrollbar.render(chunks[1], buffer, &mut self.vertical_scroll_state);
        
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

// Log content is a list of tuples of form ('element1', 'element2'), i.e. serialized python tuples
// The second element can be single or double quoted depending on content
fn parse_content(content: &str) -> Vec<(String, String)> {
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
fn colorize_log_line(line: &str) -> Line<'static> {
    let re = get_log_line_regex();
    
    if let Some(captures) = re.captures(line) {
        let timestamp = &captures[1];
        let source = &captures[2];
        let level = &captures[3];
        let message = &captures[4];
        
        // Parse source into filename and line number
        let (filename, line_num) = parse_source_location(source);
        let filename_color = hash_to_color(filename);
        
        // Get log level style for coordinating colors
        let level_style = get_level_style(level);
        
        // Parse timestamp to highlight time component with gray separators
        let (date_part, t_sep, time_part, millis, timezone) = parse_timestamp(timestamp);
        
        // Build styled spans with separate colors for each component
        let mut spans = vec![
            // [timestamp] - brackets gray, components colored, separators gray
            Span::styled("[".to_string(), Style::default().fg(BRIGHT_BLACK)),
        ];
        
        // Add date part if present (BLUE - calm, readable date color)
        if !date_part.is_empty() {
            spans.push(Span::styled(date_part.to_string(), Style::default().fg(BLUE)));
        }
        
        // Add T separator (GRAY - structural element)
        if !t_sep.is_empty() {
            spans.push(Span::styled(t_sep.to_string(), Style::default().fg(BRIGHT_BLACK)));
        }
        
        // Add time part (HH:MM:SS) in YELLOW for easy scanning
        if !time_part.is_empty() {
            spans.push(Span::styled(time_part.to_string(), Style::default().fg(YELLOW)));
        }
        
        // Add milliseconds in CYAN (secondary detail)
        if !millis.is_empty() {
            spans.push(Span::styled(millis.to_string(), Style::default().fg(CYAN)));
        }
        
        // Add timezone with gray separator
        if !timezone.is_empty() {
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
        spans.push(Span::raw(" "));
        
        // {filename:line} - braces gray, filename hashed color, line number matches log level
        spans.push(Span::styled("{".to_string(), Style::default().fg(BRIGHT_BLACK)));
        spans.push(Span::styled(filename.to_string(), Style::default().fg(filename_color)));
        
        // Add line number if present - color matches log level for visual coordination
        if !line_num.is_empty() {
            spans.push(Span::styled(":".to_string(), Style::default().fg(BRIGHT_BLACK)));
            spans.push(Span::styled(line_num.to_string(), level_style));
        }
        
        spans.extend(vec![
            Span::styled("}".to_string(), Style::default().fg(BRIGHT_BLACK)),
            Span::raw(" "),
            // LEVEL - colored by severity
            Span::styled(level.to_string(), get_level_style(level)),
            // - message
            Span::styled(" - ".to_string(), Style::default().fg(BRIGHT_BLACK)),
            Span::styled(message.to_string(), Style::default().fg(FOREGROUND)),
        ]);
        
        Line::from(spans)
    } else {
        // Fallback: unformatted line (plain text)
        Line::raw(line.to_string())
    }
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
        assert_eq!(
            result[0].1,
            r"*** Found local files:\n***   * /opt/airflow/logs/"
        );
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
}
