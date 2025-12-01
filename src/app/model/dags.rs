use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crossterm::event::KeyCode;
use log::debug;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, StatefulWidget, Table, Widget, Wrap};
use time::OffsetDateTime;

use crate::airflow::model::common::{Dag, DagRun, DagStatistic, ImportError};
use crate::app::events::custom::FlowrsEvent;
use crate::app::model::popup::dags::commands::DAG_COMMAND_POP_UP;
use crate::ui::common::create_headers;
use crate::ui::constants::{ALTERNATING_ROW_COLOR, DEFAULT_STYLE};

use super::popup::commands_help::CommandPopUp;
use super::popup::error::ErrorPopup;
use super::{filter::Filter, Model, StatefulTable};
use crate::app::worker::{OpenItem, WorkerMessage};

// Constants for DAG health monitoring and UI layout
/// Number of recent runs to analyze for DAG health indicators
pub const RECENT_RUNS_HEALTH_WINDOW: usize = 7;
/// Width of the state column (for colored square indicator)
const STATE_COLUMN_WIDTH: u16 = 5;
/// Height of the import errors panel
const IMPORT_ERRORS_PANEL_HEIGHT: u16 = 15;

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
    commands: Option<&'static CommandPopUp<'static>>,
    pub error_popup: Option<ErrorPopup>,
    ticks: u32,
    event_buffer: Vec<FlowrsEvent>,
}

impl DagModel {
    pub fn new() -> Self {
        DagModel {
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
        
        // Step 3: Sort alphabetically by name
        filtered_dags.sort_by(|a, b| {
            a.dag_id.cmp(&b.dag_id)
        });
        
        self.filtered.items = filtered_dags;
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

        if latest_run.state == "success" {
            if Self::has_recent_failures(&runs[1..]) {
                crate::ui::constants::YELLOW  // Warning - recovered from failure
            } else {
                crate::ui::constants::GREEN  // All good
            }
        } else {
            Color::Reset  // Latest still running or other state
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

/// Map a tag name to a consistent color using hash
fn tag_to_color(tag_name: &str) -> Color {
    // Available colors for tags (avoiding red/green/yellow which are used for states)
    const TAG_COLORS: &[Color] = &[
        crate::ui::constants::BLUE,
        crate::ui::constants::MAGENTA,
        crate::ui::constants::CYAN,
        crate::ui::constants::BRIGHT_BLUE,
        crate::ui::constants::BRIGHT_MAGENTA,
        crate::ui::constants::BRIGHT_CYAN,
    ];
    
    let mut hasher = DefaultHasher::new();
    tag_name.hash(&mut hasher);
    let hash = hasher.finish();
    
    TAG_COLORS[(hash as usize) % TAG_COLORS.len()]
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
                if !self.ticks.is_multiple_of(10) {
                    return (Some(FlowrsEvent::Tick), vec![]);
                }
                
                // UpdateImportErrors now fetches both count and full error list
                (
                    Some(FlowrsEvent::Tick),
                    vec![
                        WorkerMessage::UpdateDags {
                            only_active: !self.show_paused,
                        },
                        WorkerMessage::UpdateDagStats { clear: true },
                        WorkerMessage::UpdateImportErrors,
                        WorkerMessage::UpdateRecentDagRuns,
                    ],
                )
            }
            FlowrsEvent::Key(key_event) => {
                // Handle Escape key with multi-stage behavior
                if key_event.code == KeyCode::Esc {
                    if self.filter.is_enabled() {
                        // Filter dialogue is open: close it and clear any filter
                        self.filter.reset();  // Closes dialogue and clears prefix
                        self.filter_dags();
                        return (None, vec![]);
                    } else if self.filter.prefix.is_some() {
                        // Filter dialogue closed but filter is applied: clear the filter
                        self.filter.prefix = None;
                        self.filter_dags();
                        return (None, vec![]);
                    }
                    // else: no filter active, fall through to go back to environment page
                }
                
                if self.filter.is_enabled() {
                    self.filter.update(key_event);
                    self.filter_dags();
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
                    match key_event.code {
                        KeyCode::Down | KeyCode::Char('j') => {
                            // Handle scrolling based on focused section
                            match self.focused_section {
                                DagFocusedSection::ImportErrors => {
                                    if let Some(cached_lines) = &self.import_errors.cached_lines {
                                        if self.import_errors.vertical_scroll < cached_lines.len().saturating_sub(1) {
                                            self.import_errors.vertical_scroll += 1;
                                            self.import_errors.vertical_scroll_state = self.import_errors.vertical_scroll_state.position(self.import_errors.vertical_scroll);
                                        }
                                    }
                                }
                                DagFocusedSection::DagTable => {
                                    self.filtered.next();
                                }
                            }
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            // Handle scrolling based on focused section
                            match self.focused_section {
                                DagFocusedSection::ImportErrors => {
                                    if self.import_errors.vertical_scroll > 0 {
                                        self.import_errors.vertical_scroll -= 1;
                                        self.import_errors.vertical_scroll_state = self.import_errors.vertical_scroll_state.position(self.import_errors.vertical_scroll);
                                    }
                                }
                                DagFocusedSection::DagTable => {
                                    self.filtered.previous();
                                }
                            }
                        }
                        KeyCode::Char('J') => {
                            // Switch focus to DAG table (down)
                            self.focused_section = DagFocusedSection::DagTable;
                        }
                        KeyCode::Char('K') => {
                            // Switch focus to ImportErrors panel (up)
                            if self.import_errors.cached_lines.is_some() {
                                self.focused_section = DagFocusedSection::ImportErrors;
                            }
                        }
                        KeyCode::Char('G') => {
                            // Jump to bottom of focused section
                            match self.focused_section {
                                DagFocusedSection::ImportErrors => {
                                    if let Some(cached_lines) = &self.import_errors.cached_lines {
                                        self.import_errors.vertical_scroll = cached_lines.len().saturating_sub(1);
                                        self.import_errors.vertical_scroll_state = 
                                            self.import_errors.vertical_scroll_state.position(self.import_errors.vertical_scroll);
                                    }
                                }
                                DagFocusedSection::DagTable => {
                                    self.filtered.state.select_last();
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
                            self.filter_dags();
                        }
                        KeyCode::Char('?') => {
                            self.commands = Some(&*DAG_COMMAND_POP_UP);
                        }
                        KeyCode::Enter => {
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
                        KeyCode::Char('g') => {
                            if let Some(FlowrsEvent::Key(key_event)) = self.event_buffer.pop() {
                                if key_event.code == KeyCode::Char('g') {
                                    // Jump to top of focused section
                                    match self.focused_section {
                                        DagFocusedSection::ImportErrors => {
                                            self.import_errors.vertical_scroll = 0;
                                            self.import_errors.vertical_scroll_state = 
                                                self.import_errors.vertical_scroll_state.position(0);
                                        }
                                        DagFocusedSection::DagTable => {
                                            self.filtered.state.select_first();
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

        // Split into ImportErrors section and DAG table if errors exist
        let (errors_area, dags_area) = if self.import_errors.cached_lines.is_some() {
            let rects = Layout::default()
                .constraints([Constraint::Length(IMPORT_ERRORS_PANEL_HEIGHT), Constraint::Min(0)].as_ref())
                .split(base_area);
            (Some(rects[0]), rects[1])
        } else {
            (None, base_area)
        };

        // Render ImportErrors section if available
        if let (Some(errors_area), Some(cached_lines)) = (errors_area, &self.import_errors.cached_lines) {
            let border_style = if self.focused_section == DagFocusedSection::ImportErrors {
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

        let selected_style = crate::ui::constants::SELECTED_STYLE;
        let dags_border_style = if self.focused_section == DagFocusedSection::DagTable {
            DEFAULT_STYLE.fg(Color::Cyan) // Highlight when focused
        } else {
            DEFAULT_STYLE
        };

        let headers = ["State", "Name", "Schedule", "Next Run", "Tags"];
        let header_row = create_headers(headers);
        let header = Row::new(header_row)
            .style(DEFAULT_STYLE.reversed())
            .add_modifier(Modifier::BOLD);
        let search_term = self.filter.prefix.as_deref();
        let rows =
            self.filtered.items.iter().enumerate().map(|(idx, item)| {
                // Determine DAG color based on recent runs
                let color = self.get_dag_color(item);
                
                Row::new(vec![
                    Line::from(Span::styled("■", Style::default().fg(color))),
                    Line::from(highlight_search_term(&item.dag_id, search_term, Color::Reset)),
                    {
                        let schedule = item.timetable_description.as_deref().unwrap_or("None");
                        // Shorten "Never, external triggers only" to just "Never"
                        let schedule_text = if schedule.starts_with("Never") {
                            "Never"
                        } else {
                            schedule
                        };
                        // Gray out "Never" and "None"
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
                            // Create colored spans for each tag with search highlighting
                            let mut spans = Vec::new();
                            for (i, tag) in item.tags.iter().enumerate() {
                                if i > 0 {
                                    spans.push(Span::raw(", "));
                                }
                                let tag_color = tag_to_color(&tag.name);
                                // Apply highlighting while preserving tag color
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
                    // Gray out paused DAGs
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
                .title({
                    // Calculate showing/total counts
                    let showing_count = self.filtered.items.len();
                    let total_count = self.all.iter().filter(|d| d.is_active.unwrap_or(false)).count();
                    
                    format!(
                        "DAGs (showing {} of {}) - Press <?> for commands",
                        showing_count, total_count
                    )
                })
                .border_style(dags_border_style)
                .style(DEFAULT_STYLE),
        )
        .row_highlight_style(selected_style);

        StatefulWidget::render(t, dags_area, buf, &mut self.filtered.state);

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
