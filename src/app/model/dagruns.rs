use crossterm::event::KeyCode;
use log::debug;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Clear, Paragraph, Row, Scrollbar, ScrollbarOrientation,
    ScrollbarState, StatefulWidget, Table, Widget, Wrap,
};
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::SyntaxSet;
use syntect::util::LinesWithEndings;
use syntect_tui::into_span;
use time::format_description;

use crate::airflow::model::common::DagRun;
use crate::app::events::custom::FlowrsEvent;
use crate::ui::common::create_headers;
use crate::ui::constants::{AirflowStateColor, ALTERNATING_ROW_COLOR, DEFAULT_STYLE, MARKED_COLOR};
use crate::ui::TIME_FORMAT;

use super::popup::commands_help::CommandPopUp;
use super::popup::dagruns::commands::DAGRUN_COMMAND_POP_UP;
use super::popup::dagruns::trigger::TriggerDagRunPopUp;
use super::popup::dagruns::DagRunPopUp;
use super::popup::error::ErrorPopup;
use super::popup::popup_area;
use super::popup::{dagruns::clear::ClearDagRunPopup, dagruns::mark::MarkDagRunPopup};
use super::{filter::Filter, Model, StatefulTable, handle_table_scroll_keys, handle_vertical_scroll_keys};
use crate::app::worker::{OpenItem, WorkerMessage};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DagRunFocusedSection {
    InfoSection,
    DagRunsTable,
}

#[derive(Default)]
pub struct DagCodeWidget {
    pub cached_lines: Option<Vec<Line<'static>>>,
    pub vertical_scroll: usize,
    pub vertical_scroll_state: ScrollbarState,
}

impl DagCodeWidget {
    pub fn set_code(&mut self, code: &str) {
        self.cached_lines = Some(code_to_lines(code));
        self.vertical_scroll = 0;
        self.vertical_scroll_state = ScrollbarState::default();
    }

    pub fn clear(&mut self) {
        self.cached_lines = None;
        self.vertical_scroll = 0;
        self.vertical_scroll_state = ScrollbarState::default();
    }
}

#[derive(Default)]
pub struct DagInfoWidget {
    pub cached_lines: Option<Vec<Line<'static>>>,
    pub vertical_scroll: usize,
    pub vertical_scroll_state: ScrollbarState,
}

impl DagInfoWidget {
    pub fn set_info(&mut self, dag: &crate::airflow::model::common::Dag) {
        let new_lines = format_dag_info(dag);
        let content_length = new_lines.len();
        
        // Only reset scroll position if this is the first time loading
        // (i.e., cached_lines is None). Otherwise, preserve the current scroll position.
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

pub struct DagRunModel {
    pub dag_id: Option<String>,
    pub dag_details: Option<crate::airflow::model::common::Dag>,
    pub dag_info: DagInfoWidget,
    pub dag_code: DagCodeWidget,
    pub focused_section: DagRunFocusedSection,
    pub all: Vec<DagRun>,
    pub filtered: StatefulTable<DagRun>,
    pub filter: Filter,
    pub marked: Vec<usize>,
    pub popup: Option<DagRunPopUp>,
    pub commands: Option<&'static CommandPopUp<'static>>,
    pub error_popup: Option<ErrorPopup>,
    pub current_page: usize,
    pub page_size: usize,
    pub total_entries: i64,  // Total DAG runs available from API
    ticks: u32,
    event_buffer: Vec<FlowrsEvent>,
}

impl DagRunModel {
    pub fn new() -> Self {
        DagRunModel {
            dag_id: None,
            dag_details: None,
            dag_info: DagInfoWidget::default(),
            dag_code: DagCodeWidget::default(),
            focused_section: DagRunFocusedSection::DagRunsTable,
            all: vec![],
            filtered: StatefulTable::new(vec![]),
            filter: Filter::new(),
            marked: vec![],
            popup: None,
            commands: None,
            error_popup: None,
            current_page: 0,
            page_size: 20,
            total_entries: 0,
            ticks: 0,
            event_buffer: vec![],
        }
    }

    pub fn filter_dag_runs(&mut self) {
        self.filter_dag_runs_with_reset(false);
    }

    pub fn filter_dag_runs_with_reset(&mut self, reset_page: bool) {
        let prefix = &self.filter.prefix;
        let mut filtered_dag_runs = match prefix {
            Some(prefix) => self
                .all
                .iter()
                .filter(|dagrun| dagrun.dag_run_id.contains(prefix))
                .cloned()
                .collect::<Vec<DagRun>>(),
            None => self.all.clone(),
        };
        // Sort by start_date in descending order (most recent first)
        filtered_dag_runs.sort_by(|a, b| b.start_date.cmp(&a.start_date));
        self.filtered.items = filtered_dag_runs;
        
        if reset_page {
            // Reset to first page when filter changes
            self.current_page = 0;
        } else {
            // Ensure current page is still valid after data refresh
            let total_pages = self.total_pages();
            if self.current_page >= total_pages && total_pages > 0 {
                self.current_page = total_pages - 1;
            }
        }
    }

    pub fn current(&self) -> Option<&DagRun> {
        self.filtered
            .state
            .selected()
            .and_then(|i| {
                let page_offset = self.current_page * self.page_size;
                self.filtered.items.get(page_offset + i)
            })
    }

    pub fn mark_dag_run(&mut self, dag_run_id: &str, status: &str) {
        self.filtered.items.iter_mut().for_each(|dag_run| {
            if dag_run.dag_run_id == dag_run_id {
                dag_run.state = status.to_string();
            }
        });
    }

    pub fn init_info_scroll(&mut self) {
        if let Some(dag) = &self.dag_details {
            self.dag_info.set_info(dag);
        }
    }

    /// Get the total number of pages based on total entries from API
    pub fn total_pages(&self) -> usize {
        if self.total_entries == 0 {
            1
        } else {
            ((self.total_entries as usize) + self.page_size - 1) / self.page_size
        }
    }

    /// Get the paginated slice of filtered items for the current page
    pub fn paginated_items(&self) -> &[DagRun] {
        let start = self.current_page * self.page_size;
        let end = (start + self.page_size).min(self.filtered.items.len());
        &self.filtered.items[start..end]
    }

    /// Navigate to the next page
    pub fn next_page(&mut self) {
        if self.current_page + 1 < self.total_pages() {
            self.current_page += 1;
            // Reset selection to first item on new page
            self.filtered.state.select(Some(0));
        }
    }

    /// Navigate to the previous page
    pub fn previous_page(&mut self) {
        if self.current_page > 0 {
            self.current_page -= 1;
            // Reset selection to first item on new page
            self.filtered.state.select(Some(0));
        }
    }

    /// Get the range of items being displayed (e.g., "1-20")
    pub fn current_range(&self) -> (usize, usize) {
        if self.filtered.items.is_empty() {
            (0, 0)
        } else {
            let start = self.current_page * self.page_size + 1;
            let end = ((self.current_page + 1) * self.page_size).min(self.filtered.items.len());
            (start, end)
        }
    }
}

impl Default for DagRunModel {
    fn default() -> Self {
        Self::new()
    }
}

impl Model for DagRunModel {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>) {
        match event {
            FlowrsEvent::Tick => {
                self.ticks += 1;
                // No automatic refresh - use 'r' key to refresh manually
                return (Some(FlowrsEvent::Tick), vec![]);
            }
            FlowrsEvent::Key(key_event) => {
                if self.filter.is_enabled() {
                    self.filter.update(key_event);
                    self.filter_dag_runs_with_reset(true);
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
                            return (None, vec![]);
                        }
                        _ => (),
                    }
                } else if let Some(popup) = &mut self.popup {
                    // TODO: refactor this, should be all the same
                    match popup {
                        DagRunPopUp::Clear(popup) => {
                            let (key_event, messages) = popup.update(event);
                            debug!("Popup messages: {messages:?}");
                            if let Some(FlowrsEvent::Key(key_event)) = &key_event {
                                match key_event.code {
                                    KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q') => {
                                        self.popup = None;
                                    }
                                    _ => {}
                                }
                            }
                            return (None, messages);
                        }
                        DagRunPopUp::Mark(popup) => {
                            let (key_event, messages) = popup.update(event);
                            debug!("Popup messages: {messages:?}");
                            if let Some(FlowrsEvent::Key(key_event)) = &key_event {
                                match key_event.code {
                                    KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q') => {
                                        self.popup = None;
                                        self.marked = vec![];
                                    }
                                    _ => {}
                                }
                            }
                            return (None, messages);
                        }
                        DagRunPopUp::Trigger(popup) => {
                            let (key_event, messages) = popup.update(event);
                            debug!("Popup messages: {messages:?}");
                            if let Some(FlowrsEvent::Key(key_event)) = &key_event {
                                match key_event.code {
                                    KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q') => {
                                        self.popup = None;
                                    }
                                    _ => {}
                                }
                            }
                            return (None, messages);
                        }
                    }
                } else if self.dag_code.cached_lines.is_some() {
                    // Handle scrolling in code view
                    let max_lines = self.dag_code.cached_lines.as_ref().map(|lines| lines.len());
                    if handle_vertical_scroll_keys(
                        &mut self.dag_code.vertical_scroll,
                        &mut self.dag_code.vertical_scroll_state,
                        key_event,
                        max_lines,
                    ) {
                        return (None, vec![]);
                    }
                    
                    match key_event.code {
                        KeyCode::Esc | KeyCode::Char('q' | 'v') | KeyCode::Enter => {
                            self.dag_code.clear();
                            return (None, vec![]);
                        }
                        _ => {}
                    }
                } else {
                    // Handle scrolling based on focused section
                    let handled = match self.focused_section {
                        DagRunFocusedSection::InfoSection => {
                            let max_lines = self.dag_info.cached_lines.as_ref().map(|lines| lines.len());
                            handle_vertical_scroll_keys(
                                &mut self.dag_info.vertical_scroll,
                                &mut self.dag_info.vertical_scroll_state,
                                key_event,
                                max_lines,
                            )
                        }
                        DagRunFocusedSection::DagRunsTable => {
                            handle_table_scroll_keys(&mut self.filtered, key_event)
                        }
                    };
                    
                    if handled {
                        return (None, vec![]);
                    }
                    
                    match key_event.code {
                        KeyCode::Char('K') => {
                            // Switch focus to Info section (up)
                            if self.dag_info.cached_lines.is_some() {
                                self.focused_section = DagRunFocusedSection::InfoSection;
                            }
                        }
                        KeyCode::Char('J') => {
                            // Switch focus to DAGRuns table (down)
                            self.focused_section = DagRunFocusedSection::DagRunsTable;
                        }
                        KeyCode::Char('G') => {
                            // Jump to bottom of focused section
                            match self.focused_section {
                                DagRunFocusedSection::InfoSection => {
                                    if let Some(lines) = &self.dag_info.cached_lines {
                                        self.dag_info.vertical_scroll = lines.len().saturating_sub(1);
                                        self.dag_info.vertical_scroll_state = 
                                            self.dag_info.vertical_scroll_state.position(self.dag_info.vertical_scroll);
                                    }
                                }
                                DagRunFocusedSection::DagRunsTable => {
                                    self.filtered.state.select_last();
                                }
                            }
                        }
                        KeyCode::Char('g') => {
                            if let Some(FlowrsEvent::Key(key_event)) = self.event_buffer.pop() {
                                if key_event.code == KeyCode::Char('g') {
                                    // Jump to top of focused section
                                    match self.focused_section {
                                        DagRunFocusedSection::InfoSection => {
                                            self.dag_info.vertical_scroll = 0;
                                            self.dag_info.vertical_scroll_state = 
                                                self.dag_info.vertical_scroll_state.position(0);
                                        }
                                        DagRunFocusedSection::DagRunsTable => {
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
                        KeyCode::Char('[') => {
                            if let Some(FlowrsEvent::Key(key_event)) = self.event_buffer.pop() {
                                if key_event.code == KeyCode::Char('[') {
                                    // Previous page
                                    self.previous_page();
                                } else {
                                    self.event_buffer.push(FlowrsEvent::Key(key_event));
                                }
                            } else {
                                self.event_buffer.push(FlowrsEvent::Key(*key_event));
                            }
                        }
                        KeyCode::Char(']') => {
                            if let Some(FlowrsEvent::Key(key_event)) = self.event_buffer.pop() {
                                if key_event.code == KeyCode::Char(']') {
                                    // Check if we need to fetch more data (look ahead to next page)
                                    let next_page_start = (self.current_page + 1) * self.page_size;
                                    let next_page_end = next_page_start + self.page_size;
                                    let needs_fetch = next_page_end > self.filtered.items.len() 
                                        && (self.filtered.items.len() as i64) < self.total_entries;
                                    
                                    // Always page forward if possible
                                    self.next_page();
                                    
                                    // Fetch more data in background for future pages
                                    if needs_fetch {
                                        if let Some(dag_id) = &self.dag_id {
                                            let offset = self.all.len() as i64;
                                            let limit = 40; // Fetch 2 pages ahead for smoother UX
                                            return (
                                                None,
                                                vec![WorkerMessage::FetchMoreDagRuns {
                                                    dag_id: dag_id.clone(),
                                                    offset,
                                                    limit,
                                                }],
                                            );
                                        }
                                    }
                                } else {
                                    self.event_buffer.push(FlowrsEvent::Key(key_event));
                                }
                            } else {
                                self.event_buffer.push(FlowrsEvent::Key(*key_event));
                            }
                        }
                        KeyCode::Char('t') => {
                            self.popup = Some(DagRunPopUp::Trigger(TriggerDagRunPopUp::new(
                                self.dag_id.clone().unwrap(),
                            )));
                        }
                        KeyCode::Char('m') => {
                            if let Some(index) = self.filtered.state.selected() {
                                let actual_idx = self.current_page * self.page_size + index;
                                self.marked.push(actual_idx);

                                self.popup = Some(DagRunPopUp::Mark(MarkDagRunPopup::new(
                                    self.marked
                                        .iter()
                                        .map(|i| self.filtered.items[*i].dag_run_id.clone())
                                        .collect(),
                                    self.current().unwrap().dag_id.clone(),
                                )));
                            }
                        }
                        KeyCode::Char('M') => {
                            if let Some(index) = self.filtered.state.selected() {
                                let actual_idx = self.current_page * self.page_size + index;
                                if self.marked.contains(&actual_idx) {
                                    self.marked.retain(|&i| i != actual_idx);
                                } else {
                                    self.marked.push(actual_idx);
                                }
                            }
                        }
                        KeyCode::Char('?') => {
                            self.commands = Some(&*DAGRUN_COMMAND_POP_UP);
                        }
                        KeyCode::Char('/') => {
                            self.filter.toggle();
                            self.filter_dag_runs_with_reset(true);
                        }
                        KeyCode::Char('v') => {
                            if let Some(dag_id) = &self.dag_id {
                                return (
                                    None,
                                    vec![WorkerMessage::GetDagCode {
                                        dag_id: dag_id.clone(),
                                    }],
                                );
                            }
                        }
                        KeyCode::Char('c') => {
                            if let (Some(dag_run), Some(dag_id)) = (self.current(), &self.dag_id) {
                                self.popup = Some(DagRunPopUp::Clear(ClearDagRunPopup::new(
                                    dag_run.dag_run_id.clone(),
                                    dag_id.clone(),
                                )));
                            }
                        }
                        KeyCode::Enter => {
                            if let (Some(dag_id), Some(dag_run)) = (&self.dag_id, &self.current()) {
                                return (
                                    Some(FlowrsEvent::Key(*key_event)),
                                    vec![WorkerMessage::UpdateTaskInstances {
                                        dag_id: dag_id.clone(),
                                        dag_run_id: dag_run.dag_run_id.clone(),
                                        clear: true,
                                    }],
                                );
                            }
                        }
                        KeyCode::Char('o') => {
                            if let (Some(dag_id), Some(dag_run)) = (&self.dag_id, &self.current()) {
                                return (
                                    Some(FlowrsEvent::Key(*key_event)),
                                    vec![WorkerMessage::OpenItem(OpenItem::DagRun {
                                        dag_id: dag_id.clone(),
                                        dag_run_id: dag_run.dag_run_id.clone(),
                                    })],
                                );
                            }
                        }
                        KeyCode::Char('r') => {
                            // Manual refresh - reload dag runs and details
                            if let Some(dag_id) = &self.dag_id {
                                return (
                                    None,
                                    vec![
                                        WorkerMessage::UpdateDagRuns {
                                            dag_id: dag_id.clone(),
                                            clear: true,
                                        },
                                        WorkerMessage::GetDagDetails {
                                            dag_id: dag_id.clone(),
                                        },
                                    ],
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
            FlowrsEvent::Mouse => {}
        }
        (Some(event.clone()), vec![])
    }
}

impl Widget for &mut DagRunModel {
    fn render(self, area: Rect, buf: &mut ratatui::prelude::Buffer) {
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

        // Split into Info section and DAGRuns table if info is available
        let (info_area, dagruns_area) = if self.dag_info.cached_lines.is_some() {
            let rects = Layout::default()
                .constraints([Constraint::Length(10), Constraint::Min(0)].as_ref())
                .split(base_area);
            (Some(rects[0]), rects[1])
        } else {
            (None, base_area)
        };

        // Render Info section if available
        if let (Some(info_area), Some(cached_lines)) = (info_area, &self.dag_info.cached_lines) {
            let border_style = if self.focused_section == DagRunFocusedSection::InfoSection {
                DEFAULT_STYLE.fg(Color::Cyan) // Highlight when focused
            } else {
                DEFAULT_STYLE
            };

            let info_block = Block::default()
                .border_type(BorderType::Rounded)
                .borders(Borders::ALL)
                .title("Info")
                .border_style(border_style)
                .style(DEFAULT_STYLE)
                .title_style(DEFAULT_STYLE.add_modifier(Modifier::BOLD));

            let info_text = Paragraph::new(cached_lines.clone())
                .block(info_block)
                .style(DEFAULT_STYLE)
                .wrap(Wrap { trim: false })
                .scroll((self.dag_info.vertical_scroll as u16, 0));

            info_text.render(info_area, buf);

            // Render scrollbar for info section
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(Some("↑"))
                .end_symbol(Some("↓"));
            scrollbar.render(info_area, buf, &mut self.dag_info.vertical_scroll_state);
        }

        // Render DAGRuns table
        let dagruns_border_style = if self.focused_section == DagRunFocusedSection::DagRunsTable {
            DEFAULT_STYLE.fg(Color::Cyan) // Highlight when focused
        } else {
            DEFAULT_STYLE
        };

        let headers = ["State", "DAG Run ID", "Logical Date", "Duration"];
        let header_row = create_headers(headers);
        let header =
            Row::new(header_row).style(crate::ui::constants::HEADER_STYLE);

        let page_offset = self.current_page * self.page_size;
        let page_end = (page_offset + self.page_size).min(self.filtered.items.len());
        let rows = self.filtered.items[page_offset..page_end].iter().enumerate().map(|(idx, item)| {
            let actual_idx = page_offset + idx;
            Row::new(vec![
                Line::from(match item.state.as_str() {
                    "success" => {
                        Span::styled("■", Style::default().fg(AirflowStateColor::Success.into()))
                    }
                    "running" => {
                        Span::styled("■", DEFAULT_STYLE.fg(AirflowStateColor::Running.into()))
                    }
                    "failed" => {
                        Span::styled("■", DEFAULT_STYLE.fg(AirflowStateColor::Failed.into()))
                    }
                    "queued" => {
                        Span::styled("■", DEFAULT_STYLE.fg(AirflowStateColor::Queued.into()))
                    }
                    _ => Span::styled("■", DEFAULT_STYLE.fg(AirflowStateColor::None.into())),
                }),
                Line::from(Span::styled(
                    item.dag_run_id.as_str(),
                    Style::default().add_modifier(Modifier::BOLD),
                )),
                Line::from(if let Some(date) = item.logical_date {
                    date.format(&format_description::parse(TIME_FORMAT).unwrap())
                        .unwrap()
                        .clone()
                } else {
                    "None".to_string()
                }),
                Line::from(format_duration(item.start_date, item.end_date)),
            ])
            .style(if self.marked.contains(&actual_idx) {
                DEFAULT_STYLE.bg(MARKED_COLOR)
            } else if (idx % 2) == 0 {
                DEFAULT_STYLE
            } else {
                DEFAULT_STYLE.bg(ALTERNATING_ROW_COLOR)
            })
        });
        let t = Table::new(
            rows,
            &[
                Constraint::Length(8),
                Constraint::Length(40),
                Constraint::Length(22),
                Constraint::Fill(1),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .border_type(BorderType::Rounded)
                .borders(Borders::ALL)
                .title({
                    let (start, end) = self.current_range();
                    let total = self.total_entries;
                    format!("DAGRuns (showing {}-{} of {})", start, end, total)
                })
                .border_style(dagruns_border_style)
                .style(DEFAULT_STYLE),
        )
        .row_highlight_style(crate::ui::constants::SELECTED_STYLE);
        StatefulWidget::render(t, dagruns_area, buf, &mut self.filtered.state);

        if let Some(cached_lines) = &self.dag_code.cached_lines {
            let area = popup_area(area, 60, 90);

            let popup = Block::default()
                .border_type(BorderType::Rounded)
                .borders(Borders::ALL)
                .title("DAG Code")
                .border_style(DEFAULT_STYLE)
                .style(DEFAULT_STYLE)
                .title_style(DEFAULT_STYLE.add_modifier(Modifier::BOLD));

            #[allow(clippy::cast_possible_truncation)]
            let code_text = Paragraph::new(cached_lines.clone())
                .block(popup)
                .style(DEFAULT_STYLE)
                .wrap(Wrap { trim: true })
                .scroll((self.dag_code.vertical_scroll as u16, 0));

            Clear.render(area, buf); //this clears out the background
            code_text.render(area, buf);

            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(Some("↑"))
                .end_symbol(Some("↓"));

            scrollbar.render(area, buf, &mut self.dag_code.vertical_scroll_state);
        }

        match &mut self.popup {
            Some(DagRunPopUp::Clear(popup)) => {
                popup.render(area, buf);
            }
            Some(DagRunPopUp::Mark(popup)) => {
                popup.render(area, buf);
            }
            Some(DagRunPopUp::Trigger(popup)) => {
                popup.render(area, buf);
            }
            _ => (),
        }

        if let Some(commands) = &self.commands {
            commands.render(area, buf);
        }

        if let Some(error_popup) = &self.error_popup {
            error_popup.render(area, buf);
        }
    }
}

fn format_dag_info(dag: &crate::airflow::model::common::Dag) -> Vec<Line<'static>> {
    let mut lines = vec![];
    
    // DAG Name
    let display_name = dag.dag_display_name.as_ref().unwrap_or(&dag.dag_id);
    lines.push(Line::from(vec![
        Span::styled("DAG: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(display_name.to_string()),
    ]));
    
    // Owners
    if !dag.owners.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("Owners: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(dag.owners.join(", ")),
        ]));
    }
    
    // Schedule
    if let Some(schedule) = &dag.timetable_description {
        lines.push(Line::from(vec![
            Span::styled("Schedule: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(schedule.to_string()),
        ]));
    }
    
    // Empty line separator
    lines.push(Line::from(""));
    
    // Documentation
    lines.push(Line::from(Span::styled(
        "Documentation:",
        Style::default().add_modifier(Modifier::BOLD).add_modifier(Modifier::UNDERLINED),
    )));
    
    if let Some(doc_md) = &dag.doc_md {
        if doc_md.trim().is_empty() {
            lines.push(Line::from(Span::styled(
                "No documentation available",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            // Split doc_md into lines
            for line in doc_md.lines() {
                lines.push(Line::from(line.to_string()));
            }
        }
    } else {
        lines.push(Line::from(Span::styled(
            "No documentation available",
            Style::default().fg(Color::DarkGray),
        )));
    }
    
    lines
}

fn code_to_lines(dag_code: &str) -> Vec<Line<'static>> {
    let ps = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();

    let syntax = ps.find_syntax_by_extension("py").unwrap();
    let mut h = HighlightLines::new(syntax, &ts.themes["base16-ocean.dark"]);

    let mut lines: Vec<Line<'static>> = vec![];
    for line in LinesWithEndings::from(dag_code) {
        let line_spans: Vec<Span<'static>> = h
            .highlight_line(line, &ps)
            .unwrap()
            .into_iter()
            .filter_map(|segment| into_span(segment).ok())
            .map(|span: Span| {
                // Convert borrowed span to owned span
                Span::styled(span.content.to_string(), span.style)
            })
            .collect();
        lines.push(Line::from(line_spans));
    }
    lines
}

/// Format duration between start and end dates
fn format_duration(start_date: Option<time::OffsetDateTime>, end_date: Option<time::OffsetDateTime>) -> String {
    match (start_date, end_date) {
        (Some(start), Some(end)) => {
            let duration = end - start;
            let total_seconds = duration.whole_seconds();
            
            if total_seconds < 0 {
                return "Running".to_string();
            }
            
            let hours = total_seconds / 3600;
            let minutes = (total_seconds % 3600) / 60;
            let seconds = total_seconds % 60;
            
            if hours > 0 {
                format!("{}h {}m {}s", hours, minutes, seconds)
            } else if minutes > 0 {
                format!("{}m {}s", minutes, seconds)
            } else {
                format!("{}s", seconds)
            }
        }
        (Some(_), None) => "Running".to_string(),
        _ => "-".to_string(),
    }
}
