use std::collections::HashMap;
use std::vec;

use super::popup::commands_help::CommandPopUp;
use super::popup::error::ErrorPopup;
use super::popup::taskinstances::commands::create_task_command_popup;
use crossterm::event::{KeyCode, KeyModifiers};
use log::debug;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Color;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Row, StatefulWidget, Table, Widget};

use crate::airflow::graph_layout::GraphPrefix;
use crate::airflow::model::common::TaskInstance;
use crate::app::events::custom::FlowrsEvent;
use crate::ui::common::format_duration_seconds;
use crate::ui::constants::{AirflowStateColor, ALTERNATING_ROW_COLOR, CYAN, DEFAULT_STYLE, HEADER_STYLE, MARKED_COLOR, RED};

use super::popup::taskinstances::clear::ClearTaskInstancePopup;
use super::popup::taskinstances::mark::MarkTaskInstancePopup;
use super::popup::taskinstances::TaskInstancePopUp;
use super::sortable_table::{CustomSort, SortableTable};
use super::{filter::Filter, handle_command_popup_events, Model, HALF_PAGE_SIZE};
use crate::app::worker::{OpenItem, WorkerMessage};
use std::cmp::Ordering;

// Implement CustomSort for TaskInstance
impl CustomSort for TaskInstance {
    fn column_value(&self, column_index: usize) -> String {
        match column_index {
            0 => self.map_index.to_string(), // Graph (map_index)
            1 => self.task_id.clone(), // Task ID
            2 => {
                // Duration
                match (&self.start_date, &self.end_date) {
                    (Some(start), Some(end)) => {
                        let duration = *end - *start;
                        duration.whole_seconds().to_string()
                    }
                    _ => String::new(),
                }
            }
            3 => self.state.as_ref().map(|s| s.clone()).unwrap_or_default(), // State
            4 => self.try_number.to_string(), // Tries
            _ => String::new(),
        }
    }
    
    fn comparator(column_index: usize) -> Option<fn(&Self, &Self) -> Ordering> {
        match column_index {
            0 => Some(|a: &TaskInstance, b: &TaskInstance| {
                // Sort by map_index numerically
                a.map_index.cmp(&b.map_index)
            }),
            2 => Some(|a: &TaskInstance, b: &TaskInstance| {
                // Sort by duration numerically
                let duration_a = match (&a.start_date, &a.end_date) {
                    (Some(start), Some(end)) => Some((*end - *start).whole_seconds()),
                    _ => None,
                };
                let duration_b = match (&b.start_date, &b.end_date) {
                    (Some(start), Some(end)) => Some((*end - *start).whole_seconds()),
                    _ => None,
                };
                match (duration_a, duration_b) {
                    (Some(da), Some(db)) => da.cmp(&db),
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (None, None) => Ordering::Equal,
                }
            }),
            3 => Some(|a: &TaskInstance, b: &TaskInstance| {
                // Sort states meaningfully
                let state_priority = |state: Option<&String>| {
                    match state.map(|s| s.as_str()) {
                        Some("failed") | Some("Failed") => 0,
                        Some("running") | Some("Running") => 1,
                        Some("success") | Some("Success") => 2,
                        Some("queued") | Some("Queued") => 3,
                        Some("scheduled") | Some("Scheduled") => 4,
                        _ => 5,
                    }
                };
                state_priority(a.state.as_ref()).cmp(&state_priority(b.state.as_ref()))
            }),
            4 => Some(|a: &TaskInstance, b: &TaskInstance| {
                // Sort tries numerically
                a.try_number.cmp(&b.try_number)
            }),
            _ => None,
        }
    }
}

pub struct TaskInstanceModel {
    pub dag_id: Option<String>,
    pub dag_run_id: Option<String>,
    pub all: Vec<TaskInstance>,
    pub filtered: SortableTable<TaskInstance>,
    pub filter: Filter,
    pub popup: Option<TaskInstancePopUp>,
    pub marked: Vec<usize>,
    commands: Option<CommandPopUp<'static>>,
    pub error_popup: Option<ErrorPopup>,
    pub graph_layout: HashMap<String, GraphPrefix>,
    ticks: u32,
    event_buffer: Vec<FlowrsEvent>,
}

impl TaskInstanceModel {
    pub fn new() -> Self {
        let headers = ["Graph", "Task ID", "Duration", "State", "Tries"];
        // Reserved keys: j/k (scroll), g/G (jump), m (mark), c (clear), o (open), ? (help), / (filter)
        let reserved = &['j', 'k', 'g', 'G', 'm', 'c', 'o', '?', '/'];
        TaskInstanceModel {
            dag_id: None,
            dag_run_id: None,
            all: vec![],
            filtered: SortableTable::new(&headers, vec![], reserved),
            filter: Filter::new(),
            popup: None,
            marked: vec![],
            commands: None,
            error_popup: None,
            graph_layout: HashMap::new(),
            ticks: 0,
            event_buffer: vec![],
        }
    }

    pub fn filter_task_instances(&mut self) {
        let prefix = &self.filter.prefix;
        let filtered_task_instances = match prefix {
            Some(prefix) => &self
                .all
                .iter()
                .filter(|task_instance| task_instance.task_id.contains(prefix))
                .cloned()
                .collect::<Vec<TaskInstance>>(),
            None => &self.all,
        };
        self.filtered.items = filtered_task_instances.clone();
        // Reapply current sort if any
        self.filtered.reapply_sort();
    }

    #[allow(dead_code)]
    pub fn current(&mut self) -> Option<&mut TaskInstance> {
        self.filtered
            .state
            .selected()
            .map(|i| &mut self.filtered.items[i])
    }
    pub fn mark_task_instance(&mut self, task_id: &str, status: &str) {
        self.filtered.items.iter_mut().for_each(|task_instance| {
            if task_instance.task_id == task_id {
                task_instance.state = Some(status.to_string());
            }
        });
    }
}

impl Default for TaskInstanceModel {
    fn default() -> Self {
        Self::new()
    }
}

impl Model for TaskInstanceModel {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>) {
        match event {
            FlowrsEvent::Tick => {
                self.ticks += 1;
                // No automatic refresh - use 'r' key to refresh manually
                (Some(FlowrsEvent::Tick), vec![])
            }
            FlowrsEvent::Key(key_event) => {
                if self.filter.is_enabled() {
                    self.filter.update(key_event);
                    self.filter_task_instances();
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
                } else if let Some(popup) = &mut self.popup {
                    match popup {
                        TaskInstancePopUp::Clear(popup) => {
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
                        TaskInstancePopUp::Mark(popup) => {
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
                    }
                } else {
                    // Handle Ctrl+D and Ctrl+U for half-page scrolling
                    if key_event.modifiers == KeyModifiers::CONTROL {
                        match key_event.code {
                            KeyCode::Char('d') => {
                                self.filtered.scroll_by(HALF_PAGE_SIZE as isize);
                                return (None, vec![]);
                            }
                            KeyCode::Char('u') => {
                                self.filtered.scroll_by(-(HALF_PAGE_SIZE as isize));
                                return (None, vec![]);
                            }
                            _ => {}
                        }
                    }
                    
                    // Handle scrolling
                    match key_event.code {
                        KeyCode::Down | KeyCode::Char('j') => {
                            self.filtered.scroll_by(1);
                            return (None, vec![]);
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            self.filtered.scroll_by(-1);
                            return (None, vec![]);
                        }
                        KeyCode::Char(c) => {
                            // Try to handle as sort key (only if no modifiers pressed)
                            if key_event.modifiers == KeyModifiers::NONE && self.filtered.handle_key(c) {
                                // Re-filter to apply default sort if sort was cleared
                                self.filter_task_instances();
                                return (None, vec![]);
                            }
                        }
                        _ => {}
                    }
                    
                    match key_event.code {
                        KeyCode::Char('G') => {
                            self.filtered.state.select_last();
                        }
                        KeyCode::Char('g') => {
                            if let Some(FlowrsEvent::Key(key_event)) = self.event_buffer.pop() {
                                if key_event.code == KeyCode::Char('g') {
                                    self.filtered.state.select_first();
                                } else {
                                    self.event_buffer.push(FlowrsEvent::Key(key_event));
                                }
                            } else {
                                self.event_buffer.push(FlowrsEvent::Key(*key_event));
                            }
                        }
                        KeyCode::Char('m') => {
                            if let Some(index) = self.filtered.state.selected() {
                                self.marked.push(index);

                                let dag_id = self.current().unwrap().dag_id.clone();
                                let dag_run_id = self.current().unwrap().dag_run_id.clone();

                                self.popup =
                                    Some(TaskInstancePopUp::Mark(MarkTaskInstancePopup::new(
                                        self.marked
                                            .iter()
                                            .map(|i| self.filtered.items[*i].task_id.clone())
                                            .collect(),
                                        &dag_id,
                                        &dag_run_id,
                                    )));
                            }
                        }
                        KeyCode::Char('M') => {
                            if let Some(index) = self.filtered.state.selected() {
                                if self.marked.contains(&index) {
                                    self.marked.retain(|&i| i != index);
                                } else {
                                    self.marked.push(index);
                                }
                            }
                        }
                        KeyCode::Char('c') => {
                            if let Some(task_instance) = self.current() {
                                self.popup =
                                    Some(TaskInstancePopUp::Clear(ClearTaskInstancePopup::new(
                                        &task_instance.dag_run_id,
                                        &task_instance.dag_id,
                                        &task_instance.task_id,
                                    )));
                            }
                        }
                        KeyCode::Char('?') => {
                            self.commands = Some(create_task_command_popup());
                        }
                        KeyCode::Char('/') => {
                            self.filter.toggle();
                            self.filter_task_instances();
                        }
                        KeyCode::Enter => {
                            if let Some(task_instance) = self.current() {
                                return (
                                    Some(FlowrsEvent::Key(*key_event)),
                                    vec![WorkerMessage::UpdateTaskLogs {
                                        dag_id: task_instance.dag_id.clone(),
                                        dag_run_id: task_instance.dag_run_id.clone(),
                                        task_id: task_instance.task_id.clone(),
                                        #[allow(
                                            clippy::cast_sign_loss,
                                            clippy::cast_possible_truncation
                                        )]
                                        task_try: task_instance.try_number as u16,
                                        clear: true,
                                    }],
                                );
                            }
                        }
                        KeyCode::Char('o') => {
                            if let Some(task_instance) = self.current() {
                                return (
                                    Some(FlowrsEvent::Key(*key_event)),
                                    vec![WorkerMessage::OpenItem(OpenItem::TaskInstance {
                                        dag_id: task_instance.dag_id.clone(),
                                        dag_run_id: task_instance.dag_run_id.clone(),
                                        task_id: task_instance.task_id.clone(),
                                    })],
                                );
                            }
                        }
                        KeyCode::Char('r') => {
                            // Manual refresh - reload task instances and task order
                            if let (Some(dag_id), Some(dag_run_id)) = (&self.dag_id, &self.dag_run_id) {
                                return (
                                    None,
                                    vec![
                                        WorkerMessage::UpdateTaskInstances {
                                            dag_id: dag_id.clone(),
                                            dag_run_id: dag_run_id.clone(),
                                            clear: true,
                                        },
                                        WorkerMessage::FetchTaskOrder {
                                            dag_id: dag_id.clone(),
                                        },
                                    ],
                                );
                            }
                        }
                        _ => return (Some(FlowrsEvent::Key(*key_event)), vec![]), // if no match, return the event
                    }
                }
                (None, vec![])
            }
            FlowrsEvent::Mouse => (Some(event.clone()), vec![]),
        }
    }
}
impl Widget for &mut TaskInstanceModel {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let rects = if self.filter.is_enabled() {
            let rects = Layout::default()
                .constraints([Constraint::Fill(90), Constraint::Max(3)].as_ref())
                .margin(0)
                .split(area);

            self.filter.render(rects[1], buffer);
            rects
        } else {
            Layout::default()
                .constraints([Constraint::Percentage(100)].as_ref())
                .margin(0)
                .split(area)
        };

        let selected_style = crate::ui::constants::SELECTED_STYLE;

        // Automatically generated sortable headers
        let header_row = self.filtered.render_headers(HEADER_STYLE, RED);
        let header = Row::new(header_row).style(HEADER_STYLE);

        let rows = self.filtered.items.iter().enumerate().map(|(idx, item)| {
            // Determine state and color
            let (state_text, state_color) = if let Some(state) = &item.state {
                let color = match state.as_str() {
                    "success" => AirflowStateColor::Success,
                    "running" => AirflowStateColor::Running,
                    "failed" => AirflowStateColor::Failed,
                    "queued" => AirflowStateColor::Queued,
                    "up_for_retry" => AirflowStateColor::UpForRetry,
                    "upstream_failed" => AirflowStateColor::UpstreamFailed,
                    "skipped" => AirflowStateColor::Skipped,
                    "removed" => AirflowStateColor::Removed,
                    _ => AirflowStateColor::None,
                };
                (state.clone(), color.into())
            } else {
                ("None".to_string(), AirflowStateColor::None.into())
            };
            
            // Get graph prefix for this task (depth-based indentation)
            // Color the circle to match the state
            let graph_line = self.graph_layout
                .get(&item.task_id)
                .map(|prefix| {
                    let rendered = prefix.render();
                    // Add circle marker after the connectors, colored by state
                    Line::from(vec![
                        Span::raw(rendered),
                        Span::styled("◉", DEFAULT_STYLE.fg(state_color)),
                    ])
                })
                .unwrap_or_else(|| Line::from(Span::styled("◉", DEFAULT_STYLE.fg(state_color))));
            
            Row::new(vec![
                graph_line,
                Line::from(item.task_id.as_str()),
                Line::from(format_duration_seconds(item.duration)),
                Line::from(Span::styled(state_text, DEFAULT_STYLE.fg(state_color))),
                Line::from(format!("{:?}", item.try_number)),
            ])
            .style(if self.marked.contains(&idx) {
                DEFAULT_STYLE.bg(MARKED_COLOR)
            } else {
                // Alternating row colors
                if (idx % 2) == 0 {
                    DEFAULT_STYLE
                } else {
                    DEFAULT_STYLE.bg(ALTERNATING_ROW_COLOR)
                }
            })
        });
        // Create title with DAG name in cyan (matching focused panel color)
        let title = if let Some(dag_id) = &self.dag_id {
            Line::from(vec![
                Span::styled("TaskInstances - ", DEFAULT_STYLE.fg(CYAN)),
                Span::styled(dag_id, DEFAULT_STYLE.fg(CYAN)),
            ])
        } else {
            Line::from(Span::styled("TaskInstances", DEFAULT_STYLE.fg(CYAN)))
        };
        
        let t = Table::new(
            rows,
            &[
                Constraint::Length(15),
                Constraint::Fill(1),
                Constraint::Length(10),
                Constraint::Length(16),
                Constraint::Length(5),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .border_type(BorderType::Rounded)
                .borders(Borders::ALL)
                .title(title)
                .title_bottom(Line::from(vec![
                    Span::styled("Press <?> for commands", DEFAULT_STYLE.fg(Color::DarkGray)),
                ]))
                .border_style(DEFAULT_STYLE.fg(CYAN)),
        )
        .style(DEFAULT_STYLE)
        .row_highlight_style(selected_style);

        StatefulWidget::render(t, rects[0], buffer, &mut self.filtered.state);

        match &mut self.popup {
            Some(TaskInstancePopUp::Clear(popup)) => {
                popup.render(area, buffer);
            }
            Some(TaskInstancePopUp::Mark(popup)) => {
                popup.render(area, buffer);
            }
            _ => (),
        }

        if let Some(commands) = &mut self.commands {
            commands.render(area, buffer);
        }

        if let Some(error_popup) = &self.error_popup {
            error_popup.render(area, buffer);
        }
    }
}
