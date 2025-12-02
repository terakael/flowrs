use std::collections::HashMap;
use std::vec;

use super::popup::commands_help::CommandPopUp;
use super::popup::error::ErrorPopup;
use super::popup::taskinstances::commands::TASK_COMMAND_POP_UP;
use crossterm::event::KeyCode;
use log::debug;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Stylize};
use ratatui::text::Line;
use ratatui::widgets::{Block, BorderType, Borders, Row, StatefulWidget, Table, Widget};

use crate::airflow::graph_layout::GraphPrefix;
use crate::airflow::model::common::TaskInstance;
use crate::app::events::custom::FlowrsEvent;
use crate::ui::common::{create_headers, state_to_colored_square};
use crate::ui::constants::{AirflowStateColor, ALTERNATING_ROW_COLOR, DEFAULT_STYLE, MARKED_COLOR};

use super::popup::taskinstances::clear::ClearTaskInstancePopup;
use super::popup::taskinstances::mark::MarkTaskInstancePopup;
use super::popup::taskinstances::TaskInstancePopUp;
use super::{filter::Filter, Model, StatefulTable, handle_table_scroll_keys};
use crate::app::worker::{OpenItem, WorkerMessage};

pub struct TaskInstanceModel {
    pub dag_id: Option<String>,
    pub dag_run_id: Option<String>,
    pub all: Vec<TaskInstance>,
    pub filtered: StatefulTable<TaskInstance>,
    pub filter: Filter,
    pub popup: Option<TaskInstancePopUp>,
    pub marked: Vec<usize>,
    commands: Option<&'static CommandPopUp<'static>>,
    pub error_popup: Option<ErrorPopup>,
    pub graph_layout: HashMap<String, GraphPrefix>,
    ticks: u32,
    event_buffer: Vec<FlowrsEvent>,
}

impl TaskInstanceModel {
    pub fn new() -> Self {
        TaskInstanceModel {
            dag_id: None,
            dag_run_id: None,
            all: vec![],
            filtered: StatefulTable::new(vec![]),
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
                } else if let Some(_commands) = &mut self.commands {
                    match key_event.code {
                        KeyCode::Char('q' | '?') | KeyCode::Esc => {
                            self.commands = None;
                        }
                        _ => (),
                    }
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
                    // Handle standard scrolling keybinds
                    if handle_table_scroll_keys(&mut self.filtered, key_event) {
                        return (None, vec![]);
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
                            self.commands = Some(&*TASK_COMMAND_POP_UP);
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

        let headers = ["Graph", "Task ID", "Duration", "State", "Tries"];
        let header_row = create_headers(headers);
        let header =
            Row::new(header_row).style(crate::ui::constants::HEADER_STYLE);

        let rows = self.filtered.items.iter().enumerate().map(|(idx, item)| {
            // Get graph prefix for this task (depth-based indentation)
            let graph_prefix = self.graph_layout
                .get(&item.task_id)
                .map(|prefix| {
                    let rendered = prefix.render();
                    // Add circle marker after the connectors
                    format!("{}◉", rendered)
                })
                .unwrap_or_else(|| "◉".to_string());
            
            // Determine state and color
            let (state_text, state_color) = if let Some(state) = &item.state {
                let color = match state.as_str() {
                    "success" => AirflowStateColor::Success,
                    "running" => AirflowStateColor::Running,
                    "failed" => AirflowStateColor::Failed,
                    "queued" => AirflowStateColor::Queued,
                    "up_for_retry" => AirflowStateColor::UpForRetry,
                    "upstream_failed" => AirflowStateColor::UpstreamFailed,
                    _ => AirflowStateColor::None,
                };
                (state.clone(), color)
            } else {
                ("None".to_string(), AirflowStateColor::None)
            };
            
            Row::new(vec![
                Line::from(graph_prefix),
                Line::from(item.task_id.as_str()),
                Line::from(if let Some(duration) = item.duration {
                    format!("{}", duration.ceil() as i64)
                } else {
                    "None".to_string()
                }),
                Line::from(state_text),
                Line::from(format!("{:?}", item.try_number)),
            ])
            .style(if self.marked.contains(&idx) {
                DEFAULT_STYLE.bg(MARKED_COLOR)
            } else {
                // Color the entire row based on state
                let base_style = if (idx % 2) == 0 {
                    DEFAULT_STYLE
                } else {
                    DEFAULT_STYLE.bg(ALTERNATING_ROW_COLOR)
                };
                base_style.fg(state_color.into())
            })
        });
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
                .title("TaskInstances - Press <?> to see available commands"),
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

        if let Some(commands) = &self.commands {
            commands.render(area, buffer);
        }

        if let Some(error_popup) = &self.error_popup {
            error_popup.render(area, buffer);
        }
    }
}
