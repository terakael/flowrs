use std::collections::HashMap;

use crossterm::event::KeyCode;
use log::debug;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Row, StatefulWidget, Table, Widget};
use time::OffsetDateTime;

use crate::airflow::model::common::{Dag, DagStatistic};
use crate::app::events::custom::FlowrsEvent;
use crate::app::model::popup::dags::commands::DAG_COMMAND_POP_UP;
use crate::ui::common::create_headers;
use crate::ui::constants::{AirflowStateColor, ALTERNATING_ROW_COLOR, DEFAULT_STYLE};

use super::popup::commands_help::CommandPopUp;
use super::popup::error::ErrorPopup;
use super::{filter::Filter, Model, StatefulTable};
use crate::app::worker::{OpenItem, WorkerMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DagStatus {
    Failed,    // Highest priority (sort first)
    Running,
    Idle,
    Paused,    // Lowest priority (sort last)
}

pub struct DagModel {
    pub all: Vec<Dag>,
    pub dag_stats: HashMap<String, Vec<DagStatistic>>,
    pub filtered: StatefulTable<Dag>,
    pub filter: Filter,
    pub show_paused: bool,
    pub import_error_count: usize,
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
            filtered: StatefulTable::new(vec![]),
            filter: Filter::new(),
            show_paused: false,
            import_error_count: 0,
            ticks: 0,
            commands: None,
            error_popup: None,
            event_buffer: vec![],
        }
    }

    pub fn filter_dags(&mut self) {
        let prefix = &self.filter.prefix;
        
        // Step 1: Filter by text search and active status
        let mut filtered_dags: Vec<Dag> = match prefix {
            Some(prefix) => self
                .all
                .iter()
                .filter(|dag| dag.dag_id.contains(prefix) && dag.is_active.unwrap_or(false))
                .cloned()
                .collect(),
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
                if !self.ticks.is_multiple_of(10) {
                    return (Some(FlowrsEvent::Tick), vec![]);
                }
                (
                    Some(FlowrsEvent::Tick),
                    vec![
                        WorkerMessage::UpdateDags {
                            only_active: !self.show_paused,
                        },
                        WorkerMessage::UpdateDagStats { clear: true },
                        WorkerMessage::UpdateImportErrors,
                    ],
                )
            }
            FlowrsEvent::Key(key_event) => {
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
                            self.filtered.next();
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            self.filtered.previous();
                        }
                        KeyCode::Char('G') => {
                            self.filtered.state.select_last();
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
                                    self.filtered.state.select_first();
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
        let rects = if self.filter.is_enabled() {
            let rects = Layout::default()
                .constraints([Constraint::Fill(90), Constraint::Max(3)].as_ref())
                .margin(0)
                .split(area);

            self.filter.render(rects[1], buf);

            rects
        } else {
            Layout::default()
                .constraints([Constraint::Percentage(100)].as_ref())
                .margin(0)
                .split(area)
        };
        let selected_style = crate::ui::constants::SELECTED_STYLE;

        let headers = ["Name", "Schedule", "Next Run", "Tags"];
        let header_row = create_headers(headers);
        let header = Row::new(header_row)
            .style(DEFAULT_STYLE.reversed())
            .add_modifier(Modifier::BOLD);
        let rows =
            self.filtered.items.iter().enumerate().map(|(idx, item)| {
                Row::new(vec![
                    {
                        // Determine DAG name color based on status
                        let status = self.get_dag_status(item);
                        let name_style = match status {
                            DagStatus::Failed => Style::default()
                                .fg(Color::Red)
                                .add_modifier(Modifier::BOLD),
                            DagStatus::Running => Style::default()
                                .fg(Color::Green)
                                .add_modifier(Modifier::BOLD),
                            DagStatus::Paused => Style::default()
                                .fg(Color::DarkGray)
                                .add_modifier(Modifier::BOLD),
                            DagStatus::Idle => Style::default()
                                .add_modifier(Modifier::BOLD),
                        };
                        Line::from(Span::styled(item.dag_id.as_str(), name_style))
                    },
                    Line::from({
                        let schedule = item.timetable_description.as_deref().unwrap_or("None");
                        // Shorten "Never, external triggers only" to just "Never"
                        if schedule.starts_with("Never") {
                            "Never"
                        } else {
                            schedule
                        }
                    })
                    .style(Style::default().fg(Color::LightYellow)),
                    Line::from(if let Some(date) = item.next_dagrun_create_after {
                        convert_datetimeoffset_to_human_readable_remaining_time(date)
                    } else {
                        "None".to_string()
                    }),
                    Line::from(if item.tags.is_empty() {
                        "".to_string()
                    } else {
                        item.tags.iter().map(|tag| tag.name.as_str()).collect::<Vec<_>>().join(", ")
                    }),
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
                    // Calculate stats for active DAGs only
                    let active_dags: Vec<_> = self.all.iter().filter(|d| d.is_active.unwrap_or(false)).collect();
                    let total_dags = active_dags.len();
                    let paused_dags = active_dags.iter().filter(|d| d.is_paused).count();
                    let unpaused_dags = total_dags - paused_dags;
                    
                    let mode = if self.show_paused { "All" } else { "Active Only" };
                    format!(
                        "DAGs ({}) - {}/{} Unpaused, {} Import Errors - Press <?> for commands",
                        mode, unpaused_dags, total_dags, self.import_error_count
                    )
                })
                .border_style(DEFAULT_STYLE)
                .style(DEFAULT_STYLE),
        )
        .row_highlight_style(selected_style);

        StatefulWidget::render(t, rects[0], buf, &mut self.filtered.state);

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
