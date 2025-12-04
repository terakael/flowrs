use crossterm::event::{KeyCode, KeyModifiers};
use log::debug;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout, Rect};

use ratatui::text::Line;
use ratatui::widgets::{Block, BorderType, Borders, Row, StatefulWidget, Table, Widget};

use crate::airflow::config::AirflowConfig;
use crate::app::events::custom::FlowrsEvent;
use crate::app::worker::{OpenItem, WorkerMessage};
use crate::ui::constants::{ALTERNATING_ROW_COLOR, DEFAULT_STYLE, HEADER_STYLE, RED};

use super::popup::commands_help::CommandPopUp;
use super::popup::config::commands::create_config_command_popup;
use super::popup::error::ErrorPopup;
use super::sortable_table::{CustomSort, SortableTable};
use super::{filter::Filter, handle_command_popup_events, Model, HALF_PAGE_SIZE};

// Implement CustomSort for AirflowConfig
impl CustomSort for AirflowConfig {
    fn column_value(&self, column_index: usize) -> String {
        match column_index {
            0 => match self.version {
                crate::airflow::config::AirflowVersion::V2 => "v2".to_string(),
                crate::airflow::config::AirflowVersion::V3 => "v3".to_string(),
            },
            1 => self.name.clone(),
            2 => self.endpoint.clone(),
            _ => String::new(),
        }
    }
}

pub struct ConfigModel {
    pub all: Vec<AirflowConfig>,
    pub filtered: SortableTable<AirflowConfig>,
    pub filter: Filter,
    pub commands: Option<CommandPopUp<'static>>,
    pub error_popup: Option<ErrorPopup>,
}

impl ConfigModel {
    pub fn new(configs: Vec<AirflowConfig>) -> Self {
        let headers = ["Version", "Name", "Endpoint"];
        // Reserved keys: j/k (scroll), o (open), ? (help), / (filter), q (quit)
        let reserved = &['j', 'k', 'o', '?', '/', 'q'];
        ConfigModel {
            all: configs.clone(),
            filtered: SortableTable::new(&headers, configs, reserved),
            filter: Filter::new(),
            commands: None,
            error_popup: None,
        }
    }

    pub fn new_with_errors(configs: Vec<AirflowConfig>, errors: Vec<String>) -> Self {
        let error_popup = if errors.is_empty() {
            None
        } else {
            Some(ErrorPopup::from_strings(errors))
        };

        let headers = ["Version", "Name", "Endpoint"];
        let reserved = &['j', 'k', 'o', '?', '/', 'q'];
        ConfigModel {
            all: configs.clone(),
            filtered: SortableTable::new(&headers, configs, reserved),
            filter: Filter::new(),
            commands: None,
            error_popup,
        }
    }

    pub fn filter_configs(&mut self) {
        let prefix = &self.filter.prefix;
        let dags = &self.all;
        let filtered_configs = match prefix {
            Some(prefix) => self
                .all
                .iter()
                .filter(|config| config.name.contains(prefix))
                .cloned()
                .collect::<Vec<AirflowConfig>>(),
            None => dags.clone(),
        };
        self.filtered.items = filtered_configs;
        // Reapply current sort after filtering
        self.filtered.reapply_sort();
    }
}

impl Model for ConfigModel {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>) {
        match event {
            FlowrsEvent::Tick => (Some(FlowrsEvent::Tick), vec![]),
            FlowrsEvent::Key(key_event) => {
                if self.filter.enabled {
                    self.filter.update(key_event);
                    self.filter_configs();
                    return (None, vec![]);
                } else if self.error_popup.is_some() {
                    match key_event.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            self.error_popup = None;
                        }
                        _ => (),
                    }
                } else if self.commands.is_some() {
                    return handle_command_popup_events(&mut self.commands, key_event);
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
                    
                    // Handle scrolling and sorting
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
                                self.filter_configs();
                                return (None, vec![]);
                            }
                            
                            // Otherwise handle specific commands
                            match c {
                                '/' => {
                                    self.filter.toggle();
                                }
                                'o' => {
                                    let selected_config =
                                        self.filtered.state.selected().unwrap_or_default();
                                    let endpoint = self.filtered.items[selected_config].endpoint.clone();
                                    return (
                                        Some(event.clone()),
                                        vec![WorkerMessage::OpenItem(OpenItem::Config(endpoint))],
                                    );
                                }
                                '?' => {
                                    self.commands = Some(create_config_command_popup());
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Enter => {
                            let selected_config =
                                self.filtered.state.selected().unwrap_or_default();
                            debug!(
                                "Selected config: {}",
                                self.filtered.items[selected_config].name
                            );

                            return (
                                Some(event.clone()),
                                vec![WorkerMessage::ConfigSelected(selected_config)],
                            );
                        }
                        _ => (),
                    }
                    return (Some(event.clone()), vec![]);
                }
                (None, vec![])
            }
            FlowrsEvent::Mouse => (Some(event.clone()), vec![]),
        }
    }
}

impl Widget for &mut ConfigModel {
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

        // Automatically generated sortable headers
        let header_row = self.filtered.render_headers(HEADER_STYLE, RED);
        let header = Row::new(header_row).style(HEADER_STYLE);

        let rows = self.filtered.items.iter().enumerate().map(|(idx, item)| {
            Row::new(vec![
                Line::from(match item.version {
                    crate::airflow::config::AirflowVersion::V2 => "v2",
                    crate::airflow::config::AirflowVersion::V3 => "v3",
                }),
                Line::from(item.name.as_str()),
                Line::from(item.endpoint.as_str()),
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
                Constraint::Min(8),
                Constraint::Percentage(20),
                Constraint::Percentage(70),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .border_type(BorderType::Rounded)
                .borders(Borders::ALL)
                .title("Environment"),
        )
        .style(DEFAULT_STYLE)
        .row_highlight_style(selected_style);
        StatefulWidget::render(t, rects[0], buf, &mut self.filtered.state);

        if let Some(commands) = &mut self.commands {
            commands.render(area, buf);
        }

        if let Some(error_popup) = &self.error_popup {
            error_popup.render(area, buf);
        }
    }
}
