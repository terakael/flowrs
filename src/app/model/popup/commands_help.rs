use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Clear, Row, StatefulWidget, Table, Widget},
};

use super::popup_area;
use crate::app::model::{filter::Filter, StatefulTable};
use crate::ui::common::{create_headers, highlight_search_text};
use crate::ui::constants::{ALTERNATING_ROW_COLOR, DEFAULT_STYLE, HEADER_STYLE};

#[derive(Clone)]
pub struct Command<'a> {
    pub name: &'a str,
    pub key_binding: &'a str,
    pub description: &'a str,
}

pub struct CommandPopUp<'a> {
    pub title: String,
    pub all_commands: Vec<Command<'a>>,
    pub filtered: StatefulTable<Command<'a>>,
    pub filter: Filter,
}

impl<'a> CommandPopUp<'a> {
    pub fn new(title: String, commands: Vec<Command<'a>>) -> Self {
        let mut popup = CommandPopUp {
            title,
            all_commands: commands.clone(),
            filtered: StatefulTable::new(commands),
            filter: Filter::new(),
        };
        // Select first item by default (consistent with other tables)
        if !popup.filtered.items.is_empty() {
            popup.filtered.state.select(Some(0));
        }
        popup
    }

    pub fn filter_commands(&mut self) {
        let prefix = &self.filter.prefix;
        let filtered = match prefix {
            Some(prefix) => {
                let lower_prefix = prefix.to_lowercase();
                self.all_commands
                    .iter()
                    .filter(|cmd| {
                        cmd.key_binding.to_lowercase().contains(&lower_prefix)
                            || cmd.name.to_lowercase().contains(&lower_prefix)
                            || cmd.description.to_lowercase().contains(&lower_prefix)
                    })
                    .cloned()
                    .collect()
            }
            None => self.all_commands.clone(),
        };
        self.filtered.items = filtered;
        // Maintain selection or select first if current selection is out of bounds
        if !self.filtered.items.is_empty() {
            let current = self.filtered.state.selected().unwrap_or(0);
            if current >= self.filtered.items.len() {
                self.filtered.state.select(Some(0));
            }
        }
    }
}

impl Widget for &mut CommandPopUp<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let popup_area = popup_area(area, 80, 80);
        
        Clear.render(popup_area, buf);
        
        // Split area for filter if enabled
        let rects = if self.filter.is_enabled() {
            let rects = Layout::default()
                .constraints([Constraint::Fill(90), Constraint::Max(3)])
                .split(popup_area);
            self.filter.render(rects[1], buf);
            rects
        } else {
            Layout::default()
                .constraints([Constraint::Percentage(100)])
                .split(popup_area)
        };
        
        // Create table headers
        let headers = ["Key", "Command", "Description"];
        let header_row = create_headers(headers);
        let header = Row::new(header_row).style(HEADER_STYLE);
        
        // Get current filter text for highlighting
        let search_text = self.filter.prefix.as_ref().map(String::as_str);
        
        // Create table rows with alternating colors and search highlighting
        let rows = self.filtered.items.iter().enumerate().map(|(idx, cmd)| {
            Row::new(vec![
                Line::from(highlight_search_text(cmd.key_binding, search_text, Color::White)),
                Line::from(highlight_search_text(cmd.name, search_text, Color::White)),
                Line::from(highlight_search_text(cmd.description, search_text, Color::DarkGray)),
            ])
            .style(if (idx % 2) == 0 {
                DEFAULT_STYLE
            } else {
                DEFAULT_STYLE.bg(ALTERNATING_ROW_COLOR)
            })
        });
        
        // Create table
        let table = Table::new(
            rows,
            &[
                Constraint::Percentage(20),  // Key
                Constraint::Percentage(25),  // Command
                Constraint::Percentage(55),  // Description
            ],
        )
        .header(header)
        .block(
            Block::default()
                .border_type(BorderType::Rounded)
                .borders(Borders::ALL)
                .title(self.title.as_str())
                .title_bottom(Line::from(vec![
                    Span::styled("j/k: scroll", Style::default().fg(Color::DarkGray)),
                    Span::raw(" | "),
                    Span::styled("/: filter", Style::default().fg(Color::DarkGray)),
                    Span::raw(" | "),
                    Span::styled("Enter/Esc: close", Style::default().fg(Color::DarkGray)),
                ]))
        )
        .style(DEFAULT_STYLE)
        .row_highlight_style(
            Style::default()
                .bg(Color::Rgb(60, 60, 60))
                .add_modifier(Modifier::BOLD)
        );
        
        // Render as stateful widget
        StatefulWidget::render(table, rects[0], buf, &mut self.filtered.state);
    }
}

pub struct DefaultCommands(pub Vec<Command<'static>>);

impl DefaultCommands {
    pub fn new() -> Self {
        Self(vec![
            Command {
                name: "Enter",
                key_binding: "Enter",
                description: "Open the selected item",
            },
            Command {
                name: "Refresh",
                key_binding: "r",
                description: "Refresh current panel data",
            },
            Command {
                name: "Filter",
                key_binding: "/",
                description: "Filter items",
            },
            Command {
                name: "Open",
                key_binding: "o",
                description: "Open the selected item in the browser",
            },
            Command {
                name: "Previous",
                key_binding: "k / Up",
                description: "Move to the previous item",
            },
            Command {
                name: "Next",
                key_binding: "j / Down",
                description: "Move to the next item",
            },
            Command {
                name: "Previous tab",
                key_binding: "h / Left / Esc",
                description: "Move to the previous tab",
            },
            Command {
                name: "Next tab",
                key_binding: "l / Right",
                description: "Move to the next tab",
            },
            Command {
                name: "Help",
                key_binding: "?",
                description: "Show help",
            },
            Command {
                name: "Quit",
                key_binding: "q / Ctrl-c",
                description: "Quit",
            },
        ])
    }
}
