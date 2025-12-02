use crossterm::event::KeyCode;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph, Widget, Wrap},
};

use crate::{
    airflow::model::common::Connection,
    app::{events::custom::FlowrsEvent, model::Model, worker::WorkerMessage},
    ui::{common::hash_to_color, constants::DEFAULT_STYLE},
};

pub struct ConnectionDetailModel {
    pub connection: Option<Connection>,
    pub show_sensitive: bool,
    pub show_formatted: bool, // For pretty-printing JSON in extra field
}

impl ConnectionDetailModel {
    pub fn new() -> Self {
        ConnectionDetailModel {
            connection: None,
            show_sensitive: false, // Default to masked
            show_formatted: true,  // Default to formatted JSON
        }
    }

    pub fn set_connection(&mut self, connection: Connection) {
        self.connection = Some(connection);
        self.show_sensitive = false; // Reset to masked when viewing new connection
        self.show_formatted = true;  // Reset to formatted
    }

    pub fn clear(&mut self) {
        self.connection = None;
        self.show_sensitive = false;
        self.show_formatted = true;
    }

    fn format_connection(&self) -> Vec<Line<'static>> {
        if let Some(conn) = &self.connection {
            let mut lines = vec![];
            let type_color = hash_to_color(&conn.conn_type);

            // Connection ID
            lines.push(Line::from(vec![
                Span::styled("ID:       ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(conn.connection_id.clone()),
            ]));

            // Type (with color)
            lines.push(Line::from(vec![
                Span::styled("Type:     ", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(conn.conn_type.clone(), Style::default().fg(type_color)),
            ]));

            // Host
            lines.push(Line::from(vec![
                Span::styled("Host:     ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(conn.host.as_ref().map_or("-".to_string(), |h| h.clone())),
            ]));

            // Port
            lines.push(Line::from(vec![
                Span::styled("Port:     ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(conn.port.map_or("-".to_string(), |p| p.to_string())),
            ]));

            // Login
            lines.push(Line::from(vec![
                Span::styled("Login:    ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(conn.login.as_ref().map_or("-".to_string(), |l| l.clone())),
            ]));

            // Schema
            lines.push(Line::from(vec![
                Span::styled("Schema:   ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(conn.schema.as_ref().map_or("-".to_string(), |s| s.clone())),
            ]));

            lines.push(Line::from("")); // Empty line for spacing

            // Password (sensitive, always masked)
            let password_display = if let Some(pwd) = &conn.password {
                if self.show_sensitive {
                    pwd.clone()
                } else {
                    "********".to_string()
                }
            } else {
                "-".to_string()
            };
            
            let password_style = if self.show_sensitive && conn.password.is_some() {
                Style::default().fg(crate::ui::constants::RED)
            } else {
                Style::default()
            };

            lines.push(Line::from(vec![
                Span::styled("Password: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(password_display, password_style),
            ]));

            // Extra (can be JSON, not masked, supports pretty-print)
            if let Some(extra) = &conn.extra {
                lines.push(Line::from("")); // Empty line for spacing
                lines.push(Line::from(Span::styled(
                    "Extra:",
                    Style::default().add_modifier(Modifier::BOLD),
                )));

                let extra_display = if self.show_formatted {
                    // Try to pretty-print JSON if possible
                    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(extra) {
                        if let Ok(pretty) = serde_json::to_string_pretty(&json_value) {
                            pretty
                        } else {
                            extra.clone()
                        }
                    } else {
                        extra.clone()
                    }
                } else {
                    extra.clone()
                };

                for line in extra_display.lines() {
                    lines.push(Line::from(line.to_string()));
                }
            }

            lines
        } else {
            vec![Line::from(Span::styled(
                "No connection data available",
                Style::default().fg(Color::DarkGray),
            ))]
        }
    }
}

impl Default for ConnectionDetailModel {
    fn default() -> Self {
        Self::new()
    }
}

impl Model for ConnectionDetailModel {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>) {
        match event {
            FlowrsEvent::Tick => (Some(FlowrsEvent::Tick), vec![]),
            FlowrsEvent::Key(key) => match key.code {
                KeyCode::Char('s') => {
                    // Toggle show/hide password
                    self.show_sensitive = !self.show_sensitive;
                    (None, vec![])
                }
                KeyCode::Char('f') => {
                    // Toggle formatted/raw view for extra field
                    self.show_formatted = !self.show_formatted;
                    (None, vec![])
                }
                _ => (Some(FlowrsEvent::Key(*key)), vec![]),
            },
            FlowrsEvent::Mouse => (Some(event.clone()), vec![]),
        }
    }
}

impl Widget for &mut ConnectionDetailModel {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Build title with colored "Connection:" prefix (green like variables)
        let title = if let Some(conn) = &self.connection {
            let type_color = hash_to_color(&conn.conn_type);
            let sensitive_indicator = if self.show_sensitive {
                " ⚠️  PASSWORD VISIBLE "
            } else {
                ""
            };
            
            Line::from(vec![
                Span::styled("Connection: ", Style::default().fg(crate::ui::constants::GREEN)),
                Span::raw(conn.connection_id.clone()),
                Span::raw(" ("),
                Span::styled(conn.conn_type.clone(), Style::default().fg(type_color)),
                Span::raw(")"),
                Span::styled(sensitive_indicator, Style::default().fg(crate::ui::constants::RED).add_modifier(Modifier::BOLD)),
            ])
        } else {
            Line::from(vec![
                Span::styled("Connection", Style::default().fg(crate::ui::constants::GREEN)),
            ])
        };

        let border_style = if self.show_sensitive {
            DEFAULT_STYLE.fg(crate::ui::constants::RED)
        } else {
            DEFAULT_STYLE.fg(Color::Cyan)
        };

        let lines = self.format_connection();

        let format_hint = if self.show_formatted {
            "f for raw view"
        } else {
            "f for formatted view"
        };

        let block = Block::default()
            .border_type(BorderType::Rounded)
            .borders(Borders::ALL)
            .title(title)
            .title_bottom(Line::from(vec![
                Span::styled("Press Esc/h/← to go back", Style::default().fg(Color::DarkGray)),
                Span::raw(" | "),
                Span::styled(
                    if self.show_sensitive {
                        "s to hide password"
                    } else {
                        "s to show password"
                    },
                    Style::default().fg(Color::DarkGray),
                ),
                Span::raw(" | "),
                Span::styled(format_hint, Style::default().fg(Color::DarkGray)),
            ]))
            .border_style(border_style)
            .style(DEFAULT_STYLE);

        let paragraph = Paragraph::new(lines)
            .block(block)
            .style(DEFAULT_STYLE)
            .wrap(Wrap { trim: false });

        paragraph.render(area, buf);
    }
}
