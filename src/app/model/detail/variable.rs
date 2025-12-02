use crossterm::event::KeyCode;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
        StatefulWidget, Widget, Wrap,
    },
};

use crate::{
    airflow::model::common::Variable,
    app::{events::custom::FlowrsEvent, model::{handle_vertical_scroll_keys, Model}, worker::WorkerMessage},
    ui::constants::DEFAULT_STYLE,
};

pub struct VariableDetailModel {
    pub variable: Option<Variable>,
    pub show_formatted: bool,
    vertical_scroll: usize,
    vertical_scroll_state: ScrollbarState,
}

impl VariableDetailModel {
    pub fn new() -> Self {
        VariableDetailModel {
            variable: None,
            show_formatted: true, // Default to formatted view
            vertical_scroll: 0,
            vertical_scroll_state: ScrollbarState::default(),
        }
    }

    pub fn set_variable(&mut self, variable: Variable) {
        self.variable = Some(variable);
        self.vertical_scroll = 0;
        self.vertical_scroll_state = ScrollbarState::default();
    }

    pub fn clear(&mut self) {
        self.variable = None;
        self.vertical_scroll = 0;
        self.vertical_scroll_state = ScrollbarState::default();
    }

    fn format_value(&self) -> Vec<Line<'static>> {
        if let Some(variable) = &self.variable {
            if let Some(value) = &variable.value {
                if self.show_formatted {
                    // Try to parse as JSON and pretty-print
                    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(value) {
                        if let Ok(pretty) = serde_json::to_string_pretty(&json_value) {
                            return pretty
                                .lines()
                                .map(|line| Line::from(line.to_string()))
                                .collect();
                        }
                    }
                }
                // Fall back to raw value (or if formatting is disabled)
                return value.lines().map(|line| Line::from(line.to_string())).collect();
            }
        }
        vec![Line::from(Span::styled(
            "No value available",
            Style::default().fg(Color::DarkGray),
        ))]
    }
}

impl Default for VariableDetailModel {
    fn default() -> Self {
        Self::new()
    }
}

impl Model for VariableDetailModel {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>) {
        match event {
            FlowrsEvent::Tick => (Some(FlowrsEvent::Tick), vec![]),
            FlowrsEvent::Key(key) => {
                // Handle standard scrolling keybinds
                if handle_vertical_scroll_keys(
                    &mut self.vertical_scroll,
                    &mut self.vertical_scroll_state,
                    key,
                    None,
                ) {
                    return (None, vec![]);
                }

                match key.code {
                    KeyCode::Char('f') => {
                        // Toggle formatted/raw view
                        self.show_formatted = !self.show_formatted;
                        (None, vec![])
                    }
                    KeyCode::Char('g') => {
                        // Jump to top
                        self.vertical_scroll = 0;
                        self.vertical_scroll_state = self.vertical_scroll_state.position(0);
                        (None, vec![])
                    }
                    KeyCode::Char('G') => {
                        // Jump to bottom
                        let lines = self.format_value();
                        self.vertical_scroll = lines.len().saturating_sub(1);
                        self.vertical_scroll_state =
                            self.vertical_scroll_state.position(self.vertical_scroll);
                        (None, vec![])
                    }
                    _ => (Some(FlowrsEvent::Key(*key)), vec![]),
                }
            }
            FlowrsEvent::Mouse => (Some(event.clone()), vec![]),
        }
    }
}

impl Widget for &mut VariableDetailModel {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = if let Some(variable) = &self.variable {
            format!("Variable: {}", variable.key)
        } else {
            "Variable".to_string()
        };

        let format_hint = if self.show_formatted {
            " Press f for raw view "
        } else {
            " Press f for formatted view "
        };

        let lines = self.format_value();
        let content_length = lines.len();

        // Update scrollbar state
        self.vertical_scroll_state = self
            .vertical_scroll_state
            .content_length(content_length)
            .position(self.vertical_scroll);

        let block = Block::default()
            .border_type(BorderType::Rounded)
            .borders(Borders::ALL)
            .title(title)
            .title_bottom(Line::from(vec![
                Span::styled("Press Esc/h/← to go back", Style::default().fg(Color::DarkGray)),
                Span::raw(" | "),
                Span::styled(format_hint, Style::default().fg(Color::DarkGray)),
            ]))
            .border_style(DEFAULT_STYLE.fg(Color::Cyan))
            .style(DEFAULT_STYLE);

        let paragraph = Paragraph::new(lines)
            .block(block)
            .style(DEFAULT_STYLE)
            .wrap(Wrap { trim: false })
            .scroll((self.vertical_scroll as u16, 0));

        paragraph.render(area, buf);

        // Render scrollbar
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"));
        let mut scrollbar_state = self.vertical_scroll_state.clone();
        scrollbar.render(area, buf, &mut scrollbar_state);
    }
}
