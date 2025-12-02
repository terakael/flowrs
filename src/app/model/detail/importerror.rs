use crossterm::event::KeyCode;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
        StatefulWidget, Widget, Wrap,
    },
};

use crate::{
    airflow::model::common::ImportError,
    app::{events::custom::FlowrsEvent, model::{handle_vertical_scroll_keys, Model}, worker::WorkerMessage},
    ui::constants::DEFAULT_STYLE,
};

pub struct ImportErrorDetailModel {
    pub import_error: Option<ImportError>,
    cached_lines: Option<Vec<Line<'static>>>,
    vertical_scroll: usize,
    vertical_scroll_state: ScrollbarState,
}

impl ImportErrorDetailModel {
    pub fn new() -> Self {
        ImportErrorDetailModel {
            import_error: None,
            cached_lines: None,
            vertical_scroll: 0,
            vertical_scroll_state: ScrollbarState::default(),
        }
    }

    pub fn set_import_error(&mut self, import_error: ImportError) {
        self.import_error = Some(import_error);
        self.cached_lines = None; // Clear cache when setting new error
        self.vertical_scroll = 0;
        self.vertical_scroll_state = ScrollbarState::default();
    }

    pub fn clear(&mut self) {
        self.import_error = None;
        self.cached_lines = None;
        self.vertical_scroll = 0;
        self.vertical_scroll_state = ScrollbarState::default();
    }
    
    fn get_or_format_lines(&mut self) -> &Vec<Line<'static>> {
        if self.cached_lines.is_none() {
            self.cached_lines = Some(self.format_import_error());
        }
        self.cached_lines.as_ref().unwrap()
    }

    fn format_import_error(&self) -> Vec<Line<'static>> {
        if let Some(error) = &self.import_error {
            let mut lines = vec![];

            // Import Error ID
            if let Some(id) = error.import_error_id {
                lines.push(Line::from(vec![
                    Span::styled("ID:       ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(id.to_string()),
                ]));
            }

            // Filename (full path)
            lines.push(Line::from(vec![
                Span::styled("File:     ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(error.filename.as_ref().map_or("-".to_string(), |f| f.clone())),
            ]));

            // Filename stem (DAG name)
            if let Some(filename) = &error.filename {
                let stem = std::path::Path::new(filename)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("-");
                lines.push(Line::from(vec![
                    Span::styled("DAG Name: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(stem.to_string()),
                ]));
            }

            // Timestamp
            if let Some(timestamp) = &error.timestamp {
                lines.push(Line::from(vec![
                    Span::styled("Time:     ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(timestamp.clone()),
                ]));
            }

            lines.push(Line::from("")); // Empty line for spacing

            // Stack trace
            lines.push(Line::from(Span::styled(
                "Stack Trace:",
                Style::default().add_modifier(Modifier::BOLD).add_modifier(Modifier::UNDERLINED),
            )));
            lines.push(Line::from("")); // Empty line for spacing

            if let Some(stack_trace) = &error.stack_trace {
                for line in stack_trace.lines() {
                    lines.push(Line::from(line.to_string()));
                }
            } else {
                lines.push(Line::from(Span::styled(
                    "No stack trace available",
                    Style::default().fg(Color::DarkGray),
                )));
            }

            lines
        } else {
            vec![Line::from(Span::styled(
                "No import error data available",
                Style::default().fg(Color::DarkGray),
            ))]
        }
    }
}

impl Default for ImportErrorDetailModel {
    fn default() -> Self {
        Self::new()
    }
}

impl Model for ImportErrorDetailModel {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>) {
        match event {
            FlowrsEvent::Tick => (Some(FlowrsEvent::Tick), vec![]),
            FlowrsEvent::Key(key) => {
                // Get or format lines once for this update cycle
                let content_length = self.get_or_format_lines().len();
                
                // Handle standard scrolling keybinds with proper content length
                if handle_vertical_scroll_keys(
                    &mut self.vertical_scroll,
                    &mut self.vertical_scroll_state,
                    key,
                    Some(content_length),
                ) {
                    return (None, vec![]);
                }

                match key.code {
                    KeyCode::Char('g') => {
                        // Jump to top
                        self.vertical_scroll = 0;
                        self.vertical_scroll_state = self.vertical_scroll_state.position(0);
                        (None, vec![])
                    }
                    KeyCode::Char('G') => {
                        // Jump to bottom
                        self.vertical_scroll = content_length.saturating_sub(1);
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

impl Widget for &mut ImportErrorDetailModel {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = if let Some(error) = &self.import_error {
            // Extract DAG name from filename
            let dag_name = error.filename.as_ref().and_then(|f| {
                std::path::Path::new(f)
                    .file_stem()
                    .and_then(|s| s.to_str())
            }).unwrap_or("Unknown");
            
            format!("Import Error: {}", dag_name)
        } else {
            "Import Error".to_string()
        };

        // Get or format lines and store content length
        let lines = self.get_or_format_lines();
        let content_length = lines.len();
        let lines = lines.clone(); // Clone to release the borrow

        // Update scrollbar state with validation
        if content_length > 0 {
            self.vertical_scroll_state = self
                .vertical_scroll_state
                .content_length(content_length)
                .position(self.vertical_scroll);
        }

        let block = Block::default()
            .border_type(BorderType::Rounded)
            .borders(Borders::ALL)
            .title(title)
            .title_bottom(Line::from(vec![
                Span::styled("Press Esc/h/← to go back", Style::default().fg(Color::DarkGray)),
            ]))
            .border_style(DEFAULT_STYLE.fg(crate::ui::constants::RED))
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
