use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::widgets::{ScrollbarState, TableState};

use super::{events::custom::FlowrsEvent, worker::WorkerMessage};

pub mod config;
pub mod dagruns;
pub mod dags;
pub mod filter;
pub mod logs;
pub mod popup;
pub mod taskinstances;

pub trait Model {
    fn update(&mut self, event: &FlowrsEvent) -> (Option<FlowrsEvent>, Vec<WorkerMessage>);
}

/// Number of rows to jump when using half-page navigation (Ctrl+D / Ctrl+U)
pub const HALF_PAGE_SIZE: usize = 10;

#[derive(Clone)]
pub struct StatefulTable<T> {
    pub state: TableState,
    pub items: Vec<T>,
}

impl<T> StatefulTable<T> {
    pub fn new(items: Vec<T>) -> StatefulTable<T> {
        StatefulTable {
            state: TableState::default(),
            items,
        }
    }

    /// Scroll by delta rows (positive=down, negative=up)
    /// Clamps at boundaries (no wrapping)
    pub fn scroll_by(&mut self, delta: isize) {
        if self.items.is_empty() {
            return;
        }
        
        let current = self.state.selected().unwrap_or(0);
        let len = self.items.len();
        
        let new_pos = if delta > 0 {
            (current + delta as usize).min(len - 1)
        } else {
            current.saturating_sub((-delta) as usize)
        };
        
        self.state.select(Some(new_pos));
    }
}

/// Scroll vertical text content by delta lines
/// 
/// # Arguments
/// * `scroll` - Mutable reference to current scroll position
/// * `scroll_state` - Mutable reference to ScrollbarState
/// * `delta` - Lines to scroll (positive=down, negative=up)
/// * `max_lines` - Optional maximum scroll position (content length)
pub fn scroll_vertical_by(
    scroll: &mut usize,
    scroll_state: &mut ScrollbarState,
    delta: isize,
    max_lines: Option<usize>,
) {
    let new_scroll = if delta > 0 {
        scroll.saturating_add(delta as usize)
    } else {
        scroll.saturating_sub((-delta) as usize)
    };
    
    // Apply bounds if max_lines provided
    *scroll = if let Some(max) = max_lines {
        new_scroll.min(max.saturating_sub(1))
    } else {
        new_scroll
    };
    
    // Update scrollbar state
    *scroll_state = scroll_state.position(*scroll);
}

/// Handle standard scrolling keybinds for a StatefulTable
/// Returns true if the key was handled, false otherwise
pub fn handle_table_scroll_keys<T>(table: &mut StatefulTable<T>, key_event: &KeyEvent) -> bool {
    // Handle Ctrl+D and Ctrl+U for half-page scrolling
    if key_event.modifiers == KeyModifiers::CONTROL {
        match key_event.code {
            KeyCode::Char('d') => {
                table.scroll_by(HALF_PAGE_SIZE as isize);
                return true;
            }
            KeyCode::Char('u') => {
                table.scroll_by(-(HALF_PAGE_SIZE as isize));
                return true;
            }
            _ => {}
        }
    }
    
    // Handle j/k and arrow keys for single-line scrolling
    match key_event.code {
        KeyCode::Down | KeyCode::Char('j') => {
            table.scroll_by(1);
            true
        }
        KeyCode::Up | KeyCode::Char('k') => {
            table.scroll_by(-1);
            true
        }
        _ => false,
    }
}

/// Handle standard scrolling keybinds for vertical text content
/// Returns true if the key was handled, false otherwise
pub fn handle_vertical_scroll_keys(
    scroll: &mut usize,
    scroll_state: &mut ScrollbarState,
    key_event: &KeyEvent,
    max_lines: Option<usize>,
) -> bool {
    // Handle Ctrl+D and Ctrl+U for half-page scrolling
    if key_event.modifiers == KeyModifiers::CONTROL {
        match key_event.code {
            KeyCode::Char('d') => {
                scroll_vertical_by(scroll, scroll_state, HALF_PAGE_SIZE as isize, max_lines);
                return true;
            }
            KeyCode::Char('u') => {
                scroll_vertical_by(scroll, scroll_state, -(HALF_PAGE_SIZE as isize), max_lines);
                return true;
            }
            _ => {}
        }
    }
    
    // Handle j/k and arrow keys for single-line scrolling
    match key_event.code {
        KeyCode::Down | KeyCode::Char('j') => {
            scroll_vertical_by(scroll, scroll_state, 1, max_lines);
            true
        }
        KeyCode::Up | KeyCode::Char('k') => {
            scroll_vertical_by(scroll, scroll_state, -1, max_lines);
            true
        }
        _ => false,
    }
}
