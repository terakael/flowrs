use ratatui::{
    style::{Color, Style},
    text::{Line, Span},
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::constants::{AirflowStateColor, BLACK, BRIGHT_YELLOW, HEADER_STYLE};

pub fn create_headers<'a>(
    headers: impl IntoIterator<Item = &'a str>,
) -> impl Iterator<Item = Line<'a>> {
    headers
        .into_iter()
        .map(|h| Line::from(h).style(HEADER_STYLE).left_aligned())
}

pub fn state_to_colored_square<'a>(color: AirflowStateColor) -> Span<'a> {
    Span::styled("■", Style::default().fg(color.into()))
}

/// Map a string to a consistent color using hash-based mapping.
/// Useful for consistently coloring tags, connection types, etc.
pub fn hash_to_color(input: &str) -> Color {
    // Available colors (avoiding red/green/yellow which are used for states)
    // Using a wide variety of colors for better visual distinction
    const COLORS: &[Color] = &[
        // Blues
        crate::ui::constants::BLUE,
        crate::ui::constants::BRIGHT_BLUE,
        Color::Rgb(0x7f, 0xbb, 0xca),  // Light blue
        Color::Rgb(0x5a, 0x8f, 0xb0),  // Medium blue
        
        // Magentas/Purples
        crate::ui::constants::MAGENTA,
        crate::ui::constants::BRIGHT_MAGENTA,
        Color::Rgb(0xb5, 0x89, 0xd6),  // Light purple
        Color::Rgb(0x9d, 0x79, 0xd6),  // Medium purple
        
        // Cyans/Teals
        crate::ui::constants::CYAN,
        crate::ui::constants::BRIGHT_CYAN,
        Color::Rgb(0x83, 0xc0, 0x92),  // Light teal
        Color::Rgb(0x6a, 0xa8, 0x9a),  // Medium teal
        
        // Oranges (safe, not too bright)
        Color::Rgb(0xd6, 0x99, 0x78),  // Light orange
        Color::Rgb(0xc0, 0x85, 0x68),  // Medium orange
        Color::Rgb(0xa8, 0x7c, 0x5f),  // Dark orange
        
        // Pink/Rose
        Color::Rgb(0xd6, 0x9c, 0xb8),  // Light pink
        Color::Rgb(0xc5, 0x88, 0xa8),  // Medium pink
        
        // Olive/Brown tones
        Color::Rgb(0xa8, 0xa0, 0x78),  // Light olive
        Color::Rgb(0x95, 0x8d, 0x70),  // Medium olive
        
        // Gray-blues (for subtle distinction)
        Color::Rgb(0x7a, 0x8b, 0x99),  // Blue-gray
        Color::Rgb(0x8a, 0x9a, 0xa5),  // Light blue-gray
    ];
    
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    let hash = hasher.finish();
    
    COLORS[(hash as usize) % COLORS.len()]
}

/// Highlight search text with yellow background (case-insensitive matching)
/// 
/// Returns a vector of spans where matching portions are highlighted with a yellow background.
/// Empty text or search strings return a single span with the base color.
/// If search is None, returns the text with base color.
pub fn highlight_search_text<'a>(text: &'a str, search: Option<&str>, base_color: Color) -> Vec<Span<'a>> {
    let Some(search) = search else {
        return vec![Span::styled(text, Style::default().fg(base_color))];
    };
    
    if text.is_empty() || search.is_empty() {
        return vec![Span::styled(text, Style::default().fg(base_color))];
    }
    
    let mut spans = Vec::new();
    let lower_text = text.to_lowercase();
    let lower_search = search.to_lowercase();
    let mut last_end = 0;
    
    // Find all occurrences (case-insensitive)
    for (idx, _) in lower_text.match_indices(&lower_search) {
        // Add non-matching part
        if idx > last_end {
            spans.push(Span::styled(
                &text[last_end..idx],
                Style::default().fg(base_color),
            ));
        }
        
        // Add highlighted matching part with yellow background and dark text
        spans.push(Span::styled(
            &text[idx..idx + search.len()],
            Style::default()
                .fg(BLACK)
                .bg(BRIGHT_YELLOW),
        ));
        
        last_end = idx + search.len();
    }
    
    // Add remaining text
    if last_end < text.len() {
        spans.push(Span::styled(
            &text[last_end..],
            Style::default().fg(base_color),
        ));
    }
    
    spans
}
