use ratatui::{
    style::{Color, Style},
    text::{Line, Span},
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::constants::{
    AirflowStateColor, BLUE, BRIGHT_BLUE, BRIGHT_CYAN, BRIGHT_GREEN, BRIGHT_MAGENTA, BRIGHT_RED,
    BRIGHT_WHITE, BRIGHT_YELLOW, CYAN, DEFAULT_STATE_ICON, FOREGROUND, HEADER_STYLE, MAGENTA,
    RUNNING_STATE_ICON, WHITE,
};

pub fn create_headers<'a>(
    headers: impl IntoIterator<Item = &'a str>,
) -> impl Iterator<Item = Line<'a>> {
    headers
        .into_iter()
        .map(|h| Line::from(h).style(HEADER_STYLE).left_aligned())
}

pub fn state_to_colored_square<'a>(color: AirflowStateColor) -> Span<'a> {
    Span::styled(DEFAULT_STATE_ICON, Style::default().fg(color.into()))
}

/// Get state icon based on state string
/// 
/// Returns a play symbol (▶) for running states and a square (■) for all other states.
/// This provides consistent visual indication of active execution across the UI.
/// 
/// # Arguments
/// * `state` - Optional state string (e.g., "running", "success", "failed")
/// 
/// # Returns
/// * `RUNNING_STATE_ICON` ("▶") if state is "running"
/// * `DEFAULT_STATE_ICON` ("■") for all other states or None
pub fn get_state_icon(state: Option<&str>) -> &'static str {
    match state {
        Some("running") => RUNNING_STATE_ICON,
        _ => DEFAULT_STATE_ICON,
    }
}

/// Map a string to a consistent color using hash-based mapping.
/// Useful for consistently coloring tags, connection types, etc.
/// 
/// Uses theme colors plus additional RGB colors for maximum variety.
/// Avoids RED/GREEN/YELLOW which are reserved for state indication.
pub fn hash_to_color(input: &str) -> Color {
    const COLORS: &[Color] = &[
        // Theme Blues - cool, calm colors
        BLUE,
        BRIGHT_BLUE,
        Color::Rgb(0x7f, 0xbb, 0xca),  // Light blue
        Color::Rgb(0x5a, 0x8f, 0xb0),  // Medium blue
        
        // Theme Magentas/Purples - distinct and visible
        MAGENTA,
        BRIGHT_MAGENTA,
        Color::Rgb(0xb5, 0x89, 0xd6),  // Light purple
        Color::Rgb(0x9d, 0x79, 0xd6),  // Medium purple
        
        // Theme Cyans/Teals - fresh, distinguishable
        CYAN,
        BRIGHT_CYAN,
        Color::Rgb(0x83, 0xc0, 0x92),  // Light teal
        Color::Rgb(0x6a, 0xa8, 0x9a),  // Medium teal
        
        // Theme Greens (bright variants, distinct from state green)
        BRIGHT_GREEN,
        
        // Theme Whites/Grays - subtle but visible
        WHITE,
        BRIGHT_WHITE,
        
        // Theme Reds (bright variant, distinct from error red)
        BRIGHT_RED,
        
        // Additional Oranges - warm, visible (avoiding yellow)
        Color::Rgb(0xd6, 0x99, 0x78),  // Light orange
        Color::Rgb(0xc0, 0x85, 0x68),  // Medium orange
        Color::Rgb(0xa8, 0x7c, 0x5f),  // Dark orange
        
        // Additional Pink/Rose - soft, distinguishable
        Color::Rgb(0xd6, 0x9c, 0xb8),  // Light pink
        Color::Rgb(0xc5, 0x88, 0xa8),  // Medium pink
        
        // Additional Olive/Brown - earthy tones
        Color::Rgb(0xa8, 0xa0, 0x78),  // Light olive
        Color::Rgb(0x95, 0x8d, 0x70),  // Medium olive
        
        // Additional Gray-blues - subtle distinction
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
        
        // Add highlighted matching part with yellow background and cream foreground
        // Use FOREGROUND color for better readability
        spans.push(Span::styled(
            &text[idx..idx + search.len()],
            Style::default()
                .fg(FOREGROUND)
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

/// Format duration from start and end dates to human-readable format
/// 
/// Returns formats like "2h 15m 30s", "5m 45s", or "30s" depending on magnitude.
/// Returns "Running" if end is None (task still running) or if duration is negative.
/// Returns "-" if start is None.
pub fn format_duration(start_date: Option<time::OffsetDateTime>, end_date: Option<time::OffsetDateTime>) -> String {
    match (start_date, end_date) {
        (Some(start), Some(end)) => {
            let duration = end - start;
            let total_seconds = duration.whole_seconds();
            
            if total_seconds < 0 {
                return "Running".to_string();
            }
            
            format_seconds(total_seconds)
        }
        (Some(_), None) => "Running".to_string(),
        _ => "-".to_string(),
    }
}

/// Format duration in seconds (as f64) to human-readable format
/// 
/// Returns formats like "2h 15m 30s", "5m 45s", or "30s" depending on magnitude.
/// Returns "-" if duration is None or negative.
pub fn format_duration_seconds(duration_seconds: Option<f64>) -> String {
    match duration_seconds {
        Some(duration) if duration >= 0.0 => {
            format_seconds(duration as i64)
        }
        _ => "-".to_string(),
    }
}

/// Internal helper to format seconds into human-readable string
fn format_seconds(total_seconds: i64) -> String {
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    
    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}
