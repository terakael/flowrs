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
/// For running tasks (end is None), calculates elapsed time from start to current time.
/// Returns "-" if start is None.
/// 
/// # Performance Note
/// This variant calls `now_utc()` internally. For better performance when formatting
/// multiple durations in the same render frame, use `format_duration_with_now()` instead
/// to cache the current time once per frame.
pub fn format_duration(start_date: Option<time::OffsetDateTime>, end_date: Option<time::OffsetDateTime>) -> String {
    let now = time::OffsetDateTime::now_utc();
    format_duration_with_now(start_date, end_date, now)
}

/// Format duration with a provided "now" timestamp for performance
/// 
/// When rendering multiple durations in the same frame, call `now_utc()` once and pass it
/// to this function to avoid repeated syscalls.
/// 
/// # Arguments
/// * `start_date` - Start time of the duration
/// * `end_date` - End time of the duration (None if still running)
/// * `now` - Current time to use for elapsed calculation
pub fn format_duration_with_now(
    start_date: Option<time::OffsetDateTime>,
    end_date: Option<time::OffsetDateTime>,
    now: time::OffsetDateTime,
) -> String {
    match (start_date, end_date) {
        (Some(start), Some(end)) => {
            let duration = end - start;
            let total_seconds = duration.whole_seconds();
            
            if total_seconds < 0 {
                // Data error: end is before start
                // This indicates a bug in Airflow API or data corruption
                log::error!(
                    "Invalid duration: end ({}) < start ({})",
                    end.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "Invalid".to_string()),
                    start.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "Invalid".to_string())
                );
                return "Error".to_string();
            }
            
            format_seconds(total_seconds)
        }
        (Some(start), None) => {
            // Genuinely running - calculate elapsed time from start to now
            let elapsed = now - start;
            let elapsed_seconds = elapsed.whole_seconds();
            
            if elapsed_seconds < 0 {
                // Start date is in the future - also a data error or clock skew
                log::warn!(
                    "Start date in future: start ({}), now ({})",
                    start.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "Invalid".to_string()),
                    now.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "Invalid".to_string())
                );
                return "Scheduled".to_string();
            }
            
            format_seconds(elapsed_seconds)
        }
        (None, _) => "-".to_string(),
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

/// Convert a UTC OffsetDateTime to a timezone specified by offset string
/// 
/// # Arguments
/// * `dt` - The UTC datetime to convert
/// * `offset_str` - Timezone offset in format "+HH:MM" or "-HH:MM" (e.g., "+09:00", "-05:00")
/// 
/// # Returns
/// * The datetime converted to the specified timezone, or original if offset is invalid
pub fn convert_to_timezone(dt: time::OffsetDateTime, offset_str: &str) -> time::OffsetDateTime {
    // Parse offset string like "+09:00" or "-05:00"
    let parts: Vec<&str> = offset_str.trim_start_matches('+').trim_start_matches('-').split(':').collect();
    if parts.len() != 2 {
        return dt; // Invalid format, return as-is
    }
    
    let hours: i8 = parts[0].parse().unwrap_or(0);
    let minutes: i8 = parts[1].parse().unwrap_or(0);
    let is_negative = offset_str.starts_with('-');
    
    let hours = if is_negative { -hours } else { hours };
    let minutes = if is_negative { -minutes } else { minutes };
    
    match time::UtcOffset::from_hms(hours, minutes, 0) {
        Ok(offset) => dt.to_offset(offset),
        Err(_) => dt, // Invalid offset, return as-is
    }
}

/// Safely truncate a string to a maximum number of characters, respecting UTF-8 boundaries
/// 
/// # Arguments
/// * `s` - The string to truncate
/// * `max_chars` - Maximum number of characters (not bytes)
/// 
/// # Returns
/// * Truncated string with "..." appended if truncation occurred
fn truncate_str(s: &str, max_chars: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max_chars {
        s.to_string()
    } else {
        format!("{}...", s.chars().take(max_chars).collect::<String>())
    }
}

/// Sanitize text for safe terminal display by removing control characters
/// 
/// Removes ASCII control characters that can cause rendering artifacts and scrolling issues.
/// Preserves newlines for multi-line display. Use this for detail views and multi-line text.
/// 
/// Removes:
/// - Tabs (\t)
/// - Carriage returns (\r)
/// - Other ASCII control characters (0x00-0x1F except \n)
/// 
/// Preserves:
/// - Newlines (\n)
/// - All Unicode characters (international text, emojis, etc.)
/// 
/// # Arguments
/// * `text` - The text to sanitize
/// 
/// # Returns
/// * Sanitized string safe for terminal display with newlines preserved
/// 
/// # Example
/// ```
/// let input = "line1\ttest\nline2\rwith\ttabs";
/// let output = sanitize_for_display(input);
/// assert_eq!(output, "line1test\nline2withtabs");
/// ```
pub fn sanitize_for_display(text: &str) -> String {
    sanitize_control_chars(text, true)
}

/// Sanitize text for single-line display by removing control characters
/// 
/// Removes ASCII control characters including newlines. Use this for table cells
/// and single-line display contexts where newlines should become spaces.
/// 
/// Removes:
/// - Tabs (\t)
/// - Carriage returns (\r)
/// - Newlines (\n) - replaced with spaces
/// - Other ASCII control characters (0x00-0x1F)
/// 
/// Preserves:
/// - All Unicode characters (international text, emojis, etc.)
/// 
/// # Arguments
/// * `text` - The text to sanitize
/// 
/// # Returns
/// * Sanitized single-line string safe for terminal display
/// 
/// # Example
/// ```
/// let input = "line1\ttest\nline2\rwith\ttabs";
/// let output = sanitize_for_inline_display(input);
/// assert_eq!(output, "line1test line2withtabs");
/// ```
pub fn sanitize_for_inline_display(text: &str) -> String {
    sanitize_control_chars(text, false)
}

/// Internal sanitization implementation
/// 
/// This function is critical for proper display in both table views and detail views.
/// Without sanitization, control characters can cause:
/// - Visual artifacts in table rows
/// - Incorrect line wrapping calculations
/// - Scrolling position misalignment
/// - Corrupted terminal buffer state
/// 
/// # Arguments
/// * `text` - The text to sanitize
/// * `preserve_newlines` - If true, keeps \n; if false, replaces with space
/// 
/// # Returns
/// * Sanitized string safe for terminal display
fn sanitize_control_chars(text: &str, preserve_newlines: bool) -> String {
    text.chars()
        .filter_map(|c| {
            // Keep regular characters (>= 0x20) and extended ASCII/Unicode
            if c >= ' ' {
                Some(c)
            // Handle newlines based on parameter
            } else if c == '\n' {
                if preserve_newlines {
                    Some('\n')
                } else {
                    Some(' ')
                }
            // Strip all other ASCII control characters (0x00-0x1F)
            // This includes: \t, \r, \x00-\x08, \x0B, \x0C, \x0E-\x1F
            } else {
                None
            }
        })
        .collect()
}

/// Format and highlight JSON with optional minification
/// 
/// This helper consolidates JSON parsing, formatting, and highlighting logic
/// used across table and detail views. It handles both valid and invalid JSON,
/// providing appropriate fallbacks.
/// 
/// Control characters in the input are sanitized before processing to prevent
/// display artifacts and scrolling issues.
/// 
/// # Arguments
/// * `value` - The string value to process
/// * `minify` - If true, minifies valid JSON; if false, preserves formatting
/// * `max_chars` - Optional maximum characters for truncation (for table views)
/// 
/// # Returns
/// * Tuple of (formatted lines, is_valid_json)
pub fn format_and_highlight_json(
    value: &str,
    minify: bool,
    max_chars: Option<usize>,
) -> (Vec<Line<'static>>, bool) {
    // Sanitize control characters FIRST to prevent display issues
    // For minified view: replace newlines with spaces (single-line display)
    // For formatted view: preserve newlines for proper multi-line display
    let sanitized = if minify {
        sanitize_for_inline_display(value)
    } else {
        sanitize_for_display(value)
    };
    
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&sanitized) {
        // Valid JSON - format according to preferences
        let json_str = if minify {
            serde_json::to_string(&json_value)
                .expect("serializing parsed JSON should never fail")
        } else {
            serde_json::to_string_pretty(&json_value)
                .expect("serializing parsed JSON should never fail")
        };
        
        // Apply truncation if requested (for table views)
        let display_str = if let Some(max) = max_chars {
            truncate_str(&json_str, max)
        } else {
            json_str
        };
        
        // Highlight and return
        let lines = if minify {
            vec![Line::from(highlight_json_inline(&display_str))]
        } else {
            highlight_json(&display_str)
        };
        
        (lines, true)
    } else {
        // Not valid JSON - display as plain text (already sanitized)
        let display_str = if let Some(max) = max_chars {
            truncate_str(&sanitized, max)
        } else {
            sanitized
        };
        
        let lines = if minify {
            vec![Line::from(display_str)]
        } else {
            display_str.lines().map(|line| Line::from(line.to_string())).collect()
        };
        
        (lines, false)
    }
}

/// Simple, fast JSON colorization for terminal display
/// 
/// Highlights JSON strings in bright green while leaving punctuation,
/// numbers, and keywords in the default cream color. This lightweight
/// parser handles both minified and formatted JSON efficiently without
/// the overhead of full syntax tokenization.
/// 
/// ## Performance Rationale
/// 
/// This custom parser was chosen over syntect (used for Python highlighting)
/// for performance reasons:
/// - syntect requires loading syntax definitions (~10-50ms overhead)
/// - Table views render on every frame, making syntect's overhead noticeable
/// - This parser uses simple character iteration with minimal allocations
/// - Sufficient visual distinction (green strings) without complexity
/// 
/// Trade-off: Less rich highlighting than syntect, but 10-100x faster for
/// inline rendering where many rows are processed per frame.
/// 
/// # Arguments
/// * `json_str` - The JSON string to highlight
/// 
/// # Returns
/// * Vector of Spans with colorized JSON
fn colorize_json_line(json_str: &str) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut escape_next = false;
    let mut string_content = String::new();
    
    let string_color = BRIGHT_GREEN;
    let default_color = FOREGROUND;
    
    for ch in json_str.chars() {
        if escape_next {
            if in_string {
                string_content.push(ch);
            } else {
                current.push(ch);
            }
            escape_next = false;
            continue;
        }
        
        if ch == '\\' {
            escape_next = true;
            if in_string {
                string_content.push(ch);
            } else {
                current.push(ch);
            }
            continue;
        }
        
        if ch == '"' {
            if in_string {
                // End of string - emit the string with quotes
                if !current.is_empty() {
                    spans.push(Span::styled(current.clone(), Style::default().fg(default_color)));
                    current.clear();
                }
                spans.push(Span::styled(
                    format!("\"{}\"", string_content),
                    Style::default().fg(string_color),
                ));
                string_content.clear();
                in_string = false;
            } else {
                // Start of string - emit any accumulated non-string content
                if !current.is_empty() {
                    spans.push(Span::styled(current.clone(), Style::default().fg(default_color)));
                    current.clear();
                }
                in_string = true;
            }
        } else {
            if in_string {
                string_content.push(ch);
            } else {
                current.push(ch);
            }
        }
    }
    
    // Emit any remaining content
    if !current.is_empty() {
        spans.push(Span::styled(current, Style::default().fg(default_color)));
    }
    if in_string {
        // Unclosed string - still emit it with color
        spans.push(Span::styled(
            format!("\"{}\"", string_content),
            Style::default().fg(string_color),
        ));
    }
    
    spans
}

/// Highlights JSON text with simple colorization
/// 
/// Processes multi-line JSON (e.g., formatted/pretty-printed JSON).
/// Strings are highlighted in bright green, everything else uses the
/// default cream foreground color.
/// 
/// # Arguments
/// * `json_str` - The JSON string to highlight
/// 
/// # Returns
/// * Vector of Lines with colorized spans
pub fn highlight_json(json_str: &str) -> Vec<Line<'static>> {
    json_str.lines()
        .map(|line| Line::from(colorize_json_line(line)))
        .collect()
}

/// Highlights a single-line JSON string (for table previews)
/// 
/// Optimized for inline display in tables where JSON is typically
/// minified or truncated. Uses the same fast parser as `highlight_json()`
/// for consistency.
/// 
/// # Arguments
/// * `json_str` - The JSON string to highlight (typically single-line or truncated)
/// 
/// # Returns
/// * Vector of Spans with colorized JSON
pub fn highlight_json_inline(json_str: &str) -> Vec<Span<'static>> {
    colorize_json_line(json_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_tabs() {
        let input = "hello\tworld";
        let result = sanitize_control_chars(input, true);
        assert_eq!(result, "helloworld", "Tabs should be stripped");
    }

    #[test]
    fn test_sanitize_carriage_returns() {
        let input = "line1\r\nline2\r\nline3";
        let result = sanitize_control_chars(input, true);
        assert_eq!(result, "line1\nline2\nline3", "CR should be stripped, LF preserved");
    }

    #[test]
    fn test_sanitize_carriage_returns_no_newlines() {
        let input = "line1\r\nline2\r\nline3";
        let result = sanitize_control_chars(input, false);
        assert_eq!(result, "line1 line2 line3", "CR and LF should become spaces");
    }

    #[test]
    fn test_sanitize_mixed_control_chars() {
        // Include various control characters: \t, \r, \x00, \x01, \x0C (form feed)
        let input = "hello\tworld\r\ntest\x00data\x01more\x0Cstuff";
        let result = sanitize_control_chars(input, true);
        assert_eq!(result, "helloworld\ntestdatamorestuff", "All control chars except LF stripped");
    }

    #[test]
    fn test_sanitize_preserve_newlines() {
        let input = "line1\nline2\nline3";
        let result = sanitize_control_chars(input, true);
        assert_eq!(result, "line1\nline2\nline3", "Newlines should be preserved");
    }

    #[test]
    fn test_sanitize_replace_newlines() {
        let input = "line1\nline2\nline3";
        let result = sanitize_control_chars(input, false);
        assert_eq!(result, "line1 line2 line3", "Newlines should become spaces");
    }

    #[test]
    fn test_sanitize_no_control_chars() {
        let input = "hello world 123";
        let result = sanitize_control_chars(input, true);
        assert_eq!(result, "hello world 123", "Regular text should be unchanged");
    }

    #[test]
    fn test_sanitize_empty_string() {
        let input = "";
        let result = sanitize_control_chars(input, true);
        assert_eq!(result, "", "Empty string should remain empty");
    }

    #[test]
    fn test_sanitize_only_control_chars() {
        let input = "\t\r\n\x00\x01";
        let result = sanitize_control_chars(input, true);
        assert_eq!(result, "\n", "Only newline should remain");
    }

    #[test]
    fn test_sanitize_unicode_preserved() {
        let input = "hello 世界 🌍 test";
        let result = sanitize_control_chars(input, true);
        assert_eq!(result, "hello 世界 🌍 test", "Unicode should be preserved");
    }

    #[test]
    fn test_format_and_highlight_json_with_tabs() {
        // Simulate the real-world case: Valid JSON with tabs and \r\n
        let input = "[\r\n\t{\r\n\t\t\"key\": \"value\"\r\n\t}\r\n]";
        let (lines, is_json) = format_and_highlight_json(input, true, None);
        
        // Should be sanitized and parsed as JSON (becomes valid after sanitization)
        assert!(is_json, "Should be recognized as JSON after sanitization");
        assert_eq!(lines.len(), 1, "Minified should be single line");
        
        // The rendered output should not contain control characters
        let rendered = lines[0].spans.iter().map(|s| s.content.as_ref()).collect::<String>();
        assert!(!rendered.contains('\t'), "Rendered output should not contain tabs");
        assert!(!rendered.contains('\r'), "Rendered output should not contain CR");
    }

    #[test]
    fn test_format_and_highlight_json_non_json_with_tabs() {
        // Non-JSON text with tabs
        let input = "hello\tworld\ttest";
        let (lines, is_json) = format_and_highlight_json(input, true, None);
        
        assert!(!is_json, "Should not be recognized as JSON");
        assert_eq!(lines.len(), 1, "Should be single line");
        
        let rendered = lines[0].spans.iter().map(|s| s.content.as_ref()).collect::<String>();
        assert_eq!(rendered, "helloworldtest", "Tabs should be stripped");
    }

    #[test]
    fn test_format_and_highlight_json_multiline_with_tabs() {
        let input = "line1\ttest\nline2\twith\ttabs";
        let (lines, is_json) = format_and_highlight_json(input, false, None);
        
        assert!(!is_json, "Should not be JSON");
        assert_eq!(lines.len(), 2, "Should be 2 lines");
        
        // Check each line has tabs stripped
        let line1 = lines[0].spans.iter().map(|s| s.content.as_ref()).collect::<String>();
        let line2 = lines[1].spans.iter().map(|s| s.content.as_ref()).collect::<String>();
        
        assert_eq!(line1, "line1test", "First line should have tabs stripped");
        assert_eq!(line2, "line2withtabs", "Second line should have tabs stripped");
    }
}
