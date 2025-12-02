use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::TableState;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Direction of sorting
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
    None,
}

impl SortDirection {
    pub fn cycle(&self) -> Self {
        match self {
            Self::None => Self::Ascending,
            Self::Ascending => Self::Descending,
            Self::Descending => Self::None,
        }
    }

    pub fn indicator(&self) -> &'static str {
        match self {
            Self::Ascending => "▲",
            Self::Descending => "▼",
            Self::None => "",
        }
    }
}

/// Column metadata auto-generated from headers
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub sort_key: char,
    pub key_position: usize,
}

/// Optional trait for types that need custom sorting logic
/// If not implemented, falls back to string comparison
pub trait CustomSort {
    /// Return a custom comparator for a specific column index
    /// If None, uses default alphabetical sort
    fn comparator(column_index: usize) -> Option<fn(&Self, &Self) -> Ordering> {
        let _ = column_index;
        None
    }
    
    /// Extract the string value for a column (for default sorting)
    fn column_value(&self, column_index: usize) -> String;
}

/// A sortable table that automatically handles column sorting
pub struct SortableTable<T> {
    pub state: TableState,
    pub items: Vec<T>,
    columns: Vec<ColumnInfo>,
    sort_column: Option<usize>,
    sort_direction: SortDirection,
}

impl<T> SortableTable<T> {
    /// Create a new sortable table from column headers
    /// Automatically assigns sort keys to columns, avoiding reserved keys
    /// 
    /// # Arguments
    /// * `headers` - Column header names
    /// * `items` - Initial table data
    /// * `reserved_keys` - Keys to avoid (e.g., ['j', 'k', 'g', 'G'] for navigation)
    pub fn new(headers: &[impl AsRef<str>], items: Vec<T>, reserved_keys: &[char]) -> Self {
        let columns = assign_sort_keys(headers, reserved_keys);
        
        Self {
            state: TableState::default(),
            items,
            columns,
            sort_column: None,
            sort_direction: SortDirection::None,
        }
    }
    
    /// Get column info (for rendering headers with sort keys)
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }
    
    /// Get current sort state
    pub fn sort_state(&self) -> Option<(usize, &SortDirection)> {
        self.sort_column.map(|col| (col, &self.sort_direction))
    }
    
    /// Render headers with sort keys highlighted
    pub fn render_headers(&self, header_style: Style, red_color: ratatui::style::Color) -> Vec<Line<'static>> {
        self.columns.iter().enumerate().map(|(idx, col)| {
            let mut spans = vec![];
            let name = &col.name;
            
            // Highlight the sort key character in red
            if col.key_position < name.len() {
                let (before, rest) = name.split_at(col.key_position);
                let key_len = col.sort_key.len_utf8();
                
                if col.key_position + key_len <= name.len() {
                    let (key_str, after) = rest.split_at(key_len);
                    
                    if !before.is_empty() {
                        spans.push(Span::styled(before.to_string(), header_style));
                    }
                    spans.push(Span::styled(
                        key_str.to_string(),
                        header_style.fg(red_color),
                    ));
                    if !after.is_empty() {
                        spans.push(Span::styled(after.to_string(), header_style));
                    }
                } else {
                    spans.push(Span::styled(name.to_string(), header_style));
                }
            } else {
                spans.push(Span::styled(name.to_string(), header_style));
            }
            
            // Add sort indicator if this column is sorted
            if let Some((sort_col, direction)) = self.sort_state() {
                if sort_col == idx {
                    let indicator = direction.indicator();
                    if !indicator.is_empty() {
                        spans.push(Span::raw(" "));
                        spans.push(Span::styled(indicator.to_string(), header_style));
                    }
                }
            }
            
            Line::from(spans).left_aligned()
        }).collect()
    }
    
    /// Handle a character key press - returns true if it was a sort key
    pub fn handle_key(&mut self, key: char) -> bool
    where
        T: CustomSort,
    {
        // Find column with this sort key
        if let Some(col_idx) = self.columns.iter().position(|c| c.sort_key == key) {
            // Cycle sort direction
            if self.sort_column == Some(col_idx) {
                self.sort_direction = self.sort_direction.cycle();
                if self.sort_direction == SortDirection::None {
                    self.sort_column = None;
                }
            } else {
                self.sort_column = Some(col_idx);
                self.sort_direction = SortDirection::Ascending;
            }
            
            // Apply sort
            self.apply_sort();
            
            true
        } else {
            false
        }
    }
    
    /// Reapply the current sort to items (call this after updating items externally)
    pub fn reapply_sort(&mut self)
    where
        T: CustomSort,
    {
        self.apply_sort();
    }
    
    /// Apply current sort configuration
    fn apply_sort(&mut self)
    where
        T: CustomSort,
    {
        let Some(col_idx) = self.sort_column else {
            return;
        };
        
        let direction = self.sort_direction;
        if direction == SortDirection::None {
            return;
        }
        
        // Check for custom comparator
        if let Some(comparator) = T::comparator(col_idx) {
            self.items.sort_by(|a, b| {
                let ord = comparator(a, b);
                match direction {
                    SortDirection::Ascending => ord,
                    SortDirection::Descending => ord.reverse(),
                    SortDirection::None => Ordering::Equal,
                }
            });
        } else {
            // Use default string comparison
            self.items.sort_by(|a, b| {
                let val_a = a.column_value(col_idx);
                let val_b = b.column_value(col_idx);
                let ord = val_a.to_lowercase().cmp(&val_b.to_lowercase());
                match direction {
                    SortDirection::Ascending => ord,
                    SortDirection::Descending => ord.reverse(),
                    SortDirection::None => Ordering::Equal,
                }
            });
        }
    }
    
    /// Scroll by delta rows (reused from StatefulTable)
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

/// Automatically assign sort keys to columns, avoiding conflicts and reserved keys
/// Algorithm:
/// 1. Try first letter of each column (lowercase)
/// 2. If conflict or reserved, try subsequent letters
/// 3. If no letters available, use uppercase letters
/// 4. Last resort: use numbers
fn assign_sort_keys(headers: &[impl AsRef<str>], reserved_keys: &[char]) -> Vec<ColumnInfo> {
    let mut columns = Vec::new();
    let mut used_keys: HashMap<char, String> = HashMap::new();
    let mut pending: Vec<(usize, String)> = Vec::new();
    
    // Mark reserved keys as used (store both cases to prevent conflicts)
    for &key in reserved_keys {
        used_keys.insert(key.to_ascii_lowercase(), String::from("<reserved>"));
        if key.is_ascii_alphabetic() {
            used_keys.insert(key.to_ascii_uppercase(), String::from("<reserved>"));
        }
    }
    
    // First pass: try first letter of each column
    for (idx, header) in headers.iter().enumerate() {
        let name = header.as_ref().to_lowercase();
        
        if let Some(first_char) = name.chars().next() {
            if !used_keys.contains_key(&first_char) {
                used_keys.insert(first_char, name.clone());
                columns.push((idx, ColumnInfo {
                    name: name.clone(),
                    sort_key: first_char,
                    key_position: 0,
                }));
            } else {
                pending.push((idx, name));
            }
        }
    }
    
    // Second pass: for conflicts, try subsequent letters
    for (idx, name) in pending {
        let mut found = false;
        
        // Try subsequent lowercase letters
        for (pos, ch) in name.chars().enumerate() {
            if !used_keys.contains_key(&ch) {
                used_keys.insert(ch, name.clone());
                columns.push((idx, ColumnInfo {
                    name: name.clone(),
                    sort_key: ch,
                    key_position: pos,
                }));
                found = true;
                break;
            }
        }
        
        // Third pass: try uppercase letters
        if !found {
            for (pos, ch) in name.chars().enumerate() {
                let upper = ch.to_ascii_uppercase();
                if upper != ch && !used_keys.contains_key(&upper) {
                    used_keys.insert(upper, name.clone());
                    columns.push((idx, ColumnInfo {
                        name: name.clone(),
                        sort_key: upper,
                        key_position: pos,
                    }));
                    found = true;
                    break;
                }
            }
        }
        
        // Last resort: use number if no letter available
        if !found {
            let num_key = char::from_digit((idx % 10) as u32, 10).unwrap();
            columns.push((idx, ColumnInfo {
                name: name.clone(),
                sort_key: num_key,
                key_position: 0,
            }));
        }
    }
    
    // Sort by original index to maintain column order
    columns.sort_by_key(|(idx, _)| *idx);
    columns.into_iter().map(|(_, col)| col).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_sort_keys_no_conflicts() {
        let headers = ["name", "age", "city"];
        let columns = assign_sort_keys(&headers, &[]);
        
        assert_eq!(columns[0].sort_key, 'n');
        assert_eq!(columns[1].sort_key, 'a');
        assert_eq!(columns[2].sort_key, 'c');
    }

    #[test]
    fn test_assign_sort_keys_with_conflicts() {
        let headers = ["state", "schedule", "status"];
        let columns = assign_sort_keys(&headers, &[]);
        
        // First gets 's', others get subsequent letters
        assert_eq!(columns[0].sort_key, 's');
        assert_eq!(columns[1].sort_key, 'c'); // s[c]hedule
        assert_eq!(columns[2].sort_key, 't'); // s[t]atus
    }
    
    #[test]
    fn test_assign_sort_keys_with_reserved() {
        let headers = ["key", "name", "jump"];
        let columns = assign_sort_keys(&headers, &['k', 'n', 'j']);
        
        // 'k', 'n', 'j' are reserved, should use next available letters
        assert_eq!(columns[0].sort_key, 'e'); // k[e]y
        assert_eq!(columns[1].sort_key, 'a'); // n[a]me
        assert_eq!(columns[2].sort_key, 'u'); // j[u]mp
    }
    
    #[test]
    fn test_assign_sort_keys_uppercase_lowercase_collision() {
        let headers = ["go", "game", "great"];
        // Reserve 'g' (lowercase) - should also block 'G' (uppercase)
        let columns = assign_sort_keys(&headers, &['g', 'G']);
        
        // Since 'g' and 'G' are both reserved, should use next available letters
        assert_eq!(columns[0].sort_key, 'o'); // g[o]
        assert_eq!(columns[1].sort_key, 'a'); // g[a]me
        assert_eq!(columns[2].sort_key, 'r'); // g[r]eat
    }
}
