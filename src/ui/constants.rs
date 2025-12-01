use ratatui::style::{Color, Modifier, Style};

// Theme colors matching system theme
pub const BACKGROUND: Color = Color::Rgb(0x25, 0x23, 0x21);      // #252321
pub const FOREGROUND: Color = Color::Rgb(0xd3, 0xc6, 0xaa);      // #d3c6aa
pub const BLACK: Color = Color::Rgb(0x34, 0x40, 0x45);           // #344045
pub const RED: Color = Color::Rgb(0xcf, 0x6a, 0x6d);             // #cf6a6d
pub const GREEN: Color = Color::Rgb(0x96, 0xad, 0x73);           // #96ad73
pub const YELLOW: Color = Color::Rgb(0xc6, 0xab, 0x73);          // #c6ab73
pub const BLUE: Color = Color::Rgb(0x72, 0xa9, 0xa2);            // #72a9a2
pub const MAGENTA: Color = Color::Rgb(0xc1, 0x8a, 0xa5);         // #c18aa5
pub const CYAN: Color = Color::Rgb(0x76, 0xad, 0x84);            // #76ad84
pub const WHITE: Color = Color::Rgb(0xbf, 0xb4, 0x9d);           // #bfb49d

pub const BRIGHT_BLACK: Color = Color::Rgb(0x5e, 0x68, 0x60);    // #5e6860
pub const BRIGHT_RED: Color = Color::Rgb(0xa8, 0x5a, 0x5d);      // #a85a5d
pub const BRIGHT_GREEN: Color = Color::Rgb(0x78, 0x89, 0x5c);    // #78895c
pub const BRIGHT_YELLOW: Color = Color::Rgb(0x9e, 0x88, 0x5a);   // #9e885a
pub const BRIGHT_BLUE: Color = Color::Rgb(0x5a, 0x85, 0x80);     // #5a8580
pub const BRIGHT_MAGENTA: Color = Color::Rgb(0x9a, 0x6d, 0x83);  // #9a6d83
pub const BRIGHT_CYAN: Color = Color::Rgb(0x5d, 0x8a, 0x68);     // #5d8a68
pub const BRIGHT_WHITE: Color = Color::Rgb(0x99, 0x8e, 0x79);    // #998e79

// Legacy constant for compatibility
pub const DM_RGB: Color = FOREGROUND;

pub const DEFAULT_STYLE: Style = Style {
    fg: Some(FOREGROUND),
    bg: Some(BACKGROUND),
    underline_color: None,
    add_modifier: Modifier::empty(),
    sub_modifier: Modifier::empty(),
};

pub const SELECTED_STYLE: Style = Style {
    fg: Some(Color::Black),
    bg: Some(GREEN),
    underline_color: None,
    add_modifier: Modifier::BOLD,
    sub_modifier: Modifier::empty(),
};

pub const HEADER_STYLE: Style = Style {
    fg: Some(GREEN),
    bg: Some(BACKGROUND),
    underline_color: None,
    add_modifier: Modifier::BOLD,
    sub_modifier: Modifier::empty(),
};

pub const ALTERNATING_ROW_COLOR: Color = Color::Rgb(0x2d, 0x2b, 0x29); // Slightly lighter than background
pub const MARKED_COLOR: Color = YELLOW; // Use theme yellow for marked items

pub const ASCII_LOGO: &str = include_str!("logo/logo.ascii");

pub const ROTATING_LOGO: [&str; 16] = [
    include_str!("../../image/rotation/ascii/0.ascii"),
    include_str!("../../image/rotation/ascii/1.ascii"),
    include_str!("../../image/rotation/ascii/2.ascii"),
    include_str!("../../image/rotation/ascii/3.ascii"),
    include_str!("../../image/rotation/ascii/4.ascii"),
    include_str!("../../image/rotation/ascii/5.ascii"),
    include_str!("../../image/rotation/ascii/6.ascii"),
    include_str!("../../image/rotation/ascii/7.ascii"),
    include_str!("../../image/rotation/ascii/8.ascii"),
    include_str!("../../image/rotation/ascii/9.ascii"),
    include_str!("../../image/rotation/ascii/10.ascii"),
    include_str!("../../image/rotation/ascii/11.ascii"),
    include_str!("../../image/rotation/ascii/12.ascii"),
    include_str!("../../image/rotation/ascii/13.ascii"),
    include_str!("../../image/rotation/ascii/14.ascii"),
    include_str!("../../image/rotation/ascii/15.ascii"),
];

pub enum AirflowStateColor {
    Success,
    Failed,
    Running,
    Queued,
    UpForRetry,
    UpForReschedule,
    Skipped,
    UpstreamFailed,
    None,
}

impl From<AirflowStateColor> for Color {
    fn from(state: AirflowStateColor) -> Self {
        match state {
            AirflowStateColor::Success => GREEN,
            AirflowStateColor::Failed => RED,
            AirflowStateColor::Running => BRIGHT_GREEN,
            AirflowStateColor::Queued => BRIGHT_BLACK,
            AirflowStateColor::UpForRetry => YELLOW,
            AirflowStateColor::UpForReschedule => CYAN,
            AirflowStateColor::Skipped => MAGENTA,
            AirflowStateColor::UpstreamFailed => BRIGHT_YELLOW,
            AirflowStateColor::None => Color::Reset,
        }
    }
}
