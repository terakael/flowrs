use std::sync::LazyLock;

use crate::app::model::popup::commands_help::{Command, CommandPopUp, DefaultCommands};

pub static LOG_COMMANDS: LazyLock<Vec<Command<'static>>> = LazyLock::new(|| {
    let mut commands = vec![
        Command {
            name: "Open in Editor",
            key_binding: "e",
            description: "Open current log in external editor (requires VISUAL or EDITOR env var)",
        },
        Command {
            name: "Load More",
            key_binding: "m",
            description: "Manually load next chunk of logs",
        },
        Command {
            name: "Next Attempt",
            key_binding: "l / Right",
            description: "View next attempt (cycles through available attempts)",
        },
        Command {
            name: "Previous Attempt",
            key_binding: "h / Left",
            description: "View previous attempt (cycles through available attempts)",
        },
        Command {
            name: "Jump to Top",
            key_binding: "gg",
            description: "Jump to the top of logs",
        },
        Command {
            name: "Jump to Bottom",
            key_binding: "G",
            description: "Jump to the bottom of logs",
        },
        Command {
            name: "Scroll Horizontally",
            key_binding: "Shift+H / Shift+L",
            description: "Scroll left/right for long log lines",
        },
        Command {
            name: "Filter by Level",
            key_binding: "1-5",
            description: "Filter logs by minimum level (1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL)",
        },
    ];
    commands.append(&mut DefaultCommands::new().0);
    commands
});

pub fn create_log_command_popup() -> CommandPopUp<'static> {
    CommandPopUp::new("Log Commands".into(), LOG_COMMANDS.clone())
}
