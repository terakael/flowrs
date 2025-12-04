use std::sync::LazyLock;

use crate::app::model::popup::commands_help::{Command, CommandPopUp, DefaultCommands};

pub static DAGRUN_COMMANDS: LazyLock<Vec<Command<'static>>> = LazyLock::new(|| {
    let mut commands = vec![
        Command {
            name: "Clear",
            key_binding: "c",
            description: "Clear a DAG run",
        },
        Command {
            name: "Show",
            key_binding: "v",
            description: "Show DAG code (press 'e' in viewer to open in editor)",
        },
        Command {
            name: "Open in Editor",
            key_binding: "e",
            description: "Open DAG code directly in editor",
        },
        Command {
            name: "Mark",
            key_binding: "m",
            description: "Mark a DAG run",
        },
        Command {
            name: "Mark multiple",
            key_binding: "M",
            description: "Mark multiple DAG runs",
        },
        Command {
            name: "Trigger",
            key_binding: "t",
            description: "Trigger a DAG run",
        },
        Command {
            name: "Focus Info",
            key_binding: "Shift+K",
            description: "Focus Info section (up)",
        },
        Command {
            name: "Focus Table",
            key_binding: "Shift+J",
            description: "Focus DAGRuns table (down)",
        },
        Command {
            name: "Next Page",
            key_binding: "]]",
            description: "Navigate to next page",
        },
        Command {
            name: "Previous Page",
            key_binding: "[[",
            description: "Navigate to previous page",
        },
    ];
    commands.append(&mut DefaultCommands::new().0);
    commands
});

pub fn create_dagrun_command_popup() -> CommandPopUp<'static> {
    CommandPopUp::new("DAG Run Commands".into(), DAGRUN_COMMANDS.clone())
}
