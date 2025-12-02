use std::sync::LazyLock;

use crate::app::model::popup::commands_help::{Command, CommandPopUp, DefaultCommands};

pub static TASK_COMMANDS: LazyLock<Vec<Command<'static>>> = LazyLock::new(|| {
    let mut commands = vec![
        Command {
            name: "Clear",
            key_binding: "c",
            description: "Clear a task instance",
        },
        Command {
            name: "Mark",
            key_binding: "m",
            description: "Mark a task instance",
        },
        Command {
            name: "Filter",
            key_binding: "/",
            description: "Filter task instances",
        },
    ];

    commands.append(&mut DefaultCommands::new().0);
    commands
});

pub fn create_task_command_popup() -> CommandPopUp<'static> {
    CommandPopUp::new("Task Commands".into(), TASK_COMMANDS.clone())
}
