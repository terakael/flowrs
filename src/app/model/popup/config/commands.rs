use std::sync::LazyLock;

use crate::app::model::popup::commands_help::{Command, CommandPopUp, DefaultCommands};

pub static CONFIG_COMMANDS: LazyLock<Vec<Command<'static>>> = LazyLock::new(|| {
    let mut commands = vec![Command {
        name: "Open",
        key_binding: "o",
        description: "Open Airflow Web UI",
    }];
    commands.append(&mut DefaultCommands::new().0);
    commands
});

pub fn create_config_command_popup() -> CommandPopUp<'static> {
    CommandPopUp::new("Config Commands".into(), CONFIG_COMMANDS.clone())
}
