use std::sync::LazyLock;

use crate::app::model::popup::commands_help::{Command, CommandPopUp, DefaultCommands};

pub static DAG_COMMAND_POP_UP: LazyLock<CommandPopUp> = LazyLock::new(|| {
    let mut commands = vec![
        Command {
            name: "Toggle visibility",
            key_binding: "p",
            description: "Toggle showing paused DAGs",
        },
        Command {
            name: "Pause/Unpause",
            key_binding: "Shift+P",
            description: "Pause or unpause selected DAG",
        },
    ];
    commands.append(&mut DefaultCommands::new().0);
    CommandPopUp {
        title: "DAG Commands".into(),
        commands,
    }
});
