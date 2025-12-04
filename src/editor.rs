use std::env;
use std::path::Path;
use std::process::Command;
use anyhow::{Result, bail};

// Allow buffered events to drain before handing terminal to editor
const TERMINAL_SETTLE_DELAY_MS: u64 = 50;

/// Get the user's preferred editor from environment variables
/// Checks VISUAL first, then EDITOR
pub fn get_editor_command() -> Result<String> {
    env::var("VISUAL")
        .or_else(|_| env::var("EDITOR"))
        .map_err(|_| anyhow::anyhow!(
            "No editor configured.\nPlease set VISUAL or EDITOR environment variable.\nExample: export EDITOR=nvim"
        ))
}

/// Open a file in the user's editor, with proper terminal suspension
/// This function:
/// 1. Saves terminal state with stty
/// 2. Restores the terminal to normal mode (disables raw mode, exits alternate screen)
/// 3. Spawns the editor with full terminal control
/// 4. Restores terminal state with stty
/// 5. Re-initializes the terminal after editor exits
/// 
/// Returns Ok(()) when editor closes successfully
pub fn open_in_editor_with_suspend<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    filepath: &Path,
) -> Result<()> {
    use crossterm::{
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
        cursor::{Hide, Show},
    };
    use std::io::stdout;
    
    let editor = get_editor_command()?;
    
    log::info!("Suspending terminal and opening file in editor: {} {}", editor, filepath.display());
    
    // Step 1: Save current terminal settings using stty
    // This captures ALL terminal state (echo, special chars, etc.)
    let stty_output = Command::new("stty")
        .arg("-g")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        });
    
    if let Some(ref settings) = stty_output {
        log::debug!("Saved terminal settings: {}", settings.trim());
    }
    
    // Step 2: Leave alternate screen and show cursor
    execute!(stdout(), LeaveAlternateScreen, Show)?;
    
    // Step 3: Disable raw mode to allow editor to read input normally
    disable_raw_mode()?;
    
    // Step 4: Small delay to let any buffered events drain
    std::thread::sleep(std::time::Duration::from_millis(TERMINAL_SETTLE_DELAY_MS));
    
    // Step 5: Spawn editor with inherited stdio so it can take over the terminal
    // Using shell execution to support complex editor commands (e.g., "code --wait")
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!("{} \"{}\"", editor, filepath.display()))
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()?;
    
    // Step 6: Restore terminal settings using stty if we saved them
    if let Some(settings) = stty_output {
        let _ = Command::new("stty")
            .arg(settings.trim())
            .status();
        log::debug!("Restored terminal settings");
    }
    
    // Step 7: Re-enable raw mode
    enable_raw_mode()?;
    
    // Step 8: Re-enter alternate screen and hide cursor
    execute!(stdout(), EnterAlternateScreen, Hide)?;
    
    // Step 9: Clear and redraw the terminal
    terminal.clear()?;
    
    if !status.success() {
        bail!("Editor exited with non-zero status: {}", status);
    }
    
    log::info!("Editor closed successfully, terminal restored");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_get_editor_command_visual() {
        env::set_var("VISUAL", "nvim");
        env::remove_var("EDITOR");
        assert_eq!(get_editor_command().unwrap(), "nvim");
    }

    #[test]
    fn test_get_editor_command_editor() {
        env::remove_var("VISUAL");
        env::set_var("EDITOR", "vim");
        assert_eq!(get_editor_command().unwrap(), "vim");
    }

    #[test]
    fn test_get_editor_command_visual_takes_precedence() {
        env::set_var("VISUAL", "nvim");
        env::set_var("EDITOR", "vim");
        assert_eq!(get_editor_command().unwrap(), "nvim");
    }

    #[test]
    fn test_get_editor_command_none_set() {
        env::remove_var("VISUAL");
        env::remove_var("EDITOR");
        assert!(get_editor_command().is_err());
    }
}
