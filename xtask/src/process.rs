use std::env;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;

/// The cargo that launched xtask, so child commands keep its toolchain.
pub(crate) fn cargo() -> Command {
    Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
}

pub(crate) fn cargo_pgrx() -> Command {
    let mut command = cargo();
    command.arg("pgrx");
    command
}

pub(crate) fn prepend_path_env(
    command: &mut Command,
    directory: &Path,
) -> Result<(), String> {
    let mut paths = Vec::new();
    paths.push(directory.to_path_buf());

    if let Some(current_path) = env::var_os("PATH") {
        paths.extend(env::split_paths(&current_path));
    }

    let joined_path = env::join_paths(paths).map_err(|error| {
        format!(
            "failed to build PATH with {} prepended: {error}",
            directory.display()
        )
    })?;
    command.env("PATH", joined_path);

    Ok(())
}

pub(crate) fn run_command(command: &mut Command) -> Result<(), String> {
    println!("$ {}", display_command(command));
    let status = command.status().map_err(|error| {
        format!("failed to run {}: {error}", display_command(command))
    })?;

    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "{} exited with status {status}",
            display_command(command)
        ))
    }
}

fn display_command(command: &Command) -> String {
    let mut parts = Vec::new();
    parts.push(shell_quote(command.get_program()));
    parts.extend(command.get_args().map(shell_quote));
    parts.join(" ")
}

fn shell_quote(value: &OsStr) -> String {
    let value = value.to_string_lossy();
    if value.chars().all(|c| {
        c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '=')
    }) {
        value.into_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}
