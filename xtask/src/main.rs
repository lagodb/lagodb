use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

const EXTENSION_PACKAGE: &str = "pg-iceberg-am";
const EXTENSION_NAME: &str = "pg_iceberg_am";
const DEFAULT_ISOLATION_SPECS: &[&str] = &[
    "read_visibility",
    "cas_retry_stress",
    "savepoint_concurrent",
];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args_os().skip(1);
    match args.next().as_deref() {
        Some(command) if command == OsStr::new("isolation") => {
            let pg_version = args
                .next()
                .ok_or_else(|| usage_error("missing PostgreSQL version"))?;
            let specs: Vec<OsString> = args.collect();
            run_isolation(&pg_version, &specs)
        }
        Some(command) => Err(usage_error(&format!(
            "unknown command '{}'",
            command.to_string_lossy()
        ))),
        None => Err(usage_error("missing command")),
    }
}

fn usage_error(message: &str) -> String {
    format!(
        "{message}\n\nusage:\n  cargo xtask isolation <pg-version> [spec ...]\n\nexamples:\n  cargo xtask isolation pg17\n  cargo xtask isolation pg17 cas_retry_stress"
    )
}

fn run_isolation(pg_version: &OsStr, specs: &[OsString]) -> Result<(), String> {
    let workspace = workspace_root();
    let tests_dir = workspace.join("pg-iceberg-am/tests/isolation");
    let target_dir = workspace
        .join("target/isolation")
        .join(pg_version.to_string_lossy().as_ref());
    let output_dir = target_dir.join("output_iso");
    let temp_instance_dir = target_dir.join("tmp-instance");
    let temp_config = target_dir.join("postgresql.conf");

    let pg_config = cargo_pgrx_info(pg_version, "pg-config")?;
    let bindir = pg_config_value(&pg_config, "--bindir")?;
    let pkglibdir = pg_config_value(&pg_config, "--pkglibdir")?;
    let isolation_regress = pkglibdir.join("pgxs/src/test/isolation/pg_isolation_regress");

    if !isolation_regress.exists() {
        return Err(format!(
            "pg_isolation_regress not found at {}\n\
             Install PostgreSQL server test tooling for this pg_config.",
            isolation_regress.display()
        ));
    }

    println!("Installing {EXTENSION_PACKAGE} into {}", pg_config.display());
    run_command(
        Command::new("cargo")
            .arg("pgrx")
            .arg("install")
            .arg("--package")
            .arg(EXTENSION_PACKAGE)
            .arg("--pg-config")
            .arg(&pg_config),
    )?;

    reset_dir(&output_dir)?;
    reset_dir(&temp_instance_dir)?;
    fs::create_dir_all(&target_dir)
        .map_err(|error| format!("failed to create {}: {error}", target_dir.display()))?;
    fs::write(
        &temp_config,
        format!("shared_preload_libraries = '{EXTENSION_NAME}'\n"),
    )
    .map_err(|error| format!("failed to write {}: {error}", temp_config.display()))?;

    let specs: Vec<OsString> = if specs.is_empty() {
        DEFAULT_ISOLATION_SPECS
            .iter()
            .map(OsString::from)
            .collect()
    } else {
        specs.to_vec()
    };

    println!(
        "Running isolation specs with temporary PostgreSQL instance in {}",
        temp_instance_dir.display()
    );

    let mut command = Command::new(&isolation_regress);
    command
        .current_dir(&tests_dir)
        .arg(format!("--bindir={}", bindir.display()))
        .arg(format!("--dlpath={}", pkglibdir.display()))
        .arg("--inputdir=.")
        .arg("--expecteddir=.")
        .arg(format!("--outputdir={}", output_dir.display()))
        .arg(format!("--temp-instance={}", temp_instance_dir.display()))
        .arg(format!("--temp-config={}", temp_config.display()))
        .args(&specs);

    run_command(&mut command).map_err(|error| {
        format!(
            "{error}\n\nisolation output: {}\nregression diffs: {}",
            output_dir.display(),
            output_dir.join("regression.diffs").display()
        )
    })
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate must live under the workspace root")
        .to_path_buf()
}

fn cargo_pgrx_info(pg_version: &OsStr, key: &str) -> Result<PathBuf, String> {
    let output = Command::new("cargo")
        .arg("pgrx")
        .arg("info")
        .arg(key)
        .arg(pg_version)
        .output()
        .map_err(|error| format!("failed to run cargo pgrx info {key}: {error}"))?;

    if !output.status.success() {
        return Err(format!(
            "cargo pgrx info {key} failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(PathBuf::from(
        String::from_utf8_lossy(&output.stdout).trim(),
    ))
}

fn pg_config_value(pg_config: &Path, arg: &str) -> Result<PathBuf, String> {
    let output = Command::new(pg_config)
        .arg(arg)
        .output()
        .map_err(|error| format!("failed to run {} {arg}: {error}", pg_config.display()))?;

    if !output.status.success() {
        return Err(format!(
            "{} {arg} failed:\n{}",
            pg_config.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(PathBuf::from(
        String::from_utf8_lossy(&output.stdout).trim(),
    ))
}

fn reset_dir(path: &Path) -> Result<(), String> {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("failed to remove {}: {error}", path.display())),
    }

    fs::create_dir_all(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))
}

fn run_command(command: &mut Command) -> Result<(), String> {
    println!("$ {}", display_command(command));
    let status = command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|error| format!("failed to run {}: {error}", display_command(command)))?;

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
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '='))
    {
        value.into_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}
