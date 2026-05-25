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

/// Naming convention: regression tests whose filename starts with `docker_`
/// require Docker (MinIO, etc.) and are only run when `--docker` is passed.
const DOCKER_TEST_PREFIX: &str = "docker_";

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
        Some(command) if command == OsStr::new("test-all") => {
            let pg_version = args
                .next()
                .ok_or_else(|| usage_error("missing PostgreSQL version"))?;
            let flags: Vec<OsString> = args.collect();
            let include_docker = flags.iter().any(|f| f == "--docker");
            run_test_all(&pg_version, include_docker)
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
        "{message}\n\n\
         usage:\n  \
           cargo xtask test-all <pg-version> [--docker]\n  \
           cargo xtask isolation <pg-version> [spec ...]\n\n\
         examples:\n  \
           cargo xtask test-all pg17           # unit + pgrx + regress + isolation\n  \
           cargo xtask test-all pg17 --docker  # also run tests requiring Docker\n  \
           cargo xtask isolation pg17\n  \
           cargo xtask isolation pg17 cas_retry_stress"
    )
}

// ============================================================================
//  test-all: orchestrate the full test suite
// ============================================================================

fn run_test_all(pg_version: &OsStr, include_docker: bool) -> Result<(), String> {
    let pg_ver_str = pg_version.to_string_lossy();
    let mut skipped: Vec<String> = Vec::new();

    println!("=== Phase 1: Workspace unit tests (no external deps) ===\n");
    run_command(
        Command::new("cargo")
            .arg("test")
            .arg("--workspace")
            .arg("--exclude")
            .arg("pg-lakebase-core-tests"),
    )?;

    println!("\n=== Phase 2: pg-lakebase-core pg_test (PostgreSQL) ===\n");
    run_command(
        Command::new("cargo")
            .arg("pgrx")
            .arg("test")
            .arg(pg_version)
            .arg("--package")
            .arg("pg-lakebase-core-tests"),
    )?;

    println!("\n=== Phase 3: pg-iceberg-am SQL regression (PostgreSQL) ===\n");
    let regress_skipped = run_regress(pg_version, include_docker)?;
    skipped.extend(regress_skipped);

    println!("\n=== Phase 4: Isolation tests (PostgreSQL) ===\n");
    run_isolation(pg_version, &[])?;

    if include_docker {
        println!("\n=== Phase 5: pg-lakebase-storage E2E (Docker/MinIO) ===\n");
        run_command(
            Command::new("cargo")
                .arg("test")
                .arg("--package")
                .arg("pg-lakebase-storage")
                .arg("--features")
                .arg("integration")
                .arg("--test")
                .arg("e2e"),
        )?;
    } else {
        skipped.push("pg-lakebase-storage E2E".into());
    }

    println!();
    if skipped.is_empty() {
        println!("=== All tests passed! ({pg_ver_str}) ===");
    } else {
        println!("=== All tests passed! ({pg_ver_str}) ===");
        println!("    Not executed (require --docker): {}", skipped.join(", "));
    }
    Ok(())
}

// ============================================================================
//  regress: run pg_regress
// ============================================================================

/// Returns the list of test names that were skipped (Docker-dependent).
fn run_regress(pg_version: &OsStr, include_docker: bool) -> Result<Vec<String>, String> {
    let workspace = workspace_root();
    let regress_dir = workspace.join("pg-iceberg-am/tests/pg_regress");

    let docker_tests = discover_docker_tests(&regress_dir);

    if !include_docker && !docker_tests.is_empty() {
        hide_docker_tests(&regress_dir, &docker_tests)?;
    }

    let result = run_command(
        Command::new("cargo")
            .arg("pgrx")
            .arg("regress")
            .arg(pg_version)
            .arg("--package")
            .arg(EXTENSION_PACKAGE)
            .arg("--resetdb")
            .arg("--postgresql-conf")
            .arg(format!("shared_preload_libraries='{EXTENSION_NAME}'")),
    );

    if !include_docker && !docker_tests.is_empty() {
        restore_docker_tests(&regress_dir, &docker_tests);
    }

    result?;

    let skipped: Vec<String> = if !include_docker {
        docker_tests
            .iter()
            .map(|t| format!("regress/{t}"))
            .collect()
    } else {
        vec![]
    };
    Ok(skipped)
}

/// Discover Docker-dependent tests by scanning sql/ for files with the `docker_` prefix.
fn discover_docker_tests(regress_dir: &Path) -> Vec<String> {
    let sql_dir = regress_dir.join("sql");
    let mut tests = Vec::new();
    if let Ok(entries) = fs::read_dir(&sql_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(DOCKER_TEST_PREFIX) && name.ends_with(".sql") {
                let stem = name.strip_suffix(".sql").unwrap();
                tests.push(stem.to_string());
            }
        }
    }
    tests.sort();
    tests
}

/// Temporarily rename Docker-dependent test files so pg_regress won't discover them.
fn hide_docker_tests(regress_dir: &Path, tests: &[String]) -> Result<(), String> {
    for test_name in tests {
        let sql = regress_dir.join(format!("sql/{test_name}.sql"));
        let expected = regress_dir.join(format!("expected/{test_name}.out"));

        if sql.exists() {
            let dest = regress_dir.join(format!("sql/{test_name}.sql.skip"));
            fs::rename(&sql, &dest)
                .map_err(|e| format!("failed to hide {}: {e}", sql.display()))?;
        }
        if expected.exists() {
            let dest = regress_dir.join(format!("expected/{test_name}.out.skip"));
            fs::rename(&expected, &dest)
                .map_err(|e| format!("failed to hide {}: {e}", expected.display()))?;
        }
    }
    Ok(())
}

/// Restore hidden test files back to their original names.
fn restore_docker_tests(regress_dir: &Path, tests: &[String]) {
    for test_name in tests {
        let sql_skip = regress_dir.join(format!("sql/{test_name}.sql.skip"));
        let expected_skip = regress_dir.join(format!("expected/{test_name}.out.skip"));

        if sql_skip.exists() {
            let _ = fs::rename(&sql_skip, regress_dir.join(format!("sql/{test_name}.sql")));
        }
        if expected_skip.exists() {
            let _ =
                fs::rename(&expected_skip, regress_dir.join(format!("expected/{test_name}.out")));
        }
    }
}

// ============================================================================
//  isolation: run pg_isolation_regress specs
// ============================================================================

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

// ============================================================================
//  utilities
// ============================================================================

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
