use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

mod regress;

use regress::{RegressionRunner, RegressionSuite, RegressionTarget};

const EXTENSION_PACKAGE: &str = "lagodb-iceberg";
const EXTENSION_NAME: &str = "lagodb_iceberg";
const RUNTIME_PACKAGE: &str = "pg-lakebase-runtime";
const RUNTIME_NAME: &str = "pg_lakebase_runtime";
const DELTA_AM_PACKAGE: &str = "pg-delta-am";
const DELTA_AM_NAME: &str = "pg_delta_am";
const CONNECTORS_PACKAGE: &str = "lagodb-connectors";
const CONNECTORS_NAME: &str = "lagodb_connectors";

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
            ensure_no_extra_args(args)?;
            run_test_all(&pg_version)
        }
        Some(command) if command == OsStr::new("regress") => {
            let pg_version = args
                .next()
                .ok_or_else(|| usage_error("missing PostgreSQL version"))?;
            let target = RegressionTarget::parse(args)?;
            if target.includes_iceberg() {
                prepare_injection_points(&pg_version)?;
            }
            let runner = RegressionRunner::prepare(&pg_version)?;
            target.run(&runner)
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
           cargo xtask test-all <pg-version>\n  \
           cargo xtask regress <pg-version> [all]\n  \
           cargo xtask regress <pg-version> <iceberg|connectors> [test ...]\n  \
           cargo xtask isolation <pg-version> [spec ...]\n\n\
         examples:\n  \
           cargo xtask test-all pg17\n  \
           cargo xtask regress pg17\n  \
           cargo xtask regress pg17 iceberg worker\n  \
           cargo xtask regress pg17 connectors copy_codecs\n  \
           cargo xtask isolation pg17\n  \
           cargo xtask isolation pg17 cas_retry_stress"
    )
}

fn ensure_no_extra_args(
    mut args: impl Iterator<Item = OsString>,
) -> Result<(), String> {
    if let Some(arg) = args.next() {
        return Err(usage_error(&format!(
            "unexpected argument '{}'",
            arg.to_string_lossy()
        )));
    }
    Ok(())
}

// ============================================================================
//  test-all: orchestrate the full test suite
// ============================================================================

fn run_test_all(pg_version: &OsStr) -> Result<(), String> {
    let pg_ver_str = pg_version.to_string_lossy();
    let pg_feature = pg_feature(pg_version)?;

    println!("=== Phase 0: PostgreSQL test capabilities ===\n");
    prepare_injection_points(pg_version)?;

    println!("=== Phase 1: Workspace unit tests (non-pgrx extension crates) ===\n");
    run_command(
        Command::new("cargo")
            .arg("test")
            .arg("--workspace")
            .arg("--exclude")
            .arg("pg-backend-tests")
            .arg("--exclude")
            .arg(RUNTIME_PACKAGE)
            .arg("--exclude")
            .arg(EXTENSION_PACKAGE)
            .arg("--exclude")
            .arg(DELTA_AM_PACKAGE)
            .arg("--exclude")
            .arg(CONNECTORS_PACKAGE)
            .arg("--no-default-features")
            .arg("--features")
            .arg(&pg_feature),
    )?;

    println!("\n=== Phase 2: framework pg_test (PostgreSQL) ===\n");
    // Framework backend tests exercise the same cross-DSO runtime ABI used by
    // product extensions. Install the runtime before pgrx starts the test
    // cluster so pg-backend-tests can preload the sole owner of shared GUCs.
    install_runtime(pg_version)?;
    run_command(
        Command::new("cargo")
            .arg("pgrx")
            .arg("test")
            .arg(pg_version)
            .arg("--package")
            .arg("pg-backend-tests"),
    )?;

    println!("\n=== Phase 3: Extension Rust tests (host + pg_test) ===\n");
    run_command(
        Command::new("cargo")
            .arg("pgrx")
            .arg("test")
            .arg(pg_version)
            .arg("--package")
            .arg(RUNTIME_PACKAGE),
    )?;
    // `cargo pgrx test` replaces the runtime artifacts in PostgreSQL's shared
    // extension directory with a build containing `pg_test` SQL entities.
    // Restore the production artifacts before creating the runtime as a
    // dependency of another extension's test cluster.
    install_runtime(pg_version)?;
    run_command(
        Command::new("cargo")
            .arg("pgrx")
            .arg("test")
            .arg(pg_version)
            .arg("--package")
            .arg(EXTENSION_PACKAGE),
    )?;
    let regression = RegressionRunner::prepare(pg_version)?;

    println!("\n=== Phase 4: lagodb-iceberg SQL regression (PostgreSQL) ===\n");
    regression.run(RegressionSuite::Iceberg, &[])?;

    println!("\n=== Phase 5: LagoDB connectors SQL regression (PostgreSQL) ===\n");
    regression.run(RegressionSuite::Connectors, &[])?;

    println!("\n=== Phase 6: Isolation tests (PostgreSQL) ===\n");
    run_isolation(pg_version, &[])?;

    println!("\n=== Phase 7: pg-lakebase-storage E2E (Docker/MinIO) ===\n");
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

    println!();
    println!("=== All tests passed! ({pg_ver_str}) ===");
    Ok(())
}

fn pg_feature(pg_version: &OsStr) -> Result<String, String> {
    let major = pg_major(pg_version)?;
    Ok(format!("pg{major}"))
}

fn pg_major(pg_version: &OsStr) -> Result<String, String> {
    let value = pg_version.to_string_lossy();
    let major = value.strip_prefix("pg").unwrap_or(&value);

    match major {
        "16" | "17" => Ok(major.to_string()),
        _ => Err(format!(
            "unsupported PostgreSQL version '{value}'; expected pg16 or pg17"
        )),
    }
}

fn prepare_injection_points(pg_version: &OsStr) -> Result<(), String> {
    if pg_major(pg_version)? != "17" {
        return Ok(());
    }

    let pg_config = cargo_pgrx_info(pg_version, "pg-config")?;
    let server_include = pg_config_value(&pg_config, "--includedir-server")?;
    let config_header = server_include.join("pg_config.h");
    let config = fs::read_to_string(&config_header).map_err(|error| {
        format!(
            "failed to read PostgreSQL configuration {}: {error}",
            config_header.display()
        )
    })?;
    if !config
        .lines()
        .any(|line| line.trim() == "#define USE_INJECTION_POINTS 1")
    {
        return Err(format!(
            "PostgreSQL 17 at {} was built without injection-point support.\n\
             Rebuild the pgrx-managed server with:\n  \
             cargo pgrx init --pg17=download --configure-flag=--enable-injection-points",
            pg_config.display()
        ));
    }

    let install_root = cargo_pgrx_info(pg_version, "path")?;
    let source_root = install_root.parent().ok_or_else(|| {
        format!(
            "pgrx PostgreSQL install path has no source-tree parent: {}",
            install_root.display()
        )
    })?;
    let module_dir = source_root.join("src/test/modules/injection_points");
    if !module_dir.join("Makefile").is_file() {
        return Err(format!(
            "PostgreSQL injection-points test module not found at {}.\n\
             Register a pgrx-managed PostgreSQL source build with --pg17=download.",
            module_dir.display()
        ));
    }

    println!(
        "Installing PostgreSQL injection_points test extension from {}",
        module_dir.display()
    );
    run_command(
        Command::new("make")
            .arg("-C")
            .arg(module_dir)
            .arg("USE_PGXS=1")
            .arg(format!("PG_CONFIG={}", pg_config.display()))
            .arg("install"),
    )
}

fn install_runtime(pg_version: &OsStr) -> Result<(), String> {
    let pg_config = cargo_pgrx_info(pg_version, "pg-config")?;
    run_command(
        Command::new("cargo")
            .arg("pgrx")
            .arg("install")
            .arg("--package")
            .arg(RUNTIME_PACKAGE)
            .arg("--pg-config")
            .arg(pg_config),
    )
}

// ============================================================================
//  isolation: run pg_isolation_regress specs
// ============================================================================

fn run_isolation(pg_version: &OsStr, specs: &[OsString]) -> Result<(), String> {
    pg_major(pg_version)?;

    let workspace = workspace_root();
    let tests_dir = workspace.join("lagodb-iceberg/tests/isolation");
    let target_dir = workspace
        .join("target/isolation")
        .join(pg_version.to_string_lossy().as_ref());
    let output_dir = target_dir.join("output_iso");
    let temp_instance_dir = target_dir.join("tmp-instance");
    let temp_config = target_dir.join("postgresql.conf");

    let pg_config = cargo_pgrx_info(pg_version, "pg-config")?;
    let bindir = pg_config_value(&pg_config, "--bindir")?;
    let pkglibdir = pg_config_value(&pg_config, "--pkglibdir")?;
    let isolation_regress =
        pkglibdir.join("pgxs/src/test/isolation/pg_isolation_regress");

    if !isolation_regress.exists() {
        return Err(format!(
            "pg_isolation_regress not found at {}\n\
             Install PostgreSQL server test tooling for this pg_config.",
            isolation_regress.display()
        ));
    }

    println!(
        "Installing {EXTENSION_PACKAGE} into {}",
        pg_config.display()
    );
    run_command(
        Command::new("cargo")
            .arg("pgrx")
            .arg("install")
            .arg("--package")
            .arg(RUNTIME_PACKAGE)
            .arg("--pg-config")
            .arg(&pg_config),
    )?;
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
    fs::create_dir_all(&target_dir).map_err(|error| {
        format!("failed to create {}: {error}", target_dir.display())
    })?;
    fs::write(
        &temp_config,
        format!(
            "shared_preload_libraries = '{RUNTIME_NAME}'\npg_lakebase.provider_libraries = '{EXTENSION_NAME}'\n"
        ),
    )
    .map_err(|error| format!("failed to write {}: {error}", temp_config.display()))?;

    let specs: Vec<OsString> = if specs.is_empty() {
        discover_isolation_specs(&tests_dir)?
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
        .arg(format!("--load-extension={RUNTIME_NAME}"))
        .args(&specs);

    run_command(&mut command).map_err(|error| {
        format!(
            "{error}\n\nisolation output: {}\nregression diffs: {}",
            output_dir.display(),
            output_dir.join("regression.diffs").display()
        )
    })
}

fn discover_isolation_specs(tests_dir: &Path) -> Result<Vec<OsString>, String> {
    let specs_dir = tests_dir.join("specs");
    let mut specs: Vec<OsString> = fs::read_dir(&specs_dir)
        .map_err(|error| {
            format!(
                "failed to read specs directory {}: {error}",
                specs_dir.display()
            )
        })?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("spec") {
                path.file_stem().map(|s| s.to_os_string())
            } else {
                None
            }
        })
        .collect();

    specs.sort();

    if specs.is_empty() {
        return Err(format!("no .spec files found in {}", specs_dir.display()));
    }

    println!("Discovered {} isolation specs: {:?}", specs.len(), specs);
    Ok(specs)
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
    let output = Command::new(pg_config).arg(arg).output().map_err(|error| {
        format!("failed to run {} {arg}: {error}", pg_config.display())
    })?;

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

fn prepend_path_env(command: &mut Command, directory: &Path) -> Result<(), String> {
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

fn reset_dir(path: &Path) -> Result<(), String> {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!("failed to remove {}: {error}", path.display()));
        }
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
        .map_err(|error| {
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
