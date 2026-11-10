use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::extension::Extension;
use crate::pg::PgInstallation;
use crate::process::run_command;

/// Run pg_isolation_regress specs; an empty `specs` runs every discovered spec.
pub(crate) fn run(pg: &PgInstallation, specs: &[OsString]) -> Result<(), String> {
    let workspace = workspace_root();
    let tests_dir = workspace.join("lagodb-iceberg/tests/isolation");
    let target_dir = workspace.join("target/isolation").join(pg.label());
    let output_dir = target_dir.join("output_iso");
    let temp_instance_dir = target_dir.join("tmp-instance");
    let temp_config = target_dir.join("postgresql.conf");
    let specs = select_specs(&tests_dir, specs)?;

    let bindir = pg.config_value("--bindir")?;
    let pkglibdir = pg.config_value("--pkglibdir")?;
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
        "Installing {} into {}",
        Extension::Iceberg.package(),
        pg.pg_config().display()
    );
    pg.install(Extension::Runtime)?;
    pg.install(Extension::Iceberg)?;

    reset_dir(&output_dir)?;
    reset_dir(&temp_instance_dir)?;
    let runtime = Extension::Runtime.library();
    let provider = Extension::Iceberg.library();
    fs::write(
        &temp_config,
        format!(
            "shared_preload_libraries = '{runtime}'\nlagodb.provider_libraries = '{provider}'\n"
        ),
    )
    .map_err(|error| format!("failed to write {}: {error}", temp_config.display()))?;

    println!(
        "Running isolation specs with temporary PostgreSQL instance in {}",
        temp_instance_dir.display()
    );

    let mut command = Command::new(&isolation_regress);
    command
        .current_dir(&tests_dir)
        .arg(format!("--bindir={}", bindir.display()))
        .arg(format!("--dlpath={}", pkglibdir.display()))
        // isolation_main.c reads `<outputdir>/expected`, then
        // `<inputdir>/expected`; it never uses `--expecteddir`, which
        // PostgreSQL 15 does not accept.
        .arg("--inputdir=.")
        .arg(format!("--outputdir={}", output_dir.display()))
        .arg(format!("--temp-instance={}", temp_instance_dir.display()))
        .arg(format!("--temp-config={}", temp_config.display()))
        .arg(format!("--load-extension={runtime}"))
        .args(&specs);

    run_command(&mut command).map_err(|error| {
        format!(
            "{error}\n\nisolation output: {}\nregression diffs: {}",
            output_dir.display(),
            output_dir.join("regression.diffs").display()
        )
    })
}

/// Resolve the requested specs before anything touches PostgreSQL or the output
/// directory: the leading `pgXX` is optional, so a mistyped major arrives here
/// as a spec name.
fn select_specs(
    tests_dir: &Path,
    requested: &[OsString],
) -> Result<Vec<OsString>, String> {
    let available = discover_specs(tests_dir)?;
    if requested.is_empty() {
        println!(
            "Discovered {} isolation specs: {:?}",
            available.len(),
            available
        );
        return Ok(available);
    }

    if let Some(unknown) = requested.iter().find(|spec| !available.contains(spec)) {
        let available: Vec<_> = available
            .iter()
            .map(|spec| spec.to_string_lossy())
            .collect();
        return Err(format!(
            "unknown isolation spec '{}'; available specs: {}",
            unknown.to_string_lossy(),
            available.join(", ")
        ));
    }

    Ok(requested.to_vec())
}

fn discover_specs(tests_dir: &Path) -> Result<Vec<OsString>, String> {
    let specs_dir = tests_dir.join("specs");
    let read_error = |error: io::Error| {
        format!(
            "failed to read specs directory {}: {error}",
            specs_dir.display()
        )
    };

    let mut specs: Vec<OsString> = Vec::new();
    for entry in fs::read_dir(&specs_dir).map_err(read_error)? {
        let path = entry.map_err(read_error)?.path();
        if path.extension().and_then(OsStr::to_str) == Some("spec") {
            specs.extend(path.file_stem().map(OsStr::to_os_string));
        }
    }

    specs.sort();

    if specs.is_empty() {
        return Err(format!("no .spec files found in {}", specs_dir.display()));
    }

    Ok(specs)
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate must live under the workspace root")
        .to_path_buf()
}

fn reset_dir(path: &Path) -> Result<(), String> {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!("failed to remove {}: {error}", path.display()));
        }
    }

    fs::create_dir_all(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))
}
