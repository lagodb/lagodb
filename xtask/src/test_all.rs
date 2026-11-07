use crate::extension::Extension;
use crate::isolation;
use crate::pg::PgInstallation;
use crate::process::{cargo, run_command};
use crate::regress::{RegressionRunner, RegressionSuite};

/// Orchestrate the full test suite.
pub(crate) fn run(pg: &PgInstallation) -> Result<(), String> {
    println!("=== Phase 0: PostgreSQL test capabilities ===\n");
    pg.prepare_injection_points()?;

    println!("=== Phase 1: Workspace unit tests (non-pgrx extension crates) ===\n");
    let mut unit_tests = cargo();
    unit_tests.arg("test").arg("--workspace");
    for extension in Extension::ALL {
        unit_tests.args(["--exclude", extension.package()]);
    }
    unit_tests
        .arg("--no-default-features")
        .arg("--features")
        .arg(pg.label());
    run_command(&mut unit_tests)?;

    println!("\n=== Phase 2: Extension Rust tests (host + pg_test) ===\n");
    pg.test(Extension::Runtime)?;
    // `cargo pgrx test` replaces the runtime artifacts in PostgreSQL's shared
    // extension directory with a build containing `pg_test` SQL entities.
    // Restore the production artifacts before creating the runtime as a
    // dependency of another extension's test cluster.
    pg.install(Extension::Runtime)?;
    pg.test(Extension::Iceberg)?;
    let regression = RegressionRunner::new(pg)?;

    println!("\n=== Phase 3: lagodb-iceberg SQL regression (PostgreSQL) ===\n");
    regression.run(RegressionSuite::Iceberg, None)?;

    println!("\n=== Phase 4: LagoDB connectors SQL regression (PostgreSQL) ===\n");
    regression.run(RegressionSuite::Connectors, None)?;

    println!("\n=== Phase 5: Isolation tests (PostgreSQL) ===\n");
    isolation::run(pg, &[])?;

    println!();
    println!("=== All tests passed! ({}) ===", pg.label());
    Ok(())
}
