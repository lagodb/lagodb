use std::env;
use std::path::PathBuf;
use std::process::Command;

#[test]
fn test_isolation_specs() {
    // 1. Get PostgreSQL installation information using pg_config
    // We assume pg_config is in the PATH, similar to how Makefile works
    let pg_config_output = Command::new("pg_config")
        .arg("--pkglibdir")
        .output()
        .expect("Failed to execute pg_config. Is PostgreSQL installed and in PATH?");

    if !pg_config_output.status.success() {
        panic!(
            "pg_config failed: {}",
            String::from_utf8_lossy(&pg_config_output.stderr)
        );
    }

    let pkglibdir = String::from_utf8(pg_config_output.stdout)
        .expect("pg_config output is not valid UTF-8")
        .trim()
        .to_string();

    // 2. Locate pg_isolation_regress
    // Typically located at <pkglibdir>/pgxs/src/test/isolation/pg_isolation_regress
    // This is the standard location for PGXS installations (including pgrx managed ones)
    let isolation_tester_path = PathBuf::from(&pkglibdir)
        .join("pgxs/src/test/isolation/pg_isolation_regress");

    if !isolation_tester_path.exists() {
        // Try looking in the PATH just in case, otherwise fail with a helpful message
        let status = Command::new("pg_isolation_regress")
            .arg("--version")
            .status();
        if status.is_err() {
            panic!(
                "Could not find pg_isolation_regress at expected location: {:?}.\n\
                 Please ensure postgresql-server-dev (or equivalent) is installed.",
                isolation_tester_path
            );
        }
    }

    // 3. Define the tests to run (matching the Makefile)
    let specs = vec![
        "read_visibility",
        "cas_retry_stress",
        "savepoint_concurrent",
    ];

    // 4. Setup paths
    let manifest_dir =
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let tests_dir = PathBuf::from(manifest_dir).join("tests").join("isolation");
    let output_dir = tests_dir.join("output_iso");

    // Ensure output directory exists (pg_isolation_regress usually creates it, but good to be safe)
    if !output_dir.exists() {
        std::fs::create_dir_all(&output_dir)
            .expect("Failed to create output directory");
    }

    println!("Running isolation tests in: {:?}", tests_dir);
    println!("Using tester: {:?}", isolation_tester_path);

    // 5. Execute pg_isolation_regress
    // Equivalent to:
    // pg_isolation_regress --inputdir=. --outputdir=output_iso --load-extension=pg_am_iceberg <specs>
    // Note: --load-extension is optional if shared_preload_libraries is set,
    // but the Makefile didn't strictly require it if database was already prepped.
    // We stick to the Makefile's implicit assumption that the environment/DB is ready.

    // Using the absolute path to the binary we found or just the name if we fallback
    let binary = if isolation_tester_path.exists() {
        isolation_tester_path.as_os_str()
    } else {
        std::ffi::OsStr::new("pg_isolation_regress")
    };

    let mut cmd = Command::new(binary);
    cmd.current_dir(&tests_dir)
        .arg("--inputdir=.")
        .arg("--outputdir=output_iso")
        // We pass the specs
        .args(&specs);

    // Forward relevant environment variables (like PGHOST, PGPORT, PGDATABASE, PGUSER)
    // Cargo tests usually run with a clean environment, but for connecting to a live DB
    // we need these vars. Since we are running from a shell that likely has them,
    // purely inheriting env is default behavior for Command::new.

    let status = cmd
        .status()
        .expect("Failed to generate/run isolation tests process");

    assert!(
        status.success(),
        "Some isolation tests failed. Check {:?}/results/ regression.diffs for details.",
        output_dir
    );
}
