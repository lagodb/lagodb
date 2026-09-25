use std::path::PathBuf;

use pgrx_pg_config::Pgrx;

struct PgFeature {
    environment: &'static str,
    pgrx_config: &'static str,
    c_forks_supported: bool,
}

static PG_FEATURES: [PgFeature; 2] = [
    PgFeature {
        environment: "CARGO_FEATURE_PG16",
        pgrx_config: "pg16",
        // The runtime maintenance command scope has not been ported to PG16.
        c_forks_supported: false,
    },
    PgFeature {
        environment: "CARGO_FEATURE_PG17",
        pgrx_config: "pg17",
        c_forks_supported: true,
    },
];

fn active_pg_config() -> Option<&'static PgFeature> {
    let mut active = None;
    for feature in &PG_FEATURES {
        if std::env::var_os(feature.environment).is_some() {
            assert!(
                active.is_none(),
                "exactly one PostgreSQL feature must be enabled"
            );
            active = Some(feature);
        }
    }
    active
}

fn main() {
    println!("cargo:rerun-if-env-changed=PGRX_PG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed=PGRX_HOME");
    println!("cargo:rerun-if-env-changed=HOME");
    println!("cargo:rerun-if-changed=csrc/compat/lagodb_base_pg_compat.h");
    println!("cargo:rerun-if-changed=csrc/maintenance/lagodb_maintenance.c");
    println!("cargo:rerun-if-changed=csrc/maintenance/lagodb_maintenance.h");

    let Some(pg_feature) = active_pg_config() else {
        return;
    };
    if !pg_feature.c_forks_supported {
        return;
    }
    if std::env::var_os("PGRX_PG_CONFIG_PATH").is_none() {
        let pgrx_config = Pgrx::config_toml()
            .expect("failed to locate the pgrx configuration file");
        println!("cargo:rerun-if-changed={}", pgrx_config.display());
    }

    let pgrx = Pgrx::from_config().expect("failed to read pgrx configuration");
    let pg_config = pgrx.get(pg_feature.pgrx_config).unwrap_or_else(|error| {
        panic!(
            "pgrx has no {} configuration: {error}",
            pg_feature.pgrx_config
        )
    });
    let pg_config_path = pg_config.path().unwrap_or_else(|| {
        panic!(
            "{} configuration has no pg_config path",
            pg_feature.pgrx_config
        )
    });
    println!("cargo:rerun-if-changed={}", pg_config_path.display());
    let include = pg_config.includedir_server().unwrap_or_else(|error| {
        panic!(
            "{} server include directory is unavailable: {error}",
            pg_feature.pgrx_config
        )
    });
    let pg_config_header = include.join("pg_config.h");
    println!("cargo:rerun-if-changed={}", pg_config_header.display());

    cc::Build::new()
        .file("csrc/maintenance/lagodb_maintenance.c")
        .include(PathBuf::from("csrc/maintenance"))
        .include(PathBuf::from("csrc/compat"))
        .include(include)
        .flag_if_supported("-Wno-unused-function")
        .flag_if_supported("-Wno-unused-parameter")
        .compile("lagodb_runtime_pg_bridges");
}
