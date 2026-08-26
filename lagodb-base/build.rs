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
        // The runtime VACUUM bridge has not been ported to PG16.
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
    println!("cargo:rerun-if-changed=csrc/vacuum/lakebase_vacuum.c");
    println!("cargo:rerun-if-changed=csrc/vacuum/lakebase_vacuum.h");
    println!(
        "cargo:rerun-if-changed=../pg-lakebase-core/csrc/compat/lakebase_pg_compat.h"
    );

    let Some(pg_feature) = active_pg_config() else {
        return;
    };
    if !pg_feature.c_forks_supported {
        return;
    }

    let pgrx = Pgrx::from_config().expect("failed to read pgrx configuration");
    let pg_config = pgrx.get(pg_feature.pgrx_config).unwrap_or_else(|error| {
        panic!(
            "pgrx has no {} configuration: {error}",
            pg_feature.pgrx_config
        )
    });
    let include = pg_config.includedir_server().unwrap_or_else(|error| {
        panic!(
            "{} server include directory is unavailable: {error}",
            pg_feature.pgrx_config
        )
    });

    cc::Build::new()
        .file("csrc/vacuum/lakebase_vacuum.c")
        .include(PathBuf::from("../pg-lakebase-core/csrc/compat"))
        .include(PathBuf::from("csrc/vacuum"))
        .include(include)
        .flag_if_supported("-Wno-unused-function")
        .flag_if_supported("-Wno-unused-parameter")
        .compile("lakebase_runtime_pg_bridges");
}
