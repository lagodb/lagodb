use std::fs;
use std::path::{Path, PathBuf};

use pgrx_pg_config::Pgrx;

struct PgFeature {
    environment: &'static str,
    pgrx_config: &'static str,
    c_forks_supported: bool,
    copy_bridge_supported: bool,
    native_injection_points_supported: bool,
}

static PG_FEATURES: [PgFeature; 2] = [
    PgFeature {
        environment: "CARGO_FEATURE_PG16",
        pgrx_config: "pg16",
        // PG16 TableAM callbacks require a separate compatibility port.
        c_forks_supported: false,
        // The COPY bridge is intentionally not compiled for PG16. A future
        // PostgreSQL port will enable it after its C ABI is audited.
        copy_bridge_supported: false,
        native_injection_points_supported: false,
    },
    PgFeature {
        environment: "CARGO_FEATURE_PG17",
        pgrx_config: "pg17",
        c_forks_supported: true,
        copy_bridge_supported: true,
        native_injection_points_supported: true,
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
    println!("cargo:rustc-check-cfg=cfg(lakebase_pg_injection_points)");
    println!("cargo:rerun-if-env-changed=PGRX_PG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed=PGRX_HOME");
    println!("cargo:rerun-if-env-changed=HOME");
    println!("cargo:rerun-if-changed=csrc/modify/lakebase_node_modify_table.c");
    println!("cargo:rerun-if-changed=csrc/modify/lakebase_node_modify_table.h");
    println!("cargo:rerun-if-changed=csrc/analyze/lakebase_analyze.c");
    println!("cargo:rerun-if-changed=csrc/analyze/lakebase_analyze.h");
    println!("cargo:rerun-if-changed=csrc/compat/lakebase_pg_compat.h");
    println!("cargo:rerun-if-changed=csrc/compat/lakebase_injection_point.c");
    println!("cargo:rerun-if-changed=csrc/compat/lakebase_injection_point.h");
    println!("cargo:rerun-if-changed=csrc/copy/lakebase_copy.c");
    println!("cargo:rerun-if-changed=csrc/copy/lakebase_copy.h");

    let Some(pg_feature) = active_pg_config() else {
        return;
    };
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

    if pg_feature.native_injection_points_supported
        && injection_points_enabled(&pg_config_header)
    {
        println!("cargo:rustc-cfg=lakebase_pg_injection_points");
    }

    let mut build = cc::Build::new();
    build.include(include);
    let mut has_sources = false;
    // `lakebase_copy.c` owns COPY preparation, raw-field parsing, and the PG17
    // Text/CSV row-encoder bridge. The source is selected per PostgreSQL
    // feature here; the C header applies the shared major-version gate and
    // local source branches handle audited minor epochs.
    if pg_feature.copy_bridge_supported {
        build
            .file("csrc/copy/lakebase_copy.c")
            .include(PathBuf::from("csrc/copy"))
            .include(PathBuf::from("csrc/compat"))
            .flag_if_supported("-Wno-unused-function")
            .flag_if_supported("-Wno-unused-parameter");
        has_sources = true;
    }

    if pg_feature.native_injection_points_supported {
        build
            .file("csrc/compat/lakebase_injection_point.c")
            .include(PathBuf::from("csrc/compat"));
        has_sources = true;
    }

    if pg_feature.c_forks_supported {
        build
            .file("csrc/modify/lakebase_node_modify_table.c")
            .file("csrc/analyze/lakebase_analyze.c")
            .include(PathBuf::from("csrc/modify"))
            .include(PathBuf::from("csrc/analyze"))
            .include(PathBuf::from("csrc/compat"));
        has_sources = true;
    }

    if has_sources {
        build.compile("lakebase_pg_bridges");
    }
}

fn injection_points_enabled(pg_config_header: &Path) -> bool {
    let contents = fs::read_to_string(pg_config_header).unwrap_or_else(|error| {
        panic!(
            "failed to read PostgreSQL configuration header {}: {error}",
            pg_config_header.display()
        )
    });

    // Configure leaves `/* #undef USE_INJECTION_POINTS */` in standard
    // installations, so match the generated definition rather than merely
    // searching for the capability name.
    contents
        .lines()
        .any(|line| line.trim() == "#define USE_INJECTION_POINTS 1")
}
