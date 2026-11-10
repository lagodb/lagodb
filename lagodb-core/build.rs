use std::fs;
use std::path::{Path, PathBuf};

use pgrx_pg_config::Pgrx;

struct PgFeature {
    environment: &'static str,
    pgrx_config: &'static str,
    /// PostgreSQL 17 introduced the ReadStream that the sampler adapter wraps.
    read_stream: bool,
}

static PG_FEATURES: [PgFeature; 4] = [
    PgFeature {
        environment: "CARGO_FEATURE_PG15",
        pgrx_config: "pg15",
        read_stream: false,
    },
    PgFeature {
        environment: "CARGO_FEATURE_PG16",
        pgrx_config: "pg16",
        read_stream: false,
    },
    PgFeature {
        environment: "CARGO_FEATURE_PG17",
        pgrx_config: "pg17",
        read_stream: true,
    },
    PgFeature {
        environment: "CARGO_FEATURE_PG18",
        pgrx_config: "pg18",
        read_stream: true,
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
    println!("cargo:rustc-check-cfg=cfg(lagodb_pg_injection_points)");
    println!("cargo:rerun-if-env-changed=PGRX_PG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed=PGRX_HOME");
    println!("cargo:rerun-if-env-changed=HOME");
    println!("cargo:rerun-if-changed=csrc/modify/lagodb_modify_table.c");
    println!("cargo:rerun-if-changed=csrc/modify/lagodb_modify_table.h");
    println!("cargo:rerun-if-changed=csrc/modify/lagodb_modify.c");
    println!("cargo:rerun-if-changed=csrc/analyze/lagodb_analyze_sampler.c");
    println!("cargo:rerun-if-changed=csrc/analyze/lagodb_analyze_sampler.h");
    println!("cargo:rerun-if-changed=csrc/vacuum/lagodb_vacuum.c");
    println!("cargo:rerun-if-changed=csrc/vacuum/lagodb_vacuum.h");
    println!("cargo:rerun-if-changed=csrc/vacuum/lagodb_vacuum_probe.c");
    println!("cargo:rerun-if-changed=csrc/vacuum/lagodb_vacuum_probe.h");
    println!("cargo:rerun-if-changed=csrc/compat/lagodb_pg_compat.h");
    println!("cargo:rerun-if-changed=csrc/compat/lagodb_collation.c");
    println!("cargo:rerun-if-changed=csrc/compat/lagodb_collation.h");
    println!("cargo:rerun-if-changed=csrc/compat/lagodb_injection_point.c");
    println!("cargo:rerun-if-changed=csrc/compat/lagodb_injection_point.h");
    println!("cargo:rerun-if-changed=csrc/compat/lagodb_json_category.c");
    println!("cargo:rerun-if-changed=csrc/compat/lagodb_json_category.h");
    println!("cargo:rerun-if-changed=csrc/copy/lagodb_copy.c");
    println!("cargo:rerun-if-changed=csrc/copy/lagodb_copy_prepare.c");
    println!("cargo:rerun-if-changed=csrc/copy/lagodb_copy_datum.c");
    println!("cargo:rerun-if-changed=csrc/copy/lagodb_copy_from.c");
    println!("cargo:rerun-if-changed=csrc/copy/lagodb_copy_to.c");
    println!("cargo:rerun-if-changed=csrc/copy/lagodb_copy.h");
    println!("cargo:rerun-if-changed=csrc/copy/lagodb_copy_write.h");
    println!("cargo:rerun-if-changed=csrc/tuple/lagodb_array.c");
    println!("cargo:rerun-if-changed=csrc/tuple/lagodb_array.h");
    println!("cargo:rerun-if-changed=csrc/index/lagodb_index_am.c");
    println!("cargo:rerun-if-changed=csrc/index/lagodb_index_am.h");

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

    if injection_points_enabled(&pg_config_header) {
        println!("cargo:rustc-cfg=lagodb_pg_injection_points");
    }

    let mut build = cc::Build::new();
    let cppflags = pg_config.cppflags().unwrap_or_else(|error| {
        panic!(
            "{} C preprocessor flags are unavailable: {error}",
            pg_feature.pgrx_config
        )
    });
    let cppflags = shlex::split(&cppflags.to_string_lossy()).unwrap_or_else(|| {
        panic!(
            "{} C preprocessor flags contain invalid shell quoting: {}",
            pg_feature.pgrx_config,
            cppflags.to_string_lossy()
        )
    });
    for flag in cppflags {
        build.flag(flag);
    }
    // PostgreSQL headers are external runtime headers; keep the bridge's
    // warning checks without applying them to PostgreSQL inline functions.
    build.flag("-isystem").flag(include.as_os_str());
    // The COPY bridges own command preparation, raw-field parsing, and the
    // PostgreSQL Text/CSV row encoder. The shared C support gate admits only
    // fully audited majors; local branches own actual ABI differences.
    build
        .file("csrc/copy/lagodb_copy.c")
        .file("csrc/copy/lagodb_copy_prepare.c")
        .file("csrc/copy/lagodb_copy_datum.c")
        .file("csrc/copy/lagodb_copy_from.c")
        .file("csrc/copy/lagodb_copy_to.c")
        .file("csrc/compat/lagodb_collation.c")
        .file("csrc/compat/lagodb_injection_point.c")
        .file("csrc/compat/lagodb_json_category.c")
        .file("csrc/modify/lagodb_modify_table.c")
        .file("csrc/modify/lagodb_modify.c")
        .file("csrc/vacuum/lagodb_vacuum.c")
        .file("csrc/vacuum/lagodb_vacuum_probe.c")
        .file("csrc/tuple/lagodb_array.c")
        .file("csrc/index/lagodb_index_am.c")
        .include(PathBuf::from("csrc/modify"))
        .include(PathBuf::from("csrc/analyze"))
        .include(PathBuf::from("csrc/vacuum"))
        .include(PathBuf::from("csrc/copy"))
        .include(PathBuf::from("csrc/compat"))
        .include(PathBuf::from("csrc/index"))
        .flag_if_supported("-Wno-unused-function")
        .flag_if_supported("-Wno-unused-parameter");
    if pg_feature.read_stream {
        build.file("csrc/analyze/lagodb_analyze_sampler.c");
    }

    build.compile("lagodb_pg_bridges");
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
