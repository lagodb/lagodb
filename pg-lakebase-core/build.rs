use std::path::PathBuf;

use pgrx_pg_config::Pgrx;

fn main() {
    println!(
        "cargo:rerun-if-changed=csrc/custom/modify/lakebase_node_modify_table.c"
    );
    println!(
        "cargo:rerun-if-changed=csrc/custom/modify/lakebase_node_modify_table.h"
    );
    println!("cargo:rerun-if-changed=csrc/maintenance/vacuum_full.c");
    println!("cargo:rerun-if-changed=csrc/maintenance/vacuum_full.h");
    println!("cargo:rerun-if-changed=csrc/analyze/read_stream_pg17.c");
    println!("cargo:rerun-if-changed=csrc/analyze/read_stream_pg17.h");
    println!("cargo:rerun-if-changed=csrc/analyze/PG17.md");

    // These C forks/adapters are PG17 frameworks. Keep the rest of core
    // buildable for PG16 consumers without compiling or exposing them.
    if std::env::var_os("CARGO_FEATURE_PG17").is_none() {
        return;
    }

    let pgrx = Pgrx::from_config().expect("failed to read pgrx configuration");
    let pg_config = pgrx
        .get("pg17")
        .expect("pgrx has no PostgreSQL 17 configuration");
    let include = pg_config
        .includedir_server()
        .expect("PostgreSQL 17 server include directory is unavailable");

    cc::Build::new()
        .file("csrc/custom/modify/lakebase_node_modify_table.c")
        .file("csrc/maintenance/vacuum_full.c")
        .file("csrc/analyze/read_stream_pg17.c")
        .include(PathBuf::from("csrc/custom/modify"))
        .include(PathBuf::from("csrc/maintenance"))
        .include(PathBuf::from("csrc/analyze"))
        .include(include)
        .flag_if_supported("-Wno-unused-function")
        .flag_if_supported("-Wno-unused-parameter")
        .compile("lakebase_modify_table_pg17");
}
