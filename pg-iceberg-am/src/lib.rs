use pg_lakebase_core::prelude::*;
use pgrx::prelude::*;
use std::sync::OnceLock;

mod access;
pub mod catalog;
pub mod constants;
pub mod error;
pub mod gucs;
pub mod hooks;
pub mod storage;
pub mod wal;

use access::dml::IcebergModify;
use access::index::IcebergIndexFetch;
use access::scan::IcebergScan;

/// Get the cached Iceberg TableAmRoutine pointer.
/// This will initialize the routine if it hasn't been initialized yet.
#[inline]
pub fn get_iceberg_am_routine_ptr() -> *const pg_sys::TableAmRoutine {
    let routine = IcebergTableAm::cached_am_routine();
    &*routine as *const pg_sys::TableAmRoutine
}

// crypto primitive provider initialization required by rustls > v0.22.
// It is not required by every FDW, but only call it when needed.
// ref: https://docs.rs/rustls/latest/rustls/index.html#cryptography-providers
static RUSTLS_CRYPTO_PROVIDER_LOCK: OnceLock<()> = OnceLock::new();

#[allow(dead_code)]
fn setup_rustls_default_crypto_provider() {
    RUSTLS_CRYPTO_PROVIDER_LOCK.get_or_init(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .unwrap()
    });
}

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    setup_rustls_default_crypto_provider();
    gucs::init();
    hooks::init_hooks();
    wal::init_wal_rmgr();
}

// ============================================================================
//  Table Access Method Definition
// ============================================================================

#[pg_table_am(
    version = "0.1.0",
    author = "robertmu",
    website = "https://github.com/robertmu/pg-lakebase"
)]
pub struct IcebergTableAm;

impl TableAccessMethod for IcebergTableAm {
    type ScanSession = IcebergScan;
    type IndexFetchSession = IcebergIndexFetch;
    type DmlSession = IcebergModify;
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // noop
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
