use crate::error::IcebergError;
use pg_lakehouse_core::prelude::*;
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

use access::ddl::IcebergDdl;
use access::dml::IcebergModify;
use access::index::IcebergIndex;
use access::relation::IcebergRelation;
use access::scan::IcebergScan;

/// Wrapper for raw pointer to make it Send + Sync.
/// SAFETY: The TableAmRoutine pointer is allocated once in TopMemoryContext
/// and never deallocated during the lifetime of the PostgreSQL backend.
/// It is read-only after initialization, making it safe to share across threads.
struct AmRoutinePtr(*const pg_sys::TableAmRoutine);
unsafe impl Send for AmRoutinePtr {}
unsafe impl Sync for AmRoutinePtr {}

static ICEBERG_AM_ROUTINE_PTR: OnceLock<AmRoutinePtr> = OnceLock::new();

/// Get the cached Iceberg TableAmRoutine pointer.
/// This will initialize the routine if it hasn't been initialized yet.
#[inline]
pub fn get_iceberg_am_routine_ptr() -> *const pg_sys::TableAmRoutine {
    ICEBERG_AM_ROUTINE_PTR
        .get_or_init(|| {
            // Calling am_routine() triggers initialization via make_table_am_routine
            // which uses internal caching, so we always get the same pointer.
            let routine = IcebergTableAm::am_routine();
            // PgBox dereferences to the inner type, get pointer from reference
            AmRoutinePtr(&*routine as *const pg_sys::TableAmRoutine)
        })
        .0
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
    catalog::init_metadata_tracking();
}

// ============================================================================
//  Table Access Method Definition
// ============================================================================

#[pg_table_am(
    version = "0.1.0",
    author = "robertmu",
    website = "https://github.com/robertmu/pg-lakehouse"
)]
pub struct IcebergTableAm;

impl TableAccessMethod<IcebergError> for IcebergTableAm {
    type ScanState = IcebergScan;
    type RelationState = IcebergRelation;
    type IndexState = IcebergIndex;
    type DdlState = IcebergDdl;
    type ModifyState = IcebergModify;
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
