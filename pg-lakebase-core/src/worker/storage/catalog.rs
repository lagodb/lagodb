//! PostgreSQL catalog scanner and syscache dirty flag for the distributed
//! tablespace store reconciler.
//!
//! This module is the only place in the storage worker that performs
//! PostgreSQL FFI for tablespace discovery. It implements
//! [`reconciler::StoreCatalogSource`] by doing a full `pg_tablespace` scan
//! inside an `AccessShareLock` system table scan, and it owns the syscache
//! invalidation callback that wakes the reconcile loop.
//!
//! # Threading and dirty flag
//!
//! Both the syscache callback and the reconcile loop run on the bgworker main
//! thread (the only thread allowed to touch PostgreSQL FFI). The dirty flag is
//! therefore a `Cell<bool>` in `thread_local!` storage; no atomics are needed.

use std::cell::Cell;
use std::ffi::CStr;
use std::sync::Once;

use pgrx::FromDatum;
use pgrx::pg_sys;

use super::reconciler::{StoreCatalogSource, TablespaceStoreSpec};
use crate::catalog::{CatalogRelation, CatalogSnapshot};
use crate::diag::PgError;
use crate::options::tablespace::{TablespaceCacheError, parse_options_to_cached};
use crate::wrapper::CacheRegisterSyscacheCallback;

// ---------------------------------------------------------------------------
//  Dirty flag
// ---------------------------------------------------------------------------

thread_local! {
    /// Set to `true` whenever PostgreSQL invalidates a `pg_tablespace`
    /// syscache entry (CREATE / ALTER / DROP TABLESPACE, including changes
    /// driven by other backends and replicated invalidations).
    ///
    /// The reconcile loop reads + clears this flag with [`take_dirty`] each
    /// iteration; the catalog scan itself is the source of truth, so this is
    /// only a wake-up hint.
    static TABLESPACE_CATALOG_DIRTY: Cell<bool> = const { Cell::new(false) };
}

static INSTALL: Once = Once::new();

unsafe extern "C" fn tablespace_syscache_callback(
    _arg: pg_sys::Datum,
    _cacheid: std::os::raw::c_int,
    _hashvalue: u32,
) {
    // Keep the callback minimal: never touch the registry, never allocate,
    // never run the parser. Anything heavier would run inside arbitrary
    // PostgreSQL code paths that fire invalidation messages.
    TABLESPACE_CATALOG_DIRTY.with(|dirty| dirty.set(true));
}

/// Install the `pg_tablespace` syscache invalidation callback.
///
/// Idempotent: safe to call from `PgTablespaceStoreCatalog::new` and from any
/// other code path that needs the dirty flag to follow catalog changes.
fn install_syscache_callback() {
    INSTALL.call_once(|| unsafe {
        CacheRegisterSyscacheCallback(
            pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
            Some(tablespace_syscache_callback),
            pg_sys::Datum::from(0),
        );
    });
}

/// Read and clear the tablespace dirty flag.
///
/// Returns `true` if the syscache callback fired since the last call.
pub(super) fn take_dirty() -> bool {
    TABLESPACE_CATALOG_DIRTY.with(|dirty| dirty.replace(false))
}

// ---------------------------------------------------------------------------
//  Catalog scanner
// ---------------------------------------------------------------------------

/// Errors raised while loading the desired snapshot from `pg_tablespace`.
#[derive(Debug, thiserror::Error)]
pub(super) enum CatalogLoadError {
    #[error("failed to read pg_tablespace")]
    CatalogScan(#[source] PgError),

    #[error("invalid distributed tablespace '{tablespace}': {source}")]
    InvalidTablespace {
        tablespace: String,
        #[source]
        source: TablespaceCacheError,
    },
}

/// Loads distributed tablespace specs from the live PostgreSQL `pg_tablespace`
/// catalog.
///
/// The struct holds no mutable state of its own; each `load()` does a full
/// catalog scan so it can be used as the desired-state source of a
/// reconciler. Constructing one installs the syscache invalidation callback
/// so subsequent catalog changes set the dirty flag.
pub(super) struct PgTablespaceStoreCatalog;

impl PgTablespaceStoreCatalog {
    pub(super) fn new() -> Self {
        install_syscache_callback();
        Self
    }
}

impl StoreCatalogSource for PgTablespaceStoreCatalog {
    type Error = CatalogLoadError;

    fn load(&mut self) -> Result<Vec<TablespaceStoreSpec>, Self::Error> {
        scan_pg_tablespace()
    }
}

/// Scan `pg_tablespace`, parse Lakebase options, and return one spec per
/// distributed tablespace.
///
/// Native PostgreSQL tablespaces (no Lakebase options, including pg_default
/// and pg_global) are silently skipped.
///
/// The scan must run inside an active transaction; the storage worker calls
/// this via [`pgrx::bgworkers::BackgroundWorker::transaction`].
fn scan_pg_tablespace() -> Result<Vec<TablespaceStoreSpec>, CatalogLoadError> {
    let rel = CatalogRelation::open(
        pg_sys::TableSpaceRelationId,
        pg_sys::AccessShareLock as _,
    )
    .map_err(CatalogLoadError::CatalogScan)?;

    let mut scan = rel
        .begin_scan(
            pg_sys::InvalidOid,
            false,
            CatalogSnapshot::Default,
            std::iter::empty(),
        )
        .map_err(CatalogLoadError::CatalogScan)?;

    let tup_desc = rel.as_handle().tuple_desc();
    let mut specs = Vec::new();

    while let Some(tuple) = scan.get_next().map_err(CatalogLoadError::CatalogScan)? {
        let tablespace_name = read_spcname(tuple.as_raw(), tup_desc);
        let options_vec = read_spcoptions(tuple.as_raw(), tup_desc);

        // `parse_options_to_cached` takes the name by value so the parser
        // and reconciler-facing struct can keep their own copies. We pass
        // `tablespace_name.clone()` and move the original into either the
        // error variant or the resulting spec, eliminating the redundant
        // `.to_string()` we used to do via `cached.tablespace_name()`.
        let cached =
            match parse_options_to_cached(tablespace_name.clone(), options_vec) {
                Ok(Some(cached)) => cached,
                Ok(None) => continue,
                Err(source) => {
                    return Err(CatalogLoadError::InvalidTablespace {
                        tablespace: tablespace_name,
                        source,
                    });
                }
            };

        specs.push(TablespaceStoreSpec {
            store_id: cached.store_id_owned(),
            tablespace_name,
            object_namespace: cached.object_namespace().to_string(),
            base_url: cached.base_url(),
            config: cached.store_config(),
        });
    }

    Ok(specs)
}

fn read_spcname(tuple: pg_sys::HeapTuple, tup_desc: pg_sys::TupleDesc) -> String {
    let mut is_null = false;
    // SAFETY: `tuple` is the live scan tuple owned by `CatalogScan`; the
    // returned datum points into PostgreSQL memory and stays valid until the
    // next `systable_getnext` call. We copy out before that happens.
    let datum = unsafe {
        pg_sys::heap_getattr(
            tuple,
            pg_sys::Anum_pg_tablespace_spcname as i32,
            tup_desc,
            &mut is_null,
        )
    };

    // `pg_tablespace.spcname` is `name NOT NULL`. A null here would indicate
    // catalog corruption.
    debug_assert!(!is_null, "pg_tablespace.spcname is NOT NULL");

    // SAFETY: datum is non-null and points to NameData embedded in the heap
    // tuple.
    unsafe {
        let name_ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
        CStr::from_ptr((*name_ptr).data.as_ptr())
            .to_string_lossy()
            .into_owned()
    }
}

fn read_spcoptions(
    tuple: pg_sys::HeapTuple,
    tup_desc: pg_sys::TupleDesc,
) -> Vec<String> {
    let mut is_null = false;
    // SAFETY: same lifetime story as `read_spcname`.
    let datum = unsafe {
        pg_sys::heap_getattr(
            tuple,
            pg_sys::Anum_pg_tablespace_spcoptions as i32,
            tup_desc,
            &mut is_null,
        )
    };

    if is_null {
        return Vec::new();
    }

    // SAFETY: datum is non-null and points to a valid `text[]` value within
    // the heap tuple. `Vec::<String>::from_datum` copies the data out before
    // the underlying tuple is invalidated.
    unsafe { Vec::<String>::from_datum(datum, false) }.unwrap_or_default()
}
