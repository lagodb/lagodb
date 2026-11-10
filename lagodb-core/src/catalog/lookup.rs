//! Name and identity lookups through PostgreSQL's namespace and catalog caches.

use std::ffi::{CStr, c_void};
use std::panic::AssertUnwindSafe;
use std::ptr;

use pgrx::pg_sys::RVROption::RVR_MISSING_OK;
use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::{PgError, PgReportError};

pub fn get_namespace_oid(
    nspname: &CStr,
    missing_ok: bool,
) -> Result<pg_sys::Oid, PgError> {
    let nspname_ptr = nspname.as_ptr();
    unsafe {
        PgTryBuilder::new(move || {
            Ok(pg_sys::get_namespace_oid(nspname_ptr, missing_ok))
        })
        .catch_others(|err| Err(PgError::from_caught(err)))
        .execute()
    }
}

pub fn get_namespace_name_bytes(nspid: pg_sys::Oid) -> Option<Vec<u8>> {
    unsafe {
        let ptr = pg_sys::get_namespace_name(nspid);
        if ptr.is_null() {
            None
        } else {
            let name = CStr::from_ptr(ptr).to_bytes().to_vec();
            pg_sys::pfree(ptr.cast());
            Some(name)
        }
    }
}

/// The schema extension `extension` is installed in, or `InvalidOid` if it
/// does not exist.
pub fn get_extension_schema(extension: pg_sys::Oid) -> pg_sys::Oid {
    // SAFETY: the catalog functions accept any OID and report failures through
    // PostgreSQL ERROR, which pgrx's guarded bindings propagate.
    #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
    unsafe {
        pg_sys::get_extension_schema(extension)
    }
    // PostgreSQL 15 keeps `get_extension_schema()` private to extension.c;
    // this is its body.
    // SAFETY: the scan reads one pg_extension tuple while the relation is
    // open, and the guarded bindings propagate PostgreSQL ERROR.
    #[cfg(feature = "pg15")]
    unsafe {
        let lock = pg_sys::AccessShareLock as pg_sys::LOCKMODE;
        let relation = pg_sys::table_open(pg_sys::ExtensionRelationId, lock);
        let mut key = pg_sys::ScanKeyData::default();
        pg_sys::ScanKeyInit(
            &mut key,
            pg_sys::Anum_pg_extension_oid as pg_sys::AttrNumber,
            pg_sys::BTEqualStrategyNumber as pg_sys::StrategyNumber,
            pg_sys::Oid::from(pg_sys::F_OIDEQ),
            pg_sys::Datum::from(extension),
        );
        let scan = pg_sys::systable_beginscan(
            relation,
            pg_sys::Oid::from(pg_sys::ExtensionOidIndexId),
            true,
            ptr::null_mut(),
            1,
            &mut key,
        );
        let tuple = pg_sys::systable_getnext(scan);
        let schema = if tuple.is_null() {
            pg_sys::InvalidOid
        } else {
            (*pg_sys::GETSTRUCT(tuple).cast::<pg_sys::FormData_pg_extension>())
                .extnamespace
        };
        pg_sys::systable_endscan(scan);
        pg_sys::table_close(relation, lock);
        schema
    }
}

pub fn get_relation_oid(relname: &CStr, relnamespace: pg_sys::Oid) -> pg_sys::Oid {
    unsafe { pg_sys::get_relname_relid(relname.as_ptr(), relnamespace) }
}

/// Resolves a `RangeVar` to a relation OID.
///
/// # Safety
///
/// `relation` must point to a valid PostgreSQL `RangeVar` for the duration of
/// the call.
pub unsafe fn range_var_get_relid(
    relation: *const pg_sys::RangeVar,
    lockmode: pg_sys::LOCKMODE,
    missing_ok: bool,
) -> Result<pg_sys::Oid, PgError> {
    let flags = if missing_ok { RVR_MISSING_OK } else { 0 };

    unsafe {
        range_var_get_relid_extended(relation, lockmode, flags, None, ptr::null_mut())
    }
}

/// Calls PostgreSQL's callback-aware `RangeVarGetRelidExtended` and converts
/// an unwind crossing the PostgreSQL FFI boundary into `PgError`.
///
/// # Safety
///
/// `relation` must remain valid for the duration of the call. If non-null,
/// `callback_arg` must satisfy the contract established by `callback`, and
/// `callback` must uphold PostgreSQL's callback contract.
pub(super) unsafe fn range_var_get_relid_extended(
    relation: *const pg_sys::RangeVar,
    lockmode: pg_sys::LOCKMODE,
    flags: u32,
    callback: pg_sys::RangeVarGetRelidCallback,
    callback_arg: *mut c_void,
) -> Result<pg_sys::Oid, PgError> {
    let relation = AssertUnwindSafe(relation);
    let callback_arg = AssertUnwindSafe(callback_arg);

    unsafe {
        PgTryBuilder::new(move || {
            Ok(pg_sys::RangeVarGetRelidExtended(
                *relation,
                lockmode,
                flags,
                callback,
                *callback_arg,
            ))
        })
        .catch_others(|error| Err(PgError::from_caught(error)))
        .execute()
    }
}

/// Resolve and lock an ALTER TABLE target with PostgreSQL's own permission
/// callback. `InvalidOid` means an `IF EXISTS` target is missing. Admission
/// errors keep PostgreSQL's SQLSTATE, message, DETAIL and HINT; an outer hook
/// may add its own context.
///
/// # Safety
///
/// `stmt` must be a live ALTER TABLE statement.
pub unsafe fn alter_table_lookup_relation(
    stmt: *mut pg_sys::AlterTableStmt,
    lockmode: pg_sys::LOCKMODE,
) -> Result<pg_sys::Oid, PgReportError> {
    unsafe {
        PgTryBuilder::new(AssertUnwindSafe(|| {
            Ok(pg_sys::AlterTableLookupRelation(stmt, lockmode))
        }))
        .catch_others(|error| Err(PgError::from_caught(error)))
        .execute()
    }
    .map_err(PgReportError::from_pg_error)
}

pub fn get_tablespace_oid(
    spcname: &CStr,
    missing_ok: bool,
) -> Result<pg_sys::Oid, PgError> {
    let spcname_ptr = spcname.as_ptr();
    unsafe {
        PgTryBuilder::new(move || {
            Ok(pg_sys::get_tablespace_oid(spcname_ptr, missing_ok))
        })
        .catch_others(|err| Err(PgError::from_caught(err)))
        .execute()
    }
}
