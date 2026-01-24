use pgrx::{PgTryBuilder, pg_sys, varlena};
use std::ffi::CStr;
use std::panic::AssertUnwindSafe;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgWrapperError {
    #[error("Invalid string (contains null byte): {0}")]
    NulError(#[from] std::ffi::NulError),

    #[error("Postgres error: {0}")]
    PostgresError(String),
}

/// Result of a catalog tuple update operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogUpdateResult {
    /// The update succeeded.
    Success,
    /// The update failed due to a concurrent modification (optimistic lock conflict).
    Conflict,
}

pub struct PgWrapper;

impl PgWrapper {
    pub fn get_namespace_oid(
        nspname: &CStr,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgWrapperError> {
        let nspname_ptr = nspname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_namespace_oid(nspname_ptr, missing_ok))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn get_namespace_name(
        nspid: pg_sys::Oid,
    ) -> Result<Option<String>, PgWrapperError> {
        unsafe {
            PgTryBuilder::new(move || {
                let ptr = pg_sys::get_namespace_name(nspid);
                if ptr.is_null() {
                    Ok(None)
                } else {
                    let name = CStr::from_ptr(ptr).to_string_lossy().into_owned();
                    pg_sys::pfree(ptr as *mut core::ffi::c_void);
                    Ok(Some(name))
                }
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn get_relname_relid(
        relname: &CStr,
        relnamespace: pg_sys::Oid,
    ) -> Result<pg_sys::Oid, PgWrapperError> {
        let relname_ptr = relname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_relname_relid(relname_ptr, relnamespace))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn range_var_get_relid(
        relation: *const pg_sys::RangeVar,
        lockmode: pg_sys::LOCKMODE,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        // RVR_MISSING_OK = 1 << 0 = 1 (from namespace.h RVROption enum)
        const RVR_MISSING_OK: u32 = 1;
        let flags = if missing_ok { RVR_MISSING_OK } else { 0 };
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::RangeVarGetRelidExtended(
                    *relation,
                    lockmode,
                    flags,
                    None,
                    std::ptr::null_mut(),
                ))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn get_tablespace_oid(
        spcname: &CStr,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgWrapperError> {
        let spcname_ptr = spcname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_tablespace_oid(spcname_ptr, missing_ok))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn table_open(
        relation_id: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<pg_sys::Relation, PgWrapperError> {
        unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::table_open(relation_id, lockmode)))
                .catch_others(|err| {
                    Err(PgWrapperError::PostgresError(format!("{:?}", err)))
                })
                .execute()
        }
    }

    pub fn table_close(
        relation: pg_sys::Relation,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<(), PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::table_close(*relation, lockmode);
                Ok(())
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn catalog_tuple_insert(
        relation: pg_sys::Relation,
        tuple: pg_sys::HeapTuple,
    ) -> Result<(), PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleInsert(*relation, *tuple);
                Ok(())
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn catalog_tuple_update(
        relation: pg_sys::Relation,
        otid: *mut pg_sys::ItemPointerData,
        tuple: pg_sys::HeapTuple,
    ) -> Result<(), PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        let otid = AssertUnwindSafe(otid);
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleUpdate(*relation, *otid, *tuple);
                Ok(())
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn catalog_tuple_update_optimistic(
        relation: pg_sys::Relation,
        otid: *mut pg_sys::ItemPointerData,
        tuple: pg_sys::HeapTuple,
    ) -> Result<CatalogUpdateResult, PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        let otid = AssertUnwindSafe(otid);
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                // CatalogTupleUpdate is exported and correctly handles index updates.
                // It calls simple_heap_update which may throw an ERROR.
                pg_sys::CatalogTupleUpdate(*relation, *otid, *tuple);
                Ok(CatalogUpdateResult::Success)
            })
            .catch_others(|err| {
                // Use the CaughtError provided by PgTryBuilder instead of calling CopyErrorData again.
                // Extract the error message from the caught error.
                let error_msg = format!("{:?}", err);

                // From simple_heap_update:
                // TM_Updated -> elog(ERROR, "tuple concurrently updated");
                let is_conflict = error_msg.contains("tuple concurrently updated");

                // According to pgrx source code documentation:
                // "This should be called by an error handler after it's done processing
                // the error... You are not 'out' of the error subsystem until you have done this."
                //
                // CRITICAL: We must call FlushErrorState() when we handle the error without rethrowing.
                pg_sys::FlushErrorState();

                if is_conflict {
                    Ok(CatalogUpdateResult::Conflict)
                } else {
                    Err(PgWrapperError::PostgresError(error_msg))
                }
            })
            .execute()
        }
    }

    pub fn search_sys_cache_copy(
        cache_id: i32,
        key1: pg_sys::Datum,
        key2: pg_sys::Datum,
        key3: pg_sys::Datum,
        key4: pg_sys::Datum,
    ) -> Result<Option<pg_sys::HeapTuple>, PgWrapperError> {
        unsafe {
            PgTryBuilder::new(move || {
                let tuple =
                    pg_sys::SearchSysCacheCopy(cache_id, key1, key2, key3, key4);
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn sys_cache_get_attr(
        cache_id: i32,
        tuple: pg_sys::HeapTuple,
        attribute_number: i16,
        is_null: *mut bool,
    ) -> Result<pg_sys::Datum, PgWrapperError> {
        let tuple = AssertUnwindSafe(tuple);
        let is_null = AssertUnwindSafe(is_null);
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::SysCacheGetAttr(
                    cache_id,
                    *tuple,
                    attribute_number,
                    *is_null,
                ))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    /// Wrapper for `pg_sys::SearchSysCache1` with error handling.
    ///
    /// Returns `Ok(None)` if the tuple is not found, `Ok(Some(tuple))` if found.
    /// The caller is responsible for calling `release_sys_cache` when done.
    pub fn search_sys_cache1(
        cache_id: i32,
        key1: pg_sys::Datum,
    ) -> Result<Option<pg_sys::HeapTuple>, PgWrapperError> {
        unsafe {
            PgTryBuilder::new(move || {
                let tuple = pg_sys::SearchSysCache1(cache_id, key1);
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    /// Wrapper for `pg_sys::ReleaseSysCache` with error handling.
    pub fn release_sys_cache(tuple: pg_sys::HeapTuple) -> Result<(), PgWrapperError> {
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::ReleaseSysCache(*tuple);
                Ok(())
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn systable_beginscan(
        heap_relation: pg_sys::Relation,
        index_id: pg_sys::Oid,
        index_ok: bool,
        snapshot: pg_sys::Snapshot,
        nkeys: i32,
        key: pg_sys::ScanKey,
    ) -> Result<pg_sys::SysScanDesc, PgWrapperError> {
        let heap_relation = AssertUnwindSafe(heap_relation);
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::systable_beginscan(
                    *heap_relation,
                    index_id,
                    index_ok,
                    snapshot,
                    nkeys,
                    key,
                ))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn systable_getnext(
        sysscan: pg_sys::SysScanDesc,
    ) -> Result<Option<pg_sys::HeapTuple>, PgWrapperError> {
        let sysscan = AssertUnwindSafe(sysscan);
        unsafe {
            PgTryBuilder::new(move || {
                let tuple = pg_sys::systable_getnext(*sysscan);
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn systable_endscan(
        sysscan: pg_sys::SysScanDesc,
    ) -> Result<(), PgWrapperError> {
        let sysscan = AssertUnwindSafe(sysscan);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::systable_endscan(*sysscan);
                Ok(())
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    /// Rust wrapper for the C macro `ReleaseTupleDesc`.
    ///
    /// Releases a tuple descriptor reference. Equivalent to:
    /// ```c
    /// #define ReleaseTupleDesc(tupdesc) \
    ///     do { \
    ///         if ((tupdesc)->tdrefcount >= 0) \
    ///             DecrTupleDescRefCount(tupdesc); \
    ///     } while (0)
    /// ```
    ///
    /// # Safety
    ///
    /// The caller must ensure that `tupdesc` is a valid pointer to a TupleDescData.
    pub unsafe fn release_tuple_desc(tupdesc: pg_sys::TupleDesc) {
        unsafe {
            if (*tupdesc).tdrefcount >= 0 {
                pg_sys::DecrTupleDescRefCount(tupdesc);
            }
        }
    }

    /// Rust wrapper for the C macro `DatumGetHeapTupleHeader`.
    ///
    /// Equivalent to:
    /// ```c
    /// #define DatumGetHeapTupleHeader(X)  ((HeapTupleHeader) PG_DETOAST_DATUM(X))
    /// ```
    ///
    /// # Safety
    ///
    /// The caller must ensure that `datum` is a valid pointer to a composite type.
    pub unsafe fn datum_get_heap_tuple_header(
        datum: pg_sys::Datum,
    ) -> pg_sys::HeapTupleHeader {
        unsafe {
            pg_sys::pg_detoast_datum(datum.cast_mut_ptr()) as pg_sys::HeapTupleHeader
        }
    }

    /// Rust wrapper for the C macro `HeapTupleHeaderGetTypeId`.
    pub unsafe fn heap_tuple_header_get_type_id(
        header: pg_sys::HeapTupleHeader,
    ) -> pg_sys::Oid {
        unsafe { (*header).t_choice.t_datum.datum_typeid }
    }

    /// Rust wrapper for the C macro `HeapTupleHeaderGetTypMod`.
    pub unsafe fn heap_tuple_header_get_typmod(
        header: pg_sys::HeapTupleHeader,
    ) -> i32 {
        unsafe { (*header).t_choice.t_datum.datum_typmod }
    }

    /// Rust wrapper for the C macro `HeapTupleHeaderGetDatumLength`.
    pub unsafe fn heap_tuple_header_get_datum_length(
        header: pg_sys::HeapTupleHeader,
    ) -> u32 {
        unsafe { varlena::varsize(header as *const pg_sys::varlena) as u32 }
    }

    /// Encodes precision and scale into a PostgreSQL NUMERIC type modifier.
    ///
    /// The encoding follows the PostgreSQL convention:
    /// `((precision << 16) | scale) + VARHDRSZ`
    pub fn numeric_typmod(precision: u32, scale: u32) -> i32 {
        (((precision as i32) << 16) | ((scale as i32) & 0x7FF))
            + pg_sys::VARHDRSZ as i32
    }

    /// Decodes precision and scale from a PostgreSQL NUMERIC type modifier.
    ///
    /// Returns `None` if the typmod is less than 0 (e.g., -1 when not specified).
    pub fn numeric_precision_scale(typmod: i32) -> Option<(u32, u32)> {
        if typmod < 0 {
            return None;
        }

        let adjusted = (typmod - pg_sys::VARHDRSZ as i32) as u32;
        let precision = (adjusted >> 16) & 0xFFFF;
        let scale = adjusted & 0x7FF;
        Some((precision, scale))
    }

    /// Rust wrapper for the C macro `ScanKeyInit`.
    ///
    /// Initializes a `ScanKeyData` structure and looks up the function via `fmgr_info`.
    pub unsafe fn scan_key_init(
        entry: *mut pg_sys::ScanKeyData,
        attribute_number: pg_sys::AttrNumber,
        strategy: u16,
        procedure: pg_sys::RegProcedure,
        argument: pg_sys::Datum,
    ) {
        unsafe {
            (*entry).sk_flags = 0;
            (*entry).sk_attno = attribute_number;
            (*entry).sk_strategy = strategy;
            (*entry).sk_subtype = pg_sys::InvalidOid;
            (*entry).sk_collation = pg_sys::InvalidOid;
            (*entry).sk_argument = argument;
            pg_sys::fmgr_info(procedure, &mut (*entry).sk_func);
        }
    }

    /// Converts raw bytes to a PostgreSQL JSON Datum.
    ///
    /// This is a high-performance version of `json_in` that accepts a byte slice
    /// instead of a null-terminated C string, avoiding redundant allocations
    /// and extra copies.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `ptr` points to `len` bytes of valid memory.
    pub unsafe fn json_in_from_bytes(
        ptr: *const u8,
        len: usize,
    ) -> Result<pg_sys::Datum, PgWrapperError> {
        unsafe {
            PgTryBuilder::new(move || {
                let text_ptr = pg_sys::cstring_to_text_with_len(
                    ptr as *const std::os::raw::c_char,
                    len as i32,
                );

                // Note: The internal representation of JSON in PostgreSQL is the same as TEXT.
                // json_in usually validates the JSON content. Since we are coming from
                // a trusted source (Parquet via Lakebase), we skip explicit validation
                // here for maximum performance. If validation is required, one would call
                // makeJsonLexContext and pg_parse_json here if the symbols are available.
                Ok(pg_sys::Datum::from(text_ptr))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    /// Converts raw bytes to a PostgreSQL JSONB Datum.
    ///
    /// This function assumes the input bytes are already a valid PostgreSQL JSONB `varlena`
    /// (including the header). It allocates memory in the PostgreSQL memory context
    /// and performs a direct copy.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `ptr` points to `len` bytes of valid memory
    /// representing a valid JSONB varlena.
    pub unsafe fn jsonb_in_from_bytes(
        ptr: *const u8,
        len: usize,
    ) -> Result<pg_sys::Datum, PgWrapperError> {
        unsafe {
            PgTryBuilder::new(move || {
                // The bytes are already in PostgreSQL's internal JSONB binary format (varlena).
                // We simply allocate memory in the current PostgreSQL memory context and copy.
                let new_ptr = pg_sys::palloc(len);
                std::ptr::copy_nonoverlapping(ptr, new_ptr as *mut u8, len);
                Ok(pg_sys::Datum::from(new_ptr))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    /// Check if WAL logging is needed for general operations.
    ///
    /// This is equivalent to PostgreSQL's `XLogIsNeeded()` macro.
    /// Returns true if `wal_level >= WAL_LEVEL_REPLICA`.
    pub fn xlog_is_needed() -> bool {
        unsafe {
            // XLogIsNeeded() checks wal_level >= WAL_LEVEL_REPLICA
            // In pg_sys, wal_level is an int and the enum values are prefixed.
            pg_sys::wal_level >= pg_sys::WalLevel::WAL_LEVEL_REPLICA as i32
        }
    }

    /// Check if a relation needs WAL logging.
    ///
    /// This is equivalent to PostgreSQL's `RelationNeedsWAL(rel)` macro.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `rel` is a valid pointer to a RelationData.
    pub unsafe fn relation_needs_wal(rel: pg_sys::Relation) -> bool {
        unsafe {
            let rd_rel = (*rel).rd_rel;
            if (*rd_rel).relpersistence != pg_sys::RELPERSISTENCE_PERMANENT as i8 {
                return false;
            }

            if Self::xlog_is_needed() {
                return true;
            }

            // If we are here, wal_level is minimal.
            // Check if relation was created or truncated in current transaction.
            (*rel).rd_createSubid == 0 && (*rel).rd_firstRelfilelocatorSubid == 0
        }
    }

    /// True if we are currently performing crash recovery.
    /// False if we are running standby-mode continuous or archive recovery.
    pub fn is_crash_recovery_only() -> bool {
        unsafe { !pg_sys::ArchiveRecoveryRequested && !pg_sys::StandbyMode }
    }
}

// Manually declare CacheRegisterSyscacheCallback because it is not in pg_sys
unsafe extern "C" {
    pub fn CacheRegisterSyscacheCallback(
        cacheid: std::os::raw::c_int,
        func: Option<
            unsafe extern "C" fn(
                arg: pg_sys::Datum,
                cacheid: std::os::raw::c_int,
                hashvalue: u32,
            ),
        >,
        arg: pg_sys::Datum,
    );
}
