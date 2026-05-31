use super::borrowed::{PgBorrowed, PgNullable};
use crate::diag::PgError;
use crate::wrapper::PgWrapper;
use pgrx::pg_sys;

#[derive(Debug)]
pub struct RelationHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::RelationData>,
}

impl<'a> RelationHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be a non-null `Relation` pointer that remains valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::Relation) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::Relation {
        self.inner.as_ptr()
    }

    #[inline]
    pub fn oid(&self) -> pg_sys::Oid {
        unsafe { self.inner.as_ref().rd_id }
    }

    #[inline]
    pub fn access_method_oid(&self) -> pg_sys::Oid {
        unsafe { (*self.rd_rel()).relam }
    }

    #[inline]
    pub fn tablespace_oid(&self) -> pg_sys::Oid {
        unsafe { (*self.rd_rel()).reltablespace }
    }

    #[inline]
    pub fn namespace_oid(&self) -> pg_sys::Oid {
        unsafe { (*self.rd_rel()).relnamespace }
    }

    #[inline]
    pub fn relation_name(&self) -> String {
        unsafe {
            let name_ptr = (*self.rd_rel()).relname.data.as_ptr();
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .to_string()
        }
    }

    #[inline]
    pub fn relkind(&self) -> i8 {
        unsafe { (*self.rd_rel()).relkind }
    }

    /// Check if the relation needs WAL logging.
    ///
    /// This is equivalent to PostgreSQL's `RelationNeedsWAL(rel)` macro.
    #[inline]
    pub fn needs_wal(&self) -> bool {
        unsafe { PgWrapper::relation_needs_wal(self.as_raw()) }
    }

    /// The relation's tuple descriptor (`rd_att`).
    ///
    /// Borrowed for the lifetime of `self`; PostgreSQL guarantees `rd_att`
    /// stays valid as long as the relation is held open by this handle.
    #[inline]
    pub fn tuple_desc(&self) -> pg_sys::TupleDesc {
        unsafe { self.inner.as_ref().rd_att }
    }

    /// Number of attributes in the relation's tuple descriptor (`rd_att->natts`).
    ///
    /// Lets providers size row buffers without dereferencing the raw `rd_att`
    /// pointer themselves.
    #[inline]
    pub fn natts(&self) -> usize {
        let tup_desc = self.tuple_desc();
        debug_assert!(!tup_desc.is_null(), "RelationHandle::natts: rd_att is NULL");
        unsafe { (*tup_desc).natts as usize }
    }

    /// Live (non-dropped) columns of the relation, in ascending attno order.
    ///
    /// Each entry pairs the column's 1-based attribute number with its name
    /// (the same string PostgreSQL stores in `attname`). Dropped columns are
    /// skipped, so the result may be shorter than [`natts`](Self::natts).
    ///
    /// This owns the `TupleDesc` / `attrs` / `attname` unsafe boundary so
    /// callers stay on safe Rust. Names are copied into owned `String`s, so
    /// they survive any later memory-context reset.
    pub fn live_columns(&self) -> Vec<(pg_sys::AttrNumber, String)> {
        let tup_desc = self.tuple_desc();
        debug_assert!(
            !tup_desc.is_null(),
            "RelationHandle::live_columns: rd_att is NULL"
        );
        // SAFETY: a live RelationHandle yields a valid `TupleDesc`; `attrs` is a
        // contiguous array of length `natts` for the descriptor's lifetime,
        // which outlives this borrow.
        let attrs = unsafe {
            std::slice::from_raw_parts(
                (*tup_desc).attrs.as_ptr(),
                (*tup_desc).natts as usize,
            )
        };

        let mut live = Vec::with_capacity(attrs.len());
        for attr in attrs {
            if attr.attisdropped {
                continue;
            }
            // SAFETY: `attname.data` is a NUL-terminated `NameData` array owned
            // by the descriptor and valid for the borrow of `attrs`. Copied
            // into an owned `String` immediately so it survives later context
            // resets.
            let name = unsafe {
                std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                    .to_string_lossy()
                    .into_owned()
            };
            live.push((attr.attnum, name));
        }
        live
    }

    #[inline]
    fn rd_rel(&self) -> *mut pg_sys::FormData_pg_class {
        unsafe { self.inner.as_ref().rd_rel }
    }
}

/// RAII guard for an opened PostgreSQL relation.
///
/// This type only owns the open/close lifecycle. Catalog-specific operations
/// live in `crate::catalog::CatalogRelation`.
#[derive(Debug)]
pub struct RelationGuard {
    rel: pg_sys::Relation,
    lock_mode: pg_sys::LOCKMODE,
}

impl RelationGuard {
    /// Opens a relation with the specified lock mode.
    pub fn open(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        let rel = PgWrapper::table_open(oid, lock_mode)?;
        Ok(Self { rel, lock_mode })
    }

    /// Get a `RelationHandle` from this guard.
    ///
    /// The returned handle borrows from this guard, ensuring the relation
    /// remains open while the handle is in use.
    #[inline]
    pub fn as_handle(&self) -> RelationHandle<'_> {
        unsafe { RelationHandle::from_raw(self.rel) }
    }

    /// Get the raw relation pointer.
    #[inline]
    pub fn as_raw(&self) -> pg_sys::Relation {
        self.rel
    }
}

impl Drop for RelationGuard {
    fn drop(&mut self) {
        // Drop cannot report PostgreSQL errors; relation close is best-effort
        // cleanup at this point.
        let _ = unsafe { PgWrapper::table_close(self.rel, self.lock_mode) };
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RelFileLocator {
    pub spc_oid: pg_sys::Oid,
    pub db_oid: pg_sys::Oid,
    pub rel_number: pg_sys::RelFileNumber,
}

impl RelFileLocator {
    /// # Safety
    ///
    /// `ptr` must be non-null and point to a valid PostgreSQL
    /// `RelFileLocator`.
    #[inline]
    pub unsafe fn from_raw_unchecked(ptr: *const pg_sys::RelFileLocator) -> Self {
        unsafe {
            let sys = &*ptr;
            Self {
                spc_oid: sys.spcOid,
                db_oid: sys.dbOid,
                rel_number: sys.relNumber,
            }
        }
    }

    /// # Safety
    ///
    /// If `ptr` is non-null, it must point to a valid PostgreSQL
    /// `RelFileLocator`.
    #[inline]
    pub unsafe fn from_raw(ptr: *const pg_sys::RelFileLocator) -> Option<Self> {
        unsafe {
            if ptr.is_null() {
                None
            } else {
                Some(Self::from_raw_unchecked(ptr))
            }
        }
    }
}

/// Safe wrapper for PostgreSQL Snapshot.
#[derive(Debug)]
pub struct SnapshotHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::SnapshotData>,
}

impl<'a> SnapshotHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be a non-null `Snapshot` pointer that remains valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::Snapshot) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::Snapshot {
        self.inner.as_ptr()
    }

    #[inline]
    pub fn xmin(&self) -> pg_sys::TransactionId {
        unsafe { self.inner.as_ref().xmin }
    }

    #[inline]
    pub fn xmax(&self) -> pg_sys::TransactionId {
        unsafe { self.inner.as_ref().xmax }
    }
}

/// Safe wrapper for PostgreSQL BufferAccessStrategy.
#[derive(Debug)]
pub struct BufferAccessStrategyHandle<'a> {
    inner: PgNullable<'a, pg_sys::BufferAccessStrategyData>,
}

impl<'a> BufferAccessStrategyHandle<'a> {
    /// # Safety
    ///
    /// If `ptr` is non-null, it must remain valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::BufferAccessStrategy) -> Self {
        Self {
            inner: unsafe { PgNullable::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::BufferAccessStrategy {
        self.inner.as_ptr()
    }
}

/// Borrowed wrapper for PostgreSQL VacuumParams.
#[derive(Debug)]
pub struct VacuumParamsHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::VacuumParams>,
}

impl<'a> VacuumParamsHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::VacuumParams) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::VacuumParams {
        self.inner.as_ptr()
    }
}

impl AsRef<pg_sys::VacuumParams> for VacuumParamsHandle<'_> {
    #[inline]
    fn as_ref(&self) -> &pg_sys::VacuumParams {
        unsafe { self.inner.as_ref() }
    }
}

/// Safe wrapper for attribute widths array.
#[derive(Debug)]
pub struct AttrWidthsHandle<'a> {
    inner: &'a mut [i32],
}

impl<'a> AttrWidthsHandle<'a> {
    /// # Safety
    ///
    /// If `ptr` is non-null, it must be valid for `len` contiguous `i32`
    /// elements and uniquely borrowed for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut i32, len: usize) -> Option<Self> {
        unsafe {
            if ptr.is_null() {
                None
            } else {
                Some(Self {
                    inner: std::slice::from_raw_parts_mut(ptr, len),
                })
            }
        }
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [i32] {
        self.inner
    }
}

/// Safe wrapper for varlena pointer.
#[derive(Debug)]
pub struct VarlenaHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::varlena>,
}

impl<'a> VarlenaHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::varlena) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::varlena {
        self.inner.as_ptr()
    }
}
