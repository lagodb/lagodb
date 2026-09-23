use std::{ptr::addr_of, slice};

use super::borrowed::{PgBorrowed, PgNullable};
use crate::catalog::{search_syscache1, search_syscache2};
use crate::diag::PgError;
use crate::wrapper::PgWrapper;
use pgrx::pg_sys;

use super::{RelationColumn, RelationName, RelationTablespace};

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

    /// Queue relcache and dependent-plan invalidation for this relation.
    ///
    /// PostgreSQL processes the notification at the next command counter
    /// increment and manages its commit and abort lifetime.
    pub fn invalidate_relcache(&self) -> Result<(), PgError> {
        // SAFETY: the handle keeps this PostgreSQL relation valid for the call.
        unsafe { PgWrapper::invalidate_relation_cache(self.as_raw()) }
    }

    /// Whether PostgreSQL created this relation in the current subtransaction.
    ///
    /// Table AMs use this to distinguish the initial
    /// `relation_set_new_filelocator` callback from a transactional rewrite of
    /// an existing relation.
    #[inline]
    pub fn is_being_created_in_current_subtransaction(&self) -> bool {
        unsafe {
            self.inner.as_ref().rd_createSubid == pg_sys::GetCurrentSubTransactionId()
        }
    }

    #[inline]
    pub fn access_method_oid(&self) -> pg_sys::Oid {
        unsafe { (*self.rd_rel()).relam }
    }

    #[inline]
    pub fn tablespace(&self) -> RelationTablespace {
        RelationTablespace::from_catalog_oid(unsafe {
            (*self.rd_rel()).reltablespace
        })
    }

    #[inline]
    pub fn namespace_oid(&self) -> pg_sys::Oid {
        unsafe { (*self.rd_rel()).relnamespace }
    }

    /// The relation's physical file locator (`rd_locator`).
    ///
    /// For relation kinds with physical storage, the locator carries the
    /// resolved `spc_oid` PostgreSQL uses for storage. Storage-less relation
    /// kinds do not have a valid locator; callers that need logical tablespace
    /// placement should use [`Self::tablespace`].
    #[inline]
    pub fn locator(&self) -> RelFileLocator {
        unsafe { RelFileLocator::from_raw_unchecked(&(*self.as_raw()).rd_locator) }
    }

    /// Copy the relation name into inline storage, preserving server-encoding
    /// bytes. The returned value is independent of the relation and relcache.
    #[inline]
    pub fn relation_name(&self) -> RelationName {
        // SAFETY: the live relation owns rd_rel. PostgreSQL initializes relname
        // as a zero-padded, NUL-terminated NameData, and copying it does not
        // reenter PostgreSQL or retain a borrow of relcache memory.
        unsafe { RelationName::from_raw(addr_of!((*self.rd_rel()).relname)) }
    }

    #[inline]
    pub fn relkind(&self) -> i8 {
        unsafe { (*self.rd_rel()).relkind }
    }

    /// Role whose user mapping PostgreSQL uses for foreign-table maintenance.
    #[inline]
    pub fn owner_oid(&self) -> pg_sys::Oid {
        unsafe { (*self.rd_rel()).relowner }
    }

    /// Row estimate last persisted by PostgreSQL ANALYZE, or a negative value
    /// when the relation has never been analyzed.
    #[inline]
    pub fn reltuples(&self) -> f32 {
        unsafe { (*self.rd_rel()).reltuples }
    }

    #[inline]
    pub fn toast_relation_oid(&self) -> Option<pg_sys::Oid> {
        let oid = unsafe { (*self.rd_rel()).reltoastrelid };
        (oid != pg_sys::InvalidOid).then_some(oid)
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
    /// Names retain PostgreSQL's server-encoding bytes. Formats that require
    /// UTF-8 must validate them once while binding their schema.
    pub fn live_columns(&self) -> Box<[RelationColumn]> {
        let tup_desc = self.tuple_desc();
        debug_assert!(
            !tup_desc.is_null(),
            "RelationHandle::live_columns: rd_att is NULL"
        );
        // SAFETY: the live relation owns this valid descriptor for the handle's
        // lifetime. The returned metadata copies every borrowed field.
        unsafe { RelationColumn::live_from_tuple_desc(tup_desc) }
    }

    /// Per-attribute `(type oid, typmod)` indexed by `attno - 1`.
    ///
    /// Unlike [`Self::live_columns`], this preserves dropped-column positions
    /// for consumers whose physical tuple layout includes them.
    pub fn attr_types(&self) -> Vec<(pg_sys::Oid, i32)> {
        let tuple_desc = self.tuple_desc();
        debug_assert!(
            !tuple_desc.is_null(),
            "RelationHandle::attr_types: rd_att is NULL"
        );
        // SAFETY: a live relation owns a contiguous `natts` attribute array for
        // the lifetime of this handle.
        let attrs = unsafe {
            slice::from_raw_parts(
                (*tuple_desc).attrs.as_ptr(),
                (*tuple_desc).natts as usize,
            )
        };
        attrs
            .iter()
            .map(|attribute| (attribute.atttypid, attribute.atttypmod))
            .collect()
    }

    /// Largest effective column or explicitly configured extended-statistics
    /// target for this relation.
    ///
    /// PostgreSQL stores an inherited column target as SQL NULL in
    /// `pg_attribute.attstattarget`; `examine_attribute()` converts that NULL
    /// to its internal `-1` sentinel before typanalyze resolves it against
    /// `default_statistics_target`. Dropped columns are excluded. This is a
    /// relation-wide catalog summary, not PostgreSQL's actual per-scan
    /// `targrows` or inherited `childtargrows`.
    pub fn max_statistics_target(&self) -> Result<i32, PgError> {
        let tup_desc = self.tuple_desc();
        debug_assert!(
            !tup_desc.is_null(),
            "RelationHandle::max_statistics_target: rd_att is NULL"
        );
        // SAFETY: a live RelationHandle owns a valid TupleDesc and PostgreSQL's
        // backend-local GUC value is readable for the duration of this call.
        let (attrs, default_target) = unsafe {
            (
                slice::from_raw_parts(
                    (*tup_desc).attrs.as_ptr(),
                    (*tup_desc).natts as usize,
                ),
                pg_sys::default_statistics_target,
            )
        };

        let mut max_target = None;
        for attr in attrs.iter().filter(|attr| !attr.attisdropped) {
            let Some(tuple) = search_syscache2(
                pg_sys::SysCacheIdentifier::ATTNUM as i32,
                pg_sys::Datum::from(self.oid()),
                pg_sys::Datum::from(attr.attnum),
            ) else {
                continue;
            };
            let target = tuple
                .get_attr(pg_sys::Anum_pg_attribute_attstattarget as i16)
                .map_or(default_target, |datum| datum.value() as i16 as i32);
            max_target =
                Some(max_target.map_or(target, |current: i32| current.max(target)));
        }

        for statistics_oid in
            unsafe { PgWrapper::relation_stat_ext_oids(self.as_raw())? }
        {
            let Some(tuple) = search_syscache1(
                pg_sys::SysCacheIdentifier::STATEXTOID as i32,
                pg_sys::Datum::from(statistics_oid),
            ) else {
                continue;
            };
            let Some(datum) =
                tuple.get_attr(pg_sys::Anum_pg_statistic_ext_stxstattarget as i16)
            else {
                continue;
            };
            let target = datum.value() as i16 as i32;
            max_target =
                Some(max_target.map_or(target, |current: i32| current.max(target)));
        }
        Ok(max_target.unwrap_or(0))
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
    /// Opens a table relation with the specified lock mode.
    ///
    /// PostgreSQL rejects indexes, partitioned indexes, and composite types at
    /// this boundary.
    pub fn open_table(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        let rel = PgWrapper::table_open(oid, lock_mode)?;
        Ok(Self { rel, lock_mode })
    }

    /// Opens a table relation while retaining its lock until transaction end.
    ///
    /// Closing the relcache handle with `NoLock` leaves PostgreSQL's lock
    /// manager to release the originally acquired lock at commit or abort.
    pub fn open_table_retain_lock(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        let rel = PgWrapper::table_open(oid, lock_mode)?;
        Ok(Self {
            rel,
            lock_mode: pg_sys::NoLock as _,
        })
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
        unsafe { PgWrapper::relation_close(self.rel, self.lock_mode) };
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

    /// Returns PostgreSQL's snapshot kind.
    #[inline]
    pub fn snapshot_type(&self) -> pg_sys::SnapshotType::Type {
        unsafe { self.inner.as_ref().snapshot_type }
    }

    /// Returns whether this snapshot bypasses normal tuple visibility checks.
    #[inline]
    pub fn is_any(&self) -> bool {
        self.snapshot_type() == pg_sys::SnapshotType::SNAPSHOT_ANY
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
                    inner: slice::from_raw_parts_mut(ptr, len),
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
