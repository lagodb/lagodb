use std::panic::AssertUnwindSafe;
use std::{ptr::addr_of, slice};

use super::borrowed::{PgBorrowed, PgNullable};
use crate::diag::PgError;
use crate::wal::xlog_is_needed;
use pgrx::{PgTryBuilder, pg_sys};

use super::{RelationColumn, RelationName, RelationTablespace, TupleDescHandle};

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
        let relation = AssertUnwindSafe(self.as_raw());
        // SAFETY: the handle keeps this PostgreSQL relation valid for the call.
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CacheInvalidateRelcache(*relation);
                Ok(())
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
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

    /// The relation's physical file locator (`rd_locator`, `rd_node` before
    /// PostgreSQL 16).
    ///
    /// For relation kinds with physical storage, the locator carries the
    /// resolved `spc_oid` PostgreSQL uses for storage. Storage-less relation
    /// kinds do not have a valid locator; callers that need logical tablespace
    /// placement should use [`Self::tablespace`].
    #[inline]
    pub fn locator(&self) -> RelFileLocator {
        // SAFETY: the open relation owns its relcache locator.
        unsafe {
            #[cfg(feature = "pg15")]
            let raw = &(*self.as_raw()).rd_node;
            #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
            let raw = &(*self.as_raw()).rd_locator;
            RelFileLocator::from_raw_unchecked(raw)
        }
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

    /// Run a table AM's VACUUM and report it as `heap_vacuum_rel()` does.
    ///
    /// Autovacuum schedules from the dead-tuple and insert counters this report
    /// resets, so a pass that does no work must still go through here.
    pub fn with_vacuum_report<E>(
        &self,
        vacuum: impl FnOnce() -> Result<(), E>,
    ) -> Result<(), E> {
        #[cfg(feature = "pg18")]
        let start_time = unsafe { pg_sys::GetCurrentTimestamp() };
        vacuum()?;

        // pgstat_report_vacuum() always overwrites n_live_tup, but this pass
        // counts no rows. Republish the counter DML and ANALYZE maintain. Stats
        // lost in a crash have no entry, so the last ANALYZE estimate seeds it.
        // SAFETY: PostgreSQL returns a backend-local copy of the stats entry.
        let entry = unsafe { pg_sys::pgstat_fetch_stat_tabentry(self.oid()) };
        let live_tuples = unsafe { entry.as_ref() }.map_or_else(
            || self.reltuples().max(0.0) as pg_sys::PgStat_Counter,
            |entry| {
                #[cfg(feature = "pg15")]
                let live_tuples = entry.n_live_tuples;
                #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
                let live_tuples = entry.live_tuples;
                live_tuples
            },
        );
        // SAFETY: the open relation identifies a local table, and the caller
        // runs inside PostgreSQL's VACUUM of that relation.
        unsafe {
            #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
            pg_sys::pgstat_report_vacuum(self.oid(), false, live_tuples, 0);
            #[cfg(feature = "pg18")]
            pg_sys::pgstat_report_vacuum(
                self.oid(),
                false,
                live_tuples,
                0,
                start_time,
            );
        }
        Ok(())
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
        let rel = self.as_raw();
        // SAFETY: the handle keeps this relcache entry valid for the call.
        unsafe {
            if (*(*rel).rd_rel).relpersistence
                != pg_sys::RELPERSISTENCE_PERMANENT as i8
            {
                return false;
            }
            if xlog_is_needed() {
                return true;
            }
            #[cfg(feature = "pg15")]
            let first_storage_subid = (*rel).rd_firstRelfilenodeSubid;
            #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
            let first_storage_subid = (*rel).rd_firstRelfilelocatorSubid;
            (*rel).rd_createSubid == 0 && first_storage_subid == 0
        }
    }

    /// The relation's tuple descriptor (`rd_att`).
    ///
    /// Borrowed for the lifetime of `self`; PostgreSQL guarantees `rd_att`
    /// stays valid as long as the relation is held open by this handle.
    #[inline]
    pub fn tuple_desc(&self) -> pg_sys::TupleDesc {
        unsafe { self.inner.as_ref().rd_att }
    }

    /// Borrow the relation's tuple descriptor with its lifetime attached to
    /// this open relation.
    #[inline]
    pub fn tuple_desc_handle(&self) -> TupleDescHandle<'a> {
        // SAFETY: PostgreSQL keeps rd_att live while the relation is open.
        unsafe { TupleDescHandle::from_raw(self.tuple_desc()) }
    }

    /// Number of attributes in the relation's tuple descriptor (`rd_att->natts`).
    ///
    /// Lets providers size row buffers without dereferencing the raw `rd_att`
    /// pointer themselves.
    #[inline]
    pub fn natts(&self) -> usize {
        self.tuple_desc_handle().len()
    }

    /// Live (non-dropped) columns of the relation, in ascending attno order.
    ///
    /// Names retain PostgreSQL's server-encoding bytes. Formats that require
    /// UTF-8 must validate them once while binding their schema.
    pub fn live_columns(&self) -> Box<[RelationColumn]> {
        let tup_desc = self.tuple_desc();
        // SAFETY: the live relation owns this valid descriptor for the handle's
        // lifetime. The returned metadata copies every borrowed field.
        unsafe { RelationColumn::live_from_tuple_desc(tup_desc) }
    }

    /// Per-attribute `(type oid, typmod)` indexed by `attno - 1`.
    ///
    /// Unlike [`Self::live_columns`], this preserves dropped-column positions
    /// for consumers whose physical tuple layout includes them.
    pub fn attr_types(&self) -> Vec<(pg_sys::Oid, i32)> {
        self.tuple_desc_handle()
            .attributes()
            .iter()
            .map(|attribute| (attribute.atttypid, attribute.atttypmod))
            .collect()
    }

    #[inline]
    fn rd_rel(&self) -> *mut pg_sys::FormData_pg_class {
        unsafe { self.inner.as_ref().rd_rel }
    }
}

/// PostgreSQL's physical storage identity: `RelFileLocator`, named
/// `RelFileNode` before PostgreSQL 16.
#[cfg(feature = "pg15")]
pub type PgRelFileLocator = pg_sys::RelFileNode;
/// PostgreSQL's physical storage identity: `RelFileLocator`, named
/// `RelFileNode` before PostgreSQL 16.
#[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
pub type PgRelFileLocator = pg_sys::RelFileLocator;

/// Physical storage identity of a relation fork set. `rel_number` is
/// PostgreSQL's `RelFileNumber`, an OID-typed file number.
#[derive(Debug, Clone, Copy)]
pub struct RelFileLocator {
    pub spc_oid: pg_sys::Oid,
    pub db_oid: pg_sys::Oid,
    pub rel_number: pg_sys::Oid,
}

impl RelFileLocator {
    /// # Safety
    ///
    /// `ptr` must be non-null and point to a valid PostgreSQL
    /// [`PgRelFileLocator`].
    #[inline]
    pub unsafe fn from_raw_unchecked(ptr: *const PgRelFileLocator) -> Self {
        unsafe {
            let sys = &*ptr;
            #[cfg(feature = "pg15")]
            let locator = Self {
                spc_oid: sys.spcNode,
                db_oid: sys.dbNode,
                rel_number: sys.relNode,
            };
            #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
            let locator = Self {
                spc_oid: sys.spcOid,
                db_oid: sys.dbOid,
                rel_number: sys.relNumber,
            };
            locator
        }
    }

    /// The PostgreSQL representation of this locator.
    #[inline]
    pub(crate) const fn to_raw(self) -> PgRelFileLocator {
        #[cfg(feature = "pg15")]
        let raw = PgRelFileLocator {
            spcNode: self.spc_oid,
            dbNode: self.db_oid,
            relNode: self.rel_number,
        };
        #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
        let raw = PgRelFileLocator {
            spcOid: self.spc_oid,
            dbOid: self.db_oid,
            relNumber: self.rel_number,
        };
        raw
    }

    /// # Safety
    ///
    /// If `ptr` is non-null, it must point to a valid PostgreSQL
    /// [`PgRelFileLocator`].
    #[inline]
    pub unsafe fn from_raw(ptr: *const PgRelFileLocator) -> Option<Self> {
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

    /// Lend PostgreSQL's active snapshot to `f`, or `None` when none is active.
    ///
    /// The higher-ranked borrow stops the handle from escaping `f`. This method
    /// only reads the active-snapshot stack and does not manage it.
    pub fn with_active<R>(
        f: impl for<'s> FnOnce(Option<&SnapshotHandle<'s>>) -> R,
    ) -> R {
        // SAFETY: ActiveSnapshotSet proves GetActiveSnapshot returns a non-null
        // snapshot, and the calling path keeps it active while `f` runs.
        let active = unsafe {
            pg_sys::ActiveSnapshotSet()
                .then(|| SnapshotHandle::from_raw(pg_sys::GetActiveSnapshot()))
        };
        f(active.as_ref())
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::Snapshot {
        self.inner.as_ptr()
    }

    /// Command boundary for transaction-local changes in an MVCC read.
    /// Special snapshots (ANALYZE/maintenance/self visibility) do not impose
    /// an ordinary executor command boundary.
    pub fn command_id(&self) -> Option<pg_sys::CommandId> {
        // SAFETY: the handle borrows a live PostgreSQL snapshot.
        (self.snapshot_type() == pg_sys::SnapshotType::SNAPSHOT_MVCC)
            .then(|| unsafe { self.inner.as_ref().curcid })
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
