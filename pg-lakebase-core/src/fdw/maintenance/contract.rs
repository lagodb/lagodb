//! Provider contracts and callback-scoped maintenance contexts.

use core::ffi::c_int;
use core::marker::PhantomData;
use core::ptr;

use pgrx::pg_sys;

use super::error::ForeignTableMaintenanceError;
use super::super::provider::ForeignDataWrapper;
use crate::handles::{HeapTupleGuard, RelationHandle};

/// Information returned when a provider supports analyzing one foreign table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForeignAnalyzeSupport {
    total_pages: pg_sys::BlockNumber,
}

impl ForeignAnalyzeSupport {
    /// Declare the current relation size in PostgreSQL blocks.
    #[must_use]
    pub const fn new(total_pages: pg_sys::BlockNumber) -> Self {
        Self { total_pages }
    }

    pub(crate) const fn total_pages(self) -> pg_sys::BlockNumber {
        self.total_pages
    }
}

/// Callback-scoped view used to decide whether a relation supports `ANALYZE`.
pub struct ForeignAnalyzeContext<'a> {
    relation: RelationHandle<'a>,
}

impl<'a> ForeignAnalyzeContext<'a> {
    /// # Safety
    ///
    /// `relation` must be the live relation supplied by PostgreSQL to
    /// `AnalyzeForeignTable` and remain open for `'a`.
    pub(crate) unsafe fn from_raw(relation: pg_sys::Relation) -> Self {
        Self {
            relation: unsafe { RelationHandle::from_raw(relation) },
        }
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
    }
}

/// Population estimates returned together with an `ANALYZE` sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ForeignSampleStatistics {
    total_rows: f64,
    total_dead_rows: f64,
}

impl ForeignSampleStatistics {
    #[must_use]
    pub const fn new(total_rows: f64, total_dead_rows: f64) -> Self {
        Self {
            total_rows,
            total_dead_rows,
        }
    }

    pub(crate) fn validate(self) -> Result<Self, ForeignTableMaintenanceError> {
        if !self.total_rows.is_finite()
            || self.total_rows < 0.0
            || !self.total_dead_rows.is_finite()
            || self.total_dead_rows < 0.0
        {
            return Err(ForeignTableMaintenanceError::framework(
                "foreign ANALYZE returned invalid row population estimates",
            ));
        }
        Ok(self)
    }

    pub(crate) const fn total_rows(self) -> f64 {
        self.total_rows
    }

    pub(crate) const fn total_dead_rows(self) -> f64 {
        self.total_dead_rows
    }
}

/// Callback-scoped `ANALYZE` sample reservoir.
///
/// Tuples pushed into this context transfer ownership from the provider. On a
/// successful callback, ownership transfers again to PostgreSQL's ANALYZE
/// context. If the provider returns an error, this context frees every tuple it
/// already accepted before the FFI boundary reports the error.
pub struct ForeignSampleContext<'a> {
    relation: RelationHandle<'a>,
    log_level: c_int,
    rows: *mut pg_sys::HeapTuple,
    capacity: usize,
    len: usize,
    committed: bool,
    _rows: PhantomData<&'a mut pg_sys::HeapTuple>,
}

impl<'a> ForeignSampleContext<'a> {
    /// # Safety
    ///
    /// All raw arguments must be the live values supplied by PostgreSQL to one
    /// `AcquireSampleRowsFunc` invocation. `rows` must have room for exactly
    /// `target_rows` entries and remain exclusively borrowed for `'a`.
    pub(crate) unsafe fn from_raw(
        relation: pg_sys::Relation,
        log_level: c_int,
        rows: *mut pg_sys::HeapTuple,
        target_rows: c_int,
    ) -> Self {
        Self {
            relation: unsafe { RelationHandle::from_raw(relation) },
            log_level,
            rows,
            capacity: target_rows as usize,
            len: 0,
            committed: false,
            _rows: PhantomData,
        }
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
    }

    /// PostgreSQL message level passed to the sampling callback.
    #[inline]
    pub const fn log_level(&self) -> c_int {
        self.log_level
    }

    /// Maximum number of tuples PostgreSQL requested.
    #[inline]
    pub const fn target_rows(&self) -> usize {
        self.capacity
    }

    /// Number of tuples currently stored in the sample reservoir.
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append an owned sample tuple. The tuple allocation must remain valid
    /// until PostgreSQL finishes the surrounding ANALYZE operation; allocating
    /// it in the callback's current memory context satisfies that requirement.
    pub fn push(
        &mut self,
        tuple: HeapTupleGuard,
    ) -> Result<(), ForeignTableMaintenanceError> {
        if self.len == self.capacity {
            return Err(ForeignTableMaintenanceError::framework(
                "foreign ANALYZE sample exceeded PostgreSQL's target row count",
            ));
        }
        unsafe {
            ptr::write(self.rows.add(self.len), tuple.into_raw());
        }
        self.len += 1;
        Ok(())
    }

    /// Replace one tuple already stored in the reservoir.
    pub fn replace(
        &mut self,
        index: usize,
        tuple: HeapTupleGuard,
    ) -> Result<(), ForeignTableMaintenanceError> {
        if index >= self.len {
            return Err(ForeignTableMaintenanceError::framework(
                "foreign ANALYZE sample replacement index is out of range",
            ));
        }
        let previous = unsafe {
            ptr::replace(self.rows.add(index), tuple.into_raw())
        };
        drop(unsafe { HeapTupleGuard::new(previous) });
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> c_int {
        self.committed = true;
        self.len as c_int
    }
}

impl Drop for ForeignSampleContext<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for index in 0..self.len {
            unsafe {
                pg_sys::heap_freetuple(ptr::read(self.rows.add(index)));
            }
        }
    }
}

/// PostgreSQL `TRUNCATE` dependency behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignTruncateBehavior {
    Restrict,
    Cascade,
}

impl ForeignTruncateBehavior {
    pub(crate) const fn from_pg(behavior: pg_sys::DropBehavior::Type) -> Self {
        if behavior == pg_sys::DropBehavior::DROP_CASCADE {
            Self::Cascade
        } else {
            Self::Restrict
        }
    }
}

/// Same-server foreign relations supplied together by PostgreSQL `TRUNCATE`.
pub struct ForeignTruncateContext<'a> {
    relations: *mut pg_sys::List,
    relation_count: c_int,
    behavior: ForeignTruncateBehavior,
    restart_sequences: bool,
    _relations: PhantomData<&'a pg_sys::List>,
}

impl<'a> ForeignTruncateContext<'a> {
    /// # Safety
    ///
    /// `relations` must be PostgreSQL's live, non-empty list of open foreign
    /// relations for one foreign server and remain valid for the callback.
    pub(crate) unsafe fn from_raw(
        relations: *mut pg_sys::List,
        behavior: pg_sys::DropBehavior::Type,
        restart_sequences: bool,
    ) -> Self {
        Self {
            relations,
            relation_count: unsafe { pg_sys::list_length(relations) },
            behavior: ForeignTruncateBehavior::from_pg(behavior),
            restart_sequences,
            _relations: PhantomData,
        }
    }

    /// Iterate the open relations in PostgreSQL's batch without allocating an
    /// intermediate Rust collection.
    pub fn relations(
        &self,
    ) -> impl ExactSizeIterator<Item = RelationHandle<'_>> + '_ {
        let relations = self.relations;
        (0..self.relation_count).map(move |index| {
            let relation = unsafe { pg_sys::list_nth(relations, index) }
                .cast::<pg_sys::RelationData>();
            unsafe { RelationHandle::from_raw(relation) }
        })
    }

    #[inline]
    pub const fn behavior(&self) -> ForeignTruncateBehavior {
        self.behavior
    }

    #[inline]
    pub const fn restart_sequences(&self) -> bool {
        self.restart_sequences
    }
}

/// Optional `ANALYZE` capability of an FDW provider.
pub trait FdwAnalyze: ForeignDataWrapper + 'static {
    /// Decide whether this relation can be analyzed and provide its current
    /// size. `Ok(None)` asks PostgreSQL to skip the relation.
    fn analyze(
        ctx: &ForeignAnalyzeContext<'_>,
    ) -> Result<Option<ForeignAnalyzeSupport>, ForeignTableMaintenanceError>;

    /// Fill PostgreSQL's sample reservoir and return population estimates.
    fn acquire_sample_rows(
        ctx: &mut ForeignSampleContext<'_>,
    ) -> Result<ForeignSampleStatistics, ForeignTableMaintenanceError>;
}

/// Optional batched `TRUNCATE` capability of an FDW provider.
pub trait FdwTruncate: ForeignDataWrapper + 'static {
    fn truncate(
        ctx: &ForeignTruncateContext<'_>,
    ) -> Result<(), ForeignTableMaintenanceError>;
}
