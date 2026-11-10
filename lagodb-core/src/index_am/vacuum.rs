//! `ambulkdelete` / `amvacuumcleanup`.

use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::size_of;

use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{pg_guard, pg_sys};

use super::options::IndexOptions;
use super::{IndexAccessMethod, IndexAmResult};
use crate::diag::ReportableError;
use crate::handles::{RelationHandle, ValidItemPointer};

pub trait IndexVacuum: Sized {
    fn bulk_delete(
        ctx: &IndexVacuumContext<'_, Self>,
        stats: &mut IndexVacuumStats,
        callback: &BulkDeleteCallbackHandle<'_>,
    ) -> IndexAmResult<()>;

    /// Also called by ANALYZE, with [`IndexVacuumContext::analyze_only`].
    fn vacuum_cleanup(
        ctx: &IndexVacuumContext<'_, Self>,
        stats: &mut IndexVacuumStats,
    ) -> IndexAmResult<()>;
}

pub struct IndexVacuumContext<'a, A> {
    info: &'a pg_sys::IndexVacuumInfo,
    index: RelationHandle<'a>,
    /// PostgreSQL 15 does not pass the table to index VACUUM.
    #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
    heap: RelationHandle<'a>,
    _access_method: PhantomData<fn() -> A>,
}

impl<'a, A: IndexAccessMethod> IndexVacuumContext<'a, A> {
    /// # Safety
    ///
    /// `info` must be the live vacuum info PostgreSQL passed to a callback
    /// of `A`.
    unsafe fn from_raw(info: *mut pg_sys::IndexVacuumInfo) -> Self {
        // SAFETY: VACUUM keeps the index open and locked for the call, and
        // every caller sets the table (`vacuumlazy.c`, `vacuumparallel.c`,
        // `analyze.c`, `index.c`) and keeps it open.
        unsafe {
            let info = &*info;
            Self {
                info,
                index: RelationHandle::from_raw(info.index),
                #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
                heap: RelationHandle::from_raw(info.heaprel),
                _access_method: PhantomData,
            }
        }
    }

    #[inline]
    pub fn index(&self) -> &RelationHandle<'a> {
        &self.index
    }

    #[inline]
    pub fn options(&self) -> IndexOptions<'a, A> {
        IndexOptions::of_index(&self.index)
    }

    #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
    #[inline]
    pub fn heap(&self) -> &RelationHandle<'a> {
        &self.heap
    }

    #[inline]
    pub fn analyze_only(&self) -> bool {
        self.info.analyze_only
    }

    #[inline]
    pub fn report_progress(&self) -> bool {
        self.info.report_progress
    }

    #[inline]
    pub fn estimated_count(&self) -> bool {
        self.info.estimated_count
    }

    #[inline]
    pub fn message_level(&self) -> i32 {
        self.info.message_level
    }

    #[inline]
    pub fn num_heap_tuples(&self) -> f64 {
        self.info.num_heap_tuples
    }

    #[inline]
    pub fn strategy(&self) -> pg_sys::BufferAccessStrategy {
        self.info.strategy
    }

    /// Call once per index page.
    pub fn vacuum_delay_point(&self) {
        // SAFETY: called from a VACUUM or ANALYZE callback.
        unsafe {
            #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
            pg_sys::vacuum_delay_point();
            // Index VACUUM is never the ANALYZE sampling path.
            #[cfg(feature = "pg18")]
            pg_sys::vacuum_delay_point(false);
        }
    }
}

/// The `IndexBulkDeleteResult` passed between VACUUM phases, NULL before the
/// first. Parallel VACUUM copies exactly `sizeof(IndexBulkDeleteResult)`, so
/// it cannot carry access-method extensions.
pub struct IndexVacuumStats {
    raw: *mut pg_sys::IndexBulkDeleteResult,
}

impl IndexVacuumStats {
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut pg_sys::IndexBulkDeleteResult> {
        // SAFETY: a non-null result is PostgreSQL's live result for the call.
        unsafe { self.raw.as_mut() }
    }

    /// Allocates zeroed statistics in the current memory context if none
    /// exist.
    pub fn get_or_insert(&mut self) -> &mut pg_sys::IndexBulkDeleteResult {
        if self.raw.is_null() {
            // SAFETY: PostgreSQL owns the result in the caller's context.
            self.raw = unsafe {
                pg_sys::palloc0(size_of::<pg_sys::IndexBulkDeleteResult>()).cast()
            };
        }
        // SAFETY: non-null and live for the call.
        unsafe { &mut *self.raw }
    }
}

pub struct BulkDeleteCallbackHandle<'a> {
    callback: unsafe extern "C-unwind" fn(pg_sys::ItemPointer, *mut c_void) -> bool,
    state: *mut c_void,
    _call: PhantomData<&'a ()>,
}

impl BulkDeleteCallbackHandle<'_> {
    #[inline]
    pub fn is_dead(&self, tid: ValidItemPointer) -> bool {
        let mut tid = tid.to_pg_sys();
        let callback = self.callback;
        let state = self.state;
        // SAFETY: PostgreSQL's callback/state pair for this call; the guarded
        // closure owns nothing that needs drop.
        unsafe { pg_guard_ffi_boundary(|| callback(&mut tid, state)) }
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn ambulkdelete<A: IndexAccessMethod>(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    // SAFETY: PostgreSQL passes live vacuum info and always supplies the
    // dead-TID callback to ambulkdelete.
    let (ctx, callback) = unsafe {
        (
            IndexVacuumContext::<A>::from_raw(info),
            BulkDeleteCallbackHandle {
                callback: callback.unwrap_unchecked(),
                state: callback_state,
                _call: PhantomData,
            },
        )
    };
    let mut stats = IndexVacuumStats { raw: stats };
    A::bulk_delete(&ctx, &mut stats, &callback).report_unwrap();
    stats.raw
}

#[pg_guard]
pub(super) extern "C-unwind" fn amvacuumcleanup<A: IndexAccessMethod>(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    // SAFETY: PostgreSQL passes live vacuum info.
    let ctx = unsafe { IndexVacuumContext::<A>::from_raw(info) };
    let mut stats = IndexVacuumStats { raw: stats };
    A::vacuum_cleanup(&ctx, &mut stats).report_unwrap();
    stats.raw
}
