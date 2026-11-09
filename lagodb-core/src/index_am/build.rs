//! `ambuild` / `ambuildempty`, the table scan that feeds a build, and the
//! names of the progress phases a build reports (`ambuildphasename`).

use std::ffi::{CStr, c_char, c_void};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr;

use pgrx::{pg_guard, pg_sys};

use super::options::IndexOptions;
use super::values::IndexValues;
use super::{IndexAccessMethod, IndexAmResult};
use crate::diag::ReportableError;
use crate::handles::{IndexInfoHandle, RelationHandle, ValidItemPointer};

pub trait IndexBuild: Sized {
    fn build(ctx: &IndexBuildContext<'_, Self>) -> IndexAmResult<IndexBuildStats>;

    /// Write an empty index into the init fork of an unlogged index.
    fn build_empty(
        index: &RelationHandle<'_>,
        options: &IndexOptions<'_, Self>,
    ) -> IndexAmResult<()>;
}

/// `ambuildphasename`: names of the `PROGRESS_CREATEIDX_SUBPHASE` values the
/// build reports.
pub trait IndexBuildPhases {
    fn phase_name(phase: i64) -> Option<&'static CStr>;
}

#[derive(Debug, Clone, Copy)]
pub struct IndexBuildStats {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

pub struct IndexBuildTuple<'a> {
    pub tid: ValidItemPointer,
    pub values: IndexValues<'a>,
    pub tuple_is_alive: bool,
}

pub struct IndexBuildContext<'a, A> {
    heap: RelationHandle<'a>,
    index: RelationHandle<'a>,
    index_info: IndexInfoHandle<'a>,
    _access_method: PhantomData<fn() -> A>,
}

impl<'a, A: IndexAccessMethod> IndexBuildContext<'a, A> {
    #[inline]
    pub fn heap(&self) -> &RelationHandle<'a> {
        &self.heap
    }

    #[inline]
    pub fn index(&self) -> &RelationHandle<'a> {
        &self.index
    }

    #[inline]
    pub fn index_info(&self) -> &IndexInfoHandle<'a> {
        &self.index_info
    }

    #[inline]
    pub fn options(&self) -> IndexOptions<'a, A> {
        IndexOptions::of_index(&self.index)
    }

    /// `table_index_build_scan`; returns the number of table rows scanned.
    /// The scan cannot stop early, so a callback error aborts the build.
    pub fn scan_table<F>(
        &self,
        allow_sync: bool,
        progress: bool,
        mut callback: F,
    ) -> f64
    where
        F: FnMut(IndexBuildTuple<'_>) -> IndexAmResult<()>,
    {
        let mut state = BuildScanState {
            callback: &mut callback,
            columns: self.index_info.num_index_attributes(),
        };
        // SAFETY: `state` outlives the synchronous scan, and the trampoline
        // is instantiated for its exact type.
        unsafe {
            pg_sys::table_index_build_scan(
                self.heap.as_raw(),
                self.index.as_raw(),
                self.index_info.as_raw(),
                allow_sync,
                progress,
                Some(build_scan_callback::<F>),
                ptr::from_mut(&mut state).cast(),
                ptr::null_mut(),
            )
        }
    }
}

struct BuildScanState<'f, F> {
    callback: &'f mut F,
    columns: usize,
}

#[pg_guard]
unsafe extern "C-unwind" fn build_scan_callback<F>(
    index: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    tuple_is_alive: bool,
    state: *mut c_void,
) where
    F: FnMut(IndexBuildTuple<'_>) -> IndexAmResult<()>,
{
    let _ = index;
    // SAFETY: `scan_table` passed its `BuildScanState<F>`; PostgreSQL passes
    // a valid heap TID and one value per index column.
    unsafe {
        let state = &mut *state.cast::<BuildScanState<'_, F>>();
        let tuple = IndexBuildTuple {
            tid: ValidItemPointer::from_raw(tid),
            values: IndexValues::from_raw(values, isnull, state.columns),
            tuple_is_alive,
        };
        (state.callback)(tuple).report_unwrap();
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn ambuild<A: IndexAccessMethod>(
    heap: pg_sys::Relation,
    index: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    // SAFETY: PostgreSQL passes live relations and IndexInfo for the call.
    let ctx = unsafe {
        IndexBuildContext::<A> {
            heap: RelationHandle::from_raw(heap),
            index: RelationHandle::from_raw(index),
            index_info: IndexInfoHandle::from_raw(index_info),
            _access_method: PhantomData,
        }
    };
    let stats = A::build(&ctx).report_unwrap();
    // SAFETY: the caller owns the result in its memory context.
    unsafe {
        let result = pg_sys::palloc0(size_of::<pg_sys::IndexBuildResult>())
            .cast::<pg_sys::IndexBuildResult>();
        (*result).heap_tuples = stats.heap_tuples;
        (*result).index_tuples = stats.index_tuples;
        result
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn ambuildempty<A: IndexAccessMethod>(
    index: pg_sys::Relation,
) {
    // SAFETY: PostgreSQL passes a live index relation.
    let index = unsafe { RelationHandle::from_raw(index) };
    let options = IndexOptions::<A>::of_index(&index);
    A::build_empty(&index, &options).report_unwrap();
}

#[pg_guard]
pub(super) extern "C-unwind" fn ambuildphasename<A: IndexBuildPhases>(
    phase: i64,
) -> *mut c_char {
    // PostgreSQL only reads the name (`pg_indexam_progress_phasename`).
    A::phase_name(phase).map_or(ptr::null_mut(), |name| name.as_ptr().cast_mut())
}
