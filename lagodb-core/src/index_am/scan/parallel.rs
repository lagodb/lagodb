//! Parallel index scans.

#[cfg(any(feature = "pg17", feature = "pg18"))]
use std::ffi::c_int;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;

use pgrx::{pg_guard, pg_sys};

use super::context::IndexScanContext;
use super::contract::IndexScanSession;
use crate::diag::ReportableError;
#[cfg(feature = "pg18")]
use crate::handles::RelationHandle;
#[cfg(feature = "pg18")]
use crate::index_am::options::IndexOptions;
use crate::index_am::{IndexAccessMethod, IndexAmResult};

const MAXIMUM_ALIGNOF: usize = pg_sys::MAXIMUM_ALIGNOF as usize;

/// # Safety
///
/// The type must be valid when shared by several backend processes: no
/// pointers or process-local handles, no reliance on `Drop` (it never
/// runs), independent of its address, and synchronized only through atomics
/// or PostgreSQL shared-memory primitives.
pub unsafe trait IndexParallelShared: Sync + 'static {}

/// The region is a [`Self::Shared`] header followed by
/// [`Self::trailing_size`] bytes. `aminitparallelscan` receives only the
/// region's address, so the leader initializes the trailing bytes later and
/// records that in the header.
pub trait IndexParallelScan: IndexScanSession {
    type Shared: IndexParallelShared;

    fn init_shared() -> Self::Shared;

    /// The leader may call this before its own session has started.
    fn parallel_rescan(
        region: &IndexParallelRegion<'_, Self::Shared>,
    ) -> IndexAmResult<()>;

    #[cfg(feature = "pg16")]
    fn trailing_size() -> usize {
        0
    }

    #[cfg(feature = "pg17")]
    fn trailing_size(nkeys: i32, norderbys: i32) -> usize {
        let _ = (nkeys, norderbys);
        0
    }

    /// A size computed from `options` must use options declared with
    /// `IndexOptionLock::AccessExclusive`, so every process sees the same
    /// size.
    #[cfg(feature = "pg18")]
    fn trailing_size(
        index: &RelationHandle<'_>,
        options: &IndexOptions<'_, Self::AccessMethod>,
        nkeys: i32,
        norderbys: i32,
    ) -> usize {
        let _ = (index, options, nkeys, norderbys);
        0
    }
}

pub struct IndexParallelRegion<'a, T> {
    shared: NonNull<T>,
    _region: PhantomData<&'a T>,
}

impl<'a, T: IndexParallelShared> IndexParallelRegion<'a, T> {
    /// # Safety
    ///
    /// `scan` must be a live parallel-aware scan whose access method
    /// initialized a `T` header.
    unsafe fn of_scan(scan: pg_sys::IndexScanDesc) -> Option<Self> {
        unsafe {
            let parallel = (*scan).parallel_scan;
            if parallel.is_null() {
                return None;
            }
            #[cfg(any(feature = "pg16", feature = "pg17"))]
            let offset = (*parallel).ps_offset;
            #[cfg(feature = "pg18")]
            let offset = (*parallel).ps_offset_am;
            Some(Self {
                shared: NonNull::new_unchecked(
                    parallel.cast::<u8>().add(offset).cast(),
                ),
                _region: PhantomData,
            })
        }
    }

    #[inline]
    pub fn shared(&self) -> &'a T {
        // SAFETY: the header was written by aminitparallelscan and is only
        // shared immutably; `T: Sync` covers concurrent access.
        unsafe { self.shared.as_ref() }
    }

    /// # Safety
    ///
    /// Accesses must stay within the [`IndexParallelScan::trailing_size`]
    /// this access method estimated and be synchronized across processes.
    #[inline]
    pub unsafe fn trailing(&self) -> NonNull<u8> {
        unsafe { self.shared.cast::<u8>().add(shared_size::<T>()) }
    }
}

impl<'a, S: IndexParallelScan> IndexScanContext<'a, S> {
    pub fn parallel_region(&self) -> Option<IndexParallelRegion<'a, S::Shared>> {
        // SAFETY: a parallel scan of this access method was initialized by
        // `aminitparallelscan::<S>`.
        unsafe { IndexParallelRegion::of_scan(self.as_raw()) }
    }
}

/// `MAXALIGN(sizeof(T))`.
const fn shared_size<T>() -> usize {
    (size_of::<T>() + MAXIMUM_ALIGNOF - 1) & !(MAXIMUM_ALIGNOF - 1)
}

/// PostgreSQL places the region at a MAXALIGN offset.
pub(in crate::index_am) fn assert_parallel_shared_layout<S: IndexParallelScan>() {
    const {
        assert!(
            align_of::<S::Shared>() <= MAXIMUM_ALIGNOF,
            "IndexParallelScan::Shared must not need more than MAXALIGN alignment"
        );
    }
}

fn estimate<S: IndexParallelScan>(trailing: usize) -> pg_sys::Size {
    // SAFETY: add_size raises an ERROR on overflow.
    unsafe { pg_sys::add_size(shared_size::<S::Shared>(), trailing) }
}

#[cfg(feature = "pg16")]
#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amestimateparallelscan<A>()
-> pg_sys::Size
where
    A: IndexAccessMethod,
    A::ScanSession: IndexParallelScan,
{
    estimate::<A::ScanSession>(A::ScanSession::trailing_size())
}

#[cfg(feature = "pg17")]
#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amestimateparallelscan<A>(
    nkeys: c_int,
    norderbys: c_int,
) -> pg_sys::Size
where
    A: IndexAccessMethod,
    A::ScanSession: IndexParallelScan,
{
    estimate::<A::ScanSession>(A::ScanSession::trailing_size(nkeys, norderbys))
}

#[cfg(feature = "pg18")]
#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amestimateparallelscan<A>(
    index: pg_sys::Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> pg_sys::Size
where
    A: IndexAccessMethod,
    A::ScanSession: IndexParallelScan,
{
    // SAFETY: the planner passes an open index of this access method.
    let index = unsafe { RelationHandle::from_raw(index) };
    let options = IndexOptions::<A>::of_index(&index);
    estimate::<A::ScanSession>(A::ScanSession::trailing_size(
        &index, &options, nkeys, norderbys,
    ))
}

#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn aminitparallelscan<A>(
    target: *mut c_void,
) where
    A: IndexAccessMethod,
    A::ScanSession: IndexParallelScan,
{
    // SAFETY: PostgreSQL passes the MAXALIGNed region sized by
    // amestimateparallelscan, and the alignment is asserted at registration.
    unsafe {
        target
            .cast::<<A::ScanSession as IndexParallelScan>::Shared>()
            .write(A::ScanSession::init_shared())
    }
}

#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amparallelrescan<A>(
    scan: pg_sys::IndexScanDesc,
) where
    A: IndexAccessMethod,
    A::ScanSession: IndexParallelScan,
{
    // SAFETY: a parallel scan of this access method was initialized by
    // `aminitparallelscan::<A>`.
    let region = unsafe {
        IndexParallelRegion::<<A::ScanSession as IndexParallelScan>::Shared>::of_scan(
            scan,
        )
    };
    let Some(region) = region else {
        unreachable!("index_parallelrescan runs only for parallel scans");
    };
    A::ScanSession::parallel_rescan(&region).report_unwrap();
}
