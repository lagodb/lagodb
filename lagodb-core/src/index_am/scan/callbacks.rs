//! Index scan callbacks over the session contract.

use std::ffi::c_int;

use pgrx::{PgMemoryContexts, pg_guard, pg_sys};

use super::context::{IndexScanContext, ReturnedTupleDesc};
use super::contract::{
    IndexBitmapScan, IndexMarkRestore, IndexScanSession, IndexTupleScan,
};
use super::output::IndexBitmap;
use super::state::IndexScanOpaque;
use crate::diag::ReportableError;
use crate::handles::ScanDirection;
use crate::index_am::IndexAccessMethod;

#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn ambeginscan<A: IndexAccessMethod>(
    index: pg_sys::Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> pg_sys::IndexScanDesc {
    // SAFETY: PostgreSQL passes an open index; the descriptor is fresh.
    unsafe {
        let scan = pg_sys::RelationGetIndexScan(index, nkeys, norderbys);
        IndexScanOpaque::<A::ScanSession>::publish(scan);
        scan
    }
}

/// `start` and `rescan` run in the lifecycle context: the caller's context at
/// a rescan need not be the one the scan began in (as `gistrescan` uses its
/// `scanCxt`).
#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amrescan<A: IndexAccessMethod>(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: c_int,
    orderbys: pg_sys::ScanKey,
    norderbys: c_int,
) {
    let _ = (nkeys, norderbys);
    // SAFETY: the descriptor was published by ambeginscan::<A>.
    unsafe {
        let opaque = IndexScanOpaque::<A::ScanSession>::of(scan);
        opaque.apply_rescan(scan, keys, orderbys);
        let ctx = IndexScanContext::<A::ScanSession>::new(scan);
        let lifecycle = opaque.context().as_ptr();
        PgMemoryContexts::For(lifecycle)
            .switch_to(|_| {
                opaque.start_or_rescan(
                    || {
                        let mut returned = ReturnedTupleDesc::new(scan, lifecycle);
                        A::ScanSession::start(&ctx, &mut returned)
                    },
                    |session| session.rescan(&ctx),
                )
            })
            .report_unwrap();
    }
}

#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amgettuple<A>(
    scan: pg_sys::IndexScanDesc,
    direction: pg_sys::ScanDirection::Type,
) -> bool
where
    A: IndexAccessMethod,
    A::ScanSession: IndexTupleScan,
{
    // SAFETY: the descriptor was published by ambeginscan::<A>, and
    // PostgreSQL rescans before fetching.
    unsafe {
        let (session, mut out) = IndexScanOpaque::<A::ScanSession>::of(scan)
            .begin_fetch(scan, ScanDirection::from_fetch_raw(direction));
        match session.next(&mut out).report_unwrap() {
            Some(found) => {
                out.publish(found).report_unwrap();
                true
            }
            None => false,
        }
    }
}

#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amgetbitmap<A>(
    scan: pg_sys::IndexScanDesc,
    tbm: *mut pg_sys::TIDBitmap,
) -> i64
where
    A: IndexAccessMethod,
    A::ScanSession: IndexBitmapScan,
{
    // SAFETY: as for amgettuple; PostgreSQL passes a live bitmap.
    unsafe {
        let session = IndexScanOpaque::<A::ScanSession>::of(scan).session_mut();
        let mut bitmap = IndexBitmap::new(IndexScanContext::new(scan), tbm);
        session.collect(&mut bitmap).report_unwrap();
        bitmap.added()
    }
}

#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amendscan<A: IndexAccessMethod>(
    scan: pg_sys::IndexScanDesc,
) {
    // SAFETY: the descriptor was published by ambeginscan::<A>; PostgreSQL
    // frees it right after this callback.
    let ended = unsafe {
        IndexScanOpaque::<A::ScanSession>::end(scan, |session| session.end())
    };
    ended.report_unwrap();
}

#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn ammarkpos<A>(scan: pg_sys::IndexScanDesc)
where
    A: IndexAccessMethod,
    A::ScanSession: IndexMarkRestore,
{
    // SAFETY: marks are taken only on a started scan.
    let session =
        unsafe { IndexScanOpaque::<A::ScanSession>::of(scan).session_mut() };
    session.mark().report_unwrap();
}

#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn amrestrpos<A>(
    scan: pg_sys::IndexScanDesc,
) where
    A: IndexAccessMethod,
    A::ScanSession: IndexMarkRestore,
{
    // SAFETY: restores follow a mark on a started scan.
    let session =
        unsafe { IndexScanOpaque::<A::ScanSession>::of(scan).session_mut() };
    session.restore().report_unwrap();
}
