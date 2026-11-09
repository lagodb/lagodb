//! Index scan session traits.

use super::context::{IndexScanContext, ReturnedTupleDesc};
use super::output::{IndexBitmap, IndexMatch, IndexTupleOutput};
use crate::index_am::{IndexAccessMethod, IndexAmResult};

/// State of one index scan, constructed by [`Self::start`] at the first
/// `amrescan`, when the heap relation, snapshot and `xs_want_itup` are set.
///
/// `start` and `rescan` run in the scan's lifecycle context, so their
/// allocations live until the scan ends. A session owns PostgreSQL contexts
/// only as [`OwnedBackendContext`](crate::backend::OwnedBackendContext)s:
/// PostgreSQL may delete other contexts before it drops the session.
pub trait IndexScanSession: Sized + 'static {
    type AccessMethod: IndexAccessMethod;

    fn start(
        ctx: &IndexScanContext<'_, Self>,
        returned: &mut ReturnedTupleDesc<'_>,
    ) -> IndexAmResult<Self>;

    fn rescan(&mut self, ctx: &IndexScanContext<'_, Self>) -> IndexAmResult<()>;

    /// Called only on a normal end; an ERROR just drops the session.
    fn end(&mut self) -> IndexAmResult<()> {
        Ok(())
    }
}

pub trait IndexTupleScan: IndexScanSession {
    /// Runs in the caller's memory context, which the framework neither
    /// switches nor resets; per-row temporaries belong in a context the
    /// session owns and resets.
    fn next(
        &mut self,
        out: &mut IndexTupleOutput<'_, Self>,
    ) -> IndexAmResult<Option<IndexMatch>>;
}

pub trait IndexBitmapScan: IndexScanSession {
    fn collect(&mut self, bitmap: &mut IndexBitmap<'_, Self>) -> IndexAmResult<()>;
}

pub trait IndexMarkRestore: IndexTupleScan {
    fn mark(&mut self) -> IndexAmResult<()>;
    fn restore(&mut self) -> IndexAmResult<()>;
}
