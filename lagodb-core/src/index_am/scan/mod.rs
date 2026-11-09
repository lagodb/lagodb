//! Index scans: the session contract, the descriptor views it receives,
//! fetch outputs, parallel regions, and the PostgreSQL callbacks.

mod callbacks;
mod context;
mod contract;
mod output;
mod parallel;
mod state;

pub(super) use callbacks::{
    ambeginscan, amendscan, amgetbitmap, amgettuple, ammarkpos, amrescan, amrestrpos,
};
pub use context::{IndexScanContext, IndexScanKeys, ReturnedTupleDesc};
pub use contract::{
    IndexBitmapScan, IndexMarkRestore, IndexScanSession, IndexTupleScan,
};
pub use output::{
    IndexBitmap, IndexMatch, IndexOrderByDistance, IndexReturnedValues,
    IndexTupleOutput,
};
pub use parallel::{IndexParallelRegion, IndexParallelScan, IndexParallelShared};
pub(super) use parallel::{
    amestimateparallelscan, aminitparallelscan, amparallelrescan,
    assert_parallel_shared_layout,
};
