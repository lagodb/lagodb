//! Borrowed inputs for one table-AM index build callback.

use pgrx::pg_sys;

use crate::handles::{IndexInfoHandle, RelationHandle, TableScanDescHandle};

/// PostgreSQL's inputs for one index range scan.
///
/// All handles are borrowed for this callback only. For a serial build, `scan`
/// is `None` and the table AM creates its own scan. `numblocks` retains
/// PostgreSQL's block-count semantics; it is not an ending block number.
pub struct IndexBuildContext<'a> {
    pub table_rel: &'a RelationHandle<'a>,
    pub index_rel: &'a RelationHandle<'a>,
    pub index_info: &'a IndexInfoHandle<'a>,
    pub allow_sync: bool,
    pub anyvisible: bool,
    pub progress: bool,
    pub start_blockno: pg_sys::BlockNumber,
    pub numblocks: pg_sys::BlockNumber,
    pub scan: Option<&'a TableScanDescHandle<'a>>,
}
