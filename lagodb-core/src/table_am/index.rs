//! Index fetch sessions and stateless index build/validate/delete callbacks.

use pgrx::pg_sys;

use super::{AmResult, unsupported_callback};
use crate::handles::{
    IndexBuildCallbackHandle, IndexInfoHandle, ItemPointer, RelationHandle,
    SnapshotHandle, TMIndexDeleteOpHandle, TableScanDescHandle,
    ValidateIndexStateHandle,
};
use crate::tuple::Row;

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

pub trait AmIndexFetchSession {
    fn new(rel: &RelationHandle) -> AmResult<Self>
    where
        Self: Sized;

    fn index_fetch_begin(&mut self) -> AmResult<()>;
    fn index_fetch_reset(&mut self) -> AmResult<()> {
        Ok(())
    }

    fn index_fetch_tuple(
        &mut self,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
        call_again: &mut bool,
        all_dead: &mut bool,
    ) -> AmResult<bool>;

    fn index_fetch_end(&mut self) -> AmResult<()>;
}

/// Index-related callbacks in PostgreSQL's table AM vtable.
///
/// These are not callbacks for PostgreSQL index access methods such as B-tree,
/// GiST, or GIN. Relation arguments may refer to the table relation and, where
/// present, the index relation. `IndexBuildContext::scan` is `None` for serial
/// index builds, where PostgreSQL expects the table AM to create its own scan.
/// The callback handle is permanently bound to the matching index relation and
/// opaque callback state. It is borrowed only for this invocation and must not
/// be retained by a provider.
pub trait AmIndexCallbacks {
    fn index_build_range_scan(
        context: &IndexBuildContext<'_>,
        callback: &mut IndexBuildCallbackHandle,
    ) -> AmResult<f64>
    where
        Self: Sized,
    {
        let _ = (context, callback);
        unsupported_callback("index_build_range_scan")
    }

    fn index_validate_scan(
        table_rel: &RelationHandle,
        index_rel: &RelationHandle,
        index_info: &IndexInfoHandle,
        snapshot: &SnapshotHandle,
        state: &ValidateIndexStateHandle,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (table_rel, index_rel, index_info, snapshot, state);
        unsupported_callback("index_validate_scan")
    }

    fn index_delete_tuples(
        rel: &RelationHandle,
        delstate: &mut TMIndexDeleteOpHandle,
    ) -> AmResult<pg_sys::TransactionId>
    where
        Self: Sized,
    {
        let _ = (rel, delstate);
        unsupported_callback("index_delete_tuples")
    }
}
