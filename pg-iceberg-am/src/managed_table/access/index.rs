use crate::managed_table::IcebergTableAm;
use pg_lakebase_core::prelude::*;

pub struct IcebergIndexFetch;

impl AmIndexFetchSession for IcebergIndexFetch {
    fn new(rel: &RelationHandle) -> AmResult<Self> {
        let _ = rel;
        Ok(Self)
    }

    fn index_fetch_begin(&mut self) -> AmResult<()> {
        unsupported_callback("index_fetch_begin")
    }

    fn index_fetch_tuple(
        &mut self,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
        call_again: &mut bool,
        all_dead: &mut bool,
    ) -> AmResult<bool> {
        let _ = (tid, snapshot, row, call_again, all_dead);
        unsupported_callback("index_fetch_tuple")
    }

    fn index_fetch_end(&mut self) -> AmResult<()> {
        unsupported_callback("index_fetch_end")
    }
}

impl AmIndexCallbacks for IcebergTableAm {}
