use crate::error::{IcebergError, IcebergResult};
use pg_lakebase_core::prelude::*;

pub struct IcebergIndex;

impl AmIndex<IcebergError> for IcebergIndex {
    fn new(_rel: &RelationHandle) -> IcebergResult<Self> {
        Ok(IcebergIndex)
    }

    fn index_fetch_begin(&mut self) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("index_fetch_begin"))
    }

    fn index_fetch_tuple(
        &mut self,
        _tid: &ItemPointer,
        _snapshot: &SnapshotHandle,
        _row: &mut Row,
        _call_again: &mut bool,
        _all_dead: &mut bool,
    ) -> IcebergResult<bool> {
        Err(IcebergError::NotImplemented("index_fetch_tuple"))
    }

    fn index_fetch_end(&mut self) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("index_fetch_end"))
    }
}
