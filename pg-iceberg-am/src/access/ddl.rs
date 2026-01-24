use crate::error::{IcebergError, IcebergResult};
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

pub struct IcebergDdl;

impl AmDdl<IcebergError> for IcebergDdl {
    fn relation_set_new_filelocator(
        _rel: &RelationHandle,
        _newrlocator: &RelFileLocator,
        _persistence: u8,
    ) -> IcebergResult<(pg_sys::TransactionId, pg_sys::MultiXactId)> {
        Ok((pg_sys::InvalidTransactionId, 0u32.into()))
    }

    fn relation_nontransactional_truncate(_rel: &RelationHandle) -> IcebergResult<()> {
        Ok(())
    }

    fn relation_copy_data(
        _rel: &RelationHandle,
        _newrlocator: &RelFileLocator,
    ) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("relation_copy_data"))
    }

    fn relation_copy_for_cluster(
        _old_table: &RelationHandle,
        _new_table: &RelationHandle,
        _old_index: &RelationHandle,
        _use_sort: bool,
        _oldest_xmin: pg_sys::TransactionId,
    ) -> IcebergResult<(pg_sys::TransactionId, pg_sys::MultiXactId, f64, f64, f64)> {
        Err(IcebergError::NotImplemented("relation_copy_for_cluster"))
    }
}
