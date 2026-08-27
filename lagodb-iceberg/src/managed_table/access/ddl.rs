use crate::managed_table::IcebergTableAm;
use crate::managed_table::catalog::metadata_tracker::TxMetadata;
use crate::managed_table::storage::StorageContext;
use lagodb_core::prelude::*;
use pgrx::pg_sys;

impl IcebergTableAm {
    fn stage_truncate(rel: &RelationHandle<'_>) -> AmResult<()> {
        let file_io = StorageContext::for_tablespace_with_wal(
            rel.locator().spc_oid,
            rel.needs_wal(),
        )?
        .into_file_io();
        TxMetadata::current().stage_truncate(rel.oid(), &file_io)?;
        Ok(())
    }
}

impl AmDdl for IcebergTableAm {
    fn relation_set_new_filelocator(
        rel: &RelationHandle,
        _newrlocator: &RelFileLocator,
        _persistence: u8,
    ) -> AmResult<(pg_sys::TransactionId, pg_sys::MultiXactId)> {
        if !rel.is_being_created_in_current_subtransaction() {
            Self::stage_truncate(rel)?;
        }
        Ok((pg_sys::InvalidTransactionId, 0u32.into()))
    }

    fn relation_nontransactional_truncate(rel: &RelationHandle) -> AmResult<()> {
        Self::stage_truncate(rel)
    }
}
