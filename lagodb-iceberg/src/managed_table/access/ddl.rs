use crate::error::IcebergError;
use crate::managed_table::IcebergTableAm;
use crate::managed_table::catalog::table_truncate::ManagedTableTruncate;
use lagodb_core::options::get_tablespace;
use lagodb_core::prelude::*;
use pgrx::pg_sys;

impl AmDdl for IcebergTableAm {
    fn truncate(rel: &RelationHandle<'_>) -> AmResult<()> {
        ManagedTableTruncate::execute(rel, None)?;
        Ok(())
    }

    fn relation_set_new_filelocator(
        rel: &RelationHandle,
        newrlocator: &RelFileLocator,
        persistence: u8,
    ) -> AmResult<(pg_sys::TransactionId, pg_sys::MultiXactId)> {
        if get_tablespace(rel.tablespace().resolved_oid())
            .map_err(IcebergError::from)?
            .is_none()
        {
            // Create the native main fork for PG locator ownership and abort
            // cleanup. Intentionally omit an init fork for UNLOGGED: Iceberg
            // uses that persistence only to disable file WAL, without PG's
            // crash-time reset. See StorageContext's UNLOGGED contract.
            newrlocator.create_storage(persistence)?;
        }
        if !rel.is_being_created_in_current_subtransaction() {
            ManagedTableTruncate::execute(rel, Some(*newrlocator))?;
        }
        Ok((pg_sys::InvalidTransactionId, 0u32.into()))
    }

    fn relation_nontransactional_truncate(rel: &RelationHandle) -> AmResult<()> {
        Self::truncate(rel)
    }
}
