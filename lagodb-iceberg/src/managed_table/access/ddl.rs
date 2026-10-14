use crate::managed_table::IcebergTableAm;
use crate::managed_table::catalog::table_truncate::ManagedTableTruncate;
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
        // Ordinary tables own a native main fork in either storage placement.
        // Its presence protects the tablespace before CREATE commits. PG owns
        // abort cleanup, DROP deletion and TRUNCATE locator replacement; the AM
        // must not unlink this file itself. Object data remains remote.
        // Intentionally omit an init fork for UNLOGGED: see StorageContext's
        // UNLOGGED contract. Partitioned roots do not enter this PG callback.
        newrlocator.create_storage(persistence)?;
        if !rel.is_being_created_in_current_subtransaction() {
            ManagedTableTruncate::execute(rel, Some(*newrlocator))?;
        }
        Ok((pg_sys::InvalidTransactionId, 0u32.into()))
    }

    fn relation_nontransactional_truncate(rel: &RelationHandle) -> AmResult<()> {
        Self::truncate(rel)
    }
}
