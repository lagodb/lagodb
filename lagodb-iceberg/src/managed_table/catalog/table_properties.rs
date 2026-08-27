//! AM option adaptation for shared Iceberg table-property updates.

use lagodb_core::handles::RelationHandle;

use crate::engine::write::PreparedTablePropertyUpdate;
use crate::error::IcebergResult;
use crate::managed_table::catalog::metadata_tracker::TxMetadata;
use crate::managed_table::options::ResolvedIcebergOptions;
use crate::managed_table::storage::StorageContext;

pub(crate) struct ManagedTablePropertyUpdate {
    update: PreparedTablePropertyUpdate,
}

impl ManagedTablePropertyUpdate {
    /// Resolve AM-owned PostgreSQL options into a catalog-independent update.
    /// RESET removes an override from `lagodb.table_options`; the AM option
    /// resolver supplies the CREATE default before this object is constructed.
    pub(crate) fn from_options(options: ResolvedIcebergOptions) -> Self {
        Self {
            update: PreparedTablePropertyUpdate::new(
                options.format_version(),
                options.properties(),
            ),
        }
    }

    /// Validate against the transaction-local metadata view and stage the
    /// update without producing metadata files in the DDL statement.
    pub(crate) fn stage_for_relation(
        self,
        rel: &RelationHandle<'_>,
    ) -> IcebergResult<()> {
        let ctx = StorageContext::for_tablespace_with_wal(
            rel.locator().spc_oid,
            rel.needs_wal(),
        )?;
        let file_io = ctx.into_file_io();
        let tracker = TxMetadata::current();
        let loaded = tracker.begin_table_modify(rel.oid(), &file_io)?;
        self.update.validate_base_metadata(&loaded.metadata)?;
        tracker.stage_table_property_update(rel.oid(), self.update, &file_io)
    }
}
