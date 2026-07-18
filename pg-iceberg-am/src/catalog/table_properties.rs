//! Transactional Iceberg table-property updates.
//!
//! PostgreSQL table options are the DDL surface, while Iceberg metadata is the
//! runtime source of truth. This object bridges the two without committing
//! metadata from the utility hook: it captures the fully resolved property
//! state and stages it in [`TxMetadata`] for savepoint-aware, CAS-replayable
//! commit.

use std::collections::HashMap;

use iceberg_lite::spec::{FormatVersion, TableMetadata};
use iceberg_lite::transaction::{ApplyTransactionAction, Transaction};
use pg_lakebase_core::handles::RelationHandle;

use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::IcebergResult;
use crate::options::ResolvedIcebergOptions;
use crate::storage::StorageContext;

/// A fully resolved Iceberg property replacement prepared from PostgreSQL
/// table options.
#[derive(Debug, Clone)]
pub(crate) struct PreparedTablePropertyUpdate {
    format_version: FormatVersion,
    properties: HashMap<String, String>,
}

impl PreparedTablePropertyUpdate {
    pub(crate) fn from_options(options: ResolvedIcebergOptions) -> Self {
        Self {
            format_version: options.format_version(),
            properties: options.properties(),
        }
    }

    /// ALTER currently updates ordinary properties only. Format upgrades need
    /// their own lifecycle contract because Iceberg forbids downgrades and v3
    /// introduces row-lineage state.
    pub(crate) fn validate_base_metadata(
        &self,
        metadata: &TableMetadata,
    ) -> IcebergResult<()> {
        if metadata.format_version() != self.format_version {
            return Err(iceberg_lite::Error::new(
                iceberg_lite::ErrorKind::FeatureUnsupported,
                format!(
                    "ALTER TABLE cannot change Iceberg format version from {} to {}",
                    metadata.format_version(),
                    self.format_version
                ),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn apply_to_metadata(
        &self,
        metadata: &TableMetadata,
    ) -> IcebergResult<TableMetadata> {
        self.validate_base_metadata(metadata)?;
        Ok(metadata
            .clone()
            .into_builder(None)
            .set_properties(self.properties.clone())?
            .build()?
            .metadata)
    }

    pub(crate) fn apply_to_transaction(
        &self,
        mut transaction: Transaction,
    ) -> IcebergResult<Transaction> {
        let mut action = transaction.update_table_properties();
        for (key, value) in &self.properties {
            action = action.set(key.clone(), value.clone());
        }
        transaction = action.apply(transaction)?;
        Ok(transaction)
    }

    /// Validate against the transaction-local metadata view and stage the
    /// property update without producing metadata files in the DDL statement.
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
        self.validate_base_metadata(&loaded.metadata)?;
        tracker.stage_table_property_update(rel.oid(), self, &file_io)
    }
}
