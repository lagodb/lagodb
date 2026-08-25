//! Catalog-independent Iceberg table-property updates.
//!
//! This value contains only Iceberg format state. The AM and a future writable
//! FDW may construct it from different PostgreSQL option surfaces and publish
//! the resulting transaction through different catalogs.

use std::collections::HashMap;

use crate::error::IcebergResult;
use iceberg_lite::spec::{FormatVersion, TableMetadata};
use iceberg_lite::transaction::{ApplyTransactionAction, Transaction};

/// Effective Iceberg format version and table properties already resolved by
/// a catalog adapter. Applying the update extends the property map, leaving
/// properties not owned by that adapter untouched.
#[derive(Debug, Clone)]
pub(crate) struct PreparedTablePropertyUpdate {
    format_version: FormatVersion,
    effective_properties: HashMap<String, String>,
}

impl PreparedTablePropertyUpdate {
    pub(crate) fn new(
        format_version: FormatVersion,
        effective_properties: HashMap<String, String>,
    ) -> Self {
        Self {
            format_version,
            effective_properties,
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
            .set_properties(self.effective_properties.clone())?
            .build()?
            .metadata)
    }

    pub(crate) fn apply_to_transaction(
        &self,
        mut transaction: Transaction,
    ) -> IcebergResult<Transaction> {
        let mut action = transaction.update_table_properties();
        for (key, value) in &self.effective_properties {
            action = action.set(key.clone(), value.clone());
        }
        transaction = action.apply(transaction)?;
        Ok(transaction)
    }
}
