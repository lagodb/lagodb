//! Relation-local writable Iceberg foreign modify session.

use iceberg_lite::expr::Predicate;
use iceberg_lite::spec::FormatVersion;
use iceberg_lite::table::Table;
use iceberg_lite::transaction::{RowDeltaValidation, RowLevelCommand};
use lagodb_core::fdw::{
    ForeignInsertBatch, ForeignModifyError, ForeignModifyOperation,
    ForeignModifyOutcome, ForeignModifyState, ForeignRowIdentity, ModifyPlanSlot,
    ModifySlot,
};
use lagodb_core::handles::ItemPointer;
use parquet::file::properties::WriterProperties;
use pgrx::pg_sys;

use super::super::relation::RemoteTableKey;
use super::super::scan::ForeignMutationScan;
use super::super::schema::ForeignSchemaBinding;
use super::super::transaction::ForeignTransaction;
use crate::config::mutation_buffer_flush_bytes;
use crate::error::IcebergError;
use crate::schema::relation::RelationLayout;
use crate::write::{
    DataFileSink, IcebergRowIdentity, MutationSinks, PgTransactionIsolation,
    RowDeleteClaim, RowDeleteState, TxWriteBatch,
};

pub(crate) struct IcebergFdwModifyState {
    key: RemoteTableKey,
    command_id: pg_sys::CommandId,
    starting_snapshot_id: Option<i64>,
    validation: Option<(RowLevelCommand, iceberg_lite::transaction::IsolationLevel)>,
    sinks: MutationSinks,
}

impl IcebergFdwModifyState {
    pub(crate) fn new(
        key: &RemoteTableKey,
        operation: ForeignModifyOperation,
        table: &Table,
        layout: &RelationLayout,
        mutation_scan: Option<&ForeignMutationScan>,
        command_id: pg_sys::CommandId,
    ) -> Result<Self, ForeignModifyError> {
        let writes_rows = matches!(
            operation,
            ForeignModifyOperation::Insert | ForeignModifyOperation::Update
        );
        let writes_deletes = matches!(
            operation,
            ForeignModifyOperation::Update | ForeignModifyOperation::Delete
        );
        let mutation_scan = if writes_deletes {
            Some(mutation_scan.ok_or(IcebergError::InvariantViolated(
                "foreign UPDATE/DELETE has no mutation target scan",
            ))?)
        } else {
            None
        };
        if writes_deletes && table.metadata().format_version() < FormatVersion::V2 {
            return Err(IcebergError::NotImplemented(
                "writable Iceberg foreign-table UPDATE/DELETE requires format v2 or later",
            )
            .into());
        }
        if matches!(operation, ForeignModifyOperation::Update) {
            ForeignSchemaBinding::validate_update(table.metadata().current_schema())?;
        }
        let writer_properties = WriterProperties::builder().build();
        let rows = writes_rows
            .then(|| {
                DataFileSink::new(
                    table.file_io(),
                    table.metadata().current_schema(),
                    layout,
                    table.metadata(),
                    &writer_properties,
                    mutation_buffer_flush_bytes(),
                )
            })
            .transpose()?;
        let deletes = if writes_deletes {
            let registry = ForeignTransaction::row_registry(key)?;
            let scan = mutation_scan
                .as_ref()
                .expect("delete-producing operation retains its scan");
            Some(RowDeleteState::new(
                table.metadata().format_version(),
                table.file_io(),
                table.metadata(),
                &writer_properties,
                registry,
                scan.tasks(),
            )?)
        } else {
            None
        };
        let sinks = MutationSinks::new(rows, deletes)?;
        let validation = match operation {
            ForeignModifyOperation::Insert => None,
            ForeignModifyOperation::Update => {
                let table_isolation = table
                    .metadata()
                    .table_properties()
                    .write_update_isolation_level()
                    .map_err(IcebergError::from)?;
                Some((
                    RowLevelCommand::Update,
                    PgTransactionIsolation::current()?
                        .effective_iceberg(table_isolation),
                ))
            }
            ForeignModifyOperation::Delete => {
                let table_isolation = table
                    .metadata()
                    .table_properties()
                    .write_delete_isolation_level()
                    .map_err(IcebergError::from)?;
                Some((
                    RowLevelCommand::Delete,
                    PgTransactionIsolation::current()?
                        .effective_iceberg(table_isolation),
                ))
            }
        };
        Ok(Self {
            key: key.clone(),
            command_id,
            starting_snapshot_id: mutation_scan
                .as_ref()
                .and_then(|scan| scan.starting_snapshot_id()),
            validation,
            sinks,
        })
    }

    fn identity(
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<IcebergRowIdentity, ForeignModifyError> {
        let ForeignRowIdentity::ItemPointer(pointer) = plan_slot.identity(0)? else {
            return Err(ForeignModifyError::unsupported(
                "Iceberg foreign modify requires an item-pointer row identity",
            ));
        };
        Ok(IcebergRowIdentity::decode(&ItemPointer {
            block_number: pointer.block_number(),
            offset: pointer.offset(),
        })?)
    }

    fn abort(&mut self) {
        self.sinks.abort();
    }
}

impl ForeignModifyState for IcebergFdwModifyState {
    fn batch_size(&self) -> Result<core::ffi::c_int, ForeignModifyError> {
        Ok(1_000)
    }

    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        self.sinks.insert(slot.tuple_row())?;
        Ok(ForeignModifyOutcome::Applied)
    }

    fn insert_batch(
        &mut self,
        batch: &mut ForeignInsertBatch<'_>,
    ) -> Result<(), ForeignModifyError> {
        batch.process_each(|_, slot| self.insert(slot))
    }

    fn update(
        &mut self,
        slot: &mut ModifySlot<'_>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        let identity = Self::identity(plan_slot)?;
        match self
            .sinks
            .update(identity, slot.tuple_row(), self.command_id)?
        {
            RowDeleteClaim::FirstTouch => Ok(ForeignModifyOutcome::Applied),
            RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(ForeignModifyOutcome::SelfModified {
                modifying_command_id,
            }),
        }
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        let identity = Self::identity(plan_slot)?;
        match self.sinks.delete(identity, self.command_id)? {
            RowDeleteClaim::FirstTouch => Ok(ForeignModifyOutcome::Applied),
            RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(ForeignModifyOutcome::SelfModified {
                modifying_command_id,
            }),
        }
    }

    fn finish(&mut self) -> Result<(), ForeignModifyError> {
        // Iceberg foreign tables intentionally do not support AFTER triggers
        // (ROW or STATEMENT). PostgreSQL calls EndForeignModify/EndForeignInsert
        // after queued AFTER events, so publishing here cannot make these writes
        // visible to queries executed by those triggers. Keep file finalization
        // and transaction-local staging at this boundary; supporting AFTER
        // triggers is not currently planned for this provider.
        // TODO(iceberg-fdw-after-triggers): reject their creation in the foreign
        // DDL layer. That guard is not implemented yet, so this support policy
        // is not currently enforced. Managed-table trigger support is separate.
        let referenced_files = self.sinks.referenced_data_files()?;
        let data_files = self.sinks.finish_data_files()?;
        let delete_files = self.sinks.finish_position_deletes()?;
        let has_delta = !data_files.is_empty() || !delete_files.is_empty();
        let validation = self.validation.take().filter(|_| has_delta).map(
            |(command, isolation)| {
                RowDeltaValidation::new(command, Predicate::AlwaysTrue, isolation)
                    .with_starting_snapshot_id(self.starting_snapshot_id)
                    .with_referenced_data_files(referenced_files)
            },
        );
        let batch =
            TxWriteBatch::new(self.command_id, data_files, delete_files, validation)?;
        ForeignTransaction::stage_batch(&self.key, batch)?;
        Ok(())
    }
}

impl Drop for IcebergFdwModifyState {
    fn drop(&mut self) {
        self.abort();
    }
}
