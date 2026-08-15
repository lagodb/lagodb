//! Runtime state for one relation-local mutation session.

use std::collections::BTreeSet;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{DataFile, FormatVersion};
use iceberg_lite::transaction::{RowDeltaValidation, RowLevelCommand};
use parquet::file::properties::WriterProperties;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use super::data_file_sink::DataFileSink;
use super::plan::{ModifyCommand, TargetDependency, ValidationPlan};
use super::row_delete::{RowDeleteClaim, RowDeleteOutput, RowDeleteState};
use super::row_identity::{
    IcebergModifyQueryState, IcebergModifyScanContext, IcebergRowIdentity,
};
use crate::access::isolation::PgTransactionIsolation;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;
use crate::options::IcebergTableOptions;
use crate::relation_binding::RelationShape;
use crate::storage::StorageContext;

/// Final output of one relation-local PostgreSQL ModifyTable session.
struct StatementOutcome {
    command: ModifyCommand,
    target_dependency: TargetDependency,
    new_data_files: Vec<DataFile>,
    row_delete_files: Vec<RowDeleteOutput>,
    referenced_data_files: BTreeSet<String>,
}

impl StatementOutcome {
    fn has_delta(&self) -> bool {
        !self.new_data_files.is_empty() || !self.row_delete_files.is_empty()
    }

    fn row_delta_validation(
        &self,
    ) -> IcebergResult<Option<(RowLevelCommand, Option<i64>)>> {
        if !self.has_delta() {
            return Ok(None);
        }
        let Some(command) = self.command.validation_command() else {
            return Ok(None);
        };
        match self.target_dependency {
            TargetDependency::Independent => Ok(None),
            dependency => Ok(Some((command, dependency.required_snapshot()?))),
        }
    }
}

enum MutationSinks {
    Rows {
        data: DataFileSink,
    },
    Deletes {
        row_delete: RowDeleteState,
    },
    RowsAndDeletes {
        data: DataFileSink,
        row_delete: RowDeleteState,
    },
}

impl MutationSinks {
    fn insert(&mut self, row: TupleSlotRow<'_>) -> IcebergResult<()> {
        match self {
            Self::Rows { data } | Self::RowsAndDeletes { data, .. } => {
                // SAFETY: this callback belongs to the relation for which
                // `DataFileSink` captured its `RelationShape` and bound write
                // plan during construction.
                unsafe { data.append(row) }
            }
            Self::Deletes { .. } => Err(IcebergError::InvariantViolated(
                "insert callback reached a mutation state without a data sink",
            )),
        }
    }

    fn update(
        &mut self,
        identity: IcebergRowIdentity,
        new: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
    ) -> AmResult<MutationOutcome> {
        let Self::RowsAndDeletes { data, row_delete } = self else {
            return Err(IcebergError::InvariantViolated(
                "update callback reached a mutation state without both sinks",
            )
            .into());
        };
        let claim =
            row_delete.claim(identity.file_id(), identity.row_position(), cid)?;
        match claim {
            RowDeleteClaim::FirstTouch => {
                // SAFETY: `new` is the NEW slot supplied by the same
                // relation-local PostgreSQL mutation callback.
                unsafe { data.append(new)? };
                Ok(MutationOutcome::Applied)
            }
            RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(MutationOutcome::AlreadyModifiedInCurrentTransaction {
                modifying_command_id,
            }),
        }
    }

    fn delete(
        &mut self,
        identity: IcebergRowIdentity,
        cid: pg_sys::CommandId,
    ) -> AmResult<MutationOutcome> {
        let row_delete = match self {
            Self::Deletes { row_delete }
            | Self::RowsAndDeletes { row_delete, .. } => row_delete,
            Self::Rows { .. } => {
                return Err(IcebergError::InvariantViolated(
                    "delete callback reached a mutation state without a delete sink",
                )
                .into());
            }
        };
        match row_delete.claim(identity.file_id(), identity.row_position(), cid)? {
            RowDeleteClaim::FirstTouch => Ok(MutationOutcome::Applied),
            RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(MutationOutcome::AlreadyModifiedInCurrentTransaction {
                modifying_command_id,
            }),
        }
    }

    fn data_sink_mut(&mut self) -> IcebergResult<&mut DataFileSink> {
        match self {
            Self::Rows { data } | Self::RowsAndDeletes { data, .. } => Ok(data),
            Self::Deletes { .. } => Err(IcebergError::InvariantViolated(
                "data-file callback reached a mutation state without a data sink",
            )),
        }
    }

    fn abort(&mut self) {
        match self {
            Self::Rows { data } => data.abort(),
            Self::Deletes { row_delete } => row_delete.clear(),
            Self::RowsAndDeletes { data, row_delete } => {
                data.abort();
                row_delete.clear();
            }
        }
    }

    fn finish_data_files(&mut self) -> IcebergResult<Vec<DataFile>> {
        match self {
            Self::Rows { data } | Self::RowsAndDeletes { data, .. } => data.finish(),
            Self::Deletes { .. } => Ok(Vec::new()),
        }
    }

    fn referenced_data_files(&self) -> IcebergResult<BTreeSet<String>> {
        match self {
            Self::Rows { .. } => Ok(BTreeSet::new()),
            Self::Deletes { row_delete }
            | Self::RowsAndDeletes { row_delete, .. } => {
                row_delete.referenced_data_files()
            }
        }
    }

    fn finish_position_deletes(&self) -> IcebergResult<Vec<RowDeleteOutput>> {
        match self {
            Self::Rows { .. } => Ok(Vec::new()),
            Self::Deletes { row_delete }
            | Self::RowsAndDeletes { row_delete, .. } => row_delete.finish(),
        }
    }
}

/// Iceberg mutation state for INSERT/UPDATE/DELETE/MERGE operations.
///
/// Constructed eagerly: storage context, schemas, and writer are all wired up
/// by the time this struct exists. `MutationSinks` encodes the action
/// capability selected by PostgreSQL, while `ValidationPlan` retains the
/// command/dependency decisions needed after writers finish.
pub struct IcebergModifyState {
    rel_oid: pg_sys::Oid,
    file_io: FileIO,
    validation: ValidationPlan,
    sinks: MutationSinks,
}

impl AmModifyState for IcebergModifyState {
    type QueryState = IcebergModifyQueryState;
    type ScanContext = IcebergModifyScanContext;

    fn begin_modify(
        rel: &RelationHandle,
        context: ModifyStateContext<Self::QueryState, Self::ScanContext>,
    ) -> AmResult<Self> {
        Self::open(rel, context)
    }

    fn insert_slot(
        &mut self,
        new: TupleSlotRow<'_>,
        _context: MutationWriteContext,
    ) -> AmResult<()> {
        self.sinks.insert(new)?;
        Ok(())
    }

    fn update_slot(
        &mut self,
        row_id: ItemPointer,
        _old: TupleSlotRow<'_>,
        new: TupleSlotRow<'_>,
        context: MutationUpdateContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let identity = IcebergRowIdentity::decode(&row_id)?;
        self.sinks.update(identity, new, context.cid)
    }

    fn delete_slot(
        &mut self,
        row_id: ItemPointer,
        context: MutationDeleteContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let identity = IcebergRowIdentity::decode(&row_id)?;
        self.sinks.delete(identity, context.cid)
    }

    fn abort_modify(&mut self) {
        // Best-effort in-memory cleanup; persistent artifacts are unwound by
        // ResourceOwner cleanup (see the orphan-file note in `end_modify`).
        self.sinks.abort();
    }

    fn end_modify(&mut self) -> AmResult<()> {
        // Orphan-file note: data files already uploaded before a later flush
        // failure are NOT leaked. Every produced file is registered via
        // `register_object_file_staged()` / `mark_object_file_uploaded()`, and
        // `StorageTransactionResource::on_abort` unlinks staging files or
        // issues remote deletes on abort.
        let outcome = self.finish_statement()?;
        self.stage_statement(outcome)?;
        Ok(())
    }
}

impl AmCopySession for IcebergModifyState {
    fn begin_copy(rel: &RelationHandle) -> AmResult<Self> {
        let query_state = ModifyQueryState::<IcebergModifyQueryState>::new()?;
        Self::open(
            rel,
            ModifyStateContext::<
                IcebergModifyQueryState,
                IcebergModifyScanContext,
            >::independent(
                query_state,
                pg_sys::CmdType::CMD_INSERT,
                ModifyActions::INSERT,
            ),
        )
    }

    fn end_copy(&mut self) -> AmResult<()> {
        let outcome = self.finish_statement()?;
        self.stage_statement(outcome)?;
        Ok(())
    }

    fn abort_copy(&mut self) {
        self.sinks.abort();
    }

    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        self.sinks.insert(row)?;
        Ok(())
    }

    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        let sink = self.sinks.data_sink_mut()?;
        for row in rows.iter() {
            // SAFETY: every slot in this multi-insert callback belongs to the
            // relation whose bound plan owns `sink`.
            unsafe { sink.append(row)? };
        }
        Ok(())
    }
}

impl IcebergModifyState {
    /// Construct a fully-initialized session, performing all storage IO and
    /// schema/writer setup inline. The relation handle is not retained.
    fn open(
        rel: &RelationHandle,
        context: ModifyStateContext<
            IcebergModifyQueryState,
            IcebergModifyScanContext,
        >,
    ) -> AmResult<Self> {
        let (query_state, cmd_type, actions, scan_context) = context.into_parts();
        let command = ModifyCommand::from_pg(cmd_type)?;
        let target_dependency = TargetDependency::from_context(scan_context.as_ref());
        let transaction_isolation = PgTransactionIsolation::current()?;
        let rel_oid = rel.oid();
        // `locator().spc_oid` is the resolved physical tablespace (default
        // tablespaces resolve here), unlike `reltablespace`.
        let ctx = StorageContext::for_tablespace_with_wal(
            rel.locator().spc_oid,
            rel.needs_wal(),
        )?;
        let file_io = ctx.into_file_io();

        // Registers the relation with the per-transaction tracker, rebases
        // pending changes, and returns the base metadata in one step.
        let loaded = TxMetadata::current().begin_table_modify(rel_oid, &file_io)?;
        let writes_position_deletes =
            actions.writes_position_deletes() && scan_context.is_some();
        if writes_position_deletes
            && loaded.metadata.format_version() < FormatVersion::V2
        {
            return Err(IcebergError::NotImplemented(
                "UPDATE/DELETE and MERGE actions that update or delete require Iceberg format v2 or later",
            )
            .into());
        }
        let iceberg_schema = loaded.metadata.current_schema().clone();
        let table_properties = loaded
            .metadata
            .table_properties()
            .map_err(IcebergError::from)?;
        let validation = ValidationPlan::new(
            command,
            target_dependency,
            &table_properties,
            transaction_isolation,
            scan_context.as_ref(),
        );
        let write_options = IcebergTableOptions::for_relation(rel)?;
        let writer_properties = WriterProperties::builder()
            .set_compression(write_options.parquet_compression())
            .build();

        let data_sink = if actions.writes_rows() {
            // The shared relation shape drives the read and write column
            // mappings, keeping dropped-column and type-position handling
            // consistent. DELETE-only sessions do not allocate it.
            let relation_shape = RelationShape::from_relation(rel)?;
            Some(DataFileSink::new(
                &file_io,
                &iceberg_schema,
                &relation_shape,
                &loaded.metadata,
                &writer_properties,
                gucs::mutation_buffer_flush_bytes(),
            )?)
        } else {
            None
        };

        let row_delete_state = if writes_position_deletes {
            let row_registry =
                query_state.update(|state| state.relation_registry(rel_oid))?;
            let modify_state_id = row_registry.begin_modify_state()?;
            let scan_tasks = if loaded.metadata.format_version() == FormatVersion::V3
            {
                scan_context
                    .as_ref()
                    .map(IcebergModifyScanContext::scan_tasks)
            } else {
                None
            };
            Some(RowDeleteState::new(
                loaded.metadata.format_version(),
                &file_io,
                &loaded.metadata,
                &writer_properties,
                row_registry,
                modify_state_id,
                scan_tasks,
            )?)
        } else {
            None
        };

        let sinks = match (data_sink, row_delete_state) {
            (Some(data), None) => MutationSinks::Rows { data },
            (None, Some(row_delete)) => MutationSinks::Deletes { row_delete },
            (Some(data), Some(row_delete)) => {
                MutationSinks::RowsAndDeletes { data, row_delete }
            }
            (None, None) => {
                return Err(IcebergError::InvariantViolated(
                    "mutation state has no row-producing or row-deleting action",
                )
                .into());
            }
        };

        Ok(Self {
            rel_oid,
            file_io,
            validation,
            sinks,
        })
    }

    fn stage_data_files(&self, new_files: Vec<DataFile>) -> IcebergResult<()> {
        TxMetadata::current().stage_data_files(self.rel_oid, new_files, &self.file_io)
    }

    fn stage_row_delete_file(&self, output: RowDeleteOutput) -> IcebergResult<()> {
        for identity in output.removed_delete_files {
            TxMetadata::current().stage_remove_delete_file(
                self.rel_oid,
                identity,
                &self.file_io,
            )?;
        }
        TxMetadata::current().stage_position_delete_file(
            self.rel_oid,
            output.delete_file,
            output.referenced_data_files,
            &self.file_io,
        )
    }

    fn stage_validation(
        &mut self,
        command: RowLevelCommand,
        starting_snapshot_id: Option<i64>,
        referenced_data_files: BTreeSet<String>,
    ) -> IcebergResult<()> {
        let Some(isolation_level) = self.validation.isolation_level else {
            return Err(IcebergError::InvariantViolated(
                "row-level mutation validation has no effective isolation level",
            ));
        };
        let conflict_filter = self
            .validation
            .conflict_scope
            .take()
            .ok_or(IcebergError::InvariantViolated(
                "row-delta conflict scope was already consumed",
            ))?
            .into_predicate();
        let validation =
            RowDeltaValidation::new(command, conflict_filter, isolation_level)
                .with_starting_snapshot_id(starting_snapshot_id)
                .with_referenced_data_files(referenced_data_files);
        TxMetadata::current().stage_row_delta_validation(
            self.rel_oid,
            validation,
            &self.file_io,
        )
    }

    fn finish_statement(&mut self) -> IcebergResult<StatementOutcome> {
        let new_data_files = self.sinks.finish_data_files()?;
        let referenced_data_files = self.sinks.referenced_data_files()?;
        let row_delete_files = self.sinks.finish_position_deletes()?;
        Ok(StatementOutcome {
            command: self.validation.command,
            target_dependency: self.validation.target_dependency,
            new_data_files,
            row_delete_files,
            referenced_data_files,
        })
    }

    fn stage_statement(&mut self, outcome: StatementOutcome) -> IcebergResult<()> {
        let validation = outcome.row_delta_validation()?;

        if !outcome.new_data_files.is_empty() {
            self.stage_data_files(outcome.new_data_files)?;
        }
        for output in outcome.row_delete_files {
            self.stage_row_delete_file(output)?;
        }
        if let Some((command, starting_snapshot_id)) = validation {
            self.stage_validation(
                command,
                starting_snapshot_id,
                outcome.referenced_data_files,
            )?;
        }
        Ok(())
    }
}
