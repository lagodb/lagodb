//! Iceberg mutation operations.
//!
//! Implements INSERT/UPDATE/DELETE/MERGE for Iceberg tables. INSERT writes
//! Parquet data files; DELETE writes position delete files; UPDATE writes both
//! a position delete for the old row and a data-file row for the new version;
//! MERGE may combine those outcomes. All files are staged in `TxMetadata` and
//! committed through iceberg-lite's transaction API.
//!
//! [`DataFileSink`] owns the slot -> Parquet data-file pipeline; [`IcebergModifyState`]
//! is the AM session that wires tuple callbacks to the sink, crosses the
//! `IcebergError -> AmError` boundary, and stages finished files into the
//! per-transaction Iceberg metadata. All initialization happens in
//! [`IcebergModifyState::open`], so `begin_modify` is a no-op.

mod data_file_sink;
mod row_delete;
mod row_identity;

use std::collections::BTreeSet;
use std::sync::Arc;

use iceberg_lite::expr::Predicate;
use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{DataFile, FormatVersion};
use iceberg_lite::transaction::{
    IsolationLevel, RowDeltaValidation, RowLevelCommand,
};

use parquet::file::properties::WriterProperties;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use crate::access::column_mapping::RelationShape;
use crate::access::isolation::PgTransactionIsolation;
use crate::access::scan::PlannedScanTasks;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::catalog::row_mutations::{
    ModifyStateId, RelationRowRegistry, RowMutationClaim,
};
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;
use crate::options::IcebergTableOptions;
use crate::storage::StorageContext;

#[cfg(test)]
use crate::catalog::row_mutations::{ICEBERG_FILE_ID_BITS, IcebergFileId};
#[cfg(test)]
use pg_lakebase_core::api::TRIGGER_ROW_BLOCK_BASE;

use self::data_file_sink::DataFileSink;
use self::row_delete::{PositionDeleteAccumulator, RowDeleteOutput, RowDeleteSink};
use self::row_identity::IcebergRowIdentity;
pub use self::row_identity::{
    IcebergFileSource, IcebergModifyQueryState, IcebergModifyScanContext,
};

#[cfg(test)]
use self::row_identity::{FILE_MASK, MAX_POSITION};

#[derive(Debug)]
enum ConflictValidationScope {
    StaticTarget(Predicate),
    WholeTable,
}

impl ConflictValidationScope {
    fn from_predicate(predicate: Predicate) -> Self {
        if predicate == Predicate::AlwaysTrue {
            Self::WholeTable
        } else {
            Self::StaticTarget(predicate)
        }
    }

    fn into_predicate(self) -> Predicate {
        match self {
            Self::StaticTarget(predicate) => predicate,
            Self::WholeTable => Predicate::AlwaysTrue,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModifyCommand {
    Insert,
    Delete,
    Update,
    Merge,
}

impl ModifyCommand {
    fn from_pg(cmd_type: pg_sys::CmdType::Type) -> IcebergResult<Self> {
        match cmd_type {
            pg_sys::CmdType::CMD_INSERT => Ok(Self::Insert),
            pg_sys::CmdType::CMD_DELETE => Ok(Self::Delete),
            pg_sys::CmdType::CMD_UPDATE => Ok(Self::Update),
            pg_sys::CmdType::CMD_MERGE => Ok(Self::Merge),
            _ => Err(IcebergError::NotImplemented(
                "unsupported PostgreSQL mutation command for Iceberg table",
            )),
        }
    }

    fn validation_command(self) -> Option<RowLevelCommand> {
        match self {
            Self::Insert => None,
            Self::Delete => Some(RowLevelCommand::Delete),
            Self::Update => Some(RowLevelCommand::Update),
            Self::Merge => Some(RowLevelCommand::Merge),
        }
    }

    fn table_isolation_level(
        self,
        table_properties: &iceberg_lite::spec::TableProperties,
    ) -> Option<IsolationLevel> {
        match self {
            Self::Insert => None,
            Self::Delete => Some(table_properties.write_delete_isolation_level),
            Self::Update => Some(table_properties.write_update_isolation_level),
            Self::Merge => Some(table_properties.write_merge_isolation_level),
        }
    }

    fn effective_isolation_level(
        self,
        table_properties: &iceberg_lite::spec::TableProperties,
        transaction_isolation: PgTransactionIsolation,
    ) -> Option<IsolationLevel> {
        self.table_isolation_level(table_properties)
            .map(|table_isolation| {
                transaction_isolation.effective_iceberg(table_isolation)
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetDependency {
    Independent,
    ReadRequired(Option<i64>),
}

impl TargetDependency {
    fn from_context(scan_context: Option<&IcebergModifyScanContext>) -> Self {
        scan_context.map_or(Self::Independent, |context| {
            Self::ReadRequired(context.starting_snapshot_id)
        })
    }

    fn required_snapshot(self) -> IcebergResult<Option<i64>> {
        match self {
            Self::ReadRequired(snapshot_id) => Ok(snapshot_id),
            Self::Independent => Err(IcebergError::InvariantViolated(
                "independent mutation has no target snapshot",
            )),
        }
    }
}

/// Final output of one relation-local PostgreSQL ModifyTable session.
///
/// Target dependency is explicit: the owning relation execution decides
/// whether the finalized plan needs a target read, and its pinned scan context
/// supplies the snapshot used for validation.
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

/// Iceberg mutation state for INSERT/UPDATE/DELETE/MERGE operations.
///
/// Constructed eagerly: storage context, schemas, and writer are all wired up
/// by the time this struct exists.
pub struct IcebergModifyState {
    /// OID of the relation being modified.
    rel_oid: pg_sys::Oid,
    /// File IO for staging produced data files into transaction metadata.
    file_io: FileIO,
    command: ModifyCommand,
    target_dependency: TargetDependency,
    isolation_level: Option<IsolationLevel>,
    conflict_scope: Option<ConflictValidationScope>,
    /// The slot -> data-file production pipeline.
    data_sink: Option<DataFileSink>,
    row_delete_sink: Option<RowDeleteSink>,
    position_deletes: PositionDeleteAccumulator,
    row_registry: RelationRowRegistry,
    modify_state_id: ModifyStateId,
    scan_tasks: Option<Arc<PlannedScanTasks>>,
}

impl AmModifyState for IcebergModifyState {
    type QueryState = IcebergModifyQueryState;
    type ScanContext = IcebergModifyScanContext;

    fn new(
        rel: &RelationHandle,
        context: ModifyStateContext<Self::QueryState, Self::ScanContext>,
    ) -> AmResult<Self> {
        Self::open(rel, context)
    }

    fn begin_modify(&mut self) -> AmResult<()> {
        // Intentionally empty: all initialization happens in `new`.
        Ok(())
    }

    fn insert_slot(
        &mut self,
        new: TupleSlotRow<'_>,
        _context: MutationWriteContext,
    ) -> AmResult<()> {
        self.data_sink_mut()?.append(new)?;
        Ok(())
    }

    fn update_slot(
        &mut self,
        row_id: ItemPointer,
        _old: TupleSlotRow<'_>,
        new: TupleSlotRow<'_>,
        context: MutationUpdateContext<'_>,
    ) -> AmResult<MutationOutcome> {
        self.ensure_position_delete_capable()?;
        let identity = IcebergRowIdentity::decode(&row_id)?;
        let claim = self.row_registry.claim(
            self.modify_state_id,
            identity.file_id(),
            identity.row_position(),
            context.cid,
        )?;
        match claim {
            RowMutationClaim::FirstTouch { new_file_positions } => {
                if let Some(positions) = new_file_positions {
                    self.position_deletes
                        .add_file_positions(identity.file_id(), positions);
                }
                self.data_sink_mut()?.append(new)?;
                Ok(MutationOutcome::Applied)
            }
            RowMutationClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(MutationOutcome::AlreadyModifiedInCurrentTransaction {
                modifying_command_id,
            }),
        }
    }

    fn delete_slot(
        &mut self,
        row_id: ItemPointer,
        context: MutationDeleteContext<'_>,
    ) -> AmResult<MutationOutcome> {
        self.ensure_position_delete_capable()?;
        let identity = IcebergRowIdentity::decode(&row_id)?;
        let claim = self.row_registry.claim(
            self.modify_state_id,
            identity.file_id(),
            identity.row_position(),
            context.cid,
        )?;
        match claim {
            RowMutationClaim::FirstTouch { new_file_positions } => {
                if let Some(positions) = new_file_positions {
                    self.position_deletes
                        .add_file_positions(identity.file_id(), positions);
                }
                Ok(MutationOutcome::Applied)
            }
            RowMutationClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(MutationOutcome::AlreadyModifiedInCurrentTransaction {
                modifying_command_id,
            }),
        }
    }

    fn abort_modify(&mut self) {
        // Best-effort in-memory cleanup; persistent artifacts are unwound by
        // ResourceOwner cleanup (see the orphan-file note in `end_modify`).
        if let Some(sink) = self.data_sink.as_mut() {
            sink.abort();
        }
        self.position_deletes.clear();
    }

    fn end_modify(&mut self) -> AmResult<()> {
        // Orphan-file note: data files already uploaded before a later flush
        // failure are NOT leaked. Every produced file is registered via
        // `register_object_file_staged()` / `mark_object_file_uploaded()`, and
        // `StorageArtifactResource::on_abort` unlinks staging files or issues
        // remote deletes on abort. Do not re-introduce a separate cleanup list here.
        let outcome = self.finish_statement()?;
        self.stage_statement(outcome)?;
        Ok(())
    }
}

impl AmCopySession for IcebergModifyState {
    fn new(rel: &RelationHandle) -> AmResult<Self> {
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

    fn begin_copy(&mut self) -> AmResult<()> {
        Ok(())
    }

    fn end_copy(&mut self) -> AmResult<()> {
        let outcome = self.finish_statement()?;
        self.stage_statement(outcome)?;
        Ok(())
    }

    fn abort_copy(&mut self) {
        if let Some(sink) = self.data_sink.as_mut() {
            sink.abort();
        }
        self.position_deletes.clear();
    }

    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        self.data_sink_mut()?.append(row)?;
        Ok(())
    }

    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        for row in rows.iter() {
            self.data_sink_mut()?.append(row)?;
        }
        Ok(())
    }
}

impl IcebergModifyState {
    /// Construct a fully-initialized session, performing all storage IO and
    /// schema/writer setup inline.
    ///
    /// Everything is derived from `rel` here — the file locator and WAL flag
    /// for storage, the relation OID for the metadata tracker, and the live
    /// columns / tuple width / attribute types that bind the write-side
    /// source-slot mapping
    /// ([`crate::access::column_mapping::WriteColumns`]). The handle is not retained.
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
        let scan_tasks = scan_context
            .as_ref()
            .map(IcebergModifyScanContext::scan_tasks);
        let transaction_isolation = PgTransactionIsolation::current()?;
        let rel_oid = rel.oid();
        let row_registry =
            query_state.update(|state| state.relation_registry(rel_oid))?;
        let modify_state_id = row_registry.begin_modify_state()?;
        // `locator().spc_oid` is the *resolved* physical tablespace (default
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
        let isolation_level = command
            .effective_isolation_level(&table_properties, transaction_isolation);
        let conflict_scope = if command.validation_command().is_some() {
            scan_context.as_ref().map(|context| {
                ConflictValidationScope::from_predicate(
                    context.conflict_filter.clone(),
                )
            })
        } else {
            None
        };
        let write_options = IcebergTableOptions::for_relation(rel)?;
        let writer_properties = WriterProperties::builder()
            .set_compression(write_options.parquet_compression())
            .build();

        let data_sink = if actions.writes_rows() {
            // The shared relation shape drives the read and write column
            // mappings, keeping dropped-column and type-position handling
            // consistent. DELETE-only sessions do not allocate it.
            let relation_shape = RelationShape::from_relation(rel);
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
        let row_delete_sink = if writes_position_deletes {
            Some(RowDeleteSink::for_table(
                loaded.metadata.format_version(),
                &file_io,
                &loaded.metadata,
                &writer_properties,
                loaded.delta.as_ref(),
            )?)
        } else {
            None
        };

        Ok(Self {
            rel_oid,
            file_io,
            command,
            target_dependency,
            isolation_level,
            conflict_scope,
            data_sink,
            row_delete_sink,
            position_deletes: PositionDeleteAccumulator::default(),
            row_registry,
            modify_state_id,
            scan_tasks,
        })
    }

    /// Stage produced data files into transaction-local Iceberg metadata.
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
        let Some(isolation_level) = self.isolation_level else {
            return Err(IcebergError::InvariantViolated(
                "row-level mutation validation has no effective isolation level",
            ));
        };
        let conflict_filter = self
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
        let new_data_files = self.finish_data_files()?;
        let referenced_data_files = self
            .position_deletes
            .referenced_data_files(&self.row_registry)?;
        let row_delete_files = self.finish_position_deletes()?;
        Ok(StatementOutcome {
            command: self.command,
            target_dependency: self.target_dependency,
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

    fn finish_data_files(&mut self) -> IcebergResult<Vec<DataFile>> {
        match self.data_sink.as_mut() {
            Some(sink) => sink.finish(),
            None => Ok(Vec::new()),
        }
    }

    fn finish_position_deletes(&mut self) -> IcebergResult<Vec<RowDeleteOutput>> {
        if self.position_deletes.is_empty() {
            return Ok(Vec::new());
        }
        self.row_delete_sink_ref()?.write_files(
            &self.position_deletes,
            &self.row_registry,
            self.scan_tasks.as_ref(),
        )
    }

    fn data_sink_mut(&mut self) -> IcebergResult<&mut DataFileSink> {
        self.data_sink
            .as_mut()
            .ok_or(IcebergError::InvariantViolated(
                "data-file callback reached a mutation command without a data sink",
            ))
    }

    fn row_delete_sink_ref(&self) -> IcebergResult<&RowDeleteSink> {
        self.row_delete_sink
            .as_ref()
            .ok_or(IcebergError::InvariantViolated(
                "position-delete callback reached a mutation command without a delete sink",
            ))
    }

    fn ensure_position_delete_capable(&self) -> IcebergResult<()> {
        if self.row_delete_sink.is_none() {
            return Err(IcebergError::InvariantViolated(
                "position-delete callback reached a mutation command without a delete sink",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod mutation_state_tests {
    use super::*;

    #[test]
    fn synthetic_ctid_round_trips_boundaries() {
        let cases = [
            (0, 0),
            (0, MAX_POSITION),
            (u32::try_from(FILE_MASK).unwrap(), 0),
            (u32::try_from(FILE_MASK).unwrap(), MAX_POSITION),
        ];
        for (file_id, position) in cases {
            let file_id = IcebergFileId::from_raw(file_id);
            let tid = IcebergRowIdentity::encode(file_id, position).unwrap();
            assert_ne!(tid.offset, 0);
            assert!(tid.block_number < TRIGGER_ROW_BLOCK_BASE);
            let decoded = IcebergRowIdentity::decode(&tid).unwrap();
            assert_eq!(decoded.file_id(), file_id);
            assert_eq!(u64::from(decoded.row_position()), position);
        }
    }

    #[test]
    fn synthetic_ctid_rejects_out_of_range_values() {
        assert!(
            IcebergRowIdentity::encode(
                IcebergFileId::from_raw(1 << ICEBERG_FILE_ID_BITS),
                0,
            )
            .is_err()
        );
        assert!(
            IcebergRowIdentity::encode(IcebergFileId::from_raw(0), MAX_POSITION + 1,)
                .is_err()
        );
        assert!(IcebergRowIdentity::decode(&ItemPointer::default()).is_err());
        assert!(
            IcebergRowIdentity::decode(&ItemPointer {
                block_number: TRIGGER_ROW_BLOCK_BASE,
                offset: 1,
            })
            .is_err()
        );
    }

    #[test]
    fn relation_registry_interns_each_path_once() {
        let registry = RelationRowRegistry::default();
        let first_file = registry.register_file("data/a.parquet").unwrap();
        let second_file = registry.register_file("data/a.parquet").unwrap();
        let other_file = registry.register_file("data/b.parquet").unwrap();
        assert_eq!(first_file, second_file);
        assert_ne!(first_file, other_file);
        assert_eq!(
            registry.file_path(other_file).unwrap().as_ref(),
            "data/b.parquet"
        );
    }

    #[test]
    fn commands_read_their_own_isolation_property() {
        let cases = [
            (
                iceberg_lite::spec::TableProperties::PROPERTY_WRITE_DELETE_ISOLATION_LEVEL,
                ModifyCommand::Delete,
            ),
            (
                iceberg_lite::spec::TableProperties::PROPERTY_WRITE_UPDATE_ISOLATION_LEVEL,
                ModifyCommand::Update,
            ),
            (
                iceberg_lite::spec::TableProperties::PROPERTY_WRITE_MERGE_ISOLATION_LEVEL,
                ModifyCommand::Merge,
            ),
        ];

        for (snapshot_property, snapshot_command) in cases {
            let properties = iceberg_lite::spec::TableProperties::try_from(
                &std::collections::HashMap::from([(
                    snapshot_property.to_owned(),
                    "snapshot".to_owned(),
                )]),
            )
            .unwrap();

            for command in [
                ModifyCommand::Delete,
                ModifyCommand::Update,
                ModifyCommand::Merge,
            ] {
                let expected = if command == snapshot_command {
                    IsolationLevel::Snapshot
                } else {
                    IsolationLevel::Serializable
                };
                assert_eq!(
                    command.table_isolation_level(&properties),
                    Some(expected)
                );
            }
        }
    }

    #[test]
    fn target_dependency_separates_independent_and_required_reads() {
        let empty_scan = IcebergModifyScanContext::new(
            None,
            Predicate::AlwaysTrue,
            Arc::new(PlannedScanTasks::mutation(Vec::new())),
        );
        let empty_table_read = TargetDependency::from_context(Some(&empty_scan));
        assert_eq!(empty_table_read.required_snapshot().unwrap(), None);

        let independent = TargetDependency::from_context(None);
        assert_eq!(independent, TargetDependency::Independent);
    }
}
