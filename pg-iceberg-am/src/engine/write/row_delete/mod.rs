//! Row-delete accumulation and backend selection.

mod deletion_vector;
mod position;

use std::collections::BTreeSet;
use std::rc::Rc;

use iceberg_lite::io::FileIO;
use iceberg_lite::overlay::DeleteFileIdentity;
use iceberg_lite::spec::{DataFile, FormatVersion, TableMetadata};
use parquet::file::properties::WriterProperties;
use pgrx::pg_sys;

use crate::engine::write::{
    IcebergFileId, ModifyStateId, OwnedRowPositions, PlannedMutationTasks,
    RelationRowRegistry, RowMutationClaim,
};
use crate::error::{IcebergError, IcebergResult};

pub(crate) struct RowDeleteOutput {
    pub(crate) delete_file: DataFile,
    pub(crate) referenced_data_files: Vec<String>,
    pub(crate) removed_delete_files: Vec<DeleteFileIdentity>,
}

enum RowDeleteBackend {
    Position(Box<position::PositionDeleteSink>),
    DeletionVector {
        sink: Box<deletion_vector::DeletionVectorSink>,
        scan_tasks: Rc<PlannedMutationTasks>,
    },
}

impl RowDeleteBackend {
    fn for_table(
        format_version: FormatVersion,
        file_io: &FileIO,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
        scan_tasks: Option<Rc<PlannedMutationTasks>>,
    ) -> IcebergResult<Self> {
        match format_version {
            FormatVersion::V1 => Err(IcebergError::NotImplemented(
                "row deletes require Iceberg format v2 or later",
            )),
            FormatVersion::V2 => {
                Ok(Self::Position(Box::new(position::PositionDeleteSink::new(
                    file_io,
                    table_metadata,
                    writer_properties,
                )?)))
            }
            FormatVersion::V3 => {
                let scan_tasks =
                    scan_tasks.ok_or(IcebergError::InvariantViolated(
                        "deletion-vector write has no target scan task cache",
                    ))?;
                Ok(Self::DeletionVector {
                    sink: Box::new(deletion_vector::DeletionVectorSink::new(
                        file_io,
                        table_metadata,
                    )?),
                    scan_tasks,
                })
            }
        }
    }

    fn write_files(
        &self,
        deletes: &PositionDeleteAccumulator,
        row_registry: &RelationRowRegistry,
    ) -> IcebergResult<Vec<RowDeleteOutput>> {
        match self {
            Self::Position(sink) => sink.write_files(deletes, row_registry),
            Self::DeletionVector { sink, scan_tasks } => {
                sink.write_files(deletes, row_registry, scan_tasks)
            }
        }
    }
}

pub(crate) struct RowDeleteState {
    row_registry: RelationRowRegistry,
    modify_state_id: ModifyStateId,
    position_deletes: PositionDeleteAccumulator,
    backend: RowDeleteBackend,
}

impl RowDeleteState {
    pub(crate) fn new(
        format_version: FormatVersion,
        file_io: &FileIO,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
        row_registry: RelationRowRegistry,
        modify_state_id: ModifyStateId,
        scan_tasks: Option<Rc<PlannedMutationTasks>>,
    ) -> IcebergResult<Self> {
        let backend = RowDeleteBackend::for_table(
            format_version,
            file_io,
            table_metadata,
            writer_properties,
            scan_tasks,
        )?;
        Ok(Self {
            row_registry,
            modify_state_id,
            position_deletes: PositionDeleteAccumulator::default(),
            backend,
        })
    }

    pub(crate) fn claim(
        &mut self,
        file_id: IcebergFileId,
        position: u32,
        cid: pg_sys::CommandId,
    ) -> IcebergResult<RowDeleteClaim> {
        match self
            .row_registry
            .claim(self.modify_state_id, file_id, position, cid)?
        {
            RowMutationClaim::FirstTouch {
                new_file_positions: Some(positions),
            } => {
                self.position_deletes.add_file_positions(file_id, positions);
                Ok(RowDeleteClaim::FirstTouch)
            }
            RowMutationClaim::FirstTouch {
                new_file_positions: None,
            } => Ok(RowDeleteClaim::FirstTouch),
            RowMutationClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            }),
        }
    }

    pub(crate) fn finish(&self) -> IcebergResult<Vec<RowDeleteOutput>> {
        if self.position_deletes.is_empty() {
            return Ok(Vec::new());
        }
        self.backend
            .write_files(&self.position_deletes, &self.row_registry)
    }

    pub(crate) fn referenced_data_files(&self) -> IcebergResult<BTreeSet<String>> {
        self.position_deletes
            .referenced_data_files(&self.row_registry)
    }

    pub(crate) fn clear(&mut self) {
        self.position_deletes.clear();
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RowDeleteClaim {
    FirstTouch,
    PreviouslyModified {
        modifying_command_id: pg_sys::CommandId,
    },
}

#[derive(Debug, Default)]
pub(super) struct PositionDeleteAccumulator {
    /// One shared owner bitmap per file touched by this ModifyState. The
    /// registry performs the only per-row insertion; this list is updated only
    /// when the state first touches a file.
    files: Vec<(IcebergFileId, OwnedRowPositions)>,
}

impl PositionDeleteAccumulator {
    pub(super) fn add_file_positions(
        &mut self,
        file_id: IcebergFileId,
        positions: OwnedRowPositions,
    ) {
        debug_assert!(
            self.files.iter().all(|(existing, _)| *existing != file_id),
            "one ModifyState must own exactly one bitmap per file"
        );
        self.files.push((file_id, positions));
    }

    pub(super) fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub(super) fn files(
        &self,
    ) -> impl Iterator<Item = (IcebergFileId, &OwnedRowPositions)> {
        self.files
            .iter()
            .map(|(file_id, positions)| (*file_id, positions))
    }

    pub(super) fn referenced_data_files(
        &self,
        row_registry: &RelationRowRegistry,
    ) -> IcebergResult<BTreeSet<String>> {
        self.files()
            .map(|(file_id, _)| {
                row_registry.file_path(file_id).map(|path| path.to_string())
            })
            .collect()
    }

    pub(super) fn clear(&mut self) {
        self.files.clear();
    }
}
