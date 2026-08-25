//! Provider-neutral row and position-delete sinks for one modify session.

use std::collections::BTreeSet;

use iceberg_lite::spec::DataFile;
use pg_lakebase_core::tuple::TupleSlotRow;
use pgrx::pg_sys;

use super::{
    DataFileSink, IcebergRowIdentity, RowDeleteClaim, RowDeleteOutput, RowDeleteState,
};
use crate::error::{IcebergError, IcebergResult};

pub(crate) enum MutationSinks {
    Rows(DataFileSink),
    Deletes(RowDeleteState),
    RowsAndDeletes {
        rows: DataFileSink,
        deletes: RowDeleteState,
    },
}

impl MutationSinks {
    pub(crate) fn new(
        rows: Option<DataFileSink>,
        deletes: Option<RowDeleteState>,
    ) -> IcebergResult<Self> {
        match (rows, deletes) {
            (Some(rows), None) => Ok(Self::Rows(rows)),
            (None, Some(deletes)) => Ok(Self::Deletes(deletes)),
            (Some(rows), Some(deletes)) => Ok(Self::RowsAndDeletes { rows, deletes }),
            (None, None) => Err(IcebergError::InvariantViolated(
                "mutation session has no row or delete sink",
            )),
        }
    }

    pub(crate) fn insert(&mut self, row: TupleSlotRow<'_>) -> IcebergResult<()> {
        let rows = match self {
            Self::Rows(rows) | Self::RowsAndDeletes { rows, .. } => rows,
            Self::Deletes(_) => {
                return Err(IcebergError::InvariantViolated(
                    "insert callback reached a mutation session without a row sink",
                ));
            }
        };
        // SAFETY: adapters construct the data sink from the same relation
        // shape as the callback slot passed to this relation-local session.
        unsafe { rows.append(row) }
    }

    pub(crate) fn update(
        &mut self,
        identity: IcebergRowIdentity,
        row: TupleSlotRow<'_>,
        command_id: pg_sys::CommandId,
    ) -> IcebergResult<RowDeleteClaim> {
        let Self::RowsAndDeletes { rows, deletes } = self else {
            return Err(IcebergError::InvariantViolated(
                "update callback reached a mutation session without both sinks",
            ));
        };
        let claim =
            deletes.claim(identity.file_id(), identity.row_position(), command_id)?;
        if matches!(claim, RowDeleteClaim::FirstTouch) {
            // SAFETY: same relation-local binding as `insert`.
            unsafe { rows.append(row)? };
        }
        Ok(claim)
    }

    pub(crate) fn delete(
        &mut self,
        identity: IcebergRowIdentity,
        command_id: pg_sys::CommandId,
    ) -> IcebergResult<RowDeleteClaim> {
        let deletes = match self {
            Self::Deletes(deletes) | Self::RowsAndDeletes { deletes, .. } => deletes,
            Self::Rows(_) => {
                return Err(IcebergError::InvariantViolated(
                    "delete callback reached a mutation session without a delete sink",
                ));
            }
        };
        deletes.claim(identity.file_id(), identity.row_position(), command_id)
    }

    pub(crate) fn data_sink_mut(&mut self) -> IcebergResult<&mut DataFileSink> {
        match self {
            Self::Rows(rows) | Self::RowsAndDeletes { rows, .. } => Ok(rows),
            Self::Deletes(_) => Err(IcebergError::InvariantViolated(
                "batch insert reached a mutation session without a row sink",
            )),
        }
    }

    pub(crate) fn finish_data_files(&mut self) -> IcebergResult<Vec<DataFile>> {
        match self {
            Self::Rows(rows) | Self::RowsAndDeletes { rows, .. } => rows.finish(),
            Self::Deletes(_) => Ok(Vec::new()),
        }
    }

    pub(crate) fn finish_position_deletes(
        &self,
    ) -> IcebergResult<Vec<RowDeleteOutput>> {
        match self {
            Self::Rows(_) => Ok(Vec::new()),
            Self::Deletes(deletes) | Self::RowsAndDeletes { deletes, .. } => {
                deletes.finish()
            }
        }
    }

    pub(crate) fn referenced_data_files(&self) -> IcebergResult<BTreeSet<String>> {
        match self {
            Self::Rows(_) => Ok(BTreeSet::new()),
            Self::Deletes(deletes) | Self::RowsAndDeletes { deletes, .. } => {
                deletes.referenced_data_files()
            }
        }
    }

    pub(crate) fn abort(&mut self) {
        match self {
            Self::Rows(rows) => rows.abort(),
            Self::Deletes(deletes) => deletes.clear(),
            Self::RowsAndDeletes { rows, deletes } => {
                rows.abort();
                deletes.clear();
            }
        }
    }
}
