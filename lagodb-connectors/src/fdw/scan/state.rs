//! Executor-side scan state and format delegation.

use super::super::ResolvedForeignRelation;
use core::mem;

use crate::format::{FormatReader, FormatScanState};
use crate::storage::{ObjectFiles, ObjectInput};
use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignScanError, ReScanForeignScanContext,
    ScanSlotWriter, StartForeignScanContext,
};
use pg_lakebase_core::storage::foreign::StorageManager;

use super::super::Lakebase;
use crate::error::ConnectorError;

/// Executor state owns the format object selected by the serialized plan.
pub(crate) struct LakebaseScanState {
    phase: LakebaseScanPhase,
}

enum LakebaseScanPhase {
    Prepared {
        reader: Box<dyn FormatReader>,
        files: ObjectFiles,
    },
    Active(Box<dyn FormatScanState>),
    Transitioning,
}

impl LakebaseScanState {
    pub(crate) fn begin(
        context: BeginForeignScanContext<'_, Lakebase>,
    ) -> Result<Self, ForeignScanError> {
        let planned_format = context.private_data.kind();
        let selected = ResolvedForeignRelation::resolve(context.relation.oid())?;
        let format = selected.kind();
        if format != planned_format {
            return Err(ConnectorError::plan_format_changed().into());
        }
        let (reader, target) =
            selected.into_scan_parts(context.effective_user_id())?;
        let manager = StorageManager::from_pg_gucs().map_err(ConnectorError::from)?;
        let files = ObjectInput::resolve(&target, &manager, format)?.open();
        Ok(Self {
            phase: LakebaseScanPhase::Prepared { reader, files },
        })
    }

    pub(crate) fn start(
        &mut self,
        context: StartForeignScanContext<'_, Lakebase>,
    ) -> Result<(), ForeignScanError> {
        let LakebaseScanPhase::Prepared { reader, files } =
            mem::replace(&mut self.phase, LakebaseScanPhase::Transitioning)
        else {
            return Err(ConnectorError::InvalidScanLifecycle {
                detail: "scan was started more than once",
            }
            .into());
        };
        self.phase = LakebaseScanPhase::Active(reader.begin(context, files)?);
        Ok(())
    }

    pub(crate) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        let LakebaseScanPhase::Active(inner) = &mut self.phase else {
            return Err(ConnectorError::InvalidScanLifecycle {
                detail: "scan cursor is not active",
            }
            .into());
        };
        Ok(inner.next_slot(output)?)
    }

    pub(crate) fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, Lakebase>,
    ) -> Result<(), ForeignScanError> {
        let LakebaseScanPhase::Active(inner) = &mut self.phase else {
            return Err(ConnectorError::InvalidScanLifecycle {
                detail: "scan cursor is not active during rescan",
            }
            .into());
        };
        Ok(inner.rescan(context)?)
    }

    pub(crate) fn end(&mut self) -> Result<(), ForeignScanError> {
        if let LakebaseScanPhase::Active(inner) = &mut self.phase {
            inner.end()?;
        }
        Ok(())
    }
}
