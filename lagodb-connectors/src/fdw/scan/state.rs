//! Executor-side scan state and format delegation.

use super::super::ResolvedForeignRelation;
use crate::format::FormatScanState;
use crate::storage::ObjectInput;
use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignScanError, ReScanForeignScanContext,
    ScanSlotWriter,
};
use pg_lakebase_core::storage::foreign::StorageManager;

use super::super::Lakebase;
use crate::error::ConnectorError;

/// Executor state owns the format object selected by the serialized plan.
pub(crate) struct LakebaseScanState {
    inner: Box<dyn FormatScanState>,
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
        let inner = reader.begin(context, files)?;
        Ok(Self { inner })
    }

    pub(crate) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        Ok(self.inner.next_slot(output)?)
    }

    pub(crate) fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, Lakebase>,
    ) -> Result<(), ForeignScanError> {
        Ok(self.inner.rescan(context)?)
    }

    pub(crate) fn end(&mut self) -> Result<(), ForeignScanError> {
        Ok(self.inner.end()?)
    }
}
