//! FDW operation policy around one relation-bound format encoder.

use core::ffi::c_int;

use lagodb_core::fdw::{
    ForeignModifyError, ForeignModifyOutcome, ForeignModifyState, ModifyPlanSlot,
    ModifySlot,
};

use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::ResolvedForeignRelation;
use crate::fdw::format_adapters::FormatWriteState;
use crate::format::FormatKind;
use crate::gucs::WriteConfig;

pub(crate) struct ConnectorModifyState {
    format: FormatKind,
    inner: Box<dyn FormatWriteState>,
}

impl ConnectorModifyState {
    /// Open the relation's output and bind the selected format writer once
    /// for both planned modify and COPY/partition-routed INSERT.
    pub(crate) fn begin(
        selected: ResolvedForeignRelation,
        relation: &RelationHandle<'_>,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, ConnectorError> {
        let format = selected.kind();
        let (writer, target) = selected.into_write_parts(effective_user)?;
        let output =
            format.output(&target, || WriteConfig::from_guc().target_file_bytes())?;
        Ok(Self {
            format,
            inner: writer.begin(relation, output)?,
        })
    }
}

impl ForeignModifyState for ConnectorModifyState {
    fn batch_size(&self) -> Result<c_int, ForeignModifyError> {
        Ok(self.inner.batch_size())
    }

    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        self.inner.write_row(slot.tuple_row())?;
        Ok(ForeignModifyOutcome::Applied)
    }

    fn update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        Err(ConnectorError::modify_not_implemented(self.format).into())
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        Err(ConnectorError::modify_not_implemented(self.format).into())
    }

    fn finish(&mut self) -> Result<(), ForeignModifyError> {
        Ok(self.inner.finish()?)
    }
}
