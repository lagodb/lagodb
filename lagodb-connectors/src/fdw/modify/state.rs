//! FDW operation policy around one relation-bound format encoder.

use core::ffi::c_int;

use lagodb_core::fdw::{
    ForeignModifyError, ForeignModifyOutcome, ForeignModifyState, ModifyPlanSlot,
    ModifySlot,
};

use crate::error::ConnectorError;
use crate::format::{FormatKind, FormatWriteState};

pub(crate) struct ConnectorModifyState {
    format: FormatKind,
    inner: Box<dyn FormatWriteState>,
}

impl ConnectorModifyState {
    pub(crate) fn new(format: FormatKind, inner: Box<dyn FormatWriteState>) -> Self {
        Self { format, inner }
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
