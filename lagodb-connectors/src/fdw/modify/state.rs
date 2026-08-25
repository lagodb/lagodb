//! Executor-side delegation to one initialized format writer.

use pg_lakebase_core::fdw::{
    ForeignInsertBatch, ForeignModifyError, ForeignModifyOutcome, ForeignModifyState,
    ModifyPlanSlot, ModifySlot,
};

use crate::format::FormatWriteState;

/// Executor state owns the selected format writer for one modify lifecycle.
pub(crate) struct ConnectorModifyState {
    inner: Box<dyn FormatWriteState>,
}

impl ConnectorModifyState {
    pub(crate) fn new(inner: Box<dyn FormatWriteState>) -> Self {
        Self { inner }
    }
}

impl ForeignModifyState for ConnectorModifyState {
    fn batch_size(&self) -> Result<core::ffi::c_int, ForeignModifyError> {
        Ok(self.inner.batch_size()?)
    }

    fn prepare_insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<(), ForeignModifyError> {
        Ok(self.inner.prepare_insert(slot)?)
    }

    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        Ok(self.inner.insert(slot)?)
    }

    fn insert_batch(
        &mut self,
        batch: &mut ForeignInsertBatch<'_>,
    ) -> Result<(), ForeignModifyError> {
        Ok(self.inner.insert_batch(batch)?)
    }

    fn prepare_update(
        &mut self,
        slot: &mut ModifySlot<'_>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<(), ForeignModifyError> {
        Ok(self.inner.prepare_update(slot, plan_slot)?)
    }

    fn update(
        &mut self,
        slot: &mut ModifySlot<'_>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        Ok(self.inner.update(slot, plan_slot)?)
    }

    fn prepare_delete(
        &mut self,
        returned_slot: Option<&mut ModifySlot<'_>>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<(), ForeignModifyError> {
        Ok(self.inner.prepare_delete(returned_slot, plan_slot)?)
    }

    fn delete(
        &mut self,
        returned_slot: Option<&mut ModifySlot<'_>>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        Ok(self.inner.delete(returned_slot, plan_slot)?)
    }

    fn finish(&mut self) -> Result<(), ForeignModifyError> {
        Ok(self.inner.finish()?)
    }
}
