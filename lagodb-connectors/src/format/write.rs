//! Writer capability owned by one concrete format.

use pg_lakebase_core::fdw::{
    ForeignInsertBatch, ForeignInsertBeginContext, ForeignModifyBeginContext,
    ForeignModifyCapabilities, ForeignModifyOutcome, ForeignModifyPlanContext,
    ForeignModifyPlanSpec, ForeignModifyRelationContext, ForeignUpdateTargetContext,
    ModifyPlanSlot, ModifySlot,
};

use core::ffi::c_int;

use crate::error::ConnectorError;
use crate::storage::ObjectOutput;

use super::{FormatKind, FormatObject};

/// Format selection persisted in the core modify plan envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FormatWritePrivate {
    kind: FormatKind,
}

impl FormatWritePrivate {
    #[inline]
    pub(crate) const fn new(kind: FormatKind) -> Self {
        Self { kind }
    }

    #[inline]
    pub(crate) const fn kind(self) -> FormatKind {
        self.kind
    }
}

/// Write capability of one concrete format.
pub(crate) trait FormatWriter: FormatObject {
    fn capabilities(
        &self,
        _context: &ForeignModifyRelationContext<'_>,
    ) -> Result<ForeignModifyCapabilities, ConnectorError> {
        Ok(ForeignModifyCapabilities::default())
    }

    fn add_update_targets(
        &self,
        _context: &mut ForeignUpdateTargetContext<'_>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn plan_modify(
        &self,
        _context: &ForeignModifyPlanContext<'_>,
    ) -> Result<ForeignModifyPlanSpec<FormatWritePrivate>, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(self.kind()))
    }

    fn begin_modify(
        self: Box<Self>,
        _context: ForeignModifyBeginContext<'_, FormatWritePrivate>,
        _output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(self.kind()))
    }

    fn begin_insert(
        self: Box<Self>,
        _context: &mut ForeignInsertBeginContext<'_>,
        _output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(self.kind()))
    }
}

/// Relation-local state owned by the selected format writer.
pub(crate) trait FormatWriteState: 'static {
    fn batch_size(&self) -> Result<c_int, ConnectorError> {
        Ok(1)
    }

    fn prepare_insert(
        &mut self,
        _slot: &mut ModifySlot<'_>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn insert(
        &mut self,
        _slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError>;

    fn insert_batch(
        &mut self,
        batch: &mut ForeignInsertBatch<'_>,
    ) -> Result<(), ConnectorError> {
        batch.process_each_with(
            |_, slot| {
                self.prepare_insert(slot)?;
                self.insert(slot)
            },
            ConnectorError::foreign_modify,
        )
    }

    fn prepare_update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError>;

    fn prepare_delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError>;

    fn finish(&mut self) -> Result<(), ConnectorError>;
}
