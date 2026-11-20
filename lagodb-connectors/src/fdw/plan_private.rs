//! Format selection persisted in the core FDW scan and modify plan envelopes.

use lagodb_core::fdw::{
    ForeignModifyError, ForeignModifyPrivate, ForeignPlanPrivate,
    ForeignPrivateReader, ForeignPrivateWriter, ForeignScanError,
};

use crate::error::ConnectorError;
use crate::format::FormatKind;

/// The format kind chosen by the planner. Executor callbacks compare it with
/// the current catalog definition before opening the relation's objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlannedFormat {
    kind: FormatKind,
}

impl PlannedFormat {
    #[inline]
    pub(crate) const fn new(kind: FormatKind) -> Self {
        Self { kind }
    }

    #[inline]
    pub(crate) const fn kind(self) -> FormatKind {
        self.kind
    }

    fn encode(self, writer: &mut ForeignPrivateWriter) {
        writer.append_i32(self.kind.wire());
    }

    fn from_wire(wire: i32) -> Result<Self, ConnectorError> {
        FormatKind::from_wire(wire)
            .map(Self::new)
            .ok_or_else(|| ConnectorError::invalid_plan_format(wire))
    }
}

impl ForeignPlanPrivate for PlannedFormat {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignScanError> {
        Self::encode(*self, writer);
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignScanError> {
        Ok(Self::from_wire(reader.read_i32()?)?)
    }
}

impl ForeignModifyPrivate for PlannedFormat {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignModifyError> {
        Self::encode(*self, writer);
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignModifyError> {
        Ok(Self::from_wire(reader.read_i32()?)?)
    }
}
