//! FDW plan-private format selection.

use lagodb_core::fdw::{
    ForeignPlanPrivate, ForeignPrivateReader, ForeignPrivateWriter, ForeignScanError,
};

use crate::error::ConnectorError;
use crate::format::{FormatKind, FormatScanPrivate};

pub(crate) type ConnectorScanPrivate = FormatScanPrivate;

impl ForeignPlanPrivate for ConnectorScanPrivate {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignScanError> {
        writer.append_i32(self.kind().wire());
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignScanError> {
        let wire = reader.read_i32()?;
        let kind = FormatKind::from_wire(wire)
            .ok_or_else(|| ConnectorError::invalid_plan_format(wire))?;
        Ok(Self::new(kind))
    }
}
