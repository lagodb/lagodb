//! Executor-side scan state and format delegation.

use core::mem;

use lagodb_core::fdw::{
    BeginForeignScanContext, ForeignScanError, ReScanForeignScanContext,
    ScanSlotWriter, StartForeignScanContext,
};

use crate::error::ConnectorError;
use crate::fdw::format_adapters::{FormatFdwReader, FormatScanState};
use crate::storage::InputFiles;

use super::super::{LagodbConnectors, ResolvedForeignRelation};

/// Executor state owns the format object selected by the serialized plan.
pub(crate) struct ConnectorScanState {
    phase: ConnectorScanPhase,
}

enum ConnectorScanPhase {
    Prepared {
        reader: Box<dyn FormatFdwReader>,
        files: InputFiles,
    },
    Active(Box<dyn FormatScanState>),
    Transitioning,
}

impl ConnectorScanState {
    pub(crate) fn begin(
        context: BeginForeignScanContext<'_, LagodbConnectors>,
    ) -> Result<Self, ForeignScanError> {
        let planned_format = context.private_data.kind();
        let selected = ResolvedForeignRelation::resolve(context.relation.oid())?;
        let format = selected.kind();
        if format != planned_format {
            return Err(ConnectorError::plan_format_changed().into());
        }
        let (reader, target) =
            selected.into_scan_parts(context.effective_user_id())?;
        let files = format.input(&target)?.open();
        Ok(Self {
            phase: ConnectorScanPhase::Prepared { reader, files },
        })
    }

    pub(crate) fn start(
        &mut self,
        context: StartForeignScanContext<'_, LagodbConnectors>,
    ) -> Result<(), ForeignScanError> {
        let ConnectorScanPhase::Prepared { reader, files } =
            mem::replace(&mut self.phase, ConnectorScanPhase::Transitioning)
        else {
            unreachable!("core starts the connector scan exactly once");
        };
        self.phase = ConnectorScanPhase::Active(reader.begin(context, files)?);
        Ok(())
    }

    pub(crate) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        let ConnectorScanPhase::Active(inner) = &mut self.phase else {
            unreachable!("core starts the connector scan before requesting rows");
        };
        Ok(inner.next_slot(output)?)
    }

    pub(crate) fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, LagodbConnectors>,
    ) -> Result<(), ForeignScanError> {
        let ConnectorScanPhase::Active(inner) = &mut self.phase else {
            unreachable!("core starts the connector scan before rescanning");
        };
        Ok(inner.rescan(context)?)
    }

    pub(crate) fn end(&mut self) -> Result<(), ForeignScanError> {
        if let ConnectorScanPhase::Active(inner) = &mut self.phase {
            inner.end()?;
        }
        Ok(())
    }
}
