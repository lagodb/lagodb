//! FDW modify adapter for the connector's selected format writer.

mod state;

use lagodb_core::fdw::{
    FdwModify, FdwScan, ForeignInsertBeginContext, ForeignModifyBeginContext,
    ForeignModifyCapabilities, ForeignModifyError, ForeignModifyOperation,
    ForeignModifyPlanContext, ForeignModifyPlanSpec, ForeignModifyPrivate,
    ForeignModifyRelationContext, ForeignPrivateReader, ForeignPrivateWriter,
    ForeignUpdateTargetContext,
};

use super::{LagodbConnectors, ResolvedForeignRelation};
use crate::error::ConnectorError;
use crate::format::FormatKind;
use crate::gucs::WriteConfig;
use crate::storage::ObjectLocationKind;

pub(crate) use state::ConnectorModifyState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectorModifyPrivate {
    kind: FormatKind,
}

impl ConnectorModifyPrivate {
    const fn new(kind: FormatKind) -> Self {
        Self { kind }
    }

    const fn kind(self) -> FormatKind {
        self.kind
    }
}

impl ForeignModifyPrivate for ConnectorModifyPrivate {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignModifyError> {
        writer.append_i32(self.kind().wire());
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignModifyError> {
        let wire = reader.read_i32()?;
        let kind = FormatKind::from_wire(wire)
            .ok_or_else(|| ConnectorError::invalid_plan_format(wire))?;
        Ok(Self::new(kind))
    }
}

impl FdwModify for LagodbConnectors {
    type ModifyPrivateData = ConnectorModifyPrivate;
    type ModifyState = ConnectorModifyState;
    type TargetScanContext = ();

    fn capabilities(
        context: &ForeignModifyRelationContext<'_>,
    ) -> Result<ForeignModifyCapabilities, ForeignModifyError> {
        let relation = ResolvedForeignRelation::resolve(context.relation().oid())?;
        if relation.output_kind()? == ObjectLocationKind::Exact {
            return Ok(ForeignModifyCapabilities::default());
        }
        Ok(ForeignModifyCapabilities::new(true, false, false))
    }

    fn add_update_targets(
        _context: &mut ForeignUpdateTargetContext<'_>,
    ) -> Result<(), ForeignModifyError> {
        Ok(())
    }

    fn plan_modify(
        context: &ForeignModifyPlanContext<'_>,
    ) -> Result<ForeignModifyPlanSpec<Self::ModifyPrivateData>, ForeignModifyError>
    {
        let relation = ResolvedForeignRelation::resolve(context.relation().oid())?;
        if context.operation() != ForeignModifyOperation::Insert {
            return Err(
                ConnectorError::modify_not_implemented(relation.kind()).into()
            );
        }
        Ok(ForeignModifyPlanSpec::new(ConnectorModifyPrivate::new(
            relation.kind(),
        )))
    }

    fn begin_modify(
        context: ForeignModifyBeginContext<'_, Self::ModifyPrivateData>,
        _target_scan: Option<Self::TargetScanContext>,
    ) -> Result<Self::ModifyState, ForeignModifyError> {
        let planned_format = context.private_data().kind();
        let relation_oid = context.relation().oid();
        let selected = ResolvedForeignRelation::resolve(relation_oid)?;
        let format = selected.kind();
        if format != planned_format {
            return Err(ConnectorError::plan_format_changed().into());
        }
        let (writer, target) =
            selected.into_write_parts(context.effective_user_id())?;
        let output =
            format.output(&target, || WriteConfig::from_guc().target_file_bytes())?;
        let inner = writer.begin(context.relation(), output)?;
        Ok(ConnectorModifyState::new(format, inner))
    }

    fn target_scan_context(
        _state: &<Self as FdwScan>::State,
    ) -> Option<Self::TargetScanContext> {
        None
    }

    fn begin_insert(
        context: &mut ForeignInsertBeginContext<'_>,
    ) -> Result<Self::ModifyState, ForeignModifyError> {
        let relation_oid = context.relation().oid();
        let selected = ResolvedForeignRelation::resolve(relation_oid)?;
        let (writer, target) =
            selected.into_write_parts(context.effective_user_id())?;
        let format = writer.kind();
        let output =
            format.output(&target, || WriteConfig::from_guc().target_file_bytes())?;
        let inner = writer.begin(context.relation(), output)?;
        Ok(ConnectorModifyState::new(format, inner))
    }
}
