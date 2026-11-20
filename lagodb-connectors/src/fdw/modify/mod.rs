//! FDW modify adapter for the connector's selected format writer.

mod state;

use lagodb_core::fdw::{
    FdwModify, FdwScan, ForeignInsertBeginContext, ForeignModifyBeginContext,
    ForeignModifyCapabilities, ForeignModifyError, ForeignModifyOperation,
    ForeignModifyPlanContext, ForeignModifyPlanSpec, ForeignModifyRelationContext,
    ForeignUpdateTargetContext,
};

use super::{LagodbConnectors, PlannedFormat, ResolvedForeignRelation};
use crate::error::ConnectorError;
use crate::storage::ObjectLocationKind;

pub(crate) use state::ConnectorModifyState;

impl FdwModify for LagodbConnectors {
    type ModifyPrivateData = PlannedFormat;
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
        Ok(ForeignModifyPlanSpec::new(PlannedFormat::new(
            relation.kind(),
        )))
    }

    fn begin_modify(
        context: ForeignModifyBeginContext<'_, Self::ModifyPrivateData>,
        _target_scan: Option<Self::TargetScanContext>,
    ) -> Result<Self::ModifyState, ForeignModifyError> {
        let relation = context.relation();
        let selected = ResolvedForeignRelation::resolve(relation.oid())?;
        selected.ensure_planned(*context.private_data())?;
        Ok(ConnectorModifyState::begin(
            selected,
            relation,
            context.effective_user_id(),
        )?)
    }

    fn target_scan_context(
        _state: &<Self as FdwScan>::State,
    ) -> Option<Self::TargetScanContext> {
        None
    }

    fn begin_insert(
        context: &mut ForeignInsertBeginContext<'_>,
    ) -> Result<Self::ModifyState, ForeignModifyError> {
        let relation = context.relation();
        let selected = ResolvedForeignRelation::resolve(relation.oid())?;
        Ok(ConnectorModifyState::begin(
            selected,
            relation,
            context.effective_user_id(),
        )?)
    }
}
