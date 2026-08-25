//! Planner-side scan delegation.

use pg_lakebase_core::fdw::{
    BeginForeignScanContext, FdwScan, ForeignFilterExplainValues, ForeignPathBuilder,
    ForeignPathContext, ForeignPathKeys, ForeignPlanContext, ForeignPlanSpec,
    ForeignRelContext, ForeignRelSize, ForeignRelSizeContext, ForeignScanError,
    ReScanForeignScanContext, ScanSlotWriter, StartForeignScanContext,
};

use crate::format::FormatScanPlanner;

use super::super::{LagodbConnectors, ResolvedForeignRelation};
use super::private::ConnectorScanPrivate;
use super::state::ConnectorScanState;

/// Planner state contains the selected format reader's scan planner. The
/// format is resolved once during `GetForeignRelSize`; no format branch is
/// needed in later callbacks.
pub(crate) struct ConnectorScanPlanner {
    scan: Box<dyn FormatScanPlanner>,
}

impl FdwScan for LagodbConnectors {
    type PlannerState = ConnectorScanPlanner;
    type PrivateData = ConnectorScanPrivate;
    type State = ConnectorScanState;

    fn init_planner(
        context: &ForeignRelContext<'_>,
    ) -> Result<Self::PlannerState, ForeignScanError> {
        let relation = ResolvedForeignRelation::resolve(context.relation_oid())?;
        Ok(ConnectorScanPlanner {
            scan: relation.into_reader().planner(),
        })
    }

    fn estimate(
        state: &mut Self::PlannerState,
        context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ForeignScanError> {
        Ok(state.scan.estimate(context)?)
    }

    fn build_paths(
        state: &Self::PlannerState,
        context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<Self::PrivateData>,
    ) -> Result<(), ForeignScanError> {
        Ok(state.scan.build_paths(context, paths)?)
    }

    fn supports_pathkeys(
        state: &Self::PlannerState,
        context: &ForeignPathContext<'_>,
        pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ForeignScanError> {
        Ok(state.scan.supports_pathkeys(context, pathkeys)?)
    }

    fn build_plan(
        state: &mut Self::PlannerState,
        context: &ForeignPlanContext<'_, Self>,
    ) -> Result<ForeignPlanSpec<Self::PrivateData>, ForeignScanError> {
        Ok(state.scan.build_plan(context)?)
    }

    fn explain_filter(
        predicate: &Self::PlannedPredicate,
        values: ForeignFilterExplainValues<'_>,
    ) -> Result<Option<String>, ForeignScanError> {
        Ok(Some(predicate.explain(values)))
    }

    fn begin(
        context: BeginForeignScanContext<'_, Self>,
    ) -> Result<Self::State, ForeignScanError> {
        ConnectorScanState::begin(context)
    }

    fn next_slot(
        state: &mut Self::State,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        state.next_slot(output)
    }

    fn start(
        state: &mut Self::State,
        context: StartForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError> {
        state.start(context)
    }

    fn rescan(
        state: &mut Self::State,
        context: ReScanForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError> {
        state.rescan(context)
    }

    fn end(state: &mut Self::State) -> Result<(), ForeignScanError> {
        state.end()
    }
}
