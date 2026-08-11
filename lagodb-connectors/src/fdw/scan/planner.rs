//! Planner-side scan delegation.

use pg_lakebase_core::fdw::{
    BeginForeignScanContext, FdwScan, ForeignPathBuilder, ForeignPathContext,
    ForeignPathKeys, ForeignPlanContext, ForeignPlanSpec, ForeignRelContext,
    ForeignRelSize, ForeignRelSizeContext, ForeignScanError,
    ReScanForeignScanContext, ScanSlotWriter,
};

use crate::format::FormatScanPlanner;

use super::super::{Lakebase, ResolvedForeignRelation};
use super::private::LakebaseScanPrivate;
use super::state::LakebaseScanState;

/// Planner state contains the selected format reader's scan planner. The
/// format is resolved once during `GetForeignRelSize`; no format branch is
/// needed in later callbacks.
pub(crate) struct LakebaseScanPlanner {
    scan: Box<dyn FormatScanPlanner>,
}

impl FdwScan for Lakebase {
    type PlannerState = LakebaseScanPlanner;
    type PrivateData = LakebaseScanPrivate;
    type State = LakebaseScanState;

    fn init_planner(
        context: &ForeignRelContext<'_>,
    ) -> Result<Self::PlannerState, ForeignScanError> {
        let relation = ResolvedForeignRelation::resolve(context.relation_oid())?;
        Ok(LakebaseScanPlanner {
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

    fn begin(
        context: BeginForeignScanContext<'_, Self>,
    ) -> Result<Self::State, ForeignScanError> {
        LakebaseScanState::begin(context)
    }

    fn next_slot(
        state: &mut Self::State,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        state.next_slot(output)
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
