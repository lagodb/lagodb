//! Scan planning for formats without object statistics.

use lagodb_core::fdw::{
    ForeignPathBuilder, ForeignPathContext, ForeignPathKeys, ForeignPathSpec,
    ForeignPlanContext, ForeignPlanSpec, ForeignRelSize, ForeignRelSizeContext,
    ScanProjectionPolicy,
};

use crate::error::ConnectorError;
use crate::fdw::{LagodbConnectors, PlannedFormat};
use crate::format::FormatKind;

use super::FormatScanPlanner;

const DEFAULT_ESTIMATED_ROWS: f64 = 1_000.0;
const DEFAULT_ESTIMATED_WIDTH: i32 = 32;

/// One unordered path with a fixed default estimate. The projection policy is
/// the format reader's scan-tuple requirement.
pub(super) struct FixedEstimateScanPlanner {
    kind: FormatKind,
    projection_policy: ScanProjectionPolicy,
}

impl FixedEstimateScanPlanner {
    pub(super) const fn new(
        kind: FormatKind,
        projection_policy: ScanProjectionPolicy,
    ) -> Self {
        Self {
            kind,
            projection_policy,
        }
    }
}

impl FormatScanPlanner for FixedEstimateScanPlanner {
    fn estimate(
        &mut self,
        _context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ConnectorError> {
        Ok(ForeignRelSize::new(
            DEFAULT_ESTIMATED_ROWS,
            DEFAULT_ESTIMATED_WIDTH,
        ))
    }

    fn build_paths(
        &self,
        _context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<PlannedFormat>,
    ) -> Result<(), ConnectorError> {
        paths.push(ForeignPathSpec::new(
            DEFAULT_ESTIMATED_ROWS,
            0.0,
            DEFAULT_ESTIMATED_ROWS,
            PlannedFormat::new(self.kind),
        ));
        Ok(())
    }

    fn supports_pathkeys(
        &self,
        _context: &ForeignPathContext<'_>,
        _pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ConnectorError> {
        Ok(false)
    }

    fn build_plan(
        &mut self,
        context: &ForeignPlanContext<'_, LagodbConnectors>,
    ) -> Result<ForeignPlanSpec<PlannedFormat>, ConnectorError> {
        let mut plan = ForeignPlanSpec::new(context.path_private().to_owned());
        plan.projection_policy = self.projection_policy;
        Ok(plan)
    }
}
