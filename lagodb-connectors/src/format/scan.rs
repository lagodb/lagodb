//! Reader-owned scan, filter, and pathkey contracts.

use pg_lakebase_core::expr::pushdown::FilterPlanningContext;
use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignPathBuilder, ForeignPathContext, ForeignPathKeys,
    ForeignPlanContext, ForeignPlanSpec, ForeignRelSize, ForeignRelSizeContext,
    ReScanForeignScanContext, ScanSlotWriter,
};
use pg_lakebase_core::plan_data::PlanDataReader;

use crate::error::ConnectorError;
use crate::storage::ObjectFiles;

use super::filter::{
    FormatFilterPlanner, FormatPlannedFilter, NoPushdownFilterPlanner,
};
use super::{FormatKind, FormatObject};
use crate::fdw::Lakebase;

/// Format selection persisted in the core FDW plan envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FormatScanPrivate {
    kind: FormatKind,
}

impl FormatScanPrivate {
    #[inline]
    pub(crate) const fn new(kind: FormatKind) -> Self {
        Self { kind }
    }

    #[inline]
    pub(crate) const fn kind(self) -> FormatKind {
        self.kind
    }
}

/// Reader capability of one concrete format.
///
/// Filter pushdown and pathkey handling are deliberately reached through
/// this reader capability. A format that supports either capability replaces
/// the corresponding default method on its own reader implementation.
pub(crate) trait FormatReader: FormatObject {
    /// Create relation-scoped scan planning state.
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(NotImplementedScanPlanner::new(self.kind()))
    }

    /// Create relation-scoped filter planning state.
    fn begin_filter_planning(
        self: Box<Self>,
        _context: &FilterPlanningContext,
    ) -> Result<Box<dyn FormatFilterPlanner>, ConnectorError> {
        Ok(Box::new(NoPushdownFilterPlanner))
    }

    /// Decode a planned predicate after the plan envelope selected this
    /// concrete format. This is a static capability because executor plan
    /// decoding has no relation catalog from which to rebuild configured
    /// format state.
    fn decode_filter(
        kind: FormatKind,
        _reader: &mut PlanDataReader<'_>,
        _binding_count: usize,
    ) -> Result<FormatPlannedFilter, ConnectorError>
    where
        Self: Sized,
    {
        Err(ConnectorError::invalid_filter_plan(kind))
    }

    /// Create executor state for the selected reader.
    fn begin(
        self: Box<Self>,
        _context: BeginForeignScanContext<'_, Lakebase>,
        _files: ObjectFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        Err(ConnectorError::scan_not_implemented(self.kind()))
    }
}

/// Planner-side scan behavior for one selected format.
///
/// Pathkey validation is part of this scan planner rather than a sibling
/// format operation. The planner is initialized once per relation-planning
/// lifecycle and is not consulted from the row path.
pub(crate) trait FormatScanPlanner: 'static {
    fn estimate(
        &mut self,
        context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ConnectorError>;

    fn build_paths(
        &self,
        context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<FormatScanPrivate>,
    ) -> Result<(), ConnectorError>;

    fn supports_pathkeys(
        &self,
        context: &ForeignPathContext<'_>,
        pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ConnectorError>;

    fn build_plan(
        &mut self,
        context: &ForeignPlanContext<'_, Lakebase>,
    ) -> Result<ForeignPlanSpec<FormatScanPrivate>, ConnectorError>;
}

/// Default planner used by a format before it has a concrete scan
/// implementation. It reports the missing scan implementation at planning
/// time and rejects ordered paths without inspecting every row.
struct NotImplementedScanPlanner {
    format: FormatKind,
}

impl NotImplementedScanPlanner {
    const fn new(format: FormatKind) -> Self {
        Self { format }
    }
}

impl FormatScanPlanner for NotImplementedScanPlanner {
    fn estimate(
        &mut self,
        _context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ConnectorError> {
        Err(ConnectorError::scan_not_implemented(self.format))
    }

    fn build_paths(
        &self,
        _context: &ForeignPathContext<'_>,
        _paths: &mut ForeignPathBuilder<FormatScanPrivate>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::scan_not_implemented(self.format))
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
        _context: &ForeignPlanContext<'_, Lakebase>,
    ) -> Result<ForeignPlanSpec<FormatScanPrivate>, ConnectorError> {
        Err(ConnectorError::scan_not_implemented(self.format))
    }
}

/// Per-foreign-scan state owned by the selected format reader.
pub(crate) trait FormatScanState: 'static {
    fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ConnectorError>;

    fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, Lakebase>,
    ) -> Result<(), ConnectorError>;

    fn end(&mut self) -> Result<(), ConnectorError>;
}
