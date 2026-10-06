//! LagoDB connector templates for foreign-table maintenance capabilities.

use lagodb_core::fdw::{
    FdwAnalyze, FdwTruncate, ForeignAnalyzeContext, ForeignAnalyzeSupport,
    ForeignSampleContext, ForeignSampleStatistics, ForeignTableMaintenanceError,
    ForeignTruncateContext,
};

use crate::error::ConnectorError;

use super::{LagodbConnectors, ResolvedForeignRelation};

impl FdwAnalyze for LagodbConnectors {
    fn analyze(
        ctx: &ForeignAnalyzeContext<'_>,
    ) -> Result<Option<ForeignAnalyzeSupport>, ForeignTableMaintenanceError> {
        let selected = ResolvedForeignRelation::resolve(ctx.relation().oid())?;
        let kind = selected.kind();
        let Some((analyzer, target)) =
            selected.into_analyze_parts(ctx.relation().owner_oid())?
        else {
            return Ok(None);
        };
        let input = kind.input(&target)?;
        Ok(Some(analyzer.support(input.total_bytes())))
    }

    fn acquire_sample_rows(
        ctx: &mut ForeignSampleContext<'_>,
    ) -> Result<ForeignSampleStatistics, ForeignTableMaintenanceError> {
        let selected = ResolvedForeignRelation::resolve(ctx.relation().oid())?;
        let kind = selected.kind();
        let (analyzer, target) = selected
            .into_analyze_parts(ctx.relation().owner_oid())?
            .expect("PostgreSQL installed sampling only for an analyzable format");
        let files = kind.input(&target)?.open();
        analyzer.acquire_sample_rows(ctx, files)
    }
}

impl FdwTruncate for LagodbConnectors {
    fn truncate(
        _ctx: &ForeignTruncateContext<'_>,
    ) -> Result<(), ForeignTableMaintenanceError> {
        Err(ConnectorError::TruncateNotImplemented.into())
    }
}
