//! LagoDB connector templates for foreign-table maintenance capabilities.

use pg_lakebase_core::fdw::{
    FdwAnalyze, FdwTruncate, ForeignAnalyzeContext, ForeignAnalyzeSupport,
    ForeignSampleContext, ForeignSampleStatistics, ForeignTableMaintenanceError,
    ForeignTruncateContext,
};
use pg_lakebase_core::storage::foreign::StorageManager;

use crate::error::ConnectorError;
use crate::storage::ObjectInput;

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
        let manager = StorageManager::from_pg_gucs().map_err(ConnectorError::from)?;
        let input = ObjectInput::resolve(&target, &manager, kind)?;
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
        let manager = StorageManager::from_pg_gucs().map_err(ConnectorError::from)?;
        let files = ObjectInput::resolve(&target, &manager, kind)?.open();
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
