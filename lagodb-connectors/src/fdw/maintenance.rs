//! LagoDB connector templates for foreign-table maintenance capabilities.

use pg_lakebase_core::fdw::{
    FdwAnalyze, FdwTruncate, ForeignAnalyzeContext, ForeignAnalyzeSupport,
    ForeignSampleContext, ForeignSampleStatistics,
    ForeignTableMaintenanceError, ForeignTruncateContext,
};

use crate::error::ConnectorError;

use super::Lakebase;

impl FdwAnalyze for Lakebase {
    fn analyze(
        _ctx: &ForeignAnalyzeContext<'_>,
    ) -> Result<Option<ForeignAnalyzeSupport>, ForeignTableMaintenanceError> {
        Ok(None)
    }

    fn acquire_sample_rows(
        _ctx: &mut ForeignSampleContext<'_>,
    ) -> Result<ForeignSampleStatistics, ForeignTableMaintenanceError> {
        Err(ConnectorError::AnalyzeNotImplemented.into())
    }
}

impl FdwTruncate for Lakebase {
    fn truncate(
        _ctx: &ForeignTruncateContext<'_>,
    ) -> Result<(), ForeignTableMaintenanceError> {
        Err(ConnectorError::TruncateNotImplemented.into())
    }
}
