use crate::IcebergTableAm;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::IcebergResult;
use crate::storage::StorageContext;
use pg_lakebase_core::diag::report_warning;
use pg_lakebase_core::diag::PgReportError;
use pg_lakebase_core::prelude::*;
use pg_lakebase_core::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceRequest, TableMaintenanceRouter,
};
use pgrx::pg_sys;

impl AmRelation for IcebergTableAm {
    fn relation_needs_toast_table(_rel: &RelationHandle) -> AmResult<bool> {
        // Iceberg values live in managed data files; PostgreSQL must not attach
        // a heap TOAST relation to the table-AM relation.
        Ok(false)
    }

    fn relation_estimate_size(
        rel: &RelationHandle,
        _attr_widths: Option<&mut AttrWidthsHandle>,
    ) -> AmResult<(pg_sys::BlockNumber, f64, f64)> {
        let stats = RelationStats::load_or_default(rel);

        Ok((stats.pages(), stats.rows as f64, 0.0))
    }

    fn relation_size(
        rel: &RelationHandle,
        fork_number: pg_sys::ForkNumber::Type,
    ) -> AmResult<u64> {
        if fork_number != pg_sys::ForkNumber::MAIN_FORKNUM {
            return Ok(0);
        }

        Ok(RelationStats::load_or_default(rel).bytes)
    }

    fn relation_vacuum(
        rel: &RelationHandle,
        params: &VacuumParamsHandle,
        _bstrategy: &BufferAccessStrategyHandle,
    ) -> AmResult<()> {
        if unsafe { pg_sys::AmAutoVacuumWorkerProcess() } {
            return Ok(());
        }
        let options = TableMaintenanceOptions::from_vacuum_params(params);
        if !options.process_main {
            return Ok(());
        }
        let command_time = TableMaintenanceCommandTime::now()
            .map_err(PgReportError::from_domain_error)?;
        TableMaintenanceRouter::execute(TableMaintenanceRequest {
            relation: rel,
            mode: TableMaintenanceMode::Routine,
            options,
            budget: TableMaintenanceBudget::configured(),
            command_time,
        })
        .map_err(PgReportError::from_domain_error)?;
        Ok(())
    }
}

#[derive(Default)]
struct RelationStats {
    rows: u64,
    bytes: u64,
}

impl RelationStats {
    /// Load Iceberg snapshot statistics for the planner.
    ///
    /// The planner calls this on the read path and must never fail because
    /// statistics are unavailable: missing or unreadable metadata should
    /// degrade gracefully into "no information" rather than abort the query.
    /// Failures are surfaced as a warning so operators can still notice them.
    fn load_or_default(rel: &RelationHandle) -> Self {
        match Self::try_load(rel) {
            Ok(stats) => stats,
            Err(err) => {
                report_warning(format_args!(
                    "pg_iceberg_am: failed to load Iceberg statistics for relation {}: {err}; planner will use default estimates",
                    rel.oid(),
                ));
                Self::default()
            }
        }
    }

    fn try_load(rel: &RelationHandle) -> IcebergResult<Self> {
        let ctx = StorageContext::for_tablespace(rel.tablespace_oid())?;
        let loaded =
            TxMetadata::current().current_table_metadata(rel.oid(), ctx.file_io())?;
        let (rows, bytes) = loaded.relation_stats(ctx.file_io())?;

        Ok(Self { rows, bytes })
    }

    fn pages(&self) -> pg_sys::BlockNumber {
        let page_size = pg_sys::BLCKSZ as u64;
        let pages = self.bytes.div_ceil(page_size);
        pages.min(u32::MAX as u64) as pg_sys::BlockNumber
    }
}
