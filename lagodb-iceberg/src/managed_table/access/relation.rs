use crate::error::IcebergResult;
use crate::managed_table::IcebergTableAm;
use crate::managed_table::catalog::metadata_tracker::TxMetadata;
use crate::managed_table::storage::StorageContext;
use lagodb_core::diag::PgReportError;
use lagodb_core::diag::report_warning;
use lagodb_core::prelude::*;
use lagodb_core::table_maintenance::{
    LagodbTableMaintenanceProvider, TableMaintenanceBudget,
    TableMaintenanceCommandTime, TableMaintenanceMode, TableMaintenanceOptions,
    TableMaintenanceRequest,
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

        Ok((stats.pages(), stats.estimated_live_rows(rel), 0.0))
    }

    fn relation_size(
        rel: &RelationHandle,
        fork_number: pg_sys::ForkNumber::Type,
    ) -> AmResult<u64> {
        if fork_number != pg_sys::ForkNumber::MAIN_FORKNUM {
            return Ok(0);
        }

        Ok(RelationStats::load_or_default(rel).representable_bytes())
    }

    fn relation_vacuum(
        rel: &RelationHandle,
        params: &VacuumParamsHandle,
        _bstrategy: &BufferAccessStrategyHandle,
    ) -> AmResult<()> {
        if unsafe { pg_sys::MyBackendType == pg_sys::BackendType::B_AUTOVAC_WORKER } {
            return Ok(());
        }
        let options = TableMaintenanceOptions::from_vacuum_params(params);
        if !options.process_main {
            return Ok(());
        }
        let command_time = TableMaintenanceCommandTime::now()
            .map_err(PgReportError::from_domain_error)?;
        <crate::managed_table::maintenance::IcebergTableMaintenanceProvider as LagodbTableMaintenanceProvider>::execute(
            TableMaintenanceRequest {
                relation: rel,
                mode: TableMaintenanceMode::Routine,
                options,
                budget: TableMaintenanceBudget::configured(),
                command_time,
            },
        )
        .map_err(PgReportError::from_domain_error)?;
        Ok(())
    }
}

#[derive(Default)]
struct RelationStats {
    rows: u64,
    bytes: u64,
    may_have_row_deletes: bool,
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
                    "lagodb_iceberg: failed to load Iceberg statistics for relation {}: {err}; planner will use default estimates",
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
        let may_have_row_deletes = loaded.may_have_row_deletes();

        Ok(Self {
            rows,
            bytes,
            may_have_row_deletes,
        })
    }

    fn pages(&self) -> pg_sys::BlockNumber {
        let page_size = pg_sys::BLCKSZ as u64;
        let pages = self.bytes.div_ceil(page_size);
        pg_sys::BlockNumber::try_from(pages.min(u32::MAX as u64 - 1))
            .expect("capped Iceberg page count fits PostgreSQL BlockNumber")
    }

    fn representable_bytes(&self) -> u64 {
        let max = (u32::MAX as u64 - 1).saturating_mul(pg_sys::BLCKSZ as u64);
        self.bytes.min(max)
    }

    fn estimated_live_rows(&self, rel: &RelationHandle) -> f64 {
        if !self.may_have_row_deletes {
            return self.rows as f64;
        }

        let analyzed_rows = f64::from(rel.reltuples());
        if analyzed_rows >= 0.0 {
            // Do not apply PostgreSQL's heap-style page scaling here. Iceberg
            // relation bytes include delete files: adding a delete file grows
            // the byte/page count while reducing, rather than increasing, the
            // live population. The last ANALYZE estimate remains the best
            // available visibility-aware value until ANALYZE runs again.
            return analyzed_rows;
        }

        // Before the first ANALYZE, manifest records are a safe upper bound:
        // delete files can only reduce the logical live population.
        self.rows as f64
    }
}
