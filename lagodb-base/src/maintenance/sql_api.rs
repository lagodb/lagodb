//! PostgreSQL SQL adapters and worker entry point for physical maintenance.

use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::object_cleanup::{
    ObjectCleanupError, ObjectCleanupItemId, ObjectCleanupQueue,
    run_object_cleanup_worker,
};
use lagodb_core::table_maintenance::TableMaintenanceRouter;
use pgrx::PgRelation;
use pgrx::datum::{Internal, Uuid};
use pgrx::prelude::*;

use crate::ensure_runtime_preloaded;

#[derive(Debug, thiserror::Error)]
enum MaintenanceSqlError {
    #[error("failed to retry maintenance item: {source}")]
    RetryItem {
        #[source]
        source: ObjectCleanupError,
    },
}

impl SqlStateError for MaintenanceSqlError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::RetryItem { source } => source.sql_error_code(),
        }
    }
}

impl MaintenanceSqlError {
    fn report(self) -> ! {
        PgReportError::from_domain_error(self).report()
    }
}

#[pg_schema]
mod lagodb {
    use super::*;

    #[pg_extern]
    #[allow(clippy::type_complexity)]
    fn table_maintenance_stats(
        relation: PgRelation,
    ) -> TableIterator<
        'static,
        (
            name!(provider, String),
            name!(format, Option<String>),
            name!(history_points, i64),
            name!(current_content_objects, i64),
            name!(current_content_bytes, i64),
            name!(retained_content_objects, i64),
            name!(retained_content_bytes, i64),
            name!(current_data_objects, i64),
            name!(current_data_bytes, i64),
            name!(retained_data_objects, i64),
            name!(retained_data_bytes, i64),
        ),
    > {
        use lagodb_core::diag::{PgReportError, ReportableError};
        let relation = lagodb_core::handles::RelationGuard::open(
            relation.oid(),
            pg_sys::AccessShareLock as _,
        )
        .map_err(PgReportError::from_domain_error)
        .report_unwrap();
        let stats = TableMaintenanceRouter::inspect(&relation.as_handle())
            .map_err(PgReportError::from_domain_error)
            .report_unwrap();
        let sql_i64 = |value: u64, metric: &'static str| {
            i64::try_from(value).unwrap_or_else(|_| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                    format!("{metric} exceeds PostgreSQL bigint"),
                )
                .report()
            })
        };
        TableIterator::new(std::iter::once((
            stats.provider,
            stats.format,
            sql_i64(stats.history_points, "history-point count"),
            sql_i64(
                stats.current_content_objects,
                "current content object count",
            ),
            sql_i64(stats.current_content_bytes, "current content byte count"),
            sql_i64(
                stats.retained_content_objects,
                "retained content object count",
            ),
            sql_i64(stats.retained_content_bytes, "retained content byte count"),
            sql_i64(stats.current_data_objects, "current data object count"),
            sql_i64(stats.current_data_bytes, "current data byte count"),
            sql_i64(stats.retained_data_objects, "retained data object count"),
            sql_i64(stats.retained_data_bytes, "retained data byte count"),
        )))
    }

    #[pg_extern]
    fn maintenance_worker(worker_context: Internal) -> i64 {
        // SAFETY: this SQL-inaccessible `internal` argument is supplied only
        // by the worker entry point.
        let worker_context = unsafe {
            lagodb_core::extension_worker::WorkerContext::from_internal(
                &worker_context,
            )
        };
        run_object_cleanup_worker(&worker_context).into_raw()
    }

    #[pg_extern]
    fn retry_maintenance_item(target_item_id: Uuid) -> bool {
        ensure_runtime_preloaded();
        ObjectCleanupQueue::retry_failed(ObjectCleanupItemId::from_pg_uuid(
            target_item_id,
        ))
        .map_err(|source| MaintenanceSqlError::RetryItem { source })
        .unwrap_or_else(|error| error.report())
    }
}
