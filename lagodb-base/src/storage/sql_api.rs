//! PostgreSQL SQL adapters for storage service status and diagnostics.

use lagodb_core::object_cleanup::ObjectTreeObserver;
use pgrx::prelude::*;

use crate::ensure_runtime_preloaded;

#[pg_schema]
mod lagodb {
    use super::*;

    #[pg_extern]
    fn observe_object_tree(
        volume_id: i64,
        namespace: &str,
        prefix: &str,
    ) -> TableIterator<'static, (name!(objects, i64), name!(bytes, i64))> {
        use lagodb_core::diag::PgReportError;
        let volume_id =
            lagodb_core::storage::volume::StorageVolumeId::try_from(volume_id)
                .unwrap_or_else(|_| pgrx::error!("invalid storage volume id"));
        let stats = ObjectTreeObserver::connect(std::time::Duration::from_secs(5))
            .and_then(|observer| observer.observe(volume_id, namespace, prefix))
            .unwrap_or_else(|error| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("failed to observe object tree: {error}"),
                )
                .report()
            });
        let sql_i64 = |value: u64, metric: &'static str| {
            i64::try_from(value).unwrap_or_else(|_| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                    format!("object-tree {metric} exceeds PostgreSQL bigint"),
                )
                .report()
            })
        };
        TableIterator::new(std::iter::once((
            sql_i64(stats.objects, "object count"),
            sql_i64(stats.bytes, "byte count"),
        )))
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)] // pgrx requires named SQL columns in this tuple.
    fn storage_service_status() -> TableIterator<
        'static,
        (
            name!(enabled, bool),
            name!(pid, Option<i32>),
            name!(state, &'static str),
            name!(socket_path, String),
            name!(cache_dir, String),
            name!(last_start_ms, Option<i64>),
            name!(last_stop_ms, Option<i64>),
            name!(last_reload_at_ms, Option<i64>),
            name!(reload_generation, i64),
            name!(last_reload_added, i64),
            name!(last_reload_removed, i64),
            name!(last_reload_replaced, i64),
            name!(last_reload_unchanged, i64),
            name!(desired_volume_count, i64),
            name!(loaded_volume_count, i64),
            name!(stale_volume_count, i64),
            name!(unavailable_volume_count, i64),
            name!(last_error_at_ms, Option<i64>),
            name!(last_error, Option<String>),
        ),
    > {
        ensure_runtime_preloaded();
        let status = super::super::runtime_status();
        TableIterator::new(std::iter::once((
            status.enabled,
            status.pid,
            status.state,
            status.socket_path,
            status.cache_dir,
            status.last_start_ms,
            status.last_stop_ms,
            status.last_reload_at_ms,
            status.reload_generation,
            status.last_reload_added,
            status.last_reload_removed,
            status.last_reload_replaced,
            status.last_reload_unchanged,
            status.desired_volume_count,
            status.loaded_volume_count,
            status.stale_volume_count,
            status.unavailable_volume_count,
            status.last_error_at_ms,
            status.last_error,
        )))
    }
}

pgrx::extension_sql!(
    "REVOKE ALL ON FUNCTION lagodb.observe_object_tree(bigint, text, text) FROM PUBLIC;",
    name = "lock_down_observe_object_tree",
    requires = [lagodb::observe_object_tree],
);
