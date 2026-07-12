#[pgrx::pg_schema]
mod lakebase {
    use std::time::Duration;

    use crate::storage::ObjectTreeObserver;
    use pg_lakebase_core::diag::PgReportError;
    use pg_lakebase_core::maintenance::{MaintenanceItemId, MaintenanceQueue};
    use pgrx::datum::Uuid;
    use pgrx::prelude::*;

    #[pg_extern]
    fn retry_maintenance_item(target_item_id: Uuid) -> bool {
        MaintenanceQueue::retry_failed(MaintenanceItemId::from_pg_uuid(
            target_item_id,
        ))
        .unwrap_or_else(|error| PgReportError::from(error).report())
    }

    #[pg_extern]
    fn object_tree_is_empty(store_id: &str, namespace: &str, prefix: &str) -> bool {
        const OBSERVER_TIMEOUT: Duration = Duration::from_secs(5);

        ObjectTreeObserver::connect(OBSERVER_TIMEOUT)
            .and_then(|observer| observer.is_empty(store_id, namespace, prefix))
            .unwrap_or_else(|error| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("failed to observe object tree: {error}"),
                )
                .report()
            })
    }
}

pgrx::extension_sql!(
    "REVOKE ALL ON FUNCTION lakebase.retry_maintenance_item(uuid) FROM PUBLIC;",
    name = "lock_down_retry_maintenance_item",
    requires = [lakebase::retry_maintenance_item],
);

pgrx::extension_sql!(
    "REVOKE ALL ON FUNCTION lakebase.object_tree_is_empty(text, text, text) FROM PUBLIC;",
    name = "lock_down_object_tree_is_empty",
    requires = [lakebase::object_tree_is_empty],
);
