// The `iceberg` schema is created by `sql/bootstrap.sql`. Declaring it as a
// pgrx `#[pg_schema]` module registers it in pgrx's SQL entity graph so a
// `schema`-targeted `#[pg_extern]` resolves during SQL generation; pgrx emits
// `CREATE SCHEMA IF NOT EXISTS iceberg`, which is a harmless no-op after the
// bootstrap `CREATE SCHEMA iceberg`. This mirrors how `pg_lakebase_runtime`
// pairs `#[pg_schema] mod lakebase` with its own bootstrap schema.
#[pgrx::pg_schema]
mod iceberg {
    use std::time::Duration;

    use crate::storage::ObjectTreeObserver;
    use pg_lakebase_core::diag::PgReportError;
    use pgrx::prelude::*;

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
    "REVOKE ALL ON FUNCTION iceberg.object_tree_is_empty(text, text, text) FROM PUBLIC;",
    name = "lock_down_object_tree_is_empty",
    requires = [iceberg::object_tree_is_empty],
);
