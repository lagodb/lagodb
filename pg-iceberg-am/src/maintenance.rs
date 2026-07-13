mod lakebase_api {
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
    r#"
ALTER FUNCTION object_tree_is_empty(text, text, text) SET SCHEMA iceberg;
REVOKE ALL ON FUNCTION iceberg.object_tree_is_empty(text, text, text) FROM PUBLIC;
"#,
    name = "lock_down_object_tree_is_empty",
    requires = [lakebase_api::object_tree_is_empty],
);
