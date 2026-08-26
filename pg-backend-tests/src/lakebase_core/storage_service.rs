//! Backend `#[pg_test]` coverage for `pg_lakebase_core::storage::service`.
//!
//! `StorageEndpoint::from_config` (and `from_pg_gucs`) resolve default paths
//! through `pg_sys::DataDir`, a PostgreSQL backend data symbol. Per
//! `lagodb-iceberg/docs/testing.md`, a code path that transitively references
//! backend symbols cannot be exercised from an ordinary host `#[test]`: the
//! host test binary would fail to load with an unresolved `DataDir` symbol.
//! These tests therefore live here as `#[pg_test]` and run inside PostgreSQL.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::path::{Path, PathBuf};

    use pg_lakebase_core::storage::service::StorageEndpoint;
    use pgrx::pg_test;

    #[pg_test]
    fn explicit_paths_are_returned_verbatim() {
        let endpoint = StorageEndpoint::from_config(
            true,
            Some(PathBuf::from("/tmp/lagodb.sock")),
            Some(PathBuf::from("/tmp/lakebase-cache")),
            8,
        )
        .expect("explicit paths resolve without consulting the data directory");

        assert!(endpoint.is_enabled());
        assert_eq!(endpoint.socket_path(), Path::new("/tmp/lagodb.sock"));
        assert_eq!(endpoint.cache_dir(), Path::new("/tmp/lakebase-cache"));
        assert_eq!(endpoint.max_idle_connections(), 8);
    }

    #[pg_test]
    fn missing_paths_fall_back_to_the_data_directory() {
        let endpoint = StorageEndpoint::from_config(true, None, None, 8)
            .expect("default paths resolve from the backend data directory");

        assert!(endpoint.is_enabled());
        assert!(
            endpoint.socket_path().ends_with("pg_lakebase/storage.sock"),
            "unexpected default socket path: {}",
            endpoint.socket_path().display()
        );
        assert!(
            endpoint.cache_dir().ends_with("pg_lakebase/storage-cache"),
            "unexpected default cache dir: {}",
            endpoint.cache_dir().display()
        );
    }
}
