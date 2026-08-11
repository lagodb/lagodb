#[pgrx::pg_schema]
mod error_tests {
    use std::error::Error as StdError;
    use std::io;

    use pg_lakebase_core::copy::CopyError;
    use pg_lakebase_core::diag::SqlStateError;
    use pg_lakebase_storage::StorageError;
    use pgrx::pg_test;
    use pgrx::prelude::PgSqlErrorCode;

    use super::super::ConnectorError;

    #[pg_test]
    fn copy_stream_preserves_nested_storage_sqlstate_at_core_boundary() {
        let cases = [
            (
                StorageError::resource_exhausted("COPY reader limit"),
                PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
            ),
            (
                StorageError::protocol("COPY reader protocol"),
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            ),
            (
                StorageError::closed_handle(42),
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            ),
            (
                StorageError::conflict("COPY reader conflict"),
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            ),
            (
                StorageError::io(
                    "COPY reader I/O",
                    io::Error::other("connection failed"),
                ),
                PgSqlErrorCode::ERRCODE_IO_ERROR,
            ),
        ];

        for (storage_error, expected) in cases {
            let connector_error =
                ConnectorError::copy_stream_io(io::Error::other(storage_error));
            let copy_error: CopyError = connector_error.into();
            assert_eq!(copy_error.sql_error_code(), expected);

            let mut source = StdError::source(&copy_error);
            let mut storage_source_found = false;
            while let Some(cause) = source {
                if cause.downcast_ref::<StorageError>().is_some() {
                    storage_source_found = true;
                    break;
                }
                source = cause.source();
            }
            assert!(storage_source_found);
        }
    }

    #[pg_test]
    fn copy_stream_codec_error_remains_io_error() {
        let connector_error = ConnectorError::copy_stream_io(io::Error::other(
            "invalid compressed stream",
        ));
        let copy_error: CopyError = connector_error.into();
        assert_eq!(
            copy_error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_IO_ERROR
        );
    }
}
