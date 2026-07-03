//! Error layering for the Iceberg table access method.
//!
//! Keep Iceberg business logic on [`IcebergResult<T>`] and [`IcebergError`].
//! The PostgreSQL table-AM callback boundary returns
//! `pg_lakebase_core::api::AmResult<T>`, which owns a PostgreSQL
//! `ErrorReport` through a small error handle.
//! The bridge is the `From<IcebergError> for ErrorReport` implementation in
//! this file, so callback methods can use normal `?` propagation.
//!
//! Avoid adding `try_*` callback wrappers or scattered
//! `.map_err(Into::into)` / `.into()` conversions in access-method code. If
//! third-party errors need adaptation, keep that inside meaningful Iceberg
//! object methods returning [`IcebergResult<T>`], then let the callback boundary
//! perform the final conversion to PostgreSQL.

use pg_lakebase_core::diag::{PgError, SqlStateError, domain_error_report};
use pg_lakebase_core::options::TablespaceError;
use pg_lakebase_core::options::{TableOptionError, TablespaceCacheError};
use pg_lakebase_storage::{StorageError, StorageErrorKind};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use std::fmt::{Display, Formatter};
use thiserror::Error;

// ============================================================================
//  Metadata Catalog Operation
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCatalogOperation {
    Access,
    Insert,
    Read,
    Update,
    Delete,
}

impl Display for MetadataCatalogOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Access => f.write_str("access"),
            Self::Insert => f.write_str("insert"),
            Self::Read => f.write_str("read"),
            Self::Update => f.write_str("update"),
            Self::Delete => f.write_str("delete"),
        }
    }
}

// ============================================================================
//  Iceberg Error
// ============================================================================

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error("failed to {operation} lakebase.iceberg_metadata catalog: {source}")]
    MetadataCatalog {
        operation: MetadataCatalogOperation,
        #[source]
        source: PgError,
    },

    #[error("metadata catalog record not found for relid: {0}")]
    MetadataCatalogNotFound(pg_sys::Oid),

    #[error("metadata catalog record already exists for relid: {0}")]
    MetadataCatalogAlreadyExists(pg_sys::Oid),

    #[error("invalid metadata catalog record: {0}")]
    MetadataCatalogInvalidRecord(String),

    #[error("optimistic locking failed: metadata location changed concurrently")]
    MetadataCatalogConflict,

    #[error("metadata tracker error: {0}")]
    MetadataTracker(String),

    #[error(
        "Iceberg mutation exceeds the synthetic ctid limit of {max_files} data files per transaction and relation"
    )]
    FileIdLimitExceeded { max_files: usize },

    #[error("Iceberg row identity exceeds the synthetic ctid capacity")]
    RowIdentityLimitExceeded,

    #[error(
        "failed to commit metadata for relid {relid} after {max_retries} retries due to concurrent updates"
    )]
    MetadataCommitConflict {
        relid: pg_sys::Oid,
        max_retries: i32,
    },

    #[error("tablespace error: {0}")]
    TablespaceError(#[from] TablespaceError),

    #[error("tablespace cache error: {0}")]
    TablespaceCacheError(#[from] TablespaceCacheError),

    #[error("table option error: {0}")]
    TableOptionError(#[from] TableOptionError),

    #[error("storage error: {0}")]
    StorageError(#[from] pg_lakebase_storage::StorageError),

    #[error("postgres error: {0}")]
    PgError(#[from] PgError),

    #[error("conversion error: {0}")]
    ConvError(#[from] pg_arrow_conv::ConvError),

    #[error("tablespace options not found")]
    TablespaceNotFound,

    #[error("namespace name is null")]
    NamespaceNull,

    #[error("metadata location is null")]
    MetadataLocationNull,

    #[error("schema build error: {0}")]
    SchemaBuildError(String),

    #[error("column {0} is not found in source")]
    ColumnNotFound(String),

    #[error(
        "required column \"{0}\" has no live PostgreSQL column to write \
         (was it dropped without a default?)"
    )]
    RequiredColumnMissingSource(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("cannot import column '{0}' data type '{1}'")]
    ImportColumnError(String, String),

    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(
        #[from] pgrx::datum::datetime_support::DateTimeConversionError,
    ),

    #[error("uuid error: {0}")]
    UuidConversionError(#[from] uuid::Error),

    #[error("numeric error: {0}")]
    NumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("iceberg error: {0}")]
    IcebergLiteError(#[from] iceberg_lite::Error),

    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("arrow type mismatch: expected {0}")]
    ArrowTypeMismatch(String),

    #[error("SPI error: {0}")]
    SpiError(String),

    #[error("json error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// AM-internal invariant violation. Used for "cannot happen" branches
    /// where a runtime guard remains because the type system does not yet
    /// encode the invariant. Surfacing one of these in production is a bug
    /// in `pg_iceberg_am`, not a user error.
    ///
    /// Prefer expressing invariants directly in the type system (for
    /// example, an enum-based state machine) over guarding with this
    /// variant when the unreachable case can be made unrepresentable.
    #[error("invariant violation in pg_iceberg_am: {0}")]
    InvariantViolated(&'static str),

    #[error("feature not yet implemented: {0}")]
    NotImplemented(&'static str),
}

impl SqlStateError for IcebergError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            IcebergError::MetadataCatalog { source, .. } => source.sql_error_code(),

            IcebergError::MetadataCatalogNotFound(_) => {
                PgSqlErrorCode::ERRCODE_NO_DATA_FOUND
            }

            IcebergError::MetadataCatalogAlreadyExists(_) => {
                PgSqlErrorCode::ERRCODE_UNIQUE_VIOLATION
            }

            IcebergError::MetadataCatalogConflict => {
                PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
            }

            IcebergError::MetadataCatalogInvalidRecord(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::TablespaceError(error) => error.sql_error_code(),

            IcebergError::TablespaceCacheError(error) => error.sql_error_code(),

            IcebergError::TableOptionError(error) => error.sql_error_code(),

            IcebergError::StorageError(error) => storage_sql_error_code(error),

            IcebergError::TablespaceNotFound => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }

            IcebergError::PgError(error) => error.sql_error_code(),

            IcebergError::ConvError(conv) => conv.sql_error_code(),

            IcebergError::MetadataTracker(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::FileIdLimitExceeded { .. }
            | IcebergError::RowIdentityLimitExceeded => {
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED
            }

            IcebergError::MetadataCommitConflict { .. } => {
                PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
            }

            IcebergError::NamespaceNull | IcebergError::MetadataLocationNull => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
            }

            IcebergError::SchemaBuildError(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION
            }

            IcebergError::ColumnNotFound(_) => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN
            }

            IcebergError::RequiredColumnMissingSource(_) => {
                PgSqlErrorCode::ERRCODE_NOT_NULL_VIOLATION
            }

            IcebergError::UnsupportedColumnType(_)
            | IcebergError::ImportColumnError(_, _) => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }

            IcebergError::ParseFloatError(_)
            | IcebergError::DatetimeConversionError(_)
            | IcebergError::UuidConversionError(_)
            | IcebergError::NumericError(_) => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,

            IcebergError::IcebergLiteError(error) => {
                iceberg_lite_sql_error_code(error)
            }

            IcebergError::ArrowError(_)
            | IcebergError::ArrowTypeMismatch(_)
            | IcebergError::JsonError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::SpiError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::InvariantViolated(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::NotImplemented(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
        }
    }
}

impl From<IcebergError> for ErrorReport {
    fn from(value: IcebergError) -> Self {
        domain_error_report(value)
    }
}

pub type IcebergResult<T> = Result<T, IcebergError>;

impl IcebergError {
    pub fn metadata_catalog(
        operation: MetadataCatalogOperation,
        source: PgError,
    ) -> Self {
        Self::MetadataCatalog { operation, source }
    }
}

fn storage_sql_error_code(error: &StorageError) -> PgSqlErrorCode {
    match error.kind() {
        StorageErrorKind::InvalidPath | StorageErrorKind::Configuration => {
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
        }
        StorageErrorKind::NotFound => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
        StorageErrorKind::Unsupported => {
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        }
        StorageErrorKind::Busy => PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
        StorageErrorKind::ResourceExhausted => {
            PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
        }
        StorageErrorKind::Io
        | StorageErrorKind::Backend
        | StorageErrorKind::Cache
        | StorageErrorKind::CacheFillAborted => PgSqlErrorCode::ERRCODE_IO_ERROR,
        StorageErrorKind::Protocol | StorageErrorKind::ClosedHandle => {
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
        }
    }
}

fn iceberg_lite_sql_error_code(error: &iceberg_lite::Error) -> PgSqlErrorCode {
    use iceberg_lite::ErrorKind;
    match error.kind() {
        ErrorKind::FeatureUnsupported => {
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        }
        // Object-store / file IO failure surfacing from the scan or writer.
        ErrorKind::IoError => PgSqlErrorCode::ERRCODE_IO_ERROR,
        // Unparseable or corrupted Iceberg metadata / data files.
        ErrorKind::DataInvalid => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
        // Optimistic catalog commit lost the race to a concurrent update;
        // matches how the metadata-tracker conflicts are classified so the
        // executor can retry.
        ErrorKind::CatalogCommitConflicts => {
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        }
        // Optimistic row-level validation found concurrent data changes. The
        // same Iceberg commit must not be retried transparently; PostgreSQL
        // aborts the transaction and lets the client rebuild it.
        ErrorKind::DataConflict => PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE,
        ErrorKind::TableNotFound => PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
        ErrorKind::TableAlreadyExists => PgSqlErrorCode::ERRCODE_DUPLICATE_TABLE,
        ErrorKind::NamespaceNotFound => PgSqlErrorCode::ERRCODE_INVALID_SCHEMA_NAME,
        ErrorKind::NamespaceAlreadyExists => PgSqlErrorCode::ERRCODE_DUPLICATE_SCHEMA,
        ErrorKind::PreconditionFailed => {
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
        }
        // `Unexpected` and any future `#[non_exhaustive]` kind: an opaque
        // internal error with no more specific SQLSTATE.
        _ => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_catalog_sqlstate_survives_iceberg_error_boundary() {
        let conflict = IcebergError::MetadataCatalogConflict;
        assert_eq!(
            conflict.sql_error_code(),
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        );

        let not_found = IcebergError::MetadataCatalogNotFound(pg_sys::Oid::from(42));
        assert_eq!(
            not_found.sql_error_code(),
            PgSqlErrorCode::ERRCODE_NO_DATA_FOUND
        );
    }

    #[test]
    fn retry_exhaustion_reports_serialization_failure() {
        let error = IcebergError::MetadataCommitConflict {
            relid: pg_sys::Oid::from(42),
            max_retries: 3,
        };

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        );
    }

    #[test]
    fn iceberg_lite_feature_unsupported_reports_feature_not_supported() {
        let error = IcebergError::IcebergLiteError(iceberg_lite::Error::new(
            iceberg_lite::ErrorKind::FeatureUnsupported,
            "catalog method not implemented",
        ));

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        );
    }

    #[test]
    fn iceberg_data_conflict_requires_client_transaction_retry() {
        let source = iceberg_lite::Error::new(
            iceberg_lite::ErrorKind::DataConflict,
            "concurrent row delta conflict",
        );
        assert!(!source.retryable());
        let error = IcebergError::IcebergLiteError(source);

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        );
    }

    #[test]
    fn non_retryable_iceberg_precondition_preserves_prerequisite_state() {
        let error = IcebergError::IcebergLiteError(iceberg_lite::Error::new(
            iceberg_lite::ErrorKind::PreconditionFailed,
            "invalid operation state",
        ));

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
        );
    }

    /// Every representative `ConvError` variant must report the same SQLSTATE
    /// before and after being wrapped in `IcebergError::ConvError`.
    #[test]
    fn conv_error_sqlstate_survives_iceberg_error_boundary() {
        use pgrx::datum::datetime_support::DateTimeConversionError;
        use pgrx::datum::numeric_support::error::Error as PgNumericError;

        let representatives = [
            // DATATYPE_MISMATCH group
            pg_arrow_conv::ConvError::UnsupportedColumnType(
                "Decimal256(76, 10)".into(),
            ),
            pg_arrow_conv::ConvError::IncompatibleColumnType(
                "FixedSizeBinary(16)".into(),
                "expected uuid".into(),
            ),
            pg_arrow_conv::ConvError::ArrowTypeMismatch("Int32Array".into()),
            // DATA_EXCEPTION group
            pg_arrow_conv::ConvError::DatumConversionError(
                "value out of range".into(),
            ),
            pg_arrow_conv::ConvError::NumericError(PgNumericError::Invalid(
                "not a numeric".into(),
            )),
            pg_arrow_conv::ConvError::DatetimeConversionError(
                DateTimeConversionError::FieldOverflow,
            ),
            pg_arrow_conv::ConvError::InvalidUtf8({
                let bytes: &[u8] = &[0xff, 0xfe];
                // Intentionally invalid UTF-8: we need a real `Utf8Error` to
                // exercise the error-code mapping below.
                #[allow(invalid_from_utf8)]
                let err = std::str::from_utf8(bytes).unwrap_err();
                err
            }),
            // INTERNAL_ERROR group
            pg_arrow_conv::ConvError::ArrowError(
                arrow_schema::ArrowError::SchemaError("bad schema".into()),
            ),
            pg_arrow_conv::ConvError::DecimalCodecBug(
                "malformed numeric bytes".into(),
            ),
        ];

        for conv in representatives {
            let expected = conv.sql_error_code();
            let wrapped = IcebergError::from(conv);
            assert_eq!(
                wrapped.sql_error_code(),
                expected,
                "IcebergError::ConvError must preserve the inner ConvError SQLSTATE"
            );
        }
    }

    /// A `ConvError` must cross into `IcebergError` through plain `?`
    /// propagation, since the write path relies on the `#[from]` conversion to
    /// surface conversion failures without manual mapping.
    #[test]
    fn conv_error_propagates_into_iceberg_error_via_question_mark() {
        fn boundary() -> IcebergResult<()> {
            Err(pg_arrow_conv::ConvError::DatumConversionError(
                "value out of range".into(),
            ))?;
            Ok(())
        }

        let err = boundary().expect_err("ConvError should propagate as IcebergError");
        assert!(matches!(err, IcebergError::ConvError(_)));
    }

    /// A decode-path `ConvError` (here the physical-array mismatch the column
    /// decoder raises) must reach the scan boundary through plain `?`, so the
    /// decoder needs no bespoke mapping to surface failures as `IcebergError`.
    #[test]
    fn decode_conv_error_propagates_into_iceberg_error_via_question_mark() {
        fn boundary() -> IcebergResult<()> {
            Err(pg_arrow_conv::ConvError::ArrowTypeMismatch(
                "expected Int32Array".into(),
            ))?;
            Ok(())
        }

        let err = boundary()
            .expect_err("decode ConvError should propagate as IcebergError");
        assert!(matches!(err, IcebergError::ConvError(_)));
    }

    /// The UUID-conversion arm needs a real `uuid::Error`, constructed here so
    /// the representative set above stays free of fallible setup.
    #[test]
    fn conv_uuid_error_sqlstate_survives_iceberg_error_boundary() {
        let uuid_err = uuid::Uuid::parse_str("not-a-uuid").unwrap_err();
        let conv = pg_arrow_conv::ConvError::UuidConversionError(uuid_err);

        let expected = conv.sql_error_code();
        assert_eq!(expected, PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
        assert_eq!(IcebergError::from(conv).sql_error_code(), expected);
    }

    /// Each `DecimalCodecError` arm, routed through `ConvError` and wrapped in
    /// `IcebergError`, must land on its expected SQLSTATE class. The codec
    /// error reaches `IcebergError` only via `pg_arrow_conv::ConvError` (the
    /// AM no longer routes `DecimalCodecError` directly), so this is the one
    /// routing that must hold.
    #[test]
    fn decimal_codec_error_sqlstate_classes_survive_the_boundary() {
        use pg_lakebase_core::tuple::DecimalCodecError;

        let cases: [(DecimalCodecError, PgSqlErrorCode); 4] = [
            (
                DecimalCodecError::PrecisionOutOfRange { precision: 40 },
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
            ),
            (
                DecimalCodecError::ScaleOutOfRange {
                    precision: 10,
                    scale: 20,
                },
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
            ),
            (
                DecimalCodecError::ValueOutOfRange {
                    precision: 10,
                    scale: 2,
                    message: "value exceeds NUMERIC(10, 2)".into(),
                },
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
            ),
            (
                DecimalCodecError::InvalidBinaryRepresentation {
                    message: "numeric_recv rejected wire bytes".into(),
                },
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            ),
        ];

        for (codec_err, expected_class) in cases {
            // DecimalCodecError -> ConvError -> IcebergError.
            let conv: pg_arrow_conv::ConvError = codec_err.into();
            let via_conv = IcebergError::from(conv);

            assert_eq!(
                via_conv.sql_error_code(),
                expected_class,
                "DecimalCodecError -> ConvError -> IcebergError must preserve the SQLSTATE class"
            );
        }
    }
}
