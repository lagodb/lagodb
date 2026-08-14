//! Connector-domain errors and conversion at core execution boundaries.

use std::error::Error as StdError;

use pg_lakebase_core::copy::CopyError;
use pg_lakebase_core::diag::{PgReportError, SqlStateError};
use pg_lakebase_core::tuple::{DecimalCodecError, JsonValueError};
use pg_lakebase_core::fdw::{
    ForeignModifyError, ForeignScanError, ForeignTableMaintenanceError,
    ForeignValidationError,
};
use pg_lakebase_core::plan_data::PlanDataError;
use pg_lakebase_core::storage::foreign::StorageAcquireError;
use pg_lakebase_storage::StorageError;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::format::FormatKind;

/// Error owned by the connector domain.
///
/// This type never reports an error. COPY and FDW adapters convert it to their
/// corresponding core boundary errors, and the core FFI trampoline performs
/// the single PostgreSQL report.
#[derive(Debug, Error)]
pub(crate) enum ConnectorError {
    #[error("invalid lagodb connectors FDW option {option:?}: {reason}")]
    InvalidOption {
        option: Box<str>,
        reason: &'static str,
    },

    #[error("invalid COPY option {option:?}: {reason}")]
    InvalidCopyOption {
        option: Box<str>,
        reason: &'static str,
    },

    #[error("invalid format value {value:?}")]
    InvalidFormat { value: Box<str> },

    #[error("foreign table format cannot be inferred; specify a format option")]
    FormatRequired,

    #[error("invalid {format} object schema: {reason}")]
    InvalidObjectSchema {
        format: FormatKind,
        reason: Box<str>,
    },

    #[error("invalid Parquet object: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Parquet/PostgreSQL column conversion failed: {0}")]
    ArrowConversion(#[from] pg_arrow_conv::ArrowConversionError),

    #[error("Arrow record-batch processing failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("PostgreSQL datum conversion failed: {0}")]
    DatumConversion(#[from] pg_lakebase_core::tuple::DatumConversionError),

    #[error("PostgreSQL numeric conversion failed: {0}")]
    DecimalCodec(#[from] DecimalCodecError),

    #[error("invalid Avro object: {0}")]
    Avro(#[from] apache_avro::Error),

    #[error("invalid NDJSON object at logical line {line}: {source}")]
    Json {
        line: u64,
        #[source]
        source: serde_json::Error,
    },

    #[error("NDJSON record {line} exceeds the configured {max_bytes}-byte limit")]
    JsonRecordTooLarge { line: u64, max_bytes: usize },

    #[error("invalid NDJSON value at logical line {line}, column {column:?}: {reason}")]
    JsonValue {
        line: u64,
        column: Box<str>,
        reason: &'static str,
    },

    #[error("PostgreSQL JSON conversion failed: {0}")]
    JsonDatum(#[from] JsonValueError),

    #[error("NDJSON stream I/O failed: {source}")]
    JsonIo {
        sqlerrcode: PgSqlErrorCode,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to read an object while inferring its schema: {0}")]
    SchemaIo(#[from] std::io::Error),

    #[error("COPY stream I/O failed: {source}")]
    CopyStreamIo {
        sqlerrcode: PgSqlErrorCode,
        #[source]
        source: std::io::Error,
    },

    #[error(transparent)]
    Copy(CopyError),

    #[error("unsupported foreign table definition: {definition}")]
    UnsupportedForeignTableDefinition { definition: &'static str },

    #[error("invalid object URI: {reason}")]
    InvalidObjectUri { reason: &'static str },

    #[error("foreign server {server:?} does not exist")]
    ServerNotFound { server: Box<str> },

    #[error("foreign server {server:?} does not use lakebase_fdw")]
    ServerWrongFdw { server: Box<str> },

    #[error("permission denied for foreign server {server:?}")]
    ServerUsageDenied { server: Box<str> },

    #[error("foreign server {server:?} cannot access {scheme} object URIs")]
    ProviderMismatch {
        server: Box<str>,
        scheme: &'static str,
    },

    #[error("object URI is outside foreign server {server:?} scope")]
    ScopeDenied { server: Box<str> },

    #[error("COPY object format {format} is not implemented yet")]
    CopyNotImplemented { format: FormatKind },

    #[error("COPY FROM for {format} supports an exact object URI only")]
    CopyFromExactOnly { format: FormatKind },

    #[error("FDW private data contains unknown format tag {wire}")]
    InvalidPlanFormat { wire: i32 },

    #[error("foreign table format changed after the plan was created")]
    PlanFormatChanged,

    #[error("{format} format scan is not implemented")]
    ScanNotImplemented { format: FormatKind },

    #[error("{format} format modify is not implemented")]
    ModifyNotImplemented { format: FormatKind },

    #[error("foreign table ANALYZE is not implemented")]
    AnalyzeNotImplemented,

    #[error("foreign table TRUNCATE is not implemented")]
    TruncateNotImplemented,

    #[error("{format} format cannot decode its planned filter")]
    InvalidFilterPlan { format: FormatKind },

    #[error("connector plan-data error: {0}")]
    PlanData(#[from] PlanDataError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    StorageAcquire(Box<StorageAcquireError<ConnectorError>>),

    #[error(transparent)]
    ForeignModify(ForeignModifyError),

    #[error(transparent)]
    Postgres(#[from] PgReportError),
}

impl ConnectorError {
    #[inline]
    pub(crate) fn invalid_option(option: &str, reason: &'static str) -> Self {
        Self::InvalidOption {
            option: option.into(),
            reason,
        }
    }

    #[inline]
    pub(crate) fn invalid_copy_option(option: &str, reason: &'static str) -> Self {
        Self::InvalidCopyOption {
            option: option.into(),
            reason,
        }
    }

    #[inline]
    pub(crate) fn invalid_json_value(
        line: u64,
        column: &str,
        reason: &'static str,
    ) -> Self {
        Self::JsonValue {
            line,
            column: column.into(),
            reason,
        }
    }

    #[inline]
    pub(crate) fn json_datum(error: JsonValueError) -> Self {
        match error {
            JsonValueError::Postgres(error) => {
                Self::Postgres(PgReportError::from_pg_error(error))
            }
            error => Self::JsonDatum(error),
        }
    }

    #[inline]
    pub(crate) fn invalid_format(value: &str) -> Self {
        Self::InvalidFormat {
            value: value.into(),
        }
    }

    #[inline]
    pub(crate) const fn format_required() -> Self {
        Self::FormatRequired
    }

    #[inline]
    pub(crate) fn invalid_object_schema(
        format: FormatKind,
        reason: impl Into<Box<str>>,
    ) -> Self {
        Self::InvalidObjectSchema {
            format,
            reason: reason.into(),
        }
    }

    #[inline]
    pub(crate) const fn unsupported_foreign_table_definition(
        definition: &'static str,
    ) -> Self {
        Self::UnsupportedForeignTableDefinition { definition }
    }

    #[inline]
    pub(crate) const fn invalid_object_uri(reason: &'static str) -> Self {
        Self::InvalidObjectUri { reason }
    }

    #[inline]
    pub(crate) fn server_not_found(server: &str) -> Self {
        Self::ServerNotFound {
            server: server.into(),
        }
    }

    #[inline]
    pub(crate) fn server_wrong_fdw(server: &str) -> Self {
        Self::ServerWrongFdw {
            server: server.into(),
        }
    }

    #[inline]
    pub(crate) fn server_usage_denied(server: &str) -> Self {
        Self::ServerUsageDenied {
            server: server.into(),
        }
    }

    #[inline]
    pub(crate) fn provider_mismatch(server: &str, scheme: &'static str) -> Self {
        Self::ProviderMismatch {
            server: server.into(),
            scheme,
        }
    }

    #[inline]
    pub(crate) fn scope_denied(server: &str) -> Self {
        Self::ScopeDenied {
            server: server.into(),
        }
    }

    #[inline]
    pub(crate) const fn copy_not_implemented(format: FormatKind) -> Self {
        Self::CopyNotImplemented { format }
    }

    #[inline]
    pub(crate) const fn copy_from_exact_only(format: FormatKind) -> Self {
        Self::CopyFromExactOnly { format }
    }

    #[inline]
    pub(crate) fn copy_stream_io(error: std::io::Error) -> Self {
        let sqlerrcode = Self::source_io_sql_error_code(&error)
            .unwrap_or(PgSqlErrorCode::ERRCODE_IO_ERROR);
        Self::CopyStreamIo {
            sqlerrcode,
            source: error,
        }
    }

    #[inline]
    pub(crate) fn json_io(error: std::io::Error) -> Self {
        let sqlerrcode = Self::source_io_sql_error_code(&error)
            .unwrap_or(PgSqlErrorCode::ERRCODE_IO_ERROR);
        Self::JsonIo {
            sqlerrcode,
            source: error,
        }
    }

    fn source_io_sql_error_code(
        error: &(dyn StdError + 'static),
    ) -> Option<PgSqlErrorCode> {
        let mut current = Some(error);
        let mut contains_io = false;
        while let Some(source) = current {
            if let Some(storage) = source.downcast_ref::<StorageError>() {
                return Some(storage.sql_error_code());
            }
            contains_io |= source.is::<std::io::Error>();
            current = source.source();
        }
        contains_io.then_some(PgSqlErrorCode::ERRCODE_IO_ERROR)
    }

    #[inline]
    pub(crate) fn storage_acquire(
        error: StorageAcquireError<ConnectorError>,
    ) -> Self {
        Self::StorageAcquire(Box::new(error))
    }

    #[inline]
    pub(crate) fn foreign_modify(error: ForeignModifyError) -> Self {
        Self::ForeignModify(error)
    }

    #[inline]
    pub(crate) const fn invalid_plan_format(wire: i32) -> Self {
        Self::InvalidPlanFormat { wire }
    }

    #[inline]
    pub(crate) const fn plan_format_changed() -> Self {
        Self::PlanFormatChanged
    }

    #[inline]
    pub(crate) const fn scan_not_implemented(format: FormatKind) -> Self {
        Self::ScanNotImplemented { format }
    }

    #[inline]
    pub(crate) const fn modify_not_implemented(format: FormatKind) -> Self {
        Self::ModifyNotImplemented { format }
    }

    #[inline]
    pub(crate) const fn invalid_filter_plan(format: FormatKind) -> Self {
        Self::InvalidFilterPlan { format }
    }
}

impl SqlStateError for ConnectorError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::InvalidOption { .. } => {
                PgSqlErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME
            }
            Self::InvalidCopyOption { .. } => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::InvalidFormat { .. } => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::FormatRequired => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            Self::UnsupportedForeignTableDefinition { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::InvalidObjectSchema { .. }
            | Self::Json { .. }
            | Self::JsonRecordTooLarge { .. }
            | Self::JsonValue { .. } => {
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
            }
            Self::JsonDatum(error) => error.sql_error_code(),
            Self::Parquet(error) => {
                Self::source_io_sql_error_code(error).unwrap_or(match error {
                    parquet::errors::ParquetError::NYI(_) => {
                        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
                    }
                    _ => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                })
            }
            Self::Avro(error) => Self::source_io_sql_error_code(error)
                .unwrap_or(PgSqlErrorCode::ERRCODE_DATA_EXCEPTION),
            Self::ArrowConversion(error) => error.sql_error_code(),
            Self::Arrow(error) => Self::source_io_sql_error_code(error)
                .unwrap_or(PgSqlErrorCode::ERRCODE_DATA_EXCEPTION),
            Self::DatumConversion(error) => error.sql_error_code(),
            Self::DecimalCodec(error) => match error {
                DecimalCodecError::ValueOutOfRange { .. } => {
                    PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
                }
                DecimalCodecError::InvalidBinaryRepresentation { .. } => {
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
                }
                DecimalCodecError::PrecisionOutOfRange { .. }
                | DecimalCodecError::ScaleOutOfRange { .. } => {
                    PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
                }
            },
            Self::SchemaIo(error) => Self::source_io_sql_error_code(error)
                .unwrap_or(PgSqlErrorCode::ERRCODE_IO_ERROR),
            Self::JsonIo { sqlerrcode, .. } => *sqlerrcode,
            Self::CopyStreamIo { sqlerrcode, .. } => *sqlerrcode,
            Self::Copy(error) => error.sql_error_code(),
            Self::InvalidObjectUri { .. } | Self::ProviderMismatch { .. } => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::ServerNotFound { .. } => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            Self::ServerWrongFdw { .. }
            | Self::CopyNotImplemented { .. }
            | Self::CopyFromExactOnly { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::ServerUsageDenied { .. } | Self::ScopeDenied { .. } => {
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE
            }
            Self::InvalidPlanFormat { .. }
            | Self::InvalidFilterPlan { .. }
            | Self::PlanData(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::PlanFormatChanged => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }
            Self::Storage(error) => error.sql_error_code(),
            Self::StorageAcquire(error) => error.sql_error_code(),
            Self::ForeignModify(error) => error.sql_error_code(),
            Self::ScanNotImplemented { .. } | Self::ModifyNotImplemented { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::AnalyzeNotImplemented | Self::TruncateNotImplemented => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::Postgres(error) => error.sql_error_code(),
        }
    }
}

impl From<ConnectorError> for ForeignScanError {
    fn from(error: ConnectorError) -> Self {
        match error {
            ConnectorError::Postgres(error) => error.into(),
            ConnectorError::Copy(CopyError::Postgres(error)) => error.into(),
            error => ForeignScanError::provider(error),
        }
    }
}

impl From<CopyError> for ConnectorError {
    fn from(error: CopyError) -> Self {
        match error {
            CopyError::Postgres(error) => Self::Postgres(error),
            error => Self::Copy(error),
        }
    }
}

impl From<ConnectorError> for ForeignTableMaintenanceError {
    fn from(error: ConnectorError) -> Self {
        match error {
            ConnectorError::Postgres(error) => error.into(),
            error => ForeignTableMaintenanceError::provider(error),
        }
    }
}

impl From<ConnectorError> for CopyError {
    fn from(error: ConnectorError) -> Self {
        match error {
            ConnectorError::Postgres(error) => Self::Postgres(error),
            ConnectorError::Copy(error) => error,
            error => Self::provider(error),
        }
    }
}

impl From<ConnectorError> for ForeignModifyError {
    fn from(error: ConnectorError) -> Self {
        match error {
            ConnectorError::Postgres(error) => error.into(),
            ConnectorError::ForeignModify(error) => error,
            error => ForeignModifyError::provider(error),
        }
    }
}

impl From<ConnectorError> for ForeignValidationError {
    fn from(error: ConnectorError) -> Self {
        match error {
            ConnectorError::Postgres(error) => error.into(),
            error => ForeignValidationError::provider(error),
        }
    }
}
