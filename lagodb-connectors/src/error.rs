//! Connector-domain errors and conversion at core execution boundaries.

use std::error::Error as StdError;

use lagodb_arrow::ArrowConversionError;
use lagodb_core::copy::{CopyError, CopyRowRejection};
use lagodb_core::datum::{DatumConversionError, DecimalCodecError, JsonValueError};
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::fdw::{
    ForeignModifyError, ForeignScanError, ForeignTableMaintenanceError,
    ForeignValidationError,
};
use lagodb_core::plan_data::PlanDataError;
use lagodb_core::storage::foreign::StorageAcquireError;
use lagodb_core::storage::profile::StorageProfileError;
use lagodb_storage::StorageError;
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
    /// An option name the selected format or object does not accept.
    #[error("invalid option {option:?}: {reason}")]
    UnknownOption {
        option: Box<str>,
        reason: &'static str,
    },

    /// An accepted option name with an invalid value.
    #[error("invalid value for option {option:?}: {reason}")]
    InvalidOption {
        option: Box<str>,
        reason: &'static str,
    },

    #[error("conflicting or redundant option {option:?}: {reason}")]
    ConflictingOption {
        option: Box<str>,
        reason: &'static str,
    },

    #[error("invalid path: {reason}")]
    InvalidPath { reason: &'static str },

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
    ArrowConversion(#[from] ArrowConversionError),

    #[error("Arrow record-batch processing failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("PostgreSQL datum conversion failed: {0}")]
    DatumConversion(#[from] DatumConversionError),

    #[error("PostgreSQL numeric conversion failed: {0}")]
    DecimalCodec(#[from] DecimalCodecError),

    #[error("{format}/PostgreSQL row conversion failed: {reason}")]
    RowConversion {
        format: FormatKind,
        reason: Box<str>,
    },

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

    #[error(
        "invalid NDJSON value at logical line {line}, column {column:?}: {reason}"
    )]
    JsonValue {
        line: u64,
        column: Box<str>,
        reason: &'static str,
    },

    #[error("PostgreSQL JSON conversion failed: {0}")]
    JsonDatum(#[from] JsonValueError),

    #[error("failed to read an object while inferring its schema: {0}")]
    SchemaIo(#[from] std::io::Error),

    #[error("object stream I/O failed: {source}")]
    StreamIo {
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

    #[error("COPY FROM for {format} supports an exact object URI only")]
    CopyFromExactOnly { format: FormatKind },

    /// PostgreSQL reports its own format restriction, `only ON_ERROR STOP is
    /// allowed in BINARY mode`, as a syntax error.
    #[error("only ON_ERROR STOP is allowed for {format} format")]
    CopyOnErrorUnsupported { format: FormatKind },

    #[error("FDW private data contains unknown format tag {wire}")]
    InvalidPlanFormat { wire: i32 },

    #[error("foreign table format changed after the plan was created")]
    PlanFormatChanged,

    #[error("{format} format modify is not implemented")]
    ModifyNotImplemented { format: FormatKind },

    #[error("foreign table TRUNCATE is not implemented")]
    TruncateNotImplemented,

    #[error("{format} format cannot decode its planned filter")]
    InvalidFilterPlan { format: FormatKind },

    #[error("cannot decode PostgreSQL filter datum of type OID {type_oid}")]
    InvalidFilterDatum { type_oid: pgrx::pg_sys::Oid },

    #[error("connector plan-data error: {0}")]
    PlanData(#[from] PlanDataError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    StorageProfile(#[from] StorageProfileError),

    #[error(transparent)]
    StorageAcquire(Box<StorageAcquireError<StorageProfileError>>),

    #[error(transparent)]
    Postgres(#[from] PgReportError),
}

impl ConnectorError {
    /// Convert only record-local data failures that the connector detected to
    /// COPY's structured rejection path. A PostgreSQL error keeps its original
    /// report, and callers must have already separated object I/O and schema
    /// binding from row conversion.
    pub(crate) fn into_copy_row_rejection(
        self,
        column_index: Option<usize>,
        location: impl Into<String>,
    ) -> Result<CopyRowRejection, CopyError> {
        if !self.is_copy_row_data_error() {
            return Err(self.into());
        }
        let sql_error_code = self.sql_error_code();
        let mut rejection = CopyRowRejection::new(self.to_string())
            .with_sql_error_code(sql_error_code)
            .with_location(location);
        if let Some(column_index) = column_index {
            rejection = rejection.with_column_index(column_index);
        }
        Ok(rejection)
    }

    fn is_copy_row_data_error(&self) -> bool {
        match self {
            Self::ArrowConversion(error) => match error {
                ArrowConversionError::DatumConversion(error) => {
                    Self::is_datum_row_data_error(error)
                }
                ArrowConversionError::JsonValue(error) => {
                    Self::is_data_exception(error.sql_error_code())
                }
                ArrowConversionError::InvalidInput(_)
                | ArrowConversionError::ValueOutOfRange(_)
                | ArrowConversionError::DatetimeConversionError(_)
                | ArrowConversionError::NumericError(_)
                | ArrowConversionError::UuidConversionError(_)
                | ArrowConversionError::DecimalCodec(
                    DecimalCodecError::ValueOutOfRange { .. },
                ) => true,
                ArrowConversionError::UnsupportedColumnType(_)
                | ArrowConversionError::IncompatibleColumnType(_, _)
                | ArrowConversionError::ArrowTypeMismatch(_)
                | ArrowConversionError::ArrowError(_)
                | ArrowConversionError::Postgres(_)
                | ArrowConversionError::InvariantViolated(_)
                | ArrowConversionError::DecimalCodec(_) => false,
            },
            Self::DatumConversion(error) => Self::is_datum_row_data_error(error),
            Self::DecimalCodec(DecimalCodecError::ValueOutOfRange { .. })
            | Self::RowConversion { .. }
            | Self::Json { .. }
            | Self::JsonRecordTooLarge { .. }
            | Self::JsonValue { .. } => true,
            Self::JsonDatum(error) => Self::is_data_exception(error.sql_error_code()),
            _ => false,
        }
    }

    /// PostgreSQL packs the five SQLSTATE characters into six-bit fields;
    /// the first two characters therefore occupy the low twelve bits. COPY
    /// may ignore only class 22 errors produced while converting this row.
    fn is_data_exception(code: PgSqlErrorCode) -> bool {
        const CLASS_MASK: isize = (1 << 12) - 1;
        (code as isize & CLASS_MASK)
            == (PgSqlErrorCode::ERRCODE_DATA_EXCEPTION as isize & CLASS_MASK)
    }

    fn is_datum_row_data_error(error: &DatumConversionError) -> bool {
        match error {
            DatumConversionError::IncompatibleType { .. }
            | DatumConversionError::OutOfRange { .. }
            | DatumConversionError::InvalidInput { .. }
            | DatumConversionError::InvalidUtf8 { .. } => true,
            DatumConversionError::Postgres(_)
            | DatumConversionError::UnsupportedServerEncoding { .. } => false,
        }
    }

    #[inline]
    pub(crate) fn unknown_option(option: &str, reason: &'static str) -> Self {
        Self::UnknownOption {
            option: option.into(),
            reason,
        }
    }

    #[inline]
    pub(crate) fn invalid_option(option: &str, reason: &'static str) -> Self {
        Self::InvalidOption {
            option: option.into(),
            reason,
        }
    }

    #[inline]
    pub(crate) fn conflicting_option(option: &str, reason: &'static str) -> Self {
        Self::ConflictingOption {
            option: option.into(),
            reason,
        }
    }

    #[inline]
    pub(crate) fn redundant_option(option: &str) -> Self {
        Self::conflicting_option(option, "must not be specified more than once")
    }

    #[inline]
    pub(crate) const fn invalid_path(reason: &'static str) -> Self {
        Self::InvalidPath { reason }
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
    pub(crate) fn row_conversion(
        format: FormatKind,
        reason: impl Into<Box<str>>,
    ) -> Self {
        Self::RowConversion {
            format,
            reason: reason.into(),
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
    pub(crate) const fn copy_from_exact_only(format: FormatKind) -> Self {
        Self::CopyFromExactOnly { format }
    }

    #[inline]
    pub(crate) const fn copy_on_error_unsupported(format: FormatKind) -> Self {
        Self::CopyOnErrorUnsupported { format }
    }

    #[inline]
    pub(crate) fn stream_io(error: std::io::Error) -> Self {
        let sqlerrcode = Self::source_io_sql_error_code(&error)
            .unwrap_or(PgSqlErrorCode::ERRCODE_IO_ERROR);
        Self::StreamIo {
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
        error: StorageAcquireError<StorageProfileError>,
    ) -> Self {
        Self::StorageAcquire(Box::new(error))
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
    pub(crate) const fn modify_not_implemented(format: FormatKind) -> Self {
        Self::ModifyNotImplemented { format }
    }

    #[inline]
    pub(crate) const fn invalid_filter_plan(format: FormatKind) -> Self {
        Self::InvalidFilterPlan { format }
    }

    #[inline]
    pub(crate) const fn invalid_filter_datum(type_oid: pgrx::pg_sys::Oid) -> Self {
        Self::InvalidFilterDatum { type_oid }
    }
}

impl SqlStateError for ConnectorError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::UnknownOption { .. } => {
                PgSqlErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME
            }
            Self::InvalidOption { .. } | Self::InvalidPath { .. } => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            // PostgreSQL reports duplicate DefElems through
            // errorConflictingDefElem() as a syntax error.
            Self::ConflictingOption { .. } => PgSqlErrorCode::ERRCODE_SYNTAX_ERROR,
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
            | Self::JsonValue { .. }
            | Self::RowConversion { .. } => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
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
            Self::StreamIo { sqlerrcode, .. } => *sqlerrcode,
            Self::Copy(error) => error.sql_error_code(),
            Self::InvalidObjectUri { .. } => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::CopyFromExactOnly { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::CopyOnErrorUnsupported { .. } => {
                PgSqlErrorCode::ERRCODE_SYNTAX_ERROR
            }
            Self::InvalidPlanFormat { .. }
            | Self::InvalidFilterPlan { .. }
            | Self::PlanData(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::InvalidFilterDatum { .. } => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
            Self::PlanFormatChanged => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }
            Self::Storage(error) => error.sql_error_code(),
            Self::StorageProfile(error) => error.sql_error_code(),
            Self::StorageAcquire(error) => error.sql_error_code(),
            Self::ModifyNotImplemented { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::TruncateNotImplemented => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::Postgres(error) => error.sql_error_code(),
        }
    }

    fn try_into_pg_report(self) -> Result<PgReportError, Self> {
        match self {
            Self::Postgres(error) => Ok(error),
            Self::Copy(error) => error.try_into_pg_report().map_err(Self::Copy),
            Self::StorageProfile(error) => {
                error.try_into_pg_report().map_err(Self::StorageProfile)
            }
            Self::StorageAcquire(error) => (*error)
                .try_into_pg_report()
                .map_err(|error| Self::StorageAcquire(Box::new(error))),
            Self::ArrowConversion(error) => error
                .try_into_pg_report()
                .map(|report| {
                    report.contextualize(
                        "Parquet/PostgreSQL column conversion failed",
                        None,
                    )
                })
                .map_err(Self::ArrowConversion),
            Self::DatumConversion(error) => error
                .try_into_pg_report()
                .map(|report| {
                    report.contextualize("PostgreSQL datum conversion failed", None)
                })
                .map_err(Self::DatumConversion),
            error => Err(error),
        }
    }
}

impl From<ConnectorError> for ForeignScanError {
    fn from(error: ConnectorError) -> Self {
        Self::provider(error)
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
        Self::provider(error)
    }
}

impl From<ConnectorError> for CopyError {
    fn from(error: ConnectorError) -> Self {
        match error {
            ConnectorError::Copy(error) => error,
            error => Self::provider(error),
        }
    }
}

impl From<ConnectorError> for ForeignModifyError {
    fn from(error: ConnectorError) -> Self {
        Self::provider(error)
    }
}

impl From<ConnectorError> for ForeignValidationError {
    fn from(error: ConnectorError) -> Self {
        Self::provider(error)
    }
}
