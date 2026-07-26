//! SQLSTATE classification for every `ArrowConversionError` variant and the
//! `DecimalCodecError` routing. Pure enum mapping, so these run as host tests.

use pg_arrow_conv::ArrowConversionError;
use pg_lakebase_core::diag::SqlStateError;
use pg_lakebase_core::tuple::{DatumConversionError, DecimalCodecError};
use pgrx::datum::datetime_support::DateTimeConversionError;
use pgrx::datum::numeric_support::error::Error as PgNumericError;
use pgrx::prelude::PgSqlErrorCode;

// --- DATATYPE_MISMATCH group -------------------------------------------------

#[test]
fn unsupported_column_type_is_datatype_mismatch() {
    let err =
        ArrowConversionError::UnsupportedColumnType("Decimal256(76, 10)".into());
    assert_eq!(
        err.sql_error_code(),
        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
    );
}

#[test]
fn incompatible_column_type_is_datatype_mismatch() {
    let err = ArrowConversionError::IncompatibleColumnType(
        "FixedSizeBinary(16)".into(),
        "expected uuid".into(),
    );
    assert_eq!(
        err.sql_error_code(),
        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
    );
}

#[test]
fn arrow_type_mismatch_is_datatype_mismatch() {
    let err = ArrowConversionError::ArrowTypeMismatch("Int32Array".into());
    assert_eq!(
        err.sql_error_code(),
        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
    );
}

// --- value/error classification ---------------------------------------------

#[test]
fn invalid_input_is_invalid_text_representation() {
    let err = ArrowConversionError::InvalidInput("invalid JSON text".into());
    assert_eq!(
        err.sql_error_code(),
        PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION
    );
}

#[test]
fn value_out_of_range_is_data_exception() {
    let err = ArrowConversionError::ValueOutOfRange("value out of range".into());
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

#[test]
fn numeric_error_is_data_exception() {
    let err = ArrowConversionError::NumericError(PgNumericError::Invalid(
        "not a numeric".into(),
    ));
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

#[test]
fn datetime_conversion_error_is_data_exception() {
    let err = ArrowConversionError::DatetimeConversionError(
        DateTimeConversionError::FieldOverflow,
    );
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

#[test]
fn uuid_conversion_error_is_data_exception() {
    let uuid_err = uuid::Uuid::parse_str("not-a-uuid").unwrap_err();
    let err = ArrowConversionError::UuidConversionError(uuid_err);
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

// --- INTERNAL_ERROR group ----------------------------------------------------

#[test]
fn arrow_error_is_internal_error() {
    let err = ArrowConversionError::ArrowError(
        arrow_schema::ArrowError::SchemaError("bad schema".into()),
    );
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
}

#[test]
fn invariant_violated_is_internal_error() {
    let err = ArrowConversionError::InvariantViolated("invariant violated");
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
}

#[test]
fn decimal_codec_invalid_binary_is_internal_error() {
    let err = ArrowConversionError::DecimalCodec(
        DecimalCodecError::InvalidBinaryRepresentation {
            message: "malformed numeric bytes".into(),
        },
    );
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
}

// --- exhaustiveness ----------------------------------------------------------
// If a new ArrowConversionError variant is added, this match stops compiling, forcing the
// author to classify it (and add a test above).

#[test]
fn every_conv_error_variant_has_a_classification() {
    fn classify(err: &ArrowConversionError) -> PgSqlErrorCode {
        match err {
            ArrowConversionError::UnsupportedColumnType(_)
            | ArrowConversionError::IncompatibleColumnType(_, _)
            | ArrowConversionError::ArrowTypeMismatch(_) => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }
            ArrowConversionError::DatumConversion(source) => source.sql_error_code(),
            ArrowConversionError::InvalidInput(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION
            }
            ArrowConversionError::ValueOutOfRange(_) => {
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
            }
            ArrowConversionError::NumericError(_)
            | ArrowConversionError::DatetimeConversionError(_)
            | ArrowConversionError::UuidConversionError(_) => {
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
            }
            ArrowConversionError::Postgres(error) => error.sql_error_code(),
            ArrowConversionError::ArrowError(_)
            | ArrowConversionError::InvariantViolated(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
            ArrowConversionError::DecimalCodec(error) => match error {
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
        }
    }

    let datatype = ArrowConversionError::UnsupportedColumnType("x".into());
    let structured = ArrowConversionError::from(DatumConversionError::OutOfRange {
        target: pgrx::pg_sys::INT2OID,
    });
    let data = ArrowConversionError::ValueOutOfRange("x".into());
    let internal = ArrowConversionError::InvariantViolated("x");
    assert_eq!(classify(&datatype), datatype.sql_error_code());
    assert_eq!(classify(&structured), structured.sql_error_code());
    assert_eq!(classify(&data), data.sql_error_code());
    assert_eq!(classify(&internal), internal.sql_error_code());
}

// --- DecimalCodecError routing via From --------------------------------------

#[test]
fn precision_out_of_range_routes_to_datatype_mismatch() {
    let codec_err = DecimalCodecError::PrecisionOutOfRange { precision: 40 };
    let conv: ArrowConversionError = codec_err.into();
    assert!(matches!(
        conv,
        ArrowConversionError::IncompatibleColumnType(_, _)
    ));
    assert_eq!(
        conv.sql_error_code(),
        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
    );
}

#[test]
fn scale_out_of_range_routes_to_datatype_mismatch() {
    let codec_err = DecimalCodecError::ScaleOutOfRange {
        precision: 10,
        scale: 20,
    };
    let conv: ArrowConversionError = codec_err.into();
    assert!(matches!(
        conv,
        ArrowConversionError::IncompatibleColumnType(_, _)
    ));
    assert_eq!(
        conv.sql_error_code(),
        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
    );
}

#[test]
fn value_out_of_range_routes_to_data_exception() {
    let codec_err = DecimalCodecError::ValueOutOfRange {
        precision: 10,
        scale: 2,
        message: "value exceeds NUMERIC(10, 2)".into(),
    };
    let conv: ArrowConversionError = codec_err.into();
    assert!(matches!(conv, ArrowConversionError::DecimalCodec(_)));
    assert_eq!(
        conv.sql_error_code(),
        PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
    );
}

#[test]
fn invalid_binary_representation_routes_to_internal_error() {
    let codec_err = DecimalCodecError::InvalidBinaryRepresentation {
        message: "numeric_recv rejected wire bytes".into(),
    };
    let conv: ArrowConversionError = codec_err.into();
    assert!(matches!(conv, ArrowConversionError::DecimalCodec(_)));
    assert_eq!(
        conv.sql_error_code(),
        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
    );
}
