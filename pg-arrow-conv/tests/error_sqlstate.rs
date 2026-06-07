//! SQLSTATE classification for every `ConvError` variant and the
//! `DecimalCodecError` routing. Pure enum mapping, so these run as host tests.

use pg_arrow_conv::ConvError;
use pg_lakebase_core::diag::SqlStateError;
use pg_lakebase_core::tuple::DecimalCodecError;
use pgrx::datum::datetime_support::DateTimeConversionError;
use pgrx::datum::numeric_support::error::Error as PgNumericError;
use pgrx::prelude::PgSqlErrorCode;

// --- DATATYPE_MISMATCH group -------------------------------------------------

#[test]
fn unsupported_column_type_is_datatype_mismatch() {
    let err = ConvError::UnsupportedColumnType("Decimal256(76, 10)".into());
    assert_eq!(
        err.sql_error_code(),
        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
    );
}

#[test]
fn incompatible_column_type_is_datatype_mismatch() {
    let err = ConvError::IncompatibleColumnType(
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
    let err = ConvError::ArrowTypeMismatch("Int32Array".into());
    assert_eq!(
        err.sql_error_code(),
        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
    );
}

// --- DATA_EXCEPTION group ----------------------------------------------------

#[test]
fn datum_conversion_error_is_data_exception() {
    let err = ConvError::DatumConversionError("value out of range".into());
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

#[test]
fn numeric_error_is_data_exception() {
    let err =
        ConvError::NumericError(PgNumericError::Invalid("not a numeric".into()));
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

#[test]
fn datetime_conversion_error_is_data_exception() {
    let err =
        ConvError::DatetimeConversionError(DateTimeConversionError::FieldOverflow);
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

#[test]
fn uuid_conversion_error_is_data_exception() {
    let uuid_err = uuid::Uuid::parse_str("not-a-uuid").unwrap_err();
    let err = ConvError::UuidConversionError(uuid_err);
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

#[test]
fn invalid_utf8_is_data_exception() {
    let bytes: &[u8] = &[0xff, 0xfe];
    // Intentionally invalid UTF-8: we need a real `Utf8Error` to exercise the
    // error-code mapping. The literal is always invalid, which is the point.
    #[allow(invalid_from_utf8)]
    let utf8_err = std::str::from_utf8(bytes).unwrap_err();
    let err = ConvError::InvalidUtf8(utf8_err);
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
}

// --- INTERNAL_ERROR group ----------------------------------------------------

#[test]
fn arrow_error_is_internal_error() {
    let err = ConvError::ArrowError(arrow_schema::ArrowError::SchemaError(
        "bad schema".into(),
    ));
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
}

#[test]
fn invariant_violated_is_internal_error() {
    let err = ConvError::InvariantViolated("invariant violated");
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
}

#[test]
fn decimal_codec_bug_is_internal_error() {
    let err = ConvError::DecimalCodecBug("malformed numeric bytes".into());
    assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
}

// --- exhaustiveness ----------------------------------------------------------
// If a new ConvError variant is added, this match stops compiling, forcing the
// author to classify it (and add a test above).

#[test]
fn every_conv_error_variant_has_a_classification() {
    fn classify(err: &ConvError) -> PgSqlErrorCode {
        match err {
            ConvError::UnsupportedColumnType(_)
            | ConvError::IncompatibleColumnType(_, _)
            | ConvError::ArrowTypeMismatch(_) => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }
            ConvError::DatumConversionError(_)
            | ConvError::InvalidUtf8(_)
            | ConvError::NumericError(_)
            | ConvError::DatetimeConversionError(_)
            | ConvError::UuidConversionError(_) => {
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
            }
            ConvError::ArrowError(_)
            | ConvError::InvariantViolated(_)
            | ConvError::DecimalCodecBug(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }

    let datatype = ConvError::UnsupportedColumnType("x".into());
    let data = ConvError::DatumConversionError("x".into());
    let internal = ConvError::InvariantViolated("x");
    assert_eq!(classify(&datatype), datatype.sql_error_code());
    assert_eq!(classify(&data), data.sql_error_code());
    assert_eq!(classify(&internal), internal.sql_error_code());
}

// --- DecimalCodecError routing via From --------------------------------------

#[test]
fn precision_out_of_range_routes_to_datatype_mismatch() {
    let codec_err = DecimalCodecError::PrecisionOutOfRange { precision: 40 };
    let conv: ConvError = codec_err.into();
    assert!(matches!(conv, ConvError::IncompatibleColumnType(_, _)));
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
    let conv: ConvError = codec_err.into();
    assert!(matches!(conv, ConvError::IncompatibleColumnType(_, _)));
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
    let conv: ConvError = codec_err.into();
    assert!(matches!(conv, ConvError::DatumConversionError(_)));
    assert_eq!(
        conv.sql_error_code(),
        PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
    );
}

#[test]
fn invalid_binary_representation_routes_to_internal_error() {
    let codec_err = DecimalCodecError::InvalidBinaryRepresentation {
        message: "numeric_recv rejected wire bytes".into(),
    };
    let conv: ConvError = codec_err.into();
    assert!(matches!(conv, ConvError::DecimalCodecBug(_)));
    assert_eq!(
        conv.sql_error_code(),
        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
    );
}
