//! `validate_supported` parity with `resolve_column_rule`: it accepts a schema
//! iff every column resolves, and otherwise returns the lowest-positioned
//! failing column's error variant. A zero-column schema is accepted. Host
//! tests; the harness only builds schemas from `DataType`s, never arrays, so it
//! also confirms the validator consults types only. `ConvError` is not
//! `PartialEq`, so error identity is compared via a discriminant tag.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use pg_arrow_conv::{
    ConvError, PgColumnType, resolve_column_rule, validate_supported,
};
use pgrx::pg_sys;
use proptest::prelude::*;

/// Discriminant tag for `ConvError`, used to compare error *variants* without a
/// `PartialEq` impl on `ConvError` itself.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ErrTag {
    Unsupported,
    Incompatible,
    ArrowTypeMismatch,
    ArrowError,
    DatumConversion,
    InvalidUtf8,
    InvariantViolated,
    DecimalCodecBug,
    Datetime,
    Numeric,
    Uuid,
}

fn tag(e: &ConvError) -> ErrTag {
    match e {
        ConvError::UnsupportedColumnType(_) => ErrTag::Unsupported,
        ConvError::IncompatibleColumnType(_, _) => ErrTag::Incompatible,
        ConvError::ArrowTypeMismatch(_) => ErrTag::ArrowTypeMismatch,
        ConvError::ArrowError(_) => ErrTag::ArrowError,
        ConvError::DatumConversionError(_) => ErrTag::DatumConversion,
        ConvError::InvalidUtf8(_) => ErrTag::InvalidUtf8,
        ConvError::InvariantViolated(_) => ErrTag::InvariantViolated,
        ConvError::DecimalCodecBug(_) => ErrTag::DecimalCodecBug,
        ConvError::DatetimeConversionError(_) => ErrTag::Datetime,
        ConvError::NumericError(_) => ErrTag::Numeric,
        ConvError::UuidConversionError(_) => ErrTag::Uuid,
    }
}

/// Build an `arrow_schema::Schema` from an aligned list of `(DataType, _)`
/// columns. Column `i` becomes a nullable field named `c{i}`; the PG type is
/// carried separately to keep the two views position-aligned, exactly as
/// `validate_supported` consumes them.
fn schema_from(columns: &[(DataType, PgColumnType)]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(i, (dt, _))| Field::new(format!("c{i}"), dt.clone(), true))
        .collect();
    Schema::new(fields)
}

fn pg_types_from(columns: &[(DataType, PgColumnType)]) -> Vec<PgColumnType> {
    columns.iter().map(|(_, pg)| *pg).collect()
}

/// Oracle: the lowest-positioned column whose `(DataType, PgColumnType)` pair
/// fails to resolve, paired with the tag of the `ConvError` it produces. `None`
/// when every column resolves.
fn first_failing(columns: &[(DataType, PgColumnType)]) -> Option<(usize, ErrTag)> {
    columns.iter().enumerate().find_map(|(i, (dt, pg))| {
        resolve_column_rule(dt, *pg).err().map(|e| (i, tag(&e)))
    })
}

// ----------------------------------------------------------------------------
// proptest strategies
// ----------------------------------------------------------------------------

/// A list element `DataType`, mixing the supported element kinds with a few
/// unsupported ones so generated `List` columns land on both sides of the
/// `resolve_list_element_rule` boundary.
fn arb_list_element() -> impl Strategy<Value = DataType> {
    prop_oneof![
        // supported element kinds
        Just(DataType::Boolean),
        Just(DataType::Int32),
        Just(DataType::Int64),
        Just(DataType::Float32),
        Just(DataType::Float64),
        Just(DataType::Utf8),
        Just(DataType::LargeUtf8),
        // unsupported element kinds (rejected by resolve_list_element_rule)
        Just(DataType::Int16),
        Just(DataType::UInt32),
        Just(DataType::Binary),
    ]
}

/// An Arrow `DataType` spanning both the supported dispatch table and a sampling
/// of unsupported types, so generated columns exercise accept and reject paths.
fn arb_data_type() -> impl Strategy<Value = DataType> {
    prop_oneof![
        // --- supported (resolve Ok for a compatible PG type) ---
        Just(DataType::Boolean),
        Just(DataType::Int32),
        Just(DataType::Int64),
        Just(DataType::Float32),
        Just(DataType::Float64),
        Just(DataType::Utf8),
        Just(DataType::LargeUtf8),
        Just(DataType::Binary),
        Just(DataType::LargeBinary),
        Just(DataType::Date32),
        Just(DataType::Time64(TimeUnit::Microsecond)),
        // timestamps across {micros, nanos} x {naive, tz}
        (any::<bool>(), any::<bool>()).prop_map(|(nanos, tz)| {
            let unit = if nanos {
                TimeUnit::Nanosecond
            } else {
                TimeUnit::Microsecond
            };
            let tz = if tz { Some("+00:00".into()) } else { None };
            DataType::Timestamp(unit, tz)
        }),
        // decimal: precision/scale pass straight through
        (1u8..=38u8, 0i8..=10i8).prop_map(|(p, s)| DataType::Decimal128(p, s)),
        // fixed-size binary: width 16 (uuid/bytea ambiguous) plus other widths
        prop_oneof![Just(16i32), 1i32..=64i32].prop_map(DataType::FixedSizeBinary),
        // single-level lists with supported and unsupported elements
        arb_list_element()
            .prop_map(|et| DataType::List(Arc::new(Field::new("item", et, true)))),
        // --- unsupported (resolve_column_rule always rejects) ---
        Just(DataType::Null),
        Just(DataType::Int16),
        Just(DataType::Int8),
        Just(DataType::UInt32),
        Just(DataType::Float16),
        Just(DataType::Time32(TimeUnit::Second)),
        Just(DataType::Utf8View),
    ]
}

/// Every `PgColumnType`. Sampling all of them means `FixedSizeBinary(16)` often
/// pairs with neither uuid nor bytea (-> IncompatibleColumnType) and other
/// widths often pair with a non-bytea type (-> UnsupportedColumnType).
fn arb_pg_type() -> impl Strategy<Value = PgColumnType> {
    prop_oneof![
        Just(PgColumnType::Bool),
        Just(PgColumnType::Int2),
        Just(PgColumnType::Int4),
        Just(PgColumnType::Int8),
        Just(PgColumnType::Float4),
        Just(PgColumnType::Float8),
        Just(PgColumnType::Text),
        Just(PgColumnType::Bytea),
        Just(PgColumnType::Uuid),
        Just(PgColumnType::Numeric),
        Just(PgColumnType::Date),
        Just(PgColumnType::Time),
        Just(PgColumnType::Timestamp),
        Just(PgColumnType::Timestamptz),
        Just(PgColumnType::Array(pg_sys::INT4OID)),
    ]
}

fn arb_column() -> impl Strategy<Value = (DataType, PgColumnType)> {
    (arb_data_type(), arb_pg_type())
}

/// A whole schema's worth of columns: 0..=8 columns (the lower bound covers the
/// zero-column case).
fn arb_columns() -> impl Strategy<Value = Vec<(DataType, PgColumnType)>> {
    prop::collection::vec(arb_column(), 0..=8)
}

// ----------------------------------------------------------------------------
// properties
// ----------------------------------------------------------------------------

proptest! {
    /// `validate_supported` returns `Ok(())` iff
    /// `resolve_column_rule` resolves every column, and when it
    /// rejects, it returns the same `ConvError` variant as the lowest-positioned
    /// failing column. The harness only ever builds a `Schema` from `DataType`s
    /// and never an array (types only, never values).
    #[test]
    fn prop_validator_parity(columns in arb_columns()) {
        let schema = schema_from(&columns);
        let pg_types = pg_types_from(&columns);

        let result = validate_supported(&schema, &pg_types);
        let oracle = first_failing(&columns);

        match (result, oracle) {
            // accept-iff: validator OK exactly when every column resolves
            (Ok(()), None) => {}
            (Ok(()), Some((idx, t))) => prop_assert!(
                false,
                "validate_supported returned Ok but column {idx} fails to resolve with {t:?}"
            ),
            (Err(e), None) => prop_assert!(
                false,
                "validate_supported rejected with {:?} but every column resolves",
                tag(&e)
            ),
            // reject parity: same variant as the lowest-positioned failing column
            (Err(e), Some((_idx, expected_tag))) => {
                prop_assert_eq!(
                    tag(&e),
                    expected_tag,
                    "validator error variant must match lowest-positioned failing column"
                );
            }
        }
    }

    /// single-column slice: a one-column schema
    /// is accepted iff that column's pair resolves, and rejects with the
    /// matching variant otherwise.
    #[test]
    fn prop_single_column_matches_resolve(col in arb_column()) {
        let columns = vec![col.clone()];
        let schema = schema_from(&columns);
        let pg_types = pg_types_from(&columns);

        let result = validate_supported(&schema, &pg_types);
        let resolved = resolve_column_rule(&col.0, col.1);

        match (result, resolved) {
            (Ok(()), Ok(_)) => {}
            (Err(e), Err(re)) => prop_assert_eq!(tag(&e), tag(&re)),
            (Ok(()), Err(re)) => {
                prop_assert!(false, "validator accepted but resolve rejected with {:?}", tag(&re))
            }
            (Err(e), Ok(_)) => {
                prop_assert!(false, "validator rejected with {:?} but resolve accepted", tag(&e))
            }
        }
    }
}

// ----------------------------------------------------------------------------
// example-based unit tests
// ----------------------------------------------------------------------------

/// a zero-column schema is accepted.
#[test]
fn zero_column_schema_is_ok() {
    let schema = Schema::new(Vec::<Field>::new());
    assert!(validate_supported(&schema, &[]).is_ok());
}

/// an all-supported schema resolves and is accepted.
#[test]
fn all_supported_schema_is_ok() {
    let columns = vec![
        (DataType::Boolean, PgColumnType::Bool),
        (DataType::Int32, PgColumnType::Int4),
        (DataType::Int64, PgColumnType::Int8),
        (DataType::Utf8, PgColumnType::Text),
        (DataType::FixedSizeBinary(16), PgColumnType::Uuid),
        (DataType::FixedSizeBinary(32), PgColumnType::Bytea),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            PgColumnType::Timestamptz,
        ),
        (DataType::Decimal128(10, 2), PgColumnType::Numeric),
    ];
    let schema = schema_from(&columns);
    let pg_types = pg_types_from(&columns);
    assert!(validate_supported(&schema, &pg_types).is_ok());
}

/// with a single failing column, the validator surfaces that column's
/// error variant.
#[test]
fn single_failing_column_surfaces_its_variant() {
    let columns = vec![
        (DataType::Int32, PgColumnType::Int4),
        (DataType::Null, PgColumnType::Text), // unsupported DataType
    ];
    let schema = schema_from(&columns);
    let pg_types = pg_types_from(&columns);
    let err = validate_supported(&schema, &pg_types)
        .expect_err("an unsupported column must reject");
    assert_eq!(tag(&err), ErrTag::Unsupported);
}

/// when multiple columns fail, the validator returns the
/// **lowest-positioned** failing column's variant. Here column 1 is
/// `Unsupported` (Null) and column 3 is `Incompatible` (FixedSizeBinary(16) with
/// a non-uuid/bytea PG type); the validator must report `Unsupported`.
#[test]
fn multiple_failures_report_lowest_positioned_variant() {
    let columns = vec![
        (DataType::Int64, PgColumnType::Int8), // ok
        (DataType::Null, PgColumnType::Text),  // 1: Unsupported
        (DataType::Utf8, PgColumnType::Text),  // ok
        (DataType::FixedSizeBinary(16), PgColumnType::Text), // 3: Incompatible
    ];
    let schema = schema_from(&columns);
    let pg_types = pg_types_from(&columns);
    let err = validate_supported(&schema, &pg_types)
        .expect_err("schema with failing columns must reject");

    // Lowest-positioned failure is column 1 (Unsupported), not column 3.
    assert_eq!(tag(&err), ErrTag::Unsupported);
    // And it matches what resolve_column_rule says for that column.
    let col1_err = resolve_column_rule(&columns[1].0, columns[1].1)
        .expect_err("column 1 must fail to resolve");
    assert_eq!(tag(&err), tag(&col1_err));
}

/// a `FixedSizeBinary(16)` paired with neither uuid nor bytea yields
/// the `Incompatible` variant (distinct from `Unsupported`), confirming the
/// validator forwards the exact `resolve_column_rule` variant.
#[test]
fn incompatible_fixed16_variant_is_forwarded() {
    let columns = vec![(DataType::FixedSizeBinary(16), PgColumnType::Numeric)];
    let schema = schema_from(&columns);
    let pg_types = pg_types_from(&columns);
    let err = validate_supported(&schema, &pg_types)
        .expect_err("FixedSizeBinary(16) with non-uuid/bytea must reject");
    assert_eq!(tag(&err), ErrTag::Incompatible);
}
