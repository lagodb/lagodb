//! Resolution-table assertions for `resolve_column_rule` over the
//! `(DataType, PgColumnType)` -> `ColumnRule` mapping. Host tests; no backend.
//! `ColumnRule` is not `PartialEq`, so assertions use `matches!` /
//! destructuring. UUID-specific cases live in `resolve_uuid.rs`.

use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};
use pg_arrow_conv::{
    ArrowConversionError, ColumnRule, ListElementRule, PgColumnType,
    resolve_column_rule,
};
use pgrx::pg_sys;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Supported primitive mappings
// ---------------------------------------------------------------------------

/// Each primitive Arrow `DataType` resolves to its matching `ColumnRule` when
/// paired with a compatible `PgColumnType`. `resolve_column_rule` validates the
/// pair for every type (not only `FixedSizeBinary(16)`), so the pairs here are
/// deliberately the compatible ones.
#[test]
fn primitives_resolve_to_matching_rule() {
    assert!(matches!(
        resolve_column_rule(&DataType::Boolean, PgColumnType::Bool).unwrap(),
        ColumnRule::Bool
    ));
    assert!(matches!(
        resolve_column_rule(&DataType::Int32, PgColumnType::Int4).unwrap(),
        ColumnRule::I32
    ));
    assert!(matches!(
        resolve_column_rule(&DataType::Int64, PgColumnType::Int8).unwrap(),
        ColumnRule::I64
    ));
    assert!(matches!(
        resolve_column_rule(&DataType::Float32, PgColumnType::Float4).unwrap(),
        ColumnRule::F32
    ));
    assert!(matches!(
        resolve_column_rule(&DataType::Float64, PgColumnType::Float8).unwrap(),
        ColumnRule::F64
    ));
}

/// `Utf8` and `LargeUtf8` both collapse to `ColumnRule::Utf8`.
#[test]
fn utf8_variants_resolve_to_utf8() {
    assert!(matches!(
        resolve_column_rule(&DataType::Utf8, PgColumnType::Text).unwrap(),
        ColumnRule::Utf8
    ));
    assert!(matches!(
        resolve_column_rule(&DataType::LargeUtf8, PgColumnType::Text).unwrap(),
        ColumnRule::Utf8
    ));
}

/// `Binary` and `LargeBinary` both collapse to `ColumnRule::Binary`.
#[test]
fn binary_variants_resolve_to_binary() {
    assert!(matches!(
        resolve_column_rule(&DataType::Binary, PgColumnType::Bytea).unwrap(),
        ColumnRule::Binary
    ));
    assert!(matches!(
        resolve_column_rule(&DataType::LargeBinary, PgColumnType::Bytea).unwrap(),
        ColumnRule::Binary
    ));
}

/// The format-neutral resolver does not select an Iceberg-private JSONB
/// representation from the target OID. The provider must bind that rule at its
/// own planning boundary.
#[test]
fn binary_variants_do_not_infer_jsonb_codec() {
    assert!(matches!(
        resolve_column_rule(&DataType::Binary, PgColumnType::Jsonb),
        Err(ArrowConversionError::IncompatibleColumnType(_, _))
    ));
    assert!(matches!(
        resolve_column_rule(&DataType::LargeBinary, PgColumnType::Jsonb),
        Err(ArrowConversionError::IncompatibleColumnType(_, _))
    ));
}

// ---------------------------------------------------------------------------
// Date / time mappings
// ---------------------------------------------------------------------------

/// `Date32` resolves to `ColumnRule::Date32`.
#[test]
fn date32_resolves_to_date32() {
    assert!(matches!(
        resolve_column_rule(&DataType::Date32, PgColumnType::Date).unwrap(),
        ColumnRule::Date32
    ));
}

/// `Time64(Microsecond)` resolves to `ColumnRule::Time64Micros`.
#[test]
fn time64_micros_resolves_to_time64micros() {
    assert!(matches!(
        resolve_column_rule(
            &DataType::Time64(TimeUnit::Microsecond),
            PgColumnType::Time
        )
        .unwrap(),
        ColumnRule::Time64Micros
    ));
}

// ---------------------------------------------------------------------------
// Timestamp unit/tz fidelity
// ---------------------------------------------------------------------------

/// All four combos of `{Microsecond, Nanosecond} x {None, Some(tz)}` resolve to
/// `ColumnRule::Timestamp { nanos: unit == Nanosecond, tz: tz.is_some() }`.
#[test]
fn timestamp_combos_resolve_with_expected_flags() {
    let tz: Arc<str> = Arc::from("+00:00");

    // Microsecond, naive
    assert!(matches!(
        resolve_column_rule(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            PgColumnType::Timestamp,
        )
        .unwrap(),
        ColumnRule::Timestamp {
            nanos: false,
            tz: false
        }
    ));

    // Microsecond, tz-aware
    assert!(matches!(
        resolve_column_rule(
            &DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone())),
            PgColumnType::Timestamptz,
        )
        .unwrap(),
        ColumnRule::Timestamp {
            nanos: false,
            tz: true
        }
    ));

    // Nanosecond, naive
    assert!(matches!(
        resolve_column_rule(
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            PgColumnType::Timestamp,
        )
        .unwrap(),
        ColumnRule::Timestamp {
            nanos: true,
            tz: false
        }
    ));

    // Nanosecond, tz-aware
    assert!(matches!(
        resolve_column_rule(
            &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)),
            PgColumnType::Timestamptz,
        )
        .unwrap(),
        ColumnRule::Timestamp {
            nanos: true,
            tz: true
        }
    ));
}

// ---------------------------------------------------------------------------
// Decimal pass-through
// ---------------------------------------------------------------------------

/// `Decimal128(p, s)` resolves to `ColumnRule::Decimal128 { precision: p,
/// scale: s }` with precision and scale passed straight through.
#[test]
fn decimal128_passes_precision_and_scale_through() {
    for (p, s) in [(10u8, 2i8), (38, 0), (18, 4), (1, 0), (38, 38)] {
        let rule =
            resolve_column_rule(&DataType::Decimal128(p, s), PgColumnType::Numeric)
                .unwrap();
        match rule {
            ColumnRule::Decimal128 { precision, scale } => {
                assert_eq!(
                    precision, p as u32,
                    "precision must pass through for ({p},{s})"
                );
                assert_eq!(scale, s as u32, "scale must pass through for ({p},{s})");
            }
            other => panic!("expected ColumnRule::Decimal128, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// FixedSizeBinary(16) UUID/bytea boundary
// (UUID-positive cases live in resolve_uuid.rs; here we assert the wrong-PG-type
//  incompatibility for completeness of the table.)
// ---------------------------------------------------------------------------

/// `FixedSizeBinary(16)` paired with a PG type that is neither `uuid` nor
/// `bytea` yields `IncompatibleColumnType` (not `Unsupported`).
#[test]
fn fixed16_wrong_pg_type_is_incompatible() {
    for pg in [
        PgColumnType::Int4,
        PgColumnType::Text,
        PgColumnType::Numeric,
    ] {
        let err = resolve_column_rule(&DataType::FixedSizeBinary(16), pg)
            .expect_err("FixedSizeBinary(16) with wrong PG type must be rejected");
        assert!(
            matches!(err, ArrowConversionError::IncompatibleColumnType(_, _)),
            "expected IncompatibleColumnType for pg={pg:?}, got {err:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// List element resolution
// ---------------------------------------------------------------------------

fn list_of(elem: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", elem, true)))
}

/// Each supported list element `DataType` resolves through to the matching
/// `ListElementRule` inside `ColumnRule::List` when paired with a compatible
/// target element OID.
#[test]
fn list_elements_resolve_to_matching_element_rule() {
    macro_rules! assert_list_elem {
        ($elem:expr, $oid:expr, $pat:pat) => {{
            let rule =
                resolve_column_rule(&list_of($elem), PgColumnType::Array($oid))
                    .unwrap();
            match rule {
                ColumnRule::List { element, .. } => {
                    assert!(
                        matches!(element, $pat),
                        "unexpected list element rule: {element:?}"
                    );
                }
                other => panic!("expected ColumnRule::List, got {other:?}"),
            }
        }};
    }

    assert_list_elem!(DataType::Boolean, pg_sys::BOOLOID, ListElementRule::Bool);
    assert_list_elem!(DataType::Int32, pg_sys::INT4OID, ListElementRule::Int);
    assert_list_elem!(DataType::Int64, pg_sys::INT8OID, ListElementRule::Long);
    assert_list_elem!(DataType::Float32, pg_sys::FLOAT4OID, ListElementRule::Float);
    assert_list_elem!(
        DataType::Float64,
        pg_sys::FLOAT8OID,
        ListElementRule::Double
    );
    assert_list_elem!(DataType::Utf8, pg_sys::TEXTOID, ListElementRule::String);
    assert_list_elem!(
        DataType::LargeUtf8,
        pg_sys::TEXTOID,
        ListElementRule::String
    );
}

// ---------------------------------------------------------------------------
// Unsupported DataTypes
// ---------------------------------------------------------------------------

/// Recognized-but-unmappable Arrow `DataType`s resolve to
/// `ArrowConversionError::UnsupportedColumnType`.
#[test]
fn unsupported_data_types_are_rejected() {
    // Decimal256 is not handled by the layer.
    let err =
        resolve_column_rule(&DataType::Decimal256(20, 4), PgColumnType::Numeric)
            .expect_err("Decimal256 must be unsupported");
    assert!(
        matches!(err, ArrowConversionError::UnsupportedColumnType(_)),
        "Decimal256: {err:?}"
    );

    // Float16 is not a supported width.
    let err = resolve_column_rule(&DataType::Float16, PgColumnType::Float4)
        .expect_err("Float16 must be unsupported");
    assert!(
        matches!(err, ArrowConversionError::UnsupportedColumnType(_)),
        "Float16: {err:?}"
    );

    // Nested list (list of list) — the element rule rejects a list element.
    let nested = list_of(list_of(DataType::Int32));
    let err = resolve_column_rule(&nested, PgColumnType::Array(pg_sys::INT4OID))
        .expect_err("nested List must be unsupported");
    assert!(
        matches!(err, ArrowConversionError::UnsupportedColumnType(_)),
        "nested List: {err:?}"
    );

    // Struct is not a materializable column type here.
    let strukt =
        DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());
    let err = resolve_column_rule(&strukt, PgColumnType::Array(pg_sys::INT4OID))
        .expect_err("Struct must be unsupported");
    assert!(
        matches!(err, ArrowConversionError::UnsupportedColumnType(_)),
        "Struct: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Property tests: decimal & timestamp resolution fidelity
// ---------------------------------------------------------------------------

proptest! {
    /// for every valid Decimal128 precision/scale (`0 <= scale <= precision`),
    /// `resolve_column_rule` passes both through unchanged.
    #[test]
    fn prop_decimal128_resolution_passthrough(
        (p, s) in (1u8..=38).prop_flat_map(|p| (Just(p), 0i8..=p as i8)),
    ) {
        let rule =
            resolve_column_rule(&DataType::Decimal128(p, s), PgColumnType::Numeric)
                .expect("valid Decimal128 must resolve");
        match rule {
            ColumnRule::Decimal128 { precision, scale } => {
                prop_assert_eq!(precision, p as u32);
                prop_assert_eq!(scale, s as u32);
            }
            other => prop_assert!(false, "expected Decimal128, got {:?}", other),
        }
    }

    /// across the unit/tz matrix the
    /// resolved `Timestamp` rule carries `nanos == (unit == Nanosecond)` and
    /// `tz == tz.is_some()`.
    #[test]
    fn prop_timestamp_resolution_flags(
        nanos_unit in any::<bool>(),
        has_tz in any::<bool>(),
    ) {
        let unit = if nanos_unit {
            TimeUnit::Nanosecond
        } else {
            TimeUnit::Microsecond
        };
        let tz: Option<Arc<str>> = if has_tz { Some(Arc::from("+00:00")) } else { None };
        let pg = if has_tz { PgColumnType::Timestamptz } else { PgColumnType::Timestamp };

        let rule = resolve_column_rule(&DataType::Timestamp(unit, tz), pg)
            .expect("Timestamp must resolve");
        match rule {
            ColumnRule::Timestamp { nanos, tz } => {
                prop_assert_eq!(nanos, nanos_unit);
                prop_assert_eq!(tz, has_tz);
            }
            other => prop_assert!(false, "expected Timestamp, got {:?}", other),
        }
    }
}
