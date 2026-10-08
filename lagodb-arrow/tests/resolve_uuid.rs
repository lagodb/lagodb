//! UUID vs bytea disambiguation in `resolve_cell_rule`. Pure resolution-table
//! assertions; no PostgreSQL backend required. `CellRule` is not `PartialEq`,
//! so assertions use `matches!`.

use arrow_schema::DataType;
use lagodb_arrow::{ArrowConversionError, CellRule, PgColumnType, resolve_cell_rule};
use proptest::prelude::*;

/// `FixedSizeBinary(16)` + `uuid` resolves to [`CellRule::Uuid`].
#[test]
fn fixed16_uuid_resolves_to_uuid() {
    let rule = resolve_cell_rule(&DataType::FixedSizeBinary(16), PgColumnType::Uuid)
        .expect("FixedSizeBinary(16) + Uuid must resolve");
    assert!(
        matches!(rule, CellRule::Uuid),
        "expected CellRule::Uuid, got {rule:?}"
    );
}

/// `FixedSizeBinary(16)` paired with `bytea` resolves to a 16-wide
/// [`CellRule::FixedBinary`], *not* to `Uuid`.
#[test]
fn fixed16_bytea_resolves_to_fixed_binary() {
    let rule = resolve_cell_rule(&DataType::FixedSizeBinary(16), PgColumnType::Bytea)
        .expect("FixedSizeBinary(16) + Bytea must resolve");
    assert!(
        matches!(rule, CellRule::FixedBinary { len: 16 }),
        "expected CellRule::FixedBinary {{ len: 16 }}, got {rule:?}"
    );
}

/// `FixedSizeBinary(16)` paired with neither `uuid` nor `bytea`
/// is rejected with [`ArrowConversionError::IncompatibleColumnType`].
#[test]
fn fixed16_other_pg_type_is_incompatible() {
    let err = resolve_cell_rule(&DataType::FixedSizeBinary(16), PgColumnType::Text)
        .expect_err("FixedSizeBinary(16) + Text must be rejected");
    assert!(
        matches!(err, ArrowConversionError::IncompatibleColumnType(_, _)),
        "expected ArrowConversionError::IncompatibleColumnType, got {err:?}"
    );
}

proptest! {
    /// across all 16-byte widths (the width is fixed at 16
    /// for both uuid and fixed-bytea here), `uuid` resolves to `Uuid` and
    /// `bytea` resolves to `FixedBinary{len:16}`, and the two never alias.
    ///
    /// We sample the target PG type to confirm only `Uuid` yields `Uuid` and
    /// only `Bytea` yields `FixedBinary` at width 16.
    #[test]
    fn prop_fixed16_disambiguation(_seed in any::<u8>()) {
        let uuid_rule =
            resolve_cell_rule(&DataType::FixedSizeBinary(16), PgColumnType::Uuid)
                .expect("uuid must resolve");
        let bytea_rule =
            resolve_cell_rule(&DataType::FixedSizeBinary(16), PgColumnType::Bytea)
                .expect("bytea must resolve");

        // uuid -> Uuid, never FixedBinary
        prop_assert!(matches!(uuid_rule, CellRule::Uuid), "uuid must resolve to Uuid");
        prop_assert!(
            !matches!(uuid_rule, CellRule::FixedBinary { .. }),
            "uuid must not resolve to FixedBinary"
        );

        // bytea -> FixedBinary{len:16}, never Uuid
        prop_assert!(
            matches!(bytea_rule, CellRule::FixedBinary { len: 16 }),
            "bytea must resolve to FixedBinary len 16"
        );
        prop_assert!(!matches!(bytea_rule, CellRule::Uuid), "bytea must not resolve to Uuid");
    }

    /// `(FixedSizeBinary(n), Bytea)` resolves to
    /// `FixedBinary{len: n}` for every valid width `n` in `1..=i32::MAX`, and
    /// such a result is never the `Uuid` rule (the two never alias).
    #[test]
    fn prop_fixed_n_bytea_resolves_to_fixed_binary(n in 1..=i32::MAX) {
        let rule = resolve_cell_rule(&DataType::FixedSizeBinary(n), PgColumnType::Bytea)
            .expect("FixedSizeBinary(n) + Bytea must resolve for n in 1..=i32::MAX");

        prop_assert!(
            matches!(rule, CellRule::FixedBinary { len } if len == n as usize),
            "expected FixedBinary {{ len: {n} }}, got {rule:?}"
        );
        // bytea resolution never aliases the uuid rule.
        prop_assert!(!matches!(rule, CellRule::Uuid));
    }

    /// only width 16 may resolve to `Uuid`. For any width `n`
    /// other than 16 paired with `uuid`, resolution must not yield `Uuid`
    /// (the width-16 uuid rule never aliases a different width).
    #[test]
    fn prop_non16_uuid_never_resolves_to_uuid(
        n in (1..=i32::MAX).prop_filter("exclude 16", |n| *n != 16),
    ) {
        let resolved = resolve_cell_rule(&DataType::FixedSizeBinary(n), PgColumnType::Uuid);
        match resolved {
            Ok(rule) => prop_assert!(
                !matches!(rule, CellRule::Uuid),
                "width {n} must not resolve to Uuid"
            ),
            Err(_) => { /* rejected, which is also non-aliasing */ }
        }
    }
}
