//! PostgreSQL catalog and type adapter for the provider-neutral policy.

use lagodb_core::expr::{
    ColumnRef, PgComparisonIdentity, PgIntegerWidening, PgTextComparisonSemantics,
    RuntimeValueSpec,
};
use lagodb_core::tuple::{Decimal128Semantics, Utf8ServerEncoding};
use pgrx::{PgBuiltInOids, PgOid, pg_sys};

use super::super::plan::PlannedValueType;
use super::{PredicatePushdownPolicy, PredicateValueKind, SupportedComparison};

/// PostgreSQL-facing adapter that resolves catalog-backed collation facts
/// before delegating to [`PredicatePushdownPolicy`].
pub(crate) struct PgPredicatePushdownPolicy;

impl PgPredicatePushdownPolicy {
    pub(crate) fn supports_null_test(type_oid: pg_sys::Oid) -> bool {
        PredicatePushdownPolicy::null_test_value_kind(type_oid)
            .is_some_and(PredicatePushdownPolicy::supports_null_test)
    }
    pub(crate) fn plan_comparison(
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        op_key: PgComparisonIdentity,
        utf8: Option<Utf8ServerEncoding>,
    ) -> Option<(SupportedComparison, PlannedValueType)> {
        let value_type = Self::planned_value_type(column, value, utf8)?;
        let type_oid = column.value_type.type_oid;
        let operator = PredicatePushdownPolicy::op_class(op_key.opno)?;
        let text_semantics = if matches!(
            PgOid::from(type_oid),
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID)
        ) {
            unsafe { PgTextComparisonSemantics::for_comparison(op_key, operator) }
        } else {
            None
        };
        let capability = PredicatePushdownPolicy::capability_for_class(
            type_oid,
            op_key,
            text_semantics,
            operator,
        )?;
        Some((
            SupportedComparison {
                operator,
                capability,
            },
            value_type,
        ))
    }

    pub(crate) fn plan_starts_with(
        column: &ColumnRef,
        prefix: &RuntimeValueSpec,
        utf8: Option<Utf8ServerEncoding>,
    ) -> Option<PlannedValueType> {
        let value_type = Self::planned_value_type(column, prefix, utf8)?;
        let kind = Self::planned_value_kind(value_type);
        if !PredicatePushdownPolicy::supports_starts_with(kind) {
            return None;
        }
        unsafe {
            PgTextComparisonSemantics::for_equality_collation(
                column.value_type.collation,
            )?;
        }
        Some(value_type)
    }

    pub(crate) fn supports_nan_test(column: &ColumnRef) -> bool {
        let declared = PgOid::from(column.declared_type.type_oid);
        let effective = PgOid::from(column.value_type.type_oid);
        matches!(
            (declared, effective),
            (
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID)
            ) | (
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID)
            )
        ) && PredicatePushdownPolicy::supports_nan_test(PredicateValueKind::Float)
    }

    const fn planned_value_kind(value_type: PlannedValueType) -> PredicateValueKind {
        match value_type {
            PlannedValueType::Int2 | PlannedValueType::Int4 => {
                PredicateValueKind::Integer
            }
            PlannedValueType::Int8 => PredicateValueKind::Long,
            PlannedValueType::Date => PredicateValueKind::Date,
            PlannedValueType::Timestamp => PredicateValueKind::Timestamp,
            PlannedValueType::Timestamptz => PredicateValueKind::Timestamptz,
            PlannedValueType::String => PredicateValueKind::String,
            PlannedValueType::Decimal128(_) => PredicateValueKind::Decimal,
            PlannedValueType::Int8ToInt => PredicateValueKind::Integer,
        }
    }

    pub(super) fn planned_value_type(
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        utf8: Option<Utf8ServerEncoding>,
    ) -> Option<PlannedValueType> {
        let declared = PgOid::from(column.declared_type.type_oid);
        let effective = PgOid::from(column.value_type.type_oid);
        let value_oid = PgOid::from(value.value_type.type_oid);
        if column.value_type.type_oid == value.value_type.type_oid {
            match PgIntegerWidening::for_types(
                column.declared_type.type_oid,
                column.value_type.type_oid,
            ) {
                Some(PgIntegerWidening::Int2ToInt4) => {
                    return Some(PlannedValueType::Int4);
                }
                Some(
                    PgIntegerWidening::Int2ToInt8 | PgIntegerWidening::Int4ToInt8,
                ) => return Some(PlannedValueType::Int8ToInt),
                None => {}
            }
        }
        match (declared, effective, value_oid) {
            (
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
            ) => Some(PlannedValueType::Int2),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
            ) => Some(PlannedValueType::Int4),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
            ) => Some(PlannedValueType::Int8),
            (
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
            ) => Some(PlannedValueType::Date),
            (
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
            ) => Some(PlannedValueType::Timestamp),
            (
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
            ) => Some(PlannedValueType::Timestamptz),
            (
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
            ) => Decimal128Semantics::for_storage_comparison(
                column.declared_type,
                column.value_type,
                value.value_type,
            )
            .map(PlannedValueType::Decimal128),
            (
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
            ) => utf8.map(|_| PlannedValueType::String),
            _ => None,
        }
    }
}
