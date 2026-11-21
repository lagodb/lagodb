//! PostgreSQL operator and type admission for Parquet predicates.
//!
//! The admitted pairs follow `lagodb-iceberg`'s `PgPredicatePushdownPolicy`,
//! mapped to the Parquet declared contract: each PostgreSQL column type has
//! one logical Arrow family, and binding decodes the value into it. Temporal
//! comparisons are Conservative because infinite values have no file
//! representation; every other admitted comparison is Exact.

use lagodb_core::datum::{Decimal128Semantics, Utf8ServerEncoding};
use lagodb_core::expr::{
    ColumnRef, PgBpcharSemantics, PgComparisonIdentity, PgComparisonSignature,
    PgTextComparisonSemantics, RuntimeValueSpec,
};
use pgrx::pg_sys;

use super::integer::IntegerWidth;
use super::operator::ComparisonOperator;
use super::value::ValueType;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Capability {
    Exact,
    Conservative,
}

/// One comparison accepted by the policy, before operand mirroring.
#[derive(Clone, Copy)]
pub(super) struct SupportedComparison {
    pub(super) operator: ComparisonOperator,
    pub(super) value_type: ValueType,
    pub(super) capability: Capability,
}

pub(super) struct ParquetPushdownPolicy;

impl ParquetPushdownPolicy {
    pub(super) fn comparison(
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        identity: PgComparisonIdentity,
        utf8: Option<Utf8ServerEncoding>,
    ) -> Option<SupportedComparison> {
        let value_type = Self::value_type(column, value, utf8)?;
        let signature = PgComparisonSignature::for_operator(identity.opno)?;
        if !Self::supported_operator_family(
            signature.left_type(),
            signature.right_type(),
        ) {
            return None;
        }
        let kind = signature.kind();
        let collation_free = identity.opcollid == pg_sys::Oid::INVALID
            && identity.inputcollid == pg_sys::Oid::INVALID;
        let admitted = match value_type {
            ValueType::String => {
                unsafe { PgTextComparisonSemantics::for_comparison(identity, kind) }
                    .is_some()
            }
            // Binding pads the value to the column width, after which
            // `bpchareq` is byte equality under a deterministic collation.
            ValueType::Bpchar(semantics) => {
                unsafe { semantics.comparison(identity) }.is_some()
            }
            _ => collation_free,
        };
        let operator = ComparisonOperator::from(kind);
        if !admitted || !value_type.accepts_operator(operator) {
            return None;
        }
        let capability = match value_type {
            ValueType::Date | ValueType::Timestamp => Capability::Conservative,
            _ => Capability::Exact,
        };
        Some(SupportedComparison {
            operator,
            value_type,
            capability,
        })
    }

    /// Value types a set admits: each element decodes to a plain value, or
    /// to one outside the column's domain that no row can match.
    pub(super) const fn supports_set_membership(value_type: ValueType) -> bool {
        matches!(
            value_type,
            ValueType::Integer(_)
                | ValueType::Date
                | ValueType::String
                | ValueType::Bpchar(_)
                | ValueType::Uuid
        )
    }

    /// `starts_with(text, text)` is a byte-prefix test only under a
    /// deterministic collation.
    pub(super) fn supports_starts_with(
        column: &ColumnRef,
        prefix: &RuntimeValueSpec,
        utf8: Option<Utf8ServerEncoding>,
    ) -> bool {
        Self::value_type(column, prefix, utf8) == Some(ValueType::String)
            && unsafe {
                PgTextComparisonSemantics::for_equality_collation(
                    column.value_type.collation,
                )
            }
            .is_some()
    }

    pub(super) fn supports_nan_test(column: &ColumnRef) -> bool {
        let declared = column.declared_type.type_oid;
        matches!(declared, pg_sys::FLOAT4OID | pg_sys::FLOAT8OID)
            && declared == column.value_type.type_oid
    }

    pub(super) fn supports_boolean_test(column: &ColumnRef) -> bool {
        column.declared_type.type_oid == pg_sys::BOOLOID
            && column.value_type.type_oid == pg_sys::BOOLOID
    }

    fn supported_operator_family(left: pg_sys::Oid, right: pg_sys::Oid) -> bool {
        matches!(
            (left, right),
            (
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
            ) | (
                pg_sys::FLOAT4OID | pg_sys::FLOAT8OID,
                pg_sys::FLOAT4OID | pg_sys::FLOAT8OID,
            ) | (pg_sys::BOOLOID, pg_sys::BOOLOID)
                | (pg_sys::UUIDOID, pg_sys::UUIDOID)
                | (pg_sys::BYTEAOID, pg_sys::BYTEAOID)
                | (pg_sys::DATEOID, pg_sys::DATEOID)
                | (pg_sys::TIMESTAMPOID, pg_sys::TIMESTAMPOID)
                | (pg_sys::TIMESTAMPTZOID, pg_sys::TIMESTAMPTZOID)
                | (pg_sys::NUMERICOID, pg_sys::NUMERICOID)
                | (pg_sys::TEXTOID, pg_sys::TEXTOID)
                | (pg_sys::BPCHAROID, pg_sys::BPCHAROID)
        )
    }

    /// The value type for a (declared column, effective column, value) type
    /// triple under the Parquet declared contract.
    fn value_type(
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        utf8: Option<Utf8ServerEncoding>,
    ) -> Option<ValueType> {
        const INTEGERS: [pg_sys::Oid; 3] =
            [pg_sys::INT2OID, pg_sys::INT4OID, pg_sys::INT8OID];
        const STRINGS: [pg_sys::Oid; 2] = [pg_sys::TEXTOID, pg_sys::VARCHAROID];
        let declared = column.declared_type.type_oid;
        let effective = column.value_type.type_oid;
        let value_oid = value.value_type.type_oid;
        let same = |oid: pg_sys::Oid| {
            declared == oid && effective == oid && value_oid == oid
        };
        if INTEGERS.contains(&declared)
            && INTEGERS.contains(&effective)
            && INTEGERS.contains(&value_oid)
        {
            // PostgreSQL compares integers of any width in the integer domain;
            // a widened int8 column view is only a view of the narrower column.
            return match declared {
                pg_sys::INT2OID => Some(ValueType::Integer(IntegerWidth::I16)),
                pg_sys::INT4OID => Some(ValueType::Integer(IntegerWidth::I32)),
                _ if effective == pg_sys::INT8OID => {
                    Some(ValueType::Integer(IntegerWidth::I64))
                }
                _ => None,
            };
        }
        if STRINGS.contains(&declared)
            && STRINGS.contains(&effective)
            && STRINGS.contains(&value_oid)
        {
            return utf8.map(|_| ValueType::String);
        }
        match (declared, effective, value_oid) {
            (pg_sys::FLOAT8OID, pg_sys::FLOAT8OID, pg_sys::FLOAT4OID) => {
                Some(ValueType::Float4OnFloat8)
            }
            (pg_sys::FLOAT4OID, pg_sys::FLOAT4OID, pg_sys::FLOAT8OID) => {
                Some(ValueType::Float8OnFloat4)
            }
            _ if same(pg_sys::FLOAT4OID) => Some(ValueType::Float4),
            _ if same(pg_sys::FLOAT8OID) => Some(ValueType::Float8),
            _ if same(pg_sys::BOOLOID) => Some(ValueType::Bool),
            _ if same(pg_sys::UUIDOID) => Some(ValueType::Uuid),
            _ if same(pg_sys::BYTEAOID) => Some(ValueType::Binary),
            _ if same(pg_sys::DATEOID) => Some(ValueType::Date),
            _ if same(pg_sys::TIMESTAMPOID) || same(pg_sys::TIMESTAMPTZOID) => {
                Some(ValueType::Timestamp)
            }
            _ if same(pg_sys::NUMERICOID) => {
                Decimal128Semantics::for_storage_comparison(
                    column.declared_type,
                    column.value_type,
                    value.value_type,
                )
                .map(ValueType::Decimal128)
            }
            _ if same(pg_sys::BPCHAROID) => utf8
                .and(PgBpcharSemantics::for_type(column.declared_type))
                .map(ValueType::Bpchar),
            _ => None,
        }
    }
}
