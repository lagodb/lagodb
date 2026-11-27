//! PostgreSQL catalog, collation, and Iceberg source-field admission rules.

use iceberg_lite::spec::{PrimitiveType, Type};
use lagodb_core::datum::{Decimal128Semantics, Utf8ServerEncoding};
use lagodb_core::expr::{
    ColumnRef, PgBpcharSemantics, PgComparisonIdentity, PgComparisonKind,
    PgComparisonSignature, PgIntegerWidening, PgTextComparisonSemantics,
    RuntimeValueSpec,
};
use pgrx::{PgBuiltInOids, PgOid, pg_sys};

use crate::predicate::ir::PlannedValueType;
use crate::predicate::semantics::{
    PredicateCapabilities, PredicateCapability, PredicateValueKind,
    TimestampPrecision,
};

use super::PredicateStorageContract;

/// One comparison accepted by the PostgreSQL pushdown admission rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SupportedComparison {
    pub(super) operator: PgComparisonKind,
    pub(super) capability: PredicateCapability,
}

/// PostgreSQL-side scalar conversion shape before finalizing against the remote
/// Iceberg field type and storage contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PgValueType {
    Boolean,
    Uuid,
    Binary,
    Int2,
    Int4,
    Int8,
    Int2ToLong,
    Int4ToLong,
    Int8ToInt,
    Float,
    Double,
    FloatToDouble,
    DoubleToFloat,
    Date,
    Timestamp,
    Timestamptz,
    String,
    Bpchar(PgBpcharSemantics),
    Decimal128(Decimal128Semantics),
}

/// PostgreSQL-facing admission oracle that resolves catalog-backed collation
/// facts and remote Iceberg field types before delegating to
/// [`PredicateCapabilities`].
pub(super) struct PgPredicateAdmission;

impl PgPredicateAdmission {
    /// Capability oracle for `IS NULL` / `IS NOT NULL` and strict-nullity
    /// rewrites on a scan-column type.
    ///
    /// Null tests only inspect the primitive validity bitmap — no value
    /// comparison is involved — so any primitive Iceberg field is exact once
    /// the local declaration is a supported scalar type. Nested types remain
    /// excluded because their Parquet validity belongs to groups.
    pub(super) fn supports_null_test(type_oid: pg_sys::Oid, source: &Type) -> bool {
        matches!(source, Type::Primitive(_))
            && Self::null_test_value_kind(type_oid)
                .is_some_and(PredicateCapabilities::supports_null_test)
    }

    pub(super) fn supports_boolean_test(column: &ColumnRef, source: &Type) -> bool {
        column.declared_type.type_oid == pg_sys::BOOLOID
            && column.value_type.type_oid == pg_sys::BOOLOID
            && matches!(source, Type::Primitive(PrimitiveType::Boolean))
            && PredicateCapabilities::supports_boolean_test(
                PredicateValueKind::Boolean,
            )
    }

    pub(super) fn supports_nan_test(column: &ColumnRef, source: &Type) -> bool {
        let declared = PgOid::from(column.declared_type.type_oid);
        let effective = PgOid::from(column.value_type.type_oid);
        matches!(
            (declared, effective, source),
            (
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
                Type::Primitive(PrimitiveType::Float),
            ) | (
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
                Type::Primitive(PrimitiveType::Double),
            )
        ) && PredicateCapabilities::supports_nan_test(PredicateValueKind::Float)
    }

    pub(super) fn supports_starts_with(
        column: &ColumnRef,
        prefix: &RuntimeValueSpec,
        source: &Type,
        utf8: Option<Utf8ServerEncoding>,
    ) -> bool {
        matches!(
            Self::pg_value_type(column, prefix, utf8),
            Some(PgValueType::String)
        ) && matches!(source, Type::Primitive(PrimitiveType::String))
            && PredicateCapabilities::supports_starts_with(PredicateValueKind::String)
            && unsafe {
                PgTextComparisonSemantics::for_equality_collation(
                    column.value_type.collation,
                )
            }
            .is_some()
    }

    pub(super) fn plan_comparison(
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        op_key: PgComparisonIdentity,
        source: &Type,
        utf8: Option<Utf8ServerEncoding>,
        contract: PredicateStorageContract,
    ) -> Option<(SupportedComparison, PlannedValueType)> {
        let value_type =
            Self::planned_value_type(column, value, source, utf8, contract)?;
        let type_oid = column.value_type.type_oid;
        let operator = Self::comparison_kind(op_key.opno)?;
        let text_semantics = match value_type {
            PlannedValueType::String => unsafe {
                PgTextComparisonSemantics::for_comparison(op_key, operator)
            },
            // Binding pads the value to the column width, after which
            // `bpchareq` is byte equality under a deterministic collation on
            // managed storage.
            PlannedValueType::Bpchar(semantics) => {
                unsafe { semantics.comparison(op_key) }
                    .map(|_| PgTextComparisonSemantics::Equality)
            }
            _ => None,
        };
        let mut capability =
            Self::comparison_capability(type_oid, op_key, text_semantics, operator)?;
        if matches!(value_type, PlannedValueType::FixedBinary(_)) {
            // PostgreSQL bytea values do not carry Iceberg's fixed width.
            // A different-width value stays with the residual at bind time.
            capability = PredicateCapability::Conservative;
        }
        Some((
            SupportedComparison {
                operator,
                capability,
            },
            value_type,
        ))
    }

    /// Value types a set admits. Decoding each element yields a plain literal,
    /// an integer or bpchar outside the column's domain that no row can match,
    /// or nothing for an infinite date.
    pub(super) const fn supports_set_membership(
        value_type: PlannedValueType,
    ) -> bool {
        matches!(
            value_type,
            PlannedValueType::Int2
                | PlannedValueType::Int4
                | PlannedValueType::Int8
                | PlannedValueType::Int2ToLong
                | PlannedValueType::Int4ToLong
                | PlannedValueType::Int8ToInt
                | PlannedValueType::Date
                | PlannedValueType::String
                | PlannedValueType::Bpchar(_)
                | PlannedValueType::Uuid
        )
    }

    /// Map a comparison from an Iceberg-supported operator family.
    ///
    /// PostgreSQL normalization already guarantees compatibility between the
    /// selected signature and the comparison operands.
    fn comparison_kind(opno: pg_sys::Oid) -> Option<PgComparisonKind> {
        let signature = PgComparisonSignature::for_operator(opno)?;
        match (signature.left_type(), signature.right_type()) {
            (
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
            )
            | (
                pg_sys::FLOAT4OID | pg_sys::FLOAT8OID,
                pg_sys::FLOAT4OID | pg_sys::FLOAT8OID,
            )
            | (pg_sys::BOOLOID, pg_sys::BOOLOID)
            | (pg_sys::UUIDOID, pg_sys::UUIDOID)
            | (pg_sys::BYTEAOID, pg_sys::BYTEAOID)
            | (pg_sys::DATEOID, pg_sys::DATEOID)
            | (pg_sys::TIMESTAMPOID, pg_sys::TIMESTAMPOID)
            | (pg_sys::TIMESTAMPTZOID, pg_sys::TIMESTAMPTZOID)
            | (pg_sys::NUMERICOID, pg_sys::NUMERICOID)
            | (pg_sys::TEXTOID, pg_sys::TEXTOID)
            | (pg_sys::BPCHAROID, pg_sys::BPCHAROID) => Some(signature.kind()),
            _ => None,
        }
    }

    fn comparison_capability(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonIdentity,
        text_semantics: Option<PgTextComparisonSemantics>,
        kind: PgComparisonKind,
    ) -> Option<PredicateCapability> {
        match PgOid::from(type_oid) {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                PredicateCapabilities::comparison_capability(
                    PredicateValueKind::Boolean,
                    kind,
                )
            }
            PgOid::BuiltIn(PgBuiltInOids::UUIDOID | PgBuiltInOids::BYTEAOID) => {
                PredicateCapabilities::comparison_capability(
                    PredicateValueKind::Binary,
                    kind,
                )
            }
            PgOid::BuiltIn(
                PgBuiltInOids::INT2OID
                | PgBuiltInOids::INT4OID
                | PgBuiltInOids::INT8OID,
            ) => {
                if Self::is_collation_free(op_key) {
                    PredicateCapabilities::comparison_capability(
                        if type_oid == pg_sys::INT8OID {
                            PredicateValueKind::Long
                        } else {
                            PredicateValueKind::Integer
                        },
                        kind,
                    )
                } else {
                    // Integer comparisons with a tagged collation are not
                    // translatable; do not mark pushable.
                    None
                }
            }
            PgOid::BuiltIn(
                PgBuiltInOids::DATEOID
                | PgBuiltInOids::TIMESTAMPOID
                | PgBuiltInOids::TIMESTAMPTZOID,
            ) => {
                let value_kind = match PgOid::from(type_oid) {
                    PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                        PredicateValueKind::Date
                    }
                    PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                        PredicateValueKind::Timestamp
                    }
                    PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                        PredicateValueKind::Timestamptz
                    }
                    _ => unreachable!("matched temporal PostgreSQL type"),
                };
                PredicateCapabilities::comparison_capability(value_kind, kind)?;
                Some(PredicateCapability::Conservative)
            }
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                if Self::is_collation_free(op_key) {
                    PredicateCapabilities::comparison_capability(
                        PredicateValueKind::Decimal,
                        kind,
                    )
                } else {
                    None
                }
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID | PgBuiltInOids::FLOAT8OID) => {
                if Self::is_collation_free(op_key) {
                    PredicateCapabilities::comparison_capability(
                        PredicateValueKind::Float,
                        kind,
                    )
                } else {
                    None
                }
            }
            // `character(n)` equality ignores trailing blanks (`bpchareq`).
            // PostgreSQL's assignment coercion stores every value padded to
            // the declared width on managed tables, and binding pads the
            // comparison value the same way (`PlannedValueType::Bpchar`), so
            // byte equality holds only for `=` / `<>` on a declared width
            // under a deterministic collation when the storage contract
            // establishes padding. Ordering stays unsupported: `bpcharcmp`
            // orders trimmed values, while padded bytes place a blank after
            // every lower character such as a tab.
            PgOid::BuiltIn(
                PgBuiltInOids::TEXTOID
                | PgBuiltInOids::VARCHAROID
                | PgBuiltInOids::BPCHAROID,
            ) => {
                text_semantics?;
                PredicateCapabilities::comparison_capability(
                    PredicateValueKind::String,
                    kind,
                )
            }
            // Every other comparison type is intentionally unsupported.
            _ => None,
        }
    }

    /// Resolve the PostgreSQL column/value type pair and finalize storage
    /// precision, fixed binary width, decimal shape, and padding rules against
    /// the remote Iceberg field type once while the source schema is available.
    fn planned_value_type(
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        source: &Type,
        utf8: Option<Utf8ServerEncoding>,
        contract: PredicateStorageContract,
    ) -> Option<PlannedValueType> {
        let Type::Primitive(primitive) = source else {
            return None;
        };
        let pg_type = Self::pg_value_type(column, value, utf8)?;
        match (pg_type, primitive) {
            (PgValueType::Boolean, PrimitiveType::Boolean) => {
                Some(PlannedValueType::Boolean)
            }
            (PgValueType::Uuid, PrimitiveType::Uuid) => Some(PlannedValueType::Uuid),
            (PgValueType::Binary, PrimitiveType::Binary) => {
                Some(PlannedValueType::Binary)
            }
            (PgValueType::Binary, PrimitiveType::Fixed(width)) => {
                Some(PlannedValueType::FixedBinary(*width))
            }
            (PgValueType::Int2, PrimitiveType::Int) => Some(PlannedValueType::Int2),
            (PgValueType::Int4, PrimitiveType::Int) => Some(PlannedValueType::Int4),
            (PgValueType::Int8ToInt, PrimitiveType::Int) => {
                Some(PlannedValueType::Int8ToInt)
            }
            (PgValueType::Int8, PrimitiveType::Long) => Some(PlannedValueType::Int8),
            (PgValueType::Int2ToLong, PrimitiveType::Long) => {
                Some(PlannedValueType::Int2ToLong)
            }
            (PgValueType::Int4ToLong, PrimitiveType::Long) => {
                Some(PlannedValueType::Int4ToLong)
            }
            (PgValueType::Float, PrimitiveType::Float) => {
                Some(PlannedValueType::Float)
            }
            (PgValueType::DoubleToFloat, PrimitiveType::Float) => {
                Some(PlannedValueType::DoubleToFloat)
            }
            (PgValueType::Double, PrimitiveType::Double) => {
                Some(PlannedValueType::Double)
            }
            (PgValueType::FloatToDouble, PrimitiveType::Double) => {
                Some(PlannedValueType::FloatToDouble)
            }
            (PgValueType::Date, PrimitiveType::Date) => Some(PlannedValueType::Date),
            (PgValueType::Timestamp, PrimitiveType::Timestamp) => Some(
                PlannedValueType::Timestamp(TimestampPrecision::Microseconds),
            ),
            (PgValueType::Timestamp, PrimitiveType::TimestampNs) => {
                Some(PlannedValueType::Timestamp(TimestampPrecision::Nanoseconds))
            }
            (PgValueType::Timestamptz, PrimitiveType::Timestamptz) => Some(
                PlannedValueType::Timestamptz(TimestampPrecision::Microseconds),
            ),
            (PgValueType::Timestamptz, PrimitiveType::TimestamptzNs) => Some(
                PlannedValueType::Timestamptz(TimestampPrecision::Nanoseconds),
            ),
            (PgValueType::String, PrimitiveType::String) => {
                Some(PlannedValueType::String)
            }
            (PgValueType::Bpchar(semantics), PrimitiveType::String)
                if contract.allows_padded_bpchar() =>
            {
                Some(PlannedValueType::Bpchar(semantics))
            }
            (
                PgValueType::Decimal128(semantics),
                PrimitiveType::Decimal { precision, scale },
            ) if u32::from(semantics.precision()) == *precision
                && u32::try_from(semantics.scale()).ok() == Some(*scale) =>
            {
                Some(PlannedValueType::Decimal128(semantics))
            }
            _ => None,
        }
    }

    fn pg_value_type(
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        utf8: Option<Utf8ServerEncoding>,
    ) -> Option<PgValueType> {
        let declared = PgOid::from(column.declared_type.type_oid);
        let effective = PgOid::from(column.value_type.type_oid);
        let value_oid = PgOid::from(value.value_type.type_oid);
        if column.value_type.type_oid == value.value_type.type_oid {
            match PgIntegerWidening::for_types(
                column.declared_type.type_oid,
                column.value_type.type_oid,
            ) {
                Some(PgIntegerWidening::Int2ToInt4) => {
                    return Some(PgValueType::Int4);
                }
                Some(
                    PgIntegerWidening::Int2ToInt8 | PgIntegerWidening::Int4ToInt8,
                ) => return Some(PgValueType::Int8ToInt),
                None => {}
            }
        }
        match (declared, effective, value_oid) {
            (
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID),
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID),
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID),
            ) => Some(PgValueType::Boolean),
            (
                PgOid::BuiltIn(PgBuiltInOids::UUIDOID),
                PgOid::BuiltIn(PgBuiltInOids::UUIDOID),
                PgOid::BuiltIn(PgBuiltInOids::UUIDOID),
            ) => Some(PgValueType::Uuid),
            (
                PgOid::BuiltIn(PgBuiltInOids::BYTEAOID),
                PgOid::BuiltIn(PgBuiltInOids::BYTEAOID),
                PgOid::BuiltIn(PgBuiltInOids::BYTEAOID),
            ) => Some(PgValueType::Binary),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT2OID | PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(
                    PgBuiltInOids::INT2OID
                    | PgBuiltInOids::INT4OID
                    | PgBuiltInOids::INT8OID,
                ),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
            ) => Some(PgValueType::Int8ToInt),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT2OID | PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(
                    PgBuiltInOids::INT2OID
                    | PgBuiltInOids::INT4OID
                    | PgBuiltInOids::INT8OID,
                ),
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
            ) => Some(PgValueType::Int2),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT2OID | PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(
                    PgBuiltInOids::INT2OID
                    | PgBuiltInOids::INT4OID
                    | PgBuiltInOids::INT8OID,
                ),
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
            ) => Some(PgValueType::Int4),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
            ) => Some(PgValueType::Int2ToLong),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
            ) => Some(PgValueType::Int4ToLong),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
            ) => Some(PgValueType::Int8),
            (
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
            ) => Some(PgValueType::Float),
            (
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
            ) => Some(PgValueType::Double),
            // float84 operators widen the real value exactly; float48
            // operators widen the real column, so binding rounds the double
            // value to the column's precision.
            (
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
            ) => Some(PgValueType::FloatToDouble),
            (
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID),
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
            ) => Some(PgValueType::DoubleToFloat),
            (
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
            ) => Some(PgValueType::Date),
            (
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
            ) => Some(PgValueType::Timestamp),
            (
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
            ) => Some(PgValueType::Timestamptz),
            (
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
            ) => Decimal128Semantics::for_storage_comparison(
                column.declared_type,
                column.value_type,
                value.value_type,
            )
            .map(PgValueType::Decimal128),
            (
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
            ) => utf8.map(|_| PgValueType::String),
            (
                PgOid::BuiltIn(PgBuiltInOids::BPCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::BPCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::BPCHAROID),
            ) => utf8
                .and(PgBpcharSemantics::for_type(column.declared_type))
                .map(PgValueType::Bpchar),
            _ => None,
        }
    }

    #[inline]
    fn is_collation_free(op_key: PgComparisonIdentity) -> bool {
        op_key.opcollid == pg_sys::Oid::INVALID
            && op_key.inputcollid == pg_sys::Oid::INVALID
    }

    fn null_test_value_kind(type_oid: pg_sys::Oid) -> Option<PredicateValueKind> {
        match PgOid::from(type_oid) {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                Some(PredicateValueKind::Boolean)
            }
            PgOid::BuiltIn(PgBuiltInOids::UUIDOID | PgBuiltInOids::BYTEAOID) => {
                Some(PredicateValueKind::Binary)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => Some(PredicateValueKind::Time),
            PgOid::BuiltIn(PgBuiltInOids::INT2OID | PgBuiltInOids::INT4OID) => {
                Some(PredicateValueKind::Integer)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => Some(PredicateValueKind::Long),
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                Some(PredicateValueKind::Decimal)
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => Some(PredicateValueKind::Date),
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                Some(PredicateValueKind::Timestamp)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                Some(PredicateValueKind::Timestamptz)
            }
            PgOid::BuiltIn(
                PgBuiltInOids::TEXTOID
                | PgBuiltInOids::VARCHAROID
                | PgBuiltInOids::BPCHAROID,
            ) => Some(PredicateValueKind::String),
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID | PgBuiltInOids::FLOAT8OID) => {
                Some(PredicateValueKind::Float)
            }
            _ => None,
        }
    }
}
