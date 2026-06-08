//! Runtime [`IcebergPredicateTranslator`] (PG expr → iceberg [`Predicate`]).
//!
//! This module owns the whole "PG expression → iceberg [`Predicate`]" concern,
//! laid out top to bottom: the scalar leaf model ([`IcebergScalar`] /
//! [`ScalarKind`]), the error enum ([`IcebergTranslationError`]), the `unsafe`
//! PG→iceberg value decoder (`decode_datum`), the shared predicate-tree fold
//! kernel (`fold_left`), and the translator itself (whose private methods own
//! the translator-only assembly helpers `fold_predicates` / `mirror_operator`).

use iceberg_lite::expr::{
    BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression,
};
use iceberg_lite::spec::Datum;
use pg_lakebase_core::expr::ColumnNameResolver;
use pg_lakebase_core::expr::nodes::{
    PgColumnRef, PgComparisonOp, PgLiteral, PgParamValue,
};
use pg_lakebase_core::expr::translator::PgPredicateTranslator;
use pgrx::prelude::{AnyNumeric, Date, Timestamp, TimestampWithTimeZone};
use pgrx::{FromDatum, PgBuiltInOids, PgOid, pg_sys};
use rust_decimal::Decimal;

use super::predicate_pushdown_policy::{ComparisonOpClass, PredicatePushdownPolicy};
use crate::customscan::FLOAT_PUSHDOWN_ENABLED;
use pg_arrow_conv::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};

// =============================================================================
// Scalar leaf model
//
// Pure model layer: carries no dependency on the error type below. Error
// mapping (e.g. rejecting a non-column operand) lives in the translator's
// methods, never in the model.
// =============================================================================

/// Runtime scalar leaf: column, non-null literal/param, or NULL operand.
#[derive(Debug, Clone)]
pub enum IcebergScalar {
    Column {
        reference: Reference,
        atttypid: pg_sys::Oid,
    },
    Datum(Datum),
    /// NULL literal or NULL-resolved param; strict comparisons fold to `AlwaysFalse`.
    Null {
        type_oid: pg_sys::Oid,
    },
}

impl IcebergScalar {
    fn kind(&self) -> ScalarKind {
        match self {
            IcebergScalar::Column { .. } => ScalarKind::Column,
            IcebergScalar::Datum(_) => ScalarKind::Datum,
            IcebergScalar::Null { .. } => ScalarKind::Null,
        }
    }
}

/// Operand shape tag for [`IcebergTranslationError::ComparisonShape`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarKind {
    Column,
    Datum,
    Null,
}

// =============================================================================
// Error type
// =============================================================================

/// Errors surfaced by the Iceberg runtime predicate translator.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IcebergTranslationError {
    #[error(
        "iceberg-am translator: get_attname returned NULL for \
         (rel_oid={}, attno={attno})",
        u32::from(*rel_oid)
    )]
    ColumnLookupFailed {
        rel_oid: pg_sys::Oid,
        attno: pg_sys::AttrNumber,
    },

    #[error(
        "iceberg-am translator: column name for (rel_oid={}, \
         attno={attno}) is not valid UTF-8: {cause}",
        u32::from(*rel_oid)
    )]
    ColumnNameNotUtf8 {
        rel_oid: pg_sys::Oid,
        attno: pg_sys::AttrNumber,
        cause: String,
    },

    #[error(
        "iceberg-am translator: refusing to translate system / \
         whole-row column (rel_oid={}, attno={attno})",
        u32::from(*rel_oid)
    )]
    SystemOrWholeRowColumn {
        rel_oid: pg_sys::Oid,
        attno: pg_sys::AttrNumber,
    },

    #[error(
        "iceberg-am translator: comparison expects column op literal/param; \
         got left={left:?}, right={right:?}"
    )]
    ComparisonShape { left: ScalarKind, right: ScalarKind },

    #[error(
        "iceberg-am translator: IS NULL / IS NOT NULL is only \
         supported on a column term"
    )]
    NullTestOnNonColumn,

    #[error(
        "iceberg-am translator: unsupported operator triple \
         (opno={}, opcollid={}, inputcollid={})",
        u32::from(*opno),
        u32::from(*opcollid),
        u32::from(*inputcollid)
    )]
    UnsupportedOperator {
        opno: pg_sys::Oid,
        opcollid: pg_sys::Oid,
        inputcollid: pg_sys::Oid,
    },

    #[error(
        "iceberg-am translator: unsupported PG type OID {}",
        u32::from(*type_oid)
    )]
    UnsupportedType { type_oid: pg_sys::Oid },

    /// Value cannot be represented as an iceberg `Datum` (ConservativePruning-drop path).
    #[error(
        "iceberg-am translator: value of PG type OID {} is not \
         representable as an iceberg Datum (dropped to residual)",
        u32::from(*type_oid)
    )]
    ValueNotRepresentable { type_oid: pg_sys::Oid },

    #[error(
        "iceberg-am translator: failed to decode PG Datum for type OID {}",
        u32::from(*type_oid)
    )]
    DatumDecode { type_oid: pg_sys::Oid },

    #[error(
        "iceberg-am translator: AND/OR called with an empty \
         children list"
    )]
    EmptyBoolExpr,
}

// =============================================================================
// Datum decoding
//
// Decode a non-null PG `Datum` (literal/param value) into an iceberg `Datum`.
// This is the `unsafe` PG-FFI surface of the translator: each arm trusts that
// `type_oid` accurately describes the value behind `datum`. Temporal arms reuse
// the shared PG→Unix epoch offsets so pushed bounds match the storage write
// side (`pg_arrow_conv::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros}`).
// =============================================================================

/// Decode a non-null PG `Datum` into an iceberg [`Datum`].
///
/// Supports integers, numeric, date/timestamp types, optional floats, and
/// text/varchar. Collation admissibility is enforced in
/// [`IcebergPredicateTranslator::comparison`].
///
/// # Safety
///
/// `type_oid` must accurately describe the PG type the `datum` represents.
pub(crate) unsafe fn decode_datum(
    type_oid: pg_sys::Oid,
    datum: pg_sys::Datum,
) -> Result<Datum, IcebergTranslationError> {
    let pg_oid = PgOid::from(type_oid);
    let result = match pg_oid {
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
            unsafe { i16::from_datum(datum, false) }
                .map(|v| Datum::int(v as i32))
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
            unsafe { i32::from_datum(datum, false) }
                .map(Datum::int)
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
            unsafe { i64::from_datum(datum, false) }
                .map(Datum::long)
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
            unsafe { decode_numeric(type_oid, datum) }?
        }
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
            unsafe { decode_date(type_oid, datum) }?
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            unsafe { decode_timestamp(type_oid, datum) }?
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
            unsafe { decode_timestamptz(type_oid, datum) }?
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) if FLOAT_PUSHDOWN_ENABLED => {
            unsafe { f32::from_datum(datum, false) }
                .map(Datum::float)
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) if FLOAT_PUSHDOWN_ENABLED => {
            unsafe { f64::from_datum(datum, false) }
                .map(Datum::double)
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID) => {
            unsafe { String::from_datum(datum, false) }
                .map(Datum::string)
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        _ => {
            return Err(IcebergTranslationError::UnsupportedType { type_oid });
        }
    };
    Ok(result)
}

/// Decode PG `numeric` via canonical text into iceberg decimal [`Datum`].
///
/// # Safety
///
/// `datum` must be a valid non-null PG `numeric`.
unsafe fn decode_numeric(
    type_oid: pg_sys::Oid,
    datum: pg_sys::Datum,
) -> Result<Datum, IcebergTranslationError> {
    let numeric = unsafe { AnyNumeric::from_datum(datum, false) }
        .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;

    // NaN / ±Infinity have no Iceberg ordering for pruning bounds.
    if numeric.is_nan() {
        return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
    }

    let decimal = Decimal::from_str_exact(numeric.normalize())
        .map_err(|_| IcebergTranslationError::ValueNotRepresentable { type_oid })?;

    Datum::decimal(decimal)
        .map_err(|_| IcebergTranslationError::ValueNotRepresentable { type_oid })
}

/// Decode PG `date` using shared PG→Unix day offset.
///
/// # Safety
///
/// `datum` must be a valid non-null PG `date`.
unsafe fn decode_date(
    type_oid: pg_sys::Oid,
    datum: pg_sys::Datum,
) -> Result<Datum, IcebergTranslationError> {
    let date = unsafe { Date::from_datum(datum, false) }
        .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;

    // ±infinity dates have no finite day count.
    if !date.is_finite() {
        return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
    }

    let unix_days = pg_epoch_days_to_unix_days(date.to_pg_epoch_days())
        .ok_or(IcebergTranslationError::ValueNotRepresentable { type_oid })?;
    Ok(Datum::date(unix_days))
}

/// Decode PG `timestamp` using shared PG→Unix microsecond offset.
///
/// # Safety
///
/// `datum` must be a valid non-null PG `timestamp`.
unsafe fn decode_timestamp(
    type_oid: pg_sys::Oid,
    datum: pg_sys::Datum,
) -> Result<Datum, IcebergTranslationError> {
    let ts = unsafe { Timestamp::from_datum(datum, false) }
        .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;

    if !ts.is_finite() {
        return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
    }

    let pg_micros: i64 = ts.into();
    let unix_micros = pg_epoch_micros_to_unix_micros(pg_micros)
        .ok_or(IcebergTranslationError::ValueNotRepresentable { type_oid })?;
    Ok(Datum::timestamp_micros(unix_micros))
}

/// Decode PG `timestamptz` (PG stores UTC micros since PG epoch).
///
/// # Safety
///
/// `datum` must be a valid non-null PG `timestamptz`.
unsafe fn decode_timestamptz(
    type_oid: pg_sys::Oid,
    datum: pg_sys::Datum,
) -> Result<Datum, IcebergTranslationError> {
    let ts = unsafe { TimestampWithTimeZone::from_datum(datum, false) }
        .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;

    if !ts.is_finite() {
        return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
    }

    let pg_micros: i64 = ts.into();
    let unix_micros = pg_epoch_micros_to_unix_micros(pg_micros)
        .ok_or(IcebergTranslationError::ValueNotRepresentable { type_oid })?;
    Ok(Datum::timestamptz_micros(unix_micros))
}

// =============================================================================
// Predicate-tree fold kernel
//
// `fold_left` is the one genuinely shared building block on
// `iceberg_lite::expr::Predicate`: it backs both the translator's
// `fold_predicates` helper (errors on empty) and the scan provider's
// `combine_with_and` (treats empty as "no filter"). It lives here as a
// free function precisely because it is *not* translator-only. The
// translator-only assembly helpers (`fold_predicates` / `mirror_operator`)
// are private methods on [`IcebergPredicateTranslator`] below.
//
// It lives here (rather than in the vendored `iceberg-lite`, which is
// periodically re-merged from upstream) because it is first-party assembly
// logic.
// =============================================================================

/// Left-associative fold of `items` with `combine`; `None` for empty input.
///
/// The kernel shared by [`IcebergPredicateTranslator::fold_predicates`]
/// (errors on empty) and the scan provider's `combine_with_and` (treats empty
/// as "no filter").
pub(crate) fn fold_left(
    items: Vec<Predicate>,
    combine: impl Fn(Predicate, Predicate) -> Predicate,
) -> Option<Predicate> {
    let mut iter = items.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, combine))
}

// =============================================================================
// Translator
// =============================================================================

/// Runtime [`PgPredicateTranslator`] for Iceberg: column refs, datum decode, predicate assembly.
#[derive(Debug, Default)]
pub struct IcebergPredicateTranslator {
    pushdown: PredicatePushdownPolicy,
}

impl IcebergPredicateTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    pub const fn with_policy(pushdown: PredicatePushdownPolicy) -> Self {
        Self { pushdown }
    }
}

impl PgPredicateTranslator for IcebergPredicateTranslator {
    type Scalar = IcebergScalar;
    type Predicate = Predicate;
    type Error = IcebergTranslationError;

    fn column(&mut self, col: PgColumnRef<'_>) -> Result<Self::Scalar, Self::Error> {
        let name = Self::resolve_column_name(col.name, col.rel_oid, col.attno)?;
        Ok(IcebergScalar::Column {
            reference: Reference::new(name),
            atttypid: col.atttypid,
        })
    }

    /// NULL literals decode to [`IcebergScalar::Null`]; [`Self::comparison`] folds them to `AlwaysFalse`.
    fn literal(&mut self, lit: PgLiteral<'_>) -> Result<Self::Scalar, Self::Error> {
        if lit.is_null {
            return Ok(IcebergScalar::Null {
                type_oid: lit.type_oid,
            });
        }
        let datum = unsafe { decode_datum(lit.type_oid, lit.datum) }?;
        Ok(IcebergScalar::Datum(datum))
    }

    /// Mirrors [`Self::literal`]: NULL params decode to [`IcebergScalar::Null`], not an error.
    fn param_value(
        &mut self,
        param: PgParamValue,
    ) -> Result<Self::Scalar, Self::Error> {
        if param.is_null {
            return Ok(IcebergScalar::Null {
                type_oid: param.type_oid,
            });
        }
        let datum = unsafe { decode_datum(param.type_oid, param.datum) }?;
        Ok(IcebergScalar::Datum(datum))
    }

    fn comparison(
        &mut self,
        op: PgComparisonOp,
        left: Self::Scalar,
        right: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error> {
        // SQL three-valued logic: strict comparison with NULL is UNKNOWN → fold to AlwaysFalse.
        if matches!(left, IcebergScalar::Null { .. })
            || matches!(right, IcebergScalar::Null { .. })
        {
            return Ok(Predicate::AlwaysFalse);
        }

        let (reference, atttypid, datum, swap_sides) = match (left, right) {
            (
                IcebergScalar::Column {
                    reference,
                    atttypid,
                },
                IcebergScalar::Datum(datum),
            ) => (reference, atttypid, datum, false),
            (
                IcebergScalar::Datum(datum),
                IcebergScalar::Column {
                    reference,
                    atttypid,
                },
            ) => (reference, atttypid, datum, true),
            (l, r) => {
                return Err(IcebergTranslationError::ComparisonShape {
                    left: l.kind(),
                    right: r.kind(),
                });
            }
        };

        if !self.pushdown.can_build(atttypid, op) {
            return Err(IcebergTranslationError::UnsupportedType {
                type_oid: atttypid,
            });
        }

        let mut predicate_op = self.map_comparison_operator(op)?;
        if swap_sides {
            predicate_op = self.mirror_operator(predicate_op);
        }

        Ok(Predicate::Binary(BinaryExpression::new(
            predicate_op,
            reference,
            datum,
        )))
    }

    fn is_null(
        &mut self,
        value: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error> {
        let reference = self.null_test_reference(value)?;
        Ok(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            reference,
        )))
    }

    fn is_not_null(
        &mut self,
        value: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error> {
        let reference = self.null_test_reference(value)?;
        Ok(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            reference,
        )))
    }

    fn and(
        &mut self,
        items: Vec<Self::Predicate>,
    ) -> Result<Self::Predicate, Self::Error> {
        self.fold_predicates(items, /*and=*/ true)
    }

    fn or(
        &mut self,
        items: Vec<Self::Predicate>,
    ) -> Result<Self::Predicate, Self::Error> {
        self.fold_predicates(items, /*and=*/ false)
    }

    /// Wraps the child in `Predicate::Not` (schema binding applies `rewrite_not` later).
    fn not(&mut self, item: Self::Predicate) -> Result<Self::Predicate, Self::Error> {
        Ok(!item)
    }
}

impl IcebergPredicateTranslator {
    /// Extract the column [`Reference`] for an `IS NULL` / `IS NOT NULL` test.
    ///
    /// Owns the model→error mapping: a non-column operand is rejected as
    /// [`IcebergTranslationError::NullTestOnNonColumn`], and (defense in depth)
    /// a column whose type is outside the shared null-test allowlist is
    /// rejected as [`IcebergTranslationError::UnsupportedType`].
    fn null_test_reference(
        &self,
        value: IcebergScalar,
    ) -> Result<Reference, IcebergTranslationError> {
        let IcebergScalar::Column {
            reference,
            atttypid,
        } = value
        else {
            return Err(IcebergTranslationError::NullTestOnNonColumn);
        };

        if matches!(
            self.pushdown.null_test_capability(atttypid),
            super::predicate_pushdown_policy::PredicateCapability::Unsupported
        ) {
            return Err(IcebergTranslationError::UnsupportedType {
                type_oid: atttypid,
            });
        }
        Ok(reference)
    }

    /// Resolve column name from carried plan-time name or core's attname fallback.
    fn resolve_column_name(
        carried: Option<&str>,
        rel_oid: pg_sys::Oid,
        attno: pg_sys::AttrNumber,
    ) -> Result<String, IcebergTranslationError> {
        if let Some(name) = carried {
            return Ok(name.to_string());
        }

        if attno <= 0 {
            return Err(IcebergTranslationError::SystemOrWholeRowColumn {
                rel_oid,
                attno,
            });
        }

        match ColumnNameResolver::new(rel_oid).try_resolve(attno) {
            Ok(Some(name)) => Ok(name),
            Ok(None) => {
                Err(IcebergTranslationError::ColumnLookupFailed { rel_oid, attno })
            }
            Err(cause) => Err(IcebergTranslationError::ColumnNameNotUtf8 {
                rel_oid,
                attno,
                cause: cause.to_string(),
            }),
        }
    }

    /// Map PG operator triple to iceberg [`PredicateOperator`] via shared
    /// comparison-op policy. Collation admissibility is checked separately in
    /// `comparison`.
    fn map_comparison_operator(
        &self,
        op: PgComparisonOp,
    ) -> Result<PredicateOperator, IcebergTranslationError> {
        match self.pushdown.op_class(op.opno) {
            Some(ComparisonOpClass::Eq) => Ok(PredicateOperator::Eq),
            Some(ComparisonOpClass::NotEq) => Ok(PredicateOperator::NotEq),
            Some(ComparisonOpClass::Lt) => Ok(PredicateOperator::LessThan),
            Some(ComparisonOpClass::Le) => Ok(PredicateOperator::LessThanOrEq),
            Some(ComparisonOpClass::Gt) => Ok(PredicateOperator::GreaterThan),
            Some(ComparisonOpClass::Ge) => Ok(PredicateOperator::GreaterThanOrEq),
            None => Err(IcebergTranslationError::UnsupportedOperator {
                opno: op.opno,
                opcollid: op.opcollid,
                inputcollid: op.inputcollid,
            }),
        }
    }

    /// Mirror a binary operator for the `literal op column` operand order so it
    /// reads as `column op literal` (e.g. `7 < col` → `col > 7`).
    ///
    /// Translator-only assembly step used by [`Self::comparison`] when
    /// `swap_sides` holds: self-inverse on directional ops, identity on
    /// symmetric ones.
    fn mirror_operator(&self, op: PredicateOperator) -> PredicateOperator {
        match op {
            PredicateOperator::LessThan => PredicateOperator::GreaterThan,
            PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
            PredicateOperator::GreaterThan => PredicateOperator::LessThan,
            PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
            _ => op,
        }
    }

    /// Left-associative fold of predicates with `Predicate::and` or
    /// `Predicate::or`, erroring on an empty children list.
    ///
    /// Translator-only wrapper over the shared [`fold_left`] kernel: it owns
    /// the `and` / `or` empty-list semantics ([`IcebergTranslationError::EmptyBoolExpr`])
    /// that distinguish it from the scan provider's `combine_with_and`.
    fn fold_predicates(
        &self,
        items: Vec<Predicate>,
        and: bool,
    ) -> Result<Predicate, IcebergTranslationError> {
        let combine: fn(Predicate, Predicate) -> Predicate =
            if and { Predicate::and } else { Predicate::or };
        fold_left(items, combine).ok_or(IcebergTranslationError::EmptyBoolExpr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrx::pg_sys::Oid;

    const INT4_TYPE_OID: u32 = 23;

    fn map_comparison_operator(
        op: PgComparisonOp,
    ) -> Result<PredicateOperator, IcebergTranslationError> {
        IcebergPredicateTranslator::new().map_comparison_operator(op)
    }

    fn op_triple(opno: u32) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: Oid::INVALID,
        }
    }

    fn null_scalar(type_oid: u32) -> IcebergScalar {
        IcebergScalar::Null {
            type_oid: Oid::from(type_oid),
        }
    }

    #[test]
    fn maps_int4_operators() {
        assert_eq!(
            map_comparison_operator(op_triple(96)).unwrap(),
            PredicateOperator::Eq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(518)).unwrap(),
            PredicateOperator::NotEq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(97)).unwrap(),
            PredicateOperator::LessThan,
        );
        assert_eq!(
            map_comparison_operator(op_triple(523)).unwrap(),
            PredicateOperator::LessThanOrEq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(521)).unwrap(),
            PredicateOperator::GreaterThan,
        );
        assert_eq!(
            map_comparison_operator(op_triple(525)).unwrap(),
            PredicateOperator::GreaterThanOrEq,
        );
    }

    #[test]
    fn maps_int8_operators() {
        for opno in [410u32, 411, 412, 413, 414, 415] {
            assert!(
                map_comparison_operator(op_triple(opno)).is_ok(),
                "int8 opno {opno} must be in the consolidated op_class map",
            );
        }
    }

    #[test]
    fn maps_delegated_non_integer_operators() {
        assert_eq!(
            map_comparison_operator(op_triple(1754)).unwrap(),
            PredicateOperator::LessThan,
        );
        assert_eq!(
            map_comparison_operator(op_triple(1098)).unwrap(),
            PredicateOperator::GreaterThanOrEq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(98)).unwrap(),
            PredicateOperator::Eq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(674)).unwrap(),
            PredicateOperator::GreaterThan,
        );
    }

    #[test]
    fn rejects_unknown_operator() {
        assert!(matches!(
            map_comparison_operator(op_triple(558)),
            Err(IcebergTranslationError::UnsupportedOperator { .. })
        ));
    }

    #[test]
    fn map_comparison_operator_is_collation_agnostic() {
        let mut t = op_triple(96);
        t.inputcollid = Oid::from(100u32);
        assert_eq!(map_comparison_operator(t).unwrap(), PredicateOperator::Eq);

        let mut t = op_triple(96);
        t.opcollid = Oid::from(100u32);
        assert_eq!(map_comparison_operator(t).unwrap(), PredicateOperator::Eq);
    }

    #[test]
    fn is_null_with_null_scalar_fails_closed() {
        let mut t = IcebergPredicateTranslator::new();
        assert!(matches!(
            t.is_null(null_scalar(INT4_TYPE_OID)),
            Err(IcebergTranslationError::NullTestOnNonColumn)
        ));
    }

    #[test]
    fn is_not_null_with_null_scalar_fails_closed() {
        let mut t = IcebergPredicateTranslator::new();
        assert!(matches!(
            t.is_not_null(null_scalar(INT4_TYPE_OID)),
            Err(IcebergTranslationError::NullTestOnNonColumn)
        ));
    }
}

// =============================================================================
// Predicate-tree algebra tests: pure, no PG backend.
// =============================================================================
#[cfg(test)]
mod predicate_algebra_tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn mirror_operator_is_self_inverse_for_directional_ops() {
        let t = IcebergPredicateTranslator::new();
        for op in [
            PredicateOperator::LessThan,
            PredicateOperator::LessThanOrEq,
            PredicateOperator::GreaterThan,
            PredicateOperator::GreaterThanOrEq,
        ] {
            assert_eq!(t.mirror_operator(t.mirror_operator(op)), op);
        }
    }

    #[test]
    fn mirror_operator_is_identity_for_symmetric_ops() {
        let t = IcebergPredicateTranslator::new();
        for op in [
            PredicateOperator::Eq,
            PredicateOperator::NotEq,
            PredicateOperator::IsNull,
            PredicateOperator::NotNull,
        ] {
            assert_eq!(t.mirror_operator(op), op);
        }
    }

    #[test]
    fn mirror_operator_swaps_lt_and_gt() {
        let t = IcebergPredicateTranslator::new();
        assert_eq!(
            t.mirror_operator(PredicateOperator::LessThan),
            PredicateOperator::GreaterThan,
        );
        assert_eq!(
            t.mirror_operator(PredicateOperator::LessThanOrEq),
            PredicateOperator::GreaterThanOrEq,
        );
    }

    #[test]
    fn fold_predicates_handles_single_child() {
        let t = IcebergPredicateTranslator::new();
        let only = Reference::new("a").equal_to(Datum::int(1));
        let folded = t.fold_predicates(vec![only.clone()], true).unwrap();
        assert_eq!(folded, only);
    }

    #[test]
    fn fold_predicates_chains_and_left_assoc() {
        let t = IcebergPredicateTranslator::new();
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let c = Reference::new("c").equal_to(Datum::int(3));
        let folded = t
            .fold_predicates(vec![a.clone(), b.clone(), c.clone()], true)
            .unwrap();
        let expected = a.and(b).and(c);
        assert_eq!(folded, expected);
    }

    #[test]
    fn fold_predicates_chains_or() {
        let t = IcebergPredicateTranslator::new();
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let folded = t
            .fold_predicates(vec![a.clone(), b.clone()], false)
            .unwrap();
        let expected = a.or(b);
        assert_eq!(folded, expected);
    }

    #[test]
    fn fold_predicates_rejects_empty_input() {
        let t = IcebergPredicateTranslator::new();
        assert!(matches!(
            t.fold_predicates(vec![], true),
            Err(IcebergTranslationError::EmptyBoolExpr),
        ));
    }

    fn arb_leaf_predicate() -> impl Strategy<Value = Predicate> {
        prop_oneof![
            Just(Predicate::AlwaysTrue),
            Just(Predicate::AlwaysFalse),
            any::<i32>().prop_map(|v| Reference::new("a").equal_to(Datum::int(v))),
            any::<i64>().prop_map(|v| Reference::new("b").less_than(Datum::long(v))),
            any::<i32>()
                .prop_map(|v| Reference::new("c").greater_than(Datum::int(v))),
        ]
    }

    fn arb_predicate_tree() -> impl Strategy<Value = Predicate> {
        arb_leaf_predicate().prop_recursive(4, 16, 2, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(l, r)| l.and(r)),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| l.or(r)),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        })]

        #[test]
        fn prop_always_false_composes_through_and_or(x in arb_predicate_tree()) {
            prop_assert_eq!(
                Predicate::AlwaysFalse.and(x.clone()),
                Predicate::AlwaysFalse
            );
            prop_assert_eq!(
                x.clone().and(Predicate::AlwaysFalse),
                Predicate::AlwaysFalse
            );

            prop_assert_eq!(Predicate::AlwaysFalse.or(x.clone()), x.clone());
            prop_assert_eq!(x.clone().or(Predicate::AlwaysFalse), x.clone());
        }
    }
}
