//! Runtime [`IcebergPredicateTranslator`] (PG expr → iceberg [`Predicate`]).
//!
//! It owns the scalar model and predicate-tree assembly, while the contained
//! Datum module isolates PostgreSQL's unsafe scalar decoding boundary.

mod datum;
mod predicate_algebra;

pub(crate) use datum::decode_datum;

use std::rc::Rc;

use iceberg_lite::expr::{
    BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression,
};
use iceberg_lite::spec::Datum;
use pg_lakebase_core::expr::ColumnNameResolver;
use pg_lakebase_core::expr::translator::{
    PgColumnRef, PgLiteral, PgParamValue, PgPredicateTranslator,
};
use pg_lakebase_core::expr::{PgComparisonIdentity, PgComparisonOp};
use pgrx::pg_sys;

use crate::relation_binding::RelationFieldIndex;

use super::policy::{
    ComparisonOpClass, PgPredicatePushdownPolicy, PredicatePushdownPolicy,
};

// =============================================================================
// Scalar leaf model
//
// Pure model layer: carries no dependency on the error type below. Error
// mapping (e.g. rejecting a non-column operand) lives in the translator's
// methods, never in the model.
// =============================================================================

/// Runtime scalar leaf: column, non-null literal/param, or NULL operand.
#[derive(Debug, Clone)]
pub(crate) enum IcebergScalar {
    Column {
        reference: Reference,
        atttypid: pg_sys::Oid,
    },
    Datum(Datum),
    /// NULL literal or NULL-resolved param; strict comparisons fold to `AlwaysFalse`.
    Null {
        #[cfg_attr(
            not(feature = "pg_test"),
            allow(
                dead_code,
                reason = "the type OID is retained for diagnostics and decode-contract tests"
            )
        )]
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
pub(crate) enum ScalarKind {
    Column,
    Datum,
    Null,
}

// =============================================================================
// Error type
// =============================================================================

/// Errors surfaced by the Iceberg runtime predicate translator.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum IcebergTranslationError {
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
// Translator
// =============================================================================

/// Runtime [`PgPredicateTranslator`] for Iceberg: column refs, datum decode, predicate assembly.
#[derive(Debug)]
pub(crate) struct IcebergPredicateTranslator {
    field_index: Option<Rc<RelationFieldIndex>>,
}

impl IcebergPredicateTranslator {
    /// Build the legacy name-binding translator for tests that exercise the
    /// scalar/predicate algebra without a real relation schema.
    ///
    /// Production CustomScan paths must use [`Self::with_field_index`] so
    /// column identity is the Iceberg field id resolved by `RelationFieldMap`,
    /// not a late `attno -> name` lookup.
    #[cfg(any(test, feature = "pg_test"))]
    pub(crate) fn new_unbound_for_tests() -> Self {
        Self { field_index: None }
    }

    pub(crate) fn with_field_index(field_index: Rc<RelationFieldIndex>) -> Self {
        Self {
            field_index: Some(field_index),
        }
    }
}

impl PgPredicateTranslator for IcebergPredicateTranslator {
    type Scalar = IcebergScalar;
    type Predicate = Predicate;
    type Error = IcebergTranslationError;

    fn column(&mut self, col: PgColumnRef<'_>) -> Result<Self::Scalar, Self::Error> {
        let reference = if let Some(field_index) = self.field_index.as_ref() {
            if col.attno <= 0 {
                return Err(IcebergTranslationError::SystemOrWholeRowColumn {
                    rel_oid: col.rel_oid,
                    attno: col.attno,
                });
            }
            let binding = field_index.binding_for_attno(col.attno).ok_or(
                IcebergTranslationError::ColumnLookupFailed {
                    rel_oid: col.rel_oid,
                    attno: col.attno,
                },
            )?;
            Reference::new_bound_field(binding.debug_name.clone(), binding.field_id)
        } else {
            let name = Self::resolve_column_name(col.name, col.rel_oid, col.attno)?;
            Reference::new(name)
        };
        Ok(IcebergScalar::Column {
            reference,
            atttypid: col.atttypid,
        })
    }

    /// NULL literals decode to [`IcebergScalar::Null`]; [`Self::comparison`] folds them to `AlwaysFalse`.
    fn literal(&mut self, lit: PgLiteral<'_>) -> Result<Self::Scalar, Self::Error> {
        if lit.is_null() {
            return Ok(IcebergScalar::Null {
                type_oid: lit.type_oid(),
            });
        }
        // SAFETY: PgLiteral carries Const.consttype alongside Const.constvalue;
        // the NULL branch returned above, and its PG memory context is tied to
        // the literal borrow for the duration of this call.
        let datum = unsafe { decode_datum(lit.type_oid(), lit.datum().as_raw()) }?;
        Ok(IcebergScalar::Datum(datum))
    }

    /// Mirrors [`Self::literal`]: NULL params decode to [`IcebergScalar::Null`], not an error.
    fn param_value(
        &mut self,
        param: PgParamValue<'_>,
    ) -> Result<Self::Scalar, Self::Error> {
        if param.is_null() {
            return Ok(IcebergScalar::Null {
                type_oid: param.type_oid(),
            });
        }
        // SAFETY: PgParamValue carries the resolved parameter type alongside
        // its Datum; the NULL branch returned above, and the executor-owned
        // parameter storage remains live for this translation call.
        let datum =
            unsafe { decode_datum(param.type_oid(), param.datum().as_raw()) }?;
        Ok(IcebergScalar::Datum(datum))
    }

    fn comparison(
        &mut self,
        op: PgComparisonOp,
        left: Self::Scalar,
        right: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error> {
        self.comparison_with(op, left, right, PgPredicatePushdownPolicy::can_build)
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
    fn comparison_with(
        &self,
        op: PgComparisonOp,
        left: IcebergScalar,
        right: IcebergScalar,
        can_build: impl Fn(pg_sys::Oid, PgComparisonIdentity) -> bool,
    ) -> Result<Predicate, IcebergTranslationError> {
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

        if !can_build(atttypid, op.identity()) {
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

        if !PredicatePushdownPolicy::supports_null_test(atttypid) {
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
        match PredicatePushdownPolicy::op_class(op.opno) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrx::pg_sys::Oid;

    const INT4_TYPE_OID: u32 = 23;

    fn map_comparison_operator(
        op: PgComparisonOp,
    ) -> Result<PredicateOperator, IcebergTranslationError> {
        IcebergPredicateTranslator::new_unbound_for_tests()
            .map_comparison_operator(op)
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

    fn column_scalar(name: &str, type_oid: u32) -> IcebergScalar {
        IcebergScalar::Column {
            reference: Reference::new(name),
            atttypid: Oid::from(type_oid),
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
    fn maps_supported_non_integer_operators() {
        assert_eq!(
            map_comparison_operator(op_triple(1098)).unwrap(),
            PredicateOperator::GreaterThanOrEq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(98)).unwrap(),
            PredicateOperator::Eq,
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
    fn strict_comparisons_with_null_fold_before_policy_resolution() {
        for opno in [96, 518, 97, 523, 521, 525, 410, 411, 412, 413, 414, 415] {
            let type_oid = if (410..=415).contains(&opno) {
                20
            } else {
                INT4_TYPE_OID
            };
            for null_on_left in [false, true] {
                let (left, right) = if null_on_left {
                    (null_scalar(type_oid), column_scalar("id", type_oid))
                } else {
                    (column_scalar("id", type_oid), null_scalar(type_oid))
                };
                assert_eq!(
                    IcebergPredicateTranslator::new_unbound_for_tests()
                        .comparison_with(op_triple(opno), left, right, |_, _| {
                            panic!("NULL folding must precede policy resolution")
                        }),
                    Ok(Predicate::AlwaysFalse),
                    "opno {opno}, null_on_left={null_on_left}",
                );
            }
        }

        assert_eq!(
            IcebergPredicateTranslator::new_unbound_for_tests().comparison_with(
                op_triple(96),
                null_scalar(INT4_TYPE_OID),
                null_scalar(20),
                |_, _| panic!("NULL folding must precede policy resolution"),
            ),
            Ok(Predicate::AlwaysFalse),
        );
    }

    #[test]
    fn is_null_with_null_scalar_fails_closed() {
        let mut t = IcebergPredicateTranslator::new_unbound_for_tests();
        assert!(matches!(
            t.is_null(null_scalar(INT4_TYPE_OID)),
            Err(IcebergTranslationError::NullTestOnNonColumn)
        ));
    }

    #[test]
    fn is_not_null_with_null_scalar_fails_closed() {
        let mut t = IcebergPredicateTranslator::new_unbound_for_tests();
        assert!(matches!(
            t.is_not_null(null_scalar(INT4_TYPE_OID)),
            Err(IcebergTranslationError::NullTestOnNonColumn)
        ));
    }
}
