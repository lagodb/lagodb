//! Plan-stage Iceberg predicate classifier ([`IcebergPredicateClassifier`]).

use pg_lakebase_core::expr::predicate::{PlanDynamicRef, PlanPredicate, PlanScalar};
use pg_lakebase_core::expr::{
    PgComparisonIdentity, PgComparisonOp, PushdownContract, PushdownCosting,
    QualPushdownDecision,
};
use pgrx::pg_sys;

use super::policy::{
    PgPredicatePushdownPolicy, PredicateCapability, PredicatePushdownPolicy,
};

/// Iceberg per-leaf classifier; delegates type/op verdict to
/// [`PredicatePushdownPolicy`].
pub(crate) struct IcebergPredicateClassifier;

impl IcebergPredicateClassifier {
    /// Classify a parsed leaf predicate for this classifier's purpose.
    pub(crate) fn classify(&self, predicate: &PlanPredicate) -> QualPushdownDecision {
        self.classify_with(predicate, PgPredicatePushdownPolicy::capability_for)
    }

    /// Classify using an already-selected capability source. Keeping shape and
    /// costing independent from catalog access makes the complete decision
    /// table host-testable while [`Self::classify`] remains the PG adapter.
    fn classify_with<F>(
        &self,
        predicate: &PlanPredicate,
        capability_for: F,
    ) -> QualPushdownDecision
    where
        F: Fn(pg_sys::Oid, PgComparisonIdentity) -> PredicateCapability,
    {
        match predicate {
            PlanPredicate::Comparison { op, left, right } => {
                self.classify_comparison(*op, left, right, capability_for)
            }
            PlanPredicate::IsNull { value } | PlanPredicate::IsNotNull { value } => {
                self.classify_null_test(value)
            }
        }
    }

    fn classify_comparison(
        &self,
        op: PgComparisonOp,
        left: &PlanScalar,
        right: &PlanScalar,
        capability_for: impl Fn(pg_sys::Oid, PgComparisonIdentity) -> PredicateCapability,
    ) -> QualPushdownDecision {
        let shape = ComparisonShape::from_operands(left, right);
        if !shape.is_pushable() {
            return QualPushdownDecision::Unsupported;
        }

        let Some(col_type) = left.column_type().or_else(|| right.column_type())
        else {
            return QualPushdownDecision::Unsupported;
        };

        match capability_for(col_type, op.identity()) {
            PredicateCapability::ExactRowFilter => QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            },
            PredicateCapability::ConservativePruning => {
                QualPushdownDecision::Pushable {
                    contract: PushdownContract::ConservativePruning,
                    costing: self.conservative_pruning_costing(col_type, shape),
                }
            }
            PredicateCapability::Unsupported => QualPushdownDecision::Unsupported,
        }
    }

    /// Classify `IS NULL` / `IS NOT NULL` on a scan-column operand.
    fn classify_null_test(&self, value: &PlanScalar) -> QualPushdownDecision {
        let Some(col_type) = value.column_type() else {
            // Only scan-column null-tests are pushable (not literals/params).
            return QualPushdownDecision::Unsupported;
        };

        if PredicatePushdownPolicy::supports_null_test(col_type) {
            QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            }
        } else {
            QualPushdownDecision::Unsupported
        }
    }

    fn conservative_pruning_costing(
        &self,
        col_type: pg_sys::Oid,
        shape: ComparisonShape,
    ) -> PushdownCosting {
        // PlanLiteralRef has no Datum at plan time, so date/timestamp
        // const literals cannot be distinguished from NaN/infinity. All
        // ConservativePruning on these types is UncostedBestEffort even for
        // finite values like `date = '2024-01-01'` until plan-time literal
        // inspection exists.
        if shape.has_param_or_outer()
            || PredicatePushdownPolicy::is_value_sensitive_type(col_type)
        {
            return PushdownCosting::UncostedBestEffort;
        }

        // This method is reached only after policy classified the predicate as
        // ConservativePruning, so no second capability/syscache lookup is
        // needed for a non-runtime-bound, value-insensitive predicate.
        PushdownCosting::CostedPruning
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ComparisonShape {
    lhs: OperandKind,
    rhs: OperandKind,
}

impl ComparisonShape {
    fn from_operands(left: &PlanScalar, right: &PlanScalar) -> Self {
        Self {
            lhs: OperandKind::from_scalar(left),
            rhs: OperandKind::from_scalar(right),
        }
    }

    fn is_pushable(self) -> bool {
        matches!(
            (self.lhs, self.rhs),
            (OperandKind::ScanColumn, OperandKind::ConstLiteral)
                | (OperandKind::ScanColumn, OperandKind::SupportedParam)
                | (OperandKind::ScanColumn, OperandKind::OuterColumn)
                | (OperandKind::ConstLiteral, OperandKind::ScanColumn)
                | (OperandKind::SupportedParam, OperandKind::ScanColumn)
                | (OperandKind::OuterColumn, OperandKind::ScanColumn)
        )
    }

    fn has_param_or_outer(self) -> bool {
        self.lhs.is_runtime_bound() || self.rhs.is_runtime_bound()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperandKind {
    ScanColumn,
    OuterColumn,
    ConstLiteral,
    SupportedParam,
    Other,
}

impl OperandKind {
    fn from_scalar(scalar: &PlanScalar) -> Self {
        match scalar {
            PlanScalar::Column(col) => {
                if col.attno > 0 {
                    Self::ScanColumn
                } else {
                    Self::Other
                }
            }
            PlanScalar::Literal(_) => Self::ConstLiteral,
            PlanScalar::Dynamic(PlanDynamicRef::Param(p)) => {
                if p.key.paramkind == pg_sys::ParamKind::PARAM_EXTERN
                    || p.key.paramkind == pg_sys::ParamKind::PARAM_EXEC
                {
                    Self::SupportedParam
                } else {
                    Self::Other
                }
            }
            PlanScalar::Dynamic(PlanDynamicRef::OuterVar(ov)) => {
                if ov.attno > 0 {
                    Self::OuterColumn
                } else {
                    Self::Other
                }
            }
        }
    }

    fn is_runtime_bound(self) -> bool {
        matches!(self, Self::SupportedParam | Self::OuterColumn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_lakebase_core::expr::ParamKey;
    use pg_lakebase_core::expr::predicate::{
        PlanColumnRef, PlanLiteralRef, PlanOuterVarRef, PlanParamRef,
    };

    #[derive(Clone, Copy, Debug)]
    enum OperandCase {
        ScanColumn,
        Literal,
        ParamExtern,
        ParamExec,
        OuterColumn,
        ScanSystemColumn,
        ScanWholeRow,
        OuterSystemColumn,
        OuterWholeRow,
        ParamSublink,
    }

    impl OperandCase {
        const ALL: [Self; 10] = [
            Self::ScanColumn,
            Self::Literal,
            Self::ParamExtern,
            Self::ParamExec,
            Self::OuterColumn,
            Self::ScanSystemColumn,
            Self::ScanWholeRow,
            Self::OuterSystemColumn,
            Self::OuterWholeRow,
            Self::ParamSublink,
        ];

        fn scalar(self, type_oid: pg_sys::Oid) -> PlanScalar {
            match self {
                Self::ScanColumn | Self::ScanSystemColumn | Self::ScanWholeRow => {
                    let attno = match self {
                        Self::ScanColumn => 1,
                        Self::ScanSystemColumn => -1,
                        Self::ScanWholeRow => 0,
                        _ => unreachable!(),
                    };
                    PlanScalar::Column(PlanColumnRef {
                        rel_oid: pg_sys::Oid::INVALID,
                        attno,
                        atttypid: type_oid,
                        attcollation: pg_sys::Oid::INVALID,
                    })
                }
                Self::Literal => PlanScalar::Literal(PlanLiteralRef {
                    consttypid: type_oid,
                    constcollid: pg_sys::Oid::INVALID,
                    is_null: false,
                }),
                Self::ParamExtern | Self::ParamExec | Self::ParamSublink => {
                    let paramkind = match self {
                        Self::ParamExtern => pg_sys::ParamKind::PARAM_EXTERN,
                        Self::ParamExec => pg_sys::ParamKind::PARAM_EXEC,
                        Self::ParamSublink => pg_sys::ParamKind::PARAM_SUBLINK,
                        _ => unreachable!(),
                    };
                    PlanScalar::Dynamic(PlanDynamicRef::Param(PlanParamRef {
                        key: ParamKey {
                            paramkind,
                            param_id: 1,
                        },
                        paramtype: type_oid,
                        paramcollid: pg_sys::Oid::INVALID,
                    }))
                }
                Self::OuterColumn | Self::OuterSystemColumn | Self::OuterWholeRow => {
                    let attno = match self {
                        Self::OuterColumn => 1,
                        Self::OuterSystemColumn => -1,
                        Self::OuterWholeRow => 0,
                        _ => unreachable!(),
                    };
                    PlanScalar::Dynamic(PlanDynamicRef::OuterVar(PlanOuterVarRef {
                        varno: 2,
                        attno,
                        atttypid: type_oid,
                        attcollation: pg_sys::Oid::INVALID,
                    }))
                }
            }
        }

        fn accepts_with_column(self) -> bool {
            matches!(
                self,
                Self::Literal
                    | Self::ParamExtern
                    | Self::ParamExec
                    | Self::OuterColumn
            )
        }

        fn is_scan_column(self) -> bool {
            matches!(self, Self::ScanColumn)
        }

        fn is_runtime_bound(self) -> bool {
            matches!(
                self,
                Self::ParamExtern | Self::ParamExec | Self::OuterColumn
            )
        }
    }

    fn comparison(opno: u32, left: PlanScalar, right: PlanScalar) -> PlanPredicate {
        PlanPredicate::Comparison {
            op: PgComparisonOp {
                opno: pg_sys::Oid::from(opno),
                opfuncid: pg_sys::Oid::INVALID,
                opresulttype: pg_sys::BOOLOID,
                opcollid: pg_sys::Oid::INVALID,
                inputcollid: pg_sys::Oid::INVALID,
            },
            left,
            right,
        }
    }

    #[test]
    fn comparison_shape_and_capability_matrix_is_exhaustive() {
        let classifier = IcebergPredicateClassifier;
        for (capability, type_oid, opno) in [
            (PredicateCapability::ExactRowFilter, pg_sys::INT4OID, 96),
            (
                PredicateCapability::ConservativePruning,
                pg_sys::TEXTOID,
                98,
            ),
            (PredicateCapability::Unsupported, pg_sys::NUMERICOID, 1752),
        ] {
            for left in OperandCase::ALL {
                for right in OperandCase::ALL {
                    let predicate = comparison(
                        opno,
                        left.scalar(type_oid),
                        right.scalar(type_oid),
                    );
                    let got = classifier.classify_with(&predicate, |_, _| capability);
                    let pushable_shape = (left.accepts_with_column()
                        && right.is_scan_column())
                        || (right.accepts_with_column() && left.is_scan_column());
                    let expected = match (pushable_shape, capability) {
                        (true, PredicateCapability::ExactRowFilter) => {
                            QualPushdownDecision::Pushable {
                                contract: PushdownContract::ExactRowFilter,
                                costing: PushdownCosting::CostedPruning,
                            }
                        }
                        (true, PredicateCapability::ConservativePruning) => {
                            QualPushdownDecision::Pushable {
                                contract: PushdownContract::ConservativePruning,
                                costing: if left.is_runtime_bound()
                                    || right.is_runtime_bound()
                                {
                                    PushdownCosting::UncostedBestEffort
                                } else {
                                    PushdownCosting::CostedPruning
                                },
                            }
                        }
                        _ => QualPushdownDecision::Unsupported,
                    };
                    assert_eq!(
                        got, expected,
                        "left={left:?}, right={right:?}, capability={capability:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn value_sensitive_conservative_literal_is_uncosted() {
        let predicate = comparison(
            1093,
            OperandCase::ScanColumn.scalar(pg_sys::DATEOID),
            OperandCase::Literal.scalar(pg_sys::DATEOID),
        );
        assert_eq!(
            IcebergPredicateClassifier.classify_with(&predicate, |_, _| {
                PredicateCapability::ConservativePruning
            }),
            QualPushdownDecision::Pushable {
                contract: PushdownContract::ConservativePruning,
                costing: PushdownCosting::UncostedBestEffort,
            },
        );
    }

    #[test]
    fn null_test_uses_only_column_shape_and_supported_type() {
        for (value, expected) in [
            (OperandCase::ScanColumn.scalar(pg_sys::INT4OID), true),
            (OperandCase::ScanColumn.scalar(pg_sys::FLOAT8OID), true),
            (OperandCase::ScanColumn.scalar(pg_sys::BOOLOID), false),
            (OperandCase::Literal.scalar(pg_sys::INT4OID), false),
            (OperandCase::ParamExtern.scalar(pg_sys::INT4OID), false),
        ] {
            let predicate = PlanPredicate::IsNull { value };
            assert_eq!(
                IcebergPredicateClassifier
                    .classify_with(&predicate, |_, _| unreachable!()),
                if expected {
                    QualPushdownDecision::Pushable {
                        contract: PushdownContract::ExactRowFilter,
                        costing: PushdownCosting::CostedPruning,
                    }
                } else {
                    QualPushdownDecision::Unsupported
                },
            );
        }
    }
}
