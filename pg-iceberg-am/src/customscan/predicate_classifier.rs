//! Iceberg `classify_predicate` leaf classifier ([`IcebergPredicateClassifier`]).

use pg_lakebase_core::customscan::provider::PlanTranslateContext;
use pg_lakebase_core::expr::nodes::PgComparisonOp;
use pg_lakebase_core::expr::predicate::{PlanDynamicRef, PlanPredicate, PlanScalar};
use pg_lakebase_core::expr::split::{
    PushdownContract, PushdownCosting, QualPushdownDecision,
};
use pgrx::pg_sys;

use super::predicate_pushdown_policy::{
    PredicateCapability, PredicatePushdownPolicy,
};

/// Iceberg per-leaf classifier; delegates type/op verdict to
/// [`PredicatePushdownPolicy`].
#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergPredicateClassifier {
    pushdown: PredicatePushdownPolicy,
}

impl IcebergPredicateClassifier {
    pub const fn new(pushdown: PredicatePushdownPolicy) -> Self {
        Self { pushdown }
    }

    /// Classify a parsed leaf predicate (volatile/SubPlan already filtered upstream).
    pub fn classify_predicate(
        &self,
        _ctx: &PlanTranslateContext,
        predicate: &PlanPredicate<'_>,
    ) -> QualPushdownDecision {
        match predicate {
            PlanPredicate::Comparison { op, left, right } => {
                self.classify_comparison(*op, left, right)
            }
            PlanPredicate::IsNull { value } | PlanPredicate::IsNotNull { value } => {
                self.classify_null_test(value)
            }
        }
    }

    fn classify_comparison(
        &self,
        op: PgComparisonOp,
        left: &PlanScalar<'_>,
        right: &PlanScalar<'_>,
    ) -> QualPushdownDecision {
        let shape = ComparisonShape::from_operands(left, right);
        if !shape.is_pushable() {
            return QualPushdownDecision::Unsupported;
        }

        let Some(col_type) = left.column_type().or_else(|| right.column_type())
        else {
            return QualPushdownDecision::Unsupported;
        };

        match self.pushdown.capability_for(col_type, op) {
            PredicateCapability::ExactRowFilter => QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            },
            PredicateCapability::ConservativePruning => {
                QualPushdownDecision::Pushable {
                    contract: PushdownContract::ConservativePruning,
                    costing: self.conservative_pruning_costing(col_type, op, shape),
                }
            }
            PredicateCapability::Unsupported => QualPushdownDecision::Unsupported,
        }
    }

    /// Classify `IS NULL` / `IS NOT NULL` on a scan-column operand.
    fn classify_null_test(&self, value: &PlanScalar<'_>) -> QualPushdownDecision {
        let Some(col_type) = value.column_type() else {
            // Only scan-column null-tests are pushable (not literals/params).
            return QualPushdownDecision::Unsupported;
        };

        match self.pushdown.null_test_capability(col_type) {
            PredicateCapability::ExactRowFilter => QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            },
            PredicateCapability::ConservativePruning => {
                QualPushdownDecision::Pushable {
                    contract: PushdownContract::ConservativePruning,
                    costing: PushdownCosting::UncostedBestEffort,
                }
            }
            PredicateCapability::Unsupported => QualPushdownDecision::Unsupported,
        }
    }

    fn conservative_pruning_costing(
        &self,
        col_type: pg_sys::Oid,
        op: PgComparisonOp,
        shape: ComparisonShape,
    ) -> PushdownCosting {
        // PlanLiteralRef has no Datum at plan time, so numeric/date/timestamp
        // const literals cannot be distinguished from NaN/infinity. All
        // ConservativePruning on these types is UncostedBestEffort even for
        // finite values like `date = '2024-01-01'` until plan-time literal
        // inspection exists.
        if shape.has_param_or_outer()
            || self.pushdown.is_value_sensitive_type(col_type)
        {
            return PushdownCosting::UncostedBestEffort;
        }

        if self.pushdown.can_build(col_type, op) {
            PushdownCosting::CostedPruning
        } else {
            PushdownCosting::UncostedBestEffort
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ComparisonShape {
    lhs: OperandKind,
    rhs: OperandKind,
}

impl ComparisonShape {
    fn from_operands(left: &PlanScalar<'_>, right: &PlanScalar<'_>) -> Self {
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
    fn from_scalar(scalar: &PlanScalar<'_>) -> Self {
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
