//! Plan-stage Iceberg predicate classifier ([`IcebergPredicateClassifier`]).

use pg_lakebase_core::expr::predicate::{PlanDynamicRef, PlanPredicate, PlanScalar};
use pg_lakebase_core::expr::{
    PgComparisonOp, PushdownContract, PushdownCosting, QualPushdownDecision,
};
use pgrx::pg_sys;

use super::policy::{PredicateCapability, PredicatePushdownPolicy};

/// Iceberg per-leaf classifier; delegates type/op verdict to
/// [`PredicatePushdownPolicy`].
pub(crate) struct IcebergPredicateClassifier;

impl IcebergPredicateClassifier {
    /// Classify a parsed leaf predicate for this classifier's purpose.
    pub(crate) fn classify(&self, predicate: &PlanPredicate) -> QualPushdownDecision {
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
        left: &PlanScalar,
        right: &PlanScalar,
    ) -> QualPushdownDecision {
        let shape = ComparisonShape::from_operands(left, right);
        if !shape.is_pushable() {
            return QualPushdownDecision::Unsupported;
        }

        let Some(col_type) = left.column_type().or_else(|| right.column_type())
        else {
            return QualPushdownDecision::Unsupported;
        };

        match PredicatePushdownPolicy::capability_for(col_type, op.identity()) {
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
        op: PgComparisonOp,
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

        if PredicatePushdownPolicy::can_build(col_type, op.identity()) {
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

    #[test]
    fn scan_pushdown_accepts_runtime_bound_operands() {
        for dynamic in [OperandKind::SupportedParam, OperandKind::OuterColumn] {
            for shape in [
                ComparisonShape {
                    lhs: OperandKind::ScanColumn,
                    rhs: dynamic,
                },
                ComparisonShape {
                    lhs: dynamic,
                    rhs: OperandKind::ScanColumn,
                },
            ] {
                assert!(shape.is_pushable());
            }
        }
    }
}
