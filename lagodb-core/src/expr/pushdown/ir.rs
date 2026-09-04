//! Provider-neutral, owned predicate representation constructed at plan time.

use crate::expr::contract::PgComparisonOp;
use crate::expr::{ColumnRef, RuntimeValueId, RuntimeValueLayout, RuntimeValueSpec};

/// Scalar operand in a filter node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarExpr {
    Column(ColumnRef),
    Value(RuntimeValueId),
}

impl ScalarExpr {
    pub fn rebase_runtime_values(&self, offset: usize) -> Self {
        match self {
            Self::Column(column) => Self::Column(*column),
            Self::Value(value) => {
                Self::Value(RuntimeValueId::from_index(value.index() + offset))
            }
        }
    }
}

/// Complete provider-neutral predicate tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateExpr {
    Comparison {
        operator: PgComparisonOp,
        left: ScalarExpr,
        right: ScalarExpr,
    },
    IsNull(ScalarExpr),
    IsNotNull(ScalarExpr),
    And(Box<[PredicateExpr]>),
    Or(Box<[PredicateExpr]>),
    Not(Box<PredicateExpr>),
}

impl PredicateExpr {
    pub fn rebase_runtime_values(&self, offset: usize) -> Self {
        match self {
            Self::Comparison {
                operator,
                left,
                right,
            } => Self::Comparison {
                operator: *operator,
                left: left.rebase_runtime_values(offset),
                right: right.rebase_runtime_values(offset),
            },
            Self::IsNull(value) => Self::IsNull(value.rebase_runtime_values(offset)),
            Self::IsNotNull(value) => {
                Self::IsNotNull(value.rebase_runtime_values(offset))
            }
            Self::And(children) => Self::And(
                children
                    .iter()
                    .map(|child| child.rebase_runtime_values(offset))
                    .collect(),
            ),
            Self::Or(children) => Self::Or(
                children
                    .iter()
                    .map(|child| child.rebase_runtime_values(offset))
                    .collect(),
            ),
            Self::Not(child) => {
                Self::Not(Box::new(child.rebase_runtime_values(offset)))
            }
        }
    }
}

/// Owned provider-neutral filter tree and its local value-slot table.
///
/// Production fragments originate from PostgreSQL-normalized expressions.
/// PostgreSQL has already bound each comparison operator to its coerced operand
/// types; downstream provider policy decides support without revalidating that
/// catalog invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateFragment {
    root: PredicateExpr,
    runtime_values: RuntimeValueLayout,
}

impl PredicateFragment {
    pub(crate) fn new(root: PredicateExpr, values: Vec<RuntimeValueSpec>) -> Self {
        Self {
            root,
            runtime_values: RuntimeValueLayout::new(values.into_boxed_slice()),
        }
    }

    pub(crate) fn from_layout(
        root: PredicateExpr,
        runtime_values: RuntimeValueLayout,
    ) -> Self {
        Self {
            root,
            runtime_values,
        }
    }

    #[inline]
    pub fn root(&self) -> &PredicateExpr {
        &self.root
    }

    #[inline]
    pub fn values(&self) -> &[RuntimeValueSpec] {
        self.runtime_values.values()
    }

    #[inline]
    pub fn runtime_values(&self) -> &RuntimeValueLayout {
        &self.runtime_values
    }

    #[inline]
    pub fn value(&self, id: RuntimeValueId) -> &RuntimeValueSpec {
        &self.runtime_values.values()[id.index()]
    }

    pub fn into_parts(self) -> (PredicateExpr, RuntimeValueLayout) {
        (self.root, self.runtime_values)
    }
}
