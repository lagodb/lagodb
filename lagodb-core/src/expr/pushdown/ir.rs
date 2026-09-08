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

/// Complete provider-neutral predicate tree. PostgreSQL planning and concrete
/// query execution use different scalar/operator specializations of this one
/// logical structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateExpr<S = ScalarExpr, O = PgComparisonOp> {
    AlwaysTrue,
    AlwaysFalse,
    /// TRUE for every non-NULL input and UNKNOWN for NULL.
    StrictTrue(S),
    /// FALSE for every non-NULL input and UNKNOWN for NULL.
    StrictFalse(S),
    Comparison {
        operator: O,
        left: S,
        right: S,
    },
    IsNull(S),
    IsNotNull(S),
    IsNan(S),
    IsNotNan(S),
    StartsWith {
        value: S,
        prefix: S,
    },
    And(Box<[PredicateExpr<S, O>]>),
    Or(Box<[PredicateExpr<S, O>]>),
    Not(Box<PredicateExpr<S, O>>),
}

impl<S, O> PredicateExpr<S, O> {
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::AlwaysFalse, _) | (_, Self::AlwaysFalse) => Self::AlwaysFalse,
            (Self::AlwaysTrue, predicate) | (predicate, Self::AlwaysTrue) => {
                predicate
            }
            (left, right) => Self::And(vec![left, right].into_boxed_slice()),
        }
    }

    pub fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::AlwaysTrue, _) | (_, Self::AlwaysTrue) => Self::AlwaysTrue,
            (Self::AlwaysFalse, predicate) | (predicate, Self::AlwaysFalse) => {
                predicate
            }
            (left, right) => Self::Or(vec![left, right].into_boxed_slice()),
        }
    }
}

impl PredicateExpr<ScalarExpr, PgComparisonOp> {
    pub fn rebase_runtime_values(&self, offset: usize) -> Self {
        match self {
            Self::AlwaysTrue => Self::AlwaysTrue,
            Self::AlwaysFalse => Self::AlwaysFalse,
            Self::StrictTrue(value) => {
                Self::StrictTrue(value.rebase_runtime_values(offset))
            }
            Self::StrictFalse(value) => {
                Self::StrictFalse(value.rebase_runtime_values(offset))
            }
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
            Self::IsNan(value) => Self::IsNan(value.rebase_runtime_values(offset)),
            Self::IsNotNan(value) => {
                Self::IsNotNan(value.rebase_runtime_values(offset))
            }
            Self::StartsWith { value, prefix } => Self::StartsWith {
                value: value.rebase_runtime_values(offset),
                prefix: prefix.rebase_runtime_values(offset),
            },
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
