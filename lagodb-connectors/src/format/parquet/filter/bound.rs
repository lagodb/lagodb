//! File-independent bound predicate trees.
//!
//! Binding carries negation to the leaves, so a bound tree contains no NOT.
//! Without negation, a leaf's UNKNOWN and FALSE select the same rows, which
//! lets per-file evaluation return non-null masks for null-safe leaves.

use lagodb_core::expr::BooleanTestKind;
use lagodb_core::expr::pushdown::PredicatePlan;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ConnectorError;

use super::lower::{FileNode, FileSchema};
use super::operator::ComparisonOperator;
use super::value::BoundValue;

/// A predicate tree whose leaves hold values of type `V`: logical values
/// after binding, or one file's physical values after lowering. Bound trees
/// travel to parallel query-offload workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum ParquetNode<V> {
    AlwaysTrue,
    NeverTrue,
    Comparison {
        operator: ComparisonOperator,
        column: Box<str>,
        value: V,
    },
    IsNull(Box<str>),
    IsNotNull(Box<str>),
    IsNan(Box<str>),
    /// Non-NULL and not NaN.
    IsNotNan(Box<str>),
    BooleanTest {
        #[serde(with = "boolean_test_kind")]
        kind: BooleanTestKind,
        column: Box<str>,
    },
    StartsWith {
        column: Box<str>,
        prefix: V,
    },
    /// Equality with any listed value. Sets prune row groups only; the row
    /// filter projects them out (see [`ParquetNode::row_filter`]).
    InSet {
        column: Box<str>,
        values: Box<[V]>,
    },
    And(Box<[Self]>),
    Or(Box<[Self]>),
}

pub(super) type BoundNode = ParquetNode<BoundValue>;

impl<V> ParquetNode<V> {
    pub(super) fn contains_in_set(&self) -> bool {
        match self {
            Self::InSet { .. } => true,
            Self::And(children) | Self::Or(children) => {
                children.iter().any(Self::contains_in_set)
            }
            _ => false,
        }
    }

    /// The tree the reader evaluates on each row: sets only prune row
    /// groups, so this keeps what the shared AND-widening and complete-OR
    /// rules keep without them. Planning derives the same projection for its
    /// `FilterEvaluation` (`PlannedNode::evaluation`).
    pub(super) fn row_filter(&self) -> PredicatePlan<Self>
    where
        V: Clone,
    {
        match self {
            Self::InSet { .. } => PredicatePlan::Unsupported,
            Self::And(children) => PredicatePlan::conjunction(
                children.iter().map(Self::row_filter),
                |children| Self::And(children.into_boxed_slice()),
            ),
            Self::Or(children) => PredicatePlan::disjunction(
                children.iter().map(Self::row_filter),
                |children| Self::Or(children.into_boxed_slice()),
            ),
            node => PredicatePlan::Exact(node.clone()),
        }
    }
}

impl BoundNode {
    /// The complement of an Exact tree. Every Exact leaf of the runtime and
    /// FDW planners has a complement preserving SQL NULL semantics; sets and
    /// prefixes are never Exact, so they are never negated.
    pub(super) fn negated(self) -> Self {
        match self {
            Self::AlwaysTrue => Self::NeverTrue,
            Self::NeverTrue => Self::AlwaysTrue,
            Self::Comparison {
                operator,
                column,
                value,
            } => Self::Comparison {
                operator: operator.negated(),
                column,
                value,
            },
            Self::IsNull(column) => Self::IsNotNull(column),
            Self::IsNotNull(column) => Self::IsNull(column),
            Self::IsNan(column) => Self::IsNotNan(column),
            Self::IsNotNan(column) => Self::IsNan(column),
            Self::BooleanTest { kind, column } => Self::BooleanTest {
                kind: kind.negated(),
                column,
            },
            Self::And(children) => Self::Or(Self::negate_children(children)),
            Self::Or(children) => Self::And(Self::negate_children(children)),
            Self::StartsWith { .. } | Self::InSet { .. } => {
                unreachable!("only Exact predicate plans are negated")
            }
        }
    }

    fn negate_children(children: Box<[Self]>) -> Box<[Self]> {
        children.into_vec().into_iter().map(Self::negated).collect()
    }
}

/// What the Arrow reader evaluates for one bound predicate.
#[derive(Clone)]
enum RowFilter {
    /// The pruning tree as a whole; it holds no set.
    Whole,
    /// The tree with its sets projected out.
    Projected(BoundNode),
    /// Nothing safe remains once its sets are projected out.
    Skipped,
}

/// One bound predicate: a pruning tree and the row filter derived from it.
#[derive(Clone)]
pub(crate) struct ParquetBoundPredicate {
    pruning: BoundNode,
    row_filter: RowFilter,
}

impl ParquetBoundPredicate {
    pub(super) fn new(pruning: BoundNode) -> Self {
        let row_filter = if pruning.contains_in_set() {
            pruning
                .row_filter()
                .into_predicate()
                .map_or(RowFilter::Skipped, RowFilter::Projected)
        } else {
            RowFilter::Whole
        };
        Self {
            pruning,
            row_filter,
        }
    }

    /// Lower both trees against one file. A whole-tree row filter shares the
    /// pruning tree's lowering.
    pub(super) fn lower(
        &self,
        schema: FileSchema<'_>,
    ) -> Result<LoweredPredicate, ConnectorError> {
        let row_filter = match &self.row_filter {
            RowFilter::Whole => LoweredRowFilter::Whole,
            RowFilter::Projected(node) => {
                LoweredRowFilter::Projected(node.lower(schema)?)
            }
            RowFilter::Skipped => LoweredRowFilter::Skipped,
        };
        Ok(LoweredPredicate {
            pruning: self.pruning.lower(schema)?,
            row_filter,
        })
    }
}

/// One bound predicate lowered against one file.
pub(super) struct LoweredPredicate {
    pub(super) pruning: FileNode,
    row_filter: LoweredRowFilter,
}

enum LoweredRowFilter {
    Whole,
    Projected(FileNode),
    Skipped,
}

impl LoweredPredicate {
    pub(super) const fn row_filter(&self) -> Option<&FileNode> {
        match &self.row_filter {
            LoweredRowFilter::Whole => Some(&self.pruning),
            LoweredRowFilter::Projected(node) => Some(node),
            LoweredRowFilter::Skipped => None,
        }
    }
}

/// Workers rebuild the row filter from the transported pruning tree.
impl Serialize for ParquetBoundPredicate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.pruning.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ParquetBoundPredicate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BoundNode::deserialize(deserializer).map(Self::new)
    }
}

/// Worker transport of a boolean test by its plan wire id.
mod boolean_test_kind {
    use lagodb_core::expr::BooleanTestKind;
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        kind: &BooleanTestKind,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        kind.wire_id().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BooleanTestKind, D::Error> {
        BooleanTestKind::from_wire_id(i32::deserialize(deserializer)?)
            .ok_or_else(|| D::Error::custom("invalid boolean test kind"))
    }
}
