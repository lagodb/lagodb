//! Minimal relational IR for scalar `COUNT(*)` offload.

use lagodb_core::query_contract::SourceId;
use pgrx::pg_sys;

use super::OutputId;

pub(crate) const S1M_SOURCE_COUNT: usize = 1;
pub(crate) const S1M_OUTPUT_COUNT: usize = 1;

/// A semantic error in the S1M query plan.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QueryPlanError {
    #[error("source identity {index} is outside the scalar COUNT source table")]
    SourceOutOfBounds { index: usize },
    #[error("output identity {index} is outside the scalar COUNT output table")]
    OutputOutOfBounds { index: usize },
    #[error("scalar COUNT uses aggregate OID {found:?}, expected {expected:?}")]
    WrongCountAggregate {
        found: pg_sys::Oid,
        expected: pg_sys::Oid,
    },
    #[error("scalar COUNT result type is {found:?}, expected {expected:?}")]
    WrongCountResultType {
        found: pg_sys::Oid,
        expected: pg_sys::Oid,
    },
    #[error("scalar COUNT project output does not match aggregate output")]
    ProjectOutputMismatch,
    #[error("query tuple layout output does not match fragment output")]
    TupleLayoutOutputMismatch,
    #[error("query tuple layout type does not match aggregate result type")]
    TupleLayoutTypeMismatch,
    #[error("scalar COUNT tuple slot typmod must be -1, found {found}")]
    WrongCountTypmod { found: i32 },
    #[error("scalar COUNT tuple slot collation must be InvalidOid, found {found:?}")]
    WrongCountCollation { found: pg_sys::Oid },
}

/// One source leaf in a [`QueryFragment`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceNode {
    source: SourceId,
}

impl SourceNode {
    #[inline]
    pub const fn source(&self) -> SourceId {
        self.source
    }
}

/// PostgreSQL identity and result slot for the only aggregate supported by S1M.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountStar {
    function_oid: pg_sys::Oid,
    result_type: pg_sys::Oid,
    output: OutputId,
}

impl CountStar {
    pub(crate) fn from_plan_data(
        function_oid: pg_sys::Oid,
        result_type: pg_sys::Oid,
        output: OutputId,
    ) -> Result<Self, QueryPlanError> {
        let count = Self {
            function_oid,
            result_type,
            output,
        };
        count.validate()?;
        Ok(count)
    }

    #[inline]
    pub const fn function_oid(&self) -> pg_sys::Oid {
        self.function_oid
    }

    #[inline]
    pub const fn result_type(&self) -> pg_sys::Oid {
        self.result_type
    }

    #[inline]
    pub const fn output(&self) -> OutputId {
        self.output
    }

    fn validate(&self) -> Result<(), QueryPlanError> {
        let expected_function = pg_sys::Oid::from(pg_sys::F_COUNT_);
        if self.function_oid != expected_function {
            return Err(QueryPlanError::WrongCountAggregate {
                found: self.function_oid,
                expected: expected_function,
            });
        }
        let expected_type = pg_sys::INT8OID;
        if self.result_type != expected_type {
            return Err(QueryPlanError::WrongCountResultType {
                found: self.result_type,
                expected: expected_type,
            });
        }
        validate_output_id(self.output)
    }
}

/// One aggregate expression with a current execution consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateExpression {
    CountStar(CountStar),
}

/// One scalar aggregate over a query input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateNode {
    input: Box<QueryNode>,
    aggregates: Box<[AggregateExpression]>,
}

impl AggregateNode {
    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub fn aggregates(&self) -> &[AggregateExpression] {
        &self.aggregates
    }
}

/// Projection from the aggregate output into the PostgreSQL result slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectNode {
    input: Box<QueryNode>,
    output: OutputId,
}

impl ProjectNode {
    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub const fn output(&self) -> OutputId {
        self.output
    }
}

/// Provider-neutral relational node.
///
/// S1M exposes only variants with a real execution consumer. Filter, join,
/// grouped aggregate, and scalar-expression variants are added with their
/// first product stage rather than represented by inert placeholders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryNode {
    Source(SourceNode),
    Aggregate(AggregateNode),
    Project(ProjectNode),
}

/// The single semantic source of truth for a pushed query subtree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryFragment {
    root: QueryNode,
}

impl QueryFragment {
    pub fn scalar_count(
        source: SourceId,
        function_oid: pg_sys::Oid,
        result_type: pg_sys::Oid,
    ) -> Result<Self, QueryPlanError> {
        validate_source_id(source)?;
        let output = OutputId::from_index(0);
        let count = CountStar::from_plan_data(function_oid, result_type, output)?;
        let source = QueryNode::Source(SourceNode { source });
        let aggregate = QueryNode::Aggregate(AggregateNode {
            input: Box::new(source),
            aggregates: Box::new([AggregateExpression::CountStar(count)]),
        });
        Ok(Self {
            root: QueryNode::Project(ProjectNode {
                input: Box::new(aggregate),
                output,
            }),
        })
    }

    #[inline]
    pub const fn root(&self) -> &QueryNode {
        &self.root
    }

    /// Source identity referenced by the validated S1M fragment.
    #[inline]
    pub fn scalar_count_source(&self) -> SourceId {
        self.scalar_count_parts().0.source()
    }

    pub(crate) fn from_decoded_parts(
        source: SourceId,
        count: CountStar,
        project_output: OutputId,
    ) -> Result<Self, QueryPlanError> {
        validate_source_id(source)?;
        validate_output_id(project_output)?;
        if project_output != count.output {
            return Err(QueryPlanError::ProjectOutputMismatch);
        }
        let source = QueryNode::Source(SourceNode { source });
        let aggregate = QueryNode::Aggregate(AggregateNode {
            input: Box::new(source),
            aggregates: Box::new([AggregateExpression::CountStar(count)]),
        });
        Ok(Self {
            root: QueryNode::Project(ProjectNode {
                input: Box::new(aggregate),
                output: project_output,
            }),
        })
    }

    pub(crate) fn scalar_count_parts(
        &self,
    ) -> (&SourceNode, &AggregateNode, &ProjectNode) {
        let QueryNode::Project(project) = &self.root else {
            unreachable!("QueryFragment constructors preserve the S1M root shape")
        };
        let QueryNode::Aggregate(aggregate) = project.input() else {
            unreachable!(
                "QueryFragment constructors preserve the S1M aggregate shape"
            )
        };
        let QueryNode::Source(source) = aggregate.input() else {
            unreachable!("QueryFragment constructors preserve the S1M source shape")
        };
        let [_expression] = aggregate.aggregates() else {
            unreachable!(
                "QueryFragment constructors preserve one aggregate expression"
            )
        };
        (source, aggregate, project)
    }
}

/// Metadata for one physical PostgreSQL output slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTupleSlot {
    output: OutputId,
    type_oid: pg_sys::Oid,
    typmod: i32,
    collation: pg_sys::Oid,
}

impl QueryTupleSlot {
    pub(crate) const fn from_plan_data(
        output: OutputId,
        type_oid: pg_sys::Oid,
        typmod: i32,
        collation: pg_sys::Oid,
    ) -> Self {
        Self {
            output,
            type_oid,
            typmod,
            collation,
        }
    }

    #[inline]
    pub const fn output(&self) -> OutputId {
        self.output
    }

    #[inline]
    pub const fn type_oid(&self) -> pg_sys::Oid {
        self.type_oid
    }

    #[inline]
    pub const fn typmod(&self) -> i32 {
        self.typmod
    }

    #[inline]
    pub const fn collation(&self) -> pg_sys::Oid {
        self.collation
    }

    fn validate(&self) -> Result<(), QueryPlanError> {
        validate_output_id(self.output)?;
        if self.type_oid != pg_sys::INT8OID {
            return Err(QueryPlanError::WrongCountResultType {
                found: self.type_oid,
                expected: pg_sys::INT8OID,
            });
        }
        if self.typmod != -1 {
            return Err(QueryPlanError::WrongCountTypmod { found: self.typmod });
        }
        if self.collation != pg_sys::InvalidOid {
            return Err(QueryPlanError::WrongCountCollation {
                found: self.collation,
            });
        }
        Ok(())
    }
}

/// Physical output contract for the S1M scalar COUNT scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTupleLayout {
    slot: QueryTupleSlot,
}

impl QueryTupleLayout {
    pub(crate) fn scalar_count(output: OutputId, type_oid: pg_sys::Oid) -> Self {
        Self {
            slot: QueryTupleSlot {
                output,
                type_oid,
                typmod: -1,
                collation: pg_sys::InvalidOid,
            },
        }
    }

    #[inline]
    pub const fn len(&self) -> usize {
        1
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        false
    }

    #[inline]
    pub const fn slot(&self) -> &QueryTupleSlot {
        &self.slot
    }

    pub(crate) fn from_decoded_slot(
        slot: QueryTupleSlot,
    ) -> Result<Self, QueryPlanError> {
        slot.validate()?;
        Ok(Self { slot })
    }
}

fn validate_source_id(source: SourceId) -> Result<(), QueryPlanError> {
    SourceId::from_plan_data(source.index(), S1M_SOURCE_COUNT)
        .map(|_| ())
        .ok_or(QueryPlanError::SourceOutOfBounds {
            index: source.index(),
        })
}

fn validate_output_id(output: OutputId) -> Result<(), QueryPlanError> {
    OutputId::from_plan_data(output.index(), S1M_OUTPUT_COUNT)
        .map(|_| ())
        .ok_or(QueryPlanError::OutputOutOfBounds {
            index: output.index(),
        })
}
