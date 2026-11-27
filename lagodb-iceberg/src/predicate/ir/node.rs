//! Persisted Iceberg predicate AST nodes, value shapes, and `EXPLAIN` rendering.

use core::fmt::Write;

use lagodb_core::datum::Decimal128Semantics;
use lagodb_core::expr::pushdown::{FilterEvaluation, PredicatePlan};
use lagodb_core::expr::{
    BooleanTestKind, PgBpcharSemantics, PgComparisonKind, RuntimeValueId,
};

use crate::predicate::semantics::TimestampPrecision;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::predicate) enum PlannedValueType {
    Boolean,
    Uuid,
    Binary,
    FixedBinary(u64),
    Int2ToLong,
    Int4ToLong,
    Int2,
    Int4,
    Int8,
    Date,
    Timestamp(TimestampPrecision),
    Timestamptz(TimestampPrecision),
    String,
    /// A `bpchar` value padded at bind time to a `character(n)` column width.
    Bpchar(PgBpcharSemantics),
    Decimal128(Decimal128Semantics),
    Int8ToInt,
    Float,
    Double,
    FloatToDouble,
    DoubleToFloat,
}

#[derive(Debug)]
pub(in crate::predicate) enum PlannedNode {
    AlwaysTrue,
    AlwaysFalse,
    Comparison {
        operator: PgComparisonKind,
        column: PlannedColumn,
        value: RuntimeValueId,
        value_type: PlannedValueType,
    },
    BooleanTest {
        kind: BooleanTestKind,
        column: PlannedColumn,
    },
    IsNull(PlannedColumn),
    IsNotNull(PlannedColumn),
    IsNan(PlannedColumn),
    IsNotNan(PlannedColumn),
    StartsWith {
        column: PlannedColumn,
        prefix: RuntimeValueId,
    },
    InSet {
        column: PlannedColumn,
        set: RuntimeValueId,
        value_type: PlannedValueType,
    },
    And(Box<[Self]>),
    Or(Box<[Self]>),
    Not(Box<Self>),
}

impl PlannedNode {
    /// How much of this tree the reader evaluates on each row it reads: all
    /// of it, the part projected without sets, or none. Every leaf other than
    /// a set counts as kept, which a value its binding cannot represent only
    /// makes an upper bound.
    ///
    /// Sets only prune files (`PredicateCapabilities`), so the row filter
    /// keeps what the shared AND-widening and complete-OR rules keep without
    /// them (`PredicateBinder::bind_logical`). Only AND and OR can hold a set
    /// because negation refuses Conservative plans.
    pub(in crate::predicate) fn evaluation(&self) -> FilterEvaluation {
        match self.row_filter_plan() {
            PredicatePlan::Unsupported => FilterEvaluation::PruningOnly,
            PredicatePlan::Partial(()) => FilterEvaluation::Projected,
            PredicatePlan::Exact(())
            | PredicatePlan::ExactNoComplement(())
            | PredicatePlan::Conservative(()) => FilterEvaluation::PerRow,
        }
    }

    fn row_filter_plan(&self) -> PredicatePlan<()> {
        match self {
            Self::InSet { .. } => PredicatePlan::Unsupported,
            Self::And(nodes) => PredicatePlan::conjunction(
                nodes.iter().map(Self::row_filter_plan),
                |_| (),
            ),
            Self::Or(nodes) => PredicatePlan::disjunction(
                nodes.iter().map(Self::row_filter_plan),
                |_| (),
            ),
            _ => PredicatePlan::Exact(()),
        }
    }

    pub(super) fn write_explain<'a>(
        &self,
        out: &mut String,
        value_text: &mut impl FnMut(RuntimeValueId) -> &'a str,
    ) {
        match self {
            Self::AlwaysTrue => out.push_str("TRUE"),
            Self::AlwaysFalse => out.push_str("FALSE"),
            Self::Comparison {
                operator,
                column,
                value,
                ..
            } => {
                let _ = write!(
                    out,
                    "{} {} {}",
                    column.display_name,
                    operator.explain_symbol(),
                    value_text(*value)
                );
            }
            Self::BooleanTest { kind, column } => {
                let _ = write!(out, "{} IS {}", column.display_name, kind);
            }
            Self::IsNull(column) => {
                let _ = write!(out, "{} IS NULL", column.display_name);
            }
            Self::IsNotNull(column) => {
                let _ = write!(out, "{} IS NOT NULL", column.display_name);
            }
            Self::IsNan(column) => {
                let _ = write!(out, "{} IS NAN", column.display_name);
            }
            Self::IsNotNan(column) => {
                let _ = write!(out, "{} IS NOT NAN", column.display_name);
            }
            Self::StartsWith { column, prefix } => {
                let _ = write!(
                    out,
                    "{} STARTS WITH {}",
                    column.display_name,
                    value_text(*prefix)
                );
            }
            Self::InSet { column, set, .. } => {
                let _ = write!(
                    out,
                    "{} = ANY ({})",
                    column.display_name,
                    value_text(*set)
                );
            }
            Self::And(children) => {
                Self::write_explain_logical(children, out, value_text, " AND ");
            }
            Self::Or(children) => {
                Self::write_explain_logical(children, out, value_text, " OR ");
            }
            Self::Not(child) => {
                out.push_str("NOT (");
                child.write_explain(out, value_text);
                out.push(')');
            }
        }
    }

    fn write_explain_logical<'a>(
        children: &[Self],
        out: &mut String,
        value_text: &mut impl FnMut(RuntimeValueId) -> &'a str,
        separator: &str,
    ) {
        out.push('(');
        for (index, child) in children.iter().enumerate() {
            if index > 0 {
                out.push_str(separator);
            }
            child.write_explain(out, value_text);
        }
        out.push(')');
    }
}

#[derive(Debug)]
pub(in crate::predicate) struct PlannedColumn {
    pub(in crate::predicate) field_id: i32,
    pub(in crate::predicate) display_name: String,
}
