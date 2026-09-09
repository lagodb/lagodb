//! Query-level LIMIT/OFFSET evaluated from PostgreSQL runtime bindings.

use lagodb_core::expr::RuntimeValueId;

use super::ir::{QueryNode, QueryPlanError, RowEstimate};

/// PostgreSQL's cardinality facts needed to cost LIMIT/OFFSET without charging
/// the complete run cost of a streaming input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitEstimate {
    output_rows: RowEstimate,
    offset_rows: RowEstimate,
    count_rows: Option<RowEstimate>,
}

impl LimitEstimate {
    pub fn try_new(
        output_rows: f64,
        offset_rows: f64,
        count_rows: Option<f64>,
    ) -> Result<Self, QueryPlanError> {
        Ok(Self {
            output_rows: RowEstimate::try_new(output_rows)?,
            offset_rows: RowEstimate::try_new(offset_rows)?,
            count_rows: count_rows.map(RowEstimate::try_new).transpose()?,
        })
    }

    #[inline]
    pub const fn output_rows(self) -> f64 {
        self.output_rows.get()
    }

    #[inline]
    pub const fn offset_rows(self) -> f64 {
        self.offset_rows.get()
    }

    #[inline]
    pub const fn count_rows(self) -> Option<f64> {
        match self.count_rows {
            Some(rows) => Some(rows.get()),
            None => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitNode {
    input: Box<QueryNode>,
    offset: Option<RuntimeValueId>,
    count: Option<RuntimeValueId>,
    estimate: LimitEstimate,
}

impl LimitNode {
    pub fn new(
        input: QueryNode,
        offset: Option<RuntimeValueId>,
        count: Option<RuntimeValueId>,
        estimate: LimitEstimate,
    ) -> Result<Self, QueryPlanError> {
        if offset.is_none() && count.is_none() {
            return Err(QueryPlanError::EmptyLimit);
        }
        if (offset.is_none() && estimate.offset_rows() != 0.0)
            || (count.is_none() && estimate.count_rows().is_some())
        {
            return Err(QueryPlanError::InvalidLimitEstimate);
        }
        Ok(Self {
            input: Box::new(input),
            offset,
            count,
            estimate,
        })
    }

    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub const fn offset(&self) -> Option<RuntimeValueId> {
        self.offset
    }

    #[inline]
    pub const fn count(&self) -> Option<RuntimeValueId> {
        self.count
    }

    #[inline]
    pub const fn estimated_rows(&self) -> f64 {
        self.estimate.output_rows()
    }

    #[inline]
    pub const fn estimate(&self) -> LimitEstimate {
        self.estimate
    }
}
