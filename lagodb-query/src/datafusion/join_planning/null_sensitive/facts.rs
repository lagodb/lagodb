//! What the build input of a NULL-sensitive anti join held.
//!
//! PostgreSQL's hashed SubPlan notes `havehashrows` and `havenullrows` while
//! it loads the subquery (`buildSubPlanHash`) and reads them for every outer
//! row (`ExecHashSubPlan`). These are the same two facts, published once the
//! build input ends.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll, ready};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_expr::PhysicalExprRef;
use futures::{Stream, StreamExt};

use crate::plan::NullSensitiveAntiMode;

/// The unmatched outer rows a NULL-sensitive anti join returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OuterRows {
    All,
    /// NOT IN of a NULL outer key over a non-empty subquery is UNKNOWN.
    NonNullKeys,
    /// Only `x IS NULL` holds once the subquery returned a NULL key.
    NullKeys,
    /// NOT IN over a subquery with a NULL key is never true.
    NoRows,
}

/// The facts one execution of the build input published.
#[derive(Debug, Default)]
pub(super) struct BuildFacts {
    state: AtomicU8,
}

impl BuildFacts {
    const HAS_ROWS: u8 = 1;
    const HAS_NULL: u8 = 1 << 1;
    const COMPLETE: u8 = 1 << 2;

    /// The outer rows `mode` returns. Every algorithm the join can run reads
    /// its whole build input before it emits a row: the hash join builds its
    /// table, the hybrid join splits the build first, and the sort-merge join
    /// sorts it. A join that broke that order fails here instead of deciding
    /// rows on part of the subquery.
    pub(super) fn outer_rows(
        &self,
        mode: NullSensitiveAntiMode,
    ) -> Result<OuterRows> {
        let state = self.state.load(Ordering::Acquire);
        if state & Self::COMPLETE == 0 {
            return Err(DataFusionError::Internal(
                "NULL-sensitive anti join emitted a row before reading its whole \
                 subquery"
                    .to_owned(),
            ));
        }
        let has_null = state & Self::HAS_NULL != 0;
        let has_rows = state & Self::HAS_ROWS != 0;
        Ok(match mode {
            NullSensitiveAntiMode::NotIn if has_null => OuterRows::NoRows,
            NullSensitiveAntiMode::NotIn if has_rows => OuterRows::NonNullKeys,
            NullSensitiveAntiMode::NullOrNotIn if has_null => OuterRows::NullKeys,
            NullSensitiveAntiMode::NotIn | NullSensitiveAntiMode::NullOrNotIn => {
                OuterRows::All
            }
        })
    }
}

/// The build input, recording into [`BuildFacts`] as the join reads it.
pub(super) struct ObservedBuild {
    input: SendableRecordBatchStream,
    key: PhysicalExprRef,
    facts: Arc<BuildFacts>,
    /// This stream is the only writer, so it keeps what it has published.
    state: u8,
}

impl ObservedBuild {
    pub(super) fn new(
        input: SendableRecordBatchStream,
        key: PhysicalExprRef,
        facts: Arc<BuildFacts>,
    ) -> Self {
        Self {
            input,
            key,
            facts,
            state: 0,
        }
    }

    /// A NULL key implies a row, so once one is seen no batch adds a fact.
    fn record(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 || self.state & BuildFacts::HAS_NULL != 0 {
            return Ok(());
        }
        let mut state = self.state | BuildFacts::HAS_ROWS;
        if self
            .key
            .evaluate(batch)?
            .into_array(batch.num_rows())?
            .null_count()
            > 0
        {
            state |= BuildFacts::HAS_NULL;
        }
        self.publish(state);
        Ok(())
    }

    fn publish(&mut self, state: u8) {
        if state != self.state {
            self.state = state;
            self.facts.state.store(state, Ordering::Release);
        }
    }
}

impl Stream for ObservedBuild {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Poll::Ready(match ready!(this.input.poll_next_unpin(context)) {
            Some(Ok(batch)) => Some(this.record(&batch).map(|()| batch)),
            Some(Err(error)) => Some(Err(error)),
            None => {
                this.publish(this.state | BuildFacts::COMPLETE);
                None
            }
        })
    }
}

impl RecordBatchStream for ObservedBuild {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
