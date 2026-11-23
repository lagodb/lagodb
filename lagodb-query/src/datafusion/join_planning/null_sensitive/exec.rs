//! The plan node that runs a NULL-sensitive anti join and decides its rows.

use std::fmt::{self, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::arrow::compute::{filter_record_batch, is_not_null, is_null};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result};
use datafusion::execution::{
    RecordBatchStream, SendableRecordBatchStream, TaskContext,
};
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, EmptyRecordBatchStream,
    ExecutionPlan, ExecutionPlanProperties, Partitioning, PlanProperties,
    ReplaceChildrenOptions,
};
use futures::{Stream, StreamExt, stream};

use super::facts::{BuildFacts, ObservedBuild, OuterRows};
use crate::datafusion::join_planning::adaptive::AdaptiveHashJoinExec;
use crate::datafusion::join_planning::staged_build::StagedInputExec;
use crate::plan::NullSensitiveAntiMode;

/// Runs the RightAnti hash join that builds a NULL-sensitive SubPlan's
/// subquery and probes its outer relation, then keeps the unmatched outer rows
/// that [`BuildFacts`] allow.
///
/// The join is a child, directly or under [`AdaptiveHashJoinExec`], so the
/// physical optimizer places it and the protobuf codec carries it. It runs
/// once, in partition 0, over every probe partition: its build is read once,
/// and one set of facts decides every outer row. The node keeps the probe's
/// partition count so that a plan with it stays the same plan in every task
/// it is replicated to, and its other partitions are empty.
///
/// The outer key is part of the output, as the join embeds no projection, and
/// this node takes no projection either.
#[derive(Debug)]
pub(in crate::datafusion) struct NullSensitiveAntiJoinExec {
    mode: NullSensitiveAntiMode,
    join: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl NullSensitiveAntiJoinExec {
    /// `join` may be swapped while JoinSelection runs, and is checked when it
    /// executes.
    pub(in crate::datafusion) fn new(
        mode: NullSensitiveAntiMode,
        join: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let child = join.properties();
        let mut equivalences = child.eq_properties.clone();
        equivalences.clear_orderings();
        let properties = PlanProperties::new(
            equivalences,
            Partitioning::UnknownPartitioning(child.partitioning.partition_count()),
            child.emission_type,
            child.boundedness,
        );
        Self {
            mode,
            join,
            properties: Arc::new(properties),
        }
    }

    pub(in crate::datafusion) const fn mode(&self) -> NullSensitiveAntiMode {
        self.mode
    }

    pub(in crate::datafusion::join_planning) fn join(
        &self,
    ) -> &Arc<dyn ExecutionPlan> {
        &self.join
    }

    /// The planned hash join, and the adaptive node it runs under, if any.
    fn planned_join(&self) -> Result<(&HashJoinExec, Option<&AdaptiveHashJoinExec>)> {
        let (join, adaptive) = match self.join.downcast_ref::<AdaptiveHashJoinExec>()
        {
            Some(adaptive) => (adaptive.join(), Some(adaptive)),
            None => (
                self.join.downcast_ref::<HashJoinExec>().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "NullSensitiveAntiJoinExec cannot run {}",
                        self.join.name(),
                    ))
                })?,
                None,
            ),
        };
        if *join.join_type() != JoinType::RightAnti
            || join.on().len() != 1
            || join.filter().is_some()
            || join.contains_projection()
            || join.null_equality() != NullEquality::NullEqualsNothing
            || join.left().output_partitioning().partition_count() != 1
        {
            return Err(DataFusionError::Internal(
                "NullSensitiveAntiJoinExec requires a single-key RightAnti hash \
                 join that collects its subquery"
                    .to_owned(),
            ));
        }
        Ok((join, adaptive))
    }

    /// Every probe partition as one stream. All open now, as `HashJoinExec`
    /// opens its inputs in `execute`.
    fn merged_probe(
        probe: &Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut streams = (0..probe.output_partitioning().partition_count())
            .map(|partition| probe.execute(partition, Arc::clone(context)))
            .collect::<Result<Vec<_>>>()?;
        if streams.len() == 1
            && let Some(stream) = streams.pop()
        {
            return Ok(stream);
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            probe.schema(),
            stream::select_all(streams),
        )))
    }
}

impl DisplayAs for NullSensitiveAntiJoinExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> fmt::Result {
        write!(formatter, "NullSensitiveAntiJoinExec: mode={:?}", self.mode)
    }
}

impl ExecutionPlan for NullSensitiveAntiJoinExec {
    fn name(&self) -> &str {
        "NullSensitiveAntiJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.join]
    }

    /// Nothing may be inserted between this node and the join it runs.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(self.mode, children.swap_remove(0))))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        }
        let (join, adaptive) = self.planned_join()?;
        let [(build_key, outer_key)] = join.on() else {
            unreachable!("planned_join admits one key");
        };
        // Facts belong to this execution; a rescan executes a reset plan.
        let facts = Arc::new(BuildFacts::default());
        let build = ObservedBuild::new(
            join.left().execute(0, Arc::clone(&context))?,
            Arc::clone(build_key),
            Arc::clone(&facts),
        );
        let probe = Self::merged_probe(join.right(), &context)?;
        // The builder keeps the planned join's metrics, as the adaptive node
        // keeps its own.
        let run = join
            .builder()
            .with_new_children(vec![
                Arc::new(StagedInputExec::single_partition(Box::pin(build)))
                    as Arc<dyn ExecutionPlan>,
                Arc::new(StagedInputExec::single_partition(probe)),
            ])?
            .build()?;
        let run: Arc<dyn ExecutionPlan> = match adaptive {
            Some(adaptive) => Arc::new(adaptive.with_join(run)),
            None => Arc::new(run),
        };
        Ok(Box::pin(NullSensitiveAntiStream {
            input: Some(run.execute(0, context)?),
            facts,
            mode: self.mode,
            outer_key: Arc::clone(outer_key),
            rows: None,
            schema: self.schema(),
        }))
    }
}

/// The join's unmatched outer rows, filtered by the build's facts.
struct NullSensitiveAntiStream {
    /// Dropped once no further row can be returned.
    input: Option<SendableRecordBatchStream>,
    facts: Arc<BuildFacts>,
    mode: NullSensitiveAntiMode,
    outer_key: PhysicalExprRef,
    /// Read from `facts` at the first batch; the build is complete by then.
    rows: Option<OuterRows>,
    schema: SchemaRef,
}

impl NullSensitiveAntiStream {
    fn keep(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        let rows = match self.rows {
            Some(rows) => rows,
            None => *self.rows.insert(self.facts.outer_rows(self.mode)?),
        };
        let null_keys = match rows {
            OuterRows::All => return Ok(Some(batch)),
            OuterRows::NoRows => {
                self.input = None;
                return Ok(None);
            }
            OuterRows::NonNullKeys => false,
            OuterRows::NullKeys => true,
        };
        let keys = self
            .outer_key
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;
        let kept = if null_keys {
            keys.null_count()
        } else {
            batch.num_rows() - keys.null_count()
        };
        if kept == batch.num_rows() {
            return Ok(Some(batch));
        }
        if kept == 0 {
            return Ok(None);
        }
        let mask = if null_keys {
            is_null(&keys)?
        } else {
            is_not_null(&keys)?
        };
        Ok(Some(filter_record_batch(&batch, &mask)?))
    }
}

impl Stream for NullSensitiveAntiStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let Some(input) = this.input.as_mut() else {
                return Poll::Ready(None);
            };
            let batch = match ready!(input.poll_next_unpin(context)) {
                Some(Ok(batch)) => batch,
                Some(Err(error)) => return Poll::Ready(Some(Err(error))),
                None => {
                    this.input = None;
                    return Poll::Ready(None);
                }
            };
            match this.keep(batch) {
                Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                Ok(None) => {}
                Err(error) => return Poll::Ready(Some(Err(error))),
            }
        }
    }
}

impl RecordBatchStream for NullSensitiveAntiStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
