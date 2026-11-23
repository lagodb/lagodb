//! Sort-merge plan that continues one hash join partition.

use std::sync::Arc;

use arrow_schema::SortOptions;
use datafusion::common::Result;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::sorts::sort::SortExec;

/// Sort `input` for a sort-merge join. Key order is free because the join only
/// tests equality, so both sides use the default options.
fn sorted<'a>(
    keys: impl Iterator<Item = &'a Arc<dyn PhysicalExpr>>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let ordering =
        LexOrdering::new(keys.map(|key| {
            PhysicalSortExpr::new(Arc::clone(key), SortOptions::default())
        }))
        .expect("a hash join has at least one key");
    Arc::new(SortExec::new(ordering, input))
}

/// The join `hash_join` describes, over single-partition `left` and `right`
/// with the same schemas as its own inputs. The hash join's embedded projection
/// becomes a `ProjectionExec`, so the output schema is the same.
pub(super) fn sort_merge_join(
    hash_join: &HashJoinExec,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let on = hash_join.on();
    let merge: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
        sorted(on.iter().map(|(key, _)| key), left),
        sorted(on.iter().map(|(_, key)| key), right),
        on.to_vec(),
        hash_join.filter().cloned(),
        *hash_join.join_type(),
        vec![SortOptions::default(); on.len()],
        hash_join.null_equality(),
    )?);
    let Some(projection) = hash_join.projection.as_ref() else {
        return Ok(merge);
    };
    let schema = merge.schema();
    let expressions = projection
        .iter()
        .map(|&index| {
            let name = schema.field(index).name();
            ProjectionExpr::new(Arc::new(Column::new(name, index)), name.as_str())
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(expressions, merge)?))
}
