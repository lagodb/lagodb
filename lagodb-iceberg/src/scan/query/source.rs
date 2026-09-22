//! Query-offload source binding and statement-stable task planning.

use std::sync::Arc;

use iceberg_lite::expr::Predicate;
use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::scan::{FileScanTask, TableScan};
use iceberg_lite::table::Table;

use crate::error::IcebergResult;

/// Query-Offload statement binding with no PostgreSQL row decoder.
pub(crate) struct QuerySourceBinding {
    pub(crate) scan: TableScan,
    pub(crate) arrow_schema: arrow_schema::SchemaRef,
    pub(crate) row_filter: Option<Predicate>,
    pub(crate) task_planner: QueryTaskPlanner,
}

/// Statement-bound task planner used after DataFusion optimization.
#[derive(Debug)]
pub(crate) struct QueryTaskPlanner {
    table: Table,
    field_ids: Box<[i32]>,
    planning_filter: Option<Predicate>,
    delta: Option<Arc<SnapshotDelta>>,
}

impl QueryTaskPlanner {
    pub(crate) fn new(
        table: Table,
        field_ids: Box<[i32]>,
        planning_filter: Option<Predicate>,
        delta: Option<Arc<SnapshotDelta>>,
    ) -> Self {
        Self {
            table,
            field_ids,
            planning_filter,
            delta,
        }
    }

    pub(crate) fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    pub(crate) fn plan_files(
        &self,
        projected_field_ids: &[i32],
        additional_filter: Option<&Predicate>,
    ) -> IcebergResult<Vec<FileScanTask>> {
        let filter = match (&self.planning_filter, additional_filter) {
            (Some(stable), Some(runtime)) => {
                Some(Predicate::and(stable.clone(), runtime.clone()))
            }
            (Some(predicate), None) | (None, Some(predicate)) => {
                Some(predicate.clone())
            }
            (None, None) => None,
        };
        let mut builder = self
            .table
            .scan()
            .select_field_ids(projected_field_ids.iter().copied());
        if let Some(filter) = filter {
            builder = builder.with_filter(filter);
        }
        if let Some(delta) = &self.delta {
            builder = builder.with_delta(Arc::clone(delta));
        }
        Ok(builder.build()?.plan_files()?)
    }
}
