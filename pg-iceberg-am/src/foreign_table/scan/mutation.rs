//! Relation-local mutation context shared with the matching ModifyTable state.

use std::rc::Rc;

use iceberg_lite::table::Table;

use super::super::options::ForeignTableIdentity;
use super::super::relation::RemoteTableKey;
use crate::engine::schema::relation::RelationShape;
use crate::engine::write::PlannedMutationTasks;

#[derive(Debug, Clone)]
pub(crate) struct ForeignMutationScan {
    inner: Rc<ForeignMutationScanInner>,
}

#[derive(Debug)]
struct ForeignMutationScanInner {
    identity: ForeignTableIdentity,
    key: RemoteTableKey,
    table: Table,
    shape: RelationShape,
    starting_snapshot_id: Option<i64>,
    tasks: Rc<PlannedMutationTasks>,
}

impl ForeignMutationScan {
    pub(crate) fn new(
        identity: ForeignTableIdentity,
        key: RemoteTableKey,
        table: Table,
        shape: RelationShape,
        starting_snapshot_id: Option<i64>,
        tasks: Rc<PlannedMutationTasks>,
    ) -> Self {
        Self {
            inner: Rc::new(ForeignMutationScanInner {
                identity,
                key,
                table,
                shape,
                starting_snapshot_id,
                tasks,
            }),
        }
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.inner.identity
    }

    pub(crate) fn key(&self) -> &RemoteTableKey {
        &self.inner.key
    }

    pub(crate) fn table(&self) -> &Table {
        &self.inner.table
    }

    pub(crate) fn shape(&self) -> &RelationShape {
        &self.inner.shape
    }

    pub(crate) fn starting_snapshot_id(&self) -> Option<i64> {
        self.inner.starting_snapshot_id
    }

    pub(crate) fn tasks(&self) -> Rc<PlannedMutationTasks> {
        Rc::clone(&self.inner.tasks)
    }
}
