//! Relation-local mutation context shared with the matching ModifyTable state.

use std::rc::Rc;

use iceberg_lite::scan::FileScanTask;
use iceberg_lite::table::Table;

use super::super::options::ForeignTableIdentity;
use super::super::relation::RemoteTableKey;
use crate::error::IcebergResult;
use crate::scan::{PreparedRowScan, RowLocationScanInput};
use crate::schema::relation::RelationLayout;
use crate::write::PlannedMutationTasks;

#[derive(Debug, Clone)]
pub(crate) struct ForeignMutationScan {
    inner: Rc<ForeignMutationScanInner>,
}

#[derive(Debug)]
struct ForeignMutationScanInner {
    identity: ForeignTableIdentity,
    key: RemoteTableKey,
    table: Table,
    layout: RelationLayout,
    starting_snapshot_id: Option<i64>,
    tasks: Rc<PlannedMutationTasks>,
}

impl ForeignMutationScan {
    pub(crate) fn new(
        identity: ForeignTableIdentity,
        key: RemoteTableKey,
        table: Table,
        layout: RelationLayout,
        starting_snapshot_id: Option<i64>,
        tasks: Vec<FileScanTask>,
    ) -> Self {
        Self {
            inner: Rc::new(ForeignMutationScanInner {
                identity,
                key,
                table,
                layout,
                starting_snapshot_id,
                tasks: Rc::new(PlannedMutationTasks::new(tasks)),
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

    pub(crate) fn layout(&self) -> &RelationLayout {
        &self.inner.layout
    }

    pub(crate) fn starting_snapshot_id(&self) -> Option<i64> {
        self.inner.starting_snapshot_id
    }

    pub(crate) fn tasks(&self) -> Rc<PlannedMutationTasks> {
        Rc::clone(&self.inner.tasks)
    }

    pub(crate) fn open_row_location_scan(
        &self,
        prepared: &PreparedRowScan,
    ) -> IcebergResult<RowLocationScanInput> {
        prepared.open_row_location_scan(self.inner.tasks.shared_tasks())
    }
}
