//! Managed-table mutation read ownership.

use std::rc::Rc;

use iceberg_lite::expr::Predicate;
use lagodb_core::access::mutation::ModifyScanBinding;
use pgrx::pg_sys;

use super::{
    IcebergModifyQueryState, IcebergModifyScanContext, ManagedMutationCursor,
};
use crate::error::IcebergResult;
use crate::scan::{PreparedRowScan, ReaderPredicate, RowLocationScanInput};
use crate::write::PlannedMutationTasks;

/// Prepared read and task inventory for one managed-table mutation scan.
///
/// Query reads never carry this state. The inventory remains owned by the
/// mutation lifecycle because final row-delete processing needs the same task
/// metadata after the scan cursor has been opened.
pub(crate) struct PreparedManagedMutationScan {
    prepared: PreparedRowScan,
    tasks: Rc<PlannedMutationTasks>,
    conflict_filter: Predicate,
}

impl PreparedManagedMutationScan {
    pub(crate) fn prepare(
        prepared: PreparedRowScan,
        conflict_filter: Predicate,
    ) -> IcebergResult<Self> {
        let tasks = Rc::new(PlannedMutationTasks::new(
            prepared.plan_row_location_tasks()?,
        ));
        Ok(Self {
            prepared,
            tasks,
            conflict_filter,
        })
    }

    pub(crate) fn context(&self) -> IcebergModifyScanContext {
        IcebergModifyScanContext::new(
            self.prepared.starting_snapshot_id(),
            self.conflict_filter.clone(),
            Rc::clone(&self.tasks),
        )
    }

    pub(crate) fn rebind_reader_filter(&mut self, predicate: ReaderPredicate) {
        self.prepared.rebind_reader_filter(predicate);
    }

    pub(crate) fn open_cursor(
        &self,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
        table_oid: pg_sys::Oid,
    ) -> IcebergResult<ManagedMutationCursor> {
        let RowLocationScanInput { source, decoder } = self
            .prepared
            .open_row_location_scan(self.tasks.shared_tasks())?;
        Ok(ManagedMutationCursor::new(
            source, decoder, binding, table_oid,
        ))
    }
}
