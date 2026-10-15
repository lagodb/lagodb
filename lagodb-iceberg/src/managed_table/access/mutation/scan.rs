//! Managed-table mutation read ownership.

use std::rc::Rc;

use iceberg_lite::expr::Predicate;
use lagodb_core::access::mutation::ModifyScanBinding;
use lagodb_core::api::AmResult;
use lagodb_core::tuple::SlotColumns;
use pgrx::pg_sys;

use super::{
    IcebergModifyQueryState, IcebergModifyScanContext, ManagedMutationCursor,
};
use crate::error::IcebergResult;
use crate::scan::{PreparedRowScan, ReaderPredicate};
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
        let input = self
            .prepared
            .open_row_location_scan(self.tasks.shared_tasks())?;
        Ok(ManagedMutationCursor::new(input, binding, table_oid))
    }
}

/// One bound traversal shared by AM SeqScan and provider CustomScan adapters.
/// The cursor closes before the prepared read and its immutable task inventory.
pub(crate) struct ManagedMutationScan {
    cursor: ManagedMutationCursor,
    prepared: PreparedManagedMutationScan,
    binding: ModifyScanBinding<IcebergModifyQueryState>,
    relation_oid: pg_sys::Oid,
}

impl ManagedMutationScan {
    /// Open the cursor with the reader predicate already installed by its
    /// adapter, preserving the prepared task inventory and conflict scope.
    pub(crate) fn new(
        prepared: PreparedManagedMutationScan,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
        relation_oid: pg_sys::Oid,
    ) -> IcebergResult<Self> {
        let cursor = prepared.open_cursor(binding.clone(), relation_oid)?;
        Ok(Self {
            cursor,
            prepared,
            binding,
            relation_oid,
        })
    }

    pub(crate) fn binding(&self) -> &ModifyScanBinding<IcebergModifyQueryState> {
        &self.binding
    }

    pub(crate) fn context(&self) -> IcebergModifyScanContext {
        self.prepared.context()
    }

    pub(crate) fn rescan(
        &mut self,
        replacement: Option<ReaderPredicate>,
    ) -> IcebergResult<()> {
        if let Some(predicate) = replacement {
            self.prepared.rebind_reader_filter(predicate);
        }
        self.cursor = self
            .prepared
            .open_cursor(self.binding.clone(), self.relation_oid)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn next_into_slot(
        &mut self,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<bool> {
        self.cursor.next_into_slot(out)
    }
}
