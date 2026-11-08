//! Row-location target scan of a foreign UPDATE or DELETE.

use lagodb_core::fdw::{ForeignScanError, ScanSlotWriter};

use super::super::ForeignMutationScan;
use super::super::cursor::ForeignMutationCursor;
use crate::scan::{PreparedRowScan, ReaderPredicate};
use crate::write::RelationRowRegistry;

/// Begun target scan whose tasks are planned. BeginForeignModify reads its
/// context before Start opens the cursor with the first dynamic predicates.
pub(super) struct PreparedMutationScan {
    prepared: PreparedRowScan,
    registry: RelationRowRegistry,
    context: ForeignMutationScan,
}

impl PreparedMutationScan {
    pub(super) fn new(
        prepared: PreparedRowScan,
        registry: RelationRowRegistry,
        context: ForeignMutationScan,
    ) -> Self {
        Self {
            prepared,
            registry,
            context,
        }
    }

    pub(super) fn context(&self) -> ForeignMutationScan {
        self.context.clone()
    }

    pub(super) fn start(
        self,
        predicate: ReaderPredicate,
    ) -> Result<MutationTargetScan, ForeignScanError> {
        let Self {
            mut prepared,
            registry,
            context,
        } = self;
        prepared.rebind_reader_filter(predicate);
        let cursor = ForeignMutationCursor::new(
            context.open_row_location_scan(&prepared)?,
            registry,
        );
        Ok(MutationTargetScan {
            cursor,
            prepared,
            context,
        })
    }
}

/// Started target scan producing rows with their Iceberg row identity.
pub(super) struct MutationTargetScan {
    cursor: ForeignMutationCursor,
    prepared: PreparedRowScan,
    context: ForeignMutationScan,
}

impl MutationTargetScan {
    pub(super) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        self.cursor.next_slot(output)
    }

    /// Reread the same planned tasks under the current reader predicate.
    pub(super) fn rescan(
        &mut self,
        replacement: Option<ReaderPredicate>,
    ) -> Result<(), ForeignScanError> {
        if let Some(predicate) = replacement {
            self.prepared.rebind_reader_filter(predicate);
        }
        self.cursor
            .reopen(self.context.open_row_location_scan(&self.prepared)?);
        Ok(())
    }
}
