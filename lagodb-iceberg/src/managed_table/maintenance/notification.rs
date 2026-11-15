//! Notifications owned by Iceberg automatic maintenance.

use lagodb_core::extension_worker::{WorkerLocator, WorkerNotifier};

use crate::managed_table::{ManagedResult, ManagedTableError};

pub(crate) struct AutomaticMaintenanceNotifier;

impl AutomaticMaintenanceNotifier {
    const NOTIFIER: WorkerNotifier = WorkerNotifier::new(WorkerLocator::new(
        c"lagodb_iceberg",
        c"iceberg_maintenance",
    ));

    pub(crate) fn stage_wakeup() -> ManagedResult<()> {
        Self::NOTIFIER.stage_wakeup().map_err(|source| {
            ManagedTableError::AutomaticMaintenanceNotification { source }
        })
    }
}
