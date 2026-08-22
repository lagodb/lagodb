//! Notifications owned by Iceberg automatic maintenance.

use pg_lakebase_core::extension_worker::{WorkerIdentity, WorkerNotifier};

use crate::error::{IcebergError, IcebergResult};

pub(crate) struct AutomaticMaintenanceNotifier;

impl AutomaticMaintenanceNotifier {
    const NOTIFIER: WorkerNotifier = WorkerNotifier::new(WorkerIdentity::new(
        c"pg_iceberg_am",
        c"iceberg_maintenance",
    ));

    pub(crate) fn stage_wakeup() -> IcebergResult<()> {
        Self::NOTIFIER.stage_wakeup().map_err(|source| {
            IcebergError::AutomaticMaintenanceNotification { source }
        })
    }
}
