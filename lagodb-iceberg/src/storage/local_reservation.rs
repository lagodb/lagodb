//! PostgreSQL file-number reservation retained through the deletion WAL horizon.

use iceberg_lite::io::FileIO;
use lagodb_core::handles::RelFileLocator;

use crate::error::IcebergResult;

#[derive(Debug)]
pub(crate) struct LocalTableReservation {
    locator: RelFileLocator,
    backend: i32,
    path: String,
}

impl LocalTableReservation {
    pub(crate) fn new(locator: RelFileLocator, backend: i32, path: String) -> Self {
        Self {
            locator,
            backend,
            path,
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.path
    }

    /// Release after the retirement batch has flushed all deletion WAL.
    /// WAL-enabled reservations are already empty and need only PG's delayed
    /// unlink. WAL-free reservations retain their immediate cleanup policy.
    pub(crate) fn retire(
        &self,
        file_io: &FileIO,
        needs_wal: bool,
    ) -> IcebergResult<()> {
        // PG uses -1 for shared relation storage and nonnegative backend IDs
        // for temporary storage in both PG16 and PG17. MD's unlink handler
        // reconstructs only the shared path, without a temporary-backend prefix.
        if needs_wal && self.backend < 0 {
            self.locator.defer_main_fork_unlink()?;
        } else {
            // WAL-free retirement has no historical delete to protect against.
            // Immediate unlink also avoids leaving a reservation after a crash
            // that loses the checkpoint queue without any deletion WAL to redo.
            file_io.delete(&self.path)?;
        }
        Ok(())
    }
}
