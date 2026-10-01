//! WAL-ordered post-commit retirement of local generations and their reservations.

use std::iter::once;

use iceberg_lite::io::FileIO;
use lagodb_core::diag;
use lagodb_core::wal::XLogRecPtr;

use super::local_file_wal::record::{log_delete_directory, log_delete_files};
use super::{LocalStorage, LocalTableReservation};
use crate::error::{IcebergError, IcebergResult};

use self::transaction::LocalTableRetirements;

mod transaction;

#[derive(Debug)]
pub(crate) struct LocalTableRetirement {
    directory: String,
    reservation: Option<LocalTableReservation>,
    file_io: FileIO,
    needs_wal: bool,
}

impl LocalTableRetirement {
    pub(crate) fn register(
        directory: String,
        reservation: Option<LocalTableReservation>,
        file_io: FileIO,
    ) -> IcebergResult<()> {
        let storage = file_io
            .storage()
            .as_any()
            .downcast_ref::<LocalStorage>()
            .ok_or(IcebergError::InvariantViolated(
                "remote storage passed to local table-root cleanup",
            ))?;
        let needs_wal = storage.needs_wal();
        LocalTableRetirements::register(Self {
            directory,
            reservation,
            file_io,
            needs_wal,
        });
        Ok(())
    }

    fn log_delete_wal(&self) -> Option<XLogRecPtr> {
        if !self.needs_wal {
            return None;
        }
        // Both paths are PG-generated storage identities. The transaction
        // batch flushes all deletion records before directory removal or
        // reservation handoff to PostgreSQL's checkpoint unlink queue.
        // These remain separate post-commit records, not core xact WAL; a
        // crash before they are inserted still leaves an unreachable generation.
        let mut last_lsn = log_delete_directory(&self.directory);
        if let Some(reservation) = &self.reservation
            && let Some(lsn) = log_delete_files(once(reservation.as_str()))
        {
            last_lsn = lsn;
        }
        Some(last_lsn)
    }

    fn delete_storage(&self) {
        if let Err(error) = self.file_io.remove_dir_all(&self.directory) {
            diag::report_warning(format_args!(
                "failed to delete retired table directory '{}': {}",
                self.directory, error,
            ));
        }
        if let Some(reservation) = &self.reservation
            && let Err(error) = reservation.retire(&self.file_io, self.needs_wal)
        {
            diag::report_warning(format_args!(
                "failed to retire table reservation '{}': {}",
                reservation.as_str(),
                error,
            ));
        }
    }
}
