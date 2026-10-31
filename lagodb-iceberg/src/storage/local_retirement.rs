//! WAL-ordered post-commit retirement of local generations.

use iceberg_lite::io::FileIO;
use lagodb_core::diag;
use lagodb_core::wal::XLogRecPtr;

use super::LocalStorage;
use super::local_file_wal::record::log_delete_directory;
use crate::error::{IcebergError, IcebergResult};

use self::transaction::LocalTableRetirements;

mod transaction;

#[derive(Debug)]
pub(crate) struct LocalTableRetirement {
    directory: String,
    file_io: FileIO,
    needs_wal: bool,
}

impl LocalTableRetirement {
    pub(crate) fn register(directory: String, file_io: FileIO) -> IcebergResult<()> {
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
            file_io,
            needs_wal,
        });
        Ok(())
    }

    fn log_delete_wal(&self) -> Option<XLogRecPtr> {
        if !self.needs_wal {
            return None;
        }
        // The path is a PG-generated storage identity. The transaction batch
        // flushes all deletion records before directory removal. These remain
        // separate post-commit records, not core xact WAL; a crash before they
        // are inserted still leaves an unreachable generation.
        Some(log_delete_directory(&self.directory))
    }

    fn delete_storage(&self) {
        if let Err(error) = self.file_io.remove_dir_all(&self.directory) {
            diag::report_warning(format_args!(
                "failed to delete retired table directory '{}': {}",
                self.directory, error,
            ));
        }
    }
}
