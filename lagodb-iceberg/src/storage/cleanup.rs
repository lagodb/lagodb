//! `FileIO` deletes issued from transaction-end callbacks.
//!
//! PostgreSQL runs these callbacks with interrupts held, so a remote delete
//! must use the storage cleanup requests, which wait under a deadline without
//! reconnecting. Each storage adapter LagoDB installs is dispatched explicitly;
//! an unrecognized one is rejected rather than falling back to an unbounded
//! remote request.

use iceberg_lite::io::FileIO;
use iceberg_lite::{Error, ErrorKind, Result};

use super::{LocalStorage, ObjectStorage};
use crate::foreign_table::catalog::rest::CatalogStorage;

pub(crate) trait FileIoCleanup {
    /// Deletes one file.
    fn delete_cleanup(&self, uri: &str) -> Result<()>;

    /// Removes a table directory, which only managed tables register.
    fn remove_dir_all_cleanup(&self, uri: &str) -> Result<()>;
}

impl FileIoCleanup for FileIO {
    fn delete_cleanup(&self, uri: &str) -> Result<()> {
        let storage = self.storage().as_any();
        if let Some(object) = storage.downcast_ref::<ObjectStorage>() {
            object.delete_cleanup(uri)
        } else if let Some(catalog) = storage.downcast_ref::<CatalogStorage>() {
            catalog.delete_cleanup(uri)
        } else if storage.is::<LocalStorage>() {
            // Ordinary filesystem deletion.
            self.delete(uri)
        } else {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "transaction-end file deletion does not support {} storage",
                    self.storage().scheme()
                ),
            ))
        }
    }

    fn remove_dir_all_cleanup(&self, uri: &str) -> Result<()> {
        let storage = self.storage().as_any();
        if let Some(object) = storage.downcast_ref::<ObjectStorage>() {
            object.remove_dir_all_cleanup(uri)
        } else if storage.is::<LocalStorage>() {
            // Ordinary filesystem deletion.
            self.remove_dir_all(uri)
        } else {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "transaction-end directory removal does not support {} storage",
                    self.storage().scheme()
                ),
            ))
        }
    }
}
