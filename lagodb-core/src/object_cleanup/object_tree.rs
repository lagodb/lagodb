//! Observes storage objects that belong to a cleanup tree.

use std::time::Duration;

use lagodb_storage::{StorageClient, StorageResult};

use crate::storage::service::StorageEndpoint;

use super::ObjectTreeTarget;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectTreeStats {
    pub objects: u64,
    pub bytes: u64,
}

/// Paginated, format-neutral observer for a validated storage prefix.
pub struct ObjectTreeObserver {
    socket_path: std::path::PathBuf,
    timeout: Duration,
}

impl ObjectTreeObserver {
    pub fn connect(timeout: Duration) -> StorageResult<Self> {
        let endpoint = StorageEndpoint::from_pg_gucs()?.require_enabled()?;
        Ok(Self {
            socket_path: endpoint.socket_path().to_path_buf(),
            timeout,
        })
    }

    pub fn observe(
        &self,
        volume_id: crate::storage::volume::StorageVolumeId,
        namespace: &str,
        prefix: &str,
    ) -> StorageResult<ObjectTreeStats> {
        let target = ObjectTreeTarget::new(volume_id, namespace, prefix)?;
        let client = StorageClient::connect_managed_with_timeout(
            &self.socket_path,
            volume_id.get(),
            self.timeout,
        )?;
        let mut result = ObjectTreeStats::default();
        for entry in client.list(target.namespace(), Some(target.prefix())) {
            let entry = entry?;
            result.objects = result.objects.checked_add(1).ok_or_else(|| {
                lagodb_storage::StorageError::resource_exhausted(
                    "object-tree count exceeds u64",
                )
            })?;
            result.bytes = result.bytes.checked_add(entry.size).ok_or_else(|| {
                lagodb_storage::StorageError::resource_exhausted(
                    "object-tree byte count exceeds u64",
                )
            })?;
        }
        Ok(result)
    }
}
