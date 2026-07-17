use std::time::Duration;

use pg_lakebase_storage::{StorageClient, StorageResult};

use crate::storage_service::StorageEndpoint;

use super::ObjectTreeTarget;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectTreeStats {
    pub objects: u64,
    pub bytes: u64,
}

/// Paginated, format-neutral observer for a validated storage prefix.
pub struct ObjectTreeObserver {
    client: StorageClient,
}

impl ObjectTreeObserver {
    pub fn connect(timeout: Duration) -> StorageResult<Self> {
        let endpoint = StorageEndpoint::from_pg_gucs()?.require_enabled()?;
        Ok(Self {
            client: StorageClient::connect_with_timeout(endpoint.socket_path(), timeout)?,
        })
    }

    pub fn observe(
        &self,
        store_id: &str,
        namespace: &str,
        prefix: &str,
    ) -> StorageResult<ObjectTreeStats> {
        let target = ObjectTreeTarget::new(store_id, namespace, prefix)?;
        let mut result = ObjectTreeStats::default();
        for entry in self.client.list(
            target.store_id().as_str(),
            target.namespace(),
            Some(target.prefix()),
        ) {
            let entry = entry?;
            result.objects = result.objects.checked_add(1).ok_or_else(|| {
                pg_lakebase_storage::StorageError::resource_exhausted(
                    "object-tree count exceeds u64",
                )
            })?;
            result.bytes = result.bytes.checked_add(entry.size).ok_or_else(|| {
                pg_lakebase_storage::StorageError::resource_exhausted(
                    "object-tree byte count exceeds u64",
                )
            })?;
        }
        Ok(result)
    }
}
