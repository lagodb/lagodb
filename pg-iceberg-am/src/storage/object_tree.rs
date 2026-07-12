use std::time::Duration;

use pg_lakebase_core::worker::storage as storage_worker;
use pg_lakebase_storage::{StorageClient, StorageResult};

pub(crate) struct ObjectTreeObserver {
    client: StorageClient,
}

impl ObjectTreeObserver {
    pub(crate) fn connect(timeout: Duration) -> StorageResult<Self> {
        let client = StorageClient::connect_with_timeout(
            storage_worker::resolved_socket_path(),
            timeout,
        )?;
        Ok(Self { client })
    }

    pub(crate) fn is_empty(
        &self,
        store_id: &str,
        namespace: &str,
        prefix: &str,
    ) -> StorageResult<bool> {
        let page =
            self.client
                .list_page(store_id, namespace, Some(prefix), None, 1)?;
        let is_empty = page.entries.is_empty();
        if let Some(cursor) = page.next_cursor {
            let _ = self.client.close_list_cursor(cursor);
        }
        Ok(is_empty)
    }
}
