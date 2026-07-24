//! Format-neutral physical object-cleanup execution.

use std::sync::atomic::{AtomicBool, Ordering};

use pg_lakebase_storage::{
    ListCursor, StorageClient, StorageError, StorageErrorKind,
};

use super::item::{ObjectCleanupItem, ObjectCleanupTarget};

#[derive(Debug, thiserror::Error)]
#[error("maintenance storage operation failed: {source}")]
pub(crate) struct ObjectCleanupExecutionError {
    #[source]
    source: StorageError,
}

impl ObjectCleanupExecutionError {
    pub(crate) fn new(source: StorageError) -> Self {
        Self { source }
    }
}

pub(crate) enum ObjectCleanupExecutionOutcome {
    Complete,
    Retryable(ObjectCleanupExecutionError),
    Permanent(ObjectCleanupExecutionError),
    Cancelled,
}

pub(crate) struct ObjectCleanupExecutor {
    page_size: u32,
}

impl ObjectCleanupExecutor {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size: u32::try_from(page_size).unwrap_or(u32::MAX),
        }
    }

    pub(crate) fn execute(
        &self,
        client: &StorageClient,
        item: &ObjectCleanupItem,
        cancelled: &AtomicBool,
    ) -> ObjectCleanupExecutionOutcome {
        match &item.target {
            ObjectCleanupTarget::Object {
                store_id,
                namespace,
                path,
            } => {
                if cancelled.load(Ordering::Acquire) {
                    return ObjectCleanupExecutionOutcome::Cancelled;
                }
                match client.delete(
                    store_id.as_str(),
                    namespace.as_str(),
                    path.as_str(),
                ) {
                    Ok(()) => ObjectCleanupExecutionOutcome::Complete,
                    Err(error) => classify(error),
                }
            }
            ObjectCleanupTarget::Tree {
                store_id,
                namespace,
                prefix,
            } => self.delete_tree(client, store_id, namespace, prefix, cancelled),
        }
    }

    fn delete_tree(
        &self,
        client: &StorageClient,
        store_id: &str,
        namespace: &str,
        prefix: &str,
        cancelled: &AtomicBool,
    ) -> ObjectCleanupExecutionOutcome {
        let mut cursor: Option<ListCursor> = None;
        loop {
            if cancelled.load(Ordering::Acquire) {
                close_cursor(client, cursor);
                return ObjectCleanupExecutionOutcome::Cancelled;
            }

            let request_cursor = cursor.clone();
            let page = match client.list_page(
                store_id,
                namespace,
                Some(prefix),
                request_cursor.clone(),
                self.page_size,
            ) {
                Ok(page) => page,
                Err(error) => {
                    close_cursor(client, request_cursor);
                    return classify(error);
                }
            };
            cursor = page.next_cursor;

            if cancelled.load(Ordering::Acquire) {
                close_cursor(client, cursor);
                return ObjectCleanupExecutionOutcome::Cancelled;
            }

            if !page.entries.is_empty() {
                let keys: Vec<String> =
                    page.entries.into_iter().map(|entry| entry.key).collect();
                let expected = u32::try_from(keys.len()).unwrap_or(u32::MAX);
                match client.delete_objects(store_id, namespace, keys) {
                    Ok(deleted) if deleted == expected => {}
                    Ok(deleted) => {
                        close_cursor(client, cursor);
                        return classify(StorageError::protocol(format!(
                            "bulk delete acknowledged {deleted} of {expected} objects"
                        )));
                    }
                    Err(error) => {
                        close_cursor(client, cursor);
                        return classify(error);
                    }
                }
            }

            if cursor.is_none() {
                return ObjectCleanupExecutionOutcome::Complete;
            }
        }
    }
}

fn close_cursor(client: &StorageClient, cursor: Option<ListCursor>) {
    if let Some(cursor) = cursor {
        let _ = client.close_list_cursor(cursor);
    }
}

fn classify(error: StorageError) -> ObjectCleanupExecutionOutcome {
    // Expired list cursors are retryable: DeleteTree can restart listing from the
    // root and deletion is idempotent.
    let permanent = matches!(
        error.kind(),
        StorageErrorKind::InvalidPath | StorageErrorKind::Unsupported
    );
    let error = ObjectCleanupExecutionError::new(error);
    if permanent {
        ObjectCleanupExecutionOutcome::Permanent(error)
    } else {
        ObjectCleanupExecutionOutcome::Retryable(error)
    }
}
