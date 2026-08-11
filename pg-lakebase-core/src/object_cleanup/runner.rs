//! Format-neutral physical object-cleanup execution.

use std::sync::atomic::{AtomicBool, Ordering};

use pg_lakebase_storage::{StorageClient, StorageError, StorageErrorKind};

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
                volume_id: _,
                namespace,
                path,
            } => {
                if cancelled.load(Ordering::Acquire) {
                    return ObjectCleanupExecutionOutcome::Cancelled;
                }
                match client.delete(namespace.as_str(), path.as_str()) {
                    Ok(()) => ObjectCleanupExecutionOutcome::Complete,
                    Err(error) => classify(error),
                }
            }
            ObjectCleanupTarget::Tree {
                volume_id: _,
                namespace,
                prefix,
            } => self.delete_tree(client, namespace, prefix, cancelled),
        }
    }

    fn delete_tree(
        &self,
        client: &StorageClient,
        namespace: &str,
        prefix: &str,
        cancelled: &AtomicBool,
    ) -> ObjectCleanupExecutionOutcome {
        let mut listing = client.list_session(namespace, Some(prefix), self.page_size);
        loop {
            if cancelled.load(Ordering::Acquire) {
                return ObjectCleanupExecutionOutcome::Cancelled;
            }

            let entries = match listing.next_page() {
                Ok(Some(entries)) => entries,
                Ok(None) => return ObjectCleanupExecutionOutcome::Complete,
                Err(error) => return classify(error),
            };

            if cancelled.load(Ordering::Acquire) {
                return ObjectCleanupExecutionOutcome::Cancelled;
            }

            if !entries.is_empty() {
                let keys: Vec<String> = entries.into_iter().map(|entry| entry.key).collect();
                let expected = u32::try_from(keys.len()).unwrap_or(u32::MAX);
                match client.delete_objects(namespace, keys) {
                    Ok(deleted) if deleted == expected => {}
                    Ok(deleted) => {
                        return classify(StorageError::protocol(format!(
                            "bulk delete acknowledged {deleted} of {expected} objects"
                        )));
                    }
                    Err(error) => return classify(error),
                }
            }

            if listing.is_exhausted() {
                return ObjectCleanupExecutionOutcome::Complete;
            }
        }
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
