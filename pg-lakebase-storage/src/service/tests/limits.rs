//! Open-handle capacity tests for [`StorageService`].

use std::sync::Arc;

use crate::backend::{MemoryObjectBackend, StoreRegistry};
use crate::error::StorageError;
use crate::handle::OpenFlags;
use crate::service::StorageService;
use crate::service::command::{OpenCommand, StorageCommand};
use crate::session::handle_table::HandleTable;

use super::fixtures::{
    BUCKET, DEFAULT_STORE, LARGE_KEY, close, default_location,
    memory_cache_with_limits, open_file,
};

#[tokio::test]
async fn open_handle_limit_rejects_until_existing_handle_is_closed() {
    let backend = MemoryObjectBackend::new();
    backend.insert(default_location(LARGE_KEY), b"abc".to_vec());
    let cache = memory_cache_with_limits(8, 4);
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache,
    );
    let handles = HandleTable::with_max_open_handles(1);

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let error = match service
        .execute(
            &handles,
            StorageCommand::Open(OpenCommand {
                store_id: DEFAULT_STORE.to_string(),
                bucket: BUCKET.to_string(),
                key: LARGE_KEY.to_string(),
                flags: OpenFlags::READ_ONLY,
            }),
        )
        .await
    {
        Ok(_) => panic!("expected open limit error"),
        Err(error) => error,
    };

    assert!(matches!(error, StorageError::ResourceExhausted { .. }));

    close(&service, &handles, open.handle).await;

    let reopened = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    assert_ne!(reopened.handle, open.handle);
    close(&service, &handles, reopened.handle).await;
}
