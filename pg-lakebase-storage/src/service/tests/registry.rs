//! Managed attach control-path tests.

use std::sync::Arc;

use crate::backend::{
    BackendDataIdentity, ManagedStoreRegistry, MemoryObjectBackend,
};
use crate::protocol::WireRequestPayload;
use crate::service::StorageService;

use super::fixtures::{TEST_VOLUME_ID, memory_cache};

#[tokio::test]
async fn managed_attach_resolves_the_runtime_owned_slot() {
    let registry = ManagedStoreRegistry::new();
    registry
        .register_backend(
            TEST_VOLUME_ID,
            BackendDataIdentity::memory("managed"),
            Arc::new(MemoryObjectBackend::new()),
        )
        .unwrap();
    let service = StorageService::with_registry(registry, memory_cache());

    let attached = service
        .resolve_attach(WireRequestPayload::AttachManaged {
            volume_id: TEST_VOLUME_ID,
        })
        .unwrap();

    assert_eq!(attached.identity(), &BackendDataIdentity::memory("managed"));
}

#[tokio::test]
async fn unknown_managed_volume_is_rejected_at_attach() {
    let service =
        StorageService::with_registry(ManagedStoreRegistry::new(), memory_cache());

    let error = match service
        .resolve_attach(WireRequestPayload::AttachManaged { volume_id: 99 })
    {
        Ok(_) => panic!("unknown managed volume unexpectedly attached"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), crate::error::StorageErrorKind::NotFound);
}
