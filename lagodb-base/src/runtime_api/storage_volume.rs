//! Runtime-owned storage-volume route resolver.

use lagodb_core::runtime_api::{
    StorageVolumeRouteOutput, VOLUME_ROUTE_ERROR, VOLUME_ROUTE_INVALID_REQUEST,
    VOLUME_ROUTE_NOT_FOUND, VOLUME_ROUTE_OK,
};
use lagodb_core::storage::volume::StorageVolumeId;
use pgrx::PgMemoryContexts;

#[pgrx::pg_guard]
pub(super) unsafe extern "C-unwind" fn resolve_storage_volume_route(
    volume_id: u64,
    output: *mut StorageVolumeRouteOutput,
) -> u32 {
    let Some(output) = (unsafe { output.as_mut() }) else {
        return VOLUME_ROUTE_INVALID_REQUEST;
    };
    *output = StorageVolumeRouteOutput::default();
    let Ok(volume_id) = StorageVolumeId::new(volume_id) else {
        output.error_message = unsafe {
            PgMemoryContexts::CurrentMemoryContext
                .pstrdup("storage volume id is outside the valid range")
        };
        return VOLUME_ROUTE_INVALID_REQUEST;
    };
    match crate::storage::volume_config::resolve_route(volume_id) {
        Ok(Some(route)) => {
            output.object_namespace = unsafe {
                PgMemoryContexts::CurrentMemoryContext
                    .pstrdup(route.object_namespace())
            };
            output.effective_base_uri = unsafe {
                PgMemoryContexts::CurrentMemoryContext
                    .pstrdup(route.effective_base_uri())
            };
            VOLUME_ROUTE_OK
        }
        Ok(None) => VOLUME_ROUTE_NOT_FOUND,
        Err(error) => {
            let message = error.diagnostic_message();
            output.error_message =
                unsafe { PgMemoryContexts::CurrentMemoryContext.pstrdup(&message) };
            VOLUME_ROUTE_ERROR
        }
    }
}
