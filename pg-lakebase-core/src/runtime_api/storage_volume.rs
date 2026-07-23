//! Runtime ABI for resolving immutable storage-volume routing metadata.

use std::ffi::{CStr, c_char};

use crate::storage_volume::{
    StorageVolumeId, StorageVolumeRoute, StorageVolumeRouteError,
};

use super::{RuntimeApiError, RuntimeClient};

pub const VOLUME_ROUTE_OK: u32 = 0;
pub const VOLUME_ROUTE_NOT_FOUND: u32 = 1;
pub const VOLUME_ROUTE_INVALID_REQUEST: u32 = 2;
pub const VOLUME_ROUTE_ERROR: u32 = 3;

/// Call-scoped output allocated in the caller's current PostgreSQL context.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct StorageVolumeRouteV1 {
    pub object_namespace: *const c_char,
    pub effective_base_uri: *const c_char,
    pub error_message: *const c_char,
}

impl Default for StorageVolumeRouteV1 {
    fn default() -> Self {
        Self {
            object_namespace: std::ptr::null(),
            effective_base_uri: std::ptr::null(),
            error_message: std::ptr::null(),
        }
    }
}

pub type ResolveStorageVolumeRouteCallback = unsafe extern "C-unwind" fn(
    volume_id: u64,
    output: *mut StorageVolumeRouteV1,
) -> u32;

#[derive(Debug, thiserror::Error)]
pub enum StorageVolumeRouteLookupError {
    #[error(transparent)]
    Runtime(#[from] RuntimeApiError),
    #[error("storage volume id {0} does not exist in the runtime config")]
    NotFound(StorageVolumeId),
    #[error("runtime failed to resolve storage volume id {volume_id}: {message}")]
    Resolution {
        volume_id: StorageVolumeId,
        message: String,
    },
    #[error("runtime returned invalid UTF-8 for storage volume id {0}")]
    InvalidUtf8(StorageVolumeId),
    #[error("runtime returned invalid routing for storage volume id {volume_id}")]
    InvalidRoute {
        volume_id: StorageVolumeId,
        #[source]
        source: StorageVolumeRouteError,
    },
    #[error("runtime returned unknown storage-volume route status {status}")]
    UnknownStatus { status: u32 },
}

impl RuntimeClient {
    pub fn storage_volume_route(
        self,
        volume_id: StorageVolumeId,
    ) -> Result<StorageVolumeRoute, StorageVolumeRouteLookupError> {
        let mut output = StorageVolumeRouteV1::default();
        let status = unsafe {
            (self.api.resolve_storage_volume_route)(
                volume_id.get(),
                std::ptr::from_mut(&mut output),
            )
        };
        match status {
            VOLUME_ROUTE_OK => {
                if output.object_namespace.is_null()
                    || output.effective_base_uri.is_null()
                {
                    return Err(StorageVolumeRouteLookupError::Resolution {
                        volume_id,
                        message: "runtime returned an incomplete route".to_owned(),
                    });
                }
                let namespace = unsafe { CStr::from_ptr(output.object_namespace) }
                    .to_str()
                    .map_err(|_| {
                        StorageVolumeRouteLookupError::InvalidUtf8(volume_id)
                    })?;
                let base_uri = unsafe { CStr::from_ptr(output.effective_base_uri) }
                    .to_str()
                    .map_err(|_| {
                        StorageVolumeRouteLookupError::InvalidUtf8(volume_id)
                    })?;
                StorageVolumeRoute::new(namespace, base_uri).map_err(|source| {
                    StorageVolumeRouteLookupError::InvalidRoute { volume_id, source }
                })
            }
            VOLUME_ROUTE_NOT_FOUND => {
                Err(StorageVolumeRouteLookupError::NotFound(volume_id))
            }
            VOLUME_ROUTE_INVALID_REQUEST | VOLUME_ROUTE_ERROR => {
                let message = if output.error_message.is_null() {
                    "runtime returned no error detail".to_owned()
                } else {
                    unsafe { CStr::from_ptr(output.error_message) }
                        .to_string_lossy()
                        .into_owned()
                };
                Err(StorageVolumeRouteLookupError::Resolution { volume_id, message })
            }
            status => Err(StorageVolumeRouteLookupError::UnknownStatus { status }),
        }
    }
}
