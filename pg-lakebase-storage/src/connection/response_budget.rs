use std::io;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{StorageError, StorageResult};
use crate::protocol::{WireRequest, WireRequestPayload};

use super::dispatch::StorageHandlerResponse;

#[derive(Clone)]
pub(super) struct ResponseByteLimiter {
    semaphore: Arc<Semaphore>,
}

pub(super) struct ResponseBytes {
    _permit: Option<OwnedSemaphorePermit>,
}

pub(super) struct QueuedResponse {
    pub(super) response: StorageHandlerResponse,
    _response_bytes: ResponseBytes,
}

impl ResponseByteLimiter {
    pub(super) fn new(max_pending_bytes: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_pending_bytes)),
        }
    }

    pub(super) async fn acquire(&self, bytes: usize) -> StorageResult<ResponseBytes> {
        if bytes == 0 {
            return Ok(ResponseBytes { _permit: None });
        }
        let bytes = u32::try_from(bytes)
            .map_err(|_| StorageError::configuration(format!("response byte reservation too large: {bytes}")))?;
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(bytes)
            .await
            .map_err(|error| StorageError::io("response byte limiter closed", io::Error::other(error)))?;
        Ok(ResponseBytes { _permit: Some(permit) })
    }
}

impl QueuedResponse {
    pub(super) fn new(response: StorageHandlerResponse, response_bytes: ResponseBytes) -> Self {
        let response_bytes = if response.read_body_len().unwrap_or(0) == 0 {
            ResponseBytes { _permit: None }
        } else {
            response_bytes
        };
        Self {
            response,
            _response_bytes: response_bytes,
        }
    }
}

pub(super) fn reserved_read_response_bytes(request: &WireRequest, max_read_size: u32) -> usize {
    match request.payload {
        WireRequestPayload::Read { len, .. } => len.min(max_read_size) as usize,
        _ => 0,
    }
}
