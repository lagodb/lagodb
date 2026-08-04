//! Synchronous client side of the mandatory connection attach handshake.

use crate::backend::BackendDataIdentity;
use crate::error::{StorageError, StorageResult};
use crate::protocol::{WireRequestPayload, WireResponsePayload};

use super::connection::ClientConnection;
use super::socket_wait::SocketWaitContext;
use super::unexpected_response;

pub(super) fn attach(
    connection: &mut ClientConnection,
    request: WireRequestPayload,
) -> StorageResult<BackendDataIdentity> {
    let (response, _) = connection.request(request, SocketWaitContext::Foreground)?;
    let WireResponsePayload::Attach { backend_identity } = response else {
        return Err(unexpected_response("attach", &response));
    };
    BackendDataIdentity::from_cache_key(&backend_identity).map_err(|error| {
        StorageError::protocol(format!(
            "invalid backend identity in attach response: {error}"
        ))
    })
}
