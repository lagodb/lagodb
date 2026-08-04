//! Mandatory single-context handshake executed before request multiplexing starts.

use std::sync::Arc;
use std::time::Instant;

use tokio::net::UnixStream;

use crate::cache::CacheIndex;
use crate::error::{StorageError, StorageResult};
use crate::protocol::{
    WireResponse, WireResponsePayload, decode_request, encode_response,
};
use crate::request::{RequestContext, RequestHooks, RequestOutcome};
use crate::service::StorageService;
use crate::session::AttachedStorageContext;
use crate::transport::{read_frame, write_frame};

pub(crate) async fn attach<I: CacheIndex>(
    mut stream: UnixStream,
    service: &Arc<StorageService<I>>,
    request_hooks: &RequestHooks,
    client_addr: Arc<str>,
) -> StorageResult<(UnixStream, AttachedStorageContext)> {
    let frame = read_frame(&mut stream)
        .await?
        .ok_or_else(|| StorageError::protocol("connection closed before attach"))?;
    let request = decode_request(&frame)?;
    let request_id = request.request_id;
    let request_context =
        RequestContext::new(request_id, client_addr, &request.payload);
    let started = Instant::now();
    request_hooks.observer().on_request_start(&request_context);
    let resolved = request_hooks
        .policy()
        .before_dispatch(&request_context)
        .and_then(|()| service.resolve_attach(request.payload));
    let attached = match resolved {
        Ok(attached) => attached,
        Err(error) => {
            request_hooks.observer().on_request_finish(
                &request_context,
                &RequestOutcome::error(error.kind(), started.elapsed()),
            );
            let response = WireResponse::error(request_id, error);
            write_frame(&mut stream, &encode_response(&response)?).await?;
            return Err(StorageError::protocol("storage context attach rejected"));
        }
    };
    request_hooks.observer().on_request_finish(
        &request_context,
        &RequestOutcome::success(started.elapsed()),
    );
    let response = WireResponse {
        request_id,
        payload: WireResponsePayload::Attach {
            backend_identity: attached.identity().cache_key().to_owned(),
        },
    };
    write_frame(&mut stream, &encode_response(&response)?).await?;
    Ok((stream, attached))
}
