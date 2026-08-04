//! Request-scoped READ workflow.
//!
//! [`RangeReader`] dispatches on the residency bound at OPEN time — one per handle — and
//! serves bytes without ever issuing a KV call. Small-KV reads slice an in-memory
//! `Arc<[u8]>`; complete-file reads open a file range by the meta's cached-file path;
//! large-fill reads drive chunk acquisition through the live session and stream the
//! resulting partial/complete file.

use std::sync::Arc;

use crate::cache::{
    CacheActivityGuard, CacheIndex, CacheState, ChunkFillClaim, LargeFillSession,
    ResidencyBody,
};
use crate::error::{StorageError, StorageResult};
use crate::handle::OpenFileState;
use crate::service::StorageService;
use crate::service::command::ReadCommand;
use crate::service::reply::{CommandOutput, ServiceReply};
use crate::session::handle_table::{HandleTable, ReadHandleGuard};

impl<I: CacheIndex> StorageService<I> {
    pub(super) async fn handle_read(
        &self,
        handles: &HandleTable,
        command: ReadCommand,
    ) -> StorageResult<ServiceReply> {
        let read_handle = handles.begin_read(command.handle)?;
        self.handle_admitted_read(&read_handle, command).await
    }

    pub(crate) async fn handle_admitted_read(
        &self,
        read_handle: &ReadHandleGuard,
        command: ReadCommand,
    ) -> StorageResult<ServiceReply> {
        let state = read_handle.state();
        if !state.flags.read {
            return Err(StorageError::unsupported("handle is not readable"));
        }
        if command.len == 0 || command.offset >= state.size {
            return Ok(ServiceReply::new(CommandOutput::read_bytes(
                bytes::Bytes::new(),
                true,
            )));
        }
        let len = command.len.min(self.max_read_size());

        RangeReader {
            service: self,
            state,
            offset: command.offset,
            len,
        }
        .run()
        .await
    }
}

/// Per-request state for [`StorageService::handle_admitted_read`].
struct RangeReader<'a, I: CacheIndex> {
    service: &'a StorageService<I>,
    state: &'a OpenFileState,
    offset: u64,
    len: u32,
}

impl<I: CacheIndex> RangeReader<'_, I> {
    async fn run(self) -> StorageResult<ServiceReply> {
        let Some(residency) = self.state.residency.clone() else {
            return Err(StorageError::cache(format!(
                "read on handle without bound cache residency: {}",
                self.state.key
            )));
        };
        match &residency.body {
            ResidencyBody::Small { payload, .. } => self.read_small(payload.clone()),
            ResidencyBody::Complete { meta } => {
                // `read_guard` (Read-kind activity) extends the active window from the handle's
                // lifetime to the wire-response lifetime. The handle's `OpenLease` keeps the
                // residency alive across concurrent CLOSEs, but once the READ task hands off
                // its reply, the handle can legally close mid-flight. The Read guard embedded
                // in the `ReadFileRange` attachment keeps `is_active(key)` true until the wire
                // writer has finished streaming the file range, so invalidation / eviction can
                // not retire the complete-file payload while a response is still being
                // serialized. This mirrors the LargeFill branch below.
                let read_guard = self.service.cache.read_guard(&self.state.key).await;
                let (file, offset, len, eof) = self
                    .service
                    .cache
                    .open_file_range_for_meta(
                        meta,
                        CacheState::CompleteFile,
                        self.offset,
                        self.len,
                    )
                    .await?;
                Ok(ServiceReply::new(CommandOutput::read_file_range(
                    file, offset, len, eof, read_guard,
                )))
            }
            ResidencyBody::LargeFill { session } => {
                // Same reasoning as the Complete branch: pins `is_active(key)` across the
                // wire-response lifetime, beyond the handle's own `OpenLease`.
                let read_guard = self.service.cache.read_guard(&self.state.key).await;
                self.read_from_large_fill(session.clone(), read_guard).await
            }
        }
    }

    fn read_small(self, payload: Arc<[u8]>) -> StorageResult<ServiceReply> {
        let (data, eof) = slice_small_range(payload, self.offset, self.len);
        Ok(ServiceReply::new(CommandOutput::read_bytes(data, eof)))
    }

    async fn read_from_large_fill(
        self,
        session: Arc<LargeFillSession>,
        read_guard: CacheActivityGuard,
    ) -> StorageResult<ServiceReply> {
        for chunk in
            self.service
                .cache
                .chunks_for_read(self.offset, self.len, self.state.size)
        {
            self.ensure_large_chunk(&session, chunk).await?;
        }

        let (file, offset, len, eof) = self
            .service
            .cache
            .open_large_range_for_session(&session, self.offset, self.len)
            .await?;
        Ok(ServiceReply::new(CommandOutput::read_file_range(
            file, offset, len, eof, read_guard,
        )))
    }

    async fn ensure_large_chunk(
        &self,
        session: &Arc<LargeFillSession>,
        chunk: u64,
    ) -> StorageResult<()> {
        loop {
            match session.claim_chunk(chunk).await? {
                ChunkFillClaim::Complete => return Ok(()),
                ChunkFillClaim::Follower(waiter) => {
                    if waiter.wait().await? {
                        return Ok(());
                    }
                }
                ChunkFillClaim::Leader(leader) => {
                    let _download_guard =
                        self.service.cache.download_guard(&self.state.key).await;
                    let info = session.info();
                    let range = self.service.cache.chunk_range_for(info.size, chunk);
                    let data = self
                        .state
                        .backend
                        .get_range(self.state.key.path(), range)
                        .await?;
                    self.service
                        .cache
                        .store_large_chunk_for_session(
                            session.clone(),
                            chunk,
                            &data,
                            leader,
                        )
                        .await?;
                    return Ok(());
                }
            }
        }
    }
}

fn slice_small_range(data: Arc<[u8]>, offset: u64, len: u32) -> (bytes::Bytes, bool) {
    let total = data.len();
    let start = std::cmp::min(offset as usize, total);
    let end = std::cmp::min(start + len as usize, total);
    let eof = end == total;
    let bytes = bytes::Bytes::from_owner(data);
    (bytes.slice(start..end), eof)
}

// End of RangeReader impl.
