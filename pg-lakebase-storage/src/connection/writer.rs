use std::io;
use std::os::fd::AsRawFd;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, timeout, timeout_at};

use crate::error::{StorageError, StorageResult};
use crate::protocol::limits::READ_RESPONSE_PREFIX_BYTES;
use crate::protocol::{
    MAX_FRAME_BYTES, WireResponse, encode_read_response_prefix, encode_response,
};
use crate::service::reply::{ReadBody, ReadFileRange};
use crate::transport::{FdSender, write_frame};

use super::dispatch::{StorageHandlerPayload, StorageHandlerResponse};
use super::response_budget::QueuedResponse;

const FILE_RANGE_BUF_SIZE: usize = 64 * 1024;

/// Threshold below which a `ReadBody::Bytes` response is coalesced with its frame-length and
/// prefix into a single `write_all` syscall.
const COALESCE_THRESHOLD: usize = 8192;

/// Reusable buffers owned by the writer task, avoiding per-response heap allocations.
struct WriteBuffers {
    /// Scratch buffer for coalescing frame-length + prefix + small Bytes bodies into one write.
    coalesce: Vec<u8>,
    /// Scratch buffer for streaming file-range bodies to the socket.
    file_range: Vec<u8>,
}

impl WriteBuffers {
    fn new() -> Self {
        Self {
            coalesce: Vec::new(),
            file_range: Vec::new(),
        }
    }
}

/// Serializes outbound responses on a dedicated task so the read side never blocks on writes.
///
/// The response sender is held `Option`-wrapped so shutdown can drop it to signal the writer task
/// to complete, while the task handle remains joinable for draining or aborting.
pub(super) struct ResponseWriter {
    response_tx: Option<mpsc::Sender<QueuedResponse>>,
    task: JoinHandle<StorageResult<()>>,
}

impl ResponseWriter {
    pub(super) fn spawn(
        mut writer: OwnedWriteHalf,
        max_pending_responses: usize,
        response_write_timeout: Option<Duration>,
    ) -> Self {
        let (response_tx, mut response_rx) =
            mpsc::channel::<QueuedResponse>(max_pending_responses);
        let task = tokio::spawn(async move {
            let mut bufs = WriteBuffers::new();
            while let Some(queued_response) = response_rx.recv().await {
                write_queued_response(
                    &mut writer,
                    queued_response,
                    response_write_timeout,
                    &mut bufs,
                )
                .await?;
            }
            Ok(())
        });
        Self {
            response_tx: Some(response_tx),
            task,
        }
    }

    pub(super) fn sender(&self) -> mpsc::Sender<QueuedResponse> {
        self.response_tx
            .as_ref()
            .expect("response writer sender closed before connection shutdown")
            .clone()
    }

    /// Drops the outbound sender so the writer task observes channel close and exits once the queue
    /// drains.
    pub(super) fn close_sender(&mut self) {
        self.response_tx.take();
    }

    /// Awaits the writer task to completion, flattening the task's [`JoinError`] into a storage
    /// error so callers see a single result type.
    pub(super) async fn wait_finished(&mut self) -> StorageResult<()> {
        match (&mut self.task).await {
            Ok(result) => result,
            Err(error) => {
                Err(StorageError::from_join_error("writer task failed", error))
            }
        }
    }

    /// Waits for the writer task up to `drain_deadline`. Returns `Some` if the task completed in
    /// time (with its flattened result), `None` if the deadline elapsed and the task was aborted.
    pub(super) async fn wait_until(
        &mut self,
        drain_deadline: Instant,
    ) -> Option<StorageResult<()>> {
        match timeout_at(drain_deadline, &mut self.task).await {
            Ok(Ok(result)) => Some(result),
            Ok(Err(error)) => Some(Err(StorageError::from_join_error(
                "writer task failed",
                error,
            ))),
            Err(_) => {
                self.abort().await;
                None
            }
        }
    }

    pub(super) async fn abort(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }

    #[cfg(test)]
    pub(super) fn from_parts_for_test(
        response_tx: mpsc::Sender<QueuedResponse>,
        task: JoinHandle<StorageResult<()>>,
    ) -> Self {
        Self {
            response_tx: Some(response_tx),
            task,
        }
    }
}

async fn write_queued_response(
    writer: &mut OwnedWriteHalf,
    queued_response: QueuedResponse,
    response_write_timeout: Option<Duration>,
    bufs: &mut WriteBuffers,
) -> StorageResult<()> {
    let write = write_storage_response(writer, queued_response.response, bufs);
    match response_write_timeout {
        Some(duration) => match timeout(duration, write).await {
            Ok(result) => result,
            Err(_) => Err(StorageError::io(
                "response write timed out",
                io::Error::new(io::ErrorKind::TimedOut, "response write timed out"),
            )),
        },
        None => write.await,
    }
}

async fn write_storage_response(
    writer: &mut OwnedWriteHalf,
    service_response: StorageHandlerResponse,
    bufs: &mut WriteBuffers,
) -> StorageResult<()> {
    let StorageHandlerResponse {
        request_id,
        payload,
        direct_file,
    } = service_response;
    match payload {
        StorageHandlerPayload::Read { body, eof } => {
            write_read_response(writer, request_id, eof, body, bufs).await?;
        }
        StorageHandlerPayload::Wire(payload) => {
            let frame = encode_response(&WireResponse {
                request_id,
                payload,
            })?;
            write_frame(writer, &frame).await?;
        }
    }
    if let Some(file) = direct_file {
        FdSender::new(writer).send(file.as_raw_fd()).await?;
    }
    Ok(())
}

/// Frame length (4 bytes) + prefix header.
const FRAME_LEN_PLUS_PREFIX: usize = 4 + READ_RESPONSE_PREFIX_BYTES;

async fn write_read_response(
    writer: &mut OwnedWriteHalf,
    request_id: u64,
    eof: bool,
    body: ReadBody,
    bufs: &mut WriteBuffers,
) -> StorageResult<()> {
    let body_len = body.len();
    let prefix = encode_read_response_prefix(request_id, eof, body_len)?;
    let frame_len = prefix.len() + body_len;
    if frame_len > MAX_FRAME_BYTES {
        return Err(StorageError::protocol(format!(
            "frame too large: {frame_len}"
        )));
    }
    let frame_len_bytes = (frame_len as u32).to_be_bytes();
    match body {
        ReadBody::Bytes(data) if data.len() <= COALESCE_THRESHOLD => {
            let total = FRAME_LEN_PLUS_PREFIX + data.len();
            let buf = &mut bufs.coalesce;
            buf.clear();
            buf.reserve(total);
            buf.extend_from_slice(&frame_len_bytes);
            buf.extend_from_slice(&prefix);
            buf.extend_from_slice(&data);
            writer.write_all(buf).await?;
        }
        ReadBody::Bytes(data) => {
            let mut header = [0u8; FRAME_LEN_PLUS_PREFIX];
            header[..4].copy_from_slice(&frame_len_bytes);
            header[4..].copy_from_slice(&prefix);
            writer.write_all(&header).await?;
            writer.write_all(&data).await?;
        }
        ReadBody::FileRange(range) => {
            let mut header = [0u8; FRAME_LEN_PLUS_PREFIX];
            header[..4].copy_from_slice(&frame_len_bytes);
            header[4..].copy_from_slice(&prefix);
            writer.write_all(&header).await?;
            write_file_range_body(writer, range, &mut bufs.file_range).await?;
        }
    }
    writer.flush().await?;
    Ok(())
}

async fn write_file_range_body(
    writer: &mut OwnedWriteHalf,
    mut range: ReadFileRange,
    buffer: &mut Vec<u8>,
) -> StorageResult<()> {
    range.file.seek(io::SeekFrom::Start(range.offset)).await?;
    let mut remaining = range.len as usize;
    let needed = remaining.min(FILE_RANGE_BUF_SIZE);
    if buffer.len() < needed {
        buffer.resize(needed, 0);
    }
    while remaining > 0 {
        let read_len = remaining.min(buffer.len());
        let read = range.file.read(&mut buffer[..read_len]).await?;
        if read == 0 {
            return Err(StorageError::io(
                "cache file ended before queued read response was fully written",
                io::Error::new(io::ErrorKind::UnexpectedEof, "short cache file read"),
            ));
        }
        writer.write_all(&buffer[..read]).await?;
        remaining -= read;
    }
    Ok(())
}
