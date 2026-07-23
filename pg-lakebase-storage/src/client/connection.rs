//! Blocking client connection state and external file-descriptor accounting.

use std::os::unix::net::UnixStream;

use crate::error::{StorageError, StorageResult};
use crate::handle::FileHandle;
use crate::protocol::{
    ReadResponsePrefix, ResponseFrameHeader, WireRequest, WireRequestPayload,
    WireResponsePayload, decode_response, encode_read_request, encode_request,
};
use crate::transport::{
    BlockingFrameCursor, read_fd_blocking, read_frame_blocking, write_frame_blocking,
};

use super::{ExternalFdLease, ExternalFdPolicy, unexpected_response};

pub(super) struct ReceivedFd {
    pub(super) fd: std::os::fd::OwnedFd,
    pub(super) lease: Option<Box<dyn ExternalFdLease>>,
}

enum ReadStartError {
    Connection(StorageError),
    Operation(StorageError),
}

pub(super) struct ClientConnection {
    // Drop the OS descriptor before releasing its accounting lease.
    stream: Option<UnixStream>,
    socket_lease: Option<Box<dyn ExternalFdLease>>,
    fd_policy: Option<Box<dyn ExternalFdPolicy>>,
    request_ids: RequestIdGenerator,
}

impl ClientConnection {
    pub(super) fn new(
        stream: UnixStream,
        socket_lease: Option<Box<dyn ExternalFdLease>>,
        fd_policy: Option<Box<dyn ExternalFdPolicy>>,
    ) -> Self {
        Self {
            stream: Some(stream),
            socket_lease,
            fd_policy,
            request_ids: RequestIdGenerator::new(),
        }
    }

    fn stream(&mut self) -> StorageResult<&mut UnixStream> {
        self.stream.as_mut().ok_or_else(|| {
            StorageError::protocol("storage client connection is poisoned")
        })
    }

    pub(super) fn is_usable(&self) -> bool {
        self.stream.is_some()
    }

    pub(super) fn poison(&mut self) {
        // Preserve drop order even when invalidating before ClientConnection
        // itself is dropped.
        drop(self.stream.take());
        drop(self.socket_lease.take());
    }

    fn poison_with<T>(&mut self, error: StorageError) -> StorageResult<T> {
        self.poison();
        Err(error)
    }

    fn acquire_direct_fd(&self) -> StorageResult<Option<Box<dyn ExternalFdLease>>> {
        self.fd_policy
            .as_ref()
            .map(|policy| policy.acquire())
            .transpose()
    }

    fn next_request_id(&mut self) -> u64 {
        self.request_ids.next()
    }

    pub(super) fn request(
        &mut self,
        payload: WireRequestPayload,
    ) -> StorageResult<(WireResponsePayload, Option<ReceivedFd>)> {
        let request_id = self.next_request_id();
        let request = WireRequest {
            request_id,
            payload,
        };
        let frame = encode_request(&request)?;
        if let Err(error) = write_frame_blocking(self.stream()?, &frame) {
            return self.poison_with(error);
        }
        let response_frame = match read_frame_blocking(self.stream()?) {
            Ok(Some(response)) => response,
            Ok(None) => {
                return self.poison_with(StorageError::protocol("connection closed"));
            }
            Err(error) => return self.poison_with(error),
        };
        let response = match decode_response(&response_frame) {
            Ok(response) => response,
            Err(error) => return self.poison_with(error),
        };
        if response.request_id != request_id {
            return self.poison_with(StorageError::protocol(format!(
                "response id {} did not match request id {request_id}",
                response.request_id
            )));
        }
        let payload = response.into_result()?;
        let fd = if matches!(
            payload,
            WireResponsePayload::Open {
                direct_io: true,
                ..
            }
        ) {
            let lease = match self.acquire_direct_fd() {
                Ok(lease) => lease,
                Err(error) => return self.poison_with(error),
            };
            let fd = match read_fd_blocking(self.stream()?) {
                Ok(fd) => fd,
                Err(error) => return self.poison_with(error),
            };
            Some(ReceivedFd { fd, lease })
        } else {
            None
        };
        Ok((payload, fd))
    }

    pub(super) fn read_into(
        &mut self,
        handle: FileHandle,
        offset: u64,
        len: u32,
        buf: &mut [u8],
    ) -> StorageResult<usize> {
        let result = (|| {
            let (mut response_frame, prefix) =
                self.start_read(handle, offset, len)?;
            if prefix.data_len > buf.len() {
                let error = match response_frame.discard_remaining() {
                    Ok(()) => StorageError::protocol(format!(
                        "read response data length {} exceeds caller buffer length {}",
                        prefix.data_len,
                        buf.len()
                    )),
                    Err(error) => error,
                };
                return Err(ReadStartError::Connection(error));
            }
            response_frame
                .read_exact(&mut buf[..prefix.data_len])
                .map_err(ReadStartError::Connection)?;
            Ok(prefix.data_len)
        })();
        self.finish_read(result)
    }

    pub(super) fn read_alloc(
        &mut self,
        handle: FileHandle,
        offset: u64,
        len: u32,
    ) -> StorageResult<Vec<u8>> {
        let result = (|| {
            let (mut response_frame, prefix) =
                self.start_read(handle, offset, len)?;
            let mut data = vec![0u8; prefix.data_len];
            response_frame
                .read_exact(&mut data)
                .map_err(ReadStartError::Connection)?;
            Ok(data)
        })();
        self.finish_read(result)
    }

    fn finish_read<T>(
        &mut self,
        result: Result<T, ReadStartError>,
    ) -> StorageResult<T> {
        match result {
            Ok(value) => Ok(value),
            Err(ReadStartError::Operation(error)) => Err(error),
            Err(ReadStartError::Connection(error)) => self.poison_with(error),
        }
    }

    /// Sends a READ request and decodes the response header/prefix, returning
    /// the cursor positioned at the response body.
    fn start_read(
        &mut self,
        handle: FileHandle,
        offset: u64,
        len: u32,
    ) -> Result<
        (
            BlockingFrameCursor<'_, std::os::unix::net::UnixStream>,
            ReadResponsePrefix,
        ),
        ReadStartError,
    > {
        let request_id = self.next_request_id();
        let stream = self.stream().map_err(ReadStartError::Connection)?;
        let frame = encode_read_request(request_id, handle, offset, len);
        write_frame_blocking(&mut *stream, &frame)
            .map_err(ReadStartError::Connection)?;

        let mut response_frame = BlockingFrameCursor::read_from(&mut *stream)
            .map_err(ReadStartError::Connection)?
            .ok_or_else(|| {
                ReadStartError::Connection(StorageError::protocol(
                    "connection closed",
                ))
            })?;
        let mut header_bytes = [0_u8; ResponseFrameHeader::ENCODED_LEN];
        response_frame
            .read_exact(&mut header_bytes)
            .map_err(ReadStartError::Connection)?;
        let header = ResponseFrameHeader::decode(&header_bytes)
            .map_err(ReadStartError::Connection)?;
        if header.request_id != request_id {
            response_frame
                .discard_remaining()
                .map_err(ReadStartError::Connection)?;
            return Err(ReadStartError::Connection(StorageError::protocol(format!(
                "response id {} did not match request id {request_id}",
                header.request_id
            ))));
        }

        if !header.is_read() {
            let response_frame = response_frame
                .read_remaining_after(&header_bytes)
                .map_err(ReadStartError::Connection)?;
            let response = decode_response(&response_frame)
                .map_err(ReadStartError::Connection)?;
            let other = response.into_result().map_err(ReadStartError::Operation)?;
            return Err(ReadStartError::Connection(unexpected_response(
                "read", &other,
            )));
        }

        let mut read_tail = [0_u8; ReadResponsePrefix::TAIL_LEN];
        response_frame
            .read_exact(&mut read_tail)
            .map_err(ReadStartError::Connection)?;
        let prefix = ReadResponsePrefix::decode_tail(header, &read_tail)
            .map_err(ReadStartError::Connection)?;
        if response_frame.remaining() != prefix.data_len {
            let remaining = response_frame.remaining();
            response_frame
                .discard_remaining()
                .map_err(ReadStartError::Connection)?;
            return Err(ReadStartError::Connection(StorageError::protocol(format!(
                "read response frame length mismatch: header announced {} data bytes, frame has {remaining}",
                prefix.data_len
            ))));
        }
        Ok((response_frame, prefix))
    }
}

struct RequestIdGenerator {
    next: u64,
}

impl RequestIdGenerator {
    const fn new() -> Self {
        Self { next: 1 }
    }

    fn next(&mut self) -> u64 {
        let request_id = self.next;
        self.next = self.next.wrapping_add(1);
        request_id
    }
}
