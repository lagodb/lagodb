//! Synchronous client protocol state and external file-descriptor accounting.

use crate::error::{StorageError, StorageResult};
use crate::handle::FileHandle;
use crate::protocol::{
    ReadResponsePrefix, ResponseFrameHeader, WireRequest, WireRequestPayload,
    WireResponsePayload, decode_response, encode_read_request, encode_request,
};
use crate::transport::{
    BlockingFrameCursor, read_frame_blocking, write_frame_blocking,
};

use super::socket::{ClientIo, ClientTransport};
use super::socket_wait::SocketWaitContext;
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
    transport: ClientTransport,
    fd_policy: Option<Box<dyn ExternalFdPolicy>>,
    request_ids: RequestIdGenerator,
}

impl ClientConnection {
    pub(super) fn new(
        transport: ClientTransport,
        fd_policy: Option<Box<dyn ExternalFdPolicy>>,
    ) -> Self {
        Self {
            transport,
            fd_policy,
            request_ids: RequestIdGenerator::new(),
        }
    }

    pub(super) fn is_usable(&self) -> bool {
        self.transport.is_usable()
    }

    pub(super) fn poison(&mut self) {
        self.transport.poison();
    }

    fn next_request_id(&mut self) -> u64 {
        self.request_ids.next()
    }

    pub(super) fn request(
        &mut self,
        payload: WireRequestPayload,
        context: SocketWaitContext,
    ) -> StorageResult<(WireResponsePayload, Option<ReceivedFd>)> {
        let request_id = self.next_request_id();
        let request = WireRequest {
            request_id,
            payload,
        };
        let frame = encode_request(&request)?;
        let fd_policy = &self.fd_policy;
        let mut io = self.transport.session(context)?;

        write_frame_blocking(&mut io, &frame)?;
        let response_frame = read_frame_blocking(&mut io)?
            .ok_or_else(|| StorageError::protocol("connection closed"))?;
        let response = decode_response(&response_frame)?;
        if response.request_id != request_id {
            return Err(StorageError::protocol(format!(
                "response id {} did not match request id {request_id}",
                response.request_id
            )));
        }
        let payload = match response.into_result() {
            Ok(payload) => payload,
            Err(error) => {
                io.finish();
                return Err(error);
            }
        };
        let fd = if matches!(
            payload,
            WireResponsePayload::Open {
                direct_io: true,
                ..
            }
        ) {
            let (fd, lease) = io.recv_fd(fd_policy.as_deref())?;
            Some(ReceivedFd { fd, lease })
        } else {
            None
        };
        io.finish();
        Ok((payload, fd))
    }

    pub(super) fn read_into(
        &mut self,
        handle: FileHandle,
        offset: u64,
        len: u32,
        buf: &mut [u8],
    ) -> StorageResult<usize> {
        let request_id = self.next_request_id();
        let mut io = self.transport.session(SocketWaitContext::Foreground)?;
        let result = (|| {
            let (mut response_frame, prefix) =
                Self::start_read(&mut io, request_id, handle, offset, len)?;
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
        Self::finish_read(io, result)
    }

    pub(super) fn read_alloc(
        &mut self,
        handle: FileHandle,
        offset: u64,
        len: u32,
    ) -> StorageResult<Vec<u8>> {
        let request_id = self.next_request_id();
        let mut io = self.transport.session(SocketWaitContext::Foreground)?;
        let result = (|| {
            let (mut response_frame, prefix) =
                Self::start_read(&mut io, request_id, handle, offset, len)?;
            let mut data = vec![0_u8; prefix.data_len];
            response_frame
                .read_exact(&mut data)
                .map_err(ReadStartError::Connection)?;
            Ok(data)
        })();
        Self::finish_read(io, result)
    }

    /// Sends a READ request and decodes the response header/prefix, returning
    /// the cursor positioned at the response body.
    fn start_read<'io, 'transport>(
        io: &'io mut ClientIo<'transport>,
        request_id: u64,
        handle: FileHandle,
        offset: u64,
        len: u32,
    ) -> Result<
        (
            BlockingFrameCursor<'io, ClientIo<'transport>>,
            ReadResponsePrefix,
        ),
        ReadStartError,
    > {
        let frame = encode_read_request(request_id, handle, offset, len);
        write_frame_blocking(&mut *io, &frame).map_err(ReadStartError::Connection)?;

        let mut response_frame = BlockingFrameCursor::read_from(&mut *io)
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

    fn finish_read<T>(
        io: ClientIo<'_>,
        result: Result<T, ReadStartError>,
    ) -> StorageResult<T> {
        match result {
            Ok(value) => {
                io.finish();
                Ok(value)
            }
            Err(ReadStartError::Operation(error)) => {
                io.finish();
                Err(error)
            }
            Err(ReadStartError::Connection(error)) => Err(error),
        }
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
