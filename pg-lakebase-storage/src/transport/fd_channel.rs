//! SCM_RIGHTS file-descriptor side channel.
//!
//! All `unsafe libc` lives here; callers interact through [`FdSender`] / [`FdReceiver`]. A single
//! byte of payload accompanies each message because some kernels refuse to deliver ancillary data
//! on a zero-length datagram.

use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream as StdUnixStream;

use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;

use crate::error::{StorageError, StorageResult};

const FD_TOKEN_BYTE: [u8; 1] = [0];

/// Sends a single owned file descriptor through the write half of a Unix stream.
pub struct FdSender<'a> {
    writer: &'a mut OwnedWriteHalf,
}

impl<'a> FdSender<'a> {
    pub fn new(writer: &'a mut OwnedWriteHalf) -> Self {
        Self { writer }
    }

    /// Retries while the socket is non-writable rather than returning `WouldBlock`.
    pub async fn send(&mut self, fd: RawFd) -> StorageResult<()> {
        loop {
            self.writer.writable().await?;
            match sendmsg_with_fd(
                self.writer.as_ref().as_raw_fd(),
                &FD_TOKEN_BYTE,
                fd,
            ) {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// Receives a single owned file descriptor from an async Unix stream.
pub struct FdReceiver<'a> {
    stream: &'a mut UnixStream,
}

impl<'a> FdReceiver<'a> {
    pub fn new(stream: &'a mut UnixStream) -> Self {
        Self { stream }
    }

    pub async fn recv(&mut self) -> StorageResult<OwnedFd> {
        let mut token = [0_u8; 1];
        loop {
            self.stream.readable().await?;
            match recvmsg_with_fd(self.stream.as_raw_fd(), &mut token) {
                Ok((0, _)) => {
                    return Err(StorageError::protocol(
                        "connection closed while receiving fd",
                    ));
                }
                Ok((_, Some(fd))) => return Ok(fd),
                Ok((_, None)) => {
                    return Err(StorageError::protocol(
                        "fd control message was missing fd",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// Blocking companion to [`FdReceiver`] for standalone blocking streams.
pub(super) fn recv_blocking(stream: &mut StdUnixStream) -> StorageResult<OwnedFd> {
    match try_recv_fd(stream) {
        Ok((0, _)) => Err(StorageError::protocol(
            "connection closed while receiving fd",
        )),
        Ok((_, Some(fd))) => Ok(fd),
        Ok((_, None)) => {
            Err(StorageError::protocol("fd control message was missing fd"))
        }
        Err(error) => Err(error.into()),
    }
}

/// Performs one nonblocking-compatible `recvmsg` attempt.
///
/// Callers using a nonblocking stream handle `WouldBlock` through their runtime
/// readiness integration and retry this operation.
pub(crate) fn try_recv_fd(
    stream: &StdUnixStream,
) -> io::Result<(usize, Option<OwnedFd>)> {
    let mut token = [0_u8; 1];
    recvmsg_with_fd(stream.as_raw_fd(), &mut token)
}

// ---- libc glue ---------------------------------------------------------------------------------

fn sendmsg_with_fd(socket: RawFd, packet: &[u8], fd: RawFd) -> io::Result<usize> {
    // SAFETY: all pointers are derived from stack locals whose lifetime covers the `sendmsg` call,
    // the control buffer is sized via `CMSG_SPACE` for exactly one `RawFd`, and the cmsg header
    // fields are set before `sendmsg` reads them.
    unsafe {
        let mut iov = libc::iovec {
            iov_base: packet.as_ptr() as *mut libc::c_void,
            iov_len: packet.len(),
        };
        let control_len = libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) as usize;
        assert!(control_len <= 64, "CMSG_SPACE exceeds stack control buffer");
        let mut control = [0_u8; 64];
        let mut msg: libc::msghdr = mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = control_len as _;

        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(io::Error::other("failed to allocate fd control message"));
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as u32) as _;
        std::ptr::copy_nonoverlapping(
            &fd as *const RawFd as *const u8,
            libc::CMSG_DATA(cmsg),
            mem::size_of::<RawFd>(),
        );

        let sent = libc::sendmsg(socket, &msg, 0);
        if sent < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(sent as usize)
        }
    }
}

fn recvmsg_with_fd(
    socket: RawFd,
    packet: &mut [u8],
) -> io::Result<(usize, Option<OwnedFd>)> {
    // SAFETY: symmetric to `sendmsg_with_fd`. `FromRawFd::from_raw_fd` takes ownership of the
    // received descriptor exactly once, so the caller is responsible for closing it via `OwnedFd`.
    unsafe {
        let mut iov = libc::iovec {
            iov_base: packet.as_mut_ptr() as *mut libc::c_void,
            iov_len: packet.len(),
        };
        let control_len = libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) as usize;
        assert!(control_len <= 64, "CMSG_SPACE exceeds stack control buffer");
        let mut control = [0_u8; 64];
        let mut msg: libc::msghdr = mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = control_len as _;

        let bytes_received = libc::recvmsg(socket, &mut msg, 0);
        if bytes_received < 0 {
            return Err(io::Error::last_os_error());
        }

        let mut received_fd = None;
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if !cmsg.is_null()
            && (*cmsg).cmsg_level == libc::SOL_SOCKET
            && (*cmsg).cmsg_type == libc::SCM_RIGHTS
        {
            let mut raw_fd: RawFd = -1;
            std::ptr::copy_nonoverlapping(
                libc::CMSG_DATA(cmsg),
                &mut raw_fd as *mut RawFd as *mut u8,
                mem::size_of::<RawFd>(),
            );
            if raw_fd >= 0 {
                received_fd = Some(OwnedFd::from_raw_fd(raw_fd));
            }
        }

        Ok((bytes_received as usize, received_fd))
    }
}
