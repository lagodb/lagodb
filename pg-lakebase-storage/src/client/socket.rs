//! Runtime-neutral nonblocking I/O for an established synchronous client socket.

use std::io::{self, IoSlice, Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::error::{StorageError, StorageResult};
use crate::transport::try_recv_fd;

use super::fd::{ExternalFdLease, ExternalFdPolicy};
use super::socket_wait::{SocketInterest, SocketWait, SocketWaitContext};

struct SocketGeneration {
    // Drop runtime readiness resources before the socket they reference.
    waiter: Box<dyn SocketWait>,
    // Drop the OS descriptor before releasing its accounting lease.
    stream: UnixStream,
    _socket_lease: Option<Box<dyn ExternalFdLease>>,
}

pub(super) struct ClientTransport {
    generation: Option<SocketGeneration>,
    operation_timeout: Option<Duration>,
    cleanup_timeout: Duration,
}

impl ClientTransport {
    pub(super) fn connect(
        path: &Path,
        socket_lease: Option<Box<dyn ExternalFdLease>>,
        waiter: Box<dyn SocketWait>,
        operation_timeout: Option<Duration>,
        cleanup_timeout: Duration,
    ) -> StorageResult<Self> {
        let stream = UnixStream::connect(path)?;
        stream.set_nonblocking(true)?;

        Ok(Self {
            generation: Some(SocketGeneration {
                waiter,
                stream,
                _socket_lease: socket_lease,
            }),
            operation_timeout,
            cleanup_timeout,
        })
    }

    pub(super) fn is_usable(&self) -> bool {
        self.generation.is_some()
    }

    pub(super) fn poison(&mut self) {
        drop(self.generation.take());
    }

    pub(super) fn session(
        &mut self,
        context: SocketWaitContext,
    ) -> StorageResult<ClientIo<'_>> {
        if self.generation.is_none() {
            return Err(StorageError::protocol(
                "storage client connection is poisoned",
            ));
        }
        let timeout = match context {
            SocketWaitContext::Foreground => self.operation_timeout,
            SocketWaitContext::Cleanup => Some(self.cleanup_timeout),
        };
        Ok(ClientIo {
            transport: self,
            context,
            deadline: Self::deadline_after(timeout)?,
            completed: false,
        })
    }

    fn wait(
        &mut self,
        interest: SocketInterest,
        context: SocketWaitContext,
        deadline: Option<Instant>,
    ) -> io::Result<()> {
        let generation = self.generation.as_mut().ok_or_else(Self::poisoned_error)?;
        generation
            .waiter
            .wait(generation.stream.as_fd(), interest, context, deadline)
    }

    fn stream(&mut self) -> io::Result<&mut UnixStream> {
        self.generation
            .as_mut()
            .map(|generation| &mut generation.stream)
            .ok_or_else(Self::poisoned_error)
    }

    fn deadline_after(timeout: Option<Duration>) -> io::Result<Option<Instant>> {
        timeout
            .map(|timeout| {
                Instant::now().checked_add(timeout).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "storage socket timeout exceeds Instant range",
                    )
                })
            })
            .transpose()
    }

    fn poisoned_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::NotConnected,
            "storage client connection is poisoned",
        )
    }
}

pub(super) struct ClientIo<'a> {
    transport: &'a mut ClientTransport,
    context: SocketWaitContext,
    deadline: Option<Instant>,
    completed: bool,
}

impl ClientIo<'_> {
    pub(super) fn finish(mut self) {
        self.completed = true;
    }

    pub(super) fn recv_fd(
        &mut self,
        fd_policy: Option<&dyn ExternalFdPolicy>,
    ) -> StorageResult<(OwnedFd, Option<Box<dyn ExternalFdLease>>)> {
        loop {
            self.check_deadline()?;
            let lease = fd_policy.map(|policy| policy.acquire()).transpose()?;
            let received = try_recv_fd(self.transport.stream()?);
            match received {
                Ok((0, _)) => {
                    return Err(StorageError::protocol(
                        "connection closed while receiving fd",
                    ));
                }
                Ok((_, Some(fd))) => return Ok((fd, lease)),
                Ok((_, None)) => {
                    return Err(StorageError::protocol(
                        "fd control message was missing fd",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    // PostgreSQL interrupt processing may raise ERROR, so
                    // release this not-yet-backed reservation before waiting.
                    drop(lease);
                    self.wait(SocketInterest::Readable)?;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    drop(lease);
                    self.wait(SocketInterest::Readable)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn wait(&mut self, interest: SocketInterest) -> io::Result<()> {
        self.transport.wait(interest, self.context, self.deadline)
    }

    fn check_deadline(&self) -> io::Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "storage socket operation timed out",
            ))
        } else {
            Ok(())
        }
    }
}

impl Read for ClientIo<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            self.check_deadline()?;
            match self.transport.stream()?.read(buf) {
                Ok(read) => return Ok(read),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    self.wait(SocketInterest::Readable)?;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait(SocketInterest::Readable)?;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Write for ClientIo<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        loop {
            self.check_deadline()?;
            match self.transport.stream()?.write(buf) {
                Ok(written) => return Ok(written),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    self.wait(SocketInterest::Writable)?;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait(SocketInterest::Writable)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        loop {
            self.check_deadline()?;
            match self.transport.stream()?.write_vectored(bufs) {
                Ok(written) => return Ok(written),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    self.wait(SocketInterest::Writable)?;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait(SocketInterest::Writable)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for ClientIo<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.transport.poison();
        }
    }
}
