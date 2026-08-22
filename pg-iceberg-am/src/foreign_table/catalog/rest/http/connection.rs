use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::AsRawFd;
use std::time::Duration as StdDuration;

use pgrx::pg_sys;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use ureq::Error;
use ureq::config::Config;
use ureq::unversioned::resolver::ResolvedSocketAddrs;
use ureq::unversioned::transport::time::Duration;
use ureq::unversioned::transport::{
    Buffers, ConnectionDetails, Connector, LazyBuffers, NextTimeout, Transport,
};

use super::wait::{PostgresWait, SocketInterest};

#[derive(Debug, Default)]
pub(super) struct PostgresConnector;

impl<In: Transport> Connector<In> for PostgresConnector {
    type Out = PgTcpTransport;

    fn connect(
        &self,
        details: &ConnectionDetails<'_>,
        chained: Option<In>,
    ) -> Result<Option<Self::Out>, Error> {
        if chained.is_some() {
            return Err(Error::ConnectionFailed);
        }

        let (stream, lease) =
            Connection::open(&details.addrs, details.timeout, details.config)?;
        let buffers = LazyBuffers::new(
            details.config.input_buffer_size(),
            details.config.output_buffer_size(),
        );
        Ok(Some(PgTcpTransport {
            stream,
            _lease: lease,
            buffers,
            waiter: PostgresWait,
        }))
    }
}

struct Connection;

impl Connection {
    fn open(
        addrs: &ResolvedSocketAddrs,
        timeout: NextTimeout,
        config: &Config,
    ) -> Result<(TcpStream, ExternalFdLease), Error> {
        if matches!(timeout.after, Duration::Exact(duration) if duration.is_zero()) {
            return Err(Error::Timeout(timeout.reason));
        }
        let total_weight = 2.0 * (1.0 - 0.5_f64.powi(addrs.len() as i32));
        let mut weight = 1.0_f64;

        for addr in addrs {
            let attempt_timeout =
                Self::attempt_timeout(timeout, weight, total_weight);
            match Self::open_one(*addr, attempt_timeout, config.no_delay()) {
                Ok(connection) => return Ok(connection),
                Err(Error::Io(error))
                    if error.kind() == io::ErrorKind::ConnectionRefused => {}
                Err(Error::Timeout(_)) => {}
                Err(error) => return Err(error),
            }
            weight /= 2.0;
        }

        Err(Error::Io(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "failed to connect to any resolved REST endpoint address",
        )))
    }

    fn attempt_timeout(
        timeout: NextTimeout,
        weight: f64,
        total_weight: f64,
    ) -> NextTimeout {
        const MINIMUM: StdDuration = StdDuration::from_millis(10);

        let after = match timeout.after {
            Duration::NotHappening => Duration::NotHappening,
            Duration::Exact(duration) => {
                let seconds = duration.as_secs_f64() * weight / total_weight;
                Duration::Exact(StdDuration::from_secs_f64(seconds).max(MINIMUM))
            }
        };
        NextTimeout {
            after,
            reason: timeout.reason,
        }
    }

    fn open_one(
        addr: SocketAddr,
        timeout: NextTimeout,
        no_delay: bool,
    ) -> Result<(TcpStream, ExternalFdLease), Error> {
        let lease = ExternalFdLease::acquire()?;
        let domain = match addr {
            SocketAddr::V4(_) => Domain::IPV4,
            SocketAddr::V6(_) => Domain::IPV6,
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_nonblocking(true)?;

        match socket.connect(&SockAddr::from(addr)) {
            Ok(()) => {}
            Err(error) if Self::connect_in_progress(&error) => {
                PostgresWait.wait(
                    socket.as_raw_fd(),
                    SocketInterest::Writable,
                    timeout,
                )?;
                if let Some(error) = socket.take_error()? {
                    return Err(Error::Io(error));
                }
            }
            Err(error) => return Err(Error::Io(error)),
        }

        socket.set_tcp_nodelay(no_delay)?;
        Ok((socket.into(), lease))
    }

    fn connect_in_progress(error: &io::Error) -> bool {
        error.kind() == io::ErrorKind::WouldBlock
            || matches!(
                error.raw_os_error(),
                Some(libc::EINPROGRESS | libc::EALREADY | libc::EWOULDBLOCK)
            )
    }
}

pub(super) struct PgTcpTransport {
    // Field order is intentional: close the socket before returning its external
    // descriptor reservation to PostgreSQL.
    stream: TcpStream,
    _lease: ExternalFdLease,
    buffers: LazyBuffers,
    waiter: PostgresWait,
}

impl fmt::Debug for PgTcpTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgTcpTransport")
            .field("fd", &self.stream.as_raw_fd())
            .finish_non_exhaustive()
    }
}

impl Transport for PgTcpTransport {
    fn buffers(&mut self) -> &mut dyn Buffers {
        &mut self.buffers
    }

    fn transmit_output(
        &mut self,
        amount: usize,
        timeout: NextTimeout,
    ) -> Result<(), Error> {
        let mut written = 0;
        while written < amount {
            let result = self.stream.write(&self.buffers.output()[written..amount]);
            match result {
                Ok(0) => {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "REST endpoint closed the connection while writing",
                    )));
                }
                Ok(size) => written += size,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.waiter.wait(
                        self.stream.as_raw_fd(),
                        SocketInterest::Writable,
                        timeout,
                    )?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    pg_sys::check_for_interrupts!();
                }
                Err(error) => return Err(Error::Io(error)),
            }
        }
        Ok(())
    }

    fn await_input(&mut self, timeout: NextTimeout) -> Result<bool, Error> {
        loop {
            let result = self.stream.read(self.buffers.input_append_buf());
            match result {
                Ok(amount) => {
                    self.buffers.input_appended(amount);
                    return Ok(amount > 0);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.waiter.wait(
                        self.stream.as_raw_fd(),
                        SocketInterest::Readable,
                        timeout,
                    )?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    pg_sys::check_for_interrupts!();
                }
                Err(error) => return Err(Error::Io(error)),
            }
        }
    }

    fn is_open(&mut self) -> bool {
        let mut byte = [0];
        matches!(
            self.stream.peek(&mut byte),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock
        )
    }
}

struct ExternalFdLease;

impl ExternalFdLease {
    fn acquire() -> io::Result<Self> {
        // SAFETY: the REST transport is confined to the current backend thread.
        if unsafe { pg_sys::AcquireExternalFD() } {
            Ok(Self)
        } else {
            Err(io::Error::other(
                "PostgreSQL external file descriptor budget exhausted",
            ))
        }
    }
}

impl Drop for ExternalFdLease {
    fn drop(&mut self) {
        // SAFETY: every instance follows one successful AcquireExternalFD call.
        unsafe { pg_sys::ReleaseExternalFD() };
    }
}
