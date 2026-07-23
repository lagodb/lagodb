//! Construction and timeout policy for the synchronous storage client.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{StorageError, StorageResult};

use super::StorageClient;
use super::fd::ExternalFdPolicy;
use super::socket::ClientTransport;
use super::socket_wait::{PollSocketWait, SocketWait};

/// Maximum time spent closing a server-side handle from `Drop`.
///
/// This follows PostgreSQL's `postgres_fdw` cleanup budget: cleanup is allowed
/// to resynchronize a healthy connection, but cannot wait forever.
pub const DEFAULT_CLIENT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Builder for a synchronous [`StorageClient`].
pub struct StorageClientBuilder {
    socket_path: PathBuf,
    fd_policy: Option<Box<dyn ExternalFdPolicy>>,
    waiter: Box<dyn SocketWait>,
    operation_timeout: Option<Duration>,
    cleanup_timeout: Duration,
}

impl StorageClientBuilder {
    pub(super) fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            fd_policy: None,
            waiter: Box::new(PollSocketWait),
            operation_timeout: None,
            cleanup_timeout: DEFAULT_CLIENT_CLEANUP_TIMEOUT,
        }
    }

    /// Accounts for the connection socket and received direct-I/O descriptors.
    #[must_use]
    pub fn fd_policy(mut self, policy: Box<dyn ExternalFdPolicy>) -> Self {
        self.fd_policy = Some(policy);
        self
    }

    /// Replaces the default `poll(2)` waiter with an embedding-runtime waiter.
    #[must_use]
    pub fn socket_waiter(mut self, waiter: Box<dyn SocketWait>) -> Self {
        self.waiter = waiter;
        self
    }

    /// Bounds foreground request/response I/O with one absolute deadline per
    /// operation. The initial Unix-socket connect remains blocking.
    #[must_use]
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = Some(timeout);
        self
    }

    /// Bounds the best-effort Close RPC issued by `StorageFile::drop`.
    #[must_use]
    pub fn cleanup_timeout(mut self, timeout: Duration) -> Self {
        self.cleanup_timeout = timeout;
        self
    }

    /// Opens the configured Unix connection and switches established I/O to
    /// nonblocking mode.
    pub fn connect(self) -> StorageResult<StorageClient> {
        StorageClient::from_builder(self)
    }

    pub(super) fn into_parts(
        self,
    ) -> StorageResult<(ClientTransport, Option<Box<dyn ExternalFdPolicy>>)> {
        if self.operation_timeout == Some(Duration::ZERO) {
            return Err(StorageError::configuration(
                "storage client operation timeout must be greater than zero",
            ));
        }
        if self.cleanup_timeout == Duration::ZERO {
            return Err(StorageError::configuration(
                "storage client cleanup timeout must be greater than zero",
            ));
        }

        let socket_lease = self
            .fd_policy
            .as_ref()
            .map(|policy| policy.acquire())
            .transpose()?;
        let transport = ClientTransport::connect(
            &self.socket_path,
            socket_lease,
            self.waiter,
            self.operation_timeout,
            self.cleanup_timeout,
        )?;
        Ok((transport, self.fd_policy))
    }
}
