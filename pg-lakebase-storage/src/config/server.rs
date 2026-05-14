//! Per-connection limits on concurrent requests, queued outbound frames, and socket drain
//! behavior.

use std::time::Duration;

use crate::error::{StorageError, StorageResult};
use crate::protocol::MAX_READ_RESPONSE_DATA_BYTES;

pub const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 256;
pub const DEFAULT_MAX_CONNECTIONS: usize = 1024;
pub const DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION: usize = 1024;
pub const DEFAULT_CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_MAX_PENDING_RESPONSES: usize = 64;
pub const DEFAULT_MAX_PENDING_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_RESPONSE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Limits concurrent requests, queued outbound frames, and socket drain behavior per accepted Unix
/// connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageServerConfig {
    pub max_in_flight_requests: usize,
    pub max_connections: usize,
    pub max_open_handles_per_connection: usize,
    pub connection_drain_timeout: Duration,
    pub max_pending_responses: usize,
    pub max_pending_response_bytes: usize,
    pub response_write_timeout: Option<Duration>,
}

impl Default for StorageServerConfig {
    fn default() -> Self {
        Self {
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_open_handles_per_connection: DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION,
            connection_drain_timeout: DEFAULT_CONNECTION_DRAIN_TIMEOUT,
            max_pending_responses: DEFAULT_MAX_PENDING_RESPONSES,
            max_pending_response_bytes: DEFAULT_MAX_PENDING_RESPONSE_BYTES,
            response_write_timeout: Some(DEFAULT_RESPONSE_WRITE_TIMEOUT),
        }
    }
}

impl StorageServerConfig {
    pub fn with_max_in_flight_requests(
        mut self,
        max_in_flight_requests: usize,
    ) -> Self {
        self.max_in_flight_requests = max_in_flight_requests.max(1);
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }

    pub fn with_max_open_handles_per_connection(
        mut self,
        max_open_handles_per_connection: usize,
    ) -> Self {
        self.max_open_handles_per_connection = max_open_handles_per_connection.max(1);
        self
    }

    pub fn with_connection_drain_timeout(
        mut self,
        connection_drain_timeout: Duration,
    ) -> Self {
        self.connection_drain_timeout = connection_drain_timeout;
        self
    }

    pub fn with_max_pending_responses(
        mut self,
        max_pending_responses: usize,
    ) -> Self {
        self.max_pending_responses = max_pending_responses.max(1);
        self
    }

    pub fn with_max_pending_response_bytes(
        mut self,
        max_pending_response_bytes: usize,
    ) -> Self {
        self.max_pending_response_bytes = max_pending_response_bytes.max(1);
        self
    }

    pub fn with_response_write_timeout(
        mut self,
        response_write_timeout: Duration,
    ) -> Self {
        self.response_write_timeout = Some(response_write_timeout);
        self.normalized()
    }

    pub fn without_response_write_timeout(mut self) -> Self {
        self.response_write_timeout = None;
        self
    }

    pub fn normalized(mut self) -> Self {
        self.max_in_flight_requests = self.max_in_flight_requests.max(1);
        self.max_connections = self.max_connections.max(1);
        self.max_open_handles_per_connection =
            self.max_open_handles_per_connection.max(1);
        self.max_pending_responses = self.max_pending_responses.max(1);
        self.max_pending_response_bytes = self.max_pending_response_bytes.max(1);
        if self.response_write_timeout == Some(Duration::ZERO) {
            self.response_write_timeout = None;
        }
        self
    }

    pub(crate) fn validate_for_max_read_size(
        &self,
        max_read_size: u32,
    ) -> StorageResult<()> {
        let max_read_size = max_read_size as usize;
        if max_read_size > MAX_READ_RESPONSE_DATA_BYTES {
            return Err(StorageError::configuration(format!(
                "max_read_size {max_read_size} exceeds maximum in-band READ payload size \
                 ({MAX_READ_RESPONSE_DATA_BYTES} bytes)"
            )));
        }
        if self.max_pending_response_bytes < max_read_size {
            return Err(StorageError::configuration(format!(
                "max_pending_response_bytes ({}) must be at least max_read_size ({max_read_size})",
                self.max_pending_response_bytes
            )));
        }
        if self.max_pending_response_bytes > u32::MAX as usize {
            return Err(StorageError::configuration(format!(
                "max_pending_response_bytes ({}) exceeds supported response byte budget ({})",
                self.max_pending_response_bytes,
                u32::MAX
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_normalizes_response_limits() {
        let config = StorageServerConfig {
            max_in_flight_requests: 0,
            max_connections: 0,
            max_open_handles_per_connection: 0,
            connection_drain_timeout: Duration::ZERO,
            max_pending_responses: 0,
            max_pending_response_bytes: 0,
            response_write_timeout: Some(Duration::ZERO),
        }
        .normalized();

        assert_eq!(config.max_in_flight_requests, 1);
        assert_eq!(config.max_connections, 1);
        assert_eq!(config.max_open_handles_per_connection, 1);
        assert_eq!(config.max_pending_responses, 1);
        assert_eq!(config.max_pending_response_bytes, 1);
        assert_eq!(config.response_write_timeout, None);
    }

    #[test]
    fn server_config_rejects_pending_response_budget_below_read_size() {
        let error = StorageServerConfig::default()
            .with_max_pending_response_bytes(4)
            .validate_for_max_read_size(8)
            .unwrap_err();

        assert!(matches!(error, StorageError::Configuration { .. }));
        assert!(error.wire_message().contains("max_pending_response_bytes"));
    }

    #[test]
    fn server_config_rejects_read_size_that_cannot_fit_in_frame() {
        let error = StorageServerConfig::default()
            .with_max_pending_response_bytes(MAX_READ_RESPONSE_DATA_BYTES + 1)
            .validate_for_max_read_size((MAX_READ_RESPONSE_DATA_BYTES + 1) as u32)
            .unwrap_err();

        assert!(matches!(error, StorageError::Configuration { .. }));
        assert!(
            error
                .wire_message()
                .contains("maximum in-band READ payload size")
        );
    }
}
