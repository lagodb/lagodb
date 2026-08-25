use std::error::Error as StdError;
use std::fmt;
use std::io::Read;
use std::time::Instant;

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, HeaderMap, TRANSFER_ENCODING};
use http::{Request, Response, StatusCode, Uri};
use iceberg_lite::catalog::rest::HttpTransport;
use iceberg_lite::{Error, ErrorKind, Result};
use ureq::config::{AutoHeaderValue, Config};
use ureq::unversioned::transport::{Connector, RustlsConnector};
use ureq::{Agent, RequestExt};

use super::connection::PostgresConnector;
use super::resolver::PostgresResolver;
use super::wait::check_deadline;

const MAX_IDLE_CONNECTIONS: usize = 16;
// Catalog load responses contain complete table metadata, so keep a generous
// process-local ceiling while preventing an untrusted endpoint from growing a
// backend allocation without bound.
const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024 * 1024;
// Read one sentinel byte past the accepted body size so an exact-limit body
// remains valid while an oversized body is detected without unbounded reads.
const MAX_RESPONSE_READ_BYTES: u64 = MAX_RESPONSE_BODY_BYTES as u64 + 1;

/// Backend-local HTTP/1.1 transport whose DNS and socket lifecycle follow
/// PostgreSQL backend constraints.
pub(crate) struct PgRestHttpTransport {
    agent: Agent,
}

impl PgRestHttpTransport {
    pub(crate) fn new() -> Result<Self> {
        let config = Config::builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .proxy(None)
            .user_agent(AutoHeaderValue::None)
            .accept(AutoHeaderValue::None)
            .accept_encoding(AutoHeaderValue::None)
            .max_idle_connections(MAX_IDLE_CONNECTIONS)
            .max_idle_connections_per_host(MAX_IDLE_CONNECTIONS)
            .build();
        let connector = PostgresConnector.chain(RustlsConnector::default());
        Ok(Self {
            agent: Agent::with_parts(config, connector, PostgresResolver::default()),
        })
    }

    fn execute_inner(
        &self,
        request: Request<Bytes>,
        deadline: Option<Instant>,
    ) -> Result<Response<Bytes>> {
        let endpoint = request.uri().clone();
        check_deadline(deadline).map_err(|source| {
            Self::request_error(source, &endpoint, "deadline_check")
        })?;
        let (parts, body) = request.into_parts();
        let request = Request::from_parts(parts, body.as_ref());
        let mut response = match deadline {
            Some(deadline) => request
                .with_agent(&self.agent)
                .configure()
                .timeout_global(Some(
                    deadline.saturating_duration_since(Instant::now()),
                ))
                .run()
                .map_err(|source| {
                    Self::request_error(source, &endpoint, "exchange")
                })?,
            None => request.with_agent(&self.agent).run().map_err(|source| {
                Self::request_error(source, &endpoint, "exchange")
            })?,
        };

        let status = response.status();
        let mut body = Vec::new();
        let read_result = response
            .body_mut()
            .as_reader()
            .take(MAX_RESPONSE_READ_BYTES)
            .read_to_end(&mut body);
        if let Err(source) = read_result {
            let error =
                Error::new(ErrorKind::IoError, "REST HTTP response body read failed")
                    .with_source(source);
            return Err(Self::with_response_context(
                error,
                &endpoint,
                status,
                response.headers(),
                body.len(),
            ));
        }
        if body.len() > MAX_RESPONSE_BODY_BYTES {
            // Content-Length proves whether unread bytes remain only when no
            // Transfer-Encoding overrides it. An absent, invalid or
            // inconsistent declaration must remain unknown; reaching the
            // sentinel itself is reported separately below.
            let truncated = if response.headers().contains_key(TRANSFER_ENCODING) {
                "unknown"
            } else {
                response
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .map_or("unknown", |length| {
                        if length > MAX_RESPONSE_READ_BYTES {
                            "true"
                        } else if length == MAX_RESPONSE_READ_BYTES {
                            "false"
                        } else {
                            "unknown"
                        }
                    })
            };
            let error = Self::with_response_context(
                Error::new(
                    ErrorKind::IoError,
                    "REST HTTP response body exceeds the backend memory budget",
                ),
                &endpoint,
                status,
                response.headers(),
                body.len(),
            )
            .with_context("limit_bytes", MAX_RESPONSE_BODY_BYTES.to_string())
            .with_context("limit_exceeded", "true")
            .with_context("read_stopped_at_limit", "true")
            .with_context("truncated", truncated);
            return Err(error);
        }
        let (parts, _) = response.into_parts();
        Ok(Response::from_parts(parts, Bytes::from(body)))
    }

    fn request_error(
        source: impl StdError + Send + Sync + 'static,
        endpoint: &Uri,
        phase: &'static str,
    ) -> Error {
        Error::new(ErrorKind::IoError, "REST HTTP request failed")
            .with_context("endpoint", endpoint.to_string())
            .with_context("phase", phase)
            .with_source(source)
    }

    fn with_response_context(
        error: Error,
        endpoint: &Uri,
        status: StatusCode,
        headers: &HeaderMap,
        bytes_read: usize,
    ) -> Error {
        let declared_content_length = headers
            .get(CONTENT_LENGTH)
            .map_or("<absent>", |value| value.to_str().unwrap_or("<invalid>"));
        error
            .with_context("endpoint", endpoint.to_string())
            .with_context("status", status.to_string())
            .with_context("phase", "response_body")
            .with_context("bytes_read", bytes_read.to_string())
            .with_context("declared_content_length", declared_content_length)
    }
}

impl fmt::Debug for PgRestHttpTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgRestHttpTransport")
            .finish_non_exhaustive()
    }
}

impl HttpTransport for PgRestHttpTransport {
    fn execute(
        &self,
        request: Request<Bytes>,
        deadline: Option<Instant>,
    ) -> Result<Response<Bytes>> {
        self.execute_inner(request, deadline)
    }
}
