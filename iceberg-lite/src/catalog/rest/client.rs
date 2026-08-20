// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Synchronous REST HTTP client above an injected transport.

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::header::{HeaderMap, HeaderName, LOCATION};
use http::{Method, Request, Response, StatusCode};
use serde::de::DeserializeOwned;
use url::Url;

use super::auth::{AuthSession, NoopSession};
use super::catalog::RestCatalogConfig;
use super::request::{HttpRequest, HttpRequestBuilder};
use super::types::{ErrorResponse, RestError, RestErrorKind};
use crate::{Error, ErrorKind, Result};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REDIRECTS: usize = 10;

/// Executes one HTTP exchange. Redirects and authentication are handled above this boundary.
pub trait HttpTransport: Debug + Send + Sync {
    /// Executes one request before the optional absolute deadline.
    fn execute(
        &self,
        request: Request<Bytes>,
        deadline: Option<Instant>,
    ) -> Result<Response<Bytes>>;
}

/// REST client sharing one transport and authentication session.
#[derive(Clone)]
pub struct HttpClient {
    transport: Arc<dyn HttpTransport>,
    extra_headers: HeaderMap,
    redirect_restricted_headers: HashSet<HeaderName>,
    disable_header_redaction: bool,
    auth_session: Arc<dyn AuthSession>,
}

impl Debug for HttpClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpClient")
            .field(
                "extra_headers",
                &format_headers_redacted(
                    &self.extra_headers,
                    self.disable_header_redaction,
                ),
            )
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    pub(crate) fn new(config: &RestCatalogConfig) -> Result<Self> {
        Ok(Self {
            transport: Arc::clone(config.transport()?),
            extra_headers: config.extra_headers()?,
            redirect_restricted_headers: config
                .explicit_headers()?
                .keys()
                .cloned()
                .collect(),
            disable_header_redaction: config.disable_header_redaction(),
            auth_session: Arc::new(NoopSession),
        })
    }

    /// Returns a client using the supplied authentication session.
    pub fn with_auth_session(&self, auth_session: Arc<dyn AuthSession>) -> Self {
        Self {
            auth_session,
            ..self.clone()
        }
    }

    /// Returns a client that sends no authentication.
    pub fn without_auth_session(&self) -> Self {
        self.with_auth_session(Arc::new(NoopSession))
    }

    pub(crate) fn update_with(self, config: &RestCatalogConfig) -> Result<Self> {
        let configured_headers = config.extra_headers()?;
        let configured_restricted_headers: HashSet<_> =
            config.explicit_headers()?.keys().cloned().collect();
        Ok(Self {
            transport: Arc::clone(config.transport()?),
            extra_headers: if configured_headers.is_empty() {
                self.extra_headers
            } else {
                configured_headers
            },
            redirect_restricted_headers: if configured_restricted_headers.is_empty() {
                self.redirect_restricted_headers
            } else {
                configured_restricted_headers
            },
            disable_header_redaction: config.disable_header_redaction(),
            auth_session: self.auth_session,
        })
    }

    #[inline]
    pub(crate) fn request(
        &self,
        method: Method,
        url: impl AsRef<str>,
    ) -> HttpRequestBuilder {
        HttpRequestBuilder::from_str(method, url.as_ref())
            .headers(self.extra_headers.clone())
    }

    pub(crate) fn query_catalog(
        &self,
        mut request: HttpRequest,
    ) -> Result<Response<Bytes>> {
        self.auth_session.authenticate(&mut request)?;
        request.headers_mut().extend(self.extra_headers.clone());
        self.execute_redirects(request, Some(&self.extra_headers))
    }

    /// Sends a form POST for an authentication exchange.
    pub fn post_form(
        &self,
        url: &str,
        headers: &HeaderMap,
        form: &HashMap<&str, &str>,
    ) -> Result<(StatusCode, Bytes)> {
        let url = Url::parse(url).map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "invalid OAuth token endpoint URL")
                .with_source(error)
        })?;
        let mut request = HttpRequestBuilder::new(Method::POST, url)
            .headers(headers.clone())
            .form(form)
            .build()?;
        self.auth_session.authenticate(&mut request)?;
        let response = self.execute_redirects(request, None)?;
        Ok((response.status(), response.into_body()))
    }

    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.disable_header_redaction
    }

    fn execute_redirects(
        &self,
        mut request: HttpRequest,
        catalog_headers: Option<&HeaderMap>,
    ) -> Result<Response<Bytes>> {
        let deadline = Instant::now().checked_add(DEFAULT_REQUEST_TIMEOUT);
        let mut authentication_allowed = true;
        for redirect_count in 0..=MAX_REDIRECTS {
            let request_url = request.url().clone();
            let response = self
                .transport
                .execute(request.clone().into_http()?, deadline)?;
            if !matches!(
                response.status(),
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::SEE_OTHER
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            ) {
                return Ok(response);
            }
            if redirect_count == MAX_REDIRECTS {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "REST request exceeded redirect limit",
                ));
            }
            let location = response.headers().get(LOCATION).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "REST redirect response is missing the Location header",
                )
            })?;
            let location = location.to_str().map_err(|error| {
                Error::new(ErrorKind::DataInvalid, "invalid REST redirect Location")
                    .with_source(error)
            })?;
            let destination = request_url.join(location).map_err(Error::from)?;
            if !same_origin(&request_url, &destination) {
                authentication_allowed = false;
                let restricted_headers = request
                    .headers()
                    .keys()
                    .filter(|name| {
                        self.redirect_restricted_headers.contains(*name)
                            || is_sensitive_header(name.as_str())
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                for name in restricted_headers {
                    request.headers_mut().remove(name);
                }
            }
            let drops_entity =
                request.follow_redirect(response.status(), destination);
            if authentication_allowed {
                // Authentication may cover the method, path or body, so a
                // same-origin redirect must be authenticated as a new request.
                self.auth_session.authenticate(&mut request)?;
                if let Some(headers) = catalog_headers {
                    request.headers_mut().extend(headers.clone());
                }
                // Catalog defaults include Content-Type. A redirect that
                // discards the body must not restore entity headers after
                // authentication and catalog-header reapplication.
                if drops_entity {
                    request.remove_entity_headers();
                }
            }
        }
        unreachable!("redirect loop returns at or before the configured limit")
    }
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

pub(crate) fn deserialize_catalog_response<R: DeserializeOwned>(
    response: Response<Bytes>,
) -> Result<R> {
    serde_json::from_slice(response.body()).map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "failed to parse response from REST catalog server",
        )
        .with_source(error)
    })
}

pub(crate) fn deserialize_unexpected_catalog_error(
    response: Response<Bytes>,
    disable_header_redaction: bool,
) -> Error {
    let status = response.status();
    if let Ok(error_response) =
        serde_json::from_slice::<ErrorResponse>(response.body())
    {
        return error_response.into_error(status);
    }

    Error::new(
        ErrorKind::Unexpected,
        "received response with unexpected REST catalog status code",
    )
    .with_context("status", status.to_string())
    .with_context(
        "headers",
        format_headers_redacted(response.headers(), disable_header_redaction),
    )
    .with_source(RestError::new(status, None, None))
}

pub(crate) fn deserialize_unexpected_commit_error(
    response: Response<Bytes>,
    disable_header_redaction: bool,
) -> Error {
    let status = response.status();
    if let Ok(error_response) =
        serde_json::from_slice::<ErrorResponse>(response.body())
    {
        return error_response.into_commit_error(status);
    }

    let source = RestError::for_commit(status, None, None);
    let retryable = source.kind() == RestErrorKind::CommitConflict;
    let kind = if retryable {
        ErrorKind::CatalogCommitConflicts
    } else {
        ErrorKind::Unexpected
    };
    Error::new(
        kind,
        "received response with unexpected REST catalog commit status code",
    )
    .with_context("status", status.to_string())
    .with_context(
        "headers",
        format_headers_redacted(response.headers(), disable_header_redaction),
    )
    .with_retryable(retryable)
    .with_source(source)
}

fn is_sensitive_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        "auth",
        "token",
        "secret",
        "key",
        "password",
        "cookie",
        "credential",
    ]
    .iter()
    .any(|pattern| name.contains(pattern))
}

pub(super) fn format_headers_redacted(
    headers: &HeaderMap,
    disable_redaction: bool,
) -> String {
    let values: HashMap<&str, &str> = headers
        .iter()
        .filter_map(|(name, value)| {
            if !disable_redaction && is_sensitive_header(name.as_str()) {
                Some((name.as_str(), "[REDACTED]"))
            } else {
                value.to_str().ok().map(|value| (name.as_str(), value))
            }
        })
        .collect();
    format!("{values:?}")
}
