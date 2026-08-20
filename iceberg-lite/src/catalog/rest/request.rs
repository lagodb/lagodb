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

//! Transport-neutral HTTP request types used by REST authentication.

use bytes::Bytes;
use http::header::{
    CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue,
    TRANSFER_ENCODING,
};
use http::{Method, Request, StatusCode};
use serde::Serialize;
use url::Url;

use crate::{Error, ErrorKind, Result};

/// An outgoing REST request.
#[derive(Clone)]
pub struct HttpRequest {
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: Option<Bytes>,
}

impl HttpRequest {
    /// Creates a request with no body.
    pub fn new(method: Method, url: Url) -> Self {
        Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: None,
        }
    }

    pub(crate) fn build(builder: HttpRequestBuilder) -> Result<Self> {
        builder.build()
    }

    pub(crate) fn into_http(self) -> Result<Request<Bytes>> {
        let mut request = Request::builder()
            .method(self.method)
            .uri(self.url.as_str())
            .body(self.body.unwrap_or_default())
            .map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "failed to build REST HTTP request",
                )
                .with_source(error)
            })?;
        *request.headers_mut() = self.headers;
        Ok(request)
    }

    /// Returns the request method.
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Returns the complete request URL.
    pub fn url_str(&self) -> &str {
        self.url.as_str()
    }

    pub(crate) fn url(&self) -> &Url {
        &self.url
    }

    pub(crate) fn follow_redirect(&mut self, status: StatusCode, url: Url) -> bool {
        let rewrite_to_get = (status == StatusCode::SEE_OTHER
            && self.method != Method::HEAD)
            || ((status == StatusCode::MOVED_PERMANENTLY
                || status == StatusCode::FOUND)
                && self.method == Method::POST);
        let drops_entity = status == StatusCode::SEE_OTHER || rewrite_to_get;
        if drops_entity {
            if rewrite_to_get {
                self.method = Method::GET;
            }
            self.body = None;
            self.remove_entity_headers();
        }
        self.url = url;
        drops_entity
    }

    pub(crate) fn remove_entity_headers(&mut self) {
        self.headers.remove(CONTENT_TYPE);
        self.headers.remove(CONTENT_LENGTH);
        self.headers.remove(CONTENT_ENCODING);
        self.headers.remove(TRANSFER_ENCODING);
    }

    /// Returns request headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns mutable request headers.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Returns the request body exposed to an authentication session.
    pub fn body(&self) -> HttpRequestBody<'_> {
        match self.body.as_deref() {
            Some(bytes) => HttpRequestBody::Buffered(bytes),
            None => HttpRequestBody::Empty,
        }
    }
}

/// The body of an [`HttpRequest`] as seen by authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpRequestBody<'a> {
    /// No body is set.
    Empty,
    /// The complete in-memory body.
    Buffered(&'a [u8]),
}

impl<'a> HttpRequestBody<'a> {
    /// Returns the bytes used by request signers.
    pub fn as_bytes(self) -> &'a [u8] {
        match self {
            Self::Empty => &[],
            Self::Buffered(bytes) => bytes,
        }
    }
}

/// Builder retaining serialization errors until [`Self::build`].
pub(crate) struct HttpRequestBuilder {
    request: HttpRequest,
    error: Option<Error>,
}

impl HttpRequestBuilder {
    pub(crate) fn new(method: Method, url: Url) -> Self {
        Self {
            request: HttpRequest::new(method, url),
            error: None,
        }
    }

    pub(crate) fn from_str(method: Method, url: &str) -> Self {
        match Url::parse(url) {
            Ok(url) => Self::new(method, url),
            Err(error) => Self {
                request: HttpRequest::new(
                    method,
                    Url::parse("about:blank").expect("valid URL"),
                ),
                error: Some(
                    Error::new(ErrorKind::DataInvalid, "invalid REST request URL")
                        .with_source(error),
                ),
            },
        }
    }

    pub(crate) fn headers(mut self, headers: HeaderMap) -> Self {
        self.request.headers.extend(headers);
        self
    }

    pub(crate) fn query<T: Serialize + ?Sized>(mut self, query: &T) -> Self {
        if self.error.is_some() {
            return self;
        }
        match serde_urlencoded::to_string(query) {
            Ok(query) if !query.is_empty() => {
                let separator = if self.request.url.query().is_some() {
                    '&'
                } else {
                    '?'
                };
                let mut url = self.request.url.as_str().to_owned();
                url.push(separator);
                url.push_str(&query);
                match Url::parse(&url) {
                    Ok(url) => self.request.url = url,
                    Err(error) => {
                        self.error = Some(
                            Error::new(
                                ErrorKind::DataInvalid,
                                "invalid REST request URL",
                            )
                            .with_source(error),
                        )
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                self.error = Some(
                    Error::new(ErrorKind::DataInvalid, "failed to encode REST query")
                        .with_source(error),
                );
            }
        }
        self
    }

    pub(crate) fn json<T: Serialize + ?Sized>(mut self, value: &T) -> Self {
        if self.error.is_some() {
            return self;
        }
        match serde_json::to_vec(value) {
            Ok(body) => {
                self.request.body = Some(Bytes::from(body));
                self.request.headers.insert(
                    CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
            }
            Err(error) => {
                self.error = Some(
                    Error::new(
                        ErrorKind::DataInvalid,
                        "failed to encode REST JSON body",
                    )
                    .with_source(error),
                );
            }
        }
        self
    }

    pub(crate) fn form<T: Serialize + ?Sized>(mut self, value: &T) -> Self {
        if self.error.is_some() {
            return self;
        }
        match serde_urlencoded::to_string(value) {
            Ok(body) => {
                self.request.body = Some(Bytes::from(body));
                self.request.headers.insert(
                    CONTENT_TYPE,
                    HeaderValue::from_static("application/x-www-form-urlencoded"),
                );
            }
            Err(error) => {
                self.error = Some(
                    Error::new(
                        ErrorKind::DataInvalid,
                        "failed to encode REST form body",
                    )
                    .with_source(error),
                );
            }
        }
        self
    }

    pub(crate) fn build(self) -> Result<HttpRequest> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(self.request),
        }
    }
}
