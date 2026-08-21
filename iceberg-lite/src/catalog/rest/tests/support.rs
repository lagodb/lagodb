use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue, LOCATION};
use http::{Method, Request, Response, StatusCode};

use super::super::auth::AuthManager;
use super::super::catalog::{
    REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder, RestSessionCatalog,
    RestSessionCatalogBuilder,
};
use super::super::client::HttpTransport;
use crate::io::{FileIO, MemoryStorage, Storage, StorageConfig, StorageFactory};
use crate::{CatalogBuilder, Result};

pub(super) const CATALOG_URI: &str = "https://catalog.test";

#[derive(Debug)]
enum BodyExpectation {
    Any,
    Empty,
    Json(serde_json::Value),
    Form(Vec<(String, String)>),
}

#[derive(Debug)]
pub(super) struct ExpectedExchange {
    method: Method,
    uri: String,
    headers: Vec<(HeaderName, Option<HeaderValue>)>,
    body: BodyExpectation,
    response: Response<Bytes>,
}

impl ExpectedExchange {
    pub(super) fn new(method: Method, uri: impl Into<String>) -> Self {
        Self {
            method,
            uri: uri.into(),
            headers: Vec::new(),
            body: BodyExpectation::Any,
            response: Response::builder()
                .status(StatusCode::OK)
                .body(Bytes::new())
                .expect("test response is valid"),
        }
    }

    pub(super) fn get(uri: impl Into<String>) -> Self {
        Self::new(Method::GET, uri)
    }

    pub(super) fn post(uri: impl Into<String>) -> Self {
        Self::new(Method::POST, uri)
    }

    pub(super) fn delete(uri: impl Into<String>) -> Self {
        Self::new(Method::DELETE, uri)
    }

    pub(super) fn head(uri: impl Into<String>) -> Self {
        Self::new(Method::HEAD, uri)
    }

    pub(super) fn header<N, V>(mut self, name: N, value: V) -> Self
    where
        N: TryInto<HeaderName>,
        N::Error: Debug,
        V: TryInto<HeaderValue>,
        V::Error: Debug,
    {
        self.headers.push((
            name.try_into().expect("test header name is valid"),
            Some(value.try_into().expect("test header value is valid")),
        ));
        self
    }

    pub(super) fn missing_header<N>(mut self, name: N) -> Self
    where
        N: TryInto<HeaderName>,
        N::Error: Debug,
    {
        self.headers
            .push((name.try_into().expect("test header name is valid"), None));
        self
    }

    pub(super) fn empty_body(mut self) -> Self {
        self.body = BodyExpectation::Empty;
        self
    }

    pub(super) fn json_body(mut self, body: serde_json::Value) -> Self {
        self.body = BodyExpectation::Json(body);
        self
    }

    pub(super) fn form(
        mut self,
        pairs: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> Self {
        self.body = BodyExpectation::Form(
            pairs
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
        );
        self
    }

    pub(super) fn respond(
        mut self,
        status: StatusCode,
        body: impl Into<Bytes>,
    ) -> Self {
        self.response = Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/json")
            .body(body.into())
            .expect("test response is valid");
        self
    }

    pub(super) fn redirect(mut self, status: StatusCode, location: &str) -> Self {
        self.response = Response::builder()
            .status(status)
            .header(LOCATION, location)
            .body(Bytes::new())
            .expect("test redirect response is valid");
        self
    }

    fn assert_request(&self, request: &Request<Bytes>) {
        assert_eq!(
            request.method(),
            &self.method,
            "request method for {}",
            self.uri
        );
        assert_eq!(request.uri().to_string(), self.uri, "request URI");
        for (name, expected) in &self.headers {
            match expected {
                Some(value) => assert_eq!(
                    request.headers().get(name),
                    Some(value),
                    "header {name} for {}",
                    self.uri
                ),
                None => assert!(
                    !request.headers().contains_key(name),
                    "header {name} must be absent for {}",
                    self.uri
                ),
            }
        }
        match &self.body {
            BodyExpectation::Any => {}
            BodyExpectation::Empty => assert!(request.body().is_empty()),
            BodyExpectation::Json(expected) => {
                let actual: serde_json::Value =
                    serde_json::from_slice(request.body())
                        .expect("request body must contain JSON");
                assert_eq!(&actual, expected);
            }
            BodyExpectation::Form(expected) => {
                let actual: HashMap<String, String> =
                    serde_urlencoded::from_bytes(request.body())
                        .expect("request body must contain a form");
                let expected: HashMap<_, _> = expected.iter().cloned().collect();
                assert_eq!(actual, expected);
            }
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct MockHttpTransport {
    exchanges: Mutex<VecDeque<ExpectedExchange>>,
    requests: Mutex<Vec<Request<Bytes>>>,
}

impl MockHttpTransport {
    pub(super) fn expect(&self, exchange: ExpectedExchange) {
        self.exchanges
            .lock()
            .expect("test exchange lock is not poisoned")
            .push_back(exchange);
    }

    pub(super) fn request_count(&self) -> usize {
        self.requests
            .lock()
            .expect("test request lock is not poisoned")
            .len()
    }

    pub(super) fn assert_finished(&self) {
        let exchanges = self
            .exchanges
            .lock()
            .expect("test exchange lock is not poisoned");
        assert!(exchanges.is_empty(), "unconsumed exchanges: {exchanges:#?}");
    }
}

impl HttpTransport for MockHttpTransport {
    fn execute(
        &self,
        request: Request<Bytes>,
        _deadline: Option<Instant>,
    ) -> Result<Response<Bytes>> {
        let exchange = self
            .exchanges
            .lock()
            .expect("test exchange lock is not poisoned")
            .pop_front()
            .expect("unexpected HTTP request");
        exchange.assert_request(&request);
        self.requests
            .lock()
            .expect("test request lock is not poisoned")
            .push(request);
        Ok(exchange.response)
    }
}

#[derive(Debug)]
pub(super) struct TestStorageFactory {
    storage: Arc<MemoryStorage>,
    configs: Mutex<Vec<StorageConfig>>,
}

impl Default for TestStorageFactory {
    fn default() -> Self {
        Self {
            storage: Arc::new(MemoryStorage::new()),
            configs: Mutex::new(Vec::new()),
        }
    }
}

impl TestStorageFactory {
    pub(super) fn configs(&self) -> Vec<StorageConfig> {
        self.configs
            .lock()
            .expect("test storage config lock is not poisoned")
            .clone()
    }
}

impl StorageFactory for TestStorageFactory {
    fn build(&self, config: StorageConfig) -> Result<Arc<dyn Storage>> {
        self.configs
            .lock()
            .expect("test storage config lock is not poisoned")
            .push(config);
        Ok(self.storage.clone())
    }
}

#[derive(Debug)]
pub(super) struct RestTestFixture {
    transport: Arc<MockHttpTransport>,
    storage_factory: Arc<TestStorageFactory>,
}

impl Default for RestTestFixture {
    fn default() -> Self {
        Self {
            transport: Arc::new(MockHttpTransport::default()),
            storage_factory: Arc::new(TestStorageFactory::default()),
        }
    }
}

impl RestTestFixture {
    pub(super) fn expect(&self, exchange: ExpectedExchange) {
        self.transport.expect(exchange);
    }

    pub(super) fn expect_config(&self, body: serde_json::Value) {
        self.expect(
            ExpectedExchange::get(format!("{CATALOG_URI}/v1/config")).respond(
                StatusCode::OK,
                serde_json::to_vec(&body).expect("test config is serializable"),
            ),
        );
    }

    pub(super) fn catalog<'a>(
        &self,
        extra_props: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> RestSessionCatalog {
        self.catalog_builder()
            .load("test", self.props(extra_props))
            .expect("test catalog config is valid")
    }

    pub(super) fn catalog_with_auth_manager<'a>(
        &self,
        extra_props: impl IntoIterator<Item = (&'a str, &'a str)>,
        auth_manager: Arc<dyn AuthManager>,
    ) -> RestSessionCatalog {
        self.catalog_builder()
            .with_auth_manager(auth_manager)
            .load("test", self.props(extra_props))
            .expect("test catalog config is valid")
    }

    pub(super) fn bound_catalog<'a>(
        &self,
        extra_props: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> RestCatalog {
        RestCatalogBuilder::default()
            .with_http_transport(self.transport.clone())
            .with_storage_factory(self.storage_factory.clone())
            .load("test", self.props(extra_props))
            .expect("test catalog config is valid")
    }

    fn catalog_builder(&self) -> RestSessionCatalogBuilder {
        RestSessionCatalogBuilder::default()
            .with_http_transport(self.transport.clone())
            .with_storage_factory(self.storage_factory.clone())
    }

    fn props<'a>(
        &self,
        extra_props: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> HashMap<String, String> {
        let mut props = HashMap::from([(
            REST_CATALOG_PROP_URI.to_owned(),
            CATALOG_URI.to_owned(),
        )]);
        props.extend(
            extra_props
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned())),
        );
        props
    }

    pub(super) fn transport(&self) -> Arc<MockHttpTransport> {
        self.transport.clone()
    }

    pub(super) fn storage_configs(&self) -> Vec<StorageConfig> {
        self.storage_factory.configs()
    }

    pub(super) fn file_io(&self) -> FileIO {
        FileIO::new(self.storage_factory.storage.clone())
    }

    pub(super) fn assert_finished(&self) {
        self.transport.assert_finished();
    }
}
