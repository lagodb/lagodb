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

//! This module contains the iceberg REST catalog implementation.

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use http::{Method, StatusCode};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use url::Url;

use crate::encryption::kms::{KeyManagementClient, KmsClientFactory};
use crate::io::{FileIO, StorageConfig, StorageCredential, StorageFactory};
use crate::table::Table;
use crate::transaction::PreparedTableCommit;
use crate::{
    Catalog, CatalogBuilder, Error, ErrorKind, Namespace, NamespaceIdent, Result,
    SessionCatalog, SessionContext, TableCommit, TableCreation, TableIdent,
};

use super::auth::{
    AUTH_TYPE_NONE, AUTH_TYPE_OAUTH2, AuthManager, NoopAuthManager, OAuth2Manager,
};
use super::client::{
    HttpClient, HttpTransport, deserialize_catalog_response,
    deserialize_unexpected_catalog_error, deserialize_unexpected_commit_error,
};
use super::endpoint::{
    Endpoint, V1_COMMIT_TRANSACTION, V1_NAMESPACE_EXISTS, V1_TABLE_EXISTS,
};
use super::request::HttpRequest;
use super::transaction::PreparedRestCommit;
use super::types::{
    CatalogConfig, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
    CreateNamespaceRequest, CreateTableRequest, ListNamespaceResponse,
    ListTablesResponse, LoadTableResult, NamespaceResponse, RegisterTableRequest,
    RenameTableRequest,
};

type Response = http::Response<Bytes>;

/// REST catalog URI
pub const REST_CATALOG_PROP_URI: &str = "uri";
/// REST catalog warehouse location
pub const REST_CATALOG_PROP_WAREHOUSE: &str = "warehouse";
/// Disable header redaction in error logs and `Debug` output (defaults to
/// false for security)
pub const REST_CATALOG_PROP_DISABLE_HEADER_REDACTION: &str =
    "disable-header-redaction";
/// Authentication scheme: `none` or `oauth2`. When unset, `oauth2` is used
/// if a `token`, `credential` or `oauth2-server-uri` is configured, `none`
/// otherwise.
pub const REST_CATALOG_PROP_AUTH_TYPE: &str = "rest.auth.type";

const ICEBERG_REST_SPEC_VERSION: &str = "0.14.1";
const PATH_V1: &str = "v1";

/// Builder for [`RestCatalog`], the [`Catalog`]-compatible façade over a
/// [`RestSessionCatalog`].
///
/// The resulting catalog binds one [`SessionContext`] to every operation. Use
/// [`RestSessionCatalogBuilder`] when the caller supplies a context per operation.
#[derive(Debug, Default)]
pub struct RestCatalogBuilder {
    session_context: Option<SessionContext>,
    inner: RestSessionCatalogBuilder,
}

impl CatalogBuilder for RestCatalogBuilder {
    type C = RestCatalog;

    fn with_kms_client_factory(
        mut self,
        kms_client_factory: Arc<dyn KmsClientFactory>,
    ) -> Self {
        self.inner = self.inner.with_kms_client_factory(kms_client_factory);
        self
    }

    fn load(
        self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> Result<Self::C> {
        let context = self.session_context.unwrap_or_else(SessionContext::empty);
        let session_catalog = Arc::new(self.inner.load(name, props)?);

        Ok(RestCatalog::from_session_catalog(context, session_catalog))
    }
}

impl RestCatalogBuilder {
    /// Configures the transport used for REST and OAuth HTTP exchanges.
    pub fn with_http_transport(mut self, transport: Arc<dyn HttpTransport>) -> Self {
        self.inner = self.inner.with_http_transport(transport);
        self
    }

    /// Configures the factory used to build table-specific storage.
    pub fn with_storage_factory(
        mut self,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Self {
        self.inner = self.inner.with_storage_factory(storage_factory);
        self
    }

    /// Binds the session context forwarded with every catalog operation.
    ///
    /// If this is not called, [`load`](CatalogBuilder::load) creates a fresh
    /// [`SessionContext::empty`] context.
    pub fn with_session_context(mut self, context: SessionContext) -> Self {
        self.session_context = Some(context);
        self
    }

    /// Injects a custom auth manager, overriding the `rest.auth.type` configuration.
    pub fn with_auth_manager(mut self, auth_manager: Arc<dyn AuthManager>) -> Self {
        self.inner = self.inner.with_auth_manager(auth_manager);
        self
    }
}

/// Rest catalog configuration.
#[derive(Clone)]
pub(crate) struct RestCatalogConfig {
    name: Option<String>,

    uri: String,

    /// Client warehouse sent with the `/v1/config` handshake. It is consumed
    /// when the server response is merged; runtime code reads the resolved
    /// warehouse from `props`.
    config_request_warehouse: Option<String>,

    props: HashMap<String, String>,

    transport: Option<Arc<dyn HttpTransport>>,
}

/// Property keys whose values are secrets, or may embed them (headers,
/// connection strings, keys like `adls.account-key` or `s3.sse.key`).
fn is_sensitive_prop(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("token")
        || key.contains("credential")
        || key.contains("secret")
        || key.contains("password")
        || key.contains("key")
        || key.contains("connection-string")
        || key.contains("uri")
        || key.contains("endpoint")
        || key.starts_with("header.")
}

/// Redacts secret property values: this config is printed by the derived
/// [`Debug`] implementations of [`RestSessionCatalog`] and [`RestCatalog`].
impl Debug for RestCatalogConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let props: HashMap<&str, &str> = self
            .props
            .iter()
            .map(|(key, value)| {
                let value = if is_sensitive_prop(key) {
                    "[REDACTED]"
                } else {
                    value.as_str()
                };
                (key.as_str(), value)
            })
            .collect();
        f.debug_struct("RestCatalogConfig")
            .field("name", &self.name)
            .field("uri", &"[configured]")
            .field(
                "config_request_warehouse",
                &self
                    .config_request_warehouse
                    .as_ref()
                    .map(|_| "[configured]"),
            )
            .field("props", &props)
            .finish_non_exhaustive()
    }
}

impl RestCatalogConfig {
    #[cfg(test)]
    pub(crate) fn for_test(
        uri: impl Into<String>,
        props: HashMap<String, String>,
        transport: Arc<dyn HttpTransport>,
    ) -> Self {
        Self {
            name: Some("test".to_owned()),
            uri: uri.into(),
            config_request_warehouse: None,
            props,
            transport: Some(transport),
        }
    }

    fn url_prefixed(&self, parts: &[&str]) -> String {
        [&self.uri, PATH_V1]
            .into_iter()
            .chain(self.props.get("prefix").map(|s| &**s))
            .chain(parts.iter().cloned())
            .join("/")
    }

    fn config_endpoint(&self) -> String {
        [&self.uri, PATH_V1, "config"].join("/")
    }

    pub(crate) fn get_token_endpoint(&self) -> String {
        self.explicit_oauth2_server_uri()
            .unwrap_or_else(|| default_token_endpoint(&self.uri))
    }

    /// The `oauth2-server-uri` property, only when explicitly configured.
    pub(crate) fn explicit_oauth2_server_uri(&self) -> Option<String> {
        self.props.get("oauth2-server-uri").cloned()
    }

    fn namespaces_endpoint(&self) -> String {
        self.url_prefixed(&["namespaces"])
    }

    fn namespace_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string()])
    }

    fn tables_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "tables"])
    }

    fn rename_table_endpoint(&self) -> String {
        self.url_prefixed(&["tables", "rename"])
    }

    fn register_table_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "register"])
    }

    fn table_endpoint(&self, table: &TableIdent) -> String {
        self.url_prefixed(&[
            "namespaces",
            &table.namespace.to_url_string(),
            "tables",
            &table.name,
        ])
    }

    fn transaction_endpoint(&self) -> String {
        self.url_prefixed(&["transactions", "commit"])
    }

    /// Returns the injected PostgreSQL-aware HTTP transport.
    pub(crate) fn transport(&self) -> Result<&Arc<dyn HttpTransport>> {
        self.transport.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "REST catalog requires an HTTP transport",
            )
        })
    }

    /// Get the token from the config.
    ///
    /// The client can use this token to send requests.
    pub(crate) fn token(&self) -> Option<String> {
        self.props.get("token").cloned()
    }

    /// Get the credentials from the config. The client can use these credentials to fetch a new
    /// token.
    pub(crate) fn credential(&self) -> Option<(Option<String>, String)> {
        credential_from_props(&self.props)
    }

    /// Get the extra headers from config, see [`extra_headers_from_props`].
    pub(crate) fn extra_headers(&self) -> Result<HeaderMap> {
        extra_headers_from_props(&self.props)
    }

    /// Headers explicitly scoped to this catalog by `header.*` properties.
    pub(crate) fn explicit_headers(&self) -> Result<HeaderMap> {
        explicit_headers_from_props(&self.props)
    }

    /// Get the optional OAuth headers from the config.
    pub(crate) fn extra_oauth_params(&self) -> HashMap<String, String> {
        oauth_params_from_props(&self.props)
    }

    /// Check if header redaction is disabled in error logs.
    ///
    /// Returns true if the `disable-header-redaction` property is set to "true".
    /// Defaults to false for security (headers are redacted by default).
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.props
            .get(REST_CATALOG_PROP_DISABLE_HEADER_REDACTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Merge the `RestCatalogConfig` with the a [`CatalogConfig`] (fetched from the REST server).
    fn merge_with_config(mut self, mut config: CatalogConfig) -> Self {
        if let Some(uri) = config.overrides.remove("uri") {
            self.uri = uri;
        }

        let mut props = config.defaults;
        props.extend(self.props);
        // The builder moved the client warehouse off the props so it could be
        // sent with the config request. Consume it into the normalized runtime
        // properties between defaults and overrides (default < client < override).
        if let Some(warehouse) = self.config_request_warehouse.take() {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse);
        }
        props.extend(config.overrides);

        self.props = props;
        self
    }
}

/// Parses the `credential` property.
///
/// ## Output
///
/// - `None`: No credential is set.
/// - `Some(None, client_secret)`: No client_id is set, use client_secret directly.
/// - `Some(Some(client_id), client_secret)`: Both client_id and client_secret are set.
pub(crate) fn credential_from_props(
    props: &HashMap<String, String>,
) -> Option<(Option<String>, String)> {
    let cred = props.get("credential")?;

    match cred.split_once(':') {
        Some((client_id, client_secret)) => {
            Some((Some(client_id.to_string()), client_secret.to_string()))
        }
        None => Some((None, cred.to_string())),
    }
}

/// The extra headers added to each request, which include:
///
/// - `content-type`
/// - `x-client-version`
/// - `user-agent`
/// - All headers specified by `header.xxx` in props.
pub(crate) fn extra_headers_from_props(
    props: &HashMap<String, String>,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::from_iter([
        (
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        ),
        (
            HeaderName::from_static("x-client-version"),
            HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
        ),
        (
            header::USER_AGENT,
            HeaderValue::from_static(concat!(
                "iceberg-rs/",
                env!("CARGO_PKG_VERSION")
            )),
        ),
    ]);

    headers.extend(explicit_headers_from_props(props)?);

    Ok(headers)
}

/// The default OAuth2 token endpoint for a catalog `uri`.
pub(crate) fn default_token_endpoint(uri: &str) -> String {
    [uri, PATH_V1, "oauth", "tokens"].join("/")
}

/// Only the headers explicitly configured via `header.xxx` props (no defaults).
pub(crate) fn explicit_headers_from_props(
    props: &HashMap<String, String>,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (key, value) in props
        .iter()
        .filter_map(|(k, v)| k.strip_prefix("header.").map(|k| (k, v)))
    {
        headers.insert(
            HeaderName::from_str(key).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid header name: {key}"),
                )
                .with_source(e)
            })?,
            HeaderValue::from_str(value).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    // The value itself is omitted: it may be a secret.
                    format!("Invalid value for header: {key}"),
                )
                .with_source(e)
            })?,
        );
    }

    Ok(headers)
}

/// The optional OAuth parameters added to each authentication request.
pub(crate) fn oauth_params_from_props(
    props: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if let Some(scope) = props.get("scope") {
        params.insert("scope".to_string(), scope.to_string());
    } else {
        params.insert("scope".to_string(), "catalog".to_string());
    }

    let optional_params = ["audience", "resource"];
    for param_name in optional_params {
        if let Some(value) = props.get(param_name) {
            params.insert(param_name.to_string(), value.to_string());
        }
    }

    params
}

#[derive(Debug)]
struct RestClient {
    /// Carries the session the auth manager derived from the merged
    /// configuration, so every request below is authenticated.
    http_client: HttpClient,
    /// Runtime config is fetched from rest server and stored here.
    ///
    /// It could be different from the user config.
    config: RestCatalogConfig,
    /// Capabilities the server advertises (see [`RestSessionCatalog::supports_endpoint`]).
    endpoints: HashSet<Endpoint>,
}

impl RestClient {
    /// Initializes the runtime config, advertised endpoints, and authentication
    /// sessions shared by one REST catalog instance.
    fn init(
        user_config: &RestCatalogConfig,
        auth_manager: Arc<dyn AuthManager>,
    ) -> Result<Self> {
        let http_client = HttpClient::new(user_config)?;
        // The init session lives only for the config handshake, so a
        // manager whose session guards a one-shot resource can release
        // it before deriving the catalog session.
        let catalog_config = {
            let init_session = auth_manager.init_session(
                &http_client.without_auth_session(),
                &Self::auth_props(user_config),
            )?;
            Self::load_config(
                &http_client.with_auth_session(Arc::from(init_session)),
                user_config,
            )?
        };
        // Use the advertised endpoints as-is, falling back to
        // `DEFAULT_ENDPOINTS` when absent or empty.
        let endpoints = match &catalog_config.endpoints {
            Some(advertised) if !advertised.is_empty() => {
                advertised.iter().cloned().collect()
            }
            _ => super::endpoint::DEFAULT_ENDPOINTS.clone(),
        };
        let config = user_config.clone().merge_with_config(catalog_config);
        let http_client = http_client.update_with(&config)?;
        // The manager is handed an unauthenticated client: its own
        // requests must not be signed by the session it is deriving.
        let session = auth_manager.catalog_session(
            &http_client.without_auth_session(),
            &Self::auth_props(&config),
        )?;

        Ok(Self {
            config,
            http_client: http_client.with_auth_session(session),
            endpoints,
        })
    }

    /// Sends `request`, authenticated by the client's session.
    fn query_catalog(&self, request: HttpRequest) -> Result<Response> {
        self.http_client.query_catalog(request)
    }

    /// The properties handed to the [`AuthManager`], with the catalog `uri`
    /// and `warehouse` made explicit.
    fn auth_props(config: &RestCatalogConfig) -> HashMap<String, String> {
        // `oauth2-server-uri` stays absent unless explicitly configured, so an
        // injected manager keeps its own endpoint. Before the handshake, add
        // the client warehouse kept for the config request. After the handshake,
        // `props` already contains the resolved warehouse. The built-in manager
        // recomputes its token endpoint from the resolved URI.
        let mut props = config.props.clone();
        props.insert(REST_CATALOG_PROP_URI.to_string(), config.uri.clone());
        if let Some(warehouse) = &config.config_request_warehouse {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        }
        props
    }

    /// Loads the runtime config from the server using `user_config`.
    ///
    /// It's required for a REST catalog to update its config after creation.
    fn load_config(
        http_client: &HttpClient,
        user_config: &RestCatalogConfig,
    ) -> Result<CatalogConfig> {
        let mut request_builder =
            http_client.request(Method::GET, user_config.config_endpoint());

        if let Some(warehouse_location) = &user_config.config_request_warehouse {
            request_builder =
                request_builder.query(&[("warehouse", warehouse_location)]);
        }

        let request = HttpRequest::build(request_builder)?;

        let http_response = http_client.query_catalog(request)?;

        match http_response.status() {
            StatusCode::OK => deserialize_catalog_response(http_response),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                http_client.disable_header_redaction(),
            )),
        }
    }
}

/// A [`Catalog`]-compatible façade over [`RestSessionCatalog`].
///
/// Every operation is forwarded with the single [`SessionContext`] selected by
/// [`RestCatalogBuilder`]. Use [`RestSessionCatalog`] when the caller needs to
/// provide a context per operation.
#[derive(Debug, Clone)]
pub struct RestCatalog {
    session_context: SessionContext,
    inner: Arc<RestSessionCatalog>,
}

impl RestCatalog {
    fn from_session_catalog(
        context: SessionContext,
        inner: Arc<RestSessionCatalog>,
    ) -> Self {
        Self {
            session_context: context,
            inner,
        }
    }

    /// Validate that the server supports atomic table-change transactions.
    ///
    /// Writable adapters call this before producing data or delete files.
    pub fn ensure_transaction_commit_supported(&self) -> Result<()> {
        self.inner.ensure_transaction_commit_supported()
    }

    /// Builds one atomic REST catalog request from prepared table commits.
    ///
    /// # Errors
    ///
    /// Returns an error when no table changes are supplied, the server does
    /// not advertise the transaction endpoint, or request serialization fails.
    pub fn prepare_transaction_commit(
        &self,
        commits: Vec<PreparedTableCommit>,
    ) -> Result<PreparedRestCommit> {
        self.inner.prepare_transaction_commit(commits)
    }

    /// Sends a previously prepared REST catalog transaction.
    ///
    /// # Errors
    ///
    /// Returns the transport or catalog error. A caller invoking this after a
    /// local database commit must report the error without attempting rollback.
    pub fn send_prepared_commit(&self, commit: PreparedRestCommit) -> Result<()> {
        self.inner.send_prepared_commit(commit)
    }
}

/// Every operation forwards to its [`RestSessionCatalog`] equivalent with the
/// bound [`SessionContext`]; see that implementation for the REST-specific
/// behavior.
impl Catalog for RestCatalog {
    fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(&self.session_context, parent)
    }

    fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner
            .create_namespace(&self.session_context, namespace, properties)
    }

    fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(&self.session_context, namespace)
    }

    fn namespace_exists(&self, ns: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(&self.session_context, ns)
    }

    fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .update_namespace(&self.session_context, namespace, properties)
    }

    fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(&self.session_context, namespace)
    }

    fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(&self.session_context, namespace)
    }

    fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner
            .create_table(&self.session_context, namespace, creation)
    }

    fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        self.inner.load_table(&self.session_context, table_ident)
    }

    fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(&self.session_context, table)
    }

    fn purge_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.purge_table(&self.session_context, table)
    }

    fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(&self.session_context, table)
    }

    fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(&self.session_context, src, dest)
    }

    fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        self.inner.register_table(
            &self.session_context,
            table_ident,
            metadata_location,
        )
    }

    fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(&self.session_context, commit)
    }
}

/// REST catalog implementation of [`SessionCatalog`].
///
/// Each operation accepts a [`SessionContext`]. REST configuration, authentication sessions,
/// and the HTTP client are initialized lazily once per catalog and shared across all operations.
#[derive(Debug)]
pub struct RestSessionCatalog {
    /// Injected through [`RestSessionCatalogBuilder::with_auth_manager`]; otherwise
    /// one is resolved from `rest.auth.type` when the client is built.
    auth_manager: Option<Arc<dyn AuthManager>>,
    /// User config is stored as-is and never changed.
    ///
    /// It could be different from the config fetched from the server and used at runtime.
    user_config: RestCatalogConfig,
    client: OnceCell<RestClient>,
    /// Storage factory for creating FileIO instances.
    storage_factory: Option<Arc<dyn StorageFactory>>,
    /// Optional KMS client for encrypted tables.
    kms_client: Option<Arc<dyn KeyManagementClient>>,
}

impl RestSessionCatalog {
    /// Creates a `RestSessionCatalog` from a [`RestCatalogConfig`].
    fn new(
        config: RestCatalogConfig,
        auth_manager: Option<Arc<dyn AuthManager>>,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        kms_client: Option<Arc<dyn KeyManagementClient>>,
    ) -> Self {
        Self {
            auth_manager,
            user_config: config,
            client: OnceCell::new(),
            storage_factory,
            kms_client,
        }
    }

    /// Sends a DELETE request for the given table, optionally requesting purge.
    fn delete_table(
        &self,
        _context: &SessionContext,
        table: &TableIdent,
        purge: bool,
    ) -> Result<()> {
        let client = self.client()?;

        let mut request_builder = client
            .http_client
            .request(Method::DELETE, client.config.table_endpoint(table));

        if purge {
            request_builder = request_builder.query(&[("purgeRequested", "true")]);
        }

        let request = HttpRequest::build(request_builder)?;
        let http_response = client.query_catalog(request)?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::TableNotFound,
                "Tried to drop a table that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    /// The configured auth scheme: explicit `rest.auth.type` (matched
    /// case-insensitively) when set; otherwise `oauth2` when a `token`,
    /// `credential` or `oauth2-server-uri` is configured (preserving
    /// pre-`rest.auth.type` setups), `none` when none is.
    fn auth_type(config: &RestCatalogConfig) -> String {
        config
            .props
            .get(REST_CATALOG_PROP_AUTH_TYPE)
            // Matched case-insensitively, as the other flag properties are.
            .map(|auth_type| auth_type.to_ascii_lowercase())
            .unwrap_or_else(|| {
                if config.token().is_some()
                    || config.credential().is_some()
                    || config.explicit_oauth2_server_uri().is_some()
                {
                    AUTH_TYPE_OAUTH2.to_string()
                } else {
                    AUTH_TYPE_NONE.to_string()
                }
            })
    }

    /// Resolves the auth manager: a `with_auth_manager` override wins,
    /// otherwise one is built from the `rest.auth.type` configuration.
    fn resolve_auth_manager(&self) -> Result<Arc<dyn AuthManager>> {
        if let Some(auth_manager) = &self.auth_manager {
            return Ok(auth_manager.clone());
        }
        let config = &self.user_config;
        let auth_type = Self::auth_type(config);
        // Java parity (`AuthManagers`): make the inference visible so users
        // configure the type explicitly.
        if auth_type == AUTH_TYPE_OAUTH2
            && !config.props.contains_key(REST_CATALOG_PROP_AUTH_TYPE)
        {
            tracing::warn!(
                "Inferring {REST_CATALOG_PROP_AUTH_TYPE}={AUTH_TYPE_OAUTH2} from the configured \
                 OAuth properties; set it explicitly to avoid this warning"
            );
        }
        match auth_type.as_str() {
            AUTH_TYPE_NONE => Ok(Arc::new(NoopAuthManager)),
            AUTH_TYPE_OAUTH2 => Ok(Arc::new(OAuth2Manager::from_config(config)?)),
            other => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "unknown '{REST_CATALOG_PROP_AUTH_TYPE}': {other}; use \
                     `RestSessionCatalogBuilder::with_auth_manager` or \
                     `RestCatalogBuilder::with_auth_manager` to inject a custom auth manager"
                ),
            )),
        }
    }

    /// Gets the [`RestClient`] from the catalog.
    fn client(&self) -> Result<&RestClient> {
        self.client.get_or_try_init(|| {
            RestClient::init(&self.user_config, self.resolve_auth_manager()?)
        })
    }

    /// Returns whether the server supports `endpoint`, per the `endpoints` it
    /// advertised in `GET /v1/config` (or a default base set when it advertised
    /// none).
    pub(crate) fn supports_endpoint(&self, endpoint: &Endpoint) -> Result<bool> {
        Ok(self.client()?.endpoints.contains(endpoint))
    }

    /// Issue a `HEAD` request to `url` and interpret it as an existence check:
    /// `2xx` means it exists, `404` means it doesn't.
    fn check_exists_via_head(
        &self,
        client: &RestClient,
        url: String,
    ) -> Result<bool> {
        let request =
            HttpRequest::build(client.http_client.request(Method::HEAD, url))?;
        let http_response = client.query_catalog(request)?;

        match http_response.status() {
            StatusCode::NOT_FOUND => Ok(false),
            status if status.is_success() => Ok(true),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    fn load_file_io(
        &self,
        metadata_location: Option<&str>,
        extra_config: Option<HashMap<String, String>>,
        credentials: Vec<StorageCredential>,
    ) -> Result<FileIO> {
        let config = &self.client()?.config;
        let mut props = config.props.clone();
        if let Some(config) = extra_config {
            props.extend(config);
        }

        // If the warehouse is a logical identifier instead of a URL we don't want
        // to raise an exception
        let warehouse_path = match config
            .props
            .get(REST_CATALOG_PROP_WAREHOUSE)
            .map(String::as_str)
        {
            Some(url) if Url::parse(url).is_ok() => Some(url),
            Some(_) => None,
            None => None,
        };

        let location = metadata_location.or(warehouse_path).ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Unable to load file io, neither warehouse nor metadata location is set!",
            )
        })?;

        let factory = self.storage_factory.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "StorageFactory must be provided for REST catalog table operations. Use `with_storage_factory` to configure it.",
            )
        })?;

        let storage =
            factory.build(StorageConfig::new(location, props, credentials))?;
        Ok(FileIO::new(storage))
    }

    /// Fetches a table response without interpreting or materializing it.
    ///
    /// This is shared by `load_table` and the GET fallback for `table_exists`:
    /// existence checks must not depend on whether the caller configured a
    /// storage factory or whether vended object-store credentials are usable.
    fn fetch_table(&self, table_ident: &TableIdent) -> Result<Response> {
        let client = self.client()?;
        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::GET, client.config.table_endpoint(table_ident)),
        )?;
        client.query_catalog(request)
    }
}

/// All requests and expected responses are derived from the REST catalog API spec:
/// <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
impl SessionCatalog for RestSessionCatalog {
    fn list_namespaces(
        &self,
        _context: &SessionContext,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let client = self.client()?;
        let endpoint = client.config.namespaces_endpoint();
        let mut namespaces = Vec::new();
        let mut next_token = None;

        loop {
            let mut request =
                client.http_client.request(Method::GET, endpoint.clone());

            // Filter on `parent={namespace}` if a parent namespace exists.
            if let Some(ns) = parent {
                request = request.query(&[("parent", ns.to_url_string())]);
            }

            if let Some(token) = next_token {
                request = request.query(&[("pageToken", token)]);
            }

            let http_response = client.query_catalog(HttpRequest::build(request)?)?;

            match http_response.status() {
                StatusCode::OK => {
                    let response = deserialize_catalog_response::<
                        ListNamespaceResponse,
                    >(http_response)?;

                    namespaces.extend(response.namespaces);

                    match response.next_page_token {
                        Some(token) => next_token = Some(token),
                        None => break,
                    }
                }
                StatusCode::NOT_FOUND => {
                    return Err(Error::new(
                        ErrorKind::NamespaceNotFound,
                        "The parent parameter of the namespace provided does not exist",
                    ));
                }
                _ => {
                    return Err(deserialize_unexpected_catalog_error(
                        http_response,
                        client.http_client.disable_header_redaction(),
                    ));
                }
            }
        }

        Ok(namespaces)
    }

    fn create_namespace(
        &self,
        _context: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let client = self.client()?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::POST, client.config.namespaces_endpoint())
                .json(&CreateNamespaceRequest {
                    namespace: namespace.clone(),
                    properties,
                }),
        )?;

        let http_response = client.query_catalog(request)?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<NamespaceResponse>(http_response)?;
                Ok(Namespace::from(response))
            }
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::NamespaceAlreadyExists,
                "Tried to create a namespace that already exists",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    fn get_namespace(
        &self,
        _context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Namespace> {
        let client = self.client()?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::GET, client.config.namespace_endpoint(namespace)),
        )?;

        let http_response = client.query_catalog(request)?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<NamespaceResponse>(http_response)?;
                Ok(Namespace::from(response))
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::NamespaceNotFound,
                "Tried to get a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    fn namespace_exists(
        &self,
        context: &SessionContext,
        ns: &NamespaceIdent,
    ) -> Result<bool> {
        // Prefer a cheap HEAD when the server advertises it; otherwise fall back
        // to loading the namespace (GET) and treating a missing namespace as
        // `false`, so this still works against servers that don't advertise the
        // HEAD route.
        if !self.supports_endpoint(&V1_NAMESPACE_EXISTS)? {
            return match self.get_namespace(context, ns) {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == ErrorKind::NamespaceNotFound => Ok(false),
                Err(e) => Err(e),
            };
        }

        let client = self.client()?;
        self.check_exists_via_head(client, client.config.namespace_endpoint(ns))
    }

    fn update_namespace(
        &self,
        _context: &SessionContext,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating namespace not supported yet!",
        ))
    }

    fn drop_namespace(
        &self,
        _context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<()> {
        let client = self.client()?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::DELETE, client.config.namespace_endpoint(namespace)),
        )?;

        let http_response = client.query_catalog(request)?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::NamespaceNotFound,
                "Tried to drop a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    fn list_tables(
        &self,
        _context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>> {
        let client = self.client()?;
        let endpoint = client.config.tables_endpoint(namespace);
        let mut identifiers = Vec::new();
        let mut next_token = None;

        loop {
            let mut request =
                client.http_client.request(Method::GET, endpoint.clone());

            if let Some(token) = next_token {
                request = request.query(&[("pageToken", token)]);
            }

            let http_response = client.query_catalog(HttpRequest::build(request)?)?;

            match http_response.status() {
                StatusCode::OK => {
                    let response = deserialize_catalog_response::<ListTablesResponse>(
                        http_response,
                    )?;

                    identifiers.extend(response.identifiers);

                    match response.next_page_token {
                        Some(token) => next_token = Some(token),
                        None => break,
                    }
                }
                StatusCode::NOT_FOUND => {
                    return Err(Error::new(
                        ErrorKind::NamespaceNotFound,
                        "Tried to list tables of a namespace that does not exist",
                    ));
                }
                _ => {
                    return Err(deserialize_unexpected_catalog_error(
                        http_response,
                        client.http_client.disable_header_redaction(),
                    ));
                }
            }
        }

        Ok(identifiers)
    }

    /// Create a new table inside the namespace.
    ///
    /// Table response properties override catalog properties for the resulting
    /// table's storage, while scoped credentials override both at route use.
    fn create_table(
        &self,
        _context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let client = self.client()?;

        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::POST, client.config.tables_endpoint(namespace))
                .json(&CreateTableRequest {
                    name: creation.name,
                    location: creation.location,
                    schema: creation.schema,
                    partition_spec: creation.partition_spec,
                    write_order: creation.sort_order,
                    stage_create: Some(false),
                    properties: creation.properties,
                }),
        )?;

        let http_response = client.query_catalog(request)?;

        let response = match http_response.status() {
            StatusCode::OK => {
                deserialize_catalog_response::<LoadTableResult>(http_response)?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::NamespaceNotFound,
                    "Tried to create a table under a namespace that does not exist",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::TableAlreadyExists,
                    "The table already exists",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    client.http_client.disable_header_redaction(),
                ));
            }
        };

        let LoadTableResult {
            metadata_location,
            metadata,
            config,
            storage_credentials,
        } = response;
        let metadata_location = metadata_location.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Metadata location missing in `create_table` response!",
            )
        })?;

        let file_io = self.load_file_io(
            Some(&metadata_location),
            Some(config),
            storage_credentials.unwrap_or_default(),
        )?;

        let mut table_builder = Table::builder()
            .identifier(table_ident)
            .file_io(file_io)
            .metadata(metadata);
        if let Some(kms_client) = self.kms_client.clone() {
            table_builder = table_builder.kms_client(kms_client);
        }

        table_builder.metadata_location(metadata_location).build()
    }

    /// Load table from the catalog.
    ///
    /// Table response properties override catalog properties for the resulting
    /// table's storage, while scoped credentials override both at route use.
    fn load_table(
        &self,
        _context: &SessionContext,
        table_ident: &TableIdent,
    ) -> Result<Table> {
        let http_response = self.fetch_table(table_ident)?;
        let client = self.client()?;
        let response = match http_response.status() {
            StatusCode::OK => deserialize_catalog_response(http_response)?,
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    "Tried to load a table that does not exist",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    client.http_client.disable_header_redaction(),
                ));
            }
        };

        let LoadTableResult {
            metadata_location,
            metadata,
            config,
            storage_credentials,
        } = response;
        let file_io = self.load_file_io(
            metadata_location.as_deref(),
            Some(config),
            storage_credentials.unwrap_or_default(),
        )?;

        let mut table_builder = Table::builder()
            .identifier(table_ident.clone())
            .file_io(file_io)
            .metadata(metadata);
        if let Some(kms_client) = self.kms_client.clone() {
            table_builder = table_builder.kms_client(kms_client);
        }

        if let Some(metadata_location) = metadata_location {
            table_builder.metadata_location(metadata_location).build()
        } else {
            table_builder.build()
        }
    }

    /// Drop a table from the catalog.
    fn drop_table(&self, context: &SessionContext, table: &TableIdent) -> Result<()> {
        self.delete_table(context, table, false)
    }

    /// Drop a table from the catalog and purge its data by sending
    /// `purgeRequested=true` to the REST server.
    fn purge_table(
        &self,
        context: &SessionContext,
        table: &TableIdent,
    ) -> Result<()> {
        self.delete_table(context, table, true)
    }

    /// Check if a table exists in the catalog.
    fn table_exists(
        &self,
        _context: &SessionContext,
        table: &TableIdent,
    ) -> Result<bool> {
        // Prefer a cheap HEAD when the server advertises it; otherwise use only
        // the GET status, without deserializing metadata or constructing storage.
        if !self.supports_endpoint(&V1_TABLE_EXISTS)? {
            let client = self.client()?;
            let response = self.fetch_table(table)?;
            return match response.status() {
                StatusCode::NOT_FOUND => Ok(false),
                status if status.is_success() => Ok(true),
                _ => Err(deserialize_unexpected_catalog_error(
                    response,
                    client.http_client.disable_header_redaction(),
                )),
            };
        }

        let client = self.client()?;
        self.check_exists_via_head(client, client.config.table_endpoint(table))
    }

    /// Rename a table in the catalog.
    fn rename_table(
        &self,
        _context: &SessionContext,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> Result<()> {
        let client = self.client()?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::POST, client.config.rename_table_endpoint())
                .json(&RenameTableRequest {
                    source: src.clone(),
                    destination: dest.clone(),
                }),
        )?;

        let http_response = client.query_catalog(request)?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::TableNotFound,
                "Tried to rename a table that does not exist (is the namespace correct?)",
            )),
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::TableAlreadyExists,
                "Tried to rename a table to a name that already exists",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    fn register_table(
        &self,
        _context: &SessionContext,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        let client = self.client()?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(
                    Method::POST,
                    client
                        .config
                        .register_table_endpoint(table_ident.namespace()),
                )
                .json(&RegisterTableRequest {
                    name: table_ident.name.clone(),
                    metadata_location: metadata_location.clone(),
                    overwrite: Some(false),
                }),
        )?;

        let http_response = client.query_catalog(request)?;

        let response: LoadTableResult = match http_response.status() {
            StatusCode::OK => {
                deserialize_catalog_response::<LoadTableResult>(http_response)?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::NamespaceNotFound,
                    "The namespace specified does not exist.",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::TableAlreadyExists,
                    "The given table already exists.",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    client.http_client.disable_header_redaction(),
                ));
            }
        };

        let LoadTableResult {
            metadata_location,
            metadata,
            config,
            storage_credentials,
        } = response;
        let metadata_location = metadata_location.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Metadata location missing in `register_table` response!",
            )
        })?;

        let file_io = self.load_file_io(
            Some(&metadata_location),
            Some(config),
            storage_credentials.unwrap_or_default(),
        )?;

        let mut table_builder = Table::builder()
            .identifier(table_ident.clone())
            .file_io(file_io)
            .metadata(metadata)
            .metadata_location(metadata_location);
        if let Some(kms_client) = self.kms_client.clone() {
            table_builder = table_builder.kms_client(kms_client);
        }
        table_builder.build()
    }

    fn update_table(
        &self,
        context: &SessionContext,
        mut commit: TableCommit,
    ) -> Result<Table> {
        let client = self.client()?;
        let identifier = commit.identifier().clone();
        let file_io = commit.file_io().clone();
        let requirements = commit.take_requirements();
        let updates = commit.take_updates();
        let location_changed = updates
            .iter()
            .any(|update| matches!(update, crate::TableUpdate::SetLocation { .. }));

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::POST, client.config.table_endpoint(&identifier))
                .json(&CommitTableRequest {
                    identifier: Some(identifier.clone()),
                    requirements,
                    updates,
                }),
        )?;

        let http_response = client.query_catalog(request)?;

        let response: CommitTableResponse = match http_response.status() {
            StatusCode::OK => deserialize_catalog_response(http_response)?,
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    "Tried to update a table that does not exist",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_commit_error(
                    http_response,
                    client.http_client.disable_header_redaction(),
                ));
            }
        };

        // A location update can cross a provider namespace. The previous
        // table's response-scoped storage route cannot safely serve that new
        // location, so refresh the table and its vended credentials once the
        // commit has succeeded.
        if location_changed {
            return self.load_table(context, &identifier);
        }

        let mut table_builder = Table::builder()
            .identifier(identifier)
            .file_io(file_io)
            .metadata(response.metadata)
            .metadata_location(response.metadata_location);
        if let Some(kms_client) = self.kms_client.clone() {
            table_builder = table_builder.kms_client(kms_client);
        }
        table_builder.build()
    }
}

impl RestSessionCatalog {
    fn prepare_transaction_commit(
        &self,
        commits: Vec<PreparedTableCommit>,
    ) -> Result<PreparedRestCommit> {
        if commits.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "REST transaction requires at least one table change",
            ));
        }
        self.ensure_transaction_commit_supported()?;

        let client = self.client()?;
        let table_count = commits.len();
        let mut table_changes = Vec::with_capacity(table_count);
        for prepared in commits {
            let (_, mut commit) = prepared.into_parts();
            let identifier = commit.identifier().clone();
            table_changes.push(CommitTableRequest {
                identifier: Some(identifier),
                requirements: commit.take_requirements(),
                updates: commit.take_updates(),
            });
        }
        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::POST, client.config.transaction_endpoint())
                .json(&CommitTransactionRequest { table_changes }),
        )?;
        Ok(PreparedRestCommit::new(request, table_count))
    }

    fn ensure_transaction_commit_supported(&self) -> Result<()> {
        if self.supports_endpoint(&V1_COMMIT_TRANSACTION)? {
            return Ok(());
        }
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "REST catalog does not support atomic transaction commits",
        ))
    }

    fn send_prepared_commit(&self, commit: PreparedRestCommit) -> Result<()> {
        let client = self.client()?;
        let response = client.query_catalog(commit.request)?;
        match response.status() {
            status if status.is_success() => Ok(()),
            _ => Err(deserialize_unexpected_commit_error(
                response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }
}

/// Builder for an unbound [`RestSessionCatalog`].
///
/// Unlike [`RestCatalogBuilder`], the resulting catalog accepts a
/// [`SessionContext`] with each [`SessionCatalog`] operation.
#[derive(Debug)]
pub struct RestSessionCatalogBuilder {
    config: RestCatalogConfig,
    auth_manager: Option<Arc<dyn AuthManager>>,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    kms_client_factory: Option<Arc<dyn KmsClientFactory>>,
}

impl Default for RestSessionCatalogBuilder {
    fn default() -> Self {
        Self {
            config: RestCatalogConfig {
                name: None,
                uri: "".to_string(),
                config_request_warehouse: None,
                props: HashMap::new(),
                transport: None,
            },
            auth_manager: None,
            storage_factory: None,
            kms_client_factory: None,
        }
    }
}

impl RestSessionCatalogBuilder {
    /// Configures the transport used for REST and OAuth HTTP exchanges.
    pub fn with_http_transport(mut self, transport: Arc<dyn HttpTransport>) -> Self {
        self.config.transport = Some(transport);
        self
    }

    /// Injects a custom auth manager, overriding the `rest.auth.type` configuration.
    pub fn with_auth_manager(mut self, auth_manager: Arc<dyn AuthManager>) -> Self {
        self.auth_manager = Some(auth_manager);
        self
    }

    /// Set a custom StorageFactory to use for storage operations.
    ///
    /// When a StorageFactory is provided, the catalog will use it to build FileIO
    /// instances for all storage operations instead of using the default factory.
    ///
    /// # Arguments
    ///
    /// * `storage_factory` - The StorageFactory to use for creating storage instances
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::io::StorageFactory;
    /// use iceberg_catalog_rest::RestSessionCatalogBuilder;
    /// use iceberg_storage_opendal::OpenDalStorageFactory;
    /// use std::sync::Arc;
    ///
    /// let catalog = RestSessionCatalogBuilder::default()
    ///     .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
    ///         customized_credential_load: None,
    ///     }))
    ///     .load("my_catalog", props)
    ///     ?;
    /// ```
    pub fn with_storage_factory(
        mut self,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Self {
        self.storage_factory = Some(storage_factory);
        self
    }

    /// Set a [`KmsClientFactory`] to enable table encryption.
    ///
    /// When provided, the catalog calls the factory once during
    /// [`load`](Self::load) with the catalog properties to create a shared
    /// [`KeyManagementClient`].
    /// That client is then passed to each table's `TableBuilder` so tables
    /// with `encryption.key-id` set can construct an `EncryptionManager`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::encryption::kms::KmsClientFactory;
    /// use iceberg_catalog_rest::RestSessionCatalogBuilder;
    /// use std::sync::Arc;
    ///
    /// let catalog = RestSessionCatalogBuilder::default()
    ///     .with_kms_client_factory(Arc::new(MyKmsClientFactory))
    ///     .load("my_catalog", props)
    ///     ?;
    /// ```
    pub fn with_kms_client_factory(
        mut self,
        kms_client_factory: Arc<dyn KmsClientFactory>,
    ) -> Self {
        self.kms_client_factory = Some(kms_client_factory);
        self
    }

    /// Creates a new session catalog instance.
    ///
    /// The server configuration handshake, endpoint negotiation, and
    /// authentication sessions are initialized lazily on the first operation.
    pub fn load(
        mut self,
        name: impl Into<String>,
        mut props: HashMap<String, String>,
    ) -> Result<RestSessionCatalog> {
        self.config.name = Some(name.into());

        if let Some(uri) = props.remove(REST_CATALOG_PROP_URI) {
            self.config.uri = uri;
        }

        self.config.config_request_warehouse =
            props.remove(REST_CATALOG_PROP_WAREHOUSE);
        self.config.props = props;

        if self.config.uri.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Catalog uri is required",
            ));
        }
        self.config.transport()?;
        let kms_client = match self.kms_client_factory {
            Some(factory) => Some(factory.create_kms_client(&self.config.props)?),
            None => None,
        };

        Ok(RestSessionCatalog::new(
            self.config,
            self.auth_manager,
            self.storage_factory,
            kms_client,
        ))
    }
}
