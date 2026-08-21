use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use http::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use http::{Method, StatusCode};

use super::super::auth::{AuthManager, AuthSession, OAuth2Manager};
use super::super::catalog::{
    REST_CATALOG_PROP_AUTH_TYPE, REST_CATALOG_PROP_URI, RestCatalogConfig,
};
use super::super::client::HttpClient;
use super::super::request::HttpRequest;
use super::support::{
    CATALOG_URI, ExpectedExchange, MockHttpTransport, RestTestFixture,
};
use crate::{Error, ErrorKind, Result, SessionCatalog, SessionContext};

const TOKEN_RESPONSE: &str = r#"{
    "access_token":"tok",
    "token_type":"Bearer",
    "issued_token_type":"urn:ietf:params:oauth:token-type:access_token",
    "expires_in":86400
}"#;

#[test]
fn credential_is_exchanged_once_and_the_handshake_is_authenticated() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/oauth/tokens"))
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "catalog"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture.catalog([("credential", "client1:secret1")]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn oauth_optional_parameters_and_explicit_endpoint_are_preserved() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::post("https://auth.test/token")
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "custom_scope"),
                ("audience", "custom_audience"),
                ("resource", "custom_resource"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture.catalog([
        ("credential", "client1:secret1"),
        ("oauth2-server-uri", "https://auth.test/token"),
        ("scope", "custom_scope"),
        ("audience", "custom_audience"),
        ("resource", "custom_resource"),
    ]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn configured_token_takes_precedence_over_credential() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .header(AUTHORIZATION, "Bearer seeded")
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Bearer seeded")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog =
        fixture.catalog([("token", "seeded"), ("credential", "client1:secret1")]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn explicit_none_disables_auth_even_when_a_token_is_present() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .missing_header(AUTHORIZATION)
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .missing_header(AUTHORIZATION)
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture
        .catalog([(REST_CATALOG_PROP_AUTH_TYPE, "none"), ("token", "ignored")]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn explicit_authorization_header_overrides_oauth_on_the_wire() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .header(AUTHORIZATION, "Basic xyz")
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Basic xyz")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture.catalog([
        ("token", "oauth-token"),
        ("header.authorization", "Basic xyz"),
    ]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn concurrent_authentication_performs_one_token_exchange() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post("https://auth.test/token")
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "catalog"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    let config =
        RestCatalogConfig::for_test(CATALOG_URI, HashMap::new(), transport.clone());
    let client = HttpClient::new(&config).unwrap();
    let manager = OAuth2Manager::new("https://auth.test/token")
        .with_credential(Some("client1".to_owned()), "secret1".to_owned());
    let session: Arc<dyn AuthSession> =
        Arc::from(manager.init_session(&client, &HashMap::new()).unwrap());

    let workers: Vec<_> = (0..8)
        .map(|_| {
            let session = session.clone();
            thread::spawn(move || {
                let mut request = HttpRequest::new(
                    Method::GET,
                    "https://rest.test/v1/config".parse().unwrap(),
                );
                session.authenticate(&mut request).unwrap();
                request
                    .headers()
                    .get(AUTHORIZATION)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
        })
        .collect();
    let bearers: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();

    assert!(bearers.iter().all(|bearer| bearer == "Bearer tok"));
    assert_eq!(transport.request_count(), 1);
    transport.assert_finished();
}

#[test]
fn oauth_exchange_uses_manager_headers_not_catalog_client_headers() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post("https://auth.test/token")
            .header("x-from", "manager")
            .missing_header("x-catalog-only")
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    let config = RestCatalogConfig::for_test(
        CATALOG_URI,
        HashMap::from([(
            "header.x-catalog-only".to_owned(),
            "not-on-token-requests".to_owned(),
        )]),
        transport.clone(),
    );
    let client = HttpClient::new(&config).unwrap();
    let manager = OAuth2Manager::new("https://auth.test/token")
        .with_credential(Some("client1".to_owned()), "secret1".to_owned())
        .with_extra_headers(HeaderMap::from_iter([(
            HeaderName::from_static("x-from"),
            HeaderValue::from_static("manager"),
        )]));
    let session = manager.init_session(&client, &HashMap::new()).unwrap();
    let mut request = HttpRequest::new(
        Method::GET,
        "https://rest.test/v1/namespaces".parse().unwrap(),
    );

    session.authenticate(&mut request).unwrap();

    assert_eq!(request.headers().get(AUTHORIZATION).unwrap(), "Bearer tok");
    transport.assert_finished();
}

#[test]
fn default_oauth_endpoint_follows_the_merged_catalog_uri() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post("https://redirected.test/v1/oauth/tokens")
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "catalog"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    let config = RestCatalogConfig::for_test(
        CATALOG_URI,
        HashMap::from([("credential".to_owned(), "client1:secret1".to_owned())]),
        transport.clone(),
    );
    let client = HttpClient::new(&config).unwrap();
    let manager = OAuth2Manager::from_config(&config).unwrap();
    let session = manager
        .catalog_session(
            &client,
            &HashMap::from([(
                REST_CATALOG_PROP_URI.to_owned(),
                "https://redirected.test".to_owned(),
            )]),
        )
        .unwrap();
    let mut request = HttpRequest::new(
        Method::GET,
        "https://redirected.test/v1/namespaces".parse().unwrap(),
    );

    session.authenticate(&mut request).unwrap();

    assert_eq!(request.headers().get(AUTHORIZATION).unwrap(), "Bearer tok");
    transport.assert_finished();
}

#[test]
fn injected_manager_keeps_its_endpoint_headers_and_parameters() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {"credential": "client1:secret1"},
        "overrides": {}
    }));
    fixture.expect(
        ExpectedExchange::post("https://auth.test/custom/token")
            .header("x-tenant", "t1")
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "catalog"),
                ("audience", "aud-1"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );
    let manager = OAuth2Manager::new("https://auth.test/custom/token")
        .with_extra_headers(HeaderMap::from_iter([(
            HeaderName::from_static("x-tenant"),
            HeaderValue::from_static("t1"),
        )]))
        .with_extra_oauth_params(HashMap::from([(
            "audience".to_owned(),
            "aud-1".to_owned(),
        )]));
    let catalog = fixture.catalog_with_auth_manager([], Arc::new(manager));

    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();
    fixture.assert_finished();
}

#[derive(Debug)]
struct PlainSession;

impl AuthSession for PlainSession {
    fn authenticate(&self, _request: &mut HttpRequest) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct CapturingManager {
    init_props: Arc<Mutex<Option<HashMap<String, String>>>>,
    catalog_props: Arc<Mutex<Option<HashMap<String, String>>>>,
}

impl AuthManager for CapturingManager {
    fn init_session(
        &self,
        _client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        *self.init_props.lock().unwrap() = Some(props.clone());
        Ok(Box::new(PlainSession))
    }

    fn catalog_session(
        &self,
        _client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        *self.catalog_props.lock().unwrap() = Some(props.clone());
        Ok(Arc::new(PlainSession))
    }
}

#[test]
fn auth_manager_receives_user_then_merged_properties() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/config?warehouse=client-wh"
        ))
        .respond(
            StatusCode::OK,
            r#"{"defaults":{"warehouse":"default-wh"},"overrides":{"warehouse":"override-wh"}}"#,
        ),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );
    let init_props = Arc::new(Mutex::new(None));
    let catalog_props = Arc::new(Mutex::new(None));
    let manager = CapturingManager {
        init_props: init_props.clone(),
        catalog_props: catalog_props.clone(),
    };
    let catalog = fixture.catalog_with_auth_manager(
        [("warehouse", "client-wh"), ("custom", "user")],
        Arc::new(manager),
    );

    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    let init = init_props.lock().unwrap().clone().unwrap();
    assert_eq!(
        init.get(REST_CATALOG_PROP_URI).map(String::as_str),
        Some(CATALOG_URI)
    );
    assert_eq!(init.get("warehouse").map(String::as_str), Some("client-wh"));
    assert_eq!(init.get("custom").map(String::as_str), Some("user"));
    let merged = catalog_props.lock().unwrap().clone().unwrap();
    assert_eq!(
        merged.get("warehouse").map(String::as_str),
        Some("override-wh")
    );
    fixture.assert_finished();
}

#[test]
fn injected_auth_manager_overrides_the_configured_auth_type() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    let manager = CapturingManager {
        init_props: Arc::new(Mutex::new(None)),
        catalog_props: Arc::new(Mutex::new(None)),
    };
    let catalog = fixture.catalog_with_auth_manager(
        [(REST_CATALOG_PROP_AUTH_TYPE, "kerberos")],
        Arc::new(manager),
    );
    let endpoint: super::super::Endpoint =
        "GET /v1/{prefix}/namespaces".parse().unwrap();

    assert!(catalog.supports_endpoint(&endpoint).unwrap());
    fixture.assert_finished();
}

#[derive(Debug)]
struct GuardSession(Arc<AtomicBool>);

impl Drop for GuardSession {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl AuthSession for GuardSession {
    fn authenticate(&self, _request: &mut HttpRequest) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct GuardManager(Arc<AtomicBool>);

impl AuthManager for GuardManager {
    fn init_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(GuardSession(self.0.clone())))
    }

    fn catalog_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        if !self.0.load(Ordering::SeqCst) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "init session must be dropped before catalog_session",
            ));
        }
        Ok(Arc::new(PlainSession))
    }
}

#[test]
fn init_session_is_dropped_before_catalog_session_is_created() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    let dropped = Arc::new(AtomicBool::new(false));
    let catalog = fixture
        .catalog_with_auth_manager([], Arc::new(GuardManager(dropped.clone())));

    let endpoint: super::super::Endpoint =
        "GET /v1/{prefix}/namespaces".parse().unwrap();
    assert!(catalog.supports_endpoint(&endpoint).unwrap());
    assert!(dropped.load(Ordering::SeqCst));
    fixture.assert_finished();
}

#[test]
fn unknown_auth_type_fails_before_any_request() {
    let fixture = RestTestFixture::default();
    let catalog = fixture.catalog([(REST_CATALOG_PROP_AUTH_TYPE, "kerberos")]);
    let error = catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    assert!(error.to_string().contains(REST_CATALOG_PROP_AUTH_TYPE));
    assert_eq!(fixture.transport().request_count(), 0);
}
