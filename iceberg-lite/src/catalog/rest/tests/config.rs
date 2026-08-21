use std::collections::HashMap;
use std::sync::Arc;

use http::header::{CONTENT_TYPE, USER_AGENT};
use http::{HeaderValue, StatusCode};

use super::super::catalog::{
    REST_CATALOG_PROP_URI, RestCatalogConfig, RestSessionCatalogBuilder,
};
use super::super::endpoint::Endpoint;
use super::support::{
    CATALOG_URI, ExpectedExchange, MockHttpTransport, RestTestFixture,
};
use crate::{ErrorKind, NamespaceIdent, SessionCatalog, SessionContext};

#[test]
fn server_config_overrides_uri_prefix_and_warehouse() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "overrides": {
            "uri": "https://redirected.test",
            "warehouse": "s3://iceberg-catalog",
            "prefix": "ice/warehouses/my"
        },
        "defaults": {}
    }));
    fixture.expect(
        ExpectedExchange::get(
            "https://redirected.test/v1/ice/warehouses/my/namespaces",
        )
        .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture.catalog([]);
    let namespaces = catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    assert!(namespaces.is_empty());
    fixture.assert_finished();
}

#[test]
fn client_properties_override_defaults_but_server_overrides_win() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {
            "header.x-default": "server-default",
            "header.x-client": "server-default"
        },
        "overrides": {
            "header.x-override": "server-override",
            "header.x-client-override": "server-override"
        }
    }));
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header("x-default", "server-default")
            .header("x-client", "client")
            .header("x-override", "server-override")
            .header("x-client-override", "server-override")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture.catalog([
        ("header.x-client", "client"),
        ("header.x-client-override", "client"),
    ]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn advertised_endpoints_replace_the_default_capability_set() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "overrides": {},
        "defaults": {},
        "endpoints": [
            "GET /v1/{prefix}/namespaces",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"
        ]
    }));
    let catalog = fixture.catalog([]);

    let plan: Endpoint =
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"
            .parse()
            .unwrap();
    let delete_namespace: Endpoint = "DELETE /v1/{prefix}/namespaces/{namespace}"
        .parse()
        .unwrap();
    assert!(catalog.supports_endpoint(&plan).unwrap());
    assert!(!catalog.supports_endpoint(&delete_namespace).unwrap());
    fixture.assert_finished();
}

#[test]
fn absent_or_empty_endpoint_list_uses_the_default_capability_set() {
    for config in [
        serde_json::json!({"overrides": {}, "defaults": {}}),
        serde_json::json!({"overrides": {}, "defaults": {}, "endpoints": []}),
    ] {
        let fixture = RestTestFixture::default();
        fixture.expect_config(config);
        let catalog = fixture.catalog([]);
        let load_table: Endpoint =
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
                .parse()
                .unwrap();
        let plan: Endpoint =
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"
                .parse()
                .unwrap();
        assert!(catalog.supports_endpoint(&load_table).unwrap());
        assert!(!catalog.supports_endpoint(&plan).unwrap());
        fixture.assert_finished();
    }
}

#[test]
fn warehouse_is_encoded_on_the_config_handshake() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/config?warehouse=s3%3A%2F%2Fwarehouse%2Ftenant+a"
        ))
        .respond(StatusCode::OK, r#"{"overrides":{},"defaults":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );
    let catalog = fixture.catalog([("warehouse", "s3://warehouse/tenant a")]);

    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();
    fixture.assert_finished();
}

#[test]
fn standard_and_custom_headers_are_applied() {
    let transport = Arc::new(MockHttpTransport::default());
    let config = RestCatalogConfig::for_test(
        CATALOG_URI,
        HashMap::from([
            (
                "header.content-type".to_owned(),
                "application/yaml".to_owned(),
            ),
            (
                "header.customized-header".to_owned(),
                "some/value".to_owned(),
            ),
        ]),
        transport,
    );

    let headers = config.extra_headers().unwrap();
    assert_eq!(
        headers.get(CONTENT_TYPE),
        Some(&HeaderValue::from_static("application/yaml"))
    );
    assert_eq!(
        headers.get("x-client-version"),
        Some(&HeaderValue::from_static("0.14.1"))
    );
    assert_eq!(
        headers.get("customized-header"),
        Some(&HeaderValue::from_static("some/value"))
    );
    assert_eq!(
        headers.get(USER_AGENT).unwrap().to_str().unwrap(),
        concat!("iceberg-rs/", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn invalid_explicit_header_is_rejected_without_leaking_its_value() {
    let secret = "secret\nvalue";
    let transport = Arc::new(MockHttpTransport::default());
    let config = RestCatalogConfig::for_test(
        CATALOG_URI,
        HashMap::from([("header.authorization".to_owned(), secret.to_owned())]),
        transport,
    );

    let error = config.extra_headers().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    assert!(!error.to_string().contains(secret));
}

#[test]
fn builder_requires_uri_and_injected_transport() {
    let missing_uri = RestSessionCatalogBuilder::default()
        .with_http_transport(Arc::new(MockHttpTransport::default()))
        .load("test", HashMap::new())
        .unwrap_err();
    assert_eq!(missing_uri.kind(), ErrorKind::DataInvalid);

    let missing_transport = RestSessionCatalogBuilder::default()
        .load(
            "test",
            HashMap::from([(
                REST_CATALOG_PROP_URI.to_owned(),
                CATALOG_URI.to_owned(),
            )]),
        )
        .unwrap_err();
    assert_eq!(missing_transport.kind(), ErrorKind::DataInvalid);
}

#[test]
fn config_debug_redacts_secrets_and_connection_values() {
    let secret = "must-not-leak";
    let config = RestCatalogConfig::for_test(
        CATALOG_URI,
        HashMap::from([
            ("credential".to_owned(), secret.to_owned()),
            ("header.authorization".to_owned(), secret.to_owned()),
            ("ordinary".to_owned(), "visible".to_owned()),
        ]),
        Arc::new(MockHttpTransport::default()),
    );

    let debug = format!("{config:?}");
    assert!(!debug.contains(secret));
    assert!(!debug.contains(CATALOG_URI));
    assert!(debug.contains("visible"));
}

#[test]
fn parent_namespace_is_url_and_query_encoded() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"overrides": {}, "defaults": {}}));
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces?parent=parent%1Fchild"
        ))
        .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );
    let catalog = fixture.catalog([]);
    let parent =
        NamespaceIdent::from_vec(vec!["parent".to_owned(), "child".to_owned()])
            .unwrap();

    catalog
        .list_namespaces(&SessionContext::empty(), Some(&parent))
        .unwrap();
    fixture.assert_finished();
}
