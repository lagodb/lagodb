use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap};
use http::{Method, Response, StatusCode};

use super::super::auth::AuthSession;
use super::super::catalog::RestCatalogConfig;
use super::super::client::{
    HttpClient, deserialize_unexpected_catalog_error,
    deserialize_unexpected_commit_error, format_headers_redacted,
};
use super::super::request::HttpRequest;
use super::super::types::{RestError, RestErrorKind};
use super::support::{CATALOG_URI, ExpectedExchange, MockHttpTransport};
use crate::Result;

#[test]
fn not_modified_is_a_catalog_response_not_a_redirect() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::get("https://catalog.test/v1/table")
            .respond(StatusCode::NOT_MODIFIED, Bytes::new()),
    );
    let config = RestCatalogConfig::for_test(
        "https://catalog.test",
        Default::default(),
        transport.clone(),
    );
    let client = HttpClient::new(&config).unwrap();
    let response = client
        .query_catalog(HttpRequest::new(
            Method::GET,
            "https://catalog.test/v1/table".parse().unwrap(),
        ))
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    transport.assert_finished();
}

#[test]
fn cross_origin_redirect_removes_configured_authorization() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::get("https://catalog.test/v1/config")
            .header(AUTHORIZATION, "Bearer secret")
            .redirect(
                StatusCode::TEMPORARY_REDIRECT,
                "https://other.test/v1/config",
            ),
    );
    transport.expect(
        ExpectedExchange::get("https://other.test/v1/config")
            .missing_header(AUTHORIZATION)
            .respond(StatusCode::OK, Bytes::new()),
    );
    let config = RestCatalogConfig::for_test(
        "https://catalog.test",
        HashMap::from([(
            "header.authorization".to_owned(),
            "Bearer secret".to_owned(),
        )]),
        transport.clone(),
    );
    let client = HttpClient::new(&config).unwrap();
    client
        .query_catalog(HttpRequest::new(
            Method::GET,
            "https://catalog.test/v1/config".parse().unwrap(),
        ))
        .unwrap();
    transport.assert_finished();
}

#[test]
fn cross_origin_redirect_removes_all_catalog_scoped_headers() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::get("https://catalog.test/v1/config")
            .header("x-tenant", "catalog-a")
            .redirect(
                StatusCode::TEMPORARY_REDIRECT,
                "https://other.test/v1/config",
            ),
    );
    transport.expect(
        ExpectedExchange::get("https://other.test/v1/config")
            .missing_header("x-tenant")
            .respond(StatusCode::OK, Bytes::new()),
    );
    let config = RestCatalogConfig::for_test(
        "https://catalog.test",
        HashMap::from([("header.x-tenant".to_owned(), "catalog-a".to_owned())]),
        transport.clone(),
    );
    let client = HttpClient::new(&config).unwrap();
    client
        .query_catalog(HttpRequest::new(
            Method::GET,
            "https://catalog.test/v1/config".parse().unwrap(),
        ))
        .unwrap();
    transport.assert_finished();
}

#[test]
fn see_other_rewrites_post_to_get_and_removes_entity_headers() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/start"))
            .json_body(serde_json::json!({"value": 1}))
            .redirect(StatusCode::SEE_OTHER, "/v1/final"),
    );
    transport.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/final"))
            .missing_header(CONTENT_TYPE)
            .empty_body()
            .respond(StatusCode::OK, "done"),
    );
    let config =
        RestCatalogConfig::for_test(CATALOG_URI, HashMap::new(), transport.clone());
    let client = HttpClient::new(&config).unwrap();
    let request = super::super::request::HttpRequestBuilder::from_str(
        Method::POST,
        &format!("{CATALOG_URI}/v1/start"),
    )
    .json(&serde_json::json!({"value": 1}))
    .build()
    .unwrap();

    let response = client.query_catalog(request).unwrap();

    assert_eq!(response.body(), &Bytes::from_static(b"done"));
    transport.assert_finished();
}

#[test]
fn temporary_redirect_preserves_post_and_body() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/start"))
            .json_body(serde_json::json!("payload"))
            .redirect(StatusCode::TEMPORARY_REDIRECT, "/v1/final"),
    );
    transport.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/final"))
            .json_body(serde_json::json!("payload"))
            .respond(StatusCode::OK, "done"),
    );
    let config =
        RestCatalogConfig::for_test(CATALOG_URI, HashMap::new(), transport.clone());
    let client = HttpClient::new(&config).unwrap();
    let request = super::super::request::HttpRequestBuilder::from_str(
        Method::POST,
        &format!("{CATALOG_URI}/v1/start"),
    )
    .json(&"payload")
    .build()
    .unwrap();

    client.query_catalog(request).unwrap();
    transport.assert_finished();
}

#[test]
fn redirect_without_location_is_rejected() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/start"))
            .respond(StatusCode::TEMPORARY_REDIRECT, ""),
    );
    let config =
        RestCatalogConfig::for_test(CATALOG_URI, HashMap::new(), transport.clone());
    let client = HttpClient::new(&config).unwrap();

    let error = client
        .query_catalog(HttpRequest::new(
            Method::GET,
            format!("{CATALOG_URI}/v1/start").parse().unwrap(),
        ))
        .unwrap_err();

    assert!(error.to_string().contains("Location"));
    transport.assert_finished();
}

#[test]
fn redirect_limit_is_enforced() {
    let transport = Arc::new(MockHttpTransport::default());
    for index in 0..=10 {
        transport.expect(
            ExpectedExchange::get(format!("{CATALOG_URI}/v1/{index}")).redirect(
                StatusCode::TEMPORARY_REDIRECT,
                &format!("{CATALOG_URI}/v1/{}", index + 1),
            ),
        );
    }
    let config =
        RestCatalogConfig::for_test(CATALOG_URI, HashMap::new(), transport.clone());
    let client = HttpClient::new(&config).unwrap();

    let error = client
        .query_catalog(HttpRequest::new(
            Method::GET,
            format!("{CATALOG_URI}/v1/0").parse().unwrap(),
        ))
        .unwrap_err();

    assert!(error.to_string().contains("redirect limit"));
    transport.assert_finished();
}

#[derive(Debug)]
struct StaticSession;

impl AuthSession for StaticSession {
    fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
        request
            .headers_mut()
            .insert(AUTHORIZATION, "Bearer token".parse().unwrap());
        Ok(())
    }
}

#[test]
fn post_form_uses_the_session_until_the_caller_removes_it() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post("https://auth.test/token")
            .header(AUTHORIZATION, "Bearer token")
            .respond(StatusCode::OK, ""),
    );
    transport.expect(
        ExpectedExchange::post("https://auth.test/token")
            .missing_header(AUTHORIZATION)
            .respond(StatusCode::OK, ""),
    );
    let config =
        RestCatalogConfig::for_test(CATALOG_URI, HashMap::new(), transport.clone());
    let client = HttpClient::new(&config)
        .unwrap()
        .with_auth_session(Arc::new(StaticSession));

    client
        .post_form(
            "https://auth.test/token",
            &HeaderMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    client
        .without_auth_session()
        .post_form(
            "https://auth.test/token",
            &HeaderMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    transport.assert_finished();
}

#[test]
fn header_redaction_covers_empty_non_sensitive_and_sensitive_sets() {
    assert_eq!(format_headers_redacted(&HeaderMap::new(), false), "{}");

    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", "request-1".parse().unwrap());
    let visible = format_headers_redacted(&headers, false);
    assert!(visible.contains("x-request-id"));
    assert!(visible.contains("request-1"));

    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("set-cookie", "session=secret".parse().unwrap());
    let mixed = format_headers_redacted(&headers, false);
    assert!(mixed.contains("request-1"));
    assert!(mixed.contains("[REDACTED]"));
    assert!(!mixed.contains("secret"));

    headers.remove("x-request-id");
    let sensitive_only = format_headers_redacted(&headers, false);
    assert!(sensitive_only.contains("[REDACTED]"));
    assert!(!sensitive_only.contains("secret"));
}

#[test]
fn client_debug_and_unexpected_errors_redact_sensitive_headers() {
    let secret = "must-not-leak";
    let transport = Arc::new(MockHttpTransport::default());
    let config = RestCatalogConfig::for_test(
        CATALOG_URI,
        HashMap::from([
            (
                "header.authorization".to_owned(),
                format!("Bearer {secret}"),
            ),
            ("header.x-api-key".to_owned(), secret.to_owned()),
        ]),
        transport,
    );
    let client = HttpClient::new(&config).unwrap();
    let debug = format!("{client:?}");
    assert!(!debug.contains(secret));
    assert!(debug.contains("[REDACTED]"));

    let response = Response::builder()
        .status(StatusCode::IM_A_TEAPOT)
        .header("set-cookie", format!("session={secret}"))
        .header("x-request-id", "request-1")
        .body(Bytes::from_static(b"not-json"))
        .unwrap();
    let error = deserialize_unexpected_catalog_error(response, false);
    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<RestError>())
        .expect("malformed REST error body must retain its HTTP status");
    assert_eq!(error.kind(), crate::ErrorKind::Unexpected);
    assert_eq!(source.status(), StatusCode::IM_A_TEAPOT);
    assert_eq!(source.kind(), RestErrorKind::Client);

    let rendered = error.to_string();
    assert!(!rendered.contains(secret));
    assert!(rendered.contains("[REDACTED]"));
    assert!(rendered.contains("request-1"));
}

#[test]
fn redaction_can_be_explicitly_disabled() {
    let response = Response::builder()
        .status(StatusCode::IM_A_TEAPOT)
        .header("authorization", "Bearer visible-for-debugging")
        .body(Bytes::from_static(b"not-json"))
        .unwrap();

    let error = deserialize_unexpected_catalog_error(response, true).to_string();

    assert!(error.contains("Bearer visible-for-debugging"));
    assert!(!error.contains("[REDACTED]"));
}

#[test]
fn rest_error_payload_retains_structured_http_classification() {
    let response = Response::builder()
        .status(StatusCode::FORBIDDEN)
        .body(Bytes::from_static(
            br#"{"error":{"message":"access denied","type":"ForbiddenException","code":403}}"#,
        ))
        .unwrap();

    let error = deserialize_unexpected_catalog_error(response, false);
    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<RestError>())
        .expect("REST error source must remain structured");

    assert_eq!(error.kind(), crate::ErrorKind::Unexpected);
    assert_eq!(source.status(), StatusCode::FORBIDDEN);
    assert_eq!(source.kind(), RestErrorKind::Forbidden);
    assert_eq!(source.error_type(), Some("ForbiddenException"));
    assert_eq!(source.response_code(), Some(403));
}

#[test]
fn commit_error_retains_conflict_and_unknown_outcome_semantics() {
    let conflict = Response::builder()
        .status(StatusCode::CONFLICT)
        .body(Bytes::from_static(
            br#"{"error":{"message":"requirement failed","type":"CommitFailedException","code":409}}"#,
        ))
        .unwrap();
    let conflict = deserialize_unexpected_commit_error(conflict, false);
    let source = conflict
        .source()
        .and_then(|source| source.downcast_ref::<RestError>())
        .expect("commit conflict must remain structured");
    assert_eq!(conflict.kind(), crate::ErrorKind::CatalogCommitConflicts);
    assert!(conflict.retryable());
    assert_eq!(source.kind(), RestErrorKind::CommitConflict);

    let unknown = Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(Bytes::from_static(
            br#"{"error":{"message":"publication outcome unknown","type":"CommitStateUnknown","code":503}}"#,
        ))
        .unwrap();
    let unknown = deserialize_unexpected_commit_error(unknown, false);
    let source = unknown
        .source()
        .and_then(|source| source.downcast_ref::<RestError>())
        .expect("unknown commit outcome must remain structured");
    assert_eq!(unknown.kind(), crate::ErrorKind::Unexpected);
    assert!(!unknown.retryable());
    assert_eq!(source.kind(), RestErrorKind::CommitStateUnknown);
    assert_eq!(source.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(source.error_type(), Some("CommitStateUnknown"));
}
