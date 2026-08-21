use std::collections::HashMap;

use http::{Method, StatusCode};

use super::super::endpoint::Endpoint;
use super::super::request::{HttpRequest, HttpRequestBody, HttpRequestBuilder};
use super::super::types::{CatalogConfig, CreateTableRequest, NamespaceResponse};
use crate::NamespaceIdent;
use crate::io::StorageCredential;
use crate::spec::Schema;

#[test]
fn endpoint_round_trips_and_rejects_ambiguous_wire_forms() {
    let endpoint: Endpoint = "post /v1/{prefix}/namespaces/{namespace}/tables"
        .parse()
        .unwrap();
    assert_eq!(endpoint.method(), "POST");
    assert_eq!(
        endpoint.path(),
        "/v1/{prefix}/namespaces/{namespace}/tables"
    );
    assert_eq!(
        serde_json::from_str::<Endpoint>(&serde_json::to_string(&endpoint).unwrap())
            .unwrap(),
        endpoint
    );
    for invalid in ["GET", "GET  /v1", " GET /v1", "GET ", " /v1", ""] {
        assert!(invalid.parse::<Endpoint>().is_err());
    }
}

#[test]
fn create_table_request_accepts_explicit_null_optional_fields() {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "tbl1",
        "location": null,
        "schema": table_schema(),
        "partition-spec": null,
        "write-order": null,
        "stage-create": null
    }))
    .unwrap();

    assert_eq!(request.name, "tbl1");
    assert!(request.location.is_none());
    assert!(request.partition_spec.is_none());
    assert!(request.write_order.is_none());
    assert!(request.stage_create.is_none());
    assert!(request.properties.is_empty());
}

#[test]
fn request_builder_encodes_query_and_buffered_json() {
    let empty = HttpRequest::new(
        Method::GET,
        "https://catalog.test/v1/config".parse().unwrap(),
    );
    assert_eq!(empty.body(), HttpRequestBody::Empty);
    assert!(empty.body().as_bytes().is_empty());

    let request =
        HttpRequestBuilder::from_str(Method::POST, "https://catalog.test/v1/config")
            .query(&[("warehouse", "s3://bucket/a b")])
            .json(&serde_json::json!({"name": "table"}))
            .build()
            .unwrap();
    assert_eq!(
        request.url_str(),
        "https://catalog.test/v1/config?warehouse=s3%3A%2F%2Fbucket%2Fa+b"
    );
    assert_eq!(
        request.body(),
        HttpRequestBody::Buffered(br#"{"name":"table"}"#)
    );
    assert_eq!(request.body().as_bytes(), br#"{"name":"table"}"#);
}

#[test]
fn storage_credential_debug_never_displays_values() {
    let secret = "credential-value-that-must-not-leak";
    let credential = StorageCredential::new(
        "s3://bucket/table/",
        HashMap::from([("s3.secret-access-key".to_owned(), secret.to_owned())]),
    );
    let output = format!("{credential:?}");
    assert!(output.contains("s3.secret-access-key"));
    assert!(!output.contains(secret));
}

#[test]
fn see_other_preserves_head_method_and_removes_payload() {
    let mut request =
        HttpRequestBuilder::from_str(Method::HEAD, "https://catalog.test/v1/config")
            .json(&serde_json::json!({"probe": true}))
            .build()
            .unwrap();
    request.follow_redirect(
        StatusCode::SEE_OTHER,
        "https://catalog.test/v1/other".parse().unwrap(),
    );
    assert_eq!(request.method(), Method::HEAD);
    assert_eq!(request.body(), HttpRequestBody::Empty);
}

#[test]
fn namespace_response_serde_matches_the_rest_schema() {
    let json = serde_json::json!({
        "namespace": ["nested", "ns"],
        "properties": {"key1": "value1", "key2": "value2"}
    });
    let response: NamespaceResponse = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(
        response,
        NamespaceResponse {
            namespace: NamespaceIdent::from_vec(vec![
                "nested".to_owned(),
                "ns".to_owned()
            ])
            .unwrap(),
            properties: HashMap::from([
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned()),
            ]),
        }
    );
    assert_eq!(serde_json::to_value(response).unwrap(), json);

    let without_properties = serde_json::json!({"namespace": ["db", "schema"]});
    let response: NamespaceResponse =
        serde_json::from_value(without_properties.clone()).unwrap();
    assert!(response.properties.is_empty());
    assert_eq!(serde_json::to_value(response).unwrap(), without_properties);
}

fn table_schema() -> Schema {
    serde_json::from_value(serde_json::json!({
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {"id": 1, "name": "foo", "required": false, "type": "string"},
            {"id": 2, "name": "bar", "required": true, "type": "int"}
        ],
        "identifier-field-ids": [2]
    }))
    .unwrap()
}

#[test]
fn create_table_request_omits_absent_optional_fields() {
    let request = CreateTableRequest {
        name: "tbl1".to_owned(),
        location: None,
        schema: table_schema(),
        partition_spec: None,
        write_order: None,
        stage_create: None,
        properties: HashMap::new(),
    };
    let serialized = serde_json::to_value(request).unwrap();
    let object = serialized.as_object().unwrap();
    assert!(object.contains_key("name"));
    assert!(object.contains_key("schema"));
    for absent in [
        "location",
        "partition-spec",
        "write-order",
        "stage-create",
        "properties",
    ] {
        assert!(!object.contains_key(absent));
    }
}

#[test]
fn create_table_request_round_trips_full_wire_shape() {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "tbl1",
        "location": "s3://warehouse/tbl1",
        "schema": table_schema(),
        "partition-spec": {
            "spec-id": 1,
            "fields": [{
                "source-id": 2,
                "field-id": 1000,
                "name": "bar",
                "transform": "identity"
            }]
        },
        "write-order": {
            "order-id": 1,
            "fields": [{
                "transform": "identity",
                "source-id": 2,
                "direction": "asc",
                "null-order": "nulls-first"
            }]
        },
        "stage-create": true,
        "properties": {"owner": "test"}
    }))
    .unwrap();
    let serialized = serde_json::to_value(request).unwrap();
    let object = serialized.as_object().unwrap();
    assert_eq!(
        object.get("location"),
        Some(&serde_json::json!("s3://warehouse/tbl1"))
    );
    assert_eq!(object.get("stage-create"), Some(&serde_json::json!(true)));
    assert!(object.contains_key("partition-spec"));
    assert!(object.contains_key("write-order"));
    assert!(object.contains_key("properties"));
}

#[test]
fn catalog_config_endpoint_negotiation_is_strict() {
    let config: CatalogConfig = serde_json::from_str(
        r#"{"overrides":{},"defaults":{},"endpoints":["GET /v1/{prefix}/namespaces"]}"#,
    )
    .unwrap();
    assert_eq!(config.endpoints.unwrap().len(), 1);

    let without_endpoints: CatalogConfig =
        serde_json::from_str(r#"{"overrides":{},"defaults":{}}"#).unwrap();
    assert!(without_endpoints.endpoints.is_none());

    let malformed =
        r#"{"overrides":{},"defaults":{},"endpoints":["GET_v1/namespaces"]}"#;
    assert!(serde_json::from_str::<CatalogConfig>(malformed).is_err());
}
