use std::collections::HashMap;

use http::StatusCode;

use super::support::{CATALOG_URI, ExpectedExchange, RestTestFixture};
use crate::{
    ErrorKind, Namespace, NamespaceIdent, SessionCatalog, SessionContext, TableIdent,
};

#[test]
fn list_namespaces_collects_every_page_and_applies_parent_filter() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces?parent=parent"))
            .respond(
                StatusCode::OK,
                r#"{
                "namespaces":[["parent","one"],["parent","two"]],
                "next-page-token":"page two"
            }"#,
            ),
    );
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces?parent=parent&pageToken=page+two"
        ))
        .respond(StatusCode::OK, r#"{"namespaces":[["parent","three"]]}"#),
    );
    let catalog = fixture.catalog([]);

    let actual = catalog
        .list_namespaces(
            &SessionContext::empty(),
            Some(&NamespaceIdent::new("parent".to_owned())),
        )
        .unwrap();

    let expected = vec![
        NamespaceIdent::from_strs(["parent", "one"]).unwrap(),
        NamespaceIdent::from_strs(["parent", "two"]).unwrap(),
        NamespaceIdent::from_strs(["parent", "three"]).unwrap(),
    ];
    assert_eq!(actual, expected);
    fixture.assert_finished();
}

#[test]
fn create_get_and_drop_namespace_use_the_rest_wire_shapes() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces"))
            .json_body(serde_json::json!({
                "namespace": ["ns1", "ns11"],
                "properties": {"owner": "lakebase"}
            }))
            .respond(
                StatusCode::OK,
                r#"{"namespace":["ns1","ns11"],"properties":{"owner":"lakebase"}}"#,
            ),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces/ns1%1Fns11"))
            .respond(
                StatusCode::OK,
                r#"{"namespace":["ns1","ns11"],"properties":{"owner":"lakebase"}}"#,
            ),
    );
    fixture.expect(
        ExpectedExchange::delete(format!("{CATALOG_URI}/v1/namespaces/ns1%1Fns11"))
            .respond(StatusCode::NO_CONTENT, ""),
    );
    let catalog = fixture.catalog([]);
    let ident = NamespaceIdent::from_strs(["ns1", "ns11"]).unwrap();
    let properties = HashMap::from([("owner".to_owned(), "lakebase".to_owned())]);

    assert_eq!(
        catalog
            .create_namespace(&SessionContext::empty(), &ident, properties.clone())
            .unwrap(),
        Namespace::with_properties(ident.clone(), properties)
    );
    assert_eq!(
        catalog
            .get_namespace(&SessionContext::empty(), &ident)
            .unwrap()
            .name(),
        &ident
    );
    catalog
        .drop_namespace(&SessionContext::empty(), &ident)
        .unwrap();
    fixture.assert_finished();
}

#[test]
fn namespace_statuses_map_to_domain_errors() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces"))
            .respond(StatusCode::CONFLICT, ""),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces/missing"))
            .respond(StatusCode::NOT_FOUND, ""),
    );
    fixture.expect(
        ExpectedExchange::delete(format!("{CATALOG_URI}/v1/namespaces/missing"))
            .respond(StatusCode::NOT_FOUND, ""),
    );
    let catalog = fixture.catalog([]);
    let missing = NamespaceIdent::new("missing".to_owned());

    let create_error = catalog
        .create_namespace(&SessionContext::empty(), &missing, HashMap::new())
        .unwrap_err();
    assert_eq!(create_error.kind(), ErrorKind::NamespaceAlreadyExists);
    let get_error = catalog
        .get_namespace(&SessionContext::empty(), &missing)
        .unwrap_err();
    assert_eq!(get_error.kind(), ErrorKind::NamespaceNotFound);
    let drop_error = catalog
        .drop_namespace(&SessionContext::empty(), &missing)
        .unwrap_err();
    assert_eq!(drop_error.kind(), ErrorKind::NamespaceNotFound);
    fixture.assert_finished();
}

#[test]
fn namespace_exists_uses_head_only_when_the_server_advertises_it() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {},
        "overrides": {},
        "endpoints": ["HEAD /v1/{prefix}/namespaces/{namespace}"]
    }));
    fixture.expect(
        ExpectedExchange::head(format!("{CATALOG_URI}/v1/namespaces/present"))
            .respond(StatusCode::NO_CONTENT, ""),
    );
    fixture.expect(
        ExpectedExchange::head(format!("{CATALOG_URI}/v1/namespaces/missing"))
            .respond(StatusCode::NOT_FOUND, ""),
    );
    let catalog = fixture.catalog([]);

    assert!(
        catalog
            .namespace_exists(
                &SessionContext::empty(),
                &NamespaceIdent::new("present".to_owned()),
            )
            .unwrap()
    );
    assert!(
        !catalog
            .namespace_exists(
                &SessionContext::empty(),
                &NamespaceIdent::new("missing".to_owned()),
            )
            .unwrap()
    );
    fixture.assert_finished();
}

#[test]
fn namespace_exists_falls_back_to_get_without_advertised_head() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces/present"))
            .respond(
                StatusCode::OK,
                r#"{"namespace":["present"],"properties":{}}"#,
            ),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces/missing"))
            .respond(StatusCode::NOT_FOUND, ""),
    );
    let catalog = fixture.catalog([]);

    assert!(
        catalog
            .namespace_exists(
                &SessionContext::empty(),
                &NamespaceIdent::new("present".to_owned()),
            )
            .unwrap()
    );
    assert!(
        !catalog
            .namespace_exists(
                &SessionContext::empty(),
                &NamespaceIdent::new("missing".to_owned()),
            )
            .unwrap()
    );
    fixture.assert_finished();
}

#[test]
fn update_namespace_reports_the_unsupported_operation_without_http() {
    let fixture = RestTestFixture::default();
    let catalog = fixture.catalog([]);
    let error = catalog
        .update_namespace(
            &SessionContext::empty(),
            &NamespaceIdent::new("ns".to_owned()),
            HashMap::new(),
        )
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    assert_eq!(fixture.transport().request_count(), 0);
}

#[test]
fn list_tables_collects_every_page() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces/ns1/tables"))
            .respond(
                StatusCode::OK,
                r#"{
                    "identifiers":[
                        {"namespace":["ns1"],"name":"table1"},
                        {"namespace":["ns1"],"name":"table2"}
                    ],
                    "next-page-token":"next"
                }"#,
            ),
    );
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables?pageToken=next"
        ))
        .respond(
            StatusCode::OK,
            r#"{"identifiers":[{"namespace":["ns1"],"name":"table3"}]}"#,
        ),
    );
    let catalog = fixture.catalog([]);
    let namespace = NamespaceIdent::new("ns1".to_owned());

    assert_eq!(
        catalog
            .list_tables(&SessionContext::empty(), &namespace)
            .unwrap(),
        vec![
            TableIdent::new(namespace.clone(), "table1".to_owned()),
            TableIdent::new(namespace.clone(), "table2".to_owned()),
            TableIdent::new(namespace, "table3".to_owned()),
        ]
    );
    fixture.assert_finished();
}

#[test]
fn list_tables_maps_missing_namespace() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces/missing/tables"))
            .respond(StatusCode::NOT_FOUND, ""),
    );
    let catalog = fixture.catalog([]);

    let error = catalog
        .list_tables(
            &SessionContext::empty(),
            &NamespaceIdent::new("missing".to_owned()),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
    fixture.assert_finished();
}
