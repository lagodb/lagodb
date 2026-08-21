use std::collections::HashMap;

use http::StatusCode;

use super::support::{CATALOG_URI, ExpectedExchange, RestTestFixture};
use crate::catalog::rest::LoadTableResult;
use crate::spec::{FormatVersion, Schema};
use crate::table::Table;
use crate::transaction::PreparedTableCommit;
use crate::{
    Catalog, ErrorKind, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    TableUpdate,
};

const CREATE_TABLE_RESPONSE: &str =
    include_str!("../../../../testdata/rest/create_table_response.json");
const LOAD_TABLE_RESPONSE: &str =
    include_str!("../../../../testdata/rest/load_table_response.json");
const UPDATE_TABLE_RESPONSE: &str =
    include_str!("../../../../testdata/rest/update_table_response.json");

fn table_ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("ns1".to_owned()), name.to_owned())
}

fn table_schema() -> Schema {
    serde_json::from_value(serde_json::json!({
        "type": "struct",
        "schema-id": 1,
        "identifier-field-ids": [2],
        "fields": [
            {"id": 1, "name": "foo", "required": false, "type": "string"},
            {"id": 2, "name": "bar", "required": true, "type": "int"},
            {"id": 3, "name": "baz", "required": false, "type": "boolean"}
        ]
    }))
    .unwrap()
}

#[test]
fn drop_purge_and_rename_tables_use_the_expected_routes_and_bodies() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::delete(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/table1"
        ))
        .respond(StatusCode::NO_CONTENT, ""),
    );
    fixture.expect(
        ExpectedExchange::delete(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/table2?purgeRequested=true"
        ))
        .respond(StatusCode::NO_CONTENT, ""),
    );
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/tables/rename"))
            .json_body(serde_json::json!({
                "source": {"namespace": ["ns1"], "name": "table1"},
                "destination": {"namespace": ["ns1"], "name": "renamed"}
            }))
            .respond(StatusCode::NO_CONTENT, ""),
    );
    let catalog = fixture.bound_catalog([]);

    catalog.drop_table(&table_ident("table1")).unwrap();
    catalog.purge_table(&table_ident("table2")).unwrap();
    catalog
        .rename_table(&table_ident("table1"), &table_ident("renamed"))
        .unwrap();
    fixture.assert_finished();
}

#[test]
fn table_exists_uses_head_only_when_advertised() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {},
        "overrides": {},
        "endpoints": ["HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"]
    }));
    fixture.expect(
        ExpectedExchange::head(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/present"
        ))
        .respond(StatusCode::NO_CONTENT, ""),
    );
    fixture.expect(
        ExpectedExchange::head(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/missing"
        ))
        .respond(StatusCode::NOT_FOUND, ""),
    );
    let catalog = fixture.bound_catalog([]);

    assert!(catalog.table_exists(&table_ident("present")).unwrap());
    assert!(!catalog.table_exists(&table_ident("missing")).unwrap());
    assert!(fixture.storage_configs().is_empty());
    fixture.assert_finished();
}

#[test]
fn table_exists_get_fallback_does_not_construct_storage() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/present"
        ))
        .respond(StatusCode::OK, LOAD_TABLE_RESPONSE),
    );
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/missing"
        ))
        .respond(StatusCode::NOT_FOUND, ""),
    );
    let catalog = fixture.bound_catalog([]);

    assert!(catalog.table_exists(&table_ident("present")).unwrap());
    assert!(!catalog.table_exists(&table_ident("missing")).unwrap());
    assert!(fixture.storage_configs().is_empty());
    fixture.assert_finished();
}

#[test]
fn load_table_materializes_metadata_and_passes_merged_storage_configuration() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {"s3.region": "default-region"},
        "overrides": {}
    }));
    let mut response: serde_json::Value =
        serde_json::from_str(LOAD_TABLE_RESPONSE).unwrap();
    response["config"]["s3.region"] = serde_json::json!("table-region");
    response["storage-credentials"] = serde_json::json!([{
        "prefix": "s3://warehouse/database/table/",
        "config": {
            "s3.access-key-id": "key",
            "s3.secret-access-key": "secret"
        }
    }]);
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .respond(StatusCode::OK, serde_json::to_vec(&response).unwrap()),
    );
    let catalog = fixture.bound_catalog([("s3.endpoint", "https://s3.test")]);

    let table = catalog.load_table(&table_ident("test1")).unwrap();

    assert_eq!(table.identifier(), &table_ident("test1"));
    assert_eq!(table.metadata().format_version(), FormatVersion::V1);
    assert_eq!(table.metadata().location(), "s3://warehouse/database/table");
    assert_eq!(
        table.metadata().uuid().to_string(),
        "b55d9dda-6561-423a-8bfc-787980ce421f"
    );
    assert_eq!(
        table
            .metadata()
            .last_updated_timestamp()
            .unwrap()
            .timestamp_millis(),
        1_646_787_054_459
    );
    assert_eq!(table.metadata().schemas_iter().count(), 1);
    assert_eq!(table.metadata().snapshots().count(), 1);
    assert_eq!(table.metadata().history().len(), 1);
    assert_eq!(
        table
            .metadata()
            .properties()
            .get("owner")
            .map(String::as_str),
        Some("bryan")
    );
    assert_eq!(
        table.metadata_location(),
        Some(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json"
        )
    );
    let configs = fixture.storage_configs();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].location(), table.metadata_location().unwrap());
    assert_eq!(
        configs[0]
            .properties()
            .get("s3.endpoint")
            .map(String::as_str),
        Some("https://s3.test")
    );
    assert_eq!(
        configs[0].properties().get("s3.region").map(String::as_str),
        Some("table-region")
    );
    assert_eq!(configs[0].credentials().len(), 1);
    assert_eq!(
        configs[0].credentials()[0].prefix(),
        "s3://warehouse/database/table/"
    );
    fixture.assert_finished();
}

#[test]
fn load_table_without_metadata_location_uses_server_warehouse() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {},
        "overrides": {"warehouse": "s3://server-warehouse"}
    }));
    let mut response: serde_json::Value =
        serde_json::from_str(LOAD_TABLE_RESPONSE).unwrap();
    response
        .as_object_mut()
        .unwrap()
        .remove("metadata-location");
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .respond(StatusCode::OK, serde_json::to_vec(&response).unwrap()),
    );
    let catalog = fixture.bound_catalog([]);

    let table = catalog.load_table(&table_ident("test1")).unwrap();

    assert_eq!(table.metadata_location(), None);
    let configs = fixture.storage_configs();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].location(), "s3://server-warehouse");
    assert_eq!(
        configs[0].properties().get("warehouse").map(String::as_str),
        Some("s3://server-warehouse")
    );
    fixture.assert_finished();
}

#[test]
fn server_warehouse_override_replaces_client_warehouse_for_storage() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/config?warehouse=s3%3A%2F%2Fclient-warehouse"
        ))
        .respond(
            StatusCode::OK,
            r#"{"defaults":{},"overrides":{"warehouse":"s3://server-warehouse"}}"#,
        ),
    );
    let mut response: serde_json::Value =
        serde_json::from_str(LOAD_TABLE_RESPONSE).unwrap();
    response
        .as_object_mut()
        .unwrap()
        .remove("metadata-location");
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .respond(StatusCode::OK, serde_json::to_vec(&response).unwrap()),
    );
    let catalog = fixture.bound_catalog([("warehouse", "s3://client-warehouse")]);

    catalog.load_table(&table_ident("test1")).unwrap();

    let configs = fixture.storage_configs();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].location(), "s3://server-warehouse");
    assert_eq!(
        configs[0].properties().get("warehouse").map(String::as_str),
        Some("s3://server-warehouse")
    );
    fixture.assert_finished();
}

#[test]
fn create_table_serializes_the_request_and_builds_table_storage() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces/ns1/tables"))
            .json_body(serde_json::json!({
                "name": "test1",
                "location": "s3://warehouse/custom/test1",
                "schema": table_schema(),
                "stage-create": false,
                "properties": {"owner": "test"}
            }))
            .respond(StatusCode::OK, CREATE_TABLE_RESPONSE),
    );
    let catalog = fixture.bound_catalog([]);
    let creation = TableCreation::builder()
        .name("test1".to_owned())
        .location("s3://warehouse/custom/test1".to_owned())
        .schema(table_schema())
        .properties(HashMap::from([("owner".to_owned(), "test".to_owned())]))
        .build();

    let table = catalog
        .create_table(&NamespaceIdent::new("ns1".to_owned()), creation)
        .unwrap();

    assert_eq!(table.identifier(), &table_ident("test1"));
    assert_eq!(
        table.metadata_location(),
        Some("s3://warehouse/database/table/metadata.json")
    );
    assert_eq!(table.metadata().format_version(), FormatVersion::V1);
    assert_eq!(
        table.metadata().uuid().to_string(),
        "bf289591-dcc0-4234-ad4f-5c3eed811a29"
    );
    assert!(table.metadata().current_snapshot().is_none());
    assert!(table.metadata().history().is_empty());
    assert_eq!(fixture.storage_configs().len(), 1);
    fixture.assert_finished();
}

#[test]
fn load_and_create_statuses_map_to_domain_errors() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/missing"
        ))
        .respond(StatusCode::NOT_FOUND, ""),
    );
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces/ns1/tables"))
            .respond(StatusCode::NOT_FOUND, ""),
    );
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces/ns1/tables"))
            .respond(StatusCode::CONFLICT, ""),
    );
    let catalog = fixture.bound_catalog([]);
    let namespace = NamespaceIdent::new("ns1".to_owned());

    assert_eq!(
        catalog
            .load_table(&table_ident("missing"))
            .unwrap_err()
            .kind(),
        ErrorKind::TableNotFound
    );
    let creation = || {
        TableCreation::builder()
            .name("test1".to_owned())
            .schema(table_schema())
            .build()
    };
    assert_eq!(
        catalog
            .create_table(&namespace, creation())
            .unwrap_err()
            .kind(),
        ErrorKind::NamespaceNotFound
    );
    assert_eq!(
        catalog
            .create_table(&namespace, creation())
            .unwrap_err()
            .kind(),
        ErrorKind::TableAlreadyExists
    );
    fixture.assert_finished();
}

#[test]
fn register_table_sends_the_metadata_location_and_builds_storage() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    let metadata_location = "s3://warehouse/database/table/metadata/00001.json";
    let mut response: serde_json::Value =
        serde_json::from_str(LOAD_TABLE_RESPONSE).unwrap();
    response["metadata-location"] = serde_json::json!(metadata_location);
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces/ns1/register"))
            .json_body(serde_json::json!({
                "name": "test1",
                "metadata-location": metadata_location,
                "overwrite": false
            }))
            .respond(StatusCode::OK, serde_json::to_vec(&response).unwrap()),
    );
    let catalog = fixture.bound_catalog([]);

    let table = catalog
        .register_table(&table_ident("test1"), metadata_location.to_owned())
        .unwrap();

    assert_eq!(table.metadata_location(), Some(metadata_location));
    assert_eq!(fixture.storage_configs()[0].location(), metadata_location);
    fixture.assert_finished();
}

#[test]
fn register_table_maps_namespace_and_table_conflicts() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces/ns1/register"))
            .respond(StatusCode::NOT_FOUND, ""),
    );
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces/ns1/register"))
            .respond(StatusCode::CONFLICT, ""),
    );
    let catalog = fixture.bound_catalog([]);

    assert_eq!(
        catalog
            .register_table(&table_ident("test1"), "s3://metadata".to_owned())
            .unwrap_err()
            .kind(),
        ErrorKind::NamespaceNotFound
    );
    assert_eq!(
        catalog
            .register_table(&table_ident("test1"), "s3://metadata".to_owned())
            .unwrap_err()
            .kind(),
        ErrorKind::TableAlreadyExists
    );
    fixture.assert_finished();
}

#[test]
fn update_without_location_change_reuses_the_commit_file_io() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::post(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .json_body(serde_json::json!({
            "identifier": {"namespace": ["ns1"], "name": "test1"},
            "requirements": [],
            "updates": [{"action": "upgrade-format-version", "format-version": 2}]
        }))
        .respond(StatusCode::OK, UPDATE_TABLE_RESPONSE),
    );
    let catalog = fixture.bound_catalog([]);
    let commit = TableCommit::builder()
        .ident(table_ident("test1"))
        .file_io(fixture.file_io())
        .requirements(Vec::new())
        .updates(vec![TableUpdate::UpgradeFormatVersion {
            format_version: FormatVersion::V2,
        }])
        .build();

    let table = catalog.update_table(commit).unwrap();

    assert_eq!(table.metadata().format_version(), FormatVersion::V2);
    assert!(fixture.storage_configs().is_empty());
    fixture.assert_finished();
}

#[test]
fn location_update_reloads_table_and_response_scoped_storage() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    let mut reloaded: serde_json::Value =
        serde_json::from_str(LOAD_TABLE_RESPONSE).unwrap();
    reloaded["metadata-location"] =
        serde_json::json!("s3://other/table/metadata/00002.json");
    reloaded["metadata"]["location"] = serde_json::json!("s3://other/table");
    fixture.expect(
        ExpectedExchange::post(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .respond(StatusCode::OK, UPDATE_TABLE_RESPONSE),
    );
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .respond(StatusCode::OK, serde_json::to_vec(&reloaded).unwrap()),
    );
    let catalog = fixture.bound_catalog([]);
    let commit = TableCommit::builder()
        .ident(table_ident("test1"))
        .file_io(fixture.file_io())
        .requirements(Vec::new())
        .updates(vec![TableUpdate::SetLocation {
            location: "s3://other/table".to_owned(),
        }])
        .build();

    let table = catalog.update_table(commit).unwrap();

    assert_eq!(table.metadata().location(), "s3://other/table");
    assert_eq!(
        fixture.storage_configs()[0].location(),
        "s3://other/table/metadata/00002.json"
    );
    fixture.assert_finished();
}

#[test]
fn update_statuses_preserve_commit_semantics() {
    let cases = [
        (StatusCode::NOT_FOUND, ErrorKind::TableNotFound, false),
        (
            StatusCode::CONFLICT,
            ErrorKind::CatalogCommitConflicts,
            true,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Unexpected,
            false,
        ),
        (StatusCode::BAD_GATEWAY, ErrorKind::Unexpected, false),
        (StatusCode::GATEWAY_TIMEOUT, ErrorKind::Unexpected, false),
    ];

    for (status, expected_kind, retryable) in cases {
        let fixture = RestTestFixture::default();
        fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
        fixture.expect(
            ExpectedExchange::post(format!(
                "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
            ))
            .respond(status, ""),
        );
        let catalog = fixture.bound_catalog([]);
        let commit = TableCommit::builder()
            .ident(table_ident("test1"))
            .file_io(fixture.file_io())
            .requirements(Vec::new())
            .updates(Vec::new())
            .build();

        let error = catalog.update_table(commit).unwrap_err();
        assert_eq!(error.kind(), expected_kind);
        assert_eq!(error.retryable(), retryable);
        fixture.assert_finished();
    }
}

#[test]
fn prepared_transaction_preserves_each_table_requirements_and_updates() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/transactions/commit"))
            .json_body(serde_json::json!({
                "table-changes": [
                    {
                        "identifier": {"namespace": ["ns1"], "name": "test1"},
                        "requirements": [],
                        "updates": [
                            {"action": "upgrade-format-version", "format-version": 2}
                        ]
                    },
                    {
                        "identifier": {"namespace": ["ns1"], "name": "test2"},
                        "requirements": [],
                        "updates": []
                    }
                ]
            }))
            .respond(StatusCode::NO_CONTENT, ""),
    );
    let catalog = fixture.bound_catalog([]);
    let prepared = [
        (
            table_ident("test1"),
            vec![TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2,
            }],
        ),
        (table_ident("test2"), Vec::new()),
    ]
    .into_iter()
    .map(|(identifier, updates)| {
        let table = transaction_table(&fixture, identifier.clone());
        let commit = TableCommit::builder()
            .ident(identifier)
            .file_io(fixture.file_io())
            .requirements(Vec::new())
            .updates(updates)
            .build();
        PreparedTableCommit::new(table, commit)
    })
    .collect();

    let request = catalog.prepare_transaction_commit(prepared).unwrap();
    assert_eq!(request.table_count(), 2);
    catalog.send_prepared_commit(request).unwrap();
    fixture.assert_finished();
}

#[test]
fn transaction_endpoint_must_be_advertised_when_config_is_explicit() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {},
        "overrides": {},
        "endpoints": ["GET /v1/{prefix}/namespaces"]
    }));
    let catalog = fixture.bound_catalog([]);
    let table = transaction_table(&fixture, table_ident("test1"));
    let commit = TableCommit::builder()
        .ident(table_ident("test1"))
        .file_io(fixture.file_io())
        .requirements(Vec::new())
        .updates(Vec::new())
        .build();

    let error = catalog
        .prepare_transaction_commit(vec![PreparedTableCommit::new(table, commit)])
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    fixture.assert_finished();
}

fn transaction_table(fixture: &RestTestFixture, identifier: TableIdent) -> Table {
    let loaded: LoadTableResult = serde_json::from_str(LOAD_TABLE_RESPONSE).unwrap();
    Table::builder()
        .file_io(fixture.file_io())
        .metadata_location(loaded.metadata_location.unwrap())
        .metadata(loaded.metadata)
        .identifier(identifier)
        .build()
        .unwrap()
}
