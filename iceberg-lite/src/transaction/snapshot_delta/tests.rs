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

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::io::{FileIO, FileMetadata, FileWrite, OpenedFile, Storage};
use crate::memory::tests::new_memory_catalog;
use crate::overlay::SnapshotDelta;
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion,
    Literal, ManifestStatus, Struct, TableMetadata,
};
use crate::table::Table;
use crate::transaction::tests::{
    make_v2_minimal_table, make_v3_minimal_table_in_catalog,
};
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Catalog, TableCreation, TableIdent};

use super::*;

#[derive(Debug)]
struct CountingStorage {
    inner: Arc<dyn Storage>,
    opens: Arc<Mutex<HashMap<String, usize>>>,
}

impl Storage for CountingStorage {
    fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path)
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        self.inner.status(path)
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let mut opens = self.opens.lock().expect("counting lock poisoned");
        *opens.entry(path.to_owned()).or_default() += 1;
        drop(opens);
        self.inner.open_reader(path)
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.inner.writer(path)
    }

    fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
        Ok(())
    }

    fn scheme(&self) -> &str {
        self.inner.scheme()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn finalize_write(&self, path: &str) -> Result<()> {
        self.inner.finalize_write(path)
    }

    fn resolve_uri(&self, uri: &str) -> Result<usize> {
        self.inner.resolve_uri(uri)
    }
}

fn with_counting_storage(
    table: &Table,
) -> (Table, Arc<Mutex<HashMap<String, usize>>>) {
    let opens = Arc::new(Mutex::new(HashMap::new()));
    let storage = Arc::new(CountingStorage {
        inner: Arc::clone(table.file_io().storage()),
        opens: Arc::clone(&opens),
    });
    let mut builder = Table::builder()
        .file_io(FileIO::new(storage))
        .metadata(table.metadata_ref())
        .identifier(table.identifier().clone())
        .disable_cache();
    if let Some(location) = table.metadata_location() {
        builder = builder.metadata_location(location);
    }
    (builder.build().unwrap(), opens)
}

fn open_count(opens: &Arc<Mutex<HashMap<String, usize>>>, path: &str) -> usize {
    opens
        .lock()
        .expect("counting lock poisoned")
        .get(path)
        .copied()
        .unwrap_or_default()
}

#[test]
fn snapshot_delta_add_data_commits_data_manifest() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);
    let mut delta = SnapshotDelta::new();
    delta
        .add_data_file(data_file("test/data-a.parquet"))
        .unwrap();

    let updated = commit_delta(&catalog, &table, delta);

    let tasks = updated.scan().build().unwrap().plan_files().unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].data_file_path(), "test/data-a.parquet");

    let manifest_list = current_manifest_list(&updated);
    assert_eq!(manifest_list.entries().len(), 1);
    assert_eq!(
        manifest_list.entries()[0].content,
        ManifestContentType::Data
    );
}

#[test]
fn snapshot_delta_add_then_remove_is_noop() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);
    let mut delta = SnapshotDelta::new();
    delta
        .add_data_file(data_file("test/transient.parquet"))
        .unwrap();
    delta.remove_data_file("test/transient.parquet").unwrap();

    let tx = Transaction::new(&table);
    let tx = tx.snapshot_delta(Arc::new(delta)).apply(tx).unwrap();
    let updated = tx.commit(&catalog).unwrap();

    assert_eq!(
        updated.metadata().current_snapshot_id(),
        table.metadata().current_snapshot_id()
    );
    assert_eq!(updated.metadata_location(), table.metadata_location());
    assert!(
        updated
            .scan()
            .build()
            .unwrap()
            .plan_files()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn snapshot_delta_position_delete_with_add_writes_delete_manifest() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);
    let mut delta = SnapshotDelta::new();
    delta
        .add_data_file(data_file("test/data-a.parquet"))
        .unwrap();
    delta
        .add_position_delete_file(
            position_delete_file("test/pos-a.parquet"),
            ["test/data-a.parquet"],
        )
        .unwrap();

    let updated = commit_delta(&catalog, &table, delta);

    let tasks = updated.scan().build().unwrap().plan_files().unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].deletes.len(), 1);
    assert_eq!(tasks[0].deletes[0].file_path, "test/pos-a.parquet");
    assert_eq!(
        tasks[0].deletes[0].file_type,
        DataContentType::PositionDeletes
    );

    let manifest_list = current_manifest_list(&updated);
    let data_manifests = manifest_list
        .entries()
        .iter()
        .filter(|entry| entry.content == ManifestContentType::Data)
        .count();
    let delete_manifests = manifest_list
        .entries()
        .iter()
        .filter(|entry| entry.content == ManifestContentType::Deletes)
        .count();
    assert_eq!(data_manifests, 1);
    assert_eq!(delete_manifests, 1);
}

#[test]
fn snapshot_delta_append_only_reuses_manifest_list_without_loading_old_manifests() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files([data_file("test/base.parquet")])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).unwrap();
    let base_manifest_path = current_manifest_list(&table).entries()[0]
        .manifest_path
        .clone();

    table
        .file_io()
        .new_output(&base_manifest_path)
        .unwrap()
        .write(b"not an avro manifest")
        .unwrap();

    let mut delta = SnapshotDelta::new();
    delta
        .add_data_file(data_file("test/append.parquet"))
        .unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .snapshot_delta(Arc::new(delta))
        // Duplicate checks legitimately read existing manifests; this test
        // isolates the no-remove materialization path.
        .with_check_duplicate(false)
        .apply(tx)
        .unwrap();
    let updated = tx.commit(&catalog).unwrap();

    let manifest_list = current_manifest_list(&updated);
    assert_eq!(manifest_list.entries().len(), 2);
    assert!(
        manifest_list
            .entries()
            .iter()
            .any(|entry| entry.manifest_path == base_manifest_path)
    );
}

#[test]
fn snapshot_delta_remove_existing_file_rewrites_manifest() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);
    let base_file = data_file("test/base.parquet");
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files([base_file.clone()])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).unwrap();

    let mut delta = SnapshotDelta::new();
    delta.remove_data_file(base_file.file_path()).unwrap();
    let updated = commit_delta(&catalog, &table, delta);

    assert!(
        updated
            .scan()
            .build()
            .unwrap()
            .plan_files()
            .unwrap()
            .is_empty()
    );

    let manifest_list = current_manifest_list(&updated);
    assert_eq!(manifest_list.entries().len(), 1);
    assert!(manifest_list.entries()[0].has_deleted_files());

    let manifest = manifest_list.entries()[0]
        .load_manifest(updated.file_io())
        .unwrap();
    assert_eq!(manifest.entries().len(), 1);
    assert_eq!(manifest.entries()[0].status(), ManifestStatus::Deleted);
    assert_eq!(manifest.entries()[0].file_path(), "test/base.parquet");
}

#[test]
fn snapshot_delta_truncate_rewrites_data_and_delete_manifests() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);
    let mut append = SnapshotDelta::new();
    append
        .add_data_file(data_file("test/base.parquet"))
        .unwrap();
    append
        .add_position_delete_file(
            position_delete_file("test/base-delete.parquet"),
            ["test/base.parquet"],
        )
        .unwrap();
    let table = commit_delta(&catalog, &table, append);
    let parent_snapshot_id = table.metadata().current_snapshot_id();

    let tx = Transaction::new(&table);
    let tx = tx
        .snapshot_delta(Arc::new(SnapshotDelta::new()))
        .truncate_base()
        .apply(tx)
        .unwrap();
    let updated = tx.commit(&catalog).unwrap();

    assert_ne!(updated.metadata().current_snapshot_id(), parent_snapshot_id);
    assert!(
        updated
            .scan()
            .build()
            .unwrap()
            .plan_files()
            .unwrap()
            .is_empty()
    );
    let snapshot = updated.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.parent_snapshot_id(), parent_snapshot_id);
    assert_eq!(snapshot.summary().operation, Operation::Delete);
    assert_eq!(
        snapshot.summary().additional_properties["total-data-files"],
        "0"
    );
    assert_eq!(
        snapshot.summary().additional_properties["total-delete-files"],
        "0"
    );

    let manifest_list = current_manifest_list(&updated);
    assert_eq!(manifest_list.entries().len(), 2);
    for manifest_file in manifest_list.entries() {
        assert!(manifest_file.has_deleted_files());
        let manifest = manifest_file.load_manifest(updated.file_io()).unwrap();
        assert!(
            manifest
                .entries()
                .iter()
                .all(|entry| entry.status() == ManifestStatus::Deleted)
        );
    }
}

#[test]
fn snapshot_delta_truncate_with_add_is_overwrite() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files([data_file("test/base.parquet")])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).unwrap();
    let mut replacement = SnapshotDelta::new();
    replacement
        .add_data_file(data_file("test/replacement.parquet"))
        .unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .snapshot_delta(Arc::new(replacement))
        .truncate_base()
        .apply(tx)
        .unwrap();
    let updated = tx.commit(&catalog).unwrap();

    let tasks = updated.scan().build().unwrap().plan_files().unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].data_file_path(), "test/replacement.parquet");
    let summary = updated.metadata().current_snapshot().unwrap().summary();
    assert_eq!(summary.operation, Operation::Overwrite);
    assert_eq!(summary.additional_properties["total-data-files"], "1");
    assert_eq!(summary.additional_properties["total-records"], "1");
}

#[test]
fn snapshot_delta_truncate_empty_table_is_noop() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);

    let tx = Transaction::new(&table);
    let tx = tx
        .snapshot_delta(Arc::new(SnapshotDelta::new()))
        .truncate_base()
        .apply(tx)
        .unwrap();
    let updated = tx.commit(&catalog).unwrap();

    assert_eq!(
        updated.metadata().current_snapshot_id(),
        table.metadata().current_snapshot_id()
    );
    assert_eq!(updated.metadata_location(), table.metadata_location());
}

#[test]
fn snapshot_delta_v3_sets_row_range() {
    let catalog = new_memory_catalog();
    let table = make_v3_minimal_table_in_catalog(&catalog);
    let mut delta = SnapshotDelta::new();
    delta.add_data_file(data_file("test/v3.parquet")).unwrap();

    let updated = commit_delta(&catalog, &table, delta);
    let snapshot = updated.metadata().current_snapshot().unwrap();

    assert_eq!(snapshot.first_row_id(), Some(0));
    assert_eq!(snapshot.added_rows_count(), Some(1));
    assert_eq!(updated.metadata().next_row_id(), 1);
}

#[test]
fn snapshot_delta_v3_suppresses_preassigned_id_on_added_file() {
    let catalog = new_memory_catalog();
    let table = make_v3_minimal_table_in_catalog(&catalog);
    let mut file = data_file("test/preassigned.parquet");
    file.first_row_id = Some(99);
    let mut delta = SnapshotDelta::new();
    delta.add_data_file(file).unwrap();

    let updated = commit_delta(&catalog, &table, delta);
    let manifest_list = current_manifest_list(&updated);
    assert_eq!(manifest_list.entries()[0].first_row_id, Some(0));
    let manifest = manifest_list.entries()[0]
        .load_manifest(updated.file_io())
        .unwrap();
    let added = manifest
        .entries()
        .iter()
        .find(|entry| entry.is_alive())
        .unwrap();
    assert_eq!(added.data_file().first_row_id(), None);
    assert_eq!(
        updated.scan().build().unwrap().plan_files().unwrap()[0].first_row_id,
        Some(0)
    );
    assert_eq!(updated.metadata().next_row_id(), 1);
}

#[test]
fn snapshot_delta_v3_rewrite_preserves_inherited_file_row_id() {
    let catalog = new_memory_catalog();
    let table = make_v3_minimal_table_in_catalog(&catalog);
    let mut append = SnapshotDelta::new();
    append.add_data_file(data_file("test/a.parquet")).unwrap();
    append.add_data_file(data_file("test/b.parquet")).unwrap();
    let table = commit_delta(&catalog, &table, append);

    let mut remove = SnapshotDelta::new();
    remove.remove_data_file("test/a.parquet").unwrap();
    let updated = commit_delta(&catalog, &table, remove);

    let snapshot = updated.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.first_row_id(), Some(2));
    assert_eq!(snapshot.added_rows_count(), Some(1));
    assert_eq!(updated.metadata().next_row_id(), 3);

    let tasks = updated.scan().build().unwrap().plan_files().unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].data_file_path(), "test/b.parquet");
    assert_eq!(tasks[0].first_row_id, Some(1));

    let manifest_list = current_manifest_list(&updated);
    assert_eq!(manifest_list.entries()[0].first_row_id, Some(2));
    let manifest = manifest_list.entries()[0]
        .load_manifest(updated.file_io())
        .unwrap();
    let remaining = manifest
        .entries()
        .iter()
        .find(|entry| entry.is_alive())
        .unwrap();
    assert_eq!(remaining.data_file().first_row_id(), Some(1));
}

#[test]
fn rewrite_files_preserves_sequence_and_manifest_status() {
    let catalog = new_memory_catalog();
    let table = make_v2_table_in_catalog(&catalog);
    let input_a = data_file("test/input-a.parquet");
    let input_b = data_file("test/input-b.parquet");
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files([input_a.clone(), input_b.clone()])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).unwrap();
    let starting_snapshot = table.metadata().current_snapshot().unwrap();
    let starting_snapshot_id = starting_snapshot.snapshot_id();
    let starting_sequence_number = starting_snapshot.sequence_number();
    let source_manifest_paths: Vec<_> = current_manifest_list(&table)
        .entries()
        .iter()
        .map(|manifest| manifest.manifest_path.clone())
        .collect();
    let (table, opens) = with_counting_storage(&table);
    let replacement = data_file("test/replacement.parquet");

    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_files(starting_snapshot_id, starting_sequence_number)
        .rewrite_data_files([input_a], [replacement])
        .apply(tx)
        .unwrap();
    let updated = tx.commit(&catalog).unwrap();
    for path in source_manifest_paths {
        assert_eq!(
            open_count(&opens, &path),
            1,
            "RewriteFiles must validate and rewrite each source manifest with one read"
        );
    }

    let snapshot = updated.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.summary().operation, Operation::Replace);
    assert_eq!(snapshot.parent_snapshot_id(), Some(starting_snapshot_id));
    let manifest_list = current_manifest_list(&updated);
    let entries: Vec<_> = manifest_list
        .entries()
        .iter()
        .flat_map(|manifest_file| {
            manifest_file
                .load_manifest(updated.file_io())
                .unwrap()
                .entries()
                .to_vec()
        })
        .collect();
    let replacement = entries
        .iter()
        .find(|entry| entry.file_path() == "test/replacement.parquet")
        .unwrap();
    assert_eq!(replacement.status(), ManifestStatus::Added);
    assert_eq!(
        replacement.sequence_number(),
        Some(starting_sequence_number)
    );
    let carried = entries
        .iter()
        .find(|entry| entry.file_path() == "test/input-b.parquet")
        .unwrap();
    assert!(carried.is_alive());
    assert_eq!(carried.sequence_number(), Some(starting_sequence_number));
}

#[test]
fn rewrite_manifests_streams_entries_into_fewer_manifests() {
    let catalog = new_memory_catalog();
    let mut table = make_v2_table_in_catalog(&catalog);
    for index in 0..6 {
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files([data_file(&format!("test/manifest-{index}.parquet"))])
            .apply(tx)
            .unwrap();
        table = tx.commit(&catalog).unwrap();
    }
    assert_eq!(current_manifest_list(&table).entries().len(), 6);
    let source_manifest_paths: Vec<_> = current_manifest_list(&table)
        .entries()
        .iter()
        .map(|manifest| manifest.manifest_path.clone())
        .collect();
    let (table, opens) = with_counting_storage(&table);

    let tx = Transaction::new(&table);
    let tx = tx.rewrite_manifests(2, u64::MAX).apply(tx).unwrap();
    let updated = tx.commit(&catalog).unwrap();
    for path in source_manifest_paths {
        assert_eq!(
            open_count(&opens, &path),
            1,
            "RewriteManifests must stream each selected manifest once"
        );
    }

    let manifest_list = current_manifest_list(&updated);
    assert_eq!(manifest_list.entries().len(), 1);
    let manifest = manifest_list.entries()[0]
        .load_manifest(updated.file_io())
        .unwrap();
    let live_paths: Vec<_> = manifest
        .entries()
        .iter()
        .filter(|entry| entry.is_alive())
        .map(|entry| entry.file_path())
        .collect();
    assert_eq!(live_paths.len(), 6);
    for index in 0..6 {
        assert!(
            live_paths.contains(&format!("test/manifest-{index}.parquet").as_str())
        );
    }
}

#[test]
fn expire_snapshots_preserves_retained_refs_then_removes_aged_refs() {
    let catalog = new_memory_catalog();
    let mut table = make_v3_minimal_table_in_catalog(&catalog);
    for index in 0..3 {
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(data_file(&format!("test/ref-{index}.parquet")))
            .unwrap();
        table = commit_delta(&catalog, &table, delta);
    }
    let snapshots: Vec<_> = table.metadata().snapshots().cloned().collect();
    let oldest = snapshots
        .iter()
        .find(|snapshot| snapshot.parent_snapshot_id().is_none())
        .expect("first append snapshot has no parent");
    let newest_timestamp = snapshots
        .iter()
        .map(|snapshot| snapshot.timestamp_ms())
        .max()
        .unwrap();

    let retained_metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_ref(
            "retained-tag",
            SnapshotReference::new(
                oldest.snapshot_id(),
                SnapshotRetention::Tag {
                    max_ref_age_ms: Some(i64::MAX),
                },
            ),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let retained_table = table.clone().with_metadata(Arc::new(retained_metadata));
    let action = Transaction::new(&retained_table)
        .expire_snapshots()
        .with_as_of_ms(newest_timestamp.saturating_add(1))
        .expire_older_than_ms(i64::MAX);
    let mut commit = Arc::new(action).commit(&retained_table).unwrap();
    let retained_updates = commit.take_updates();
    assert!(!retained_updates.iter().any(|update| {
        matches!(
            update,
            TableUpdate::RemoveSnapshotRef { ref_name }
                if ref_name == "retained-tag"
        )
    }));
    assert!(!retained_updates.iter().any(|update| {
        matches!(
            update,
            TableUpdate::RemoveSnapshots { snapshot_ids }
                if snapshot_ids.contains(&oldest.snapshot_id())
        )
    }));

    let aged_metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_ref(
            "aged-tag",
            SnapshotReference::new(
                oldest.snapshot_id(),
                SnapshotRetention::Tag {
                    max_ref_age_ms: Some(0),
                },
            ),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let aged_table = table.clone().with_metadata(Arc::new(aged_metadata));
    let action = Transaction::new(&aged_table)
        .expire_snapshots()
        .with_as_of_ms(newest_timestamp.saturating_add(1))
        .expire_older_than_ms(i64::MAX);
    let mut commit = Arc::new(action).commit(&aged_table).unwrap();
    let aged_updates = commit.take_updates();
    assert!(aged_updates.iter().any(|update| {
        matches!(
            update,
            TableUpdate::RemoveSnapshotRef { ref_name }
                if ref_name == "aged-tag"
        )
    }));
    assert!(aged_updates.iter().any(|update| {
        matches!(
            update,
            TableUpdate::RemoveSnapshots { snapshot_ids }
                if snapshot_ids.contains(&oldest.snapshot_id())
        )
    }));
}

fn commit_delta(
    catalog: &impl Catalog,
    table: &Table,
    delta: SnapshotDelta,
) -> Table {
    let tx = Transaction::new(table);
    let tx = tx.snapshot_delta(Arc::new(delta)).apply(tx).unwrap();
    tx.commit(catalog).unwrap()
}

fn current_manifest_list(table: &Table) -> crate::spec::ManifestList {
    table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .unwrap()
}

fn make_v2_table_in_catalog(catalog: &impl Catalog) -> Table {
    let table_ident =
        TableIdent::from_strs([format!("ns-{}", Uuid::new_v4()), "test".to_owned()])
            .unwrap();
    catalog
        .create_namespace(table_ident.namespace(), HashMap::new())
        .unwrap();

    let base_table = make_v2_minimal_table();
    let base_metadata: &TableMetadata = base_table.metadata();
    let table_creation = TableCreation::builder()
        .schema((**base_metadata.current_schema()).clone())
        .partition_spec((**base_metadata.default_partition_spec()).clone())
        .sort_order((**base_metadata.default_sort_order()).clone())
        .name(table_ident.name().to_owned())
        .format_version(FormatVersion::V2)
        .build();

    catalog
        .create_table(table_ident.namespace(), table_creation)
        .unwrap()
}

fn data_file(path: &str) -> DataFile {
    file_builder(DataContentType::Data, path).build().unwrap()
}

fn position_delete_file(path: &str) -> DataFile {
    file_builder(DataContentType::PositionDeletes, path)
        .build()
        .unwrap()
}

fn file_builder(content: DataContentType, path: &str) -> DataFileBuilder {
    let mut builder = DataFileBuilder::default();
    builder
        .content(content)
        .file_path(path.to_owned())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::from_iter([Some(Literal::long(300))]));
    builder
}
