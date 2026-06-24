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

//! Utility functions for catalog operations.

use std::collections::HashSet;

use crate::Result;
use crate::io::FileIO;
use crate::spec::Manifest;
use crate::table::Table;

/// Deletes all data and metadata files referenced by the table metadata.
///
/// Data files within manifests are only deleted when `gc.enabled` is true
/// (the default), so tables that intentionally share data files are not
/// corrupted by catalog cleanup.
pub(crate) fn drop_table_data(table_info: &Table) -> Result<()> {
    let mut manifest_lists_to_delete: HashSet<String> = HashSet::new();
    let mut manifests_to_delete: HashSet<String> = HashSet::new();

    let metadata = table_info.metadata_ref();
    let file_io = table_info.file_io();

    for snapshot in metadata.snapshots() {
        let manifest_list = snapshot.load_manifest_list(file_io, &metadata)?;
        let manifest_list_location = snapshot.manifest_list();
        if !manifest_list_location.is_empty() {
            manifest_lists_to_delete.insert(manifest_list_location.to_string());
        }
        for manifest_file in manifest_list.entries() {
            manifests_to_delete.insert(manifest_file.manifest_path.clone());
        }
    }

    if metadata.table_properties()?.gc_enabled {
        delete_data_files(file_io, &manifests_to_delete)?;
    }

    delete_paths(file_io, manifests_to_delete)?;
    delete_paths(file_io, manifest_lists_to_delete)?;

    delete_paths(
        file_io,
        metadata
            .metadata_log()
            .iter()
            .map(|entry| entry.metadata_file.clone())
            .collect(),
    )?;

    delete_paths(
        file_io,
        metadata
            .statistics_iter()
            .map(|entry| entry.statistics_path.clone())
            .collect(),
    )?;

    delete_paths(
        file_io,
        metadata
            .partition_statistics_iter()
            .map(|entry| entry.statistics_path.clone())
            .collect(),
    )?;

    if let Some(location) = table_info.metadata_location() {
        file_io.delete(location)?;
    }

    Ok(())
}

fn delete_data_files(
    file_io: &FileIO,
    manifest_paths: &HashSet<String>,
) -> Result<()> {
    for manifest_path in manifest_paths {
        let manifest_content = file_io.new_input(manifest_path)?.read()?;
        let manifest = Manifest::parse_avro(&manifest_content)?;
        let data_file_paths = manifest
            .entries()
            .iter()
            .map(|entry| entry.data_file.file_path().to_string())
            .collect::<HashSet<_>>();
        delete_paths(file_io, data_file_paths)?;
    }

    Ok(())
}

fn delete_paths(file_io: &FileIO, paths: HashSet<String>) -> Result<()> {
    for path in paths {
        file_io.delete(path)?;
    }
    Ok(())
}
