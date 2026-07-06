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

use std::sync::Arc;

use crate::delete_file_index::DeleteFileIndex;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::object_cache::ObjectCache;
use crate::scan::{
    BoundPredicates, ExpressionEvaluatorCache, FileScanTask, ManifestEvaluatorCache,
    PartitionFilterCache,
};
use crate::spec::{
    FirstRowIdInheritance, ManifestContentType, ManifestEntryRef, ManifestFile,
    ManifestList, NameMapping, PartitionSpecRef, SchemaRef, SnapshotRef,
    TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

/// Wraps a [`ManifestFile`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestFileContext {
    pub manifest_file: ManifestFile,

    pub field_ids: Arc<Vec<i32>>,
    pub bound_predicates: Option<Arc<BoundPredicates>>,
    pub snapshot_bound_predicate: Option<Arc<BoundPredicate>>,
    pub object_cache: Arc<ObjectCache>,
    pub snapshot_schema: SchemaRef,
    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    pub delete_file_index: Option<DeleteFileIndex>,
    pub name_mapping: Option<Arc<NameMapping>>,
    pub case_sensitive: bool,
    pub partition_spec: PartitionSpecRef,
}

/// Wraps a [`ManifestEntryRef`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestEntryContext {
    pub manifest_entry: ManifestEntryRef,

    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    pub field_ids: Arc<Vec<i32>>,
    pub bound_predicates: Option<Arc<BoundPredicates>>,
    pub snapshot_bound_predicate: Option<Arc<BoundPredicate>>,
    pub partition_spec_id: i32,
    pub partition_spec: PartitionSpecRef,
    pub snapshot_schema: SchemaRef,
    pub delete_file_index: Option<DeleteFileIndex>,
    pub name_mapping: Option<Arc<NameMapping>>,
    pub first_row_id: Option<u64>,
    pub case_sensitive: bool,
}

impl ManifestFileContext {
    /// Fetches the Manifest from FileIO and returns the list of ManifestEntryContexts.
    /// This is a synchronous operation.
    pub(crate) fn fetch_manifest_entries(&self) -> Result<Vec<ManifestEntryContext>> {
        let manifest = self.object_cache.get_manifest(&self.manifest_file)?;

        let mut entries = Vec::with_capacity(manifest.entries().len());
        let mut row_id_inheritance =
            FirstRowIdInheritance::new(self.manifest_file.first_row_id);
        for manifest_entry in manifest.entries() {
            let entry_first_row_id = row_id_inheritance.resolve(manifest_entry)?;

            entries.push(ManifestEntryContext {
                manifest_entry: manifest_entry.clone(),
                expression_evaluator_cache: self.expression_evaluator_cache.clone(),
                field_ids: self.field_ids.clone(),
                partition_spec_id: self.manifest_file.partition_spec_id,
                partition_spec: self.partition_spec.clone(),
                bound_predicates: self.bound_predicates.clone(),
                snapshot_bound_predicate: self.snapshot_bound_predicate.clone(),
                snapshot_schema: self.snapshot_schema.clone(),
                delete_file_index: self.delete_file_index.clone(),
                name_mapping: self.name_mapping.clone(),
                first_row_id: entry_first_row_id,
                case_sensitive: self.case_sensitive,
            });
        }

        Ok(entries)
    }
}

impl ManifestEntryContext {
    /// Consume this `ManifestEntryContext`, returning a `FileScanTask`
    /// created from it. This is a synchronous operation.
    pub(crate) fn into_file_scan_task(self) -> Result<FileScanTask> {
        let index = self.delete_file_index.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Delete file index not initialized for data manifest entry",
            )
        })?;

        let deletes = index.get_deletes_for_data_file(
            self.manifest_entry.data_file(),
            self.manifest_entry.sequence_number(),
        );

        Ok(FileScanTask {
            file_size_in_bytes: self.manifest_entry.file_size_in_bytes(),
            start: 0,
            // Manifest-planned tasks read the full data file. Keep the local
            // full-file sentinel so byte-range filtering is only applied to
            // explicit split tasks.
            length: 0,
            record_count: Some(self.manifest_entry.record_count()),
            first_row_id: self.first_row_id,

            data_file_path: self.manifest_entry.file_path().to_string(),
            data_file_format: self.manifest_entry.file_format(),
            partition_spec_id: self.partition_spec_id,

            schema: self.snapshot_schema,
            project_field_ids: self.field_ids.to_vec(),
            predicate: self
                .snapshot_bound_predicate
                .as_ref()
                .map(|predicate| predicate.as_ref().clone()),

            deletes,

            // Include partition data and spec from manifest entry
            partition: Some(self.manifest_entry.data_file.partition.clone()),
            partition_spec: Some(self.partition_spec),
            name_mapping: self.name_mapping,
            case_sensitive: self.case_sensitive,
        })
    }
}

/// PlanContext wraps an optional [`SnapshotRef`] alongside all the other
/// objects that are required to perform a scan file plan.
#[derive(Debug)]
pub(crate) struct PlanContext {
    pub snapshot: Option<SnapshotRef>,

    pub table_metadata: TableMetadataRef,
    pub snapshot_schema: SchemaRef,
    pub case_sensitive: bool,
    pub predicate: Option<Arc<Predicate>>,
    pub snapshot_bound_predicate: Option<Arc<BoundPredicate>>,
    pub object_cache: Arc<ObjectCache>,
    pub field_ids: Arc<Vec<i32>>,
    pub name_mapping: Option<Arc<NameMapping>>,

    pub partition_filter_cache: Arc<PartitionFilterCache>,
    pub manifest_evaluator_cache: Arc<ManifestEvaluatorCache>,
    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
}

impl PlanContext {
    /// Get the manifest list for this snapshot. This is a synchronous operation.
    pub(crate) fn get_manifest_list(&self) -> Result<Option<Arc<ManifestList>>> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Ok(None);
        };

        self.object_cache
            .as_ref()
            .get_manifest_list(snapshot, &self.table_metadata)
            .map(Some)
    }

    pub(crate) fn base_sequence_number(&self) -> i64 {
        self.snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.sequence_number())
    }

    pub(crate) fn create_manifest_entry_context(
        &self,
        manifest_entry: ManifestEntryRef,
        partition_spec_id: i32,
        bound_predicates: Option<Arc<BoundPredicates>>,
        delete_file_index: Option<DeleteFileIndex>,
    ) -> Result<ManifestEntryContext> {
        let partition_spec = self.partition_spec(partition_spec_id)?;
        Ok(ManifestEntryContext {
            manifest_entry,
            expression_evaluator_cache: self.expression_evaluator_cache.clone(),
            field_ids: self.field_ids.clone(),
            partition_spec_id,
            partition_spec,
            bound_predicates,
            snapshot_bound_predicate: self.snapshot_bound_predicate.clone(),
            snapshot_schema: self.snapshot_schema.clone(),
            delete_file_index,
            name_mapping: self.name_mapping.clone(),
            first_row_id: None,
            case_sensitive: self.case_sensitive,
        })
    }

    pub(crate) fn create_delta_manifest_entry_context(
        &self,
        manifest_entry: ManifestEntryRef,
        partition_spec_id: i32,
        delete_file_index: Option<DeleteFileIndex>,
    ) -> Result<ManifestEntryContext> {
        let partition_filter = if self.predicate.is_some() {
            Some(self.get_partition_filter_for_spec(partition_spec_id)?)
        } else {
            None
        };
        let bound_predicates = self.bound_predicates(partition_filter);
        self.create_manifest_entry_context(
            manifest_entry,
            partition_spec_id,
            bound_predicates,
            delete_file_index,
        )
    }

    fn get_partition_filter(
        &self,
        manifest_file: &ManifestFile,
    ) -> Result<Arc<BoundPredicate>> {
        self.get_partition_filter_for_spec(manifest_file.partition_spec_id)
    }

    fn get_partition_filter_for_spec(
        &self,
        partition_spec_id: i32,
    ) -> Result<Arc<BoundPredicate>> {
        self.partition_filter_cache.get(
            partition_spec_id,
            &self.table_metadata,
            &self.snapshot_schema,
            self.case_sensitive,
            self.predicate
                .as_ref()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Expected a predicate but none present",
                    )
                })?
                .as_ref()
                .bind(self.snapshot_schema.clone(), self.case_sensitive)?,
        )
    }

    fn partition_spec(&self, partition_spec_id: i32) -> Result<PartitionSpecRef> {
        self.table_metadata
            .partition_spec_by_id(partition_spec_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("unknown partition spec id {partition_spec_id}"),
                )
            })
    }

    /// Build manifest file contexts, separating data and delete manifests.
    /// Returns (data_manifest_contexts, delete_manifest_contexts).
    pub(crate) fn build_manifest_file_contexts(
        &self,
        manifest_list: Arc<ManifestList>,
    ) -> Result<(Vec<ManifestFileContext>, Vec<ManifestFileContext>)> {
        let mut data_manifest_contexts = Vec::new();
        let mut delete_manifest_contexts = Vec::new();

        for manifest_file in manifest_list.entries().iter() {
            let partition_bound_predicate = if self.predicate.is_some() {
                let partition_bound_predicate =
                    self.get_partition_filter(manifest_file)?;

                // evaluate the ManifestFile against the partition filter. Skip
                // if it cannot contain any matching rows
                if !self
                    .manifest_evaluator_cache
                    .get(
                        manifest_file.partition_spec_id,
                        partition_bound_predicate.clone(),
                    )
                    .eval(manifest_file)?
                {
                    continue;
                }

                Some(partition_bound_predicate)
            } else {
                None
            };

            let mfc = self.create_manifest_file_context(
                manifest_file,
                partition_bound_predicate,
            )?;

            match manifest_file.content {
                ManifestContentType::Data => data_manifest_contexts.push(mfc),
                ManifestContentType::Deletes => delete_manifest_contexts.push(mfc),
            }
        }

        Ok((data_manifest_contexts, delete_manifest_contexts))
    }

    fn create_manifest_file_context(
        &self,
        manifest_file: &ManifestFile,
        partition_filter: Option<Arc<BoundPredicate>>,
    ) -> Result<ManifestFileContext> {
        let bound_predicates = self.bound_predicates(partition_filter);
        let partition_spec = self.partition_spec(manifest_file.partition_spec_id)?;

        Ok(ManifestFileContext {
            manifest_file: manifest_file.clone(),
            bound_predicates,
            snapshot_bound_predicate: self.snapshot_bound_predicate.clone(),
            object_cache: self.object_cache.clone(),
            snapshot_schema: self.snapshot_schema.clone(),
            field_ids: self.field_ids.clone(),
            expression_evaluator_cache: self.expression_evaluator_cache.clone(),
            delete_file_index: None,
            name_mapping: self.name_mapping.clone(),
            case_sensitive: self.case_sensitive,
            partition_spec,
        })
    }

    fn bound_predicates(
        &self,
        partition_filter: Option<Arc<BoundPredicate>>,
    ) -> Option<Arc<BoundPredicates>> {
        if let (Some(ref partition_bound_predicate), Some(snapshot_bound_predicate)) =
            (partition_filter, &self.snapshot_bound_predicate)
        {
            Some(Arc::new(BoundPredicates {
                partition_bound_predicate: partition_bound_predicate.as_ref().clone(),
                snapshot_bound_predicate: snapshot_bound_predicate.as_ref().clone(),
            }))
        } else {
            None
        }
    }
}
