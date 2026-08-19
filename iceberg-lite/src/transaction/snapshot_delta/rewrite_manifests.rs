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

use super::*;

impl<'a> DeltaSnapshotProducer<'a> {
    pub(in crate::transaction) fn commit_manifest_rewrite(
        &mut self,
        min_count_to_merge: usize,
        target_size_bytes: u64,
    ) -> Result<ActionCommit> {
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        };
        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())?;
        let rewrite_plan =
            crate::transaction::manifest_rewrite::ManifestRewritePlan::build(
                manifest_list.entries(),
                min_count_to_merge,
                target_size_bytes,
            )?;
        if rewrite_plan.is_empty() {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }
        let (by_group, selected) = rewrite_plan.into_parts();
        let mut by_group: Vec<_> = by_group.into_iter().collect();
        by_group.sort_unstable_by(
            |((left_spec, left_content), _), ((right_spec, right_content), _)| {
                left_spec.cmp(right_spec).then_with(|| {
                    (*left_content as i32).cmp(&(*right_content as i32))
                })
            },
        );

        let mut output = Vec::new();
        for ((spec_id, content), manifests) in by_group {
            let selected_group: Vec<&ManifestFile> = manifests
                .iter()
                .filter(|manifest| selected.contains(manifest.manifest_path.as_str()))
                .collect();
            if selected_group.is_empty() {
                output.extend(manifests);
                continue;
            }

            let total_bytes =
                selected_group.iter().try_fold(0_u64, |total, manifest| {
                    total
                        .checked_add(
                            u64::try_from(manifest.manifest_length).map_err(
                                |_| {
                                    Error::new(
                                        ErrorKind::DataInvalid,
                                        "negative manifest length",
                                    )
                                },
                            )?,
                        )
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "manifest byte count overflow",
                            )
                        })
                })?;
            let output_count =
                usize::try_from(total_bytes.div_ceil(target_size_bytes).max(1))
                    .map_err(|_| {
                        Error::new(ErrorKind::DataInvalid, "manifest count overflow")
                    })?;
            let total_live_entries = selected_group.iter().try_fold(
                Some(0_usize),
                |total, manifest| {
                    let Some(total) = total else {
                        return Ok::<Option<usize>, Error>(None);
                    };
                    let (Some(added), Some(existing)) =
                        (manifest.added_files_count, manifest.existing_files_count)
                    else {
                        return Ok(None);
                    };
                    let live = usize::try_from(added)
                        .ok()
                        .and_then(|added| {
                            usize::try_from(existing)
                                .ok()
                                .and_then(|existing| added.checked_add(existing))
                        })
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "manifest live-entry count overflow",
                            )
                        })?;
                    total.checked_add(live).map(Some).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "manifest group live-entry count overflow",
                        )
                    })
                },
            )?;
            let entries_per_output =
                total_live_entries.map(|count| count.div_ceil(output_count).max(1));
            let source_manifests_per_output =
                selected_group.len().div_ceil(output_count).max(1);
            let mut writer: Option<ManifestWriter> = None;
            let mut writer_entries = 0_usize;
            let mut writer_source_manifests = 0_usize;
            let mut written_outputs = 0_usize;
            for manifest_file in selected_group {
                let manifest = manifest_file.load_manifest(self.table.file_io())?;
                let mut row_ids =
                    FirstRowIdInheritance::new(manifest_file.first_row_id);
                for entry in manifest.entries() {
                    let effective_first_row_id = row_ids.resolve(entry)?;
                    if !entry.is_alive() {
                        continue;
                    }
                    let mut file = entry.data_file().clone();
                    if self.table.metadata().format_version() == FormatVersion::V3
                        && file.content_type() == DataContentType::Data
                    {
                        file.first_row_id = effective_first_row_id
                            .map(|value| {
                                i64::try_from(value).map_err(|_| {
                                    Error::new(
                                        ErrorKind::DataInvalid,
                                        "first row id does not fit Iceberg long",
                                    )
                                })
                            })
                            .transpose()?;
                    }
                    let snapshot_id = entry.snapshot_id().ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "live manifest entry has no snapshot id",
                        )
                    })?;
                    let sequence_number =
                        entry.sequence_number().ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "live manifest entry has no sequence number",
                            )
                        })?;
                    let file_sequence_number =
                        entry.file_sequence_number.ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "live manifest entry has no file sequence number",
                            )
                        })?;
                    if writer.is_none() {
                        writer = Some(self.new_manifest_writer(content, spec_id)?);
                    }
                    writer
                        .as_mut()
                        .expect("manifest writer was initialized")
                        .add_existing_file(
                            file,
                            snapshot_id,
                            sequence_number,
                            Some(file_sequence_number),
                        )?;
                    writer_entries =
                        writer_entries.checked_add(1).ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "manifest output entry count overflow",
                            )
                        })?;
                    if entries_per_output.is_some_and(|target| {
                        writer_entries >= target
                            && written_outputs
                                .checked_add(1)
                                .is_some_and(|written| written < output_count)
                    }) {
                        output.push(
                            writer
                                .take()
                                .expect("manifest writer was initialized")
                                .write_manifest_file()?,
                        );
                        written_outputs = written_outputs.checked_add(1).expect(
                            "written output count is bounded by output_count",
                        );
                        writer_entries = 0;
                        writer_source_manifests = 0;
                    }
                }
                writer_source_manifests =
                    writer_source_manifests.checked_add(1).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "source manifest count overflow",
                        )
                    })?;
                if entries_per_output.is_none()
                    && writer.is_some()
                    && writer_source_manifests >= source_manifests_per_output
                    && written_outputs
                        .checked_add(1)
                        .is_some_and(|written| written < output_count)
                {
                    output.push(
                        writer
                            .take()
                            .expect("manifest writer was initialized")
                            .write_manifest_file()?,
                    );
                    written_outputs = written_outputs
                        .checked_add(1)
                        .expect("written output count is bounded by output_count");
                    writer_entries = 0;
                    writer_source_manifests = 0;
                }
            }
            if let Some(writer) = writer {
                output.push(writer.write_manifest_file()?);
            }
        }

        let summary = update_snapshot_summaries(
            Summary {
                operation: Operation::Replace,
                additional_properties: HashMap::new(),
            },
            Some(current_snapshot.summary()),
            false,
        )?;
        let manifest_list = self.write_manifest_list(output)?;
        let snapshot = self.new_snapshot(&manifest_list, summary)?;
        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_owned(),
                    reference: SnapshotReference::new(
                        self.snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ],
            vec![
                TableRequirement::UuidMatch {
                    uuid: self.table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_owned(),
                    snapshot_id: self.table.metadata().current_snapshot_id(),
                },
            ],
        ))
    }
}
