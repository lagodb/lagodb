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

use std::collections::{BTreeSet, HashSet};

use crate::overlay::{
    ResolvedSnapshotDelta, SnapshotDelta, SnapshotDeltaRemovals,
};
use crate::spec::{DataFile, Operation};

#[derive(Default)]
pub(in crate::transaction) struct DeltaPlan {
    pub(in crate::transaction) added_data_files: Vec<DataFile>,
    pub(in crate::transaction) position_delete_files: Vec<DataFile>,
    pub(in crate::transaction) removals: SnapshotDeltaRemovals,
    pub(in crate::transaction) added_file_paths: HashSet<String>,
    pub(in crate::transaction) referenced_data_files: BTreeSet<String>,
}

impl DeltaPlan {
    pub(in crate::transaction) fn from_delta_with_truncate(
        delta: &SnapshotDelta,
        truncate_base: bool,
    ) -> Self {
        let mut plan = Self::from_resolved(delta.resolve());
        if truncate_base {
            plan.removals.set_truncates_base();
        }
        plan
    }

    fn from_resolved(resolved: ResolvedSnapshotDelta<'_>) -> Self {
        let removals = resolved.removals();
        let added_data_files = resolved
            .added_data_files
            .into_iter()
            .map(|data_file| data_file.file.clone())
            .collect();

        let mut position_delete_files =
            Vec::with_capacity(resolved.position_delete_files.len());
        let mut referenced_data_file_set = BTreeSet::new();
        for pending in resolved.position_delete_files {
            let referenced_data_files = pending.referenced_data_files.as_slice();
            debug_assert!(
                !referenced_data_files.is_empty(),
                "resolved position delete should reference at least one data file"
            );
            let Some((path, remaining_paths)) = referenced_data_files.split_first()
            else {
                continue;
            };
            referenced_data_file_set.extend(
                referenced_data_files
                    .iter()
                    .map(|referenced| (*referenced).to_owned()),
            );

            let mut file = (*pending.file).clone();
            if remaining_paths.is_empty() {
                file.referenced_data_file = Some((*path).to_owned());
            } else {
                file.referenced_data_file = None;
            }
            position_delete_files.push(file);
        }

        Self {
            added_data_files,
            position_delete_files,
            removals,
            added_file_paths: resolved
                .added_file_paths
                .into_iter()
                .map(str::to_owned)
                .collect(),
            referenced_data_files: referenced_data_file_set,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.added_data_files.is_empty()
            && self.position_delete_files.is_empty()
            && self.removals.is_empty()
    }

    pub(super) fn operation(&self) -> Operation {
        let has_adds = !self.added_data_files.is_empty();
        let has_deletes =
            !self.position_delete_files.is_empty() || !self.removals.is_empty();

        match (has_adds, has_deletes) {
            (true, false) => Operation::Append,
            (false, true) => Operation::Delete,
            (true, true) => Operation::Overwrite,
            (false, false) => Operation::Append,
        }
    }
}
