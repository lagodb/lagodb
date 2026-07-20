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

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;

use crate::spec::{
    MAIN_BRANCH, SnapshotReference, SnapshotRetention, TableMetadata, TableProperties,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// A transaction action that removes snapshots from table metadata.
///
/// This only rewrites metadata; the now-unreferenced data and metadata files are left untouched.
/// Physical file cleanup is the responsibility of a higher-level maintenance operation built on
/// top of this action.
///
/// Explicit ids and age-based expiry are combined: a snapshot is expired if it
/// is named explicitly or selected by age. Branch retention protects snapshots
/// from age-based expiry, but does not protect snapshots named explicitly.
/// Heads of retained refs, including the current snapshot, cannot be expired.
pub struct ExpireSnapshotsAction {
    as_of_ms: i64,
    explicit_ids_to_remove: Vec<i64>,
    older_than_ms: Option<i64>,
    retain_last: Option<usize>,
}

impl ExpireSnapshotsAction {
    pub(crate) fn new() -> Self {
        Self {
            // Freeze the action clock when the logical operation is created.
            // Catalog retries may re-run `commit` against newer metadata, but
            // must not move the retention boundary forward while doing so.
            as_of_ms: Utc::now().timestamp_millis(),
            explicit_ids_to_remove: vec![],
            older_than_ms: None,
            retain_last: None,
        }
    }

    /// Evaluate all age-based retention rules at `as_of_ms`.
    ///
    /// This is primarily useful to share one command timestamp across several
    /// independently materialized transaction attempts.  The value is a Unix
    /// epoch timestamp in milliseconds.
    pub fn with_as_of_ms(mut self, as_of_ms: i64) -> Self {
        self.as_of_ms = as_of_ms;
        self
    }

    /// Expire these snapshot ids in addition to any age-based selection.
    ///
    /// An id that is still the head of a retained branch or tag cannot be
    /// expired. Other ids named explicitly are not protected by age-based
    /// branch retention.
    pub fn expire_snapshot_ids(
        mut self,
        snapshot_ids: impl IntoIterator<Item = i64>,
    ) -> Self {
        self.explicit_ids_to_remove.extend(snapshot_ids);
        self
    }

    /// Expire snapshots whose timestamp is strictly older than `older_than_ms`.
    pub fn expire_older_than_ms(mut self, older_than_ms: i64) -> Self {
        self.older_than_ms = Some(older_than_ms);
        self
    }

    /// Keep at least the `retain_last` most recent snapshots of each branch when expiring by age.
    ///
    /// This does not protect snapshots named via
    /// [`expire_snapshot_ids`](Self::expire_snapshot_ids).
    pub fn retain_last(mut self, retain_last: usize) -> Self {
        self.retain_last = Some(retain_last);
        self
    }

    fn plan(
        &self,
        table: &Table,
        properties: &TableProperties,
    ) -> Result<ExpirePlan> {
        if self.retain_last == Some(0) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Number of snapshots to retain must be at least 1",
            ));
        }
        if properties.max_snapshot_age_ms < 0
            || properties.min_snapshots_to_keep == 0
            || properties.max_ref_age_ms < 0
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "snapshot retention table properties must be non-negative and retain at least one snapshot",
            ));
        }

        let metadata = table.metadata();
        let now = self.as_of_ms;
        let default_cutoff = self.older_than_ms.map_or_else(
            || Self::age_cutoff(now, properties.max_snapshot_age_ms),
            Ok,
        )?;
        let default_min_to_keep =
            self.retain_last.unwrap_or(properties.min_snapshots_to_keep);

        let mut removed_ref_names: Vec<String> = vec![];
        let mut retained_refs: Vec<&SnapshotReference> = vec![];
        for (ref_name, snapshot_ref) in &metadata.refs {
            Self::validate_ref_retention(ref_name, snapshot_ref)?;
            if ref_name == MAIN_BRANCH {
                retained_refs.push(snapshot_ref);
            } else if metadata.snapshot_by_id(snapshot_ref.snapshot_id).is_none() {
                // Match RemoveSnapshots: a non-main reference whose target no
                // longer exists is invalid metadata, not a permanent retention
                // root.  Removing it also keeps it out of `ref_head_ids` below.
                removed_ref_names.push(ref_name.clone());
            } else if !Self::ref_aged_out(
                metadata,
                snapshot_ref,
                now,
                properties.max_ref_age_ms,
            )? {
                retained_refs.push(snapshot_ref);
            } else {
                removed_ref_names.push(ref_name.clone());
            }
        }

        let mut ref_head_ids: HashSet<i64> =
            retained_refs.iter().map(|r| r.snapshot_id).collect();
        if let Some(current_id) = metadata.current_snapshot_id() {
            ref_head_ids.insert(current_id);
        }

        let existing_ids: HashSet<i64> =
            metadata.snapshots().map(|s| s.snapshot_id()).collect();
        let mut expiring_ids: HashSet<i64> = HashSet::new();
        for id in &self.explicit_ids_to_remove {
            if ref_head_ids.contains(id) {
                return Err(Self::reference_error(metadata, *id));
            }
            if existing_ids.contains(id) {
                expiring_ids.insert(*id);
            }
        }

        let mut retained_ids = ref_head_ids.clone();
        let mut referenced_ids = ref_head_ids.clone();
        let mut branches: Vec<(i64, usize, i64)> = vec![];
        for snapshot_ref in &retained_refs {
            match &snapshot_ref.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    max_snapshot_age_ms,
                    ..
                } => {
                    let min_to_keep = (*min_snapshots_to_keep)
                        .map(|value| {
                            usize::try_from(value).map_err(|_| {
                                Error::new(
                                    ErrorKind::DataInvalid,
                                    "branch min-snapshots-to-keep does not fit usize",
                                )
                            })
                        })
                        .transpose()?
                        .unwrap_or(default_min_to_keep);
                    let cutoff = (*max_snapshot_age_ms)
                        .map(|age| Self::age_cutoff(now, age))
                        .transpose()?
                        .unwrap_or(default_cutoff);
                    branches.push((snapshot_ref.snapshot_id, min_to_keep, cutoff));
                }
                SnapshotRetention::Tag { .. } => {
                    referenced_ids.insert(snapshot_ref.snapshot_id);
                }
            }
        }
        if let Some(current_id) = metadata.current_snapshot_id()
            && !branches
                .iter()
                .any(|(head_id, _, _)| *head_id == current_id)
        {
            branches.push((current_id, default_min_to_keep, default_cutoff));
        }
        for (head_id, min_to_keep, cutoff) in branches {
            Self::retain_branch(
                metadata,
                head_id,
                min_to_keep,
                cutoff,
                &mut retained_ids,
                &mut referenced_ids,
            );
        }

        for snapshot in metadata.snapshots() {
            let id = snapshot.snapshot_id();
            if !referenced_ids.contains(&id)
                && snapshot.timestamp_ms() >= default_cutoff
            {
                retained_ids.insert(id);
            }
        }
        // Explicit ids are already in `expiring_ids`; branch retention only
        // controls which additional snapshots are selected by age.
        for snapshot in metadata.snapshots() {
            if !retained_ids.contains(&snapshot.snapshot_id()) {
                expiring_ids.insert(snapshot.snapshot_id());
            }
        }

        let mut ids_to_remove: Vec<i64> = expiring_ids.into_iter().collect();
        ids_to_remove.sort_unstable();
        removed_ref_names.sort();
        Ok(ExpirePlan {
            ids_to_remove,
            refs_to_remove: removed_ref_names,
        })
    }

    fn ref_aged_out(
        metadata: &TableMetadata,
        snapshot_ref: &SnapshotReference,
        now: i64,
        default_max_ref_age_ms: i64,
    ) -> Result<bool> {
        let max_ref_age_ms = match &snapshot_ref.retention {
            SnapshotRetention::Branch { max_ref_age_ms, .. }
            | SnapshotRetention::Tag { max_ref_age_ms } => *max_ref_age_ms,
        }
        .unwrap_or(default_max_ref_age_ms);
        match metadata.snapshot_by_id(snapshot_ref.snapshot_id) {
            Some(snapshot) => {
                Ok(snapshot.timestamp_ms() < Self::age_cutoff(now, max_ref_age_ms)?)
            }
            None => Ok(false),
        }
    }

    fn age_cutoff(now: i64, age_ms: i64) -> Result<i64> {
        if age_ms < 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "snapshot retention age must not be negative",
            ));
        }
        // A retention duration larger than the representable history means
        // that no timestamp can be old enough; clamp rather than turning a
        // valid long-retention policy into a planning failure.
        Ok(now.checked_sub(age_ms).unwrap_or(i64::MIN))
    }

    fn validate_ref_retention(
        ref_name: &str,
        snapshot_ref: &SnapshotReference,
    ) -> Result<()> {
        let (min_snapshots_to_keep, max_snapshot_age_ms, max_ref_age_ms) =
            match &snapshot_ref.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    max_snapshot_age_ms,
                    max_ref_age_ms,
                } => (
                    *min_snapshots_to_keep,
                    *max_snapshot_age_ms,
                    *max_ref_age_ms,
                ),
                SnapshotRetention::Tag { max_ref_age_ms } => {
                    (None, None, *max_ref_age_ms)
                }
            };
        if min_snapshots_to_keep.is_some_and(|value| value <= 0)
            || max_snapshot_age_ms.is_some_and(|value| value < 0)
            || max_ref_age_ms.is_some_and(|value| value < 0)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "snapshot reference {ref_name:?} has invalid retention values"
                ),
            ));
        }
        Ok(())
    }

    fn retain_branch(
        metadata: &TableMetadata,
        head_id: i64,
        min_to_keep: usize,
        cutoff: i64,
        retained_ids: &mut HashSet<i64>,
        referenced_ids: &mut HashSet<i64>,
    ) {
        let mut kept_count = 0usize;
        for ancestor_id in Self::ancestors(metadata, head_id) {
            referenced_ids.insert(ancestor_id);
            let timestamp = metadata
                .snapshot_by_id(ancestor_id)
                .map_or(i64::MIN, |snapshot| snapshot.timestamp_ms());
            if kept_count < min_to_keep || timestamp >= cutoff {
                retained_ids.insert(ancestor_id);
                kept_count += 1;
            }
        }
    }

    fn ancestors(
        metadata: &TableMetadata,
        head_id: i64,
    ) -> impl Iterator<Item = i64> + '_ {
        let mut next_id = Some(head_id);
        std::iter::from_fn(move || {
            let id = next_id?;
            next_id = metadata
                .snapshot_by_id(id)
                .and_then(|snapshot| snapshot.parent_snapshot_id());
            Some(id)
        })
    }

    fn reference_error(metadata: &TableMetadata, snapshot_id: i64) -> Error {
        if metadata.current_snapshot_id() == Some(snapshot_id) {
            return Error::new(
                ErrorKind::DataInvalid,
                "Cannot expire the current snapshot",
            );
        }
        let ref_names: Vec<&str> = metadata
            .refs
            .iter()
            .filter(|(_, snapshot_ref)| snapshot_ref.snapshot_id == snapshot_id)
            .map(|(ref_name, _)| ref_name.as_str())
            .collect();
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot expire snapshot {snapshot_id}: still referenced by {ref_names:?}"
            ),
        )
    }
}

struct ExpirePlan {
    ids_to_remove: Vec<i64>,
    refs_to_remove: Vec<String>,
}

impl TransactionAction for ExpireSnapshotsAction {
    fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let properties = metadata.table_properties()?;

        if !properties.gc_enabled {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot expire snapshots: gc.enabled is false",
            ));
        }

        let plan = self.plan(table, &properties)?;

        if plan.ids_to_remove.is_empty() && plan.refs_to_remove.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let mut updates: Vec<TableUpdate> = plan
            .refs_to_remove
            .into_iter()
            .map(|ref_name| TableUpdate::RemoveSnapshotRef { ref_name })
            .collect();

        let mut stats_updates: Vec<TableUpdate> = vec![];
        for &snapshot_id in &plan.ids_to_remove {
            stats_updates.extend(
                metadata
                    .statistics_for_snapshot(snapshot_id)
                    .is_some()
                    .then_some(TableUpdate::RemoveStatistics { snapshot_id }),
            );
            stats_updates.extend(
                metadata
                    .partition_statistics_for_snapshot(snapshot_id)
                    .is_some()
                    .then_some(TableUpdate::RemovePartitionStatistics {
                        snapshot_id,
                    }),
            );
        }

        if !plan.ids_to_remove.is_empty() {
            updates.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: plan.ids_to_remove,
            });
        }
        updates.extend(stats_updates);

        Ok(ActionCommit::new(
            updates,
            vec![
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: metadata.current_snapshot_id(),
                },
            ],
        ))
    }
}
