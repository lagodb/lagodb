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

use crate::spec::{SnapshotRef, TableMetadataRef};

struct Ancestors {
    next: Option<SnapshotRef>,
    get_snapshot: Box<dyn Fn(i64) -> Option<SnapshotRef> + Send>,
}

impl Iterator for Ancestors {
    type Item = SnapshotRef;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = self.next.take()?;
        self.next = snapshot
            .parent_snapshot_id()
            .and_then(|id| (self.get_snapshot)(id));
        Some(snapshot)
    }
}

/// Iterate from `snapshot_id` to the root snapshot, inclusive.
pub fn ancestors_of(
    table_metadata: &TableMetadataRef,
    snapshot_id: i64,
) -> impl Iterator<Item = SnapshotRef> + Send {
    let initial = table_metadata.snapshot_by_id(snapshot_id).cloned();
    let table_metadata = table_metadata.clone();
    Ancestors {
        next: initial,
        get_snapshot: Box::new(move |id| table_metadata.snapshot_by_id(id).cloned()),
    }
}

/// Iterate from `latest_snapshot_id` inclusive to `oldest_snapshot_id` exclusive.
pub fn ancestors_between(
    table_metadata: &TableMetadataRef,
    latest_snapshot_id: i64,
    oldest_snapshot_id: Option<i64>,
) -> impl Iterator<Item = SnapshotRef> + Send {
    ancestors_of(table_metadata, latest_snapshot_id).take_while(move |snapshot| {
        oldest_snapshot_id
            .map(|id| snapshot.snapshot_id() != id)
            .unwrap_or(true)
    })
}
