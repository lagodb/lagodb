//! Iceberg manifest rewrite transaction action.
//!
//! The action is generic and metadata-only. Selection is evaluated against
//! the transaction-local current snapshot, so callers may apply it after a
//! data-file rewrite without exposing an intermediate table pointer.

use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use crate::table::Table;
use crate::transaction::snapshot_delta::DeltaSnapshotProducer;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::Result;

/// Rewrites selected live manifests without changing the live content-file set.
pub struct RewriteManifestsAction {
    min_count_to_merge: usize,
    target_size_bytes: u64,
    commit_uuid: Option<Uuid>,
}

impl RewriteManifestsAction {
    pub(crate) fn new(min_count_to_merge: usize, target_size_bytes: u64) -> Self {
        Self {
            min_count_to_merge,
            target_size_bytes,
            commit_uuid: None,
        }
    }

    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }
}

impl TransactionAction for RewriteManifestsAction {
    fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let mut producer = DeltaSnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            None,
            HashMap::new(),
        );
        producer.commit_manifest_rewrite(
            self.min_count_to_merge,
            self.target_size_bytes,
        )
    }
}
