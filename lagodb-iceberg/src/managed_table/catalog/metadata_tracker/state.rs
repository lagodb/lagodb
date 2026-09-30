//! AM-owned storage-generation changes and their savepoint lifetime.

use std::mem;

use iceberg_lite::io::FileIO;

use super::ManagedTableTransaction;
use crate::write::RelationRowRegistry;

#[derive(Debug)]
struct RebuildFrame {
    nest_level: i32,
    transaction: ManagedTableTransaction,
    file_io: Option<FileIO>,
    previous_rebuild_level: Option<i32>,
}

#[derive(Debug)]
pub(super) struct ManagedTableState {
    pub(super) transaction: ManagedTableTransaction,
    // Transaction-scoped identities and claims survive generation changes.
    // Rebuild frames restore actions without owning this registry's lifecycle.
    pub(super) row_registry: RelationRowRegistry,
    pub(super) file_io: Option<FileIO>,
    // CREATE and local TRUNCATE register the active generation's owner. RELEASE
    // reparents it; rollback restores the previous generation and its owner.
    rebuild_level: Option<i32>,
    rebuild_history: Vec<RebuildFrame>,
}

impl ManagedTableState {
    pub(super) fn new() -> Self {
        Self {
            transaction: ManagedTableTransaction::new(),
            row_registry: RelationRowRegistry::default(),
            file_io: None,
            rebuild_level: None,
            rebuild_history: Vec::new(),
        }
    }

    pub(super) fn has_changes(&self) -> bool {
        self.rebuild_level.is_some() || !self.transaction.actions.is_empty()
    }

    pub(super) fn was_rebuilt(&self) -> bool {
        self.rebuild_level.is_some()
    }

    pub(super) fn owns_local_generation_at(&self, nest_level: i32) -> bool {
        self.rebuild_level == Some(nest_level)
    }

    pub(super) fn record_rebuild(&mut self, nest_level: i32, file_io: &FileIO) {
        let previous =
            mem::replace(&mut self.transaction, ManagedTableTransaction::new());
        let previous_file_io = self.file_io.replace(file_io.clone());
        // One pre-rebuild baseline per savepoint is sufficient. Later rebuilds
        // at that level cannot be restored independently by PostgreSQL.
        if nest_level > 1
            && !self
                .rebuild_history
                .last()
                .is_some_and(|frame| frame.nest_level == nest_level)
        {
            self.rebuild_history.push(RebuildFrame {
                nest_level,
                transaction: previous,
                file_io: previous_file_io,
                previous_rebuild_level: self.rebuild_level,
            });
        }
        self.rebuild_level = Some(nest_level);
    }

    pub(super) fn rollback_to_level(&mut self, level: i32) {
        self.transaction.rollback_to_level(level);
        self.row_registry.rollback_to_level(level);
        while self
            .rebuild_history
            .last()
            .is_some_and(|frame| frame.nest_level >= level)
        {
            let frame = self.rebuild_history.pop().expect("rebuild frame exists");
            self.transaction = frame.transaction;
            self.file_io = frame.file_io;
            self.rebuild_level = frame.previous_rebuild_level;
            self.transaction.rollback_to_level(level);
        }
    }

    pub(super) fn promote_to_level(&mut self, level: i32) {
        self.transaction.promote_to_level(level);
        self.row_registry.promote_to_level(level);
        if let Some(rebuild_level) = &mut self.rebuild_level
            && *rebuild_level >= level
        {
            *rebuild_level = level - 1;
        }
        for frame in &mut self.rebuild_history {
            frame.transaction.promote_to_level(level);
            if frame.nest_level >= level {
                frame.nest_level = level - 1;
            }
            if let Some(previous_level) = &mut frame.previous_rebuild_level
                && *previous_level >= level
            {
                *previous_level = level - 1;
            }
        }
        self.rebuild_history.retain(|frame| frame.nest_level > 1);
        // Releasing a child savepoint can merge its baseline into a parent
        // that already has one. Keep the oldest state for parent rollback.
        self.rebuild_history.dedup_by_key(|frame| frame.nest_level);
    }
}
