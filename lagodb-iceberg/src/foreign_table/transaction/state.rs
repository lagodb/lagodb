//! Per-identity foreign-table state and staged-create generations.

use std::mem;

use iceberg_lite::io::FileIO;
use iceberg_lite::table::Table;
use iceberg_lite::transaction::{StagedTableCreate, Transaction};
use pgrx::pg_sys;

use super::ForeignTableTransaction;
use crate::error::IcebergResult;
use crate::foreign_table::error::IcebergFdwError;
use crate::write::RelationRowRegistry;

#[derive(Debug, Clone, Copy)]
struct StagedRelation {
    oid: pg_sys::Oid,
    created_at: i32,
    dropped_at: Option<i32>,
}

impl StagedRelation {
    const fn new(oid: pg_sys::Oid, created_at: i32) -> Self {
        Self {
            oid,
            created_at,
            dropped_at: None,
        }
    }

    const fn is_active(self) -> bool {
        self.dropped_at.is_none()
    }

    fn rollback_to_level(&mut self, level: i32) {
        if self
            .dropped_at
            .is_some_and(|dropped_at| dropped_at >= level)
        {
            self.dropped_at = None;
        }
    }

    fn promote_to_level(&mut self, level: i32) {
        if self.created_at >= level {
            self.created_at = level - 1;
        }
        if let Some(dropped_at) = &mut self.dropped_at
            && *dropped_at >= level
        {
            *dropped_at = level - 1;
        }
    }
}

#[derive(Debug)]
enum ForeignTableOrigin {
    Published,
    StagedCreate {
        nest_level: i32,
        staged: StagedTableCreate,
        // A provisional definition has exactly one local owner. A replacement
        // may be installed only after this relation is dropped; older
        // generations live in ForeignTableState::superseded for savepoint
        // restoration and created-file cleanup.
        relation: StagedRelation,
    },
}

#[derive(Debug)]
struct ForeignTableGeneration {
    base: Table,
    origin: ForeignTableOrigin,
    transaction: ForeignTableTransaction,
    row_registry: RelationRowRegistry,
}

impl ForeignTableGeneration {
    fn published(base: Table) -> Self {
        Self {
            base,
            origin: ForeignTableOrigin::Published,
            transaction: ForeignTableTransaction::new(),
            row_registry: RelationRowRegistry::default(),
        }
    }

    fn staged(
        relation_oid: pg_sys::Oid,
        nest_level: i32,
        staged: StagedTableCreate,
    ) -> Self {
        Self {
            base: staged.table().clone(),
            origin: ForeignTableOrigin::StagedCreate {
                nest_level,
                staged,
                relation: StagedRelation::new(relation_oid, nest_level),
            },
            transaction: ForeignTableTransaction::new(),
            row_registry: RelationRowRegistry::default(),
        }
    }

    fn is_staged_create(&self) -> bool {
        matches!(self.origin, ForeignTableOrigin::StagedCreate { .. })
    }

    fn staged_at_or_after(&self, level: i32) -> bool {
        matches!(
            self.origin,
            ForeignTableOrigin::StagedCreate { nest_level, .. }
                if nest_level >= level
        )
    }

    fn has_staged_relation(&self, relation_oid: pg_sys::Oid) -> bool {
        matches!(
            &self.origin,
            ForeignTableOrigin::StagedCreate { relation, .. }
                if relation.oid == relation_oid && relation.is_active()
        )
    }

    fn owns_staged_relation(&self, relation_oid: pg_sys::Oid) -> bool {
        matches!(
            &self.origin,
            ForeignTableOrigin::StagedCreate { relation, .. }
                if relation.oid == relation_oid
        )
    }

    fn has_active_staged_relation(&self) -> bool {
        matches!(
            &self.origin,
            ForeignTableOrigin::StagedCreate { relation, .. }
                if relation.is_active()
        )
    }

    fn drop_staged_relation(
        &mut self,
        relation_oid: pg_sys::Oid,
        level: i32,
    ) -> Result<bool, IcebergFdwError> {
        let ForeignTableOrigin::StagedCreate { relation, .. } = &mut self.origin
        else {
            return Ok(false);
        };
        if relation.oid != relation_oid {
            return Ok(false);
        }
        relation.dropped_at = Some(level);
        self.transaction.record_drop(level)?;
        Ok(true)
    }

    fn rollback_to_level(&mut self, level: i32) {
        if let ForeignTableOrigin::StagedCreate { relation, .. } = &mut self.origin {
            relation.rollback_to_level(level);
        }
        self.transaction.rollback_to_level(level);
        self.row_registry.rollback_to_level(level);
    }

    fn promote_to_level(&mut self, level: i32) {
        if let ForeignTableOrigin::StagedCreate {
            nest_level,
            relation,
            ..
        } = &mut self.origin
        {
            if *nest_level >= level {
                *nest_level = level - 1;
            }
            relation.promote_to_level(level);
        }
        self.transaction.promote_to_level(level);
        self.row_registry.promote_to_level(level);
    }

    fn iceberg_transaction(&self) -> Transaction {
        match &self.origin {
            ForeignTableOrigin::Published => Transaction::new(&self.base),
            ForeignTableOrigin::StagedCreate { staged, .. } => staged.transaction(),
        }
    }

    fn canceled_files(&self) -> IcebergResult<(FileIO, Vec<String>)> {
        Ok((
            self.base.file_io().clone(),
            self.transaction.actions.created_file_paths()?,
        ))
    }
}

#[derive(Debug)]
struct SupersededGeneration {
    replaced_at: i32,
    generation: ForeignTableGeneration,
}

/// How the current generation constrains another staged create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StagedCreateDisposition {
    Published,
    ActiveStaged,
    ReplaceDropped,
}

#[derive(Debug)]
pub(super) struct ForeignTableState {
    // Only the current generation can be read or published. Replaced canceled
    // generations remain transaction-local for savepoint restoration and file
    // cleanup; they never re-enter catalog commit planning directly.
    current: ForeignTableGeneration,
    superseded: Vec<SupersededGeneration>,
}

impl ForeignTableState {
    pub(super) fn published(base: Table) -> Self {
        Self {
            current: ForeignTableGeneration::published(base),
            superseded: Vec::new(),
        }
    }

    pub(super) fn staged(
        relation_oid: pg_sys::Oid,
        nest_level: i32,
        staged: StagedTableCreate,
    ) -> Self {
        Self {
            current: ForeignTableGeneration::staged(relation_oid, nest_level, staged),
            superseded: Vec::new(),
        }
    }

    pub(super) fn commit_base(&self) -> &Table {
        &self.current.base
    }

    /// Immutable table returned when the current staged generation was created.
    pub(super) fn staged_base(&self) -> &Table {
        &self.current.base
    }

    pub(super) fn transaction(&self) -> &ForeignTableTransaction {
        &self.current.transaction
    }

    pub(super) fn transaction_mut(&mut self) -> &mut ForeignTableTransaction {
        &mut self.current.transaction
    }

    pub(super) fn row_registry(&self) -> RelationRowRegistry {
        self.current.row_registry.clone()
    }

    pub(super) fn has_catalog_changes(&self) -> bool {
        match &self.current.origin {
            ForeignTableOrigin::Published => {
                !self.current.transaction.actions.is_empty()
            }
            ForeignTableOrigin::StagedCreate { .. } => {
                self.current.has_active_staged_relation()
            }
        }
    }

    pub(super) fn is_staged_create(&self) -> bool {
        self.current.is_staged_create()
    }

    pub(super) fn has_transactional_state(&self) -> bool {
        self.current.is_staged_create()
            || !self.superseded.is_empty()
            || !self.current.transaction.actions.is_empty()
    }

    pub(super) fn iceberg_transaction(&self) -> Transaction {
        self.current.iceberg_transaction()
    }

    pub(super) fn has_staged_relation(&self, relation_oid: pg_sys::Oid) -> bool {
        self.current.has_staged_relation(relation_oid)
    }

    pub(super) fn owns_staged_relation(&self, relation_oid: pg_sys::Oid) -> bool {
        self.current.owns_staged_relation(relation_oid)
    }

    pub(super) fn staged_create_disposition(&self) -> StagedCreateDisposition {
        match &self.current.origin {
            ForeignTableOrigin::Published => StagedCreateDisposition::Published,
            ForeignTableOrigin::StagedCreate { relation, .. } => {
                if relation.is_active() {
                    StagedCreateDisposition::ActiveStaged
                } else {
                    StagedCreateDisposition::ReplaceDropped
                }
            }
        }
    }

    pub(super) fn is_same_generation(&self, base: &Table) -> bool {
        self.current.base.metadata().uuid() == base.metadata().uuid()
    }

    pub(super) fn replace_dropped_staged_generation(
        &mut self,
        relation_oid: pg_sys::Oid,
        level: i32,
        staged: StagedTableCreate,
    ) {
        debug_assert_eq!(
            self.staged_create_disposition(),
            StagedCreateDisposition::ReplaceDropped,
        );
        let replacement = ForeignTableGeneration::staged(relation_oid, level, staged);
        let previous = mem::replace(&mut self.current, replacement);
        self.superseded.push(SupersededGeneration {
            replaced_at: level,
            generation: previous,
        });
    }

    pub(super) fn drop_staged_relation(
        &mut self,
        relation_oid: pg_sys::Oid,
        level: i32,
    ) -> Result<bool, IcebergFdwError> {
        self.current.drop_staged_relation(relation_oid, level)
    }

    pub(super) fn staged_at_or_after(&self, level: i32) -> bool {
        // Whole-state removal follows the oldest retained creation. A newer
        // replacement below the savepoint must restore its predecessor, not
        // discard the state; rollback_to_level pops those replacements first.
        self.superseded.first().map_or_else(
            || self.current.staged_at_or_after(level),
            |generation| generation.generation.staged_at_or_after(level),
        )
    }

    pub(super) fn rollback_to_level(&mut self, level: i32) {
        while self
            .superseded
            .last()
            .is_some_and(|generation| generation.replaced_at >= level)
        {
            let previous =
                self.superseded.pop().expect("superseded generation exists");
            self.current = previous.generation;
        }
        self.current.rollback_to_level(level);
    }

    pub(super) fn promote_to_level(&mut self, level: i32) {
        self.current.promote_to_level(level);
        for superseded in &mut self.superseded {
            superseded.generation.promote_to_level(level);
            if superseded.replaced_at >= level {
                superseded.replaced_at = level - 1;
            }
        }
    }

    pub(super) fn canceled_files(&self) -> IcebergResult<Vec<(FileIO, Vec<String>)>> {
        let include_current = self.current.is_staged_create()
            && !self.current.has_active_staged_relation();
        let current_capacity = if include_current { 1 } else { 0 };
        let mut canceled =
            Vec::with_capacity(self.superseded.len() + current_capacity);
        for generation in &self.superseded {
            canceled.push(generation.generation.canceled_files()?);
        }
        if include_current {
            canceled.push(self.current.canceled_files()?);
        }
        Ok(canceled)
    }
}
