//! Synthetic row identities and their transaction-owned relation registries.

use std::collections::HashMap;
use std::rc::Rc;

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::prelude::{AmModifyQueryState, AmResult, ItemPointer};
use pgrx::pg_sys;

use crate::engine::write::{
    IcebergFileId, IcebergRowIdentity, PlannedMutationTasks, RelationRowRegistry,
};
use crate::managed_table::catalog::metadata_tracker::TxMetadata;

/// Iceberg metadata captured once by a Modify-purpose target scan and consumed
/// when the corresponding relation-local modify state is opened.
#[derive(Debug, Clone)]
pub struct IcebergModifyScanContext {
    pub(super) starting_snapshot_id: Option<i64>,
    pub(super) conflict_filter: Predicate,
    scan_tasks: Rc<PlannedMutationTasks>,
}

impl IcebergModifyScanContext {
    pub(crate) fn new(
        starting_snapshot_id: Option<i64>,
        conflict_filter: Predicate,
        scan_tasks: Rc<PlannedMutationTasks>,
    ) -> Self {
        Self {
            starting_snapshot_id,
            conflict_filter,
            scan_tasks,
        }
    }

    pub(crate) fn scan_tasks(&self) -> Rc<PlannedMutationTasks> {
        Rc::clone(&self.scan_tasks)
    }
}

impl PartialEq for IcebergModifyScanContext {
    fn eq(&self, other: &Self) -> bool {
        self.starting_snapshot_id == other.starting_snapshot_id
            && self.conflict_filter == other.conflict_filter
            && Rc::ptr_eq(&self.scan_tasks, &other.scan_tasks)
    }
}

/// Borrowed data-file source passed to the transaction-scoped path interner.
#[derive(Debug, Clone, Copy)]
pub struct IcebergFileSource<'a>(&'a str);

impl<'a> IcebergFileSource<'a> {
    pub(crate) const fn new(path: &'a str) -> Self {
        Self(path)
    }
}

/// Iceberg identity registry shared by all ModifyTable nodes in one PostgreSQL
/// executor query. It caches only handles to transaction-owned relation
/// registries; file paths and file-ID namespaces never live at query scope.
#[derive(Debug, Default)]
pub struct IcebergModifyQueryState {
    pub(crate) relations: HashMap<pg_sys::Oid, RelationRowRegistry>,
}

impl IcebergModifyQueryState {
    pub(super) fn relation_registry(
        &mut self,
        relation_oid: pg_sys::Oid,
    ) -> AmResult<RelationRowRegistry> {
        if let Some(registry) = self.relations.get(&relation_oid) {
            return Ok(registry.clone());
        }
        let registry = TxMetadata::current().row_registry(relation_oid)?;
        self.relations.insert(relation_oid, registry.clone());
        Ok(registry)
    }
}

impl AmModifyQueryState for IcebergModifyQueryState {
    type ScanIdentitySource<'a> = IcebergFileSource<'a>;
    type RegisteredScanIdentity = IcebergFileId;
    type ScanIdentity<'a> = u64;

    fn new() -> AmResult<Self> {
        Ok(Self::default())
    }

    fn register_scan_identity_source(
        &mut self,
        relation_oid: pg_sys::Oid,
        source: &Self::ScanIdentitySource<'_>,
    ) -> AmResult<Self::RegisteredScanIdentity> {
        Ok(self
            .relation_registry(relation_oid)?
            .register_file(source.0)?)
    }

    fn encode_row_identity(
        source: Self::RegisteredScanIdentity,
        position: &Self::ScanIdentity<'_>,
    ) -> AmResult<ItemPointer> {
        Ok(IcebergRowIdentity::encode(source, *position)?)
    }
}
