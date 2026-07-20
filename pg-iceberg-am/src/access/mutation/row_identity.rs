//! Synthetic row identities and their transaction-owned relation registries.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::api::TRIGGER_ROW_BLOCK_BASE;
use pg_lakebase_core::prelude::{AmModifyQueryState, AmResult, ItemPointer};
use pgrx::pg_sys;

use crate::access::scan::PlannedScanTasks;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::catalog::row_mutations::{
    ICEBERG_FILE_ID_BITS, IcebergFileId, RelationRowRegistry,
};
use crate::error::{IcebergError, IcebergResult};

/// Iceberg metadata captured once by a Modify-purpose target scan and consumed
/// when the corresponding relation-local modify state is opened.
#[derive(Debug, Clone)]
pub struct IcebergModifyScanContext {
    pub(super) starting_snapshot_id: Option<i64>,
    pub(super) conflict_filter: Predicate,
    scan_tasks: Arc<PlannedScanTasks>,
}

impl IcebergModifyScanContext {
    pub(crate) fn new(
        starting_snapshot_id: Option<i64>,
        conflict_filter: Predicate,
        scan_tasks: Arc<PlannedScanTasks>,
    ) -> Self {
        Self {
            starting_snapshot_id,
            conflict_filter,
            scan_tasks,
        }
    }

    pub(crate) fn scan_tasks(&self) -> Arc<PlannedScanTasks> {
        Arc::clone(&self.scan_tasks)
    }
}

impl PartialEq for IcebergModifyScanContext {
    fn eq(&self, other: &Self) -> bool {
        self.starting_snapshot_id == other.starting_snapshot_id
            && self.conflict_filter == other.conflict_filter
            && Arc::ptr_eq(&self.scan_tasks, &other.scan_tasks)
    }
}

/// Compact Iceberg row identity decoded from the PostgreSQL `ctid` carrier.
/// File paths remain interned and are resolved only when delete files are
/// finalized, never on the per-row UPDATE/DELETE hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IcebergRowIdentity {
    file_id: IcebergFileId,
    row_position: u32,
}

impl IcebergRowIdentity {
    const fn new(file_id: IcebergFileId, row_position: u32) -> Self {
        Self {
            file_id,
            row_position,
        }
    }

    pub(super) const fn file_id(self) -> IcebergFileId {
        self.file_id
    }

    pub(super) const fn row_position(self) -> u32 {
        self.row_position
    }

    pub(super) fn encode(
        file_id: IcebergFileId,
        position: u64,
    ) -> IcebergResult<ItemPointer> {
        if u64::from(file_id.raw()) > FILE_MASK || position > MAX_POSITION {
            return Err(IcebergError::RowIdentityLimitExceeded);
        }
        let payload = (u64::from(file_id.raw()) << POSITION_BITS) | position;
        let block_number = u32::try_from(payload / OFFSET_BASE).map_err(|_| {
            IcebergError::InvariantViolated("synthetic ctid block number overflow")
        })?;
        if block_number >= TRIGGER_ROW_BLOCK_BASE {
            return Err(IcebergError::InvariantViolated(
                "Iceberg row identity overlaps the trigger-row namespace",
            ));
        }
        let offset = u16::try_from((payload % OFFSET_BASE) + 1).map_err(|_| {
            IcebergError::InvariantViolated("synthetic ctid offset overflow")
        })?;
        Ok(ItemPointer {
            block_number,
            offset,
        })
    }

    pub(super) fn decode(tid: &ItemPointer) -> IcebergResult<Self> {
        if tid.offset == 0 || tid.block_number >= TRIGGER_ROW_BLOCK_BASE {
            return Err(IcebergError::InvariantViolated(
                "ctid is not an Iceberg physical row identity",
            ));
        }
        let payload = u64::from(tid.block_number)
            .checked_mul(OFFSET_BASE)
            .and_then(|base| base.checked_add(u64::from(tid.offset - 1)))
            .ok_or_else(|| {
                IcebergError::InvariantViolated("synthetic ctid payload overflow")
            })?;
        if payload >= PAYLOAD_LIMIT {
            return Err(IcebergError::InvariantViolated(
                "ctid is not an Iceberg physical row identity",
            ));
        }
        let file_id = u32::try_from((payload >> POSITION_BITS) & FILE_MASK)
            .map(IcebergFileId::from_raw)
            .map_err(|_| {
                IcebergError::InvariantViolated("synthetic ctid file id overflow")
            })?;
        let row_position = u32::try_from(payload & POSITION_MASK).map_err(|_| {
            IcebergError::InvariantViolated("synthetic ctid row position overflow")
        })?;
        Ok(Self::new(file_id, row_position))
    }
}

// TODO(synthetic-ctid-capacity): this 17/30-bit split caps one relation at
// 131,072 registered files and each file at 2^30 rows. Target scans may
// register files before quals eliminate all their rows, so redesign the
// identity carrier/registry before workloads can approach either bound.
const POSITION_BITS: u32 = 30;
pub(super) const MAX_POSITION: u64 = (1u64 << POSITION_BITS) - 1;
pub(super) const FILE_MASK: u64 = (1u64 << ICEBERG_FILE_ID_BITS) - 1;
const POSITION_MASK: u64 = (1u64 << POSITION_BITS) - 1;
const PAYLOAD_LIMIT: u64 = 1u64 << (ICEBERG_FILE_ID_BITS + POSITION_BITS);
const OFFSET_BASE: u64 = u16::MAX as u64;

/// Borrowed data-file source registered once per contiguous scan run.
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
