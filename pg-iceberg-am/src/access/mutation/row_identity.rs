//! Synthetic row identities and their transaction-owned relation registries.

use std::collections::HashMap;
use std::rc::Rc;

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::api::TRIGGER_ROW_BLOCK_BASE;
use pg_lakebase_core::prelude::{AmModifyQueryState, AmResult, ItemPointer};
use pgrx::pg_sys;

use crate::access::scan::PlannedMutationTasks;
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
        if position > MAX_POSITION {
            return Err(IcebergError::RowIdentityLimitExceeded);
        }
        debug_assert!(u64::from(file_id.raw()) <= FILE_MASK);
        let payload = (u64::from(file_id.raw()) << POSITION_BITS) | position;
        // The validated 17/30-bit payload is at most 2^47 - 1. Dividing that
        // by 65535 yields at most 0x80008000, below both u32::MAX and the
        // 0xC0000000 trigger-row boundary. The remainder plus one is at most
        // u16::MAX.
        let block_number = (payload / OFFSET_BASE) as u32;
        let offset = ((payload % OFFSET_BASE) + 1) as u16;
        debug_assert!(block_number < TRIGGER_ROW_BLOCK_BASE);
        debug_assert_ne!(offset, 0);
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
        // `block_number < 0xC0000000` and `offset <= u16::MAX`, so this
        // reconstruction is strictly below 2^48 and cannot overflow `u64`.
        let payload =
            u64::from(tid.block_number) * OFFSET_BASE + u64::from(tid.offset - 1);
        if payload >= PAYLOAD_LIMIT {
            return Err(IcebergError::InvariantViolated(
                "ctid is not an Iceberg physical row identity",
            ));
        }
        let raw_file_id = ((payload >> POSITION_BITS) & FILE_MASK) as u32;
        // SAFETY: masking with FILE_MASK establishes the 17-bit ID bound.
        let file_id = unsafe { IcebergFileId::from_valid_raw(raw_file_id) };
        // Masking with the 30-bit POSITION_MASK establishes the u32 bound.
        let row_position = (payload & POSITION_MASK) as u32;
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

#[cfg(test)]
mod tests {
    use pg_lakebase_core::api::TRIGGER_ROW_BLOCK_BASE;
    use pg_lakebase_core::prelude::ItemPointer;

    use super::*;

    #[test]
    fn synthetic_ctid_round_trips_boundaries() {
        let cases = [
            (0, 0),
            (0, MAX_POSITION),
            (u32::try_from(FILE_MASK).unwrap(), 0),
            (u32::try_from(FILE_MASK).unwrap(), MAX_POSITION),
        ];
        for (file_id, position) in cases {
            let file_id = IcebergFileId::try_from_raw(file_id).unwrap();
            let tid = IcebergRowIdentity::encode(file_id, position).unwrap();
            assert_ne!(tid.offset, 0);
            assert!(tid.block_number < TRIGGER_ROW_BLOCK_BASE);
            let decoded = IcebergRowIdentity::decode(&tid).unwrap();
            assert_eq!(decoded.file_id(), file_id);
            assert_eq!(u64::from(decoded.row_position()), position);
        }
    }

    #[test]
    fn synthetic_ctid_rejects_out_of_range_values() {
        assert!(IcebergFileId::try_from_raw(1 << ICEBERG_FILE_ID_BITS).is_err());
        assert!(
            IcebergRowIdentity::encode(
                IcebergFileId::try_from_raw(0).unwrap(),
                MAX_POSITION + 1,
            )
            .is_err()
        );
        assert!(IcebergRowIdentity::decode(&ItemPointer::default()).is_err());
        assert!(
            IcebergRowIdentity::decode(&ItemPointer {
                block_number: TRIGGER_ROW_BLOCK_BASE,
                offset: 1,
            })
            .is_err()
        );
    }
}
