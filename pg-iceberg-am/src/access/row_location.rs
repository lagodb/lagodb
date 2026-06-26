//! Statement-local row identity for Iceberg DML.
//!
//! PostgreSQL's table-AM UPDATE/DELETE path communicates the target row through
//! `ItemPointer` (`ctid`). Iceberg's native row identity for v2 DML is
//! `(data_file_path, position)`, so this module owns the lossy boundary:
//! scans intern file paths in a DML-frame-local registry and pack
//! `(file_index, pos)` into `ctid`; tuple callbacks decode it back.
//!
//! ## Why no `map_id` in the ctid
//!
//! The relation a ctid belongs to is never ambiguous at decode time: every
//! tuple callback (`tuple_delete` / `tuple_update_slot` / `tuple_fetch_row_version`)
//! is dispatched for a specific relation, so `rel_oid` is always in hand. The
//! registry is therefore keyed by `(frame_id, rel_oid)` and the ctid carries
//! only `(file_index, pos)`. This mirrors how `pg_lake`'s FDW recovers the
//! relation from the executor's per-result-relation dispatch rather than from
//! ctid bits, and frees the whole 47-bit payload for the file/row split.
//!
//! ## Fixed bit split
//!
//! - `file_index`: 17 bits, identifying an interned data-file path in the
//!   relation's map (up to 131,072 data files per relation per statement);
//! - `pos`: 30 bits, the original row ordinal inside the data file (up to
//!   1,073,741,824 rows per data file).
//!
//! A `pg_lake`-style split sized from the planned file count is intentionally
//! avoided: the scan only plans files lazily inside `to_arrow`, so sizing the
//! split up front would require an extra `plan_files` pass on every DML scan.
//! The limits are enforced loudly (hard errors) instead of silently wrapping.
//!
//! TODO(scan-plan-decoupling): once `ScanSpec` plans files once and caches the
//! task list (see `scan.rs::build_scan`), pass the planned data-file count into
//! [`begin_dml_scan`] and size `FILE_BITS`/`POS_BITS` per map from it, replacing
//! this fixed split with a dynamic one at no extra planning cost.
//!
//! Reference — how `pg_lake` does this (verified in its FDW
//! `RowIdRecordStringToItemPointer` / `postgresBeginForeignScan`): it interns
//! the file path into a per-scan small integer (an `HTAB resultFileIndexes`) and
//! packs the ctid as
//!
//! ```c
//! uint64 ctidInt = ((uint64) resultFileIndex->index << fileRowNumberBits) | fileRowNumber;
//! ItemPointer ctid = UInt64ToItemPointer(ctidInt);
//! ```
//!
//! with the split sized dynamically from the file count known up front:
//!
//! ```c
//! int fileIndexBits = (int)(log2(resultFileCount) + 1 + 0.5); // per-scan file count
//! fsstate->fileRowNumberBits = 48 - fileIndexBits;            // remainder to row number
//! ```
//!
//! `pg_lake` can do this because, as an FDW, it has the full file list at scan
//! begin; our table-AM scan plans lazily, hence the decoupling prerequisite
//! above. Note `pg_lake` also uses the full 48 bits and carries no `map_id`
//! (relation context comes from per-result-relation dispatch, as here).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use pg_lakebase_core::access::dml::{
    DmlFrameId, current_dml_frame_id, current_dml_target_frame,
    register_current_dml_frame_cleanup,
};
use pg_lakebase_core::handles::ItemPointer;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};

const FILE_BITS: u32 = 17;
const POS_BITS: u32 = 30;

const MAX_FILES_PER_MAP: usize = 1usize << FILE_BITS;
const MAX_POS: u64 = (1u64 << POS_BITS) - 1;

const POS_MASK: u64 = (1u64 << POS_BITS) - 1;
const FILE_MASK: u64 = (1u64 << FILE_BITS) - 1;
const PAYLOAD_LIMIT: u64 = 1u64 << (FILE_BITS + POS_BITS);

/// Use a base-65535 representation so `ip_posid` is never zero. PostgreSQL's
/// `ItemPointerIsValid` treats offset 0 as invalid even for table AMs whose
/// TID is synthetic.
const ITEM_POINTER_OFFSET_BASE: u64 = u16::MAX as u64;

/// Handle to one relation's file map within the current DML frame.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RowLocationMapHandle {
    frame_id: DmlFrameId,
    rel_oid: pg_sys::Oid,
}

impl RowLocationMapHandle {
    /// Intern `data_file_path` in this relation's map, returning its index.
    pub(crate) fn file_index_for(&self, data_file_path: &str) -> IcebergResult<u32> {
        REGISTRY.with(|registry| {
            registry.borrow_mut().file_index_for(*self, data_file_path)
        })
    }

    /// Pack an already-interned `(file_index, position)` into a synthetic ctid.
    pub(crate) fn tid_for_file_index(
        &self,
        file_index: u32,
        position: u64,
    ) -> IcebergResult<ItemPointer> {
        RowLocationCodec::encode(file_index, position)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RowLocation {
    pub(crate) data_file_path: Rc<str>,
    pub(crate) position: u64,
    pub(crate) starting_snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RegistryKey {
    frame_id: DmlFrameId,
    rel_oid: pg_sys::Oid,
}

#[derive(Debug)]
struct RowLocationMap {
    starting_snapshot_id: Option<i64>,
    files: Vec<Rc<str>>,
    file_indexes: HashMap<Rc<str>, u32>,
}

impl RowLocationMap {
    fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            starting_snapshot_id,
            files: Vec::new(),
            file_indexes: HashMap::new(),
        }
    }

    fn intern_file(&mut self, data_file_path: &str) -> IcebergResult<u32> {
        if let Some(index) = self.file_indexes.get(data_file_path) {
            return Ok(*index);
        }
        if self.files.len() >= MAX_FILES_PER_MAP {
            return Err(IcebergError::MetadataTracker(format!(
                "too many data files in one Iceberg DML scan; maximum is {MAX_FILES_PER_MAP}"
            )));
        }
        let index = u32::try_from(self.files.len()).map_err(|_| {
            IcebergError::InvariantViolated("row-location file index overflow")
        })?;
        let data_file_path = Rc::<str>::from(data_file_path);
        self.files.push(Rc::clone(&data_file_path));
        self.file_indexes.insert(data_file_path, index);
        Ok(index)
    }

    fn file_path(&self, file_index: u32) -> IcebergResult<Rc<str>> {
        self.files
            .get(usize::try_from(file_index).unwrap_or(usize::MAX))
            .cloned()
            .ok_or_else(|| {
                IcebergError::MetadataTracker(format!(
                    "Iceberg DML ctid references unknown file index {file_index}"
                ))
            })
    }
}

#[derive(Debug, Default)]
struct RowLocationRegistry {
    maps: HashMap<RegistryKey, RowLocationMap>,
}

impl RowLocationRegistry {
    fn begin_scan(
        &mut self,
        frame_id: DmlFrameId,
        rel_oid: pg_sys::Oid,
        starting_snapshot_id: Option<i64>,
    ) -> IcebergResult<(RowLocationMapHandle, bool)> {
        let key = RegistryKey { frame_id, rel_oid };
        let mut inserted = false;
        match self.maps.get(&key) {
            Some(map) => {
                if map.starting_snapshot_id != starting_snapshot_id {
                    return Err(IcebergError::MetadataTracker(format!(
                        "Iceberg DML scan for relation {rel_oid} was rebound to a different snapshot"
                    )));
                }
            }
            None => {
                self.maps
                    .insert(key, RowLocationMap::new(starting_snapshot_id));
                inserted = true;
            }
        }
        Ok((RowLocationMapHandle { frame_id, rel_oid }, inserted))
    }

    fn file_index_for(
        &mut self,
        handle: RowLocationMapHandle,
        data_file_path: &str,
    ) -> IcebergResult<u32> {
        let key = RegistryKey {
            frame_id: handle.frame_id,
            rel_oid: handle.rel_oid,
        };
        let map = self.maps.get_mut(&key).ok_or_else(|| {
            IcebergError::MetadataTracker(
                "Iceberg DML row-location map no longer exists".to_owned(),
            )
        })?;
        map.intern_file(data_file_path)
    }

    fn lookup(
        &self,
        frame_id: DmlFrameId,
        rel_oid: pg_sys::Oid,
        tid: &ItemPointer,
    ) -> IcebergResult<Option<RowLocation>> {
        let Some((file_index, position)) = RowLocationCodec::decode(tid)? else {
            return Ok(None);
        };
        let key = RegistryKey { frame_id, rel_oid };
        let map = self.maps.get(&key).ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "Iceberg DML ctid has no active row-location map for relation {rel_oid}"
            ))
        })?;
        Ok(Some(RowLocation {
            data_file_path: map.file_path(file_index)?,
            position,
            starting_snapshot_id: map.starting_snapshot_id,
        }))
    }

    fn remove_map(&mut self, key: RegistryKey) {
        self.maps.remove(&key);
    }
}

struct RowLocationCodec;

impl RowLocationCodec {
    fn encode(file_index: u32, position: u64) -> IcebergResult<ItemPointer> {
        if position > MAX_POS {
            return Err(IcebergError::MetadataTracker(format!(
                "Iceberg row position {position} is too large for synthetic ctid"
            )));
        }
        if u64::from(file_index) > FILE_MASK {
            return Err(IcebergError::MetadataTracker(format!(
                "Iceberg DML file index {file_index} is too large for synthetic ctid"
            )));
        }

        let payload = (u64::from(file_index) << POS_BITS) | position;
        debug_assert!(payload < PAYLOAD_LIMIT);

        let block_number = payload / ITEM_POINTER_OFFSET_BASE;
        let offset = (payload % ITEM_POINTER_OFFSET_BASE) + 1;
        let block_number = u32::try_from(block_number).map_err(|_| {
            IcebergError::InvariantViolated("row-location block number overflow")
        })?;
        let offset = u16::try_from(offset).map_err(|_| {
            IcebergError::InvariantViolated("row-location offset overflow")
        })?;

        Ok(ItemPointer {
            block_number,
            offset,
        })
    }

    fn decode(tid: &ItemPointer) -> IcebergResult<Option<(u32, u64)>> {
        if tid.offset == 0 {
            return Ok(None);
        }
        let payload = u64::from(tid.block_number)
            .checked_mul(ITEM_POINTER_OFFSET_BASE)
            .and_then(|base| base.checked_add(u64::from(tid.offset - 1)))
            .ok_or(IcebergError::InvariantViolated(
                "row-location ctid payload overflow",
            ))?;
        if payload >= PAYLOAD_LIMIT {
            return Ok(None);
        }

        let file_index =
            u32::try_from((payload >> POS_BITS) & FILE_MASK).map_err(|_| {
                IcebergError::InvariantViolated("row-location file id overflow")
            })?;
        let position = payload & POS_MASK;
        Ok(Some((file_index, position)))
    }
}

thread_local! {
    static REGISTRY: RefCell<RowLocationRegistry> =
        RefCell::new(RowLocationRegistry::default());
}

/// Begin row-location tracking for a scan, but only when `rel_oid` is the
/// relation the active DML frame rewrites in place. Source-only scans (an
/// `UPDATE ... FROM` join input, a subquery relation) and non-DML scans get
/// `None` and skip `_file`/`_pos` synthesis entirely.
pub(crate) fn begin_dml_scan(
    rel_oid: pg_sys::Oid,
    starting_snapshot_id: Option<i64>,
) -> IcebergResult<Option<RowLocationMapHandle>> {
    let Some(frame_id) = current_dml_target_frame(rel_oid) else {
        return Ok(None);
    };
    let (handle, inserted) = REGISTRY.with(|registry| {
        registry
            .borrow_mut()
            .begin_scan(frame_id, rel_oid, starting_snapshot_id)
    })?;

    if inserted {
        let key = RegistryKey { frame_id, rel_oid };
        if let Err(error) = register_current_dml_frame_cleanup(move || {
            REGISTRY.with(|registry| registry.borrow_mut().remove_map(key));
        }) {
            REGISTRY.with(|registry| registry.borrow_mut().remove_map(key));
            return Err(IcebergError::MetadataTracker(format!(
                "failed to register Iceberg DML row-location cleanup: {error}"
            )));
        }
    }

    Ok(Some(handle))
}

pub(crate) fn lookup_current(
    rel_oid: pg_sys::Oid,
    tid: &ItemPointer,
) -> IcebergResult<Option<RowLocation>> {
    let Some(frame_id) = current_dml_frame_id() else {
        return Ok(None);
    };
    REGISTRY.with(|registry| registry.borrow().lookup(frame_id, rel_oid, tid))
}
