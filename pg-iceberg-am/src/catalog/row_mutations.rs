//! Transaction-scoped physical identity and mutation ownership for Iceberg rows.
//!
//! PostgreSQL heap rows carry both a stable physical identity and transaction
//! metadata in their tuple header. Iceberg rows do not. This registry provides
//! the narrower facts required by the executor: a transaction-stable file ID
//! and the command that first modified a `(file, row-position)` pair.

use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use pgrx::pg_sys;
use roaring::RoaringBitmap;

use crate::error::{IcebergError, IcebergResult};

pub(crate) const ICEBERG_FILE_ID_BITS: u32 = 17;
pub(crate) const MAX_ICEBERG_FILES: usize = 1usize << ICEBERG_FILE_ID_BITS;

/// Stable ID for one data-file path within a transaction and relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IcebergFileId(u32);

impl IcebergFileId {
    pub(crate) const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0
    }

    pub(crate) fn index(self) -> usize {
        usize::try_from(self.0)
            .expect("PostgreSQL platforms can index every Iceberg file ID")
    }
}

/// Identity of one relation-local modify-state instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ModifyStateId(u64);

/// Shared position set owned by one ModifyState for one Iceberg data file.
#[derive(Debug, Clone)]
pub(crate) struct OwnedRowPositions(Rc<RefCell<RoaringBitmap>>);

impl OwnedRowPositions {
    fn new_with(position: u32) -> Self {
        let mut positions = RoaringBitmap::new();
        let inserted = positions.insert(position);
        debug_assert!(inserted);
        Self(Rc::new(RefCell::new(positions)))
    }

    pub(crate) fn borrow(&self) -> IcebergResult<Ref<'_, RoaringBitmap>> {
        self.0.try_borrow().map_err(|_| {
            IcebergError::InvariantViolated(
                "Iceberg mutation-owner bitmap is mutably borrowed",
            )
        })
    }

    fn contains(&self, position: u32) -> IcebergResult<bool> {
        Ok(self.borrow()?.contains(position))
    }

    fn insert(&self, position: u32) -> IcebergResult<bool> {
        self.0
            .try_borrow_mut()
            .map_err(|_| {
                IcebergError::InvariantViolated(
                    "Iceberg mutation-owner bitmap is already borrowed",
                )
            })
            .map(|mut positions| positions.insert(position))
    }
}

/// Result of claiming one physical Iceberg row for mutation.
#[derive(Debug, Clone)]
pub(crate) enum RowMutationClaim {
    FirstTouch {
        /// Present exactly once, when this ModifyState first touches the file.
        /// The position-delete accumulator retains this shared bitmap handle;
        /// subsequent successful rows require no second bitmap insertion.
        new_file_positions: Option<OwnedRowPositions>,
    },
    PreviouslyModified {
        modifying_command_id: pg_sys::CommandId,
    },
}

#[derive(Debug)]
struct MutationOwner {
    modify_state_id: ModifyStateId,
    command_id: pg_sys::CommandId,
    nest_level: i32,
    positions: OwnedRowPositions,
}

#[derive(Debug)]
struct RegisteredFile {
    path: Rc<str>,
    mutation_owners: Vec<MutationOwner>,
}

#[derive(Debug, Default)]
struct RelationRowRegistryInner {
    file_ids: HashMap<Rc<str>, IcebergFileId>,
    files: Vec<RegisteredFile>,
    next_modify_state_id: u64,
}

/// Cloneable handle to one relation's transaction-scoped row registry.
#[derive(Debug, Clone, Default)]
pub(crate) struct RelationRowRegistry {
    inner: Rc<RefCell<RelationRowRegistryInner>>,
}

impl RelationRowRegistry {
    /// Intern one path and return its stable transaction/relation file ID.
    /// IDs are monotonic for the life of this registry and are never reused
    /// after a savepoint rollback.
    pub(crate) fn register_file(
        &self,
        file_path: &str,
    ) -> IcebergResult<IcebergFileId> {
        let mut inner = self.inner.try_borrow_mut().map_err(|_| {
            IcebergError::InvariantViolated(
                "transaction row registry is already borrowed",
            )
        })?;
        if let Some(&file_id) = inner.file_ids.get(file_path) {
            return Ok(file_id);
        }
        if inner.files.len() >= MAX_ICEBERG_FILES {
            return Err(IcebergError::FileIdLimitExceeded {
                max_files: MAX_ICEBERG_FILES,
            });
        }

        let raw_id = u32::try_from(inner.files.len()).map_err(|_| {
            IcebergError::MetadataTracker(
                "Iceberg file ID cannot be represented as u32".to_owned(),
            )
        })?;
        let file_id = IcebergFileId(raw_id);
        let path = Rc::<str>::from(file_path);
        inner.file_ids.insert(Rc::clone(&path), file_id);
        inner.files.push(RegisteredFile {
            path,
            mutation_owners: Vec::new(),
        });
        Ok(file_id)
    }

    pub(crate) fn file_path(&self, file_id: IcebergFileId) -> IcebergResult<Rc<str>> {
        self.inner
            .try_borrow()
            .map_err(|_| {
                IcebergError::InvariantViolated(
                    "transaction row registry is mutably borrowed",
                )
            })?
            .files
            .get(file_id.index())
            .map(|file| Rc::clone(&file.path))
            .ok_or_else(|| {
                IcebergError::MetadataTracker(format!(
                    "unknown Iceberg file ID {}",
                    file_id.raw()
                ))
            })
    }

    /// Allocate a stable owner ID for one relation-local ModifyState.
    pub(crate) fn begin_modify_state(&self) -> IcebergResult<ModifyStateId> {
        let mut inner = self.inner.try_borrow_mut().map_err(|_| {
            IcebergError::InvariantViolated(
                "transaction row registry is already borrowed",
            )
        })?;
        let id = ModifyStateId(inner.next_modify_state_id);
        inner.next_modify_state_id =
            inner.next_modify_state_id.checked_add(1).ok_or_else(|| {
                IcebergError::MetadataTracker(
                    "Iceberg ModifyState ID space was exhausted".to_owned(),
                )
            })?;
        Ok(id)
    }

    /// Claim a physical row for the current ModifyState.
    ///
    /// The common case (one owner for a file) performs one `RoaringBitmap`
    /// insertion. Other owner bitmaps are only probed when multiple
    /// ModifyStates touch the same file.
    pub(crate) fn claim(
        &self,
        modify_state_id: ModifyStateId,
        file_id: IcebergFileId,
        position: u32,
        command_id: pg_sys::CommandId,
    ) -> IcebergResult<RowMutationClaim> {
        // SAFETY: AM callbacks run in an active backend transaction. No PG
        // pointer is retained.
        let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        self.claim_at_level(
            modify_state_id,
            file_id,
            position,
            command_id,
            nest_level,
        )
    }

    fn claim_at_level(
        &self,
        modify_state_id: ModifyStateId,
        file_id: IcebergFileId,
        position: u32,
        command_id: pg_sys::CommandId,
        nest_level: i32,
    ) -> IcebergResult<RowMutationClaim> {
        let mut inner = self.inner.try_borrow_mut().map_err(|_| {
            IcebergError::InvariantViolated(
                "transaction row registry is already borrowed",
            )
        })?;
        let file = inner.files.get_mut(file_id.index()).ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "unknown Iceberg file ID {}",
                file_id.raw()
            ))
        })?;

        let current_owner = file
            .mutation_owners
            .iter()
            .position(|owner| owner.modify_state_id == modify_state_id);

        // TODO(row-claim-index): this probes every other ModifyState bitmap for
        // each row, making repeated cross-node mutations O(owner count). Keep a
        // per-file union bitmap (with a same-owner fast path) once this becomes
        // measurable; subtransaction rollback must rebuild or version that
        // union together with `mutation_owners`.
        for (index, owner) in file.mutation_owners.iter().enumerate() {
            if Some(index) != current_owner && owner.positions.contains(position)? {
                return Ok(RowMutationClaim::PreviouslyModified {
                    modifying_command_id: owner.command_id,
                });
            }
        }

        if let Some(index) = current_owner {
            let owner = &file.mutation_owners[index];
            if owner.command_id != command_id {
                return Err(IcebergError::InvariantViolated(
                    "one Iceberg ModifyState observed multiple command IDs",
                ));
            }
            if !owner.positions.insert(position)? {
                return Ok(RowMutationClaim::PreviouslyModified {
                    modifying_command_id: owner.command_id,
                });
            }
            return Ok(RowMutationClaim::FirstTouch {
                new_file_positions: None,
            });
        }

        let positions = OwnedRowPositions::new_with(position);
        file.mutation_owners.push(MutationOwner {
            modify_state_id,
            command_id,
            nest_level,
            positions: positions.clone(),
        });
        Ok(RowMutationClaim::FirstTouch {
            new_file_positions: Some(positions),
        })
    }

    pub(crate) fn rollback_to_level(&self, target_level: i32) {
        let mut inner = self.inner.borrow_mut();
        for file in &mut inner.files {
            file.mutation_owners
                .retain(|owner| owner.nest_level < target_level);
        }
        // File paths and IDs deliberately survive rollback. A later scan of
        // the same path receives the same ID; new paths never reuse old IDs.
    }

    pub(crate) fn promote_to_level(&self, from_level: i32) {
        let mut inner = self.inner.borrow_mut();
        for file in &mut inner.files {
            for owner in &mut file.mutation_owners {
                if owner.nest_level >= from_level {
                    owner.nest_level = from_level - 1;
                }
            }
        }
    }

    #[cfg(test)]
    fn registered_file_count(&self) -> usize {
        self.inner.borrow().files.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_ids_are_stable_and_not_reused_after_rollback() {
        let registry = RelationRowRegistry::default();
        let first = registry.register_file("data/a.parquet").unwrap();
        let second = registry.register_file("data/b.parquet").unwrap();
        registry.rollback_to_level(2);

        assert_eq!(registry.register_file("data/a.parquet").unwrap(), first);
        assert_eq!(registry.register_file("data/b.parquet").unwrap(), second);
        assert_eq!(registry.register_file("data/c.parquet").unwrap().raw(), 2);
        assert_eq!(registry.registered_file_count(), 3);
    }

    #[test]
    fn file_registry_enforces_the_17_bit_ctid_limit() {
        let registry = RelationRowRegistry::default();
        {
            let mut inner = registry.inner.borrow_mut();
            let path = Rc::<str>::from("already-registered.parquet");
            inner
                .files
                .resize_with(MAX_ICEBERG_FILES, || RegisteredFile {
                    path: Rc::clone(&path),
                    mutation_owners: Vec::new(),
                });
        }
        assert!(registry.register_file("overflow.parquet").is_err());
    }

    #[test]
    fn owner_bitmap_attributes_duplicates_to_first_command() {
        let registry = RelationRowRegistry::default();
        let file = registry.register_file("data/a.parquet").unwrap();
        let first_owner = registry.begin_modify_state().unwrap();
        let second_owner = registry.begin_modify_state().unwrap();

        let first = registry
            .claim_at_level(first_owner, file, 7, 10, 1)
            .unwrap();
        assert!(matches!(
            first,
            RowMutationClaim::FirstTouch {
                new_file_positions: Some(_)
            }
        ));
        assert!(matches!(
            registry
                .claim_at_level(first_owner, file, 8, 10, 1)
                .unwrap(),
            RowMutationClaim::FirstTouch {
                new_file_positions: None
            }
        ));
        assert!(matches!(
            registry
                .claim_at_level(first_owner, file, 7, 10, 1)
                .unwrap(),
            RowMutationClaim::PreviouslyModified {
                modifying_command_id: 10
            }
        ));
        assert!(matches!(
            registry
                .claim_at_level(second_owner, file, 7, 11, 1)
                .unwrap(),
            RowMutationClaim::PreviouslyModified {
                modifying_command_id: 10
            }
        ));
    }

    #[test]
    fn sibling_owners_can_modify_different_rows_in_one_file() {
        let registry = RelationRowRegistry::default();
        let file = registry.register_file("data/a.parquet").unwrap();
        let first_owner = registry.begin_modify_state().unwrap();
        let second_owner = registry.begin_modify_state().unwrap();

        registry
            .claim_at_level(first_owner, file, 7, 10, 1)
            .unwrap();
        assert!(matches!(
            registry
                .claim_at_level(second_owner, file, 8, 11, 1)
                .unwrap(),
            RowMutationClaim::FirstTouch { .. }
        ));
    }

    #[test]
    fn subtransaction_abort_releases_owner_bitmap_but_keeps_file_id() {
        let registry = RelationRowRegistry::default();
        let file = registry.register_file("data/a.parquet").unwrap();
        let aborted_owner = registry.begin_modify_state().unwrap();
        registry
            .claim_at_level(aborted_owner, file, 7, 10, 2)
            .unwrap();
        registry.rollback_to_level(2);

        let later_owner = registry.begin_modify_state().unwrap();
        assert!(matches!(
            registry
                .claim_at_level(later_owner, file, 7, 11, 1)
                .unwrap(),
            RowMutationClaim::FirstTouch { .. }
        ));
        assert_eq!(registry.register_file("data/a.parquet").unwrap(), file);
    }

    #[test]
    fn released_owner_survives_sibling_abort() {
        let registry = RelationRowRegistry::default();
        let file = registry.register_file("data/a.parquet").unwrap();
        let released_owner = registry.begin_modify_state().unwrap();
        registry
            .claim_at_level(released_owner, file, 7, 10, 2)
            .unwrap();
        registry.promote_to_level(2);

        let aborted_owner = registry.begin_modify_state().unwrap();
        registry
            .claim_at_level(aborted_owner, file, 8, 11, 2)
            .unwrap();
        registry.rollback_to_level(2);

        let later_owner = registry.begin_modify_state().unwrap();
        assert!(matches!(
            registry
                .claim_at_level(later_owner, file, 7, 12, 1)
                .unwrap(),
            RowMutationClaim::PreviouslyModified {
                modifying_command_id: 10
            }
        ));
        assert!(matches!(
            registry
                .claim_at_level(later_owner, file, 8, 12, 1)
                .unwrap(),
            RowMutationClaim::FirstTouch { .. }
        ));
    }
}
