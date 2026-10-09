//! Query-scoped tuple preservation for PostgreSQL AFTER ROW triggers.
//!
//! PostgreSQL normally queues a table-AM row identity and later calls
//! `tuple_fetch_row_version`. ModifyTable query state and COPY relation
//! sessions retain complete rows in spillable tuplestores, using the same
//! temporary identity namespace and fetch routing.
//!
//! TODO(pg-trigger-materialization-hook): this extension-only implementation
//! must preserve a row before PostgreSQL's `TriggerEnabled` decision, because
//! that decision lives in `trigger.c` and there is no TableAM materialization
//! hook. Consequently disabled or `WHEN=false` AFTER ROW triggers can still
//! incur one tuple copy. Moving the decision boundary requires a
//! PostgreSQL-core hook and is deliberately out of scope here.

use std::any::TypeId;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::api::{AmResult, TRIGGER_ROW_BLOCK_BASE, TableAccessMethod};
use crate::diag::PgReportError;
use crate::handles::ItemPointer;

use self::store::TriggerRowStore;

mod layout;
mod store;

/// Core-reserved top quarter of the ItemPointer block-number space.
///
/// A 47-bit physical payload encoded with 65535 usable offsets reaches a
/// little above `0x80000000`, so reserving only the high bit would overlap its
/// upper boundary. `0xC0000000` preserves the complete physical range while
/// leaving roughly 46 bits for trigger tokens.
const OFFSET_BASE: u64 = u16::MAX as u64;
const TRIGGER_ROW_BLOCK_COUNT: u64 =
    (u32::MAX as u64) - (TRIGGER_ROW_BLOCK_BASE as u64) + 1;
const MAX_TRIGGER_ROW_TOKEN: u64 = TRIGGER_ROW_BLOCK_COUNT * OFFSET_BASE;

/// Core-owned codec for trigger-only row identities.
///
/// Core's reserved block-number range separates this namespace from the
/// 47-bit physical identities used by LagoDB providers. Tokens are
/// backend-global and monotonic, so nested queries and sibling ModifyTable
/// nodes cannot generate the same temporary identity.
struct TriggerRowId;

impl TriggerRowId {
    fn allocate() -> AmResult<ItemPointer> {
        NEXT_TRIGGER_ROW_TOKEN.with(|next| {
            let token = next.get();
            if token >= MAX_TRIGGER_ROW_TOKEN {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                    "LagoDB AFTER-trigger row identity space was exhausted",
                ));
            }
            next.set(token + 1);
            let block_offset = u32::try_from(token / OFFSET_BASE).map_err(|_| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "trigger-row block number overflow",
                )
            })?;
            let block_number = TRIGGER_ROW_BLOCK_BASE
                .checked_add(block_offset)
                .ok_or_else(|| {
                    PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        "trigger-row block number overflow",
                    )
                })?;
            let offset = u16::try_from((token % OFFSET_BASE) + 1).map_err(|_| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "trigger-row offset overflow",
                )
            })?;
            Ok(ItemPointer {
                block_number,
                offset,
            })
        })
    }

    fn is_encoded(row_id: ItemPointer) -> bool {
        row_id.offset != 0 && row_id.block_number >= TRIGGER_ROW_BLOCK_BASE
    }

    /// Decode an identity allocated by this codec, preserving token order.
    fn token(row_id: ItemPointer) -> u64 {
        u64::from(row_id.block_number - TRIGGER_ROW_BLOCK_BASE) * OFFSET_BASE
            + u64::from(row_id.offset - 1)
    }
}

thread_local! {
    static NEXT_TRIGGER_ROW_TOKEN: Cell<u64> = const { Cell::new(0) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct StoreKey {
    access_method: TypeId,
    relation_oid: pg_sys::Oid,
}

struct ActiveStore {
    key: StoreKey,
    store: Weak<RefCell<TriggerRowStore>>,
}

thread_local! {
    static ACTIVE_STORES: RefCell<Vec<ActiveStore>> = const {
        RefCell::new(Vec::new())
    };
}

/// Core-owned trigger rows shared by all ModifyTable nodes in one executor
/// query. Provider business state remains separate in `AmModifyQueryState`.
#[derive(Default)]
pub(crate) struct TriggerQueryState {
    stores: HashMap<StoreKey, TriggerRelationRows>,
}

impl TriggerQueryState {
    /// Discard this query's rows during exceptional executor teardown.
    /// PG owns resource reclamation; detach every store before dropping its
    /// handle so normal Drop cannot close an already released spill file.
    pub(crate) fn abandon(&mut self) {
        for rows in self.stores.values_mut() {
            rows.abandon();
        }
        // Dropping the handles also unregisters their exact store identities.
        // Shared ModifyTable nodes can repeat this cleanup on the empty map.
        self.stores.clear();
    }

    /// # Safety
    ///
    /// `tuple_desc` and `slot` must remain valid for the query/call as required
    /// by PostgreSQL's executor contracts.
    pub(crate) unsafe fn preserve<A: TableAccessMethod>(
        &mut self,
        relation_oid: pg_sys::Oid,
        tuple_desc: pg_sys::TupleDesc,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> AmResult<ItemPointer> {
        let key = StoreKey {
            access_method: TypeId::of::<A>(),
            relation_oid,
        };
        let rows = self.stores.entry(key).or_insert_with(|| unsafe {
            TriggerRelationRows::new::<A>(relation_oid, tuple_desc)
        });
        unsafe { pg_sys::slot_getallattrs(slot) };
        unsafe { rows.preserve(slot) }
    }
}

/// Bound relation handle. COPY retains this directly, without a per-row
/// relation lookup; ModifyTable shares handles through its query owner.
pub(super) struct TriggerRelationRows {
    store: Rc<RefCell<TriggerRowStore>>,
}

impl TriggerRelationRows {
    /// # Safety
    ///
    /// The current memory context and descriptor must outlive this handle.
    /// Abort owners must call `abandon` before PG reclaims those allocations.
    pub(super) unsafe fn new<A: TableAccessMethod>(
        relation_oid: pg_sys::Oid,
        tuple_desc: pg_sys::TupleDesc,
    ) -> Self {
        let store =
            Rc::new(RefCell::new(unsafe { TriggerRowStore::new(tuple_desc) }));
        ACTIVE_STORES.with_borrow_mut(|stores| {
            stores.push(ActiveStore {
                key: StoreKey {
                    access_method: TypeId::of::<A>(),
                    relation_oid,
                },
                store: Rc::downgrade(&store),
            });
        });
        Self { store }
    }

    /// # Safety
    ///
    /// `slot` must be live, fully materialized, and shaped for the bound
    /// relation. The caller must use the store's memory context when lazily
    /// initializing its PG objects. COPY's TupleSlotRow already materializes
    /// its attributes, so preservation does not deform that slot again.
    pub(super) unsafe fn preserve(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> AmResult<ItemPointer> {
        let row_id = TriggerRowId::allocate()?;
        let mut store = self.store.try_borrow_mut().map_err(|_| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "trigger-row store is already borrowed",
            )
        })?;
        unsafe { store.preserve(row_id, slot) };
        Ok(row_id)
    }

    pub(super) fn abandon(&mut self) {
        self.store.borrow_mut().abandon();
    }

    pub(super) fn finish(&mut self) {
        self.store.borrow_mut().close();
    }
}

impl Drop for TriggerRelationRows {
    fn drop(&mut self) {
        // Unregister by store identity: nested COPY/queries can retain
        // separate stores for the same relation. This only releases Rust
        // registry entries; the owner decides how to close or abandon PG data.
        let pointer = Rc::as_ptr(&self.store);
        ACTIVE_STORES.with_borrow_mut(|stores| {
            stores.retain(|active| active.store.as_ptr() != pointer);
        });
    }
}

pub(crate) enum FetchResult {
    PhysicalRow,
    Found,
    Missing,
}

/// Route a temporary row identity to its exact live query-level store.
///
/// Global monotonic trigger IDs prevent namespace collisions. If a tagged ID
/// is absent, callers must report an invariant failure rather than falling
/// through to a provider's physical fetch callback.
pub(crate) unsafe fn fetch<A: TableAccessMethod>(
    relation_oid: pg_sys::Oid,
    row_id: ItemPointer,
    destination: *mut pg_sys::TupleTableSlot,
) -> FetchResult {
    if !TriggerRowId::is_encoded(row_id) {
        return FetchResult::PhysicalRow;
    }

    let key = StoreKey {
        access_method: TypeId::of::<A>(),
        relation_oid,
    };
    ACTIVE_STORES.with_borrow(|stores| {
        for active in stores.iter().rev().filter(|active| active.key == key) {
            let Some(store) = active.store.upgrade() else {
                continue;
            };
            let Ok(mut store) = store.try_borrow_mut() else {
                return FetchResult::Missing;
            };
            if unsafe { store.fetch(row_id, destination) } {
                return FetchResult::Found;
            }
        }
        FetchResult::Missing
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_row_codec_uses_a_disjoint_monotonic_namespace() {
        let first = TriggerRowId::allocate().unwrap();
        let second = TriggerRowId::allocate().unwrap();
        assert!(TriggerRowId::is_encoded(first));
        assert!(TriggerRowId::is_encoded(second));
        assert_ne!(first, second);
        assert!(!TriggerRowId::is_encoded(ItemPointer {
            block_number: 0,
            offset: 1,
        }));
    }
}
