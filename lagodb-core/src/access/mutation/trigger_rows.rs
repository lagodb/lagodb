//! Query-scoped tuple preservation for PostgreSQL AFTER ROW triggers.
//!
//! PostgreSQL normally queues a table-AM row identity and later calls
//! `tuple_fetch_row_version`. LagoDB already has the complete OLD/NEW row in
//! `nodeModifyTable`, so the core Modify query state preserves those rows in
//! one spillable tuplestore per `(access method, relation)`.
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
use std::ptr::NonNull;
use std::rc::{Rc, Weak};

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::api::{AmResult, TRIGGER_ROW_BLOCK_BASE, TableAccessMethod};
use crate::diag::PgReportError;
use crate::handles::ItemPointer;

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
}

thread_local! {
    static NEXT_TRIGGER_ROW_TOKEN: Cell<u64> = const { Cell::new(0) };
}

struct TriggerRowStore {
    tuple_desc: pg_sys::TupleDesc,
    tuplestore: Option<NonNull<pg_sys::Tuplestorestate>>,
    read_slot: Option<NonNull<pg_sys::TupleTableSlot>>,
    previous_slot: Option<NonNull<pg_sys::TupleTableSlot>>,
    row_ids: Vec<ItemPointer>,
    next_read: usize,
    current_row_id: Option<ItemPointer>,
    previous_row_id: Option<ItemPointer>,
}

impl TriggerRowStore {
    /// # Safety
    ///
    /// `tuple_desc` must remain valid for the query lifetime.
    unsafe fn new(tuple_desc: pg_sys::TupleDesc) -> Self {
        debug_assert!(!tuple_desc.is_null());
        Self {
            tuple_desc,
            tuplestore: None,
            read_slot: None,
            previous_slot: None,
            row_ids: Vec::new(),
            next_read: 0,
            current_row_id: None,
            previous_row_id: None,
        }
    }

    unsafe fn ensure_initialized(&mut self) {
        if self.tuplestore.is_some() {
            return;
        }
        let tuplestore = NonNull::new(unsafe {
            pg_sys::tuplestore_begin_heap(false, false, pg_sys::work_mem)
        })
        .expect("tuplestore_begin_heap returned NULL");
        let read_slot = NonNull::new(unsafe {
            pg_sys::MakeSingleTupleTableSlot(
                self.tuple_desc,
                &pg_sys::TTSOpsMinimalTuple,
            )
        })
        .expect("MakeSingleTupleTableSlot returned NULL");
        let previous_slot = NonNull::new(unsafe {
            pg_sys::MakeSingleTupleTableSlot(
                self.tuple_desc,
                &pg_sys::TTSOpsMinimalTuple,
            )
        })
        .expect("MakeSingleTupleTableSlot returned NULL");
        self.tuplestore = Some(tuplestore);
        self.read_slot = Some(read_slot);
        self.previous_slot = Some(previous_slot);
    }

    /// # Safety
    ///
    /// `slot` must be live and relation-shaped for `tuple_desc`.
    unsafe fn preserve(
        &mut self,
        row_id: ItemPointer,
        slot: *mut pg_sys::TupleTableSlot,
    ) {
        unsafe { self.ensure_initialized() };
        unsafe {
            pg_sys::slot_getallattrs(slot);
            pg_sys::tuplestore_putvalues(
                self.tuplestore
                    .expect("trigger tuplestore is initialized")
                    .as_ptr(),
                self.tuple_desc,
                (*slot).tts_values,
                (*slot).tts_isnull,
            );
        }
        self.row_ids.push(row_id);
    }

    /// Copy a preserved row into the TableAM destination slot.
    ///
    /// Rows are consumed in event order, matching PostgreSQL's FDW
    /// tuplestore. `current` and `previous` allow multiple triggers for the
    /// same event to reuse one materialized tuple.
    unsafe fn fetch(
        &mut self,
        row_id: ItemPointer,
        destination: *mut pg_sys::TupleTableSlot,
    ) -> bool {
        let Some(tuplestore) = self.tuplestore else {
            return false;
        };
        let read_slot = self
            .read_slot
            .expect("trigger tuplestore read slot is initialized");
        let previous_slot = self
            .previous_slot
            .expect("trigger tuplestore previous slot is initialized");

        if self.current_row_id == Some(row_id) {
            unsafe { pg_sys::ExecCopySlot(destination, read_slot.as_ptr()) };
            return true;
        }
        if self.previous_row_id == Some(row_id) {
            unsafe { pg_sys::ExecCopySlot(destination, previous_slot.as_ptr()) };
            return true;
        }

        let Some(relative_index) = self.row_ids[self.next_read..]
            .iter()
            .position(|id| *id == row_id)
        else {
            return false;
        };
        let target_index = self.next_read + relative_index;
        while self.next_read <= target_index {
            if self.current_row_id.is_some() {
                unsafe {
                    pg_sys::ExecCopySlot(previous_slot.as_ptr(), read_slot.as_ptr())
                };
                self.previous_row_id = self.current_row_id;
            }
            let found = unsafe {
                pg_sys::tuplestore_gettupleslot(
                    tuplestore.as_ptr(),
                    true,
                    false,
                    read_slot.as_ptr(),
                )
            };
            if !found {
                return false;
            }
            self.current_row_id = Some(self.row_ids[self.next_read]);
            self.next_read += 1;
        }
        unsafe { pg_sys::ExecCopySlot(destination, read_slot.as_ptr()) };
        true
    }
}

impl Drop for TriggerRowStore {
    fn drop(&mut self) {
        unsafe {
            if let Some(slot) = self.read_slot.take() {
                pg_sys::ExecDropSingleTupleTableSlot(slot.as_ptr());
            }
            if let Some(slot) = self.previous_slot.take() {
                pg_sys::ExecDropSingleTupleTableSlot(slot.as_ptr());
            }
            if let Some(tuplestore) = self.tuplestore.take() {
                pg_sys::tuplestore_end(tuplestore.as_ptr());
            }
        }
    }
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
    stores: HashMap<StoreKey, Rc<RefCell<TriggerRowStore>>>,
}

impl TriggerQueryState {
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
        let store = if let Some(store) = self.stores.get(&key) {
            Rc::clone(store)
        } else {
            let store =
                Rc::new(RefCell::new(unsafe { TriggerRowStore::new(tuple_desc) }));
            ACTIVE_STORES.with_borrow_mut(|stores| {
                stores.push(ActiveStore {
                    key,
                    store: Rc::downgrade(&store),
                });
            });
            self.stores.insert(key, Rc::clone(&store));
            store
        };

        let row_id = TriggerRowId::allocate()?;
        let mut store = store.try_borrow_mut().map_err(|_| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "trigger-row store is already borrowed",
            )
        })?;
        unsafe { store.preserve(row_id, slot) };
        Ok(row_id)
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
    ACTIVE_STORES.with_borrow_mut(|stores| {
        stores.retain(|active| active.store.strong_count() > 0);
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
