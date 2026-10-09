//! Spillable row storage shared by ModifyTable and COPY trigger owners.

use std::mem;
use std::ptr::NonNull;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use crate::handles::ItemPointer;

use super::TriggerRowId;
use super::layout::TriggerRowLayout;

pub(super) struct TriggerRowStore {
    context: pg_sys::MemoryContext,
    tuple_desc: pg_sys::TupleDesc,
    layout: Option<TriggerRowLayout>,
    tuplestore: Option<NonNull<pg_sys::Tuplestorestate>>,
    read_slot: Option<NonNull<pg_sys::TupleTableSlot>>,
    previous_slot: Option<NonNull<pg_sys::TupleTableSlot>>,
    probe_slot: Option<NonNull<pg_sys::TupleTableSlot>>,
    probe_pointer: i32,
    current_row_id: Option<ItemPointer>,
    previous_row_id: Option<ItemPointer>,
}

impl TriggerRowStore {
    /// # Safety
    ///
    /// `tuple_desc` must remain valid for the query lifetime.
    pub(super) unsafe fn new(tuple_desc: pg_sys::TupleDesc) -> Self {
        debug_assert!(!tuple_desc.is_null());
        Self {
            context: unsafe { pg_sys::CurrentMemoryContext },
            tuple_desc,
            layout: None,
            tuplestore: None,
            read_slot: None,
            previous_slot: None,
            probe_slot: None,
            probe_pointer: 0,
            current_row_id: None,
            previous_row_id: None,
        }
    }

    unsafe fn ensure_initialized(&mut self) {
        if self.tuplestore.is_some() {
            return;
        }
        let layout = unsafe { TriggerRowLayout::new(self.tuple_desc) };
        let tuplestore = NonNull::new(unsafe {
            pg_sys::tuplestore_begin_heap(false, false, pg_sys::work_mem)
        })
        .expect("tuplestore_begin_heap returned NULL");
        unsafe {
            // Pointer 0 marks the last successful fetch. Only the probe
            // pointer advances during lookup, including on-disk reads.
            self.probe_pointer = pg_sys::tuplestore_alloc_read_pointer(
                tuplestore.as_ptr(),
                pg_sys::EXEC_FLAG_REWIND as i32,
            );
            pg_sys::tuplestore_select_read_pointer(
                tuplestore.as_ptr(),
                self.probe_pointer,
            );
            self.read_slot = NonNull::new(layout.make_read_slot());
            self.previous_slot = NonNull::new(layout.make_read_slot());
            self.probe_slot = NonNull::new(layout.make_read_slot());
        }
        self.layout = Some(layout);
        self.tuplestore = Some(tuplestore);
    }

    /// # Safety
    ///
    /// `slot` must be live, relation-shaped for `tuple_desc`, and have all
    /// attributes materialized by the caller. The current PG memory context
    /// must be the owning context captured by `new`.
    pub(super) unsafe fn preserve(
        &mut self,
        row_id: ItemPointer,
        slot: *mut pg_sys::TupleTableSlot,
    ) {
        unsafe {
            self.ensure_initialized();
            self.layout
                .as_ref()
                .expect("trigger row layout is initialized")
                .put(
                    self.tuplestore
                        .expect("trigger tuplestore is initialized")
                        .as_ptr(),
                    TriggerRowId::token(row_id),
                    slot,
                );
        }
    }

    /// Copy a preserved row into the TableAM destination slot.
    ///
    /// Rows are consumed in event order, matching PostgreSQL's FDW
    /// tuplestore. `current` and `previous` allow multiple triggers for the
    /// same event to reuse OLD/NEW without copying between cache slots.
    pub(super) unsafe fn fetch(
        &mut self,
        row_id: ItemPointer,
        destination: *mut pg_sys::TupleTableSlot,
    ) -> bool {
        // Disk reads allocate MinimalTuples in CurrentMemoryContext. Cached
        // read slots must survive the trigger executor's per-tuple resets.
        unsafe {
            PgMemoryContexts::For(self.context)
                .switch_to(|_| self.fetch_in_context(row_id, destination))
        }
    }

    unsafe fn fetch_in_context(
        &mut self,
        row_id: ItemPointer,
        destination: *mut pg_sys::TupleTableSlot,
    ) -> bool {
        let Some(tuplestore) = self.tuplestore else {
            return false;
        };
        let layout = self
            .layout
            .as_ref()
            .expect("trigger row layout is initialized");
        let read_slot = self
            .read_slot
            .expect("trigger tuplestore read slot is initialized");
        let previous_slot = self
            .previous_slot
            .expect("trigger tuplestore previous slot is initialized");

        if self.current_row_id == Some(row_id) {
            unsafe { layout.copy_row(read_slot.as_ptr(), destination) };
            return true;
        }
        if self.previous_row_id == Some(row_id) {
            unsafe { layout.copy_row(previous_slot.as_ptr(), destination) };
            return true;
        }

        let probe_slot = self
            .probe_slot
            .expect("trigger tuplestore probe slot is initialized");
        let target = TriggerRowId::token(row_id);
        // Global tokens increase within each store, but nested execution
        // can leave gaps. Probe the stored tokens, never infer membership
        // from a range or advance another owner's committed read position.
        while unsafe {
            pg_sys::tuplestore_gettupleslot(
                tuplestore.as_ptr(),
                true,
                false,
                probe_slot.as_ptr(),
            )
        } {
            let token = unsafe { TriggerRowLayout::token(probe_slot.as_ptr()) };
            if token < target {
                continue;
            }
            if token > target {
                break;
            }
            unsafe {
                pg_sys::tuplestore_copy_read_pointer(
                    tuplestore.as_ptr(),
                    self.probe_pointer,
                    0,
                );
            }
            // PG fetches UPDATE's OLD before NEW. Keep the two most recent
            // successful fetches, leaving both untouched on a failed probe.
            mem::swap(&mut self.previous_slot, &mut self.read_slot);
            mem::swap(&mut self.read_slot, &mut self.probe_slot);
            self.previous_row_id = self.current_row_id;
            self.current_row_id = Some(row_id);
            unsafe {
                layout.copy_row(probe_slot.as_ptr(), destination);
                // The old previous tuple is now scratch, no longer cached.
                pg_sys::ExecClearTuple(
                    self.probe_slot
                        .expect("trigger tuplestore probe slot is initialized")
                        .as_ptr(),
                );
            }
            return true;
        }

        unsafe {
            pg_sys::ExecClearTuple(probe_slot.as_ptr());
            pg_sys::tuplestore_copy_read_pointer(
                tuplestore.as_ptr(),
                0,
                self.probe_pointer,
            );
        }
        false
    }

    /// Detach PG objects during transaction abort. ResourceOwner and the
    /// owning transaction MemoryContext reclaim files and allocations; late
    /// Rust cleanup must not close or dereference them a second time.
    pub(super) fn abandon(&mut self) {
        self.tuplestore = None;
        self.read_slot = None;
        self.previous_slot = None;
        self.probe_slot = None;
        self.layout = None;
    }

    pub(super) fn close(&mut self) {
        unsafe {
            if let Some(slot) = self.read_slot.take() {
                pg_sys::ExecDropSingleTupleTableSlot(slot.as_ptr());
            }
            if let Some(slot) = self.previous_slot.take() {
                pg_sys::ExecDropSingleTupleTableSlot(slot.as_ptr());
            }
            if let Some(slot) = self.probe_slot.take() {
                pg_sys::ExecDropSingleTupleTableSlot(slot.as_ptr());
            }
            if let Some(tuplestore) = self.tuplestore.take() {
                pg_sys::tuplestore_end(tuplestore.as_ptr());
            }
            if let Some(layout) = self.layout.take() {
                layout.close();
            }
        }
    }
}

impl Drop for TriggerRowStore {
    fn drop(&mut self) {
        self.close();
    }
}
