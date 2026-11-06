//! Private tuplestore records: a trigger token followed by relation columns.
//! Both the identity and row data participate in PostgreSQL's spill policy.

use std::ptr;

use pgrx::pg_sys;

/// Fixed per-store workspace, allocated in the trigger owner's PG context.
/// No Drop: the store explicitly closes it on success or abandons it on abort.
pub(super) struct TriggerRowLayout {
    descriptor: pg_sys::TupleDesc,
    input: *mut pg_sys::TupleTableSlot,
    output: *mut pg_sys::TupleTableSlot,
    natts: usize,
}

impl TriggerRowLayout {
    /// The relation descriptor must outlive the workspace. All allocations
    /// belong to the current memory context, as do the store's read slots.
    pub(super) unsafe fn new(relation_desc: pg_sys::TupleDesc) -> Self {
        unsafe {
            let natts = (*relation_desc).natts;
            let descriptor = pg_sys::CreateTemplateTupleDesc(natts + 1);
            pg_sys::TupleDescInitEntry(
                descriptor,
                1,
                c"trigger_token".as_ptr(),
                pg_sys::INT8OID,
                -1,
                0,
            );
            for attno in 1..=natts {
                // Copy dropped-column/type metadata too, while resetting
                // physical offsets for the extra leading attribute.
                pg_sys::TupleDescCopyEntry(
                    descriptor,
                    (attno + 1) as pg_sys::AttrNumber,
                    relation_desc,
                    attno as pg_sys::AttrNumber,
                );
            }
            Self {
                descriptor,
                input: pg_sys::MakeSingleTupleTableSlot(
                    descriptor,
                    &pg_sys::TTSOpsVirtual,
                ),
                output: pg_sys::MakeSingleTupleTableSlot(
                    relation_desc,
                    &pg_sys::TTSOpsVirtual,
                ),
                natts: natts as usize,
            }
        }
    }

    pub(super) unsafe fn make_read_slot(&self) -> *mut pg_sys::TupleTableSlot {
        unsafe {
            pg_sys::MakeSingleTupleTableSlot(
                self.descriptor,
                &pg_sys::TTSOpsMinimalTuple,
            )
        }
    }

    /// The caller has already materialized all relation attributes in `row`.
    pub(super) unsafe fn put(
        &self,
        store: *mut pg_sys::Tuplestorestate,
        token: u64,
        row: *mut pg_sys::TupleTableSlot,
    ) {
        unsafe {
            *(*self.input).tts_values = pg_sys::Datum::from(token);
            *(*self.input).tts_isnull = false;
            ptr::copy_nonoverlapping(
                (*row).tts_values,
                (*self.input).tts_values.add(1),
                self.natts,
            );
            ptr::copy_nonoverlapping(
                (*row).tts_isnull,
                (*self.input).tts_isnull.add(1),
                self.natts,
            );
            pg_sys::tuplestore_putvalues(
                store,
                self.descriptor,
                (*self.input).tts_values,
                (*self.input).tts_isnull,
            );
        }
    }

    pub(super) unsafe fn token(row: *mut pg_sys::TupleTableSlot) -> u64 {
        unsafe {
            // Probing skipped rows only needs the leading, non-null token.
            pg_sys::slot_getsomeattrs(row, 1);
            pg_sys::DatumGetInt64(*(*row).tts_values) as u64
        }
    }

    pub(super) unsafe fn copy_row(
        &self,
        row: *mut pg_sys::TupleTableSlot,
        destination: *mut pg_sys::TupleTableSlot,
    ) {
        unsafe {
            pg_sys::slot_getallattrs(row);
            pg_sys::ExecClearTuple(self.output);
            ptr::copy_nonoverlapping(
                (*row).tts_values.add(1),
                (*self.output).tts_values,
                self.natts,
            );
            ptr::copy_nonoverlapping(
                (*row).tts_isnull.add(1),
                (*self.output).tts_isnull,
                self.natts,
            );
            pg_sys::ExecStoreVirtualTuple(self.output);
            // ExecCopySlot requires matching descriptors. The relation-shaped
            // virtual slot hides our token; the destination owns its copy.
            pg_sys::ExecCopySlot(destination, self.output);
            pg_sys::ExecClearTuple(self.output);
        }
    }

    /// Read slots using our private descriptor must already have been dropped.
    pub(super) unsafe fn close(self) {
        unsafe {
            pg_sys::ExecDropSingleTupleTableSlot(self.input);
            pg_sys::ExecDropSingleTupleTableSlot(self.output);
            pg_sys::FreeTupleDesc(self.descriptor);
        }
    }
}
