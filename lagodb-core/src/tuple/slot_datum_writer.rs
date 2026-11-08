//! `SlotDatumWriter`: the output-only substrate for writing a slot's datum arrays.

use std::marker::PhantomData;
use std::slice;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use crate::diag::PgReportError;
use crate::handles::ItemPointer;

use super::row::Row;
use super::row_codec::RowDatumCodec;

/// Per-column slot writer.
///
/// `datum_context` is taken explicitly so the memory-context discipline lives at
/// the call site (the TableAM seqscan passes its per-row `tmp_ctx`; the
/// CustomScan emit path passes the scan node's per-tuple context), never
/// scattered into an access method.
pub struct SlotDatumWriter<'a> {
    /// Mutable views over the slot's `tts_values`/`tts_isnull` C arrays, built
    /// once in `new` for an output-only callback. The base pointers are stable
    /// for the slot's lifetime, so caching the slices here keeps `set_datum` a
    /// plain bounds-checked index instead of rebuilding both views
    /// (`from_raw_parts_mut`) on every per-column write in the scan hot path.
    values: &'a mut [pg_sys::Datum],
    nulls: &'a mut [bool],
    slot: *mut pg_sys::TupleTableSlot,
    datum_context: pg_sys::MemoryContext,
    _marker: PhantomData<&'a mut pg_sys::TupleTableSlot>,
}

impl<'a> SlotDatumWriter<'a> {
    /// # Safety
    ///
    /// `slot` must be a valid, initialized slot with a non-NULL tuple
    /// descriptor; `datum_context` must be the context the caller wants varlena
    /// datums palloc'd into. The slice width is derived only from that live
    /// descriptor, never from provider-supplied metadata.
    pub unsafe fn new(
        slot: *mut pg_sys::TupleTableSlot,
        datum_context: pg_sys::MemoryContext,
    ) -> Self {
        let tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
        let natts = unsafe { (*tuple_desc).natts as usize };
        // SAFETY: the caller guarantees `slot` and its descriptor are valid,
        // so `tts_values`/`tts_isnull` are each a live array of `natts`
        // elements. These arrays are stable for the slot's lifetime
        // (allocated at slot init, never reallocated by `ExecClearTuple` /
        // `ExecStoreVirtualTuple`), and this output callback is the sole
        // accessor of them while `SlotDatumWriter` is alive, so holding `&mut`
        // views for `'a` does not alias any other live reference. This type
        // never calls a PostgreSQL slot deform API while those views live;
        // modify callbacks use `ModifySlotBuffer` for that input/output case.
        unsafe {
            Self {
                values: slice::from_raw_parts_mut((*slot).tts_values, natts),
                nulls: slice::from_raw_parts_mut((*slot).tts_isnull, natts),
                slot,
                datum_context,
                _marker: PhantomData,
            }
        }
    }

    pub fn natts(&self) -> usize {
        self.values.len()
    }

    /// Set the tuple identity carried by this slot.
    pub fn set_tid(&mut self, tid: &ItemPointer) {
        // SAFETY: `self.slot` was validated by `SlotDatumWriter::new`; `tts_tid`
        // is part of the same live slot and is not aliased through the cached
        // values/nulls slices.
        unsafe {
            (*self.slot).tts_tid = tid.to_pg_sys();
        }
    }

    /// Set the physical relation OID carried by this slot.
    pub fn set_table_oid(&mut self, table_oid: pg_sys::Oid) {
        // SAFETY: `self.slot` is live for the lifetime of this writer.
        unsafe { (*self.slot).tts_tableOid = table_oid };
    }

    /// Write column `index`; `None` denotes SQL NULL. This is the output
    /// writer for the cached `tts_values`/`tts_isnull` views.
    ///
    /// Positions this method never touches are **not** guaranteed SQL NULL by
    /// the slot itself: `ExecClearTuple` does not reset `tts_isnull`, and the
    /// slot's `tts_values`/`tts_isnull` arrays are `palloc0`'d (so they start
    /// non-NULL; only `ExecStoreAllNullTuple` sets them all NULL). PostgreSQL
    /// reads every attribute, dropped ones included, when it forms a whole-row
    /// datum or copies the slot into a HeapTuple. The adapter that owns the
    /// slot must therefore make every position its layout never writes read
    /// as NULL before rows are produced.
    ///
    /// `index` must be `< natts`. This is an always-on assertion rather than a
    /// `debug_assert!`: as a shared data-path substrate the bound is a domain
    /// invariant, and a release-mode out-of-range write would otherwise still
    /// trip an opaque slice-bounds panic — a named check fails with a meaningful
    /// message instead.
    pub fn set_datum(&mut self, index: usize, value: Option<pg_sys::Datum>) {
        assert!(index < self.values.len(), "slot column index out of range");
        match value {
            Some(datum) => {
                self.values[index] = datum;
                self.nulls[index] = false;
            }
            None => self.nulls[index] = true,
        }
    }

    /// Write column `index` after the decoder has validated the destination
    /// against the same slot descriptor at its planning boundary.
    ///
    /// # Safety
    ///
    /// `index` must be less than `natts`. `self` must be the output writer for
    /// the slot whose descriptor was used to validate the index.
    #[inline]
    pub unsafe fn set_datum_unchecked(
        &mut self,
        index: usize,
        value: Option<pg_sys::Datum>,
    ) {
        debug_assert!(index < self.values.len());
        unsafe {
            match value {
                Some(datum) => {
                    *self.values.get_unchecked_mut(index) = datum;
                    *self.nulls.get_unchecked_mut(index) = false;
                }
                None => *self.nulls.get_unchecked_mut(index) = true,
            }
        }
    }

    /// Row-world bridge: materialize an owned [`Row`] into the slot. Cells are
    /// converted under `datum_context` and written into the slot arrays by the
    /// bound row codec; the caller (core scan shim) owns the single
    /// `ExecStoreVirtualTuple`.
    ///
    /// # Safety
    ///
    /// `codec` must have been bound to the same tuple descriptor as this slot.
    /// Its target OIDs and width must match the slot attributes. Missing row
    /// positions are written as SQL NULL.
    pub(crate) unsafe fn fill_from_row(
        &mut self,
        row: &mut Row,
        codec: &RowDatumCodec,
    ) -> Result<(), PgReportError> {
        let natts = self.values.len();
        unsafe {
            PgMemoryContexts::For(self.datum_context).switch_to(|_| {
                let cells = (0..natts).map(|index| row.take_cell(index));
                codec
                    .cells_to_datums(cells, self.values, self.nulls)
                    .map_err(PgReportError::from_domain_error)
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Owns the backing arrays a [`SlotDatumWriter`] writes through. The
    /// buffers never reallocate after construction, so the raw pointers handed
    /// to the slot stay valid for the slot's lifetime.
    struct HostSlot {
        slot: pg_sys::TupleTableSlot,
        tuple_desc: Box<pg_sys::TupleDescData>,
        values: Vec<pg_sys::Datum>,
        nulls: Vec<bool>,
    }

    impl HostSlot {
        fn new(natts: usize) -> Box<Self> {
            // `nulls` starts all-true so the test can assert the writer flips
            // only the positions it actually writes. This is the test harness's
            // own initial state, not what a PostgreSQL slot starts with.
            let mut tuple_desc: Box<pg_sys::TupleDescData> =
                Box::new(unsafe { std::mem::zeroed() });
            tuple_desc.natts = natts as i32;
            let mut boxed = Box::new(HostSlot {
                slot: unsafe { std::mem::zeroed() },
                tuple_desc,
                values: vec![pg_sys::Datum::from(0usize); natts],
                nulls: vec![true; natts],
            });
            boxed.slot.tts_values = boxed.values.as_mut_ptr();
            boxed.slot.tts_isnull = boxed.nulls.as_mut_ptr();
            boxed.slot.tts_tupleDescriptor = &mut *boxed.tuple_desc;
            boxed
        }

        fn columns(&mut self) -> SlotDatumWriter<'_> {
            unsafe { SlotDatumWriter::new(&mut self.slot, std::ptr::null_mut()) }
        }
    }

    #[test]
    fn set_datum_writes_only_the_given_positions() {
        let mut host = HostSlot::new(3);
        {
            let mut cols = host.columns();
            cols.set_datum(0, Some(pg_sys::Datum::from(42usize)));
            cols.set_datum(1, None);
            // Index 2 is intentionally never written.
        }

        assert_eq!(host.nulls, vec![false, true, true]);
        assert_eq!(host.values[0].value() as i64, 42);
    }
}
