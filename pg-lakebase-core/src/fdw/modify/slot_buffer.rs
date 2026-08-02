//! Input/output slot access for foreign modify callbacks.

use core::ffi::c_int;

use pgrx::pg_sys;

use super::row_layout::ModifyRowLayout;

/// Callback-scoped slot buffer that does not hold Rust references to PG slot
/// arrays across calls into PostgreSQL.
pub(crate) struct ModifySlotBuffer {
    slot: *mut pg_sys::TupleTableSlot,
    values: *mut pg_sys::Datum,
    nulls: *mut bool,
    natts: usize,
    target_ctx: pg_sys::MemoryContext,
}

impl ModifySlotBuffer {
    /// # Safety
    ///
    /// `slot` must remain live for the lifetime of the returned buffer, and
    /// its descriptor must have the same attribute count as `layout`.
    /// `target_ctx` must be a live PostgreSQL memory context. The PostgreSQL
    /// modify callback contract supplies the row-type compatibility. The caller
    /// must not create Rust references to the arrays while this buffer is used:
    /// PostgreSQL may deform the slot in `slot_getattr`, and the buffer only
    /// accesses the arrays through immediate raw-pointer operations after
    /// that call returns.
    #[inline]
    pub(crate) unsafe fn from_raw(
        slot: *mut pg_sys::TupleTableSlot,
        layout: &ModifyRowLayout,
        target_ctx: pg_sys::MemoryContext,
    ) -> Self {
        // SAFETY: the modify callback contract supplies a live, relation-shaped
        // slot matching the Begin-time layout and conversion context.
        unsafe { Self::from_raw_parts_unchecked(slot, layout, target_ctx) }
    }

    /// Build the returned-row buffer used by PostgreSQL's DELETE callback.
    /// The callback supplies a relation-shaped slot that may be empty; this
    /// constructor records its arrays and resets them before the row view is
    /// exposed.
    ///
    /// # Safety
    ///
    /// `slot` and `layout` must be live for the callback, and `target_ctx` must
    /// be a live PostgreSQL memory context. The returned slot must be the
    /// relation-shaped slot supplied by PostgreSQL for this DELETE callback.
    #[inline]
    pub(crate) unsafe fn from_delete_raw(
        slot: *mut pg_sys::TupleTableSlot,
        layout: &ModifyRowLayout,
        target_ctx: pg_sys::MemoryContext,
    ) -> Self {
        // SAFETY: PostgreSQL supplies the relation-shaped DELETE returning
        // slot, which may be empty before this callback initializes it.
        let buffer =
            unsafe { Self::from_raw_parts_unchecked(slot, layout, target_ctx) };
        // SAFETY: `from_raw_parts_unchecked` requires the live relation-shaped
        // slot and its arrays before PostgreSQL clears the row representation.
        unsafe { pg_sys::ExecStoreAllNullTuple(slot) };
        buffer
    }

    /// # Safety
    ///
    /// `slot` and `layout` must be live callback objects with matching relation
    /// widths and valid Datum arrays. `target_ctx` must be a live PostgreSQL
    /// memory context. The slot may be empty. The caller has already established
    /// these invariants from PostgreSQL's modify callback contract, so this
    /// constructor only records raw views.
    unsafe fn from_raw_parts_unchecked(
        slot: *mut pg_sys::TupleTableSlot,
        layout: &ModifyRowLayout,
        target_ctx: pg_sys::MemoryContext,
    ) -> Self {
        Self {
            slot,
            values: unsafe { (*slot).tts_values },
            nulls: unsafe { (*slot).tts_isnull },
            natts: layout.natts(),
            target_ctx,
        }
    }

    #[inline]
    pub(crate) fn as_raw(&self) -> *mut pg_sys::TupleTableSlot {
        self.slot
    }

    #[inline]
    pub(crate) fn natts(&self) -> usize {
        self.natts
    }

    #[inline]
    pub(crate) fn target_context(&self) -> pg_sys::MemoryContext {
        self.target_ctx
    }

    /// Deform the input slot up to the provider-supplied `index`. PostgreSQL
    /// still owns the lazy extraction work; the unsafe caller contract supplies
    /// the relation-index invariant without a duplicate Rust-side range check.
    ///
    /// # Safety
    ///
    /// `index` must be less than `self.natts`, and the slot/arrays must satisfy
    /// the constructor contract.
    #[inline]
    unsafe fn deform_unchecked(&self, index: usize) {
        // `natts` comes from PostgreSQL's TupleDesc and is bounded by the
        // c_int attribute-number representation.
        let attno = (index + 1) as c_int;
        let mut is_null = false;
        unsafe {
            let _ = pg_sys::slot_getattr(self.slot, attno, &mut is_null);
        }
    }

    /// Load a datum whose relation index is covered by the caller contract.
    ///
    /// # Safety
    ///
    /// `index` must be less than `self.natts`; the slot must remain live, and
    /// no Rust reference may alias its arrays while PostgreSQL deforms it.
    #[inline]
    pub(crate) unsafe fn load_datum_unchecked(
        &self,
        index: usize,
    ) -> (pg_sys::Datum, bool) {
        unsafe { self.deform_unchecked(index) };
        // SAFETY: the constructor contract establishes both arrays, and the
        // caller contract establishes the index. No Rust slice or reference to these
        // arrays is live here.
        unsafe { (self.values.add(index).read(), self.nulls.add(index).read()) }
    }

    /// Write a datum after PostgreSQL has completed any lazy slot deformation.
    ///
    /// # Safety
    ///
    /// `index` must be less than `self.natts`; the slot must remain live, and
    /// no Rust reference may alias its arrays while PostgreSQL deforms it.
    #[inline]
    pub(crate) unsafe fn set_datum_after_deform_unchecked(
        &self,
        index: usize,
        value: Option<pg_sys::Datum>,
    ) {
        unsafe { self.deform_unchecked(index) };
        unsafe { self.write_datum_unchecked(index, value) };
    }

    #[inline]
    unsafe fn write_datum_unchecked(
        &self,
        index: usize,
        value: Option<pg_sys::Datum>,
    ) {
        // SAFETY: the constructor contract covers both arrays and the caller
        // contract covers the index used by this private primitive.
        unsafe {
            match value {
                Some(datum) => {
                    self.values.add(index).write(datum);
                    self.nulls.add(index).write(false);
                }
                None => self.nulls.add(index).write(true),
            }
        }
    }

    /// Write a datum after Begin established the relation index and completed
    /// any required slot initialization.
    ///
    /// # Safety
    ///
    /// `index` must be less than the slot's attribute count, and the slot's
    /// arrays must remain valid for the duration of this call.
    #[inline]
    pub(crate) unsafe fn set_datum_without_deform_unchecked(
        &self,
        index: usize,
        value: Option<pg_sys::Datum>,
    ) {
        // SAFETY: the slot arrays are covered by the constructor contract; the
        // caller has already completed any PostgreSQL deform operation for this
        // slot.
        unsafe { self.write_datum_unchecked(index, value) };
    }

    /// Deform a complete relation-shaped heap tuple directly into the slot
    /// arrays.
    ///
    /// # Safety
    ///
    /// `tuple` and `tuple_desc` must be valid, and `tuple_desc` must describe
    /// exactly `self.natts` attributes compatible with this slot.
    #[inline]
    pub(crate) unsafe fn deform_heap_tuple_unchecked(
        &self,
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
    ) {
        unsafe {
            pg_sys::heap_deform_tuple(tuple, tuple_desc, self.values, self.nulls);
        }
    }
}
