//! Provider-facing modify slot and value API.

use core::ffi::c_int;
use core::mem::MaybeUninit;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use crate::diag::PgReportError;
use crate::handles::ValidItemPointer;
use crate::tuple::{Cell, PgDatumRef};
use crate::wrapper::PgWrapper;

use super::error::ForeignModifyError;
use super::return_layout::ForeignModifyReturnColumn;
use super::row_layout::ModifyRowLayout;
use super::slot_buffer::ModifySlotBuffer;

/// Mutable, relation-shaped view of a slot passed to a modify callback.
pub struct ModifySlot<'slot> {
    columns: ModifySlotBuffer,
    layout: &'slot mut ModifyRowLayout,
    relation_oid: pg_sys::Oid,
    returned_item_pointer_required: bool,
    returned_item_pointer: Option<ValidItemPointer>,
    representation_dirty: bool,
    slot_arrays_complete: bool,
}

impl<'slot> ModifySlot<'slot> {
    /// # Safety
    ///
    /// `slot` and `layout` are live executor objects for the duration of the
    /// provider callback, and the layout was built for this slot's relation.
    /// Semantic codecs are bound lazily per target column;
    /// raw-Datum providers and SQL NULL writes therefore do not incur an
    /// unrelated column's semantic capability check.
    /// `conversion_context` is the context in which provider conversions that
    /// write pass-by-reference datums must allocate. When
    /// `returned_item_pointer_required` is true, `slot` must support storing a
    /// heap tuple.
    pub(crate) unsafe fn from_raw(
        slot: *mut pg_sys::TupleTableSlot,
        conversion_context: pg_sys::MemoryContext,
        layout: &'slot mut ModifyRowLayout,
        returned_item_pointer_required: bool,
    ) -> Self {
        let relation_oid = layout.relation_oid();
        Self {
            columns: unsafe {
                ModifySlotBuffer::from_raw(slot, layout, conversion_context)
            },
            layout,
            relation_oid,
            returned_item_pointer_required,
            returned_item_pointer: None,
            representation_dirty: false,
            slot_arrays_complete: false,
        }
    }

    /// Build a returned-row view for PostgreSQL's DELETE callback. The slot
    /// supplied by PostgreSQL is intentionally empty; initialize it only when
    /// a DELETE consumer needs a provider/framework-owned returned row.
    ///
    /// # Safety
    ///
    /// `slot`, `layout`, and `conversion_context` must be live callback
    /// objects. The supplied slot may be empty; PostgreSQL's DELETE callback
    /// contract supplies the relation-shaped descriptor and arrays. When
    /// `returned_item_pointer_required` is true, `slot` must support storing a
    /// heap tuple.
    pub(crate) unsafe fn from_delete_raw(
        slot: *mut pg_sys::TupleTableSlot,
        conversion_context: pg_sys::MemoryContext,
        layout: &'slot mut ModifyRowLayout,
        returned_item_pointer_required: bool,
    ) -> Self {
        let relation_oid = layout.relation_oid();
        let columns = unsafe {
            ModifySlotBuffer::from_delete_raw(slot, layout, conversion_context)
        };
        Self {
            columns,
            layout,
            relation_oid,
            returned_item_pointer_required,
            returned_item_pointer: None,
            representation_dirty: false,
            slot_arrays_complete: true,
        }
    }
    #[inline]
    pub fn natts(&self) -> usize {
        self.columns.natts()
    }

    #[inline]
    pub fn relation_oid(&self) -> pg_sys::Oid {
        self.relation_oid
    }

    pub(crate) fn as_raw(&self) -> *mut pg_sys::TupleTableSlot {
        self.columns.as_raw()
    }

    /// Read a provider column derived from this slot's relation metadata.
    ///
    /// # Safety
    ///
    /// `index` must identify a live, non-dropped attribute in this slot's
    /// relation layout.
    pub unsafe fn datum(&mut self, index: usize) -> PgDatumRef<'_> {
        let attr = unsafe { self.layout.attribute_unchecked(index) };
        // SAFETY: the caller supplied a relation-derived index, and the
        // callback constructor established equal slot/layout widths.
        let (datum, is_null) = unsafe { self.columns.load_datum_unchecked(index) };
        PgDatumRef::from_parts(datum, is_null, attr.type_oid, attr.type_mod, index)
    }

    /// # Safety
    ///
    /// `attno` must identify a live, non-dropped user attribute in this slot's
    /// relation layout.
    pub unsafe fn datum_by_attno(
        &mut self,
        attno: pg_sys::AttrNumber,
    ) -> PgDatumRef<'_> {
        let index = (attno as i32 - 1) as usize;
        unsafe { self.datum(index) }
    }

    /// Set one row value through Cell's type-aware conversion path.
    ///
    /// The row owns target attribute lookup, PostgreSQL memory-context switching,
    /// and the slot write. A view Cell is consumed before this method returns,
    /// so it cannot outlive the source buffer merely because it was passed
    /// through this API.
    /// # Safety
    ///
    /// `index` must identify a live, non-dropped attribute in this slot's
    /// relation layout.
    pub unsafe fn set_cell(
        &mut self,
        index: usize,
        value: Option<Cell>,
    ) -> Result<(), ForeignModifyError> {
        let datum = match value {
            Some(cell) => {
                let codec = unsafe { self.layout.codec_for_unchecked(index) }?;
                let converted = unsafe {
                    PgMemoryContexts::For(self.columns.target_context())
                        .switch_to(|_| codec.cell_to_datum(cell))
                };
                Some(converted.map_err(PgReportError::from_domain_error)?)
            }
            None => None,
        };
        // SAFETY: the caller supplied a relation-derived target index; the slot
        // buffer retains PostgreSQL's lazy-deform operation.
        unsafe { self.columns.set_datum_after_deform_unchecked(index, datum) };
        self.representation_dirty = true;
        Ok(())
    }

    /// # Safety
    ///
    /// `attno` must identify a live, non-dropped user attribute in this slot's
    /// relation layout.
    pub unsafe fn set_cell_by_attno(
        &mut self,
        attno: pg_sys::AttrNumber,
        value: Option<Cell>,
    ) -> Result<(), ForeignModifyError> {
        let index = (attno as i32 - 1) as usize;
        unsafe { self.set_cell(index, value) }
    }

    /// Write a raw PostgreSQL Datum for a provider-specific representation.
    ///
    /// Providers should use set_cell for ordinary values. This method remains
    /// available for a provider that owns a PostgreSQL-specific datum
    /// construction path, but the caller must uphold the target attribute's
    /// type, typmod, memory-context, alignment, and lifetime requirements.
    ///
    /// # Safety
    ///
    /// `index` must identify a live, non-dropped attribute in this slot's
    /// relation layout. If value is Some, it must be a valid Datum for that
    /// attribute and any referenced memory must remain valid in the target
    /// slot's lifetime.
    pub unsafe fn set_raw_datum(
        &mut self,
        index: usize,
        value: Option<pg_sys::Datum>,
    ) {
        // SAFETY: the caller supplied a relation-derived target index.
        unsafe { self.columns.set_datum_after_deform_unchecked(index, value) };
        self.representation_dirty = true;
    }

    /// Write a raw Datum addressed by a one-based PostgreSQL attribute number.
    ///
    /// # Safety
    ///
    /// `attno` must identify a live, non-dropped user attribute in this slot's
    /// relation layout. The same Datum validity and lifetime requirements as
    /// set_raw_datum apply.
    pub unsafe fn set_raw_datum_by_attno(
        &mut self,
        attno: pg_sys::AttrNumber,
        value: Option<pg_sys::Datum>,
    ) {
        let index = (attno as i32 - 1) as usize;
        unsafe { self.set_raw_datum(index, value) }
    }

    pub(crate) fn set_plan_datum(
        &mut self,
        index: usize,
        datum: pg_sys::Datum,
        is_null: bool,
    ) {
        let copied = if is_null { None } else { Some(datum) };
        // SAFETY: the return layout validated the relation index at Begin, and
        // DELETE starts from a slot whose arrays are already initialized.
        unsafe {
            self.columns
                .set_datum_without_deform_unchecked(index, copied)
        };
        self.representation_dirty = true;
    }

    pub(crate) fn set_columns_from_composite(
        &mut self,
        datum: pg_sys::Datum,
        required_columns: &[ForeignModifyReturnColumn],
        full_whole_row: bool,
    ) {
        let tuple_desc = self.layout.tuple_desc();
        let header = unsafe { PgWrapper::datum_get_heap_tuple_header(datum) };

        let mut tuple = MaybeUninit::<pg_sys::HeapTupleData>::uninit();
        let tuple_ptr = tuple.as_mut_ptr();
        unsafe {
            (*tuple_ptr).t_len =
                PgWrapper::heap_tuple_header_get_datum_length(header);
            pg_sys::ItemPointerSetInvalid(&mut (*tuple_ptr).t_self);
            (*tuple_ptr).t_tableOid = pg_sys::InvalidOid;
            (*tuple_ptr).t_data = header;
        }

        if full_whole_row {
            unsafe {
                self.columns
                    .deform_heap_tuple_unchecked(tuple_ptr, tuple_desc);
            }
        } else {
            for column in required_columns {
                let index = column.relation_index;
                let mut value_is_null = false;
                let value = unsafe {
                    pg_sys::heap_getattr(
                        tuple_ptr,
                        column.relation_attno as c_int,
                        tuple_desc,
                        &mut value_is_null,
                    )
                };
                let copied = if value_is_null { None } else { Some(value) };
                // SAFETY: the return layout validated each relation index at
                // Begin, and DELETE starts from a slot whose arrays are already
                // initialized.
                unsafe {
                    self.columns
                        .set_datum_without_deform_unchecked(index, copied)
                };
            }
        }
        self.representation_dirty = true;
    }

    /// # Safety
    ///
    /// `index` must identify a live, non-dropped attribute in this slot's
    /// relation layout.
    #[inline]
    pub unsafe fn set_null(&mut self, index: usize) {
        // SAFETY: the caller supplied a relation-derived target index.
        unsafe { self.columns.set_datum_after_deform_unchecked(index, None) };
        self.representation_dirty = true;
    }

    /// Set the provider-defined ItemPointer carried by the returned row.
    /// PostgreSQL tuple-header fields are written by the framework when the
    /// returned slot is materialized.
    #[inline]
    pub fn set_returned_item_pointer(&mut self, item_pointer: &ValidItemPointer) {
        self.returned_item_pointer = Some(*item_pointer);
        self.representation_dirty = true;
    }

    pub(crate) fn finish(
        &mut self,
        return_slot_required: bool,
    ) -> Result<(), ForeignModifyError> {
        if self.returned_item_pointer_required && self.returned_item_pointer.is_none()
        {
            return Err(ForeignModifyError::framework(
                "foreign provider returned no required item-pointer identity",
            ));
        }
        if return_slot_required
            && self.representation_dirty
            && (self.returned_item_pointer_required || !self.slot_arrays_complete)
        {
            self.materialize_returned_slot();
        }
        Ok(())
    }

    /// Rebuild the physical representation used by PostgreSQL's modify slot
    /// after the provider has changed its datum arrays.
    fn materialize_returned_slot(&mut self) {
        let slot = self.columns.as_raw();
        // SAFETY: the row is callback-scoped and was built from a live slot.
        unsafe {
            if self.slot_arrays_complete {
                // DELETE starts from ExecStoreAllNullTuple, so its arrays are
                // complete already. Re-mark the slot virtual before building
                // a physical tuple for a returned ctid.
                pg_sys::ExecClearTuple(slot);
                pg_sys::ExecStoreVirtualTuple(slot);
            } else {
                // INSERT/UPDATE may preserve untouched values from a lazy
                // input tuple and therefore still need to deform it here.
                pg_sys::slot_getallattrs(slot);
            }
            let slot_context = (*slot).tts_mcxt;
            let prior_context = pg_sys::MemoryContextSwitchTo(slot_context);
            let tuple = pg_sys::heap_form_tuple(
                (*slot).tts_tupleDescriptor,
                (*slot).tts_values,
                (*slot).tts_isnull,
            );
            pg_sys::MemoryContextSwitchTo(prior_context);
            (*(*tuple).t_data).t_choice.t_heap.t_xmin = 0.into();
            (*(*tuple).t_data).t_choice.t_heap.t_xmax = 0.into();
            (*(*tuple).t_data).t_choice.t_heap.t_field3.t_cid = 0;
            if let Some(item_pointer) = self.returned_item_pointer {
                (*tuple).t_self = item_pointer.to_pg_sys();
                (*(*tuple).t_data).t_ctid = item_pointer.to_pg_sys();
            }
            pg_sys::ExecForceStoreHeapTuple(tuple, slot, true);
            if !self.slot_arrays_complete {
                // Preserve the existing INSERT/UPDATE post-materialization
                // state; DELETE's complete virtual arrays do not need this.
                pg_sys::slot_getallattrs(slot);
            }
        }
        self.representation_dirty = false;
    }
}
