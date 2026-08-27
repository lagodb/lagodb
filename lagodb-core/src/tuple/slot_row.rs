//! Callback-scoped views over PostgreSQL tuple slots and their datums.
//!
//! These views borrow PostgreSQL-owned memory and must be consumed during the
//! callback that supplied the slot. Use [`super::Row`] when values must outlive
//! that callback.

use std::marker::PhantomData;
use std::slice;

use pgrx::pg_sys;

use super::cell::Cell;
use super::datum::DatumConversionError;
use super::row::Row;
use super::row_codec::RowDatumCodec;

/// Borrowed view of one PostgreSQL datum in a tuple slot.
///
/// The value is valid only while the owning [`TupleSlotRow`] is valid. It is a
/// source view, not an owned value: callers can inspect PostgreSQL type
/// metadata or materialize it into a [`Cell`].
#[derive(Clone, Copy)]
pub struct PgDatumRef<'slot> {
    datum: pg_sys::Datum,
    is_null: bool,
    type_oid: pg_sys::Oid,
    typmod: i32,
    index: usize,
    _marker: PhantomData<&'slot ()>,
}

impl<'slot> PgDatumRef<'slot> {
    pub(crate) const fn from_parts(
        datum: pg_sys::Datum,
        is_null: bool,
        type_oid: pg_sys::Oid,
        typmod: i32,
        index: usize,
    ) -> Self {
        Self {
            datum,
            is_null,
            type_oid,
            typmod,
            index,
            _marker: PhantomData,
        }
    }

    pub fn datum(&self) -> pg_sys::Datum {
        self.datum
    }

    pub fn is_null(&self) -> bool {
        self.is_null
    }

    pub fn type_oid(&self) -> pg_sys::Oid {
        self.type_oid
    }

    pub fn typmod(&self) -> i32 {
        self.typmod
    }

    /// Materialize this datum into the framework's owned cell representation.
    ///
    /// This is the row-mode fallback. Columnar mutation hot paths should use a
    /// provider's relation-bound writer instead of materializing a [`Cell`].
    ///
    /// # Safety
    ///
    /// `codec` must have been bound to the same relation/slot tuple descriptor
    /// that produced this datum. The slot and its datum must remain valid for
    /// the duration of the conversion.
    pub unsafe fn to_cell(
        self,
        codec: &RowDatumCodec,
    ) -> Result<Option<Cell>, DatumConversionError> {
        unsafe { codec.datum_to_cell(self.index, self.datum, self.is_null) }
    }
}

/// Borrowed view of a PostgreSQL [`TupleTableSlot`](pg_sys::TupleTableSlot).
///
/// This view is scoped to the table-AM callback that received the slot. It must
/// not be stored across callbacks because PostgreSQL can reuse the slot and
/// reset the surrounding memory context.
#[derive(Clone, Copy)]
pub struct TupleSlotRow<'slot> {
    slot: *mut pg_sys::TupleTableSlot,
    _marker: PhantomData<&'slot pg_sys::TupleTableSlot>,
}

impl<'slot> TupleSlotRow<'slot> {
    /// # Safety
    ///
    /// `slot` must be a valid, non-null pointer to an initialized
    /// `TupleTableSlot`, and it must remain valid for `'slot`.
    pub unsafe fn from_raw(slot: *mut pg_sys::TupleTableSlot) -> Self {
        unsafe {
            // Deform once at the callback boundary so repeated datum access
            // only reads the slot's already-populated arrays.
            pg_sys::slot_getallattrs(slot);
        }
        Self {
            slot,
            _marker: PhantomData,
        }
    }

    pub fn as_raw(&self) -> *mut pg_sys::TupleTableSlot {
        self.slot
    }

    pub fn len(&self) -> usize {
        // SAFETY: `self` can only be constructed from a valid initialized
        // TupleTableSlot by `from_raw`.
        unsafe {
            let tup_desc = (*self.slot).tts_tupleDescriptor;
            (*tup_desc).natts as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn datum_at(&self, index: usize) -> Option<PgDatumRef<'slot>> {
        self.datums().datum_at(index)
    }

    /// Build a [`SlotDatums`] view over the slot's deformed columns.
    ///
    /// The slot's `tts_values` / `tts_isnull` arrays and the descriptor's
    /// attribute list are resolved a single time here, so per-column access
    /// through [`SlotDatums::datum_at`] is a plain bounds-checked index.
    pub fn datums(&self) -> SlotDatums<'slot> {
        // SAFETY: the slot pointer and its descriptor remain valid for the
        // callback-scoped lifetime carried by `self`.
        unsafe {
            let tup_desc = (*self.slot).tts_tupleDescriptor;
            let natts = (*tup_desc).natts as usize;
            SlotDatums {
                values: slice::from_raw_parts((*self.slot).tts_values, natts),
                nulls: slice::from_raw_parts((*self.slot).tts_isnull, natts),
                attrs: slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts),
            }
        }
    }

    /// Materialize the slot view as an owned [`Row`].
    ///
    /// # Safety
    ///
    /// `codec` must have been bound to the same tuple descriptor as `self`.
    /// Every attribute OID and datum must therefore match the corresponding
    /// target in the codec.
    pub unsafe fn to_owned_row(
        &self,
        codec: &RowDatumCodec,
    ) -> Result<Row, DatumConversionError> {
        // SAFETY: required by this method's contract; the codec matches self.
        unsafe { Row::from_slot_view(*self, codec) }
    }
}

/// A tuple slot's columns deformed once: the backing `tts_values` /
/// `tts_isnull` arrays and the descriptor's per-attribute metadata, resolved a
/// single time so each [`Self::datum_at`] is an O(1) index rather than
/// rebuilding the slices per column.
#[derive(Clone, Copy)]
pub struct SlotDatums<'slot> {
    values: &'slot [pg_sys::Datum],
    nulls: &'slot [bool],
    attrs: &'slot [pg_sys::FormData_pg_attribute],
}

/// A source-slot index validated against a relation's complete tuple width at
/// plan construction time.
#[derive(Debug, Clone, Copy)]
pub struct SlotDatumIndex {
    index: usize,
    slot_width: usize,
}

impl SlotDatumIndex {
    /// Validate one zero-based source index against the complete tuple width.
    pub fn new(index: usize, slot_width: usize) -> Option<Self> {
        (index < slot_width).then_some(Self { index, slot_width })
    }
}

impl<'slot> SlotDatums<'slot> {
    /// Number of columns (`natts`).
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn raw_parts(&self) -> (&'slot [pg_sys::Datum], &'slot [bool]) {
        (self.values, self.nulls)
    }

    /// Borrowed view of column `index`, or `None` when out of range.
    pub fn datum_at(&self, index: usize) -> Option<PgDatumRef<'slot>> {
        if index >= self.values.len() {
            return None;
        }
        let attr = &self.attrs[index];
        Some(PgDatumRef::from_parts(
            self.values[index],
            self.nulls[index],
            attr.atttypid,
            attr.atttypmod,
            index,
        ))
    }

    /// Read a datum through an index validated by a relation-bound write plan.
    ///
    /// Unlike [`Self::datum_at`], this does not inspect the slot descriptor or
    /// perform a per-row bounds check. It returns only the raw datum and NULL
    /// flag because the bound writer owns the source codec and does not need to
    /// reconstruct a generic [`PgDatumRef`].
    ///
    /// # Safety
    ///
    /// `index` must have been validated against the tuple layout that produced
    /// `self`, and that layout must have the same `slot_width` recorded in the
    /// token. The caller must keep the returned datum within the callback
    /// lifetime of this `SlotDatums` view.
    pub unsafe fn datum_at_bound(
        &self,
        index: SlotDatumIndex,
    ) -> (pg_sys::Datum, bool) {
        debug_assert_eq!(self.values.len(), index.slot_width);
        // SAFETY: the caller guarantees that the token was validated against
        // this exact tuple layout, so both arrays contain `index.index`.
        unsafe {
            (
                *self.values.get_unchecked(index.index),
                *self.nulls.get_unchecked(index.index),
            )
        }
    }
}

/// Borrowed view of a PostgreSQL multi-insert slot array.
#[derive(Clone, Copy)]
pub struct TupleSlotBatch<'slot> {
    slots: &'slot [*mut pg_sys::TupleTableSlot],
}

impl<'slot> TupleSlotBatch<'slot> {
    /// # Safety
    ///
    /// `slots` must point to `len` valid `TupleTableSlot` pointers that remain
    /// valid for `'slot`.
    pub unsafe fn from_raw(
        slots: *mut *mut pg_sys::TupleTableSlot,
        len: usize,
    ) -> Self {
        // SAFETY: required by this constructor's contract; the caller owns a
        // valid array of callback-scoped slot pointers.
        let slots = unsafe { slice::from_raw_parts(slots, len) };
        Self { slots }
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn row_at(&self, index: usize) -> Option<TupleSlotRow<'slot>> {
        self.slots
            .get(index)
            // SAFETY: every slot pointer was validated by `from_raw`.
            .map(|slot| unsafe { TupleSlotRow::from_raw(*slot) })
    }

    pub fn iter(&self) -> impl Iterator<Item = TupleSlotRow<'slot>> + '_ {
        self.slots
            .iter()
            // SAFETY: every slot pointer was validated by `from_raw`.
            .map(|slot| unsafe { TupleSlotRow::from_raw(*slot) })
    }

    ///
    /// # Safety
    ///
    /// Every slot in this batch must use the same tuple descriptor as `codec`,
    /// and each slot must remain valid while its row is materialized.
    pub unsafe fn to_owned_rows(
        &self,
        codec: &RowDatumCodec,
    ) -> Result<Vec<Row>, DatumConversionError> {
        // SAFETY: required by this method's contract; every row shares the
        // codec's tuple descriptor and remains valid for the conversion.
        self.iter()
            .map(|row| unsafe { row.to_owned_row(codec) })
            .collect()
    }
}
