//! `Row`: a buffered representation of a PostgreSQL tuple plus the writer
//! that materializes it back into a virtual `TupleTableSlot`.

use crate::diag::PgReportError;
use pgrx::FromDatum;
use pgrx::pg_sys;
use std::marker::PhantomData;

use super::cell::Cell;
use super::slot_columns::SlotColumns;

/// Borrowed view of one PostgreSQL datum in a tuple slot.
///
/// The value is valid only while the owning [`TupleSlotRow`] is valid. It is a
/// source view, not an owned value: columnar encoders can inspect the PostgreSQL
/// type metadata and append directly into their own builders without first
/// materializing a [`Cell`].
#[derive(Clone, Copy)]
pub struct PgDatumRef<'slot> {
    datum: pg_sys::Datum,
    is_null: bool,
    type_oid: pg_sys::Oid,
    typmod: i32,
    _marker: PhantomData<&'slot ()>,
}

impl<'slot> PgDatumRef<'slot> {
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
    /// This is the row-mode fallback. Columnar DML hot paths should prefer
    /// appending the datum view directly to their builders.
    pub fn to_cell(self) -> Option<Cell> {
        unsafe {
            Cell::from_polymorphic_datum(self.datum, self.is_null, self.type_oid)
        }
    }
}

/// Borrowed view of a PostgreSQL [`TupleTableSlot`](pg_sys::TupleTableSlot).
///
/// This view is scoped to the table-AM callback that received the slot. It must
/// not be stored across callbacks because PostgreSQL can reuse the slot and
/// reset the surrounding memory context.
///
/// The intended DML flow is:
///
/// - row-mode AMs call [`Self::to_owned_row`] and buffer owned values;
/// - columnar AMs read [`PgDatumRef`] values with [`Self::datum_at`] and append
///   them directly into AM-owned builders.
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

    /// Deform the slot's columns once into a [`SlotDatums`] view.
    ///
    /// The slot's `tts_values` / `tts_isnull` arrays and the descriptor's
    /// attribute list are resolved a single time here, so per-column access
    /// through [`SlotDatums::datum_at`] is a plain bounds-checked index. Callers
    /// reading more than one column (the columnar DML write path, `to_owned_row`)
    /// must go through this instead of repeated [`Self::datum_at`], which rebuilds
    /// the three slices on every call.
    pub fn datums(&self) -> SlotDatums<'slot> {
        unsafe {
            let tup_desc = (*self.slot).tts_tupleDescriptor;
            let natts = (*tup_desc).natts as usize;
            SlotDatums {
                values: std::slice::from_raw_parts((*self.slot).tts_values, natts),
                nulls: std::slice::from_raw_parts((*self.slot).tts_isnull, natts),
                attrs: std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts),
            }
        }
    }

    /// Materialize the slot view as an owned [`Row`].
    ///
    /// Use this only when the AM needs row-shaped values after the callback
    /// returns. It intentionally performs the ownership conversion that
    /// columnar DML paths avoid.
    pub fn to_owned_row(&self) -> Row {
        Row::from_slot_view(*self)
    }
}

/// A tuple slot's columns deformed once: the backing `tts_values` /
/// `tts_isnull` arrays and the descriptor's per-attribute metadata, resolved a
/// single time so each [`Self::datum_at`] is an O(1) index rather than
/// rebuilding the slices per column.
///
/// Built by [`TupleSlotRow::datums`] and consumed within the same callback as
/// the originating slot view, so the borrowed arrays stay valid.
#[derive(Clone, Copy)]
pub struct SlotDatums<'slot> {
    values: &'slot [pg_sys::Datum],
    nulls: &'slot [bool],
    attrs: &'slot [pg_sys::FormData_pg_attribute],
}

impl<'slot> SlotDatums<'slot> {
    /// Number of columns (`natts`).
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Borrowed view of column `index`, or `None` when out of range.
    pub fn datum_at(&self, index: usize) -> Option<PgDatumRef<'slot>> {
        if index >= self.values.len() {
            return None;
        }
        let attr = &self.attrs[index];
        Some(PgDatumRef {
            datum: self.values[index],
            is_null: self.nulls[index],
            type_oid: attr.atttypid,
            typmod: attr.atttypmod,
            _marker: PhantomData,
        })
    }
}

/// Borrowed view of a PostgreSQL multi-insert slot array.
///
/// Like [`TupleSlotRow`], this is callback-scoped. It should be consumed during
/// the callback by either materializing owned rows or by appending each slot row
/// into AM-owned column builders.
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
        let slots = unsafe { std::slice::from_raw_parts(slots, len) };
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
            .map(|slot| unsafe { TupleSlotRow::from_raw(*slot) })
    }

    pub fn iter(&self) -> impl Iterator<Item = TupleSlotRow<'slot>> + '_ {
        self.slots
            .iter()
            .map(|slot| unsafe { TupleSlotRow::from_raw(*slot) })
    }

    pub fn to_owned_rows(&self) -> Vec<Row> {
        self.iter().map(|row| row.to_owned_row()).collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Row {
    cells: Vec<Option<Cell>>,
    pub size: usize,
}

impl Row {
    /// Create an empty row
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cells: vec![None; capacity],
            size: 0,
        }
    }

    pub fn push(&mut self, cell: Option<Cell>) {
        if let Some(ref c) = cell {
            self.size += c.mem_size();
        }
        self.cells.push(cell);
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Option<Cell>> {
        self.cells.get(index)
    }

    /// Returns a reference to the cell value at `index`, or `None` if the
    /// index is out of bounds or the column is NULL.
    pub fn get_cell(&self, index: usize) -> Option<&Cell> {
        self.cells.get(index).and_then(|c| c.as_ref())
    }

    pub fn ensure_len(&mut self, len: usize) {
        if self.cells.len() < len {
            self.cells.resize_with(len, || None);
        }
    }

    pub fn set_cell(&mut self, index: usize, cell: Option<Cell>) {
        self.ensure_len(index + 1);

        if let Some(old_cell) = self.cells[index].take() {
            self.size = self.size.saturating_sub(old_cell.mem_size());
        }

        if let Some(new_cell) = &cell {
            self.size += new_cell.mem_size();
        }

        self.cells[index] = cell;
    }

    pub fn take_cell(&mut self, index: usize) -> Option<Cell> {
        let cell = self.cells.get_mut(index).and_then(Option::take);
        if let Some(cell) = &cell {
            self.size = self.size.saturating_sub(cell.mem_size());
        }
        cell
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Option<Cell>> {
        self.cells.iter()
    }

    #[inline]
    pub fn replace_with(&mut self, src: Row) {
        *self = src;
    }

    pub fn clear(&mut self) {
        self.cells.clear();
        self.size = 0;
    }

    pub fn from_slot_view(slot: TupleSlotRow<'_>) -> Self {
        let datums = slot.datums();
        let natts = datums.len();
        let mut row = Self::with_capacity(natts);
        row.size = 0;

        for index in 0..natts {
            row.cells[index] = datums.datum_at(index).and_then(PgDatumRef::to_cell);
            if let Some(cell) = &row.cells[index] {
                row.size += cell.mem_size();
            }
        }

        row
    }

    /// # Safety
    /// `slot` must be a valid, non-null pointer to an initialized TupleTableSlot.
    pub unsafe fn update_from_slot(&mut self, slot: *mut pg_sys::TupleTableSlot) {
        unsafe {
            // Ensure slot contents are accessible (deform tuple if needed)
            pg_sys::slot_getallattrs(slot);

            let tup_desc = (*slot).tts_tupleDescriptor;
            let natts = (*tup_desc).natts as usize;
            let values = std::slice::from_raw_parts((*slot).tts_values, natts);
            let nulls = std::slice::from_raw_parts((*slot).tts_isnull, natts);
            let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);

            // Resize and fill
            self.ensure_len(natts);
            self.size = 0;

            for i in 0..natts {
                self.cells[i] = if nulls[i] {
                    None
                } else {
                    let attr = &attrs[i];
                    Cell::from_polymorphic_datum(values[i], false, attr.atttypid)
                        .inspect(|c| {
                            self.size += c.mem_size();
                        })
                };
            }
        }
    }

    /// # Safety
    /// `slot` must be a valid, non-null pointer to an initialized TupleTableSlot.
    pub unsafe fn from_slot(slot: *mut pg_sys::TupleTableSlot) -> Self {
        unsafe { TupleSlotRow::from_raw(slot).to_owned_row() }
    }
}

/// Writes a [`Row`] into a PostgreSQL virtual tuple slot.
///
/// This keeps the hot path zero-copy with respect to `Cell`: values are moved
/// out of the row with `take_cell()` and converted directly into the slot's
/// preallocated `tts_values` / `tts_isnull` arrays.
pub struct TupleSlotWriter {
    slot: *mut pg_sys::TupleTableSlot,
    memory_context: pg_sys::MemoryContext,
}

impl TupleSlotWriter {
    /// # Safety
    ///
    /// `slot` must point to a valid PostgreSQL tuple slot. `memory_context`
    /// must remain valid while writing datum values for that slot.
    pub unsafe fn new(
        slot: *mut pg_sys::TupleTableSlot,
        memory_context: pg_sys::MemoryContext,
    ) -> Self {
        Self {
            slot,
            memory_context,
        }
    }

    /// # Safety
    ///
    /// The slot and memory context passed to [`Self::new`] must still be
    /// valid. The row is consumed in-place: written cells are taken from it.
    pub unsafe fn write_row(&self, row: &mut Row) -> Result<(), PgReportError> {
        unsafe {
            let natts = (*(*self.slot).tts_tupleDescriptor).natts as usize;

            // `SlotColumns` is the single substrate that owns the unsafe
            // `tts_values`/`tts_isnull` writes; the row world funnels through it
            // rather than re-deriving the slot pointers here.
            let mut cols = SlotColumns::new(self.slot, self.memory_context, natts);
            cols.fill_from_row(row)?;

            // The row-world provider API marks the slot non-empty itself
            // (`fill_from_row` deliberately does not), so existing callers that
            // do not issue their own store keep working.
            pg_sys::ExecStoreVirtualTuple(self.slot);
            Ok(())
        }
    }
}
