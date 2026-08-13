//! Owned [`Row`] values and the writer that materializes them into a virtual
//! PostgreSQL tuple slot.

use std::slice;

use crate::diag::PgReportError;
use pgrx::pg_sys;

use super::cell::Cell;
use super::datum::DatumConversionError;
use super::row_codec::RowDatumCodec;
use super::slot_columns::SlotColumns;
use super::slot_row::TupleSlotRow;

#[derive(Debug, Clone, Default)]
pub struct Row {
    cells: Vec<Option<Cell>>,
    pub size: usize,
}

impl Row {
    /// Create an empty row.
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

        // SAFETY: ensure_len established this index immediately above.
        unsafe { self.set_cell_at_bound(index, cell) };
    }

    /// Replace a cell at an index validated against this row's fixed layout.
    ///
    /// # Safety
    ///
    /// `index` must be smaller than [`Self::len`].
    pub unsafe fn set_cell_at_bound(&mut self, index: usize, cell: Option<Cell>) {
        // SAFETY: required by this method's contract.
        let slot = unsafe { self.cells.get_unchecked_mut(index) };

        if let Some(old_cell) = slot.take() {
            self.size = self.size.saturating_sub(old_cell.mem_size());
        }

        if let Some(new_cell) = &cell {
            self.size += new_cell.mem_size();
        }

        *slot = cell;
    }

    pub fn take_cell(&mut self, index: usize) -> Option<Cell> {
        let cell = self.cells.get_mut(index).and_then(Option::take);
        if let Some(cell) = &cell {
            self.size = self.size.saturating_sub(cell.mem_size());
        }
        cell
    }

    pub fn iter(&self) -> slice::Iter<'_, Option<Cell>> {
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

    ///
    /// # Safety
    ///
    /// `codec` must have been bound to the same tuple descriptor as `slot`.
    /// Every datum in the slot must be valid for its corresponding target.
    pub unsafe fn from_slot_view(
        slot: TupleSlotRow<'_>,
        codec: &RowDatumCodec,
    ) -> Result<Self, DatumConversionError> {
        let datums = slot.datums();
        let natts = datums.len();
        if codec.len() != natts {
            return Err(DatumConversionError::IncompatibleType {
                target: pg_sys::InvalidOid,
            });
        }
        let mut row = Self::with_capacity(natts);
        let (values, nulls) = datums.raw_parts();
        unsafe { codec.datums_to_cells(values, nulls, &mut row.cells) }?;
        row.size = row
            .cells
            .iter()
            .filter_map(Option::as_ref)
            .map(Cell::mem_size)
            .sum();

        Ok(row)
    }

    /// # Safety
    ///
    /// `slot` must be a valid, non-null pointer to an initialized
    /// `TupleTableSlot`, and `codec` must be bound to that slot's tuple
    /// descriptor.
    pub unsafe fn update_from_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
        codec: &RowDatumCodec,
    ) -> Result<(), DatumConversionError> {
        unsafe {
            // Ensure slot contents are accessible (deform tuple if needed)
            pg_sys::slot_getallattrs(slot);

            let tup_desc = (*slot).tts_tupleDescriptor;
            let natts = (*tup_desc).natts as usize;
            if codec.len() != natts {
                return Err(DatumConversionError::IncompatibleType {
                    target: pg_sys::InvalidOid,
                });
            }
            let values = slice::from_raw_parts((*slot).tts_values, natts);
            let nulls = slice::from_raw_parts((*slot).tts_isnull, natts);
            self.ensure_len(natts);
            self.size = 0;

            codec.datums_to_cells(values, nulls, &mut self.cells[..natts])?;
            self.size = self.cells[..natts]
                .iter()
                .filter_map(Option::as_ref)
                .map(Cell::mem_size)
                .sum();
        }
        Ok(())
    }

    /// # Safety
    ///
    /// `slot` must be a valid, non-null pointer to an initialized
    /// `TupleTableSlot`, and `codec` must be bound to that slot's tuple
    /// descriptor.
    pub unsafe fn from_slot(
        slot: *mut pg_sys::TupleTableSlot,
        codec: &RowDatumCodec,
    ) -> Result<Self, DatumConversionError> {
        unsafe { TupleSlotRow::from_raw(slot).to_owned_row(codec) }
    }
}

/// Writes a [`Row`] into a PostgreSQL virtual tuple slot.
pub struct TupleSlotWriter<'codec> {
    slot: *mut pg_sys::TupleTableSlot,
    memory_context: pg_sys::MemoryContext,
    codec: &'codec RowDatumCodec,
}

impl<'codec> TupleSlotWriter<'codec> {
    /// # Safety
    ///
    /// `slot` must point to a valid PostgreSQL tuple slot. `memory_context`
    /// must remain valid while writing datum values for that slot. `codec` must
    /// have been bound to the same tuple descriptor as `slot`.
    pub unsafe fn new(
        slot: *mut pg_sys::TupleTableSlot,
        memory_context: pg_sys::MemoryContext,
        codec: &'codec RowDatumCodec,
    ) -> Self {
        Self {
            slot,
            memory_context,
            codec,
        }
    }

    /// # Safety
    ///
    /// The slot and memory context passed to [`Self::new`] must still be
    /// valid. The row is consumed in-place: written cells are taken from it.
    pub unsafe fn write_row(&self, row: &mut Row) -> Result<(), PgReportError> {
        unsafe {
            // `SlotColumns` is the row-world substrate that owns these
            // output-only `tts_values`/`tts_isnull` writes; the row world
            // funnels through it rather than re-deriving slot pointers here.
            let mut cols = SlotColumns::new(self.slot, self.memory_context);
            cols.fill_from_row(row, self.codec)?;

            // The row-world provider API marks the slot non-empty itself
            // (`fill_from_row` deliberately does not), so existing callers that
            // do not issue their own store keep working.
            pg_sys::ExecStoreVirtualTuple(self.slot);
            Ok(())
        }
    }
}
