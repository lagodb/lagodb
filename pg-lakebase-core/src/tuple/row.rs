//! `Row`: a buffered representation of a PostgreSQL tuple plus the writer
//! that materializes it back into a virtual `TupleTableSlot`.

use crate::diag::PgReportError;
use pgrx::FromDatum;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;

use super::cell::Cell;

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
        unsafe {
            let mut row = Self::new();
            row.update_from_slot(slot);
            row
        }
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
            let tup_desc = (*self.slot).tts_tupleDescriptor;
            let natts = (*tup_desc).natts as usize;

            if row.len() < natts {
                return Err(ErrorReport::new(
                    PgSqlErrorCode::ERRCODE_INVALID_COLUMN_REFERENCE,
                    format!(
                        "row has {} columns but tuple slot expects {}",
                        row.len(),
                        natts
                    ),
                    "",
                )
                .into());
            }

            let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);
            let slot_values =
                std::slice::from_raw_parts_mut((*self.slot).tts_values, natts);
            let slot_nulls =
                std::slice::from_raw_parts_mut((*self.slot).tts_isnull, natts);

            PgMemoryContexts::For(self.memory_context).switch_to(|_| {
                for i in 0..natts {
                    match row.take_cell(i) {
                        Some(cell) => {
                            let attr = &attrs[i];
                            let datum = cell
                                .into_datum_typed(attr.atttypid, attr.atttypmod)
                                .ok_or_else(|| {
                                    PgReportError::new(ErrorReport::new(
                                        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
                                        format!(
                                            "failed to convert row column {} to datum",
                                            i + 1
                                        ),
                                        "",
                                    ))
                                })?;

                            slot_values[i] = datum;
                            slot_nulls[i] = false;
                        }
                        None => {
                            slot_nulls[i] = true;
                        }
                    }
                }

                pg_sys::ExecStoreVirtualTuple(self.slot);
                Ok(())
            })
        }
    }
}
