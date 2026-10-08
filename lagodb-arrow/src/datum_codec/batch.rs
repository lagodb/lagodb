//! Batch-owned readers shared by slot, COPY and sampling consumers.

use lagodb_core::tuple::SlotColumns;
use pgrx::pg_sys;

use super::reader::BoundDatumReader;
use crate::ArrowConversionResult;

/// Concrete Arrow readers bound once for one record batch.
pub struct BoundBatch {
    pub(super) columns: Box<[BoundColumn]>,
    pub(super) num_rows: usize,
}

/// One batch column and its provider-bound destination.
pub struct BoundColumn {
    pub(super) reader: BoundDatumReader,
    pub(super) dest: usize,
}

impl BoundBatch {
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Columns remain in the order supplied to the decoder at binding.
    pub fn columns(&self) -> &[BoundColumn] {
        &self.columns
    }

    /// Write a row into the slot layout used to bind this batch's decoder.
    ///
    /// # Safety
    /// `row` is within this batch, and every bound destination has the same
    /// type and position in `out`. The current PG row context owns the datums.
    pub unsafe fn write_row_unchecked(
        &self,
        row: usize,
        out: &mut SlotColumns<'_>,
    ) -> ArrowConversionResult<()> {
        for column in &self.columns {
            let value = unsafe { column.read_datum_unchecked(row) }?;
            unsafe { out.set_datum_unchecked(column.dest, value) };
        }
        Ok(())
    }
}

impl BoundColumn {
    pub fn destination(&self) -> usize {
        self.dest
    }

    /// Decode without repeating the enclosing batch's row-bound check.
    ///
    /// # Safety
    /// `row` is within this column's batch. PostgreSQL must be active with
    /// the destination row memory context selected. By-reference datums must
    /// be consumed before that context is reset.
    pub unsafe fn read_datum_unchecked(
        &self,
        row: usize,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        unsafe { self.reader.read_datum_unchecked(row) }
    }
}
