//! Row-world write dispatch.
//!
//! [`ColumnRule::build`] (buffered `Cell`s → Arrow array) is the `Cell`-based
//! row-world write API a row-mode access method or FDW consumes. It drives the
//! same [`ArrowColumnEncoder`] the columnar hot path uses (via `append_cell`),
//! so a buffered `Cell` and a live datum produce a bit-identical Arrow array.
//!
//! The read direction has no per-value downcast here. The row-world binds
//! semantic columns through [`ColumnReader`](crate::read::ColumnReader); the
//! slot-first path binds provider datum codecs through
//! [`ArrowColumnDecoder`](crate::read::ArrowColumnDecoder). Both resolve the
//! concrete Arrow array once per batch.

use arrow_array::ArrayRef;
use pg_lakebase_core::tuple::Row;

use crate::error::ArrowConversionResult;
use crate::rule::ColumnRule;
use crate::types::ArrowColumnEncoder;

impl ColumnRule {
    /// Build an Arrow array for this rule from column `col_idx` of every row,
    /// appending one slot per row so all columns stay NULL-aligned.
    ///
    /// This is the row-world / FDW write entry point. It drives the *same*
    /// per-type [`ArrowColumnEncoder`] the columnar hot path uses, via
    /// [`ArrowColumnEncoder::append_cell`], so a buffered `Cell` and a live
    /// datum produce a bit-identical Arrow array. A missing/NULL cell appends a
    /// NULL slot.
    pub fn build(
        &self,
        rows: &[Row],
        col_idx: usize,
    ) -> ArrowConversionResult<ArrayRef> {
        let mut encoder = ArrowColumnEncoder::new(self, rows.len());
        for cell in rows
            .iter()
            .map(|row| row.get(col_idx).and_then(|c| c.as_ref()))
        {
            match cell {
                Some(cell) => encoder.append_cell(cell)?,
                None => encoder.append_null(),
            }
        }
        encoder.finish()
    }
}
