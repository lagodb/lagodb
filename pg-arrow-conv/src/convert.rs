//! Row-world write dispatch.
//!
//! [`ColumnRule::build`] (buffered `Cell`s → Arrow array) is the `Cell`-based
//! row-world write API a row-mode access method or FDW consumes. It drives the
//! same [`ArrowColumnEncoder`] the columnar hot path uses (via `append_cell`),
//! so a buffered `Cell` and a live datum produce a bit-identical Arrow array.
//!
//! The read direction has no per-value dispatcher here: both worlds decode a
//! batch through [`ColumnReader`](crate::read), which binds a column's concrete
//! typed array once per batch and then reads values without a per-value
//! downcast — [`read_datum`](crate::read::ColumnReader::read_datum) for the
//! slot-first scan, [`read_cell`](crate::read::ColumnReader::read_cell) for the
//! row-world `Cell`.

use arrow_array::ArrayRef;
use pg_lakebase_core::batch::DatumColumnAppender;
use pg_lakebase_core::tuple::Row;

use crate::error::ConvResult;
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
    pub fn build(&self, rows: &[Row], col_idx: usize) -> ConvResult<ArrayRef> {
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
