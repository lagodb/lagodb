//! Row-world value-conversion dispatch.
//!
//! [`ColumnRule::extract`] (`Arrow → Cell`) and [`ColumnRule::build`]
//! (buffered `Cell`s → Arrow array) are the `Cell`-based row-world API a
//! row-mode access method or FDW consumes. Both are thin dispatchers over the
//! per-type modules in [`crate::types`]: `extract` keys into each type's
//! `extract_*`, and `build` drives the same [`ArrowColumnEncoder`] the columnar
//! hot path uses (via `append_cell`), so the datum and `Cell` write sources
//! stay in lockstep.
//!
//! The slot-first columnar **read** path does not go through `extract`: it
//! decodes each batch through [`ArrowColumnDecoder`](crate::read), which binds
//! a column's concrete typed array once per batch and reads values without a
//! per-value downcast.

use arrow_array::{Array, ArrayRef};
use pg_lakebase_core::batch::DatumColumnAppender;
use pg_lakebase_core::tuple::{Cell, Row};

use crate::error::ConvResult;
use crate::rule::ColumnRule;
use crate::types::{
    ArrowColumnEncoder, binary, decimal, list, primitive, string, temporal,
};

impl ColumnRule {
    /// Extract the [`Cell`] at `row_idx`. The caller guarantees the slot is
    /// non-null (it checks `column.is_null(row_idx)` first), so this always
    /// returns `Some`.
    pub fn extract(
        &self,
        column: &dyn Array,
        row_idx: usize,
    ) -> ConvResult<Option<Cell>> {
        let cell = match self {
            ColumnRule::Bool => primitive::extract_bool(column, row_idx)?,
            ColumnRule::I32 => primitive::extract_i32(column, row_idx)?,
            ColumnRule::I64 => primitive::extract_i64(column, row_idx)?,
            ColumnRule::F32 => primitive::extract_f32(column, row_idx)?,
            ColumnRule::F64 => primitive::extract_f64(column, row_idx)?,
            ColumnRule::Utf8 => string::extract_utf8(column, row_idx)?,
            ColumnRule::Binary => binary::extract_binary(column, row_idx)?,
            ColumnRule::FixedBinary { .. } => {
                binary::extract_fixed_binary(column, row_idx)?
            }
            ColumnRule::Uuid => binary::extract_uuid(column, row_idx)?,
            ColumnRule::Date32 => temporal::extract_date(column, row_idx)?,
            ColumnRule::Time64Micros => temporal::extract_time(column, row_idx)?,
            ColumnRule::Timestamp { nanos, tz } => {
                temporal::extract_timestamp(column, row_idx, *nanos, *tz)?
            }
            ColumnRule::Decimal128 { precision, scale } => {
                decimal::extract(column, row_idx, *precision, *scale)?
            }
            ColumnRule::List { element, .. } => {
                list::extract(column, row_idx, *element)?
            }
        };
        Ok(Some(cell))
    }

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
