//! Iceberg Arrow batch adaptation and row-location binding shared by adapters.

use arrow_array::types::Int32Type;
use arrow_array::{Array, Int64Array, RecordBatch, RunArray, StringArray};
use iceberg_lite::metadata_columns::{
    RESERVED_COL_NAME_FILE, RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_FILE,
    RESERVED_FIELD_ID_POS,
};
use iceberg_lite::scan::ArrowRecordBatchIterator;
use lagodb_core::prelude::*;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pg_arrow_conv::ArrowBatchSource;
use pgrx::pg_sys;

use crate::error::IcebergError;

/// Adapts Iceberg's batch iterator to the format-neutral conversion source.
///
/// The producer error remains an [`IcebergError`] at the callback boundary,
/// and PostgreSQL cancellation is checked once for every underlying batch.
/// Empty batches are deliberately preserved so consumers retain the old
/// batch-schema validation behavior.
pub(crate) struct IcebergArrowBatches(pub(crate) ArrowRecordBatchIterator);

impl Iterator for IcebergArrowBatches {
    type Item = Result<RecordBatch, IcebergError>;

    fn next(&mut self) -> Option<Self::Item> {
        pg_sys::check_for_interrupts!();
        self.0.next().map(|batch| batch.map_err(IcebergError::from))
    }
}

pub(crate) type IcebergArrowBatchSource =
    ArrowBatchSource<IcebergArrowBatches, IcebergError>;

/// Stable positions of Iceberg's row-location metadata columns.
#[derive(Clone, Copy)]
pub(crate) struct RowLocationLayout {
    file_index: usize,
    position_index: usize,
}

impl RowLocationLayout {
    pub(crate) fn try_new(batch: &RecordBatch) -> AmResult<Self> {
        let mut file_index = None;
        let mut position_index = None;
        for (index, field) in batch.schema().fields().iter().enumerate() {
            let Some(field_id) = field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|raw| raw.parse::<i32>().ok())
            else {
                continue;
            };
            match field_id {
                RESERVED_FIELD_ID_FILE if file_index.is_none() => {
                    file_index = Some(index);
                }
                RESERVED_FIELD_ID_POS if position_index.is_none() => {
                    position_index = Some(index);
                }
                _ => {}
            }
        }
        Ok(Self {
            file_index: file_index.ok_or(IcebergError::InvariantViolated(
                "row-location file column is missing from scan batch",
            ))?,
            position_index: position_index.ok_or(IcebergError::InvariantViolated(
                "row-location position column is missing from scan batch",
            ))?,
        })
    }

    /// Bind a reader batch to its file path and position array.
    ///
    /// Empty batches still have their metadata types validated, but return
    /// `None` because they have no batch-constant `_file` value to read.
    ///
    /// # Safety
    ///
    /// `batch` must come from `IcebergArrowBatches` and have the same field
    /// order and types as the batch used to create this layout. Iceberg's
    /// reader produces `_file` as one required `RunArray<Int32Type>` constant
    /// for the current `FileReadRequest`, and `_pos` as a required,
    /// non-negative `Int64Array`.
    pub(crate) unsafe fn bind<'a>(
        self,
        batch: &'a RecordBatch,
    ) -> AmResult<Option<BoundRowLocations<'a>>> {
        let file_array = batch.column(self.file_index);
        let files = file_array
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .ok_or_else(|| {
                IcebergError::ArrowTypeMismatch(format!(
                    "metadata column {RESERVED_COL_NAME_FILE} has unexpected Arrow type {:?}",
                    file_array.data_type()
                ))
            })?;
        let file_values = files
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                IcebergError::ArrowTypeMismatch(format!(
                    "metadata column {RESERVED_COL_NAME_FILE} has unexpected value type {:?}",
                    files.values().data_type()
                ))
            })?;

        let position_array = batch.column(self.position_index);
        let positions = position_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                IcebergError::ArrowTypeMismatch(format!(
                    "metadata column {RESERVED_COL_NAME_POS} has unexpected Arrow type {:?}",
                    position_array.data_type()
                ))
            })?;

        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let file_value_index = files.run_ends().get_physical_index(0);

        Ok(Some(BoundRowLocations {
            file_path: file_values.value(file_value_index),
            positions: positions.clone(),
        }))
    }
}

/// Typed row-location columns for one non-empty, single-file reader batch.
pub(crate) struct BoundRowLocations<'a> {
    file_path: &'a str,
    positions: Int64Array,
}

impl BoundRowLocations<'_> {
    pub(crate) fn file_path(&self) -> &str {
        self.file_path
    }

    pub(crate) fn into_positions(self) -> Int64Array {
        self.positions
    }
}

/// Read a non-null, non-negative position produced by Iceberg's row-number
/// reader.
///
/// # Safety
///
/// `row` must be within `positions`. The array must be the `_pos` column bound
/// by `RowLocationLayout`.
#[inline]
pub(crate) unsafe fn position_unchecked(positions: &Int64Array, row: usize) -> u64 {
    let position = unsafe { positions.value_unchecked(row) };
    position as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    fn row_location_batch(rows: usize, path: &str) -> RecordBatch {
        let physical_rows = rows.max(1);
        let run_ends = Int32Array::from(vec![i32::try_from(physical_rows).unwrap()]);
        let values = StringArray::from(vec![path]);
        let files = RunArray::<Int32Type>::try_new(&run_ends, &values)
            .unwrap()
            .slice(0, rows);
        let positions = Int64Array::from_iter_values(
            (0..rows).map(|row| i64::try_from(row).unwrap()),
        );
        let file_field =
            Field::new(RESERVED_COL_NAME_FILE, files.data_type().clone(), false)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_owned(),
                    RESERVED_FIELD_ID_FILE.to_string(),
                )]));
        let position_field =
            Field::new(RESERVED_COL_NAME_POS, DataType::Int64, false).with_metadata(
                HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_owned(),
                    RESERVED_FIELD_ID_POS.to_string(),
                )]),
            );
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![file_field, position_field])),
            vec![Arc::new(files), Arc::new(positions)],
        )
        .unwrap()
    }

    #[test]
    fn binds_one_file_path_and_positions_per_non_empty_batch() {
        let batch = row_location_batch(3, "data.parquet");
        let layout = RowLocationLayout::try_new(&batch).unwrap();
        // SAFETY: the test batch follows the same row-location schema contract.
        let locations = unsafe { layout.bind(&batch) }.unwrap().unwrap();

        assert_eq!(locations.file_path(), "data.parquet");
        let positions = locations.into_positions();
        assert_eq!(positions.values(), &[0, 1, 2]);
    }

    #[test]
    fn binds_empty_batch_without_reading_file_value() {
        let batch = row_location_batch(0, "data.parquet");
        let layout = RowLocationLayout::try_new(&batch).unwrap();
        // SAFETY: the test batch follows the same row-location schema contract.
        let locations = unsafe { layout.bind(&batch) }.unwrap();

        assert!(locations.is_none());
    }
}
