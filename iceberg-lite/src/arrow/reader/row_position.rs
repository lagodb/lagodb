// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Parquet row-group and row-selection constraints for point reads.

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;

use crate::{Error, ErrorKind, Result};

/// Restricts a Parquet read to one original, zero-based file position.
///
/// The selection is expressed relative to the selected row group, as required
/// by arrow-rs. Keeping this translation in the reader layer avoids leaking
/// Parquet-specific selection types into [`crate::scan::FileScanTask`].
pub(super) enum RowPositionSelection {
    Target {
        row_group_index: usize,
        row_selection: RowSelection,
    },
    OutOfBounds,
}

impl RowPositionSelection {
    pub(super) fn try_new(metadata: &ParquetMetaData, position: i64) -> Result<Self> {
        if position < 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "row position must not be negative",
            ));
        }

        let mut row_group_start = 0_i64;
        for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
            let row_count = row_group.num_rows();
            if row_count < 0 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "parquet row group has a negative row count",
                ));
            }
            let row_group_end =
                row_group_start.checked_add(row_count).ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "parquet row count exceeds iceberg long range",
                    )
                })?;

            if position < row_group_end {
                let offset =
                    usize::try_from(position - row_group_start).map_err(|_| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "row position does not fit platform index range",
                        )
                    })?;
                let row_count = usize::try_from(row_count).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "parquet row-group size does not fit platform index range",
                    )
                })?;

                return Ok(Self::Target {
                    row_group_index,
                    row_selection: Self::single_row_selection(offset, row_count),
                });
            }
            row_group_start = row_group_end;
        }

        Ok(Self::OutOfBounds)
    }

    /// Intersects an existing row-group restriction with the target row group.
    pub(super) fn restrict_row_groups(
        &self,
        selected_row_groups: &mut Option<Vec<usize>>,
    ) {
        let target = match self {
            Self::Target {
                row_group_index, ..
            } => *row_group_index,
            Self::OutOfBounds => {
                *selected_row_groups = Some(Vec::new());
                return;
            }
        };

        match selected_row_groups {
            Some(indices) if indices.contains(&target) => {
                indices.clear();
                indices.push(target);
            }
            Some(indices) => indices.clear(),
            None => *selected_row_groups = Some(vec![target]),
        }
    }

    /// Intersects an existing row selection with the single-row constraint.
    pub(super) fn merge_row_selection(
        self,
        row_selection: &mut Option<RowSelection>,
    ) {
        let position_selection = match self {
            Self::Target { row_selection, .. } => row_selection,
            Self::OutOfBounds => {
                *row_selection = Some(RowSelection::default());
                return;
            }
        };
        *row_selection = Some(match row_selection.take() {
            Some(existing) => existing.intersection(&position_selection),
            None => position_selection,
        });
    }

    fn single_row_selection(offset: usize, row_count: usize) -> RowSelection {
        let mut selectors = Vec::with_capacity(3);
        if offset != 0 {
            selectors.push(RowSelector::skip(offset));
        }
        selectors.push(RowSelector::select(1));
        let trailing_rows = row_count - offset - 1;
        if trailing_rows != 0 {
            selectors.push(RowSelector::skip(trailing_rows));
        }
        RowSelection::from(selectors)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;

    use super::*;

    fn metadata_with_three_row_groups() -> Arc<ParquetMetaData> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
        Arc::clone(builder.metadata())
    }

    #[test]
    fn locates_positions_at_row_group_boundaries() {
        let metadata = metadata_with_three_row_groups();

        for (position, expected_row_group, expected_selectors) in [
            (0, 0, vec![RowSelector::select(1), RowSelector::skip(3)]),
            (3, 0, vec![RowSelector::skip(3), RowSelector::select(1)]),
            (4, 1, vec![RowSelector::select(1), RowSelector::skip(3)]),
            (9, 2, vec![RowSelector::skip(1), RowSelector::select(1)]),
        ] {
            let RowPositionSelection::Target {
                row_group_index,
                row_selection,
            } = RowPositionSelection::try_new(&metadata, position).unwrap()
            else {
                panic!("position should resolve to a row group");
            };
            assert_eq!(row_group_index, expected_row_group);
            assert_eq!(Vec::<RowSelector>::from(row_selection), expected_selectors);
        }
    }

    #[test]
    fn out_of_bounds_position_selects_no_row_groups_or_rows() {
        let metadata = metadata_with_three_row_groups();
        let selection = RowPositionSelection::try_new(&metadata, 10).unwrap();
        let mut row_groups = Some(vec![0, 1, 2]);
        selection.restrict_row_groups(&mut row_groups);
        let mut rows = Some(RowSelection::from(vec![RowSelector::select(10)]));
        selection.merge_row_selection(&mut rows);

        assert_eq!(row_groups, Some(Vec::new()));
        assert_eq!(rows, Some(RowSelection::default()));
    }

    #[test]
    fn intersects_existing_row_group_and_delete_selections() {
        let metadata = metadata_with_three_row_groups();
        let selection = RowPositionSelection::try_new(&metadata, 5).unwrap();
        let mut row_groups = Some(vec![0, 1]);
        selection.restrict_row_groups(&mut row_groups);

        let delete_selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(2),
        ]);
        let mut rows = Some(delete_selection);
        selection.merge_row_selection(&mut rows);

        assert_eq!(row_groups, Some(vec![1]));
        assert_eq!(
            Vec::<RowSelector>::from(rows.unwrap()),
            vec![RowSelector::skip(4)]
        );
    }
}
