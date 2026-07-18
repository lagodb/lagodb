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

//! Parquet row-group and row-selection constraints for batch point reads.

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;

use crate::{Error, ErrorKind, Result};

/// Restricts a Parquet read to selected original, zero-based file positions.
pub(super) struct RowPositionsSelection {
    groups: Vec<RowGroupPositions>,
}

struct RowGroupPositions {
    index: usize,
    selectors: Vec<RowSelector>,
}

impl RowPositionsSelection {
    pub(super) fn try_new(
        metadata: &ParquetMetaData,
        positions: &[i64],
    ) -> Result<Self> {
        if positions.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "row-position selection must not be empty",
            ));
        }
        if positions[0] < 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "row position must not be negative",
            ));
        }
        if positions.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "row positions must be strictly increasing",
            ));
        }

        let mut row_group_start = 0_i64;
        let mut position_index = 0;
        let mut groups = Vec::new();
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

            let first = position_index;
            while position_index < positions.len()
                && positions[position_index] < row_group_end
            {
                position_index += 1;
            }
            if first != position_index {
                let row_count = usize::try_from(row_count).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "parquet row-group size does not fit platform index range",
                    )
                })?;
                groups.push(RowGroupPositions {
                    index: row_group_index,
                    selectors: Self::selectors(
                        &positions[first..position_index],
                        row_group_start,
                        row_count,
                    )?,
                });
            }
            row_group_start = row_group_end;
        }

        if position_index != positions.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "row position exceeds parquet footer row count",
            ));
        }
        Ok(Self { groups })
    }

    /// Intersects an existing row-group restriction with the target groups.
    pub(super) fn restrict_row_groups(
        &self,
        selected_row_groups: &mut Option<Vec<usize>>,
    ) {
        match selected_row_groups {
            Some(indices) => indices.retain(|index| {
                self.groups
                    .binary_search_by_key(index, |group| group.index)
                    .is_ok()
            }),
            None => {
                *selected_row_groups =
                    Some(self.groups.iter().map(|group| group.index).collect())
            }
        }
    }

    /// Intersects an existing row selection with the position constraint.
    pub(super) fn merge_row_selection(
        self,
        selected_row_groups: &Option<Vec<usize>>,
        row_selection: &mut Option<RowSelection>,
    ) {
        let selected = selected_row_groups.as_deref();
        let selectors = self
            .groups
            .into_iter()
            .filter(|group| {
                selected
                    .is_none_or(|indices| indices.binary_search(&group.index).is_ok())
            })
            .flat_map(|group| group.selectors)
            .collect::<Vec<_>>();
        let position_selection = RowSelection::from(selectors);
        *row_selection = Some(match row_selection.take() {
            Some(existing) => existing.intersection(&position_selection),
            None => position_selection,
        });
    }

    fn selectors(
        positions: &[i64],
        row_group_start: i64,
        row_count: usize,
    ) -> Result<Vec<RowSelector>> {
        let mut selectors = Vec::with_capacity(positions.len() * 2 + 1);
        let mut cursor = 0;
        for position in positions {
            let offset =
                usize::try_from(*position - row_group_start).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "row position does not fit platform index range",
                    )
                })?;
            let skipped = offset - cursor;
            if skipped != 0 {
                selectors.push(RowSelector::skip(skipped));
            }
            selectors.push(RowSelector::select(1));
            cursor = offset + 1;
        }
        let trailing_rows = row_count - cursor;
        if trailing_rows != 0 {
            selectors.push(RowSelector::skip(trailing_rows));
        }
        Ok(selectors)
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
    fn batches_positions_across_row_groups_in_order() {
        let metadata = metadata_with_three_row_groups();
        let selection =
            RowPositionsSelection::try_new(&metadata, &[1, 3, 4, 9]).unwrap();

        assert_eq!(
            selection
                .groups
                .iter()
                .map(|group| group.index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            selection.groups[0].selectors,
            vec![
                RowSelector::skip(1),
                RowSelector::select(1),
                RowSelector::skip(1),
                RowSelector::select(1),
            ]
        );
    }

    #[test]
    fn intersects_existing_row_groups_and_row_selection() {
        let metadata = metadata_with_three_row_groups();
        let selection =
            RowPositionsSelection::try_new(&metadata, &[1, 5, 9]).unwrap();
        let mut row_groups = Some(vec![1, 2]);
        selection.restrict_row_groups(&mut row_groups);
        let mut rows = Some(RowSelection::from(vec![RowSelector::select(6)]));
        selection.merge_row_selection(&row_groups, &mut rows);

        assert_eq!(row_groups, Some(vec![1, 2]));
        assert_eq!(
            Vec::<RowSelector>::from(rows.unwrap()),
            vec![
                RowSelector::skip(1),
                RowSelector::select(1),
                RowSelector::skip(3),
                RowSelector::select(1),
            ]
        );
    }

    #[test]
    fn rejects_invalid_positions() {
        let metadata = metadata_with_three_row_groups();

        for positions in [&[][..], &[-1][..], &[1, 1][..], &[2, 1][..], &[10][..]] {
            assert!(RowPositionsSelection::try_new(&metadata, positions).is_err());
        }
    }
}
