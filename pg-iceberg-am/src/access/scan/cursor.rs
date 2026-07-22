//! Arrow batch adaptation and slot-first scan cursors.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, RunArray, StringArray};
use iceberg_lite::metadata_columns::{
    RESERVED_COL_NAME_FILE, RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_FILE,
    RESERVED_FIELD_ID_POS,
};
use iceberg_lite::scan::ArrowRecordBatchIterator;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pg_arrow_conv::{ArrowBatchSource, ArrowColumnDecoder, BoundBatch};
use pg_lakebase_core::access::mutation::ModifyScanBinding;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use crate::access::mutation::{IcebergFileSource, IcebergModifyQueryState};
use crate::catalog::row_mutations::IcebergFileId;
use crate::error::{IcebergError, IcebergResult};

/// Adapts the Iceberg Arrow batch iterator into the conversion crate's batch
/// source. The producer error (`iceberg_lite::Error`: IO, Parquet, metadata,
/// schema) is preserved as an [`IcebergError`] so it reaches the callback
/// boundary with its own SQLSTATE (IO/internal/feature) rather than being
/// reclassified as a `ConvError::DatumConversionError` (`DATA_EXCEPTION`).
/// `pg-arrow-conv` stays format-neutral: it only requires the error to map into
/// the boundary error, which `IcebergError` already does.
pub(crate) struct IcebergArrowBatches(pub(crate) ArrowRecordBatchIterator);

impl Iterator for IcebergArrowBatches {
    type Item = Result<RecordBatch, IcebergError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Cooperative cancellation at the batch boundary. PG's `ExecScanFetch`
        // already fires `CHECK_FOR_INTERRUPTS` once per *returned* tuple for both
        // the TableAM seqscan and the CustomScan, but a single `getnextslot` /
        // `next_slot` call can pull many batches here (skipping batches fully
        // eliminated by pushed filters, or reading the next Parquet row group),
        // so a query cancel issued mid-IO would otherwise wait until the next
        // tuple surfaces. Checking per batch — the unit of Iceberg scan IO —
        // closes that gap for both scan paths, which share this iterator.
        pgrx::pg_sys::check_for_interrupts!();
        self.0.next().map(|batch| batch.map_err(IcebergError::from))
    }
}

pub(crate) type IcebergArrowBatchSource =
    ArrowBatchSource<IcebergArrowBatches, IcebergError>;

struct IcebergBoundBatch {
    decoded: BoundBatch,
    metadata: Option<BatchMetadataColumns>,
    file_runs: Option<Box<[RegisteredFileRun]>>,
}

struct RegisteredFileRun {
    end_row: usize,
    file_id: IcebergFileId,
}

#[derive(Clone)]
struct ModifyCursorContext {
    binding: ModifyScanBinding<IcebergModifyQueryState>,
    table_oid: pg_sys::Oid,
    /// Fast path for adjacent runs/batches from the same planned file. This
    /// avoids re-hashing a long file path while retaining the transaction
    /// registry as the sole identity authority.
    last_file: Option<(Box<str>, IcebergFileId)>,
}

impl ModifyCursorContext {
    fn register_file(&mut self, path: &str) -> AmResult<IcebergFileId> {
        if let Some((cached_path, file_id)) = self.last_file.as_ref()
            && cached_path.as_ref() == path
        {
            return Ok(*file_id);
        }
        let source = IcebergFileSource::new(path);
        let file_id = self.binding.register_identity_source(&source)?;
        self.last_file = Some((path.into(), file_id));
        Ok(file_id)
    }
}

#[derive(Clone)]
enum MetadataStringColumn {
    Plain(StringArray),
    RunEndEncoded(RunArray<Int32Type>),
}

impl MetadataStringColumn {
    fn try_new(array: ArrayRef, name: &'static str) -> AmResult<Self> {
        if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Plain(strings.clone()));
        }

        if let Some(run_array) = array.as_any().downcast_ref::<RunArray<Int32Type>>()
            && run_array.values().as_any().is::<StringArray>()
        {
            return Ok(Self::RunEndEncoded(run_array.clone()));
        }

        Err(IcebergError::ArrowTypeMismatch(format!(
            "metadata column {name} has unexpected Arrow type {:?}",
            array.data_type()
        ))
        .into())
    }

    fn value(&self, row: usize) -> IcebergResult<&str> {
        let (values, index) = match self {
            Self::Plain(values) => (values, row),
            Self::RunEndEncoded(runs) => {
                let values =
                    runs.values().as_any().downcast_ref::<StringArray>().expect(
                        "metadata string column values type checked at construction",
                    );
                (values, runs.get_physical_index(row))
            }
        };
        if values.is_null(index) {
            return Err(IcebergError::InvariantViolated(
                "row-location file metadata cannot be NULL",
            ));
        }
        Ok(values.value(index))
    }

    /// Visit contiguous logical runs without expanding run-end encoding.
    fn try_for_each_run<E>(
        &self,
        mut visit: impl FnMut(usize, &str) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<IcebergError>,
    {
        match self {
            Self::Plain(strings) => {
                let mut start = 0;
                while start < strings.len() {
                    if strings.is_null(start) {
                        return Err(IcebergError::InvariantViolated(
                            "Row identity file cannot be NULL",
                        )
                        .into());
                    }
                    let path = strings.value(start);
                    let mut end = start + 1;
                    while end < strings.len()
                        && !strings.is_null(end)
                        && strings.value(end) == path
                    {
                        end += 1;
                    }
                    visit(end, path)?;
                    start = end;
                }
            }
            Self::RunEndEncoded(runs) => {
                let values =
                    runs.values().as_any().downcast_ref::<StringArray>().expect(
                        "metadata string column values type checked at construction",
                    );
                let first_value = runs.get_start_physical_index();
                for (value_idx, end_row) in
                    (first_value..).zip(runs.run_ends().sliced_values())
                {
                    if values.is_null(value_idx) {
                        return Err(IcebergError::InvariantViolated(
                            "Row identity file cannot be NULL",
                        )
                        .into());
                    }
                    let end_row = usize::try_from(end_row).map_err(|_| {
                        IcebergError::InvariantViolated(
                            "Row identity run end cannot be negative",
                        )
                    })?;
                    visit(end_row, values.value(value_idx))?;
                }
            }
        }
        Ok(())
    }
}

/// Typed access to Iceberg's `_file` and `_pos` virtual columns.
///
/// Both Modify and ANALYZE cursors use this object so metadata field-id lookup,
/// Arrow type validation, NULL handling, and run-end decoding have one owner.
pub(crate) struct BatchMetadataColumns {
    files: MetadataStringColumn,
    positions: Int64Array,
}

impl BatchMetadataColumns {
    pub(crate) fn try_new(batch: &RecordBatch) -> AmResult<Self> {
        let files = MetadataStringColumn::try_new(
            Self::metadata_column_ref(
                batch,
                RESERVED_FIELD_ID_FILE,
                RESERVED_COL_NAME_FILE,
            )?,
            RESERVED_COL_NAME_FILE,
        )?;
        let positions = Self::typed_metadata_column::<Int64Array>(
            batch,
            RESERVED_FIELD_ID_POS,
            RESERVED_COL_NAME_POS,
        )?;
        Ok(Self { files, positions })
    }

    pub(crate) fn file(&self, row: usize) -> IcebergResult<&str> {
        self.files.value(row)
    }

    pub(crate) fn position(&self, row: usize) -> IcebergResult<u64> {
        if self.positions.is_null(row) {
            return Err(IcebergError::InvariantViolated(
                "row-location position metadata cannot be NULL",
            ));
        }
        u64::try_from(self.positions.value(row)).map_err(|_| {
            IcebergError::InvariantViolated(
                "row-location position cannot be negative",
            )
        })
    }

    fn files(&self) -> &MetadataStringColumn {
        &self.files
    }

    fn typed_metadata_column<T: Array + Clone + 'static>(
        batch: &RecordBatch,
        field_id: i32,
        name: &'static str,
    ) -> AmResult<T> {
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| {
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|raw| raw.parse::<i32>().ok())
                    == Some(field_id)
            })
            .ok_or(IcebergError::InvariantViolated(
                "row-location metadata column is missing from mutation scan",
            ))?;
        let array = batch.column(index);
        array.as_any().downcast_ref::<T>().cloned().ok_or_else(|| {
            IcebergError::ArrowTypeMismatch(format!(
                "metadata column {name} has unexpected Arrow type {:?}",
                array.data_type()
            ))
            .into()
        })
    }

    fn metadata_column_ref(
        batch: &RecordBatch,
        field_id: i32,
        _name: &'static str,
    ) -> AmResult<ArrayRef> {
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| {
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|raw| raw.parse::<i32>().ok())
                    == Some(field_id)
            })
            .ok_or(IcebergError::InvariantViolated(
                "row-location metadata column is missing from mutation scan",
            ))?;
        Ok(Arc::clone(batch.column(index)))
    }
}

/// Arrow batches decoded straight into the slot. Provider Modify mode consumes
/// `_file`/`_pos` internally to synthesize the PostgreSQL row-identity column.
pub struct IcebergBatchCursor {
    source: IcebergArrowBatchSource,
    decoder: ArrowColumnDecoder,
    current: Option<IcebergBoundBatch>,
    row_idx: usize,
    file_run_idx: usize,
    modify: Option<ModifyCursorContext>,
}

impl IcebergBatchCursor {
    pub(super) fn query(
        source: IcebergArrowBatchSource,
        decoder: ArrowColumnDecoder,
    ) -> Self {
        Self {
            source,
            decoder,
            current: None,
            row_idx: 0,
            file_run_idx: 0,
            modify: None,
        }
    }

    pub(super) fn mutation(
        source: IcebergArrowBatchSource,
        decoder: ArrowColumnDecoder,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
        table_oid: pg_sys::Oid,
    ) -> Self {
        Self {
            source,
            decoder,
            current: None,
            row_idx: 0,
            file_run_idx: 0,
            modify: Some(ModifyCursorContext {
                binding,
                table_oid,
                last_file: None,
            }),
        }
    }

    fn bind_batch(&mut self, batch: RecordBatch) -> AmResult<IcebergBoundBatch> {
        let metadata = if self.modify.is_some() {
            Some(BatchMetadataColumns::try_new(&batch)?)
        } else {
            None
        };

        let file_runs = match (&metadata, self.modify.as_mut()) {
            (Some(metadata), Some(modify)) => {
                Some(Self::register_file_runs(metadata.files(), modify)?)
            }
            (None, None) => None,
            _ => {
                return Err(IcebergError::InvariantViolated(
                    "row-location columns and Modify binding disagree",
                )
                .into());
            }
        };
        let decoded = self.decoder.bind(batch)?;
        Ok(IcebergBoundBatch {
            decoded,
            metadata,
            file_runs,
        })
    }

    fn register_file_runs(
        files: &MetadataStringColumn,
        modify: &mut ModifyCursorContext,
    ) -> AmResult<Box<[RegisteredFileRun]>> {
        let mut runs = Vec::new();
        files.try_for_each_run(|end_row, path| -> AmResult<()> {
            let file_id = modify.register_file(path)?;
            runs.push(RegisteredFileRun { end_row, file_id });
            Ok(())
        })?;
        Ok(runs.into_boxed_slice())
    }

    /// Emit one modification row and encode its Iceberg row identity into the
    /// PostgreSQL `ctid` carried by the plan.
    pub(crate) fn next_mutation_into_slot(
        &mut self,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<bool> {
        let table_oid = self
            .modify
            .as_ref()
            .ok_or(IcebergError::InvariantViolated(
                "mutation cursor has no Modify binding",
            ))?
            .table_oid;
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_idx < self.decoder.num_rows(&bound.decoded)
            {
                let row_idx = self.row_idx;
                self.decoder.write_row(&bound.decoded, row_idx, out)?;
                let metadata = bound.metadata.as_ref().ok_or(
                    IcebergError::InvariantViolated(
                        "Modify scan is missing row-location metadata",
                    ),
                )?;
                let position = metadata.position(row_idx)?;

                let runs = bound.file_runs.as_ref().ok_or(
                    IcebergError::InvariantViolated(
                        "Modify scan has no registered file runs",
                    ),
                )?;
                while self.file_run_idx < runs.len()
                    && row_idx >= runs[self.file_run_idx].end_row
                {
                    self.file_run_idx += 1;
                }
                let run = runs.get(self.file_run_idx).ok_or(
                    IcebergError::InvariantViolated(
                        "Modify row has no registered file identity",
                    ),
                )?;
                let tid = IcebergModifyQueryState::encode_row_identity(
                    run.file_id,
                    &position,
                )?;
                out.set_tid(&tid);
                out.set_table_oid(table_oid);
                self.row_idx += 1;
                return Ok(true);
            }

            self.current = None;
            match self.source.next_batch()? {
                Some(batch) => {
                    self.current = Some(self.bind_batch(batch)?);
                    self.row_idx = 0;
                    self.file_run_idx = 0;
                }
                None => return Ok(false),
            }
        }
    }
}

impl ScanBatchDriver for IcebergBatchCursor {
    fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool> {
        if self.modify.is_some() {
            return self.next_mutation_into_slot(out);
        }
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_idx < self.decoder.num_rows(&bound.decoded)
            {
                self.decoder.write_row(&bound.decoded, self.row_idx, out)?;
                self.row_idx += 1;
                return Ok(true);
            }

            self.current = None;
            match self.source.next_batch()? {
                Some(batch) => {
                    self.current = Some(self.bind_batch(batch)?);
                    self.row_idx = 0;
                    self.file_run_idx = 0;
                }
                None => return Ok(false),
            }
        }
    }
}

#[cfg(test)]
mod metadata_column_tests {
    use super::*;
    use arrow_array::Int32Array;

    #[test]
    fn plain_strings_are_grouped_into_logical_runs() {
        let column = MetadataStringColumn::Plain(StringArray::from(vec![
            "a.parquet",
            "a.parquet",
            "b.parquet",
        ]));
        let mut actual = Vec::new();

        column
            .try_for_each_run(|end_row, path| -> IcebergResult<()> {
                actual.push((end_row, path.to_owned()));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            actual,
            vec![(2, "a.parquet".to_owned()), (3, "b.parquet".to_owned())]
        );
    }

    #[test]
    fn run_end_encoded_strings_visit_physical_runs_only() {
        let run_ends = Int32Array::from(vec![2, 5, 6]);
        let values = StringArray::from(vec!["a.parquet", "b.parquet", "c.parquet"]);
        let runs = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let column = MetadataStringColumn::RunEndEncoded(runs.slice(1, 4));
        let mut actual = Vec::new();

        column
            .try_for_each_run(|end_row, path| -> IcebergResult<()> {
                actual.push((end_row, path.to_owned()));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            actual,
            vec![(1, "a.parquet".to_owned()), (4, "b.parquet".to_owned())]
        );
    }

    #[test]
    fn run_end_encoded_null_file_is_rejected_without_expansion() {
        let run_ends = Int32Array::from(vec![4]);
        let values = StringArray::from(vec![None::<&str>]);
        let runs = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let column = MetadataStringColumn::RunEndEncoded(runs);

        assert!(
            column
                .try_for_each_run(|_, _| -> IcebergResult<()> { Ok(()) })
                .is_err()
        );
    }
}
