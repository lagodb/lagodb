//! Columnar write buffer: one [`ArrowColumnEncoder`] per column plus the bound
//! Arrow schema, producing a [`RecordBatch`] on flush. The write-path analogue
//! of the row-mode batch buffer, fed directly from PostgreSQL tuple slots.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use pg_lakebase_core::batch::{
    BatchBuffer, DatumColumnAppender, SlotColumnarBatchBuffer,
};
use pg_lakebase_core::tuple::PgDatumRef;

use crate::error::{ConvError, ConvResult};
use crate::rule::ColumnRule;
use crate::types::ArrowColumnEncoder;

/// Pre-size each column builder for a typical flush window so steady-state
/// appends reuse the initial allocation instead of growing per row.
const DEFAULT_ROW_CAPACITY: usize = 1024;

pub struct SlotRecordBatchBuffer {
    schema: Arc<Schema>,
    columns: Vec<ArrowColumnEncoder>,
    rows: usize,
}

impl SlotRecordBatchBuffer {
    /// Build a buffer from the bound Arrow schema and the per-column rules, in
    /// column order. `rules.len()` must equal `schema.fields().len()`.
    pub fn new(schema: Arc<Schema>, rules: &[ColumnRule]) -> Self {
        assert_eq!(rules.len(), schema.fields().len());
        let columns = rules
            .iter()
            .map(|rule| ArrowColumnEncoder::new(rule, DEFAULT_ROW_CAPACITY))
            .collect();
        Self {
            schema,
            columns,
            rows: 0,
        }
    }
}

impl BatchBuffer for SlotRecordBatchBuffer {
    type Batch = RecordBatch;
    type Error = ConvError;

    fn finish_batch(&mut self) -> ConvResult<RecordBatch> {
        // An empty batch must not touch the builders; emit the schema-only
        // batch that matches the legacy empty-flush behavior.
        if self.rows == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let arrays = self.finish_columns()?;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }

    fn clear(&mut self) {
        for column in &mut self.columns {
            column.clear();
        }
        self.rows = 0;
    }

    fn len(&self) -> usize {
        self.rows
    }

    fn estimated_size(&self) -> usize {
        self.columns.iter().map(|c| c.estimated_size()).sum()
    }
}

impl SlotColumnarBatchBuffer for SlotRecordBatchBuffer {
    type Column = ArrayRef;

    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn append_datum_to_column(
        &mut self,
        column_index: usize,
        value: Option<PgDatumRef<'_>>,
    ) -> ConvResult<()> {
        self.columns[column_index].append_datum(value)
    }

    fn finish_row(&mut self) -> ConvResult<()> {
        self.rows += 1;
        Ok(())
    }

    fn finish_columns(&mut self) -> ConvResult<Vec<ArrayRef>> {
        let rows = self.rows;
        let mut arrays = Vec::with_capacity(self.columns.len());
        for column in &mut self.columns {
            // NULL alignment guarantees every column holds exactly `rows`
            // slots, so `try_new` can never trip a length mismatch.
            assert_eq!(column.len(), rows);
            arrays.push(column.finish()?);
        }
        self.rows = 0;
        Ok(arrays)
    }
}
