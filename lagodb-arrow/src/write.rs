//! Relation-bound PostgreSQL-datum write plans.
//!
//! This module is the relation-bound write API for callers that have validated
//! both their relation layout and source OIDs: it binds the source codec and
//! the Arrow encoder together once, then appends rows through a single
//! bound-column slice.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use lagodb_core::batch::BatchBuffer;
use lagodb_core::datum::ColumnDatumTarget;
use lagodb_core::tuple::{SlotDatumIndex, SlotDatumView, TupleSlotRow};
use pgrx::pg_sys;

use crate::BoundDatumPlan;
use crate::datum_codec::DatumEncoder;
use crate::error::{ArrowConversionError, ArrowConversionResult};

const DEFAULT_ROW_CAPACITY: usize = 1024;
// Payload estimates omit offsets and validity buffers. Bound the row count
// independently so NULL and empty values cannot accumulate until statement end.
// Both flush checks use existing counters, without inspecting column builders.
const MAX_BUFFERED_ROWS: usize = 65_536;

/// A declared Datum plan bound to one mandatory source slot.
/// Providers establish complete output-field coverage before constructing it.
pub struct BoundWriteColumnPlan {
    index: SlotDatumIndex,
    plan: BoundDatumPlan,
}

/// A declared Datum plan for a stream without tuple-slot positions (COPY).
pub struct BoundDatumColumnPlan {
    plan: BoundDatumPlan,
}

impl BoundDatumColumnPlan {
    pub fn new(plan: BoundDatumPlan) -> Self {
        Self { plan }
    }
}

impl BoundWriteColumnPlan {
    /// Bind a mandatory source position, checked once against the slot width.
    pub fn bind(
        plan: BoundDatumPlan,
        source_slot: usize,
        slot_width: usize,
    ) -> ArrowConversionResult<Self> {
        let index = SlotDatumIndex::new(source_slot, slot_width).ok_or(
            ArrowConversionError::InvariantViolated(
                "bound write source index is outside the relation slot width",
            ),
        )?;
        Ok(Self { index, plan })
    }

    fn into_column(self) -> ArrowConversionResult<BoundWriteColumn> {
        Ok(BoundWriteColumn {
            index: self.index,
            encoder: self.plan.encoder(DEFAULT_ROW_CAPACITY)?,
        })
    }
}

struct BoundWriteColumn {
    index: SlotDatumIndex,
    encoder: DatumEncoder,
}

impl BoundWriteColumn {
    fn append(&mut self, datums: &SlotDatumView<'_>) -> ArrowConversionResult<usize> {
        // SAFETY: binding establishes the slot index and declared datum type;
        // the caller supplies that same layout for the buffer's lifetime.
        let (raw, is_null) = unsafe { datums.datum_at_bound(self.index) };
        if is_null {
            self.encoder.append_null();
            Ok(0)
        } else {
            unsafe { self.encoder.append(raw) }
        }
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        self.encoder.finish()
    }

    fn clear(&mut self) {
        self.encoder.clear();
    }

    fn len(&self) -> usize {
        self.encoder.len()
    }
}

/// A relation-bound Arrow batch buffer for tuple-slot mutation writes.
pub struct BoundWriteBuffer {
    schema: Arc<Schema>,
    columns: Box<[BoundWriteColumn]>,
    rows: usize,
    estimated_bytes: usize,
}

/// Arrow batch buffer for an already-bound stream of PostgreSQL datums.
///
/// Unlike [`BoundWriteBuffer`], this buffer is not tied to a relation slot.
/// Callers establish the row width and datum OIDs while constructing
/// [`BoundDatumColumnPlan`] values, then append callback-scoped datums without
/// constructing an intermediate slot or owned row.
pub struct BoundDatumBuffer {
    schema: Arc<Schema>,
    columns: Box<[DatumEncoder]>,
    rows: usize,
    estimated_bytes: usize,
}

impl BoundDatumBuffer {
    pub fn new(
        schema: Arc<Schema>,
        plans: Box<[BoundDatumColumnPlan]>,
    ) -> ArrowConversionResult<Self> {
        if plans.len() != schema.fields().len() {
            return Err(ArrowConversionError::InvariantViolated(
                "bound datum column and Arrow schema counts differ",
            ));
        }
        if plans
            .iter()
            .any(|plan| plan.plan.requires_utf8_server_encoding())
        {
            ColumnDatumTarget::validate_utf8_server_encoding()
                .map_err(ArrowConversionError::from)?;
        }
        let columns = plans
            .into_vec()
            .into_iter()
            .map(|plan| plan.plan.encoder(DEFAULT_ROW_CAPACITY))
            .collect::<ArrowConversionResult<Vec<_>>>()?
            .into_boxed_slice();
        Ok(Self {
            schema,
            columns,
            rows: 0,
            estimated_bytes: 0,
        })
    }

    /// Append one row through the fixed column plan.
    ///
    /// # Safety
    ///
    /// `values` must yield exactly one value per bound column. Every non-NULL
    /// datum must be valid for the source OID supplied to the corresponding
    /// [`BoundDatumColumnPlan`]. Any referenced memory must remain valid until
    /// this call returns. The unchecked width contract keeps a duplicate row
    /// validation out of adapters that already parse a fixed-width protocol.
    pub unsafe fn append_row_unchecked(
        &mut self,
        values: impl Iterator<Item = Option<pg_sys::Datum>>,
    ) -> ArrowConversionResult<()> {
        for (column, value) in self.columns.iter_mut().zip(values) {
            match value {
                Some(datum) => {
                    self.estimated_bytes += unsafe { column.append(datum) }?;
                }
                None => column.append_null(),
            }
        }
        self.rows += 1;
        Ok(())
    }

    fn finish_columns(&mut self) -> ArrowConversionResult<Vec<ArrayRef>> {
        let rows = self.rows;
        let mut arrays = Vec::with_capacity(self.columns.len());
        for column in &mut self.columns {
            assert_eq!(column.len(), rows);
            arrays.push(column.finish()?);
        }
        self.rows = 0;
        self.estimated_bytes = 0;
        Ok(arrays)
    }
}

impl BatchBuffer for BoundDatumBuffer {
    type Batch = RecordBatch;
    type Error = ArrowConversionError;

    fn finish_batch(&mut self) -> ArrowConversionResult<RecordBatch> {
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
        self.estimated_bytes = 0;
    }

    fn len(&self) -> usize {
        self.rows
    }

    fn estimated_size(&self) -> usize {
        self.estimated_bytes
    }

    fn should_flush(&self, max_bytes: usize) -> bool {
        self.estimated_bytes >= max_bytes || self.rows >= MAX_BUFFERED_ROWS
    }
}

impl BoundWriteBuffer {
    /// Build a buffer from already-bound output columns in schema order.
    pub fn new(
        schema: Arc<Schema>,
        plans: Box<[BoundWriteColumnPlan]>,
    ) -> ArrowConversionResult<Self> {
        if plans.len() != schema.fields().len() {
            return Err(ArrowConversionError::InvariantViolated(
                "bound write column and Arrow schema counts differ",
            ));
        }
        // Validate the relation-wide capability before materializing any
        // Arrow builders. A failed capability check must not allocate the
        // runtime column state for a writer that cannot accept rows.
        if plans
            .iter()
            .any(|plan| plan.plan.requires_utf8_server_encoding())
        {
            ColumnDatumTarget::validate_utf8_server_encoding()
                .map_err(ArrowConversionError::from)?;
        }
        let columns = plans
            .into_vec()
            .into_iter()
            .map(BoundWriteColumnPlan::into_column)
            .collect::<ArrowConversionResult<Vec<_>>>()?
            .into_boxed_slice();
        Ok(Self {
            schema,
            columns,
            rows: 0,
            estimated_bytes: 0,
        })
    }

    /// Append one row through the fixed bound-column plan.
    ///
    /// # Safety
    ///
    /// `row` must come from the same relation tuple layout used to create the
    /// plans: its tuple width and every source attribute OID must match the
    /// bound relation. The callback-scoped row must also remain valid for the
    /// duration of this call. The plan deliberately omits a per-row descriptor
    /// identity check so the validated relation invariant stays out of the
    /// hot path.
    ///
    /// Every non-NULL source datum must be a valid PostgreSQL internal value
    /// for its bound source OID. For text/name values and string-array
    /// elements, the validated PG_UTF8 server-encoding invariant must hold.
    /// The bound codecs rely on these conditions when they construct UTF-8
    /// string views without rescanning each value.
    pub unsafe fn append_slot_row(
        &mut self,
        row: TupleSlotRow<'_>,
    ) -> ArrowConversionResult<()> {
        let datums = row.datums();
        for column in &mut self.columns {
            // Keep the estimate in the same partial-progress state as the
            // generic buffer if a later column reports an invariant/value
            // error during this row.
            self.estimated_bytes += column.append(&datums)?;
        }
        self.rows += 1;
        Ok(())
    }

    fn finish_columns(&mut self) -> ArrowConversionResult<Vec<ArrayRef>> {
        let rows = self.rows;
        let mut arrays = Vec::with_capacity(self.columns.len());
        for column in &mut self.columns {
            assert_eq!(column.len(), rows);
            arrays.push(column.finish()?);
        }
        self.rows = 0;
        self.estimated_bytes = 0;
        Ok(arrays)
    }
}

impl BatchBuffer for BoundWriteBuffer {
    type Batch = RecordBatch;
    type Error = ArrowConversionError;

    fn finish_batch(&mut self) -> ArrowConversionResult<RecordBatch> {
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
        self.estimated_bytes = 0;
    }

    fn len(&self) -> usize {
        self.rows
    }

    fn estimated_size(&self) -> usize {
        self.estimated_bytes
    }

    fn should_flush(&self, max_bytes: usize) -> bool {
        self.estimated_bytes >= max_bytes || self.rows >= MAX_BUFFERED_ROWS
    }
}
