//! Bound PostgreSQL-datum write buffers.
//!
//! Callers validate both their source layout and source OIDs; the buffers bind
//! every column's source codec and Arrow encoder once, then append rows
//! through a single bound-encoder slice.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use lagodb_core::batch::BatchBuffer;
use lagodb_core::datum::ColumnDatumTarget;
use lagodb_core::tuple::{SlotDatumIndex, TupleSlotRow};
use pgrx::pg_sys;

use super::{BoundDatumPlan, DatumEncoder};
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
}

/// Arrow encoders and flush accounting shared by the bound datum buffers.
struct DatumColumnBatch {
    schema: Arc<Schema>,
    encoders: Box<[DatumEncoder]>,
    rows: usize,
    estimated_bytes: usize,
}

impl DatumColumnBatch {
    /// Bind one encoder per schema field. The schema-wide capability is
    /// validated before any Arrow builder is allocated, so a writer that
    /// cannot accept rows never materializes runtime column state.
    fn bind(
        schema: Arc<Schema>,
        plans: &[BoundDatumPlan],
    ) -> ArrowConversionResult<Self> {
        if plans.len() != schema.fields().len() {
            return Err(ArrowConversionError::InvariantViolated(
                "bound datum column and Arrow schema counts differ",
            ));
        }
        if plans
            .iter()
            .any(BoundDatumPlan::requires_utf8_server_encoding)
        {
            ColumnDatumTarget::validate_utf8_server_encoding()?;
        }
        let encoders = plans
            .iter()
            .map(|plan| plan.encoder(DEFAULT_ROW_CAPACITY))
            .collect::<ArrowConversionResult<_>>()?;
        Ok(Self {
            schema,
            encoders,
            rows: 0,
            estimated_bytes: 0,
        })
    }

    /// # Safety
    /// `values` yields one value per encoder; each non-NULL datum is valid for
    /// its bound source OID and remains valid until this call returns.
    unsafe fn append_row(
        &mut self,
        values: impl Iterator<Item = Option<pg_sys::Datum>>,
    ) -> ArrowConversionResult<()> {
        for (encoder, value) in self.encoders.iter_mut().zip(values) {
            // Keep the estimate in partial-progress state if a later column
            // reports an invariant/value error during this row.
            match value {
                Some(datum) => {
                    self.estimated_bytes += unsafe { encoder.append(datum) }?;
                }
                None => encoder.append_null(),
            }
        }
        self.rows += 1;
        Ok(())
    }
}

impl BatchBuffer for DatumColumnBatch {
    type Batch = RecordBatch;
    type Error = ArrowConversionError;

    fn finish_batch(&mut self) -> ArrowConversionResult<RecordBatch> {
        if self.rows == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let rows = self.rows;
        let arrays = self
            .encoders
            .iter_mut()
            .map(|encoder| {
                assert_eq!(encoder.len(), rows);
                encoder.finish()
            })
            .collect::<ArrowConversionResult<Vec<_>>>()?;
        self.rows = 0;
        self.estimated_bytes = 0;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), arrays)?)
    }

    fn clear(&mut self) {
        for encoder in &mut self.encoders {
            encoder.clear();
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

/// A relation-bound Arrow batch buffer for tuple-slot mutation writes.
pub struct BoundWriteBuffer {
    batch: DatumColumnBatch,
    /// Source slot position of each encoder, in schema order.
    sources: Box<[SlotDatumIndex]>,
}

impl BoundWriteBuffer {
    /// Build a buffer from already-bound output columns in schema order.
    pub fn new(
        schema: Arc<Schema>,
        columns: Box<[BoundWriteColumnPlan]>,
    ) -> ArrowConversionResult<Self> {
        let (sources, plans): (Vec<_>, Vec<_>) = columns
            .into_vec()
            .into_iter()
            .map(|column| (column.index, column.plan))
            .unzip();
        Ok(Self {
            batch: DatumColumnBatch::bind(schema, &plans)?,
            sources: sources.into_boxed_slice(),
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
        let values = self.sources.iter().map(|&index| {
            // SAFETY: binding establishes the slot index and declared datum
            // type; the caller supplies that same layout for the buffer's
            // lifetime.
            let (raw, is_null) = unsafe { datums.datum_at_bound(index) };
            (!is_null).then_some(raw)
        });
        unsafe { self.batch.append_row(values) }
    }
}

/// Arrow batch buffer for an already-bound stream of PostgreSQL datums.
///
/// Unlike [`BoundWriteBuffer`], this buffer is not tied to a relation slot.
/// Callers establish the row width and datum OIDs while binding the
/// [`BoundDatumPlan`] values, then append callback-scoped datums without
/// constructing an intermediate slot or owned row.
pub struct BoundDatumBuffer {
    batch: DatumColumnBatch,
}

impl BoundDatumBuffer {
    pub fn new(
        schema: Arc<Schema>,
        plans: Box<[BoundDatumPlan]>,
    ) -> ArrowConversionResult<Self> {
        Ok(Self {
            batch: DatumColumnBatch::bind(schema, &plans)?,
        })
    }

    /// Append one row through the fixed column plan.
    ///
    /// # Safety
    ///
    /// `values` must yield exactly one value per bound column. Every non-NULL
    /// datum must be valid for the source OID of the corresponding
    /// [`BoundDatumPlan`]. Any referenced memory must remain valid until this
    /// call returns. The unchecked width contract keeps a duplicate row
    /// validation out of adapters that already parse a fixed-width protocol.
    pub unsafe fn append_row_unchecked(
        &mut self,
        values: impl Iterator<Item = Option<pg_sys::Datum>>,
    ) -> ArrowConversionResult<()> {
        unsafe { self.batch.append_row(values) }
    }
}

/// Both public buffers expose the shared batch lifecycle unchanged.
macro_rules! delegate_batch_buffer {
    ($($buffer:ty),+) => {$(
        impl BatchBuffer for $buffer {
            type Batch = RecordBatch;
            type Error = ArrowConversionError;

            fn finish_batch(&mut self) -> ArrowConversionResult<RecordBatch> {
                self.batch.finish_batch()
            }

            fn clear(&mut self) {
                self.batch.clear();
            }

            fn len(&self) -> usize {
                self.batch.len()
            }

            fn estimated_size(&self) -> usize {
                self.batch.estimated_size()
            }

            fn should_flush(&self, max_bytes: usize) -> bool {
                self.batch.should_flush(max_bytes)
            }
        }
    )+};
}

delegate_batch_buffer!(BoundWriteBuffer, BoundDatumBuffer);
