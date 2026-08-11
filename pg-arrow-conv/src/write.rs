//! Relation-bound PostgreSQL-datum write plans.
//!
//! This module is the relation-bound write API for callers that have validated
//! both their relation layout and source OIDs: it binds the source codec and
//! the Arrow encoder together once, then appends rows through a single
//! bound-column slice.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use pg_lakebase_core::batch::BatchBuffer;
use pg_lakebase_core::tuple::{
    ColumnDatumTarget, SlotDatumIndex, SlotDatums, TupleSlotRow,
};
use pgrx::pg_sys;

use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::rule::ColumnRule;
use crate::types::{ArrowColumnEncoder, BoundColumnEncoder, BoundEncoderPlan};

const DEFAULT_ROW_CAPACITY: usize = 1024;

/// One validated output-column plan without runtime Arrow builder state.
///
/// Keeping this plan separate from [`BoundWriteBuffer`] lets construction check
/// relation-wide capabilities before allocating any per-column builders.
enum BoundWriteInput {
    Null {
        rule: ColumnRule,
    },
    Datum {
        index: SlotDatumIndex,
        plan: BoundEncoderPlan,
    },
}

pub struct BoundWriteColumnPlan {
    input: BoundWriteInput,
    requires_utf8: bool,
}

/// One validated PostgreSQL-datum input plan without a tuple-slot position.
///
/// This is the companion to [`BoundWriteColumnPlan`] for adapters that already
/// own a typed datum stream, such as a COPY bridge.  The PostgreSQL source OID
/// and Arrow rule are bound once; row appends perform no OID lookup or Arrow
/// type dispatch.
pub struct BoundDatumColumnPlan {
    plan: BoundEncoderPlan,
    requires_utf8: bool,
}

impl BoundDatumColumnPlan {
    pub fn bind(
        rule: ColumnRule,
        source_oid: pg_sys::Oid,
    ) -> ArrowConversionResult<Self> {
        let requires_utf8 = rule.requires_utf8_server_encoding();
        Ok(Self {
            plan: BoundEncoderPlan::bind(rule, source_oid)?,
            requires_utf8,
        })
    }
}

impl BoundWriteColumnPlan {
    /// Bind one output rule to an optional source slot.
    ///
    /// `source_slot` is zero-based and is checked against the complete
    /// relation width.  `source_oid` must be present exactly when
    /// `source_slot` is present; an absent pair represents a typed all-NULL
    /// output column from a missing optional source.
    pub fn bind(
        rule: ColumnRule,
        source_slot: Option<usize>,
        source_oid: Option<pg_sys::Oid>,
        slot_width: usize,
    ) -> ArrowConversionResult<Self> {
        let requires_utf8 = rule.requires_utf8_server_encoding();
        let input = match (source_slot, source_oid) {
            (None, None) => BoundWriteInput::Null { rule },
            (Some(index), Some(oid)) => {
                let index = SlotDatumIndex::new(index, slot_width).ok_or(
                    ArrowConversionError::InvariantViolated(
                        "bound write source index is outside the relation slot width",
                    ),
                )?;
                BoundWriteInput::Datum {
                    index,
                    plan: BoundEncoderPlan::bind(rule, oid)?,
                }
            }
            _ => {
                return Err(ArrowConversionError::InvariantViolated(
                    "bound write source index and source OID must be paired",
                ));
            }
        };
        Ok(Self {
            input,
            requires_utf8,
        })
    }

    fn into_column(self) -> BoundWriteColumn {
        match self.input {
            BoundWriteInput::Null { rule } => BoundWriteColumn::Null(
                ArrowColumnEncoder::new(&rule, DEFAULT_ROW_CAPACITY),
            ),
            BoundWriteInput::Datum { index, plan } => BoundWriteColumn::Datum {
                index,
                encoder: plan.materialize(DEFAULT_ROW_CAPACITY),
            },
        }
    }
}

/// Runtime state for one bound output column.
enum BoundWriteColumn {
    Null(ArrowColumnEncoder),
    Datum {
        index: SlotDatumIndex,
        encoder: BoundColumnEncoder,
    },
}

impl BoundWriteColumn {
    fn append(&mut self, datums: &SlotDatums<'_>) -> ArrowConversionResult<usize> {
        match self {
            Self::Null(encoder) => {
                encoder.append_null();
                Ok(0)
            }
            Self::Datum { index, encoder } => {
                // SAFETY: the index was validated against the relation width
                // while this column was bound, and the callback supplies a slot
                // of that same relation layout. The raw accessor reads only the
                // two validated arrays.
                let (raw, is_null) = unsafe { datums.datum_at_bound(*index) };
                if is_null {
                    encoder.append_null();
                    Ok(0)
                } else {
                    // SAFETY: the bound source codec and Arrow encoder were
                    // selected together during plan construction; `raw` is the
                    // corresponding non-NULL slot datum.
                    unsafe { encoder.append(raw) }
                }
            }
        }
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        match self {
            Self::Null(encoder) => encoder.finish(),
            Self::Datum { encoder, .. } => encoder.finish(),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Null(encoder) => encoder.clear(),
            Self::Datum { encoder, .. } => encoder.clear(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Null(encoder) => encoder.len(),
            Self::Datum { encoder, .. } => encoder.len(),
        }
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
    columns: Box<[BoundColumnEncoder]>,
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
        if plans.iter().any(|plan| plan.requires_utf8) {
            ColumnDatumTarget::validate_utf8_server_encoding()
                .map_err(ArrowConversionError::from)?;
        }
        let columns = plans
            .into_vec()
            .into_iter()
            .map(|plan| plan.plan.materialize(DEFAULT_ROW_CAPACITY))
            .collect::<Vec<_>>()
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
        if plans.iter().any(|plan| plan.requires_utf8) {
            ColumnDatumTarget::validate_utf8_server_encoding()
                .map_err(ArrowConversionError::from)?;
        }
        let columns = plans
            .into_vec()
            .into_iter()
            .map(BoundWriteColumnPlan::into_column)
            .collect::<Vec<_>>()
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
}
