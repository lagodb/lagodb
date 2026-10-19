//! Arrow output binding for PostgreSQL query slots.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use lagodb_arrow::{
    ArrowColumnDecoder, BoundBatch, BoundDatumPlan, CellRule, DatumCodec,
    DecodedColumn,
};
use lagodb_core::datum::PgType;
use lagodb_core::diag::PgReportError;
use lagodb_core::tuple::SlotDatumWriter;
use pgrx::{PgMemoryContexts, pg_sys};

use crate::plan::QueryTupleLayout;

use super::super::error::QueryExecutionError;

/// Arrow output columns bound once to the plan's PostgreSQL slot layout.
pub(super) struct QueryOutputDecoder {
    decoder: ArrowColumnDecoder,
    nullable: Box<[bool]>,
    width: usize,
    requires_datum_context: bool,
}

impl QueryOutputDecoder {
    pub(super) fn try_new(
        layout: &QueryTupleLayout,
        schema: &SchemaRef,
    ) -> Result<Self, QueryExecutionError> {
        if schema.fields().len() != layout.len() {
            return Err(QueryExecutionError::InvalidQueryOutput {
                columns: schema.fields().len(),
                rows: 0,
            });
        }
        let mut columns = Vec::with_capacity(layout.len());
        let mut nullable = Vec::with_capacity(layout.len());
        for (position, (field, slot)) in
            schema.fields().iter().zip(layout.slots()).enumerate()
        {
            let physical = match (slot.type_oid(), field.data_type()) {
                (pg_sys::NUMERICOID, DataType::Binary) => {
                    // SAFETY: only LagoDB's numeric UDAF emits this complete
                    // detoasted PostgreSQL NUMERIC varlena.
                    Some((CellRule::Binary, unsafe {
                        DatumCodec::postgres_numeric_varlena()
                    }))
                }
                (pg_sys::NUMERICOID, DataType::Int64) => {
                    Some((CellRule::I64, DatumCodec::numeric_from_int64()))
                }
                (pg_sys::NUMERICOID, DataType::Float64) => {
                    Some((CellRule::F64, DatumCodec::numeric_from_float64()))
                }
                _ => None,
            };
            // SAFETY: the target list establishes the slot layout. Forwarded
            // JSON text comes from the provider's validated JSON representation.
            // TextIo outputs likewise forward the provider's NUL-free PG
            // type-output text. Native string functions produce PG text, and
            // ExecutionScalarRepr excludes TextIo types from runtime literals
            // and PostgreSQL fallback evaluation; these cannot manufacture a
            // new TextIo payload from an arbitrary Arrow string.
            let column = if let Some((rule, codec)) = physical {
                unsafe {
                    DecodedColumn::new(
                        rule,
                        position,
                        position,
                        slot.type_oid(),
                        codec,
                    )
                }
            } else {
                // SAFETY: json/jsonb text obeys the validated, NUL-free
                // forwarding contract above, including array/composite leaves.
                let plan = unsafe {
                    BoundDatumPlan::bind_with_prevalidated_json_text(
                        field,
                        PgType::new(slot.type_oid(), slot.typmod()),
                    )
                }
                .map_err(PgReportError::from_domain_error)?;
                unsafe { DecodedColumn::bind(plan, position, position) }
            }
            .map_err(PgReportError::from_domain_error)?;
            columns.push(column);
            nullable.push(slot.nullable());
        }
        let requires_datum_context =
            columns.iter().any(DecodedColumn::requires_datum_context);
        Ok(Self {
            decoder: ArrowColumnDecoder::new(columns),
            nullable: nullable.into_boxed_slice(),
            width: layout.len(),
            requires_datum_context,
        })
    }

    /// # Safety
    /// `batch` must come from the execution plan schema supplied to `try_new`.
    pub(super) unsafe fn bind_batch_unchecked(
        &self,
        batch: RecordBatch,
    ) -> Result<BoundBatch, PgReportError> {
        // SAFETY: RecordBatchStream's fixed-schema contract is supplied by the
        // caller; PG target nullability is checked separately by execution state.
        unsafe { self.decoder.bind_batch_unchecked(batch) }
    }

    /// # Safety
    ///
    /// `slot` must be the scan slot built by PostgreSQL from the same target
    /// list that produced this decoder's query layout.
    pub(super) unsafe fn write_row(
        &self,
        bound: &BoundBatch,
        row: usize,
        slot: *mut pg_sys::TupleTableSlot,
        datum_context: pg_sys::MemoryContext,
    ) -> Result<(), PgReportError> {
        let mut columns = unsafe { SlotDatumWriter::new(slot, datum_context) };
        // SAFETY: decoder construction bound every destination to the
        // CustomScan output layout; batch iteration proves `row` exists.
        let mut write =
            || unsafe { self.decoder.write_row_unchecked(bound, row, &mut columns) };
        if self.requires_datum_context {
            unsafe { PgMemoryContexts::For(datum_context).switch_to(|_| write()) }
        } else {
            write()
        }
    }

    #[inline]
    pub(super) const fn width(&self) -> usize {
        self.width
    }

    pub(super) fn accepts_nulls(&self, batch: &RecordBatch) -> bool {
        batch
            .columns()
            .iter()
            .zip(self.nullable.iter())
            .all(|(column, nullable)| *nullable || column.null_count() == 0)
    }
}
