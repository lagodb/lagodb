//! Arrow output binding for PostgreSQL query slots.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use lagodb_arrow::{
    ArrowColumnDecoder, BoundBatch, ColumnRule, DatumCodec, DecodedColumn,
    PgColumnType, resolve_column_rule,
};
use lagodb_core::batch::BatchRowDecoder;
use lagodb_core::diag::PgReportError;
use lagodb_core::tuple::SlotColumns;
use pgrx::prelude::PgSqlErrorCode;
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
            let pg_type =
                PgColumnType::from_pg_type(slot.type_oid()).ok_or_else(|| {
                    PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
                        format!(
                            "query output type {:?} has no Arrow conversion",
                            slot.type_oid()
                        ),
                    )
                })?;
            let (rule, codec) = match (slot.type_oid(), field.data_type()) {
                (pg_sys::FLOAT4OID, DataType::Float64) => {
                    (ColumnRule::F64, DatumCodec::float4_from_float64())
                }
                (pg_sys::NUMERICOID, DataType::Binary) => {
                    // SAFETY: only LagoDB's numeric UDAF emits this complete
                    // detoasted PostgreSQL NUMERIC varlena.
                    (ColumnRule::Binary, unsafe {
                        DatumCodec::postgres_numeric_varlena()
                    })
                }
                (pg_sys::NUMERICOID, DataType::Int64) => {
                    (ColumnRule::I64, DatumCodec::numeric_from_int64())
                }
                (pg_sys::NUMERICOID, DataType::Float64) => {
                    (ColumnRule::F64, DatumCodec::numeric_from_float64())
                }
                _ => (
                    resolve_column_rule(field.data_type(), pg_type)
                        .map_err(PgReportError::from_domain_error)?,
                    DatumCodec::standard(slot.type_oid())
                        .map_err(PgReportError::from_domain_error)?,
                ),
            };
            // SAFETY: the output layout is constructed from the CustomScan
            // target list that PostgreSQL uses to create the destination slot.
            // `position` is bounded by that validated layout for this decoder.
            columns.push(
                unsafe {
                    DecodedColumn::new(
                        rule,
                        position,
                        position,
                        slot.type_oid(),
                        codec,
                    )
                }
                .map_err(PgReportError::from_domain_error)?,
            );
            nullable.push(slot.nullable());
        }
        // Resolve this once with PostgreSQL's type metadata. By-value datums
        // cannot retain allocations in the current memory context, so the row
        // hot path only switches contexts when an output can be by-ref.
        let requires_datum_context = layout
            .slots()
            .iter()
            .any(|slot| unsafe { !pg_sys::get_typbyval(slot.type_oid()) });
        Ok(Self {
            decoder: ArrowColumnDecoder::new(columns),
            nullable: nullable.into_boxed_slice(),
            width: layout.len(),
            requires_datum_context,
        })
    }

    pub(super) fn bind(
        &self,
        batch: RecordBatch,
    ) -> Result<BoundBatch, PgReportError> {
        self.decoder.bind(batch)
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
        let mut columns = unsafe { SlotColumns::new(slot, datum_context) };
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
