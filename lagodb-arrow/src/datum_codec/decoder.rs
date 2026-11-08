//! Scan-owned datum codecs and batch-owned Arrow array bindings.

use std::rc::Rc;

use arrow_array::RecordBatch;
use lagodb_core::datum::ColumnDatumTarget;
use lagodb_core::diag::PgReportError;
use lagodb_core::tuple::SlotDatumWriter;
use pgrx::pg_sys;

use super::batch::{BoundBatch, BoundColumn};
use super::reader::ScalarRead;
use super::state::DatumReadState;
use super::{BoundDatumPlan, DatumCodec};
use crate::error::ArrowConversionResult;
use crate::rule::CellRule;

/// One mapped column: which Arrow batch column to read, its bound conversion
/// and the destination slot index. Every fallible conversion setup happens
/// while the column is constructed; binding a batch only selects arrays. A
/// physical codec is part of the bound conversion so a generic Arrow type or
/// PostgreSQL OID cannot silently select storage or query-result semantics.
pub struct DecodedColumn {
    source: DecoderSource,
    src_col: usize,
    dest: usize,
    target_oid: pg_sys::Oid,
}

enum DecoderSource {
    Physical {
        read: ScalarRead,
    },
    Declared {
        plan: BoundDatumPlan,
        state: DatumReadState,
    },
}

impl DecodedColumn {
    /// Bind a declared Datum plan to the output slot positions.
    ///
    /// # Safety
    /// `dest` has the plan's declared PG type and remains stable for this scan.
    /// Widened int2, OID and interval representations must come from a
    /// producer preserving their declared PG value domains. External formats
    /// must reject wider, unannotated source types rather than treating this
    /// decoder as an SQL narrowing cast, including in recursive containers.
    /// A plan bound with `BoundDatumPlan::bind_with_prevalidated_json_text` requires
    /// already validated, NUL-free json/jsonb text. Ordinary
    /// `BoundDatumPlan::bind` checks and parses those strings instead.
    /// Other TextIo values, including array/composite leaves, must preserve
    /// the NUL-free text produced by PostgreSQL's type output function.
    /// This contract applies to every batch supplied to the decoder; an Arrow
    /// UTF-8 type or destination OID alone does not establish it. Trusted
    /// TextIo readers do not repeat that guarantee with a per-datum NUL scan.
    pub unsafe fn bind(
        plan: BoundDatumPlan,
        src_col: usize,
        dest: usize,
    ) -> ArrowConversionResult<Self> {
        if plan.requires_utf8_server_encoding() {
            ColumnDatumTarget::validate_utf8_server_encoding()?;
        }
        let target_oid = plan.pg_type_oid();
        let state = DatumReadState::new(&plan)?;
        Ok(Self {
            source: DecoderSource::Declared { plan, state },
            src_col,
            dest,
            target_oid,
        })
    }

    /// Bind an explicit physical representation supplied by the query engine.
    ///
    /// # Safety
    /// `dest` belongs to the bound output layout and has `target_oid`. The
    /// source values satisfy the selected physical codec's producer contract.
    pub unsafe fn new(
        rule: CellRule,
        src_col: usize,
        dest: usize,
        target_oid: pg_sys::Oid,
        codec: DatumCodec,
    ) -> ArrowConversionResult<Self> {
        codec.validate_target_oid(target_oid)?;
        if rule.requires_utf8_server_encoding() {
            ColumnDatumTarget::validate_utf8_server_encoding()?;
        }
        let read = ScalarRead::bind(&rule, codec, None)?;
        Ok(Self {
            source: DecoderSource::Physical { read },
            src_col,
            dest,
            target_oid,
        })
    }

    pub fn target_oid(&self) -> pg_sys::Oid {
        self.target_oid
    }

    /// Resolved during executor setup, never for an individual row.
    pub fn requires_datum_context(&self) -> bool {
        match &self.source {
            DecoderSource::Declared { plan, .. } => plan.requires_datum_context(),
            DecoderSource::Physical { .. } => unsafe {
                !pg_sys::get_typbyval(self.target_oid)
            },
        }
    }
}

/// Decodes Arrow column values into slot datums. Rule resolution and reader
/// state setup happen before row access; concrete arrays bind per batch.
/// Pass-by-reference Datums still allocate in the destination row context.
/// Unmapped slot positions are left untouched.
///
/// The decoder writes only its mapped columns. Unmapped positions, such as
/// dropped attributes, can still be read: PostgreSQL reads every attribute
/// when it forms a whole-row datum or copies the slot into a HeapTuple. The
/// adapter that owns the slot must make every position its layout never
/// writes read as NULL before using a decoder on that slot. `ExecClearTuple`
/// does not do this: the slot's `tts_values`/`tts_isnull` arrays are
/// `palloc0`'d, so they start non-NULL, and clearing leaves them unchanged.
///
/// The immutable column plan is reference-counted with [`Rc`]: PostgreSQL scan
/// state is backend-local, while rescans need cheap decoder handles without
/// rebuilding or cloning every column rule. The row loop still iterates the
/// slice directly and never touches the reference count.
#[derive(Clone)]
pub struct ArrowColumnDecoder {
    columns: Rc<[DecodedColumn]>,
}

impl ArrowColumnDecoder {
    pub fn new(columns: Vec<DecodedColumn>) -> Self {
        Self {
            columns: Rc::from(columns.into_boxed_slice()),
        }
    }

    /// Bind fresh arrays from a producer that guarantees the scan schema. This
    /// cannot fail because all conversion state was bound with the columns.
    ///
    /// # Safety
    /// Every mapped source column must exist and satisfy its complete conversion
    /// plan, including nested field order, decimal precision/scale, fixed binary
    /// width and required interval components. The batch must come from the
    /// projection/execution schema used to construct this decoder. File schema
    /// evolution belongs to the producer, not this binding layer. Unrelated,
    /// unmapped columns are permitted.
    pub unsafe fn bind_batch_unchecked(&self, batch: RecordBatch) -> BoundBatch {
        let columns = self
            .columns
            .iter()
            .map(|col| {
                let array = batch.column(col.src_col).as_ref();
                // SAFETY: the caller establishes the full contract for each
                // mapped column, so recursive children need not revalidate
                // their subtrees.
                let reader = unsafe {
                    match &col.source {
                        DecoderSource::Declared { plan, state } => {
                            plan.bind_reader_unchecked(array, state)
                        }
                        DecoderSource::Physical { read } => read.bind_array(array),
                    }
                };
                BoundColumn {
                    reader,
                    dest: col.dest,
                }
            })
            .collect();
        BoundBatch {
            columns,
            num_rows: batch.num_rows(),
        }
    }

    /// Decode one row directly into a slot whose layout is the one used to
    /// validate the decoder's [`DecodedColumn`] destinations.
    ///
    /// This is unsafe because the decoder stores scalar destinations, not a
    /// type-level association with a particular `SlotDatumWriter` descriptor.
    ///
    /// # Safety
    ///
    /// `bound` must have been produced by this decoder. Every `col.dest` in
    /// `bound` must be within `out.natts()`, and `out` must be backed by the
    /// same relation slot layout whose width and
    /// destination mapping were validated when these `DecodedColumn`s were
    /// constructed. `row_idx` must be within `bound` and every bound Arrow
    /// array. The caller must also satisfy the memory-context requirements of
    /// [`BoundColumn::read_datum_unchecked`].
    #[inline]
    pub unsafe fn write_row_unchecked(
        &self,
        bound: &BoundBatch,
        row_idx: usize,
        out: &mut SlotDatumWriter<'_>,
    ) -> Result<(), PgReportError> {
        Ok(unsafe { bound.write_row_unchecked(row_idx, out) }?)
    }
}
