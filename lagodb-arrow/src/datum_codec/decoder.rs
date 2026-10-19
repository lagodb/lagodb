//! Scan-owned datum codecs and batch-owned Arrow array bindings.

use std::rc::Rc;

use arrow_array::RecordBatch;
use lagodb_core::api::AmResult;
use lagodb_core::batch::BatchRowDecoder;
use lagodb_core::datum::ColumnDatumTarget;
use lagodb_core::tuple::SlotDatumWriter;
use pgrx::pg_sys;

use super::batch::{BoundBatch, BoundColumn};
use super::reader::BoundDatumReader;
use super::state::DatumReadState;
use crate::{
    ArrowConversionError, ArrowConversionResult, BoundDatumPlan, CellRule, DatumCodec,
};

/// One mapped column: which Arrow batch column to read, the resolved rule, the
/// destination slot index, and the selected datum codec. A physical codec is
/// part of the bound plan so a generic Arrow type or PostgreSQL OID cannot
/// silently select storage or query-result semantics.
pub struct DecodedColumn {
    source: DecoderSource,
    src_col: usize,
    dest: usize,
    target_oid: pg_sys::Oid,
}

enum DecoderSource {
    Physical {
        rule: CellRule,
        codec: DatumCodec,
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
        let state = DatumReadState::new(&plan);
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
        Ok(Self {
            source: DecoderSource::Physical { rule, codec },
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
/// Leaving a position untouched is safe only because every column the executor
/// can read is mapped: a whole-row reference falls back to `NeededColumns::All`
/// (so the plan covers every live column), and a projected scan maps exactly
/// the referenced columns — unmapped positions are therefore dropped or
/// otherwise never-read columns. This does **not** rely on `ExecClearTuple`
/// resetting `tts_isnull`: it does not (the slot's `tts_values`/`tts_isnull`
/// arrays are `palloc0`'d, i.e. start non-NULL, and only `ExecStoreAllNullTuple`
/// marks them all NULL). Correctness comes from the "unmapped == unreferenced"
/// invariant, not from the cleared slot.
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

    /// Bind fresh arrays from a producer that already guarantees the scan schema.
    /// Downcasts and buffer ownership are still established for every batch;
    /// only the repeated recursive type comparisons are skipped.
    ///
    /// # Safety
    /// Every mapped source column must exist and satisfy its complete conversion
    /// plan, including nested field order, decimal precision/scale, fixed binary
    /// width and required interval components. The batch must come from the
    /// projection/execution schema used to construct this decoder (or have been
    /// checked against it). File schema evolution belongs to the producer, not
    /// this binding layer. Unrelated, unmapped columns are permitted.
    pub unsafe fn bind_batch_unchecked(
        &self,
        batch: RecordBatch,
    ) -> AmResult<BoundBatch> {
        // SAFETY: this adapter preserves the producer contract below.
        Ok(unsafe { self.bind_datums_unchecked(batch) }?)
    }

    /// Bind a trusted batch without converting errors into an access-method error.
    /// COPY and FDW providers retain their own error boundary and row policy.
    ///
    /// # Safety
    /// The complete producer contract of [`Self::bind_batch_unchecked`] applies.
    pub unsafe fn bind_datums_unchecked(
        &self,
        batch: RecordBatch,
    ) -> ArrowConversionResult<BoundBatch> {
        let num_rows = batch.num_rows();
        let mut columns = Vec::with_capacity(self.columns.len());
        for col in self.columns.iter() {
            let array = batch.column(col.src_col);
            // SAFETY: the caller establishes the full contract for each mapped
            // column, so recursive children need not revalidate their subtrees.
            let reader = unsafe {
                match &col.source {
                    DecoderSource::Declared { plan, state } => {
                        plan.bind_reader_unchecked(array.as_ref(), state)?
                    }
                    DecoderSource::Physical { rule, codec } => {
                        BoundDatumReader::bind_unchecked(
                            rule,
                            array.as_ref(),
                            *codec,
                        )?
                    }
                }
            };
            columns.push(BoundColumn {
                reader,
                dest: col.dest,
            });
        }
        Ok(BoundBatch {
            columns: columns.into_boxed_slice(),
            num_rows,
        })
    }

    /// Decode one row directly into a slot whose layout is the one used to
    /// validate the decoder's [`DecodedColumn`] destinations.
    ///
    /// This is the trusted-provider physical path. It is deliberately an
    /// unsafe method instead of being the implementation of the safe
    /// [`BatchRowDecoder::write_row`] API: the decoder stores scalar
    /// destinations, not a type-level association with a particular
    /// `SlotDatumWriter` descriptor.
    ///
    /// # Safety
    ///
    /// `bound` must have been produced by this decoder. Every `col.dest` in
    /// `bound` must be within `out.natts()`, and `out` must be backed by the
    /// same relation slot layout whose width and
    /// destination mapping were validated when these `DecodedColumn`s were
    /// constructed. `row_idx` must be within `bound` and every bound Arrow
    /// array. The caller must also satisfy the memory-context requirements of
    /// [`BoundDatumReader::read_datum_unchecked`].
    #[inline]
    pub unsafe fn write_row_unchecked(
        &self,
        bound: &BoundBatch,
        row_idx: usize,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<()> {
        Ok(unsafe { bound.write_row_unchecked(row_idx, out) }?)
    }
}

impl BatchRowDecoder for ArrowColumnDecoder {
    type Batch = RecordBatch;
    type Bound = BoundBatch;

    /// Resolve every mapped column to its concrete typed array, once per batch.
    /// Each mapped column's complete type is checked once before sharing the
    /// trusted array-binding implementation. This includes NestedJsonb leaves,
    /// where downcasts alone cannot establish scale or UUID width. Type drift
    /// surfaces as a clean
    /// [`ArrowConversionError::ArrowTypeMismatch`] at the batch boundary.
    fn bind(&self, batch: RecordBatch) -> AmResult<BoundBatch> {
        for col in self.columns.iter() {
            let array = batch.columns().get(col.src_col).ok_or_else(|| {
                ArrowConversionError::ArrowTypeMismatch(
                    format!(
                        "batch has {} columns but the plan reads column {}",
                        batch.num_columns(),
                        col.src_col
                    )
                    .into(),
                )
            })?;
            let accepts = match &col.source {
                DecoderSource::Declared { plan, .. } => {
                    plan.accepts_arrow_type(array.data_type())
                }
                DecoderSource::Physical { rule, .. } => {
                    rule.accepts(array.data_type())
                }
            };
            if !accepts {
                return Err(ArrowConversionError::ArrowTypeMismatch(
                    format!(
                        "batch column {} has type {:?}, incompatible with its conversion plan",
                        col.src_col,
                        array.data_type()
                    )
                    .into(),
                )
                .into());
            }
        }
        // SAFETY: each mapped column was checked once above, recursively,
        // including NestedJsonb's decimal scale and UUID width prerequisites.
        unsafe { self.bind_batch_unchecked(batch) }
    }

    fn num_rows(&self, bound: &BoundBatch) -> usize {
        bound.num_rows
    }

    /// Safe generic path: the destination bound is checked by `SlotDatumWriter`.
    /// Trusted relation-bound providers can use
    /// [`Self::write_row_unchecked`] after upholding its layout contract.
    fn write_row(
        &self,
        bound: &BoundBatch,
        row_idx: usize,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<()> {
        if row_idx >= bound.num_rows {
            return Err(ArrowConversionError::InvariantViolated(
                "batch row index is out of bounds",
            )
            .into());
        }
        for col in bound.columns.iter() {
            // The shim already switched to the slot's target context, so the
            // varlena/array palloc lands where the per-row reset expects it.
            let datum = unsafe { col.reader.read_datum_unchecked(row_idx) }?;
            out.set_datum(col.dest, datum);
        }
        Ok(())
    }
}
