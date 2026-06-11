//! Per-type-family conversion modules and the central column-encoder dispatch.
//!
//! Each submodule owns one logical column type's **write** vertical slice — its
//! value math (codec) and its write path (datum / `Cell` → Arrow builder) — plus
//! any value helpers the read path reuses (epoch math, decimal codec,
//! `list::cell_at`). The per-value **read** dispatch is not here: it lives in
//! the bound [`ColumnReader`](crate::read), which downcasts a column once per
//! batch and reads values without a per-value downcast.
//!
//! This module hosts the cross-cutting write dispatch that ties them together:
//! the [`ColumnAppend`] contract every per-type encoder implements, the
//! [`ArrowColumnEncoder`] enum that stores one concrete encoder per column
//! (heterogeneous, but `dyn`-free so the per-row append monomorphizes), and the
//! small set of helpers shared across the type modules (`read_oid`,
//! `detoasted_payload`, `downcast`, `cell_type_mismatch`). The row-world `Cell`
//! **write** dispatch (`ColumnRule::build`) lives in [`convert`](crate::convert).

use std::borrow::Cow;

use arrow_array::{Array, ArrayRef};
use pg_lakebase_core::batch::DatumColumnAppender;
use pg_lakebase_core::tuple::{Cell, PgDatumRef};
use pgrx::{pg_sys, varlena};

use crate::error::{ConvError, ConvResult};
use crate::rule::ColumnRule;

pub(crate) mod binary;
pub(crate) mod decimal;
pub(crate) mod list;
pub(crate) mod primitive;
pub(crate) mod string;
pub(crate) mod temporal;

pub use temporal::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};

use binary::{BinaryEncoder, FixedBinaryEncoder, UuidEncoder};
use decimal::Decimal128Encoder;
use list::ListEncoder;
use primitive::{BoolEncoder, F32Conv, F64Conv, I32Conv, I64Conv, PrimitiveEncoder};
use string::Utf8Encoder;
use temporal::{Date32Conv, Time64Conv, TimestampEncoder};

/// The append contract every per-type Arrow encoder implements. The two write
/// sources — a live PostgreSQL datum (columnar hot path) and a buffered [`Cell`]
/// (row-world / FDW path) — feed the *same* builder, `finish`, NULL append, and
/// byte accounting, so a type's write logic is written once per type.
pub(crate) trait ColumnAppend {
    /// Append a present, non-null datum (columnar path), returning the number
    /// of bytes it added to the builder's in-memory footprint.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-null datum of this column's source type.
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<usize>;

    /// Append a present, non-null buffered [`Cell`] (row-world path). A cell
    /// whose variant does not match the column is rejected.
    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()>;

    /// Append a SQL NULL.
    fn append_null(&mut self);

    /// Finish the current column and reset the builder for reuse.
    fn finish(&mut self) -> ConvResult<ArrayRef>;

    /// Number of values appended for the current batch.
    fn len(&self) -> usize;
}

/// A per-column Arrow builder, one variant per physical Arrow column type.
/// Constructed from the column's resolved [`ColumnRule`] so the write and read
/// paths share one dispatch key.
///
/// This is a thin public wrapper over the private [`Encoder`] enum: the variant
/// set and the per-type encoder structs are implementation details, so the
/// public surface is just `new` + the [`DatumColumnAppender`] impl +
/// `append_cell` / `append_null`.
pub struct ArrowColumnEncoder(Encoder);

enum Encoder {
    Bool(BoolEncoder),
    I32(PrimitiveEncoder<I32Conv>),
    I64(PrimitiveEncoder<I64Conv>),
    F32(PrimitiveEncoder<F32Conv>),
    F64(PrimitiveEncoder<F64Conv>),
    Utf8(Utf8Encoder),
    Binary(BinaryEncoder),
    FixedBinary(FixedBinaryEncoder),
    Uuid(UuidEncoder),
    Date32(PrimitiveEncoder<Date32Conv>),
    Time64Micros(PrimitiveEncoder<Time64Conv>),
    Timestamp(TimestampEncoder),
    Decimal128(Decimal128Encoder),
    List(ListEncoder),
}

/// Expand `$body` once per [`Encoder`] variant, binding the active variant's
/// inner encoder to `$e`. The 14-variant list lives here and nowhere else, so
/// every uniform dispatch (`append_datum`, `append_cell`, null append,
/// `finish`, `len`, byte accounting) is written once and a newly added variant
/// cannot be silently omitted from one of them.
macro_rules! dispatch_encoder {
    ($self:expr, $e:ident => $body:expr) => {
        match $self {
            Encoder::Bool($e) => $body,
            Encoder::I32($e) => $body,
            Encoder::I64($e) => $body,
            Encoder::F32($e) => $body,
            Encoder::F64($e) => $body,
            Encoder::Utf8($e) => $body,
            Encoder::Binary($e) => $body,
            Encoder::FixedBinary($e) => $body,
            Encoder::Uuid($e) => $body,
            Encoder::Date32($e) => $body,
            Encoder::Time64Micros($e) => $body,
            Encoder::Timestamp($e) => $body,
            Encoder::Decimal128($e) => $body,
            Encoder::List($e) => $body,
        }
    };
}

impl ArrowColumnEncoder {
    /// Build the encoder for a resolved column rule, pre-sizing builders to
    /// `capacity` rows.
    pub fn new(rule: &ColumnRule, capacity: usize) -> Self {
        let encoder = match rule {
            ColumnRule::Bool => Encoder::Bool(BoolEncoder::with_capacity(capacity)),
            ColumnRule::I32 => {
                Encoder::I32(PrimitiveEncoder::with_capacity(capacity))
            }
            ColumnRule::I64 => {
                Encoder::I64(PrimitiveEncoder::with_capacity(capacity))
            }
            ColumnRule::F32 => {
                Encoder::F32(PrimitiveEncoder::with_capacity(capacity))
            }
            ColumnRule::F64 => {
                Encoder::F64(PrimitiveEncoder::with_capacity(capacity))
            }
            ColumnRule::Utf8 => Encoder::Utf8(Utf8Encoder::with_capacity(capacity)),
            ColumnRule::Binary => {
                Encoder::Binary(BinaryEncoder::with_capacity(capacity))
            }
            ColumnRule::FixedBinary { len } => Encoder::FixedBinary(
                FixedBinaryEncoder::with_capacity(capacity, *len),
            ),
            ColumnRule::Uuid => Encoder::Uuid(UuidEncoder::with_capacity(capacity)),
            ColumnRule::Date32 => {
                Encoder::Date32(PrimitiveEncoder::with_capacity(capacity))
            }
            ColumnRule::Time64Micros => {
                Encoder::Time64Micros(PrimitiveEncoder::with_capacity(capacity))
            }
            ColumnRule::Timestamp { nanos, tz } => Encoder::Timestamp(
                TimestampEncoder::with_capacity(capacity, *nanos, *tz),
            ),
            ColumnRule::Decimal128 { precision, scale } => Encoder::Decimal128(
                Decimal128Encoder::with_capacity(capacity, *precision, *scale),
            ),
            ColumnRule::List { element, field, .. } => Encoder::List(
                ListEncoder::with_capacity(capacity, *element, field.clone()),
            ),
        };
        Self(encoder)
    }

    /// Row-world write: append one buffered [`Cell`] to the active variant.
    pub fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        dispatch_encoder!(&mut self.0, e => e.append_cell(cell))
    }

    /// Append a NULL to the active variant. Public so the row-world build loop
    /// can mark a missing/NULL cell.
    pub fn append_null(&mut self) {
        dispatch_encoder!(&mut self.0, e => e.append_null())
    }
}

impl DatumColumnAppender for ArrowColumnEncoder {
    type Column = ArrayRef;
    type Error = ConvError;

    fn append_datum(&mut self, value: Option<PgDatumRef<'_>>) -> ConvResult<usize> {
        // One slot is appended either way; the null path never reads the datum
        // and adds no bytes.
        match value {
            Some(v) if !v.is_null() => {
                dispatch_encoder!(&mut self.0, e => unsafe { e.append_datum(v) })
            }
            _ => {
                dispatch_encoder!(&mut self.0, e => e.append_null());
                Ok(0)
            }
        }
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        dispatch_encoder!(&mut self.0, e => e.finish())
    }

    fn clear(&mut self) {
        // Arrow builders have no separate reset entry point: `finish` is what
        // resets their internal buffers, so calling it and discarding the
        // produced array is the idiomatic way to clear in-progress contents.
        //
        // Discarding the `Result` is intentional and sound: of all variants
        // only `Decimal128`'s `finish` is fallible (its precision/scale tag is
        // applied to the *already-finished* — i.e. already-reset — array), so
        // an error here still leaves every encoder empty and reusable. There is
        // no buffered state left to report on, which is exactly what `clear`
        // promises.
        let _ = self.finish();
    }

    fn len(&self) -> usize {
        dispatch_encoder!(&self.0, e => e.len())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers used across the type modules
// ---------------------------------------------------------------------------

/// Downcast an Arrow array to a concrete type, mapping a mismatch to
/// [`ConvError::ArrowTypeMismatch`] naming the expected type.
pub(crate) fn downcast<'a, A: Array + 'static>(
    column: &'a dyn Array,
    expected: &'static str,
) -> ConvResult<&'a A> {
    column
        .as_any()
        .downcast_ref::<A>()
        .ok_or(ConvError::ArrowTypeMismatch(Cow::Borrowed(expected)))
}

/// The error returned when a buffered row `Cell`'s variant does not match the
/// column it is being appended to. `column` names the target column type.
pub(crate) fn cell_type_mismatch(column: &str) -> ConvError {
    ConvError::IncompatibleColumnType(
        column.to_string(),
        "buffered row cell variant does not match the column type".to_string(),
    )
}

/// Read a present `datum` as `T`, requiring its source OID to be `expected`.
///
/// Reaching this with a mismatched OID, or with `from` yielding `None` for a
/// datum already established as present and non-null, is a broken invariant:
/// the schema validator resolved this column's rule from the same source type,
/// so the encoder should never see a datum it cannot read. Both cases surface
/// as [`ConvError::InvariantViolated`] rather than being silently coerced to
/// SQL NULL, which would lose data on a schema/`TupleDesc` drift.
///
/// # Safety
///
/// `datum` must be a valid, non-null datum; `from` must be the `FromDatum`
/// reader for `expected`.
pub(crate) unsafe fn read_oid<T>(
    datum: PgDatumRef<'_>,
    expected: pg_sys::Oid,
    from: unsafe fn(pg_sys::Datum, bool) -> Option<T>,
    invariant: &'static str,
) -> ConvResult<T> {
    if datum.type_oid() != expected {
        return Err(ConvError::InvariantViolated(invariant));
    }
    unsafe { from(datum.datum(), false) }
        .ok_or(ConvError::InvariantViolated(invariant))
}

/// RAII view over a detoasted varlena payload, shared by the varlena encoders.
///
/// Detoasting must precede any byte read: an in-line `bytea`/`text` datum can be
/// compressed or stored out-of-line, so reading its raw bytes without
/// detoasting yields the toast pointer, not the value.
pub(crate) struct DetoastGuard {
    detoasted: *mut pg_sys::varlena,
    data: *const u8,
    len: usize,
    // Only a freshly palloc'd result is ours to free; an unchanged in-line datum
    // is borrowed from the caller and must be left alone.
    owned: bool,
}

/// Detoast a varlena datum and expose its header-stripped payload.
///
/// # Safety
///
/// `datum` must be a non-null varlena datum.
pub(crate) unsafe fn detoasted_payload(datum: pg_sys::Datum) -> DetoastGuard {
    unsafe {
        let input = datum.cast_mut_ptr::<pg_sys::varlena>();
        let detoasted = pg_sys::pg_detoast_datum(input);
        DetoastGuard {
            detoasted,
            data: varlena::vardata_any(detoasted) as *const u8,
            len: varlena::varsize_any_exhdr(detoasted),
            owned: !std::ptr::eq(detoasted, input),
        }
    }
}

impl DetoastGuard {
    pub(crate) fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }

    /// The complete detoasted varlena, including its header. `jsonb` is stored
    /// in PostgreSQL's internal varlena form (the read side copies the bytes
    /// back verbatim and expects the header present), so its encoder must keep
    /// the header that [`bytes`](Self::bytes) strips off.
    pub(crate) fn full_varlena_bytes(&self) -> &[u8] {
        unsafe {
            let len = varlena::varsize_any(self.detoasted);
            std::slice::from_raw_parts(self.detoasted as *const u8, len)
        }
    }
}

impl Drop for DetoastGuard {
    fn drop(&mut self) {
        if self.owned {
            unsafe { pg_sys::pfree(self.detoasted.cast()) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};

    // The physical type a primitive rule finishes into is fixed at
    // construction, so it is checkable host-side without a backend.
    #[test]
    fn scalar_rules_finish_with_their_widened_arrow_type() {
        let cases = [
            (ColumnRule::Bool, DataType::Boolean),
            (ColumnRule::I32, DataType::Int32),
            (ColumnRule::I64, DataType::Int64),
            (ColumnRule::F32, DataType::Float32),
            (ColumnRule::F64, DataType::Float64),
            (ColumnRule::Date32, DataType::Date32),
            (
                ColumnRule::Time64Micros,
                DataType::Time64(TimeUnit::Microsecond),
            ),
        ];
        for (rule, expected) in cases {
            let mut encoder = ArrowColumnEncoder::new(&rule, 4);
            let array = encoder.finish().expect("finish");
            assert_eq!(array.data_type(), &expected, "rule {rule:?}");
        }
    }

    // `finish` must select the physical unit from `nanos` and tag tz-aware
    // columns with the `+00:00` zone, leaving tz-naive ones unzoned.
    #[test]
    fn timestamp_finish_selects_unit_and_timezone() {
        let cases = [
            (false, false, TimeUnit::Microsecond, None),
            (false, true, TimeUnit::Microsecond, Some("+00:00")),
            (true, false, TimeUnit::Nanosecond, None),
            (true, true, TimeUnit::Nanosecond, Some("+00:00")),
        ];
        for (nanos, tz, expected_unit, expected_tz) in cases {
            let mut encoder =
                ArrowColumnEncoder::new(&ColumnRule::Timestamp { nanos, tz }, 4);
            let array = encoder.finish().expect("finish");
            match array.data_type() {
                DataType::Timestamp(unit, zone) => {
                    assert_eq!(*unit, expected_unit, "nanos={nanos}");
                    assert_eq!(zone.as_deref(), expected_tz, "tz={tz}");
                }
                other => panic!("expected Timestamp, got {other:?}"),
            }
        }
    }

    // NOTE: the null path of `append_datum` (`ArrowColumnEncoder::append_datum`)
    // cannot be exercised here: its `Some` dispatch arm references every
    // variant's `append_datum`, including `Decimal128Encoder`'s numeric encode
    // path, which pulls PostgreSQL backend symbols into a host test binary and
    // fails to link on Linux. That coverage lives as a `#[pg_test]` in
    // `pg-backend-tests` (see `arrow_conv::decoder_equivalence`).
}
