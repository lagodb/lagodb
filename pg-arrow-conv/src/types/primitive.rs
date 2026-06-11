//! Primitive scalar conversion: `bool` plus the integer/float family, both
//! read (`Arrow → Cell`) and write (datum / `Cell` → Arrow builder).
//!
//! The integer/float/`date`/`time` write encoders all collapse into the single
//! generic [`PrimitiveEncoder`], parameterized by a zero-sized [`PrimitiveConv`]
//! marker that carries only the per-type differences (Arrow type, accepted
//! source OIDs / widening, value codec). `bool` keeps a dedicated encoder
//! because `BooleanBuilder` is not an [`arrow_array::builder::PrimitiveBuilder`].

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{ArrayRef, ArrowPrimitiveType};
use pg_lakebase_core::tuple::{Cell, PgDatumRef};
use pgrx::{FromDatum, pg_sys};

use super::{ColumnAppend, cell_type_mismatch, read_oid};
use crate::error::{ConvError, ConvResult};

// ---------------------------------------------------------------------------
// Generic primitive write encoder
// ---------------------------------------------------------------------------

/// Per-type conversion for an Arrow primitive column: how to read the column's
/// native Arrow value from a present, non-null PostgreSQL datum or a buffered
/// [`Cell`]. One zero-sized marker per primitive `ColumnRule` collapses the
/// near-identical primitive encoders into the single generic
/// [`PrimitiveEncoder`]; dispatch stays a monomorphized `match` (no `dyn`).
pub(crate) trait PrimitiveConv {
    /// The physical Arrow primitive type this column builds into.
    type Arrow: ArrowPrimitiveType;

    /// Column-type label used in mismatch diagnostics.
    const LABEL: &'static str;

    /// Read the native Arrow value from a present, non-null datum.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-null datum of this column's source type.
    unsafe fn from_datum(
        datum: PgDatumRef<'_>,
    ) -> ConvResult<<Self::Arrow as ArrowPrimitiveType>::Native>;

    /// Read the native Arrow value from a buffered [`Cell`].
    ///
    /// `Ok(None)` means the cell variant does not match this column (the write
    /// path rejects it); `Err` is a value-level codec failure.
    fn from_cell(
        cell: &Cell,
    ) -> ConvResult<Option<<Self::Arrow as ArrowPrimitiveType>::Native>>;
}

/// A primitive Arrow column builder driven by a [`PrimitiveConv`] marker.
pub(crate) struct PrimitiveEncoder<C: PrimitiveConv> {
    builder: PrimitiveBuilder<C::Arrow>,
    _conv: PhantomData<fn() -> C>,
}

impl<C: PrimitiveConv> PrimitiveEncoder<C> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<C::Arrow>::with_capacity(capacity),
            _conv: PhantomData,
        }
    }

    const VALUE_BYTES: usize =
        std::mem::size_of::<<C::Arrow as ArrowPrimitiveType>::Native>();
}

impl<C: PrimitiveConv> ColumnAppend for PrimitiveEncoder<C> {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<usize> {
        let value = unsafe { C::from_datum(datum) }?;
        self.builder.append_value(value);
        Ok(Self::VALUE_BYTES)
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        let value =
            C::from_cell(cell)?.ok_or_else(|| cell_type_mismatch(C::LABEL))?;
        self.builder.append_value(value);
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}

/// `int4` column, widening an `int2` / `char` source as the row-build path does.
pub(crate) struct I32Conv;
impl PrimitiveConv for I32Conv {
    type Arrow = Int32Type;
    const LABEL: &'static str = "int4";

    unsafe fn from_datum(datum: PgDatumRef<'_>) -> ConvResult<i32> {
        let oid = datum.type_oid();
        let raw = datum.datum();
        unsafe {
            if oid == pg_sys::INT4OID {
                i32::from_datum(raw, false)
            } else if oid == pg_sys::INT2OID {
                i16::from_datum(raw, false).map(|v| v as i32)
            } else if oid == pg_sys::CHAROID {
                i8::from_datum(raw, false).map(|v| v as i32)
            } else {
                return Err(ConvError::InvariantViolated(
                    "I32 encoder: datum source type is not int4/int2/char",
                ));
            }
        }
        .ok_or(ConvError::InvariantViolated(
            "I32 encoder: present integer datum read as null",
        ))
    }

    fn from_cell(cell: &Cell) -> ConvResult<Option<i32>> {
        Ok(match cell {
            Cell::I32(v) => Some(*v),
            Cell::I16(v) => Some(*v as i32),
            Cell::I8(v) => Some(*v as i32),
            _ => None,
        })
    }
}

/// `int8` column, widening an `int4` / `int2` source.
pub(crate) struct I64Conv;
impl PrimitiveConv for I64Conv {
    type Arrow = Int64Type;
    const LABEL: &'static str = "int8";

    unsafe fn from_datum(datum: PgDatumRef<'_>) -> ConvResult<i64> {
        let oid = datum.type_oid();
        let raw = datum.datum();
        unsafe {
            if oid == pg_sys::INT8OID {
                i64::from_datum(raw, false)
            } else if oid == pg_sys::INT4OID {
                i32::from_datum(raw, false).map(|v| v as i64)
            } else if oid == pg_sys::INT2OID {
                i16::from_datum(raw, false).map(|v| v as i64)
            } else {
                return Err(ConvError::InvariantViolated(
                    "I64 encoder: datum source type is not int8/int4/int2",
                ));
            }
        }
        .ok_or(ConvError::InvariantViolated(
            "I64 encoder: present integer datum read as null",
        ))
    }

    fn from_cell(cell: &Cell) -> ConvResult<Option<i64>> {
        Ok(match cell {
            Cell::I64(v) => Some(*v),
            Cell::I32(v) => Some(*v as i64),
            Cell::I16(v) => Some(*v as i64),
            _ => None,
        })
    }
}

/// `float4` column.
pub(crate) struct F32Conv;
impl PrimitiveConv for F32Conv {
    type Arrow = Float32Type;
    const LABEL: &'static str = "float4";

    unsafe fn from_datum(datum: PgDatumRef<'_>) -> ConvResult<f32> {
        unsafe {
            read_oid(
                datum,
                pg_sys::FLOAT4OID,
                f32::from_datum,
                "F32 encoder: datum source type is not float4",
            )
        }
    }

    fn from_cell(cell: &Cell) -> ConvResult<Option<f32>> {
        Ok(match cell {
            Cell::F32(v) => Some(*v),
            _ => None,
        })
    }
}

/// `float8` column, widening a `float4` source.
pub(crate) struct F64Conv;
impl PrimitiveConv for F64Conv {
    type Arrow = Float64Type;
    const LABEL: &'static str = "float8";

    unsafe fn from_datum(datum: PgDatumRef<'_>) -> ConvResult<f64> {
        let oid = datum.type_oid();
        let raw = datum.datum();
        unsafe {
            if oid == pg_sys::FLOAT8OID {
                f64::from_datum(raw, false)
            } else if oid == pg_sys::FLOAT4OID {
                f32::from_datum(raw, false).map(|v| v as f64)
            } else {
                return Err(ConvError::InvariantViolated(
                    "F64 encoder: datum source type is not float8/float4",
                ));
            }
        }
        .ok_or(ConvError::InvariantViolated(
            "F64 encoder: present float datum read as null",
        ))
    }

    fn from_cell(cell: &Cell) -> ConvResult<Option<f64>> {
        Ok(match cell {
            Cell::F64(v) => Some(*v),
            Cell::F32(v) => Some(*v as f64),
            _ => None,
        })
    }
}

// ---------------------------------------------------------------------------
// Bool write encoder
// ---------------------------------------------------------------------------

pub(crate) struct BoolEncoder {
    builder: BooleanBuilder,
}

impl BoolEncoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: BooleanBuilder::with_capacity(capacity),
        }
    }
}

impl ColumnAppend for BoolEncoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<usize> {
        let v = unsafe {
            read_oid(
                datum,
                pg_sys::BOOLOID,
                bool::from_datum,
                "Bool encoder: datum source type is not bool",
            )
        }?;
        self.builder.append_value(v);
        Ok(std::mem::size_of::<bool>())
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        let Cell::Bool(v) = cell else {
            return Err(cell_type_mismatch("bool"));
        };
        self.builder.append_value(*v);
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}
