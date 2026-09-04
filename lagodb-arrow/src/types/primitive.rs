//! Primitive scalar conversion: `bool` plus the integer/float family, both
//! read (`Arrow → Cell`) and write (bound datum / `Cell` → Arrow builder).
//!
//! The integer/float/`date`/`time` write encoders all collapse into the single
//! generic [`PrimitiveEncoder`], parameterized by a zero-sized [`PrimitiveConv`]
//! marker that carries only the per-type differences (Arrow type and `Cell`
//! value codec). Relation-bound source OIDs and widening choices are selected
//! by the enclosing bound encoder. `bool` keeps a dedicated encoder
//! because `BooleanBuilder` is not an [`arrow_array::builder::PrimitiveBuilder`].

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{ArrayRef, ArrowPrimitiveType};
use lagodb_core::tuple::Cell;
use pgrx::{FromDatum, pg_sys};

use super::{ColumnAppend, cell_type_mismatch, read_bound};
use crate::error::ArrowConversionResult;

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

    /// Read the native Arrow value from a buffered [`Cell`].
    ///
    /// `Ok(None)` means the cell variant does not match this column (the write
    /// path rejects it); `Err` is a value-level codec failure.
    fn from_cell(
        cell: &Cell,
    ) -> ArrowConversionResult<Option<<Self::Arrow as ArrowPrimitiveType>::Native>>;
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

    pub(super) fn append_bound_value(
        &mut self,
        value: ArrowConversionResult<<C::Arrow as ArrowPrimitiveType>::Native>,
    ) -> ArrowConversionResult<usize> {
        self.builder.append_value(value?);
        Ok(Self::VALUE_BYTES)
    }
}

impl<C: PrimitiveConv> ColumnAppend for PrimitiveEncoder<C> {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        let value =
            C::from_cell(cell)?.ok_or_else(|| cell_type_mismatch(C::LABEL))?;
        self.builder.append_value(value);
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
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

    fn from_cell(cell: &Cell) -> ArrowConversionResult<Option<i32>> {
        Ok(match cell {
            Cell::I32(v) => Some(*v),
            Cell::I16(v) => Some(*v as i32),
            Cell::I8(v) => Some(*v as i32),
            _ => None,
        })
    }
}

impl I32Conv {
    /// Read a non-NULL PostgreSQL `int2` datum and widen it to `int4`.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `int2` Datum.
    pub(super) unsafe fn from_int2(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<i32> {
        unsafe {
            read_bound(
                datum,
                i16::from_datum,
                "I32 encoder: present int2 datum read as null",
            )
        }
        .map(|value| value as i32)
    }

    /// Read a non-NULL PostgreSQL `int4` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `int4` Datum.
    pub(super) unsafe fn from_int4(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<i32> {
        unsafe {
            read_bound(
                datum,
                i32::from_datum,
                "I32 encoder: present int4 datum read as null",
            )
        }
    }

    /// Read a non-NULL PostgreSQL internal `char` datum and widen it to `int4`.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `char` Datum.
    pub(super) unsafe fn from_char(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<i32> {
        unsafe {
            read_bound(
                datum,
                i8::from_datum,
                "I32 encoder: present char datum read as null",
            )
        }
        .map(|value| value as i32)
    }
}

/// `int8` column, widening an `int4` / `int2` source.
pub(crate) struct I64Conv;
impl PrimitiveConv for I64Conv {
    type Arrow = Int64Type;
    const LABEL: &'static str = "int8";

    fn from_cell(cell: &Cell) -> ArrowConversionResult<Option<i64>> {
        Ok(match cell {
            Cell::I64(v) => Some(*v),
            Cell::I32(v) => Some(*v as i64),
            Cell::I16(v) => Some(*v as i64),
            _ => None,
        })
    }
}

impl I64Conv {
    /// Read a non-NULL PostgreSQL `int2` datum and widen it to `int8`.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `int2` Datum.
    pub(super) unsafe fn from_int2(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<i64> {
        unsafe {
            read_bound(
                datum,
                i16::from_datum,
                "I64 encoder: present int2 datum read as null",
            )
        }
        .map(|value| value as i64)
    }

    /// Read a non-NULL PostgreSQL `int4` datum and widen it to `int8`.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `int4` Datum.
    pub(super) unsafe fn from_int4(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<i64> {
        unsafe {
            read_bound(
                datum,
                i32::from_datum,
                "I64 encoder: present int4 datum read as null",
            )
        }
        .map(|value| value as i64)
    }

    /// Read a non-NULL PostgreSQL `int8` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `int8` Datum.
    pub(super) unsafe fn from_int8(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<i64> {
        unsafe {
            read_bound(
                datum,
                i64::from_datum,
                "I64 encoder: present int8 datum read as null",
            )
        }
    }
}

/// `float4` column.
pub(crate) struct F32Conv;
impl PrimitiveConv for F32Conv {
    type Arrow = Float32Type;
    const LABEL: &'static str = "float4";

    fn from_cell(cell: &Cell) -> ArrowConversionResult<Option<f32>> {
        Ok(match cell {
            Cell::F32(v) => Some(*v),
            _ => None,
        })
    }
}

impl F32Conv {
    /// Read a non-NULL PostgreSQL `float4` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `float4` Datum.
    pub(super) unsafe fn from_float4(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<f32> {
        unsafe {
            read_bound(
                datum,
                f32::from_datum,
                "F32 encoder: present float4 datum read as null",
            )
        }
    }
}

/// `float8` column, widening a `float4` source.
pub(crate) struct F64Conv;
impl PrimitiveConv for F64Conv {
    type Arrow = Float64Type;
    const LABEL: &'static str = "float8";

    fn from_cell(cell: &Cell) -> ArrowConversionResult<Option<f64>> {
        Ok(match cell {
            Cell::F64(v) => Some(*v),
            Cell::F32(v) => Some(*v as f64),
            _ => None,
        })
    }
}

impl F64Conv {
    /// Read a non-NULL PostgreSQL `float4` datum and widen it to `float8`.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `float4` Datum.
    pub(super) unsafe fn from_float4(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<f64> {
        unsafe {
            read_bound(
                datum,
                f32::from_datum,
                "F64 encoder: present float4 datum read as null",
            )
        }
        .map(|value| value as f64)
    }

    /// Read a non-NULL PostgreSQL `float8` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `float8` Datum.
    pub(super) unsafe fn from_float8(
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<f64> {
        unsafe {
            read_bound(
                datum,
                f64::from_datum,
                "F64 encoder: present float8 datum read as null",
            )
        }
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

    /// Append a non-NULL PostgreSQL `bool` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `bool` Datum.
    pub(super) unsafe fn append_bound(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let value = unsafe {
            read_bound(
                datum,
                bool::from_datum,
                "Bool encoder: present bool datum read as null",
            )
        }?;
        self.builder.append_value(value);
        Ok(std::mem::size_of::<bool>())
    }
}

impl ColumnAppend for BoolEncoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        let Cell::Bool(v) = cell else {
            return Err(cell_type_mismatch("bool"));
        };
        self.builder.append_value(*v);
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}
