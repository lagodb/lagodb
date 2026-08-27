//! Timestamp write encoders shared by the row-world and relation-bound paths.
//!
//! PostgreSQL stores both `timestamp` and `timestamptz` as microsecond values.
//! The outer encoder chooses the source reader and timezone metadata, while
//! the unit marker selects the Arrow value conversion at compile time.

use std::mem::size_of;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, PrimitiveBuilder};
use arrow_array::types::{
    ArrowTimestampType, TimestampMicrosecondType, TimestampNanosecondType,
};
use lagodb_core::tuple::Cell;
use pgrx::prelude::{Timestamp, TimestampWithTimeZone};
use pgrx::{FromDatum, pg_sys};

use super::temporal::{unix_micros_from_timestamp, unix_nanos_from_timestamp};
use super::{ColumnAppend, cell_type_mismatch, read_bound};
use crate::error::ArrowConversionResult;

/// Per-unit conversion and Arrow type for a timestamp column. PostgreSQL
/// always supplies microseconds; the marker selects the output unit at
/// compile time so the datum path has no unit dispatch.
pub(crate) trait TimestampUnit {
    type Arrow: ArrowTimestampType;

    fn from_pg_micros(pg_micros: i64) -> ArrowConversionResult<i64>;
}

pub(crate) struct Micros;

impl TimestampUnit for Micros {
    type Arrow = TimestampMicrosecondType;

    #[inline]
    fn from_pg_micros(pg_micros: i64) -> ArrowConversionResult<i64> {
        unix_micros_from_timestamp(pg_micros)
    }
}

pub(crate) struct Nanos;

impl TimestampUnit for Nanos {
    type Arrow = TimestampNanosecondType;

    #[inline]
    fn from_pg_micros(pg_micros: i64) -> ArrowConversionResult<i64> {
        unix_nanos_from_timestamp(pg_micros)
    }
}

/// Arrow timestamp builder shared by the row-world and relation-bound paths.
/// The unit is static in `U`; timezone metadata remains an output concern and
/// is supplied by the caller when the batch is finished.
pub(crate) struct TimestampColumn<U: TimestampUnit> {
    builder: PrimitiveBuilder<U::Arrow>,
}

impl<U: TimestampUnit> TimestampColumn<U> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<U::Arrow>::with_capacity(capacity),
        }
    }

    /// Convert a PostgreSQL microsecond value and append it to the builder.
    #[inline]
    fn append_pg_micros(&mut self, pg_micros: i64) -> ArrowConversionResult<()> {
        self.builder.append_value(U::from_pg_micros(pg_micros)?);
        Ok(())
    }

    #[inline]
    unsafe fn append_datum<T>(
        &mut self,
        datum: pg_sys::Datum,
        from: unsafe fn(pg_sys::Datum, bool) -> Option<T>,
        invariant: &'static str,
    ) -> ArrowConversionResult<usize>
    where
        T: Into<i64>,
    {
        let pg_micros: i64 = unsafe { read_bound(datum, from, invariant) }?.into();
        self.append_pg_micros(pg_micros)?;
        Ok(size_of::<i64>())
    }

    /// Append a non-NULL PostgreSQL `timestamp` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `timestamp` Datum.
    pub(super) unsafe fn append_timestamp(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        unsafe {
            self.append_datum(
                datum,
                Timestamp::from_datum,
                "Timestamp encoder: present timestamp datum read as null",
            )
        }
    }

    /// Append a non-NULL PostgreSQL `timestamptz` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `timestamptz` Datum.
    pub(super) unsafe fn append_timestamptz(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        unsafe {
            self.append_datum(
                datum,
                TimestampWithTimeZone::from_datum,
                "Timestamp encoder: present timestamptz datum read as null",
            )
        }
    }

    pub(super) fn append_null(&mut self) {
        self.builder.append_null();
    }

    pub(super) fn finish(
        &mut self,
        timezone: Option<&str>,
    ) -> ArrowConversionResult<ArrayRef> {
        let array = self.builder.finish();
        let array = match timezone {
            Some(timezone) => array.with_timezone(timezone),
            None => array,
        };
        Ok(Arc::new(array))
    }

    pub(super) fn len(&self) -> usize {
        self.builder.len()
    }
}

enum TsBuilder {
    Micros(TimestampColumn<Micros>),
    Nanos(TimestampColumn<Nanos>),
}

pub(crate) struct TimestampEncoder {
    inner: TsBuilder,
    tz: bool,
}

impl TimestampEncoder {
    pub(crate) fn with_capacity(capacity: usize, nanos: bool, tz: bool) -> Self {
        let inner = if nanos {
            TsBuilder::Nanos(TimestampColumn::<Nanos>::with_capacity(capacity))
        } else {
            TsBuilder::Micros(TimestampColumn::<Micros>::with_capacity(capacity))
        };
        Self { inner, tz }
    }

    fn append_micros(&mut self, pg_micros: i64) -> ArrowConversionResult<()> {
        match &mut self.inner {
            TsBuilder::Nanos(column) => column.append_pg_micros(pg_micros),
            TsBuilder::Micros(column) => column.append_pg_micros(pg_micros),
        }
    }
}

impl ColumnAppend for TimestampEncoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        let pg_micros = match (self.tz, cell) {
            (true, Cell::Timestamptz(ts)) => i64::from(*ts),
            (false, Cell::Timestamp(ts)) => i64::from(*ts),
            _ => {
                return Err(cell_type_mismatch(if self.tz {
                    "timestamptz"
                } else {
                    "timestamp"
                }));
            }
        };
        self.append_micros(pg_micros)
    }

    fn append_null(&mut self) {
        match &mut self.inner {
            TsBuilder::Nanos(column) => column.append_null(),
            TsBuilder::Micros(column) => column.append_null(),
        }
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        // tz-aware columns carry the `+00:00` tag; the stored values are
        // tz-independent.
        let timezone = if self.tz { Some("+00:00") } else { None };
        match &mut self.inner {
            TsBuilder::Nanos(column) => column.finish(timezone),
            TsBuilder::Micros(column) => column.finish(timezone),
        }
    }

    fn len(&self) -> usize {
        match &self.inner {
            TsBuilder::Nanos(column) => column.len(),
            TsBuilder::Micros(column) => column.len(),
        }
    }
}

/// The bound enum chooses timestamp versus timestamptz metadata. These aliases
/// keep the Arrow unit fixed in the inner encoder without duplicating it.
pub(crate) type BoundTimestampMicrosEncoder = TimestampColumn<Micros>;
pub(crate) type BoundTimestampNanosEncoder = TimestampColumn<Nanos>;
