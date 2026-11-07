//! Timestamp Arrow builders.
//!
//! PostgreSQL stores both `timestamp` and `timestamptz` as PG-epoch
//! microseconds. The bound `DatumEncoder` variant chooses the timezone
//! metadata, while the unit marker selects the Arrow value conversion at
//! compile time.

use std::mem::size_of;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, PrimitiveBuilder};
use arrow_array::types::{
    ArrowTimestampType, TimestampMicrosecondType, TimestampNanosecondType,
};
use pgrx::pg_sys;

use crate::error::ArrowConversionResult;
use crate::temporal::{unix_micros_from_timestamp, unix_nanos_from_timestamp};

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

/// Arrow timestamp builder. The unit is static in `U`; timezone metadata is an
/// output concern supplied by the caller when the batch is finished.
pub(crate) struct TimestampEncoder<U: TimestampUnit> {
    builder: PrimitiveBuilder<U::Arrow>,
}

impl<U: TimestampUnit> TimestampEncoder<U> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<U::Arrow>::with_capacity(capacity),
        }
    }

    /// Append a non-NULL PostgreSQL `timestamp` or `timestamptz` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `timestamp` or
    /// `timestamptz` Datum.
    #[inline]
    pub(crate) unsafe fn append(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        // DatumGetTimestamp / DatumGetTimestampTz: both are by-value int64.
        let pg_micros = datum.value() as i64;
        self.builder.append_value(U::from_pg_micros(pg_micros)?);
        Ok(size_of::<i64>())
    }

    pub(crate) fn append_null(&mut self) {
        self.builder.append_null();
    }

    pub(crate) fn finish(
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

    pub(crate) fn len(&self) -> usize {
        self.builder.len()
    }
}
