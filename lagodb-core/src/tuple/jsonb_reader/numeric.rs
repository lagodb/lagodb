//! Numeric leaves retain their binary representation through conversion.

use std::marker::PhantomData;

use pgrx::{FromDatum, pg_sys};

use crate::tuple::{Decimal128NumericCodec, DecimalCodecError};

unsafe extern "C-unwind" {
    #[link_name = "numeric_float4"]
    fn pg_numeric_float4(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
    #[link_name = "numeric_float8"]
    fn pg_numeric_float8(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
}

/// A Numeric borrowed from JSONB. All methods run inside `JsonbReader::read`.
#[derive(Clone, Copy)]
pub struct JsonbNumeric<'a> {
    pointer: pg_sys::Numeric,
    borrowed: PhantomData<&'a pg_sys::NumericData>,
}

impl JsonbNumeric<'_> {
    pub(super) fn new(pointer: pg_sys::Numeric) -> Self {
        Self {
            pointer,
            borrowed: PhantomData,
        }
    }

    /// Exact fixed-scale conversion: fractional loss and overflow are errors.
    pub fn decimal(
        self,
        codec: &Decimal128NumericCodec,
    ) -> Result<i128, DecimalCodecError> {
        // SAFETY: the Numeric borrows the live JSONB document. The enclosing
        // provider/FFI boundary handles errors, including from numeric_send.
        unsafe { codec.encode_bound_datum(pg_sys::Datum::from(self.pointer)) }
    }

    pub fn float4(self) -> f32 {
        // SAFETY: numeric_float4 accepts a live Numeric and reports range
        // errors to the provider/FFI boundary. DirectFunctionCall uses a stack frame.
        unsafe {
            let datum = pg_sys::DirectFunctionCall1Coll(
                Some(pg_numeric_float4),
                pg_sys::InvalidOid,
                pg_sys::Datum::from(self.pointer),
            );
            f32::from_datum(datum, false).expect("numeric_float4 returns non-NULL")
        }
    }

    pub fn float8(self) -> f64 {
        // SAFETY: identical lifetime/error contract to float4.
        unsafe {
            let datum = pg_sys::DirectFunctionCall1Coll(
                Some(pg_numeric_float8),
                pg_sys::InvalidOid,
                pg_sys::Datum::from(self.pointer),
            );
            f64::from_datum(datum, false).expect("numeric_float8 returns non-NULL")
        }
    }
}
