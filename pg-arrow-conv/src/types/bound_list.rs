//! Relation-bound PostgreSQL array encoders.
//!
//! The element marker is selected during bound plan construction, so runtime
//! appends decode the already validated PostgreSQL array representation.

use std::ffi::CStr;
use std::marker::PhantomData;
use std::mem::size_of;
use std::str;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, StringBuilder,
};
use arrow_schema::FieldRef;
use pg_lakebase_core::tuple::DetoastedVarlena;
use pgrx::{Array as PgArray, FromDatum, pg_sys};

use crate::error::{ArrowConversionError, ArrowConversionResult};

/// Detoast one PostgreSQL array and reject shape information that a single
/// Arrow `List` cannot preserve.
///
/// # Safety
///
/// `raw` must be a valid non-NULL PostgreSQL array datum.
unsafe fn linear_array_datum(
    raw: pg_sys::Datum,
) -> ArrowConversionResult<DetoastedVarlena> {
    let detoasted = unsafe { DetoastedVarlena::from_datum(raw) };
    let array = detoasted.as_datum().cast_mut_ptr::<pg_sys::ArrayType>();
    let dimensions = unsafe { (*array).ndim };
    if dimensions > 1 {
        return Err(ArrowConversionError::IncompatibleColumnType(
            "PostgreSQL multidimensional array".to_owned(),
            "a single Arrow List cannot preserve array dimensions".to_owned(),
        ));
    }
    if dimensions == 1 {
        // PostgreSQL stores `ndim` dimensions followed by `ndim` lower bounds
        // immediately after ArrayType. Arrow List has an implicit lower bound
        // of one, so accepting another value would silently change subscripts.
        let lower_bounds = unsafe {
            array
                .cast::<u8>()
                .add(size_of::<pg_sys::ArrayType>())
                .cast::<i32>()
                .add(dimensions as usize)
        };
        if unsafe { *lower_bounds } != 1 {
            return Err(ArrowConversionError::IncompatibleColumnType(
                "PostgreSQL array with a non-one lower bound".to_owned(),
                "Arrow List cannot preserve PostgreSQL array lower bounds".to_owned(),
            ));
        }
    }
    Ok(detoasted)
}

/// Element-specific part of a relation-bound PostgreSQL array codec.
///
/// The marker type is selected while the bound write plan is built.  The
/// resulting [`BoundListEncoder`] is monomorphized for that element type, so
/// appending a row never re-matches the source family.
pub(crate) trait BoundListElement {
    type Values: ArrayBuilder;

    fn new_values() -> Self::Values;

    /// Decode and append one non-NULL array datum for this element type.
    ///
    /// # Safety
    ///
    /// `raw` must be a valid PostgreSQL array datum for the implementing
    /// marker. String implementations additionally require the relation-bound
    /// PG_UTF8 server-encoding invariant.
    unsafe fn append_values(
        values: &mut Self::Values,
        raw: pg_sys::Datum,
    ) -> ArrowConversionResult<usize>;
}

macro_rules! primitive_bound_list_element {
    ($marker:ident, $pg_type:ty, $builder:ty, $width:expr, $map:expr) => {
        pub(crate) struct $marker;

        impl BoundListElement for $marker {
            type Values = $builder;

            fn new_values() -> Self::Values {
                <$builder>::new()
            }

            unsafe fn append_values(
                values: &mut Self::Values,
                raw: pg_sys::Datum,
            ) -> ArrowConversionResult<usize> {
                let detoasted = unsafe { linear_array_datum(raw) }?;
                let array = unsafe {
                    PgArray::<$pg_type>::from_datum(detoasted.as_datum(), false)
                }
                    .ok_or(ArrowConversionError::InvariantViolated(
                        "List encoder: incompatible bound source",
                    ))?;
                for value in array.iter() {
                    values.append_option(value.map($map));
                }
                Ok(array.len() * $width)
            }
        }
    };
}

primitive_bound_list_element!(
    BoolArrayElement,
    bool,
    BooleanBuilder,
    size_of::<bool>(),
    |value| value
);
primitive_bound_list_element!(
    Int2ArrayElement,
    i16,
    Int32Builder,
    size_of::<i32>(),
    |value| value as i32
);
primitive_bound_list_element!(
    Int4ArrayElement,
    i32,
    Int32Builder,
    size_of::<i32>(),
    |value| value
);
primitive_bound_list_element!(
    Int8ArrayElement,
    i64,
    Int64Builder,
    size_of::<i64>(),
    |value| value
);
primitive_bound_list_element!(
    Float4ArrayElement,
    f32,
    Float32Builder,
    size_of::<f32>(),
    |value| value
);
primitive_bound_list_element!(
    Float8ArrayElement,
    f64,
    Float64Builder,
    size_of::<f64>(),
    |value| value
);

macro_rules! string_bound_list_element {
    ($marker:ident, $pg_type:ty, |$value:ident| $map:block) => {
        pub(crate) struct $marker;

        impl BoundListElement for $marker {
            type Values = StringBuilder;

            fn new_values() -> Self::Values {
                StringBuilder::new()
            }

            unsafe fn append_values(
                values: &mut Self::Values,
                raw: pg_sys::Datum,
            ) -> ArrowConversionResult<usize> {
                let detoasted = unsafe { linear_array_datum(raw) }?;
                let array = unsafe {
                    PgArray::<$pg_type>::from_datum(detoasted.as_datum(), false)
                }
                    .ok_or(ArrowConversionError::InvariantViolated(
                        "List encoder: incompatible bound source",
                    ))?;
                let mut payload = 0usize;
                for value in array.iter() {
                    match value {
                        Some($value) => {
                            let value = $map;
                            payload += value.len();
                            values.append_value(value);
                        }
                        None => values.append_null(),
                    }
                }
                Ok(payload)
            }
        }
    };
}

string_bound_list_element!(TextArrayElement, &str, |value| { value });
string_bound_list_element!(VarcharArrayElement, &str, |value| { value });
string_bound_list_element!(BpcharArrayElement, &str, |value| { value });
string_bound_list_element!(JsonArrayElement, &str, |value| { value });
string_bound_list_element!(NameArrayElement, &CStr, |value| {
    // SAFETY: the bound writer validates the PG_UTF8 server-encoding
    // invariant before constructing any runtime encoder.
    unsafe { str::from_utf8_unchecked(value.to_bytes()) }
});

/// A source-specific list encoder. Its marker type is part of the concrete
/// runtime type, so the source conversion is chosen once during plan binding.
pub(crate) struct BoundListEncoder<E: BoundListElement> {
    builder: ListBuilder<E::Values>,
    _element: PhantomData<fn() -> E>,
}

impl<E: BoundListElement> BoundListEncoder<E> {
    pub(super) fn with_capacity(capacity: usize, field: FieldRef) -> Self {
        Self {
            builder: ListBuilder::with_capacity(E::new_values(), capacity)
                .with_field(field),
            _element: PhantomData,
        }
    }

    /// Append a non-NULL PostgreSQL array through the source codec selected at
    /// binding.
    ///
    /// # Safety
    ///
    /// `raw` must be a valid, non-NULL PostgreSQL array datum for `E`, and the
    /// relation-bound UTF-8 invariant must hold for string elements.
    pub(super) unsafe fn append(
        &mut self,
        raw: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let payload = unsafe { E::append_values(self.builder.values(), raw) }?;
        self.builder.append(true);
        Ok(payload)
    }

    pub(super) fn append_null(&mut self) {
        self.builder.append(false);
    }

    pub(super) fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    pub(super) fn len(&self) -> usize {
        self.builder.len()
    }
}

pub(crate) type BoundBoolArrayEncoder = BoundListEncoder<BoolArrayElement>;
pub(crate) type BoundInt2ArrayEncoder = BoundListEncoder<Int2ArrayElement>;
pub(crate) type BoundInt4ArrayEncoder = BoundListEncoder<Int4ArrayElement>;
pub(crate) type BoundInt8ArrayEncoder = BoundListEncoder<Int8ArrayElement>;
pub(crate) type BoundFloat4ArrayEncoder = BoundListEncoder<Float4ArrayElement>;
pub(crate) type BoundFloat8ArrayEncoder = BoundListEncoder<Float8ArrayElement>;
pub(crate) type BoundTextArrayEncoder = BoundListEncoder<TextArrayElement>;
pub(crate) type BoundVarcharArrayEncoder = BoundListEncoder<VarcharArrayElement>;
pub(crate) type BoundBpcharArrayEncoder = BoundListEncoder<BpcharArrayElement>;
pub(crate) type BoundNameArrayEncoder = BoundListEncoder<NameArrayElement>;
pub(crate) type BoundJsonArrayEncoder = BoundListEncoder<JsonArrayElement>;
