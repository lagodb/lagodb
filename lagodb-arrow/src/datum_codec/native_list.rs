//! Relation-bound PostgreSQL array encoders.
//!
//! The element marker is selected during bound plan construction, so runtime
//! appends decode the already validated PostgreSQL array representation.

use std::ffi::CStr;
use std::marker::PhantomData;
use std::mem::{self, size_of};
use std::str;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, ListArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::FieldRef;
use lagodb_core::tuple::{ArrayDatumIter, ArrayDatumLayout};
use pgrx::pg_sys;
use pgrx::varlena::text_to_rust_str_unchecked;

use crate::error::{ArrowConversionError, ArrowConversionResult};

/// Element-specific part of a relation-bound PostgreSQL array codec.
///
/// The marker type is selected while the bound write plan is built.  The
/// resulting [`BoundListEncoder`] is monomorphized for that element type, so
/// appending a row never re-matches the source family.
pub(crate) trait BoundListElement {
    type Values: ArrayBuilder;
    const ELEMENT_OID: pg_sys::Oid;

    fn new_values() -> Self::Values;

    /// Decode and append the elements of one array for this element type,
    /// returning their estimated additional buffer size in bytes.
    ///
    /// # Safety
    ///
    /// `array` must contain elements of the implementing marker's type.
    /// String implementations additionally require the relation-bound PG_UTF8
    /// server-encoding invariant. Borrowed elements must not outlive `array`.
    unsafe fn append_values(
        values: &mut Self::Values,
        array: &mut ArrayDatumIter,
    ) -> usize;
}

macro_rules! primitive_bound_list_element {
    ($marker:ident, $oid:expr, $builder:ty, $width:expr, $map:expr) => {
        pub(crate) struct $marker;

        impl BoundListElement for $marker {
            type Values = $builder;
            const ELEMENT_OID: pg_sys::Oid = $oid;

            fn new_values() -> Self::Values {
                <$builder>::new()
            }

            unsafe fn append_values(
                values: &mut Self::Values,
                array: &mut ArrayDatumIter,
            ) -> usize {
                let start = values.len();
                for value in array {
                    values.append_option(value.map($map));
                }
                (values.len() - start) * $width
            }
        }
    };
}

// Match pgrx's UnboxDatum conversions, including signed int2 widening and
// bit-preserving floating-point decoding. The cursor handles element NULLs.
primitive_bound_list_element!(
    BoolArrayElement,
    pg_sys::BOOLOID,
    BooleanBuilder,
    size_of::<bool>(),
    |value: pg_sys::Datum| value.value() != 0
);
primitive_bound_list_element!(
    ShortArrayElement,
    pg_sys::INT2OID,
    Int16Builder,
    size_of::<i16>(),
    |value: pg_sys::Datum| value.value() as i16
);
primitive_bound_list_element!(
    Int2ArrayElement,
    pg_sys::INT2OID,
    Int32Builder,
    size_of::<i32>(),
    |value: pg_sys::Datum| i32::from(value.value() as i16)
);
primitive_bound_list_element!(
    Int4ArrayElement,
    pg_sys::INT4OID,
    Int32Builder,
    size_of::<i32>(),
    |value: pg_sys::Datum| value.value() as i32
);
primitive_bound_list_element!(
    Int8ArrayElement,
    pg_sys::INT8OID,
    Int64Builder,
    size_of::<i64>(),
    |value: pg_sys::Datum| value.value() as i64
);
primitive_bound_list_element!(
    Float4ArrayElement,
    pg_sys::FLOAT4OID,
    Float32Builder,
    size_of::<f32>(),
    |value: pg_sys::Datum| f32::from_bits(value.value() as u32)
);
primitive_bound_list_element!(
    Float8ArrayElement,
    pg_sys::FLOAT8OID,
    Float64Builder,
    size_of::<f64>(),
    |value: pg_sys::Datum| f64::from_bits(value.value() as u64)
);

macro_rules! string_bound_list_element {
    ($marker:ident, $oid:expr, |$value:ident| $map:block) => {
        pub(crate) struct $marker;

        impl BoundListElement for $marker {
            type Values = StringBuilder;
            const ELEMENT_OID: pg_sys::Oid = $oid;

            fn new_values() -> Self::Values {
                StringBuilder::new()
            }

            unsafe fn append_values(
                values: &mut Self::Values,
                array: &mut ArrayDatumIter,
            ) -> usize {
                let start_len = values.len();
                let start_data_len = values.values_slice().len();
                for value in array {
                    match value {
                        Some($value) => {
                            // Copy borrowed bytes while the cursor retains
                            // their backing array, before advancing it.
                            let value = $map;
                            values.append_value(value);
                        }
                        None => values.append_null(),
                    }
                }
                let added_elements = values.len() - start_len;
                let added_data_bytes = values.values_slice().len() - start_data_len;
                // Every element, including NULL and empty strings, adds an
                // i32 offset. Budget one byte per element for the validity
                // bitmap, matching the recursive encoder's conservative
                // estimate. Builder length deltas need no second traversal.
                added_data_bytes
                    + added_elements * (size_of::<i32>() + size_of::<u8>())
            }
        }
    };
}

string_bound_list_element!(TextArrayElement, pg_sys::TEXTOID, |value| {
    // SAFETY: the cursor retains a non-NULL flat text value; PG_UTF8 is bound.
    unsafe { text_to_rust_str_unchecked(value.cast_mut_ptr()) }
});
string_bound_list_element!(VarcharArrayElement, pg_sys::VARCHAROID, |value| {
    // SAFETY: varchar has flat text storage retained by the cursor; PG_UTF8 is bound.
    unsafe { text_to_rust_str_unchecked(value.cast_mut_ptr()) }
});
string_bound_list_element!(BpcharArrayElement, pg_sys::BPCHAROID, |value| {
    // SAFETY: bpchar has flat text storage retained by the cursor; PG_UTF8 is bound.
    unsafe { text_to_rust_str_unchecked(value.cast_mut_ptr()) }
});
string_bound_list_element!(JsonArrayElement, pg_sys::JSONOID, |value| {
    // SAFETY: json has flat text storage retained by the cursor; PG_UTF8 is bound.
    unsafe { text_to_rust_str_unchecked(value.cast_mut_ptr()) }
});
string_bound_list_element!(NameArrayElement, pg_sys::NAMEOID, |value| {
    // SAFETY: the cursor retains a non-NULL, NUL-terminated name; PG_UTF8 is bound.
    unsafe {
        str::from_utf8_unchecked(CStr::from_ptr(value.cast_mut_ptr()).to_bytes())
    }
});

/// A source-specific list encoder. Its marker type is part of the concrete
/// runtime type, so the source conversion is chosen once during plan binding.
pub(crate) struct BoundListEncoder<E: BoundListElement> {
    values: E::Values,
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    field: FieldRef,
    layout: ArrayDatumLayout,
    _element: PhantomData<fn() -> E>,
}

impl<E: BoundListElement> BoundListEncoder<E> {
    pub(super) fn with_capacity(capacity: usize, field: FieldRef) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Self {
            // Own the list buffers so completion can use try_new directly.
            // ListBuilder only exposes an infallible finish; finishing it
            // first and reconstructing the array would validate twice.
            values: E::new_values(),
            offsets,
            nulls: NullBufferBuilder::new(capacity),
            field,
            layout: ArrayDatumLayout::bind(E::ELEMENT_OID),
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
        // SAFETY: the caller supplies an array of the marker's bound type.
        // The cursor owns any detoasted copy until all elements are appended.
        let mut array = unsafe { self.layout.values(raw) };
        if !array.is_linear() {
            return Err(ArrowConversionError::IncompatibleColumnType(
                "PostgreSQL multidimensional array or non-one lower bound".to_owned(),
                "a single Arrow List cannot preserve array dimensions or lower bounds"
                    .to_owned(),
            ));
        }
        // SAFETY: layout and conversion share the same marker, and the caller
        // guarantees PG_UTF8. Borrowed string elements are copied into Arrow
        // while `array` still retains their backing storage.
        let estimated_bytes =
            unsafe { E::append_values(&mut self.values, &mut array) };
        let offset = i32::try_from(self.values.len()).map_err(|_| {
            ArrowConversionError::ValueOutOfRange(
                "Arrow list offset exceeds i32".into(),
            )
        })?;
        self.offsets.push(offset);
        self.nulls.append_non_null();
        Ok(estimated_bytes)
    }

    pub(super) fn append_null(&mut self) {
        self.offsets
            .push(*self.offsets.last().expect("list starts at zero"));
        self.nulls.append_null();
    }

    pub(super) fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        let values = self.values.finish();
        let nulls = self.nulls.finish();
        let offsets = mem::take(&mut self.offsets);
        self.offsets.push(0);
        // SAFETY: offsets start at zero, only increase with the append-only
        // element builder, and fit i32. As in Arrow's ListBuilder, there is
        // no need to rescan these encoder-owned offsets at batch completion.
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
        // All buffers are reset before this fallible call. Required elements
        // containing NULL return an error and the encoder remains reusable.
        // Nullable fields skip Arrow's element-nullability check entirely.
        Ok(Arc::new(ListArray::try_new(
            Arc::clone(&self.field),
            offsets,
            values,
            nulls,
        )?))
    }

    pub(super) fn len(&self) -> usize {
        self.nulls.len()
    }
}

pub(crate) type BoundBoolArrayEncoder = BoundListEncoder<BoolArrayElement>;
pub(crate) type BoundInt2ArrayEncoder = BoundListEncoder<Int2ArrayElement>;
pub(crate) type BoundShortArrayEncoder = BoundListEncoder<ShortArrayElement>;
pub(crate) type BoundInt4ArrayEncoder = BoundListEncoder<Int4ArrayElement>;
pub(crate) type BoundInt8ArrayEncoder = BoundListEncoder<Int8ArrayElement>;
pub(crate) type BoundFloat4ArrayEncoder = BoundListEncoder<Float4ArrayElement>;
pub(crate) type BoundFloat8ArrayEncoder = BoundListEncoder<Float8ArrayElement>;
pub(crate) type BoundTextArrayEncoder = BoundListEncoder<TextArrayElement>;
pub(crate) type BoundVarcharArrayEncoder = BoundListEncoder<VarcharArrayElement>;
pub(crate) type BoundBpcharArrayEncoder = BoundListEncoder<BpcharArrayElement>;
pub(crate) type BoundNameArrayEncoder = BoundListEncoder<NameArrayElement>;
pub(crate) type BoundJsonArrayEncoder = BoundListEncoder<JsonArrayElement>;
