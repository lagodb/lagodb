//! Native primitive-list readers.
//!
//! The element codec and the reusable PostgreSQL array buffer are bound once
//! per decoder in [`ListRead`]; each batch only rebinds the concrete arrays.

use std::{cell::RefCell, rc::Rc};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, LargeStringArray, ListArray, StringArray,
};
use arrow_schema::DataType;
use lagodb_core::datum::{
    ArrayDatumBuffer, ArrayDatumLayout, BoundColumnDatumCodec, Cell,
};
use pgrx::pg_sys;

use super::reader::{is_null_unchecked, str_view_cell};
use crate::{ArrowConversionResult, ListElementRule};

/// Decoder-owned conversion state for one native primitive-list column.
pub(crate) struct ListRead {
    element: ListElementRule,
    codec: BoundColumnDatumCodec,
    buffer: Rc<RefCell<ArrayDatumBuffer>>,
}

impl ListRead {
    /// `codec` converts one element; `layout` forms the declared array type.
    pub(crate) fn new(
        element: ListElementRule,
        codec: BoundColumnDatumCodec,
        layout: ArrayDatumLayout,
    ) -> Self {
        Self {
            element,
            codec,
            buffer: Rc::new(RefCell::new(ArrayDatumBuffer::new(layout))),
        }
    }

    /// Bind one batch's list array.
    ///
    /// # Safety
    /// `array` must be a `List` whose element type is accepted by this
    /// element rule.
    pub(crate) unsafe fn bind(&self, array: &dyn Array) -> ListReader {
        let array = array.as_list::<i32>();
        let values = array.values().as_ref();
        let values = match self.element {
            ListElementRule::Bool => ListValues::Bool(values.as_boolean().clone()),
            ListElementRule::Short => {
                ListValues::I16(values.as_primitive::<Int16Type>().clone())
            }
            ListElementRule::Int => {
                ListValues::I32(values.as_primitive::<Int32Type>().clone())
            }
            ListElementRule::Long => {
                ListValues::I64(values.as_primitive::<Int64Type>().clone())
            }
            ListElementRule::Float => {
                ListValues::F32(values.as_primitive::<Float32Type>().clone())
            }
            ListElementRule::Double => {
                ListValues::F64(values.as_primitive::<Float64Type>().clone())
            }
            ListElementRule::String => match values.data_type() {
                DataType::LargeUtf8 => {
                    ListValues::LargeUtf8(values.as_string::<i64>().clone())
                }
                _ => ListValues::Utf8(values.as_string::<i32>().clone()),
            },
        };
        ListReader {
            array: array.clone(),
            values,
            codec: self.codec,
            buffer: Rc::clone(&self.buffer),
        }
    }
}

enum ListValues {
    Bool(BooleanArray),
    I16(Int16Array),
    I32(Int32Array),
    I64(Int64Array),
    F32(Float32Array),
    F64(Float64Array),
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
}

pub(crate) struct ListReader {
    array: ListArray,
    values: ListValues,
    codec: BoundColumnDatumCodec,
    buffer: Rc<RefCell<ArrayDatumBuffer>>,
}

impl ListReader {
    /// # Safety
    /// The row is within the bound batch and its destination context is active.
    pub(crate) unsafe fn datum(
        &self,
        row: usize,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        if unsafe { is_null_unchecked(&self.array, row) } {
            return Ok(None);
        }
        let offsets = self.array.value_offsets();
        let start = unsafe { *offsets.get_unchecked(row) } as usize;
        let end = unsafe { *offsets.get_unchecked(row + 1) } as usize;
        let mut buffer = self.buffer.borrow_mut();
        buffer.reset(end - start);
        unsafe { self.values.append_to(start, end, self.codec, &mut buffer) }?;
        Ok(Some(unsafe { buffer.form() }))
    }
}

impl ListValues {
    unsafe fn append_to(
        &self,
        start: usize,
        end: usize,
        codec: BoundColumnDatumCodec,
        buffer: &mut ArrayDatumBuffer,
    ) -> ArrowConversionResult<()> {
        macro_rules! value {
            ($array:expr, $convert:expr) => {{
                for index in start..end {
                    let datum = if unsafe { is_null_unchecked($array, index) } {
                        None
                    } else {
                        let cell = $convert(unsafe { $array.value_unchecked(index) });
                        Some(unsafe { codec.cell_to_datum(cell) }?)
                    };
                    buffer.push(datum);
                }
            }};
        }
        match self {
            Self::Bool(a) => value!(a, Cell::Bool),
            Self::I16(a) => value!(a, Cell::I16),
            Self::I32(a) => value!(a, Cell::I32),
            Self::I64(a) => value!(a, Cell::I64),
            Self::F32(a) => value!(a, Cell::F32),
            Self::F64(a) => value!(a, Cell::F64),
            Self::Utf8(a) => value!(a, str_view_cell),
            Self::LargeUtf8(a) => value!(a, str_view_cell),
        }
        Ok(())
    }
}
