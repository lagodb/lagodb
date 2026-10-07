//! Cell-compatible list readers, bound to complete child arrays once per batch.

use std::{cell::RefCell, rc::Rc};

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, LargeStringArray, ListArray, StringArray,
};
use arrow_schema::DataType;
use lagodb_core::tuple::{
    ArrayDatumBuffer, ArrayDatumLayout, Cell, ColumnDatumCodec, ColumnDatumTarget,
};
use pgrx::pg_sys;

use super::{is_null_unchecked, str_view_cell};
use crate::column_encoder::downcast;
use crate::{ArrowConversionResult, ListElementRule};

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
    codec: ColumnDatumCodec,
    buffer: Rc<RefCell<ArrayDatumBuffer>>,
}

impl ListReader {
    pub(super) fn bind(
        array: &dyn Array,
        element: ListElementRule,
        oid: pg_sys::Oid,
    ) -> ArrowConversionResult<Self> {
        Self::bind_with_buffer(
            array,
            element,
            ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(oid))?,
            Rc::new(RefCell::new(ArrayDatumBuffer::new(ArrayDatumLayout::bind(
                oid,
            )))),
        )
    }

    pub(crate) fn bind_with_buffer(
        array: &dyn Array,
        element: ListElementRule,
        codec: ColumnDatumCodec,
        buffer: Rc<RefCell<ArrayDatumBuffer>>,
    ) -> ArrowConversionResult<Self> {
        let array = downcast::<ListArray>(array, "List")?;
        let values = array.values().as_ref();
        let values = match element {
            ListElementRule::Bool => {
                ListValues::Bool(downcast::<BooleanArray>(values, "Boolean")?.clone())
            }
            ListElementRule::Short => {
                ListValues::I16(downcast::<Int16Array>(values, "Int16")?.clone())
            }
            ListElementRule::Int => {
                ListValues::I32(downcast::<Int32Array>(values, "Int32")?.clone())
            }
            ListElementRule::Long => {
                ListValues::I64(downcast::<Int64Array>(values, "Int64")?.clone())
            }
            ListElementRule::Float => {
                ListValues::F32(downcast::<Float32Array>(values, "Float32")?.clone())
            }
            ListElementRule::Double => {
                ListValues::F64(downcast::<Float64Array>(values, "Float64")?.clone())
            }
            ListElementRule::String if *values.data_type() == DataType::LargeUtf8 => {
                ListValues::LargeUtf8(
                    downcast::<LargeStringArray>(values, "LargeUtf8")?.clone(),
                )
            }
            ListElementRule::String => {
                ListValues::Utf8(downcast::<StringArray>(values, "Utf8")?.clone())
            }
        };
        Ok(Self {
            array: array.clone(),
            values,
            codec,
            buffer,
        })
    }

    /// # Safety
    /// The row is within the bound batch and its destination context is active.
    pub(crate) unsafe fn datum(
        &self,
        row: usize,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        if unsafe { is_null_unchecked(&self.array, row) } {
            return Ok(None);
        }
        let (start, len) = unsafe { self.range(row) };
        let mut buffer = self.buffer.borrow_mut();
        buffer.reset(len);
        unsafe { self.values.append_to(start, len, self.codec, &mut buffer) }?;
        Ok(Some(unsafe { buffer.form() }))
    }

    /// # Safety
    /// The row is within the bound batch. The returned Cell owns its elements.
    pub(super) unsafe fn cell(
        &self,
        row: usize,
    ) -> ArrowConversionResult<Option<Cell>> {
        if unsafe { is_null_unchecked(&self.array, row) } {
            return Ok(None);
        }
        let (start, len) = unsafe { self.range(row) };
        let cell = match &self.values {
            ListValues::Bool(a) => {
                Cell::BoolArray(a.slice(start, len).iter().collect())
            }
            ListValues::I16(a) => {
                Cell::I16Array(a.slice(start, len).iter().collect())
            }
            ListValues::I32(a) => {
                Cell::I32Array(a.slice(start, len).iter().collect())
            }
            ListValues::I64(a) => {
                Cell::I64Array(a.slice(start, len).iter().collect())
            }
            ListValues::F32(a) => {
                Cell::F32Array(a.slice(start, len).iter().collect())
            }
            ListValues::F64(a) => {
                Cell::F64Array(a.slice(start, len).iter().collect())
            }
            ListValues::Utf8(a) => Cell::StringArray(
                a.slice(start, len)
                    .iter()
                    .map(|v| v.map(str::to_owned))
                    .collect(),
            ),
            ListValues::LargeUtf8(a) => Cell::StringArray(
                a.slice(start, len)
                    .iter()
                    .map(|v| v.map(str::to_owned))
                    .collect(),
            ),
        };
        Ok(Some(cell))
    }

    unsafe fn range(&self, row: usize) -> (usize, usize) {
        let offsets = self.array.value_offsets();
        let start = unsafe { *offsets.get_unchecked(row) } as usize;
        let end = unsafe { *offsets.get_unchecked(row + 1) } as usize;
        (start, end - start)
    }
}

impl ListValues {
    unsafe fn append_to(
        &self,
        start: usize,
        len: usize,
        codec: ColumnDatumCodec,
        buffer: &mut ArrayDatumBuffer,
    ) -> ArrowConversionResult<()> {
        macro_rules! value {
            ($array:expr, $convert:expr) => {{
                for index in start..start + len {
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
