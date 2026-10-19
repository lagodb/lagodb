//! Owned Cell array encoders. Datum sources use the bound container codecs.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, ListBuilder, StringBuilder,
};
use arrow_schema::FieldRef;
use lagodb_core::datum::Cell;

use super::ColumnAppend;
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::rule::ListElementRule;

/// Expand `$body` once per [`ListInner`] element-builder variant, binding the
/// active builder to `$b`. Keeps the six element kinds in one place for the
/// uniform list operations (`finish`, `len`, null append).
macro_rules! dispatch_list_inner {
    ($self:expr, $b:ident => $body:expr) => {
        match $self {
            ListInner::Bool($b) => $body,
            ListInner::Short($b) => $body,
            ListInner::Int($b) => $body,
            ListInner::Long($b) => $body,
            ListInner::Float($b) => $body,
            ListInner::Double($b) => $body,
            ListInner::String($b) => $body,
        }
    };
}

// ---------------------------------------------------------------------------
// Write encoder
// ---------------------------------------------------------------------------

enum ListInner {
    Bool(ListBuilder<BooleanBuilder>),
    Short(ListBuilder<Int16Builder>),
    Int(ListBuilder<Int32Builder>),
    Long(ListBuilder<Int64Builder>),
    Float(ListBuilder<Float32Builder>),
    Double(ListBuilder<Float64Builder>),
    String(ListBuilder<StringBuilder>),
}

pub(crate) struct ListEncoder {
    inner: ListInner,
}

impl ListEncoder {
    pub(crate) fn with_capacity(
        capacity: usize,
        element: ListElementRule,
        field: FieldRef,
    ) -> Self {
        // Only the matched arm runs, so `field` moves into it without a clone.
        let inner = match element {
            ListElementRule::Bool => ListInner::Bool(
                ListBuilder::with_capacity(BooleanBuilder::new(), capacity)
                    .with_field(field),
            ),
            ListElementRule::Short => ListInner::Short(
                ListBuilder::with_capacity(Int16Builder::new(), capacity)
                    .with_field(field),
            ),
            ListElementRule::Int => ListInner::Int(
                ListBuilder::with_capacity(Int32Builder::new(), capacity)
                    .with_field(field),
            ),
            ListElementRule::Long => ListInner::Long(
                ListBuilder::with_capacity(Int64Builder::new(), capacity)
                    .with_field(field),
            ),
            ListElementRule::Float => ListInner::Float(
                ListBuilder::with_capacity(Float32Builder::new(), capacity)
                    .with_field(field),
            ),
            ListElementRule::Double => ListInner::Double(
                ListBuilder::with_capacity(Float64Builder::new(), capacity)
                    .with_field(field),
            ),
            ListElementRule::String => ListInner::String(
                ListBuilder::with_capacity(StringBuilder::new(), capacity)
                    .with_field(field),
            ),
        };
        Self { inner }
    }
}

impl ColumnAppend for ListEncoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        let mismatch = |element: &str| {
            ArrowConversionError::IncompatibleColumnType(
                format!("List<{element}>"),
                "row cell has an incompatible array type".to_string(),
            )
        };
        match &mut self.inner {
            ListInner::Bool(b) => {
                let Cell::BoolArray(arr) = cell else {
                    return Err(mismatch("boolean"));
                };
                arr.iter().for_each(|v| b.values().append_option(*v));
                b.append(true);
            }
            ListInner::Short(b) => {
                let Cell::I16Array(arr) = cell else {
                    return Err(mismatch("smallint"));
                };
                arr.iter().for_each(|v| b.values().append_option(*v));
                b.append(true);
            }
            ListInner::Int(b) => match cell {
                Cell::I32Array(arr) => {
                    arr.iter().for_each(|v| b.values().append_option(*v));
                    b.append(true);
                }
                // Widen an Int16 source to Int32, as the bound datum path does.
                Cell::I16Array(arr) => {
                    arr.iter()
                        .for_each(|v| b.values().append_option(v.map(|x| x as i32)));
                    b.append(true);
                }
                _ => return Err(mismatch("int")),
            },
            ListInner::Long(b) => {
                let Cell::I64Array(arr) = cell else {
                    return Err(mismatch("long"));
                };
                arr.iter().for_each(|v| b.values().append_option(*v));
                b.append(true);
            }
            ListInner::Float(b) => {
                let Cell::F32Array(arr) = cell else {
                    return Err(mismatch("float"));
                };
                arr.iter().for_each(|v| b.values().append_option(*v));
                b.append(true);
            }
            ListInner::Double(b) => {
                let Cell::F64Array(arr) = cell else {
                    return Err(mismatch("double"));
                };
                arr.iter().for_each(|v| b.values().append_option(*v));
                b.append(true);
            }
            ListInner::String(b) => {
                let Cell::StringArray(arr) = cell else {
                    return Err(mismatch("string"));
                };
                for v in arr {
                    match v {
                        Some(s) => b.values().append_value(s),
                        None => b.values().append_null(),
                    }
                }
                b.append(true);
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        dispatch_list_inner!(&mut self.inner, b => b.append(false));
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        let array: ArrayRef =
            dispatch_list_inner!(&mut self.inner, b => Arc::new(b.finish()));
        Ok(array)
    }

    fn len(&self) -> usize {
        dispatch_list_inner!(&self.inner, b => b.len())
    }
}
