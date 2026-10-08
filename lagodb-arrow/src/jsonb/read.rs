//! Batch-bound nested traversal. Only the requested row/column is materialized.

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use lagodb_core::tuple::{JsonbBuilder, JsonbPosition};
use pgrx::pg_sys;

use super::scalar::ScalarReader;
use super::{NestedJsonbPlan, NodePlan};
use crate::ArrowConversionResult;

pub(crate) struct BoundNestedJsonb {
    nulls: Option<NullBuffer>,
    value: BoundValue,
}

enum BoundValue {
    Scalar(ScalarReader),
    Struct(Box<[(Arc<str>, BoundNestedJsonb)]>),
    List {
        offsets: OffsetBuffer<i32>,
        element: Box<BoundNestedJsonb>,
    },
    Map {
        offsets: OffsetBuffer<i32>,
        key: Box<BoundNestedJsonb>,
        value: Box<BoundNestedJsonb>,
        object: bool,
    },
}

impl BoundNestedJsonb {
    /// # Safety
    /// `plan.accepts(array.data_type())` must hold for the complete tree.
    /// The decoder's checked entry validates it once; trusted scans instead
    /// establish it through their producer's fixed projection schema. Downcast
    /// alone does not establish decimal scale or fixed binary UUID width.
    pub(crate) unsafe fn bind_unchecked(
        plan: &NestedJsonbPlan,
        array: &dyn Array,
    ) -> ArrowConversionResult<Self> {
        let value = match &plan.node {
            NodePlan::Scalar(plan) => BoundValue::Scalar(plan.bind(array)?),
            NodePlan::Struct(fields) => {
                let array = array.as_struct();
                let fields = fields
                    .iter()
                    .zip(array.columns())
                    .map(|(field, column)| {
                        Ok((
                            Arc::clone(&field.name),
                            // SAFETY: the parent contract includes field order
                            // and all child types.
                            unsafe {
                                Self::bind_unchecked(&field.value, column.as_ref())
                            }?,
                        ))
                    })
                    .collect::<ArrowConversionResult<Box<[_]>>>()?;
                BoundValue::Struct(fields)
            }
            NodePlan::List(element) => {
                let array = array.as_list::<i32>();
                BoundValue::List {
                    offsets: array.offsets().clone(),
                    // SAFETY: the parent contract includes the element type.
                    element: Box::new(unsafe {
                        Self::bind_unchecked(element, array.values().as_ref())
                    }?),
                }
            }
            NodePlan::Map { key, value, object } => {
                let array = array.as_map();
                BoundValue::Map {
                    offsets: array.offsets().clone(),
                    // SAFETY: the map contract includes key and value types.
                    key: Box::new(unsafe {
                        Self::bind_unchecked(key, array.keys().as_ref())
                    }?),
                    // SAFETY: the map contract includes key and value types.
                    value: Box::new(unsafe {
                        Self::bind_unchecked(value, array.values().as_ref())
                    }?),
                    object: *object,
                }
            }
        };
        Ok(Self {
            nulls: array.nulls().cloned(),
            value,
        })
    }

    /// # Safety
    /// `row` is in bounds; a backend and its destination row context are active.
    pub(crate) unsafe fn read_datum(
        &self,
        row: usize,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        // SAFETY: the caller established the row bound.
        if unsafe { self.is_null(row) } {
            return Ok(None);
        }
        // SAFETY: the root is a non-null container and the bound traversal
        // emits balanced containers. Array buffers stay live through build.
        unsafe {
            JsonbBuilder::build(|out| {
                self.append_value(row, JsonbPosition::Value, out)
            })
        }
        .map(Some)
    }

    unsafe fn is_null(&self, row: usize) -> bool {
        self.nulls.as_ref().is_some_and(|nulls| {
            // SAFETY: parent offsets or the enclosing batch established row.
            !unsafe { nulls.inner().value_unchecked(row) }
        })
    }

    unsafe fn append<'a>(
        &'a self,
        row: usize,
        position: JsonbPosition,
        out: &mut JsonbBuilder<'a>,
    ) -> ArrowConversionResult<()> {
        // SAFETY: parent offsets establish the child row; token is schema-bound.
        unsafe {
            if self.is_null(row) {
                out.null(position);
                return Ok(());
            }
            self.append_value(row, position, out)
        }
    }

    unsafe fn append_value<'a>(
        &'a self,
        row: usize,
        position: JsonbPosition,
        out: &mut JsonbBuilder<'a>,
    ) -> ArrowConversionResult<()> {
        // SAFETY: each arm emits a valid sequence for its already-bound node.
        unsafe {
            match &self.value {
                BoundValue::Scalar(reader) => reader.append(row, position, out)?,
                BoundValue::Struct(fields) => {
                    out.begin_object();
                    for (name, field) in fields {
                        out.string_unchecked(JsonbPosition::Key, name);
                        field.append(row, JsonbPosition::Value, out)?;
                    }
                    out.end_object();
                }
                BoundValue::List { offsets, element } => {
                    out.begin_array();
                    let start = *offsets.get_unchecked(row) as usize;
                    let end = *offsets.get_unchecked(row + 1) as usize;
                    for index in start..end {
                        element.append(index, JsonbPosition::Element, out)?;
                    }
                    out.end_array();
                }
                BoundValue::Map {
                    offsets,
                    key,
                    value,
                    object,
                } => {
                    let start = *offsets.get_unchecked(row) as usize;
                    let end = *offsets.get_unchecked(row + 1) as usize;
                    if *object {
                        out.begin_object();
                        for index in start..end {
                            // Arrow map keys are required. No repeated null
                            // validation of this producer invariant is needed.
                            key.append_value(index, JsonbPosition::Key, out)?;
                            value.append(index, JsonbPosition::Value, out)?;
                        }
                        out.end_object();
                    } else {
                        out.begin_array();
                        for index in start..end {
                            out.begin_object();
                            out.string_unchecked(JsonbPosition::Key, "key");
                            key.append_value(index, JsonbPosition::Value, out)?;
                            out.string_unchecked(JsonbPosition::Key, "value");
                            value.append(index, JsonbPosition::Value, out)?;
                            out.end_object();
                        }
                        out.end_array();
                    }
                }
            }
        }
        Ok(())
    }
}
