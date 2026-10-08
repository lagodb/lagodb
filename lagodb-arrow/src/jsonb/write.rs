//! Recursive JSONB writers sharing the existing column/batch write lifecycle.

use std::mem;
use std::sync::Arc;

use arrow_array::{ArrayRef, ListArray, StructArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, FieldRef, Fields};
use lagodb_core::tuple::{JsonbReader, JsonbRef};
use pgrx::pg_sys;

use super::write_map::MapWriter;
use super::write_scalar::ScalarWriter;
use super::{NestedJsonbDefault, NestedJsonbPlan, NodePlan};
use crate::{ArrowConversionError, ArrowConversionResult};

pub(crate) struct NestedJsonbEncoder {
    root: ValueWriter,
}

pub(super) struct ValueWriter {
    pub(super) node: WriteNode,
    nullable: bool,
    name: Arc<str>,
    default: Option<NestedJsonbDefault>,
}

pub(super) enum WriteNode {
    Scalar(ScalarWriter),
    Struct {
        fields: Fields,
        children: Box<[ValueWriter]>,
        nulls: NullBufferBuilder,
    },
    List {
        field: FieldRef,
        child: Box<ValueWriter>,
        offsets: Vec<i32>,
        nulls: NullBufferBuilder,
    },
    Map(MapWriter),
}

impl NestedJsonbEncoder {
    pub(crate) fn new(
        plan: &NestedJsonbPlan,
        capacity: usize,
    ) -> ArrowConversionResult<Self> {
        Ok(Self {
            root: ValueWriter::new(plan, capacity, false)?,
        })
    }

    /// # Safety
    /// A valid JSONB datum and a PG row context must be live. The enclosing
    /// bound write buffer has already validated UTF-8 server encoding.
    pub(crate) unsafe fn append(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        unsafe { JsonbReader::read(datum, |value| self.append_root(value)) }
    }

    pub(crate) fn clear(&mut self) {
        self.root.clear();
    }

    fn append_root(&mut self, value: JsonbRef<'_>) -> ArrowConversionResult<usize> {
        if matches!(value, JsonbRef::Null) {
            return Err(ArrowConversionError::InvalidInput(
                "use SQL NULL for a null nested column, not JSON null".into(),
            ));
        }
        self.root.append(value)
    }
}

impl NestedJsonbEncoder {
    pub(crate) fn append_null(&mut self) {
        self.root.append_null();
    }
    pub(crate) fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        self.root.finish()
    }
    pub(crate) fn len(&self) -> usize {
        self.root.len()
    }
}

impl ValueWriter {
    pub(super) fn new(
        plan: &NestedJsonbPlan,
        capacity: usize,
        map_key: bool,
    ) -> ArrowConversionResult<Self> {
        let node = match (&plan.node, plan.field.data_type()) {
            (NodePlan::Scalar(scalar), dt) => {
                WriteNode::Scalar(ScalarWriter::new(scalar, dt, capacity)?)
            }
            (NodePlan::Struct(children), DataType::Struct(fields)) => {
                WriteNode::Struct {
                    fields: fields.clone(),
                    children: children
                        .iter()
                        .map(|f| Self::new(&f.value, capacity, map_key))
                        .collect::<ArrowConversionResult<Box<[_]>>>()?,
                    nulls: NullBufferBuilder::new(capacity),
                }
            }
            (NodePlan::List(child), DataType::List(field)) => WriteNode::List {
                field: Arc::clone(field),
                child: Box::new(Self::new(child, capacity, map_key)?),
                offsets: vec![0],
                nulls: NullBufferBuilder::new(capacity),
            },
            (NodePlan::Map { .. }, DataType::Map(_, _)) => {
                WriteNode::Map(MapWriter::new(plan, capacity, map_key)?)
            }
            _ => unreachable!("nested plan and its field are compiled together"),
        };
        Ok(Self {
            node,
            nullable: plan.field.is_nullable(),
            name: plan.field.name().as_str().into(),
            default: plan.default.clone(),
        })
    }

    pub(super) fn append(
        &mut self,
        value: JsonbRef<'_>,
    ) -> ArrowConversionResult<usize> {
        if matches!(value, JsonbRef::Null) {
            if !self.nullable {
                return Err(self.required());
            }
            self.append_null();
            return Ok(1);
        }
        match (&mut self.node, value) {
            (WriteNode::Scalar(scalar), value) => scalar.append(value),
            (
                WriteNode::Struct {
                    children, nulls, ..
                },
                JsonbRef::Object(object),
            ) => {
                let mut matched = 0;
                let mut bytes = 1;
                for child in children {
                    if let Some(value) = object.get(&child.name) {
                        matched += 1;
                        bytes += child.append(value)?;
                    } else {
                        bytes += child.append_missing()?;
                    }
                }
                if matched != object.len() {
                    return Err(ArrowConversionError::InvalidInput(
                        "JSONB object contains fields absent from the target struct"
                            .into(),
                    ));
                }
                nulls.append_non_null();
                Ok(bytes)
            }
            (
                WriteNode::List {
                    child,
                    offsets,
                    nulls,
                    ..
                },
                JsonbRef::Array(array),
            ) => {
                let mut bytes = 5;
                for value in array.values() {
                    bytes += child.append(value)?;
                }
                offsets.push(Self::offset(child.len())?);
                nulls.append_non_null();
                Ok(bytes)
            }
            (WriteNode::Map(map), value) => map.append(value),
            _ => Err(ArrowConversionError::InvalidInput(format!(
                "JSONB container does not match target field {}",
                self.name
            ))),
        }
    }

    fn required(&self) -> ArrowConversionError {
        ArrowConversionError::InvalidInput(format!(
            "required nested field {} cannot be null or missing",
            self.name
        ))
    }

    fn append_missing(&mut self) -> ArrowConversionResult<usize> {
        if let Some(default) = &self.default {
            self.node.append_default(default)
        } else {
            self.append(JsonbRef::Null)
        }
    }

    pub(super) fn append_default(
        &mut self,
        value: Option<&NestedJsonbDefault>,
    ) -> ArrowConversionResult<usize> {
        match value {
            Some(value) => self.node.append_default(value),
            None => self.append(JsonbRef::Null),
        }
    }

    /// Parent validity masks required struct children. These are placeholders,
    /// not user NULLs, so required checks belong in append/append_missing only.
    pub(super) fn append_null(&mut self) {
        match &mut self.node {
            WriteNode::Scalar(scalar) => scalar.append_null(),
            WriteNode::Struct {
                children, nulls, ..
            } => {
                for child in children {
                    child.append_null();
                }
                nulls.append_null();
            }
            WriteNode::List { offsets, nulls, .. } => {
                offsets.push(*offsets.last().expect("list has an initial offset"));
                nulls.append_null();
            }
            WriteNode::Map(map) => map.append_null(),
        }
    }

    pub(super) fn offset(len: usize) -> ArrowConversionResult<i32> {
        i32::try_from(len).map_err(|_| {
            ArrowConversionError::ValueOutOfRange(
                "nested array exceeds Arrow's i32 offset range".into(),
            )
        })
    }

    pub(super) fn len(&self) -> usize {
        match &self.node {
            WriteNode::Scalar(scalar) => scalar.len(),
            WriteNode::Struct { nulls, .. } | WriteNode::List { nulls, .. } => {
                nulls.len()
            }
            WriteNode::Map(map) => map.len(),
        }
    }

    pub(super) fn clear(&mut self) {
        // An input error may leave child lengths unequal. Discard builders
        // directly; constructing an array here would validate a partial row.
        match &mut self.node {
            WriteNode::Scalar(scalar) => {
                scalar.finish();
            }
            WriteNode::Struct {
                children, nulls, ..
            } => {
                for child in children {
                    child.clear();
                }
                nulls.finish();
            }
            WriteNode::List {
                child,
                offsets,
                nulls,
                ..
            } => {
                child.clear();
                offsets.truncate(1);
                nulls.finish();
            }
            WriteNode::Map(map) => map.clear(),
        }
    }

    pub(super) fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        match &mut self.node {
            WriteNode::Scalar(scalar) => Ok(scalar.finish()),
            WriteNode::Struct {
                fields,
                children,
                nulls,
            } => {
                let len = nulls.len();
                let arrays = children
                    .iter_mut()
                    .map(Self::finish)
                    .collect::<ArrowConversionResult<Vec<_>>>()?;
                Ok(Arc::new(StructArray::try_new_with_length(
                    fields.clone(),
                    arrays,
                    nulls.finish(),
                    len,
                )?))
            }
            WriteNode::List {
                field,
                child,
                offsets,
                nulls,
            } => {
                let values = child.finish()?;
                let offsets =
                    OffsetBuffer::new(mem::replace(offsets, vec![0]).into());
                Ok(Arc::new(ListArray::try_new(
                    field.clone(),
                    offsets,
                    values,
                    nulls.finish(),
                )?))
            }
            WriteNode::Map(map) => map.finish(),
        }
    }
}

impl WriteNode {
    fn append_default(
        &mut self,
        value: &NestedJsonbDefault,
    ) -> ArrowConversionResult<usize> {
        match (self, value) {
            (Self::Scalar(scalar), value) => scalar.append_default(value),
            (
                Self::Struct {
                    children, nulls, ..
                },
                NestedJsonbDefault::Struct(values),
            ) => {
                let mut bytes = 1;
                if values.is_empty() {
                    for child in children {
                        bytes += child.append_missing()?;
                    }
                } else {
                    for (child, value) in children.iter_mut().zip(values) {
                        bytes += child.append_default(value.as_ref())?;
                    }
                }
                nulls.append_non_null();
                Ok(bytes)
            }
            (
                Self::List {
                    child,
                    offsets,
                    nulls,
                    ..
                },
                NestedJsonbDefault::List(values),
            ) => {
                let mut bytes = 5;
                for value in values {
                    bytes += child.append_default(value.as_ref())?;
                }
                offsets.push(ValueWriter::offset(child.len())?);
                nulls.append_non_null();
                Ok(bytes)
            }
            (Self::Map(map), NestedJsonbDefault::Map(values)) => {
                map.append_default(values)
            }
            _ => Err(ArrowConversionError::InvariantViolated(
                "write default does not match its container plan",
            )),
        }
    }
}
