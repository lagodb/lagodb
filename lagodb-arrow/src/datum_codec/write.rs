//! Source-bound recursive writers. Scalar leaves use the existing encoders.

use std::{mem, sync::Arc};

use arrow_array::builder::{ArrayBuilder, Int64Builder};
use arrow_array::{ArrayRef, ListArray, StructArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, FieldRef, Fields};
use lagodb_core::tuple::{ArrayDatumLayout, CompositeDatumCodec};
use pgrx::pg_sys;

use super::text::TextIoEncoder;
use super::{BoundDatumPlan, NodePlan};
use super::{DatumEncoder, NativeEncoderPlan};
use crate::jsonb::NestedJsonbEncoder;
use crate::{ArrowConversionError, ArrowConversionResult};

impl BoundDatumPlan {
    pub(crate) fn encoder(
        &self,
        capacity: usize,
    ) -> ArrowConversionResult<DatumEncoder> {
        match &self.node {
            NodePlan::Scalar { rule, .. } | NodePlan::NativeList { rule, .. } => {
                NativeEncoderPlan::bind(rule.clone(), self.base_type.oid)?
                    .materialize(capacity)
            }
            NodePlan::NestedJsonb(plan) => Ok(DatumEncoder::NestedJsonb(Box::new(
                NestedJsonbEncoder::new(plan, capacity)?,
            ))),
            _ => Ok(DatumEncoder::Recursive(Box::new(
                RecursiveDatumEncoder::new(self, capacity)?,
            ))),
        }
    }
}

/// Reusable Arrow builders for a column's bound type and storage representation.
pub(crate) struct RecursiveDatumEncoder {
    state: EncoderState,
    nullable: bool,
}

enum EncoderState {
    Scalar(Box<DatumEncoder>),
    TextIo(TextIoEncoder),
    Oid(Int64Builder),
    List {
        layout: ArrayDatumLayout,
        field: FieldRef,
        child: Box<RecursiveDatumEncoder>,
        offsets: Vec<i32>,
        nulls: NullBufferBuilder,
    },
    Struct {
        codec: CompositeDatumCodec,
        fields: Fields,
        children: Box<[(usize, RecursiveDatumEncoder)]>,
        nulls: NullBufferBuilder,
    },
    Interval {
        fields: Fields,
        parts: [Int64Builder; 3],
        nulls: NullBufferBuilder,
    },
}

impl RecursiveDatumEncoder {
    pub(crate) fn new(
        plan: &BoundDatumPlan,
        capacity: usize,
    ) -> ArrowConversionResult<Self> {
        let state = match &plan.node {
            NodePlan::Scalar { rule, .. } => EncoderState::Scalar(Box::new(
                NativeEncoderPlan::bind(rule.clone(), plan.base_type.oid)?
                    .materialize(capacity)?,
            )),
            NodePlan::TextIo { .. } => EncoderState::TextIo(TextIoEncoder::new(
                plan.base_type,
                plan.field.data_type(),
                capacity,
            )),
            NodePlan::Oid => EncoderState::Oid(Int64Builder::with_capacity(capacity)),
            NodePlan::NativeList { rule, .. } => EncoderState::Scalar(Box::new(
                NativeEncoderPlan::bind(rule.clone(), plan.base_type.oid)?
                    .materialize(capacity)?,
            )),
            NodePlan::NestedJsonb(_) => {
                EncoderState::Scalar(Box::new(plan.encoder(capacity)?))
            }
            NodePlan::List {
                layout, element, ..
            } => EncoderState::List {
                layout: *layout,
                field: Arc::clone(&element.field),
                child: Box::new(Self::new(element, capacity)?),
                offsets: vec![0],
                nulls: NullBufferBuilder::new(capacity),
            },
            NodePlan::Struct(children) => {
                let DataType::Struct(fields) = plan.field.data_type() else {
                    unreachable!()
                };
                EncoderState::Struct {
                    codec: CompositeDatumCodec::bind(plan.base_type),
                    fields: fields.clone(),
                    children: children
                        .iter()
                        .map(|(index, child)| {
                            Ok((*index, Self::new(child, capacity)?))
                        })
                        .collect::<ArrowConversionResult<_>>()?,
                    nulls: NullBufferBuilder::new(capacity),
                }
            }
            NodePlan::Interval => {
                let DataType::Struct(fields) = plan.field.data_type() else {
                    unreachable!()
                };
                EncoderState::Interval {
                    fields: fields.clone(),
                    parts: std::array::from_fn(|_| {
                        Int64Builder::with_capacity(capacity)
                    }),
                    nulls: NullBufferBuilder::new(capacity),
                }
            }
        };
        Ok(Self {
            state,
            nullable: plan.field.is_nullable(),
        })
    }

    unsafe fn append_optional(
        &mut self,
        value: Option<pg_sys::Datum>,
    ) -> ArrowConversionResult<usize> {
        match value {
            Some(value) => unsafe { self.append(value) },
            None if self.nullable => {
                self.append_null();
                Ok(1)
            }
            None => Err(ArrowConversionError::InvalidInput(
                "NULL in a required nested field".into(),
            )),
        }
    }

    /// # Safety
    /// The datum has the plan's PG type and a backend row context is active.
    /// PG errors unwind to the enclosing provider/FFI boundary.
    pub(crate) unsafe fn append(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        match &mut self.state {
            EncoderState::Scalar(encoder) => unsafe { encoder.append(datum) },
            EncoderState::TextIo(encoder) => Ok(unsafe { encoder.append(datum) }),
            EncoderState::Oid(builder) => {
                builder.append_value(datum.value() as u32 as i64);
                Ok(8)
            }
            EncoderState::List {
                layout,
                child,
                offsets,
                nulls,
                ..
            } => {
                let values = unsafe { layout.values(datum) };
                if !values.is_linear() {
                    return Err(ArrowConversionError::InvalidInput(
                        "Arrow lists require one-dimensional PG arrays with lower bound one".into()));
                }
                let mut bytes = 5;
                for value in values {
                    bytes += unsafe { child.append_optional(value)? };
                }
                offsets.push(i32::try_from(child.len()).map_err(|_| {
                    ArrowConversionError::ValueOutOfRange(
                        "Arrow list offset exceeds i32".into(),
                    )
                })?);
                nulls.append_non_null();
                Ok(bytes)
            }
            EncoderState::Struct {
                codec,
                children,
                nulls,
                ..
            } => {
                let bytes = unsafe {
                    codec.visit(datum, |values, nulls| {
                        let mut bytes = 1;
                        for (index, child) in children {
                            bytes += child.append_optional(
                                (!nulls[*index]).then_some(values[*index]),
                            )?;
                        }
                        Ok::<_, ArrowConversionError>(bytes)
                    })
                }?;
                nulls.append_non_null();
                Ok(bytes)
            }
            EncoderState::Interval { parts, nulls, .. } => {
                let interval = unsafe { &*datum.cast_mut_ptr::<pg_sys::Interval>() };
                for (builder, value) in parts.iter_mut().zip([
                    i64::from(interval.month),
                    i64::from(interval.day),
                    interval.time,
                ]) {
                    builder.append_value(value);
                }
                nulls.append_non_null();
                Ok(24)
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        match &mut self.state {
            EncoderState::Scalar(encoder) => encoder.clear(),
            EncoderState::TextIo(encoder) => {
                encoder.finish();
            }
            EncoderState::Oid(builder) => {
                builder.finish();
            }
            EncoderState::List {
                child,
                offsets,
                nulls,
                ..
            } => {
                child.clear();
                offsets.clear();
                offsets.push(0);
                nulls.finish();
            }
            EncoderState::Struct {
                children, nulls, ..
            } => {
                for (_, child) in children {
                    child.clear();
                }
                nulls.finish();
            }
            EncoderState::Interval { parts, nulls, .. } => {
                for part in parts {
                    part.finish();
                }
                nulls.finish();
            }
        }
    }
}

impl RecursiveDatumEncoder {
    pub(crate) fn append_null(&mut self) {
        match &mut self.state {
            EncoderState::Scalar(encoder) => encoder.append_null(),
            EncoderState::TextIo(encoder) => encoder.append_null(),
            EncoderState::Oid(builder) => builder.append_null(),
            EncoderState::List { offsets, nulls, .. } => {
                offsets.push(*offsets.last().expect("list starts at zero"));
                nulls.append_null();
            }
            EncoderState::Struct {
                children, nulls, ..
            } => {
                for (_, child) in children {
                    child.append_null();
                }
                nulls.append_null();
            }
            EncoderState::Interval { parts, nulls, .. } => {
                for part in parts {
                    part.append_null();
                }
                nulls.append_null();
            }
        }
    }

    pub(crate) fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(match &mut self.state {
            EncoderState::Scalar(encoder) => return encoder.finish(),
            EncoderState::TextIo(encoder) => encoder.finish(),
            EncoderState::Oid(builder) => Arc::new(builder.finish()),
            EncoderState::List {
                field,
                child,
                offsets,
                nulls,
                ..
            } => Arc::new(ListArray::try_new(
                Arc::clone(field),
                OffsetBuffer::new(mem::replace(offsets, vec![0]).into()),
                child.finish()?,
                nulls.finish(),
            )?),
            EncoderState::Struct {
                fields,
                children,
                nulls,
                ..
            } => {
                let arrays = children
                    .iter_mut()
                    .map(|(_, child)| child.finish())
                    .collect::<ArrowConversionResult<Vec<_>>>()?;
                let len = nulls.len();
                Arc::new(StructArray::try_new_with_length(
                    fields.clone(),
                    arrays,
                    nulls.finish(),
                    len,
                )?)
            }
            EncoderState::Interval {
                fields,
                parts,
                nulls,
            } => {
                let arrays = parts
                    .iter_mut()
                    .map(|part| Arc::new(part.finish()) as ArrayRef)
                    .collect();
                let len = nulls.len();
                Arc::new(StructArray::try_new_with_length(
                    fields.clone(),
                    arrays,
                    nulls.finish(),
                    len,
                )?)
            }
        })
    }

    pub(crate) fn len(&self) -> usize {
        match &self.state {
            EncoderState::Scalar(encoder) => encoder.len(),
            EncoderState::TextIo(encoder) => encoder.len(),
            EncoderState::Oid(builder) => builder.len(),
            EncoderState::List { nulls, .. }
            | EncoderState::Struct { nulls, .. }
            | EncoderState::Interval { nulls, .. } => nulls.len(),
        }
    }
}
