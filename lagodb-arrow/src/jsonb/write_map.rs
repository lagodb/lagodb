//! Map input and batch-level typed key validation.

use std::mem;
use std::sync::Arc;

use arrow_array::{ArrayRef, MapArray, StructArray, UInt32Array};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_ord::ord::make_comparator;
use arrow_schema::{DataType, FieldRef, SortOptions};
use arrow_select::take::take;
use lagodb_core::tuple::JsonbRef;

use super::write::ValueWriter;
use super::{NestedJsonbDefault, NestedJsonbPlan, NodePlan};
use crate::{ArrowConversionError, ArrowConversionResult};

pub(super) struct MapWriter {
    entries: FieldRef,
    key: Box<ValueWriter>,
    value: Box<ValueWriter>,
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    object: bool,
    sorted: bool,
    canonical: bool,
}

impl MapWriter {
    pub(super) fn new(
        plan: &NestedJsonbPlan,
        capacity: usize,
        map_key: bool,
    ) -> ArrowConversionResult<Self> {
        let NodePlan::Map { key, value, object } = &plan.node else {
            unreachable!("map writer is selected by the compiled plan")
        };
        let DataType::Map(entries, sorted) = plan.field.data_type() else {
            unreachable!("map plans retain their Arrow field")
        };
        Ok(Self {
            entries: entries.clone(),
            key: Box::new(ValueWriter::new(key, capacity, true)?),
            value: Box::new(ValueWriter::new(value, capacity, map_key)?),
            offsets: vec![0],
            nulls: NullBufferBuilder::new(capacity),
            object: *object,
            sorted: *sorted,
            canonical: map_key || *sorted,
        })
    }

    pub(super) fn append(
        &mut self,
        input: JsonbRef<'_>,
    ) -> ArrowConversionResult<usize> {
        let mut bytes = 5;
        match (self.object, input) {
            (true, JsonbRef::Object(object)) => {
                let mut values = object.values();
                while let Some(key) = values.next() {
                    bytes += self.key.append(key)?;
                    bytes += self.value.append(
                        values.next().expect("JSONB object keys have values"),
                    )?;
                }
            }
            (false, JsonbRef::Array(array)) => {
                for entry in array.values() {
                    let JsonbRef::Object(entry) = entry else {
                        return Err(Self::shape());
                    };
                    let key = entry.get("key").ok_or_else(Self::shape)?;
                    let value = entry.get("value").ok_or_else(Self::shape)?;
                    if entry.len() != 2 {
                        return Err(Self::shape());
                    }
                    bytes += self.key.append(key)?;
                    bytes += self.value.append(value)?;
                }
            }
            _ => return Err(Self::shape()),
        }
        self.offsets.push(ValueWriter::offset(self.key.len())?);
        self.nulls.append_non_null();
        Ok(bytes)
    }

    pub(super) fn append_default(
        &mut self,
        values: &[(NestedJsonbDefault, Option<NestedJsonbDefault>)],
    ) -> ArrowConversionResult<usize> {
        let mut bytes = 5;
        for (key, value) in values {
            bytes += self.key.append_default(Some(key))?;
            bytes += self.value.append_default(value.as_ref())?;
        }
        self.offsets.push(ValueWriter::offset(self.key.len())?);
        self.nulls.append_non_null();
        Ok(bytes)
    }

    fn shape() -> ArrowConversionError {
        ArrowConversionError::InvalidInput("expected an object for a string-key map or an array of key/value objects for other maps".into())
    }

    pub(super) fn append_null(&mut self) {
        self.offsets
            .push(*self.offsets.last().expect("map has an initial offset"));
        self.nulls.append_null();
    }

    pub(super) fn len(&self) -> usize {
        self.nulls.len()
    }

    pub(super) fn clear(&mut self) {
        self.key.clear();
        self.value.clear();
        self.offsets.truncate(1);
        self.nulls.finish();
    }

    pub(super) fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        let mut key = self.key.finish()?;
        let mut value = self.value.finish()?;
        // JSONB already guarantees unique string object keys. Other keys must
        // be checked *after* conversion (numeric/UUID/date spellings can alias).
        // A map used inside a key is sorted as well, so equality does not depend
        // on entry order. Child map writers canonicalize recursively first.
        if !self.object || self.canonical {
            let compare =
                make_comparator(key.as_ref(), key.as_ref(), SortOptions::default())?;
            let mut indices = Vec::with_capacity(key.len());
            for range in self.offsets.windows(2) {
                let start = indices.len();
                indices.extend((range[0] as u32)..(range[1] as u32));
                let group = &mut indices[start..];
                group.sort_unstable_by(|a, b| compare(*a as usize, *b as usize));
                if !self.object
                    && group.windows(2).any(|pair| {
                        compare(pair[0] as usize, pair[1] as usize).is_eq()
                    })
                {
                    return Err(ArrowConversionError::InvalidInput("map contains duplicate keys after conversion to its target type".into()));
                }
            }
            if self.canonical {
                let indices = UInt32Array::from(indices);
                key = take(key.as_ref(), &indices, None)?;
                value = take(value.as_ref(), &indices, None)?;
            }
        }
        let DataType::Struct(fields) = self.entries.data_type() else {
            unreachable!("map entries are compiled as a struct")
        };
        let entries = StructArray::try_new(fields.clone(), vec![key, value], None)?;
        let offsets =
            OffsetBuffer::new(mem::replace(&mut self.offsets, vec![0]).into());
        Ok(Arc::new(MapArray::try_new(
            self.entries.clone(),
            offsets,
            entries,
            self.nulls.finish(),
            self.sorted,
        )?))
    }
}
