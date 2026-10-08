//! Batch-bound readers for typed PostgreSQL containers and storage adapters.

use std::{cell::RefCell, rc::Rc};

use arrow_array::{
    Array, Int64Array, LargeStringArray, ListArray, StringArray, StructArray,
};
use arrow_buffer::NullBuffer;
use lagodb_core::tuple::{ArrayDatumBuffer, CompositeDatumCodec, PgTextIo};
use pgrx::pg_sys;

use super::reader::BoundDatumReader;
use super::{BoundDatumPlan, DatumReadState, NodePlan};
use crate::column_encoder::downcast;
use crate::jsonb::BoundNestedJsonb;
use crate::read::is_null_unchecked;
use crate::read::list::ListReader;
use crate::{ArrowConversionResult, CellRule};

impl BoundDatumPlan {
    /// Materialize direct scalar/native readers without an extra recursive
    /// dispatch. Container state remains owned by the scan across batches.
    ///
    /// # Safety
    /// `self.accepts_arrow_type(array.data_type())` must hold for the entire tree, either
    /// checked at the decoder boundary or guaranteed by its producer.
    pub(crate) unsafe fn bind_reader_unchecked(
        &self,
        array: &dyn Array,
        state: &DatumReadState,
    ) -> ArrowConversionResult<BoundDatumReader> {
        match (&self.node, state) {
            (NodePlan::Scalar { rule, codec }, _) => {
                // SAFETY: the enclosing plan guarantees this scalar's type.
                unsafe { BoundDatumReader::bind_unchecked(rule, array, *codec) }
                    .map(|reader| reader.with_numeric_typmod(self.base_type.typmod))
            }
            (
                NodePlan::TextIo {
                    read_codec: Some(codec),
                },
                _,
            ) => {
                // SAFETY: TextIo binds Utf8/LargeUtf8 storage. The semantic
                // codec checks the complete external text before PG input.
                unsafe {
                    BoundDatumReader::bind_unchecked(&CellRule::Utf8, array, *codec)
                }
            }
            (
                NodePlan::NativeList {
                    rule: CellRule::List { element, .. },
                    codec,
                    ..
                },
                DatumReadState::NativeList(buffer),
            ) => Ok(BoundDatumReader::List(Box::new(
                ListReader::bind_with_buffer(
                    array,
                    *element,
                    *codec,
                    Rc::clone(buffer),
                )?,
            ))),
            (NodePlan::NestedJsonb(plan), _) => Ok(BoundDatumReader::NestedJsonb(
                // SAFETY: the enclosing plan includes the complete JSONB tree.
                Box::new(unsafe { BoundNestedJsonb::bind_unchecked(plan, array) }?),
            )),
            _ => Ok(BoundDatumReader::Recursive(Box::new(
                // SAFETY: inherited from this binding entry point.
                unsafe { RecursiveDatumReader::bind_unchecked(self, array, state) }?,
            ))),
        }
    }
}

/// A typed conversion plan bound to one Arrow batch, with reusable datum buffers.
pub(crate) struct RecursiveDatumReader(ReaderState);

enum ReaderState {
    Scalar(Box<BoundDatumReader>),
    TextIo {
        array: TextArray,
        io: Rc<PgTextIo>,
    },
    Oid(Int64Array),
    List {
        array: ListArray,
        child: Box<RecursiveDatumReader>,
        buffer: Rc<RefCell<ArrayDatumBuffer>>,
    },
    Struct {
        array: StructArray,
        children: Box<[(usize, RecursiveDatumReader)]>,
        codec: Rc<RefCell<CompositeDatumCodec>>,
    },
    Interval {
        nulls: Option<NullBuffer>,
        parts: [Int64Array; 3],
    },
}

enum TextArray {
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
}

impl TextArray {
    fn bind(array: &dyn Array) -> ArrowConversionResult<Self> {
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            Ok(Self::Utf8(array.clone()))
        } else {
            Ok(Self::LargeUtf8(
                downcast::<LargeStringArray>(array, "LargeUtf8")?.clone(),
            ))
        }
    }

    unsafe fn value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(array) => (!unsafe { is_null_unchecked(array, row) })
                .then(|| unsafe { array.value_unchecked(row) }),
            Self::LargeUtf8(array) => (!unsafe { is_null_unchecked(array, row) })
                .then(|| unsafe { array.value_unchecked(row) }),
        }
    }
}

impl RecursiveDatumReader {
    /// # Safety
    /// `plan.accepts_arrow_type(array.data_type())` must hold for the entire subtree.
    unsafe fn bind_unchecked(
        plan: &BoundDatumPlan,
        array: &dyn Array,
        state: &DatumReadState,
    ) -> ArrowConversionResult<Self> {
        Ok(Self(match (&plan.node, state) {
            (NodePlan::Scalar { rule, codec }, DatumReadState::Scalar) => {
                // SAFETY: the subtree contract includes this scalar's type.
                ReaderState::Scalar(Box::new(
                    unsafe { BoundDatumReader::bind_unchecked(rule, array, *codec) }?
                        .with_numeric_typmod(plan.base_type.typmod),
                ))
            }
            (NodePlan::NativeList { .. }, DatumReadState::NativeList(_))
            | (NodePlan::NestedJsonb(_), DatumReadState::Scalar)
            | (
                NodePlan::TextIo {
                    read_codec: Some(_),
                },
                DatumReadState::Scalar,
            ) => {
                // SAFETY: inherited from this subtree's binding contract.
                ReaderState::Scalar(Box::new(unsafe {
                    plan.bind_reader_unchecked(array, state)
                }?))
            }
            (NodePlan::TextIo { read_codec: None }, DatumReadState::TextIo(io)) => {
                ReaderState::TextIo {
                    array: TextArray::bind(array)?,
                    io: Rc::clone(io),
                }
            }
            (NodePlan::Oid, DatumReadState::Oid) => {
                ReaderState::Oid(downcast::<Int64Array>(array, "Int64")?.clone())
            }
            (
                NodePlan::List { element, .. },
                DatumReadState::List { buffer, child },
            ) => {
                let array = downcast::<ListArray>(array, "List")?;
                ReaderState::List {
                    // SAFETY: the list contract includes its element type.
                    child: Box::new(unsafe {
                        Self::bind_unchecked(element, array.values().as_ref(), child)
                    }?),
                    array: array.clone(),
                    buffer: Rc::clone(buffer),
                }
            }
            (
                NodePlan::Struct(fields),
                DatumReadState::Struct { codec, children },
            ) => {
                let array = downcast::<StructArray>(array, "Struct")?;
                ReaderState::Struct {
                    children: fields
                        .iter()
                        .zip(array.columns())
                        .zip(children)
                        .map(|(((index, plan), array), state)| {
                            // SAFETY: the struct contract includes field order
                            // and each child's complete type.
                            Ok((*index, unsafe {
                                Self::bind_unchecked(plan, array.as_ref(), state)
                            }?))
                        })
                        .collect::<ArrowConversionResult<_>>()?,
                    array: array.clone(),
                    codec: Rc::clone(codec),
                }
            }
            (NodePlan::Interval, DatumReadState::Interval) => {
                let array = downcast::<StructArray>(array, "interval struct")?;
                let parts = [0, 1, 2].map(|i| {
                    downcast::<Int64Array>(array.column(i).as_ref(), "Int64").cloned()
                });
                let [months, days, micros] = parts;
                let parts = [months?, days?, micros?];
                // The plan and batch schema require non-nullable components.
                // StructArray permits their nulls only where the parent is NULL;
                // the encoder's append_null() deliberately uses that masking.
                // Trust this Arrow invariant: do not rescan child validity or
                // merge child bitmaps to turn an incomplete value into SQL NULL.
                // Interval nullness belongs exclusively to the parent struct.
                let nulls = array.nulls().cloned();
                ReaderState::Interval { nulls, parts }
            }
            _ => unreachable!("reader state is constructed from this type plan"),
        }))
    }

    /// # Safety
    /// `row` is within the bound batch and its destination row context is active.
    /// TextIo leaves satisfy the producer contract of `DecodedColumn::bind`.
    /// Widened OID and interval components retain their PG value domains.
    /// PG errors unwind to the enclosing provider/FFI boundary.
    pub(crate) unsafe fn read_datum(
        &self,
        row: usize,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        let datum = match &self.0 {
            ReaderState::Scalar(reader) => {
                return unsafe { reader.read_datum_unchecked(row) };
            }
            ReaderState::TextIo { array, io } => {
                let Some(text) = (unsafe { array.value(row) }) else {
                    return Ok(None);
                };
                // SAFETY: the declared decoder binds PG type-output text.
                // PgTextIo::with_output uses CStr::to_bytes(), so the stored
                // bytes contain no NUL. TextIoEncoder preserves those bytes,
                // including for array/composite leaves. Do not rescan each
                // datum for this producer invariant: it adds an O(text.len())
                // pass before input copies the text into its C-string buffer.
                // This is not a guarantee about arbitrary Arrow strings;
                // callers establish it through DecodedColumn::bind.
                unsafe { io.input(text) }
            }
            ReaderState::Oid(array) => {
                if unsafe { is_null_unchecked(array, row) } {
                    return Ok(None);
                }
                let value = unsafe { array.value_unchecked(row) };
                // The provider admits this representation only for PG OIDs
                // widened by the matching writer, not arbitrary external longs.
                pg_sys::Datum::from(value as u32)
            }
            ReaderState::List {
                array,
                child,
                buffer,
            } => {
                if unsafe { is_null_unchecked(array, row) } {
                    return Ok(None);
                }
                let offsets = array.value_offsets();
                let start = unsafe { *offsets.get_unchecked(row) } as usize;
                let end = unsafe { *offsets.get_unchecked(row + 1) } as usize;
                let mut buffer = buffer.borrow_mut();
                buffer.reset(end - start);
                for index in start..end {
                    buffer.push(unsafe { child.read_datum(index)? });
                }
                unsafe { buffer.form() }
            }
            ReaderState::Struct {
                array,
                children,
                codec,
            } => {
                if unsafe { is_null_unchecked(array, row) } {
                    return Ok(None);
                }
                let mut codec = codec.borrow_mut();
                for (index, child) in children {
                    codec.set(*index, unsafe { child.read_datum(row)? });
                }
                unsafe { codec.form() }
            }
            ReaderState::Interval { nulls, parts } => {
                if nulls.as_ref().is_some_and(|nulls| unsafe {
                    !nulls.inner().value_unchecked(row)
                }) {
                    return Ok(None);
                }
                let [month, day, time] = parts
                    .each_ref()
                    .map(|part| unsafe { part.value_unchecked(row) });
                let interval = pg_sys::Interval {
                    // PG interval components were widened by the writer.
                    month: month as i32,
                    day: day as i32,
                    time,
                };
                let pointer =
                    unsafe { pg_sys::palloc(size_of::<pg_sys::Interval>()) }
                        .cast::<pg_sys::Interval>();
                unsafe { pointer.write(interval) };
                pg_sys::Datum::from(pointer)
            }
        };
        Ok(Some(datum))
    }
}
