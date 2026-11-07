//! Batch-bound readers for typed PostgreSQL containers and storage adapters.

use std::{cell::RefCell, rc::Rc};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, Int64Array, LargeStringArray, ListArray, StringArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use lagodb_core::datum::{ArrayDatumBuffer, CompositeDatumCodec, PgTextIo};
use pgrx::pg_sys;

use super::reader::{BoundDatumReader, is_null_unchecked};
use super::{BoundDatumPlan, DatumReadState, NodePlan};
use crate::ArrowConversionResult;
use crate::jsonb::BoundNestedJsonb;

impl BoundDatumPlan {
    /// Materialize direct scalar/native readers without an extra recursive
    /// dispatch. Conversion state remains owned by the scan across batches.
    ///
    /// # Safety
    /// `self.accepts_arrow_type(array.data_type())` must hold for the entire
    /// tree, either checked at the decoder boundary or guaranteed by its
    /// producer, and `state` must have been built from this plan.
    pub(crate) unsafe fn bind_reader_unchecked(
        &self,
        array: &dyn Array,
        state: &DatumReadState,
    ) -> BoundDatumReader {
        match (&self.node, state) {
            // SAFETY: the enclosing plan guarantees this scalar's type.
            (_, DatumReadState::Scalar(read)) => unsafe { read.bind_array(array) },
            (NodePlan::NestedJsonb(plan), DatumReadState::NestedJsonb) => {
                BoundDatumReader::NestedJsonb(Box::new(
                    // SAFETY: the enclosing plan includes the complete JSONB tree.
                    unsafe { BoundNestedJsonb::bind_unchecked(plan, array) },
                ))
            }
            _ => BoundDatumReader::Recursive(Box::new(
                // SAFETY: inherited from this binding entry point.
                unsafe { RecursiveDatumReader::bind_unchecked(self, array, state) },
            )),
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
    /// # Safety
    /// `array` must be `Utf8` or `LargeUtf8`.
    unsafe fn bind(array: &dyn Array) -> Self {
        match array.data_type() {
            DataType::Utf8 => Self::Utf8(array.as_string::<i32>().clone()),
            _ => Self::LargeUtf8(array.as_string::<i64>().clone()),
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
    /// `plan.accepts_arrow_type(array.data_type())` must hold for the entire
    /// subtree, and `state` must have been built from `plan`.
    unsafe fn bind_unchecked(
        plan: &BoundDatumPlan,
        array: &dyn Array,
        state: &DatumReadState,
    ) -> Self {
        Self(match (&plan.node, state) {
            (_, DatumReadState::Scalar(_))
            | (NodePlan::NestedJsonb(_), DatumReadState::NestedJsonb) => {
                // SAFETY: inherited from this subtree's binding contract.
                ReaderState::Scalar(Box::new(unsafe {
                    plan.bind_reader_unchecked(array, state)
                }))
            }
            (NodePlan::TextIo { read_codec: None }, DatumReadState::TextIo(io)) => {
                ReaderState::TextIo {
                    // SAFETY: TextIo plans accept only Utf8/LargeUtf8.
                    array: unsafe { TextArray::bind(array) },
                    io: Rc::clone(io),
                }
            }
            (NodePlan::Oid, DatumReadState::Oid) => {
                ReaderState::Oid(array.as_primitive::<Int64Type>().clone())
            }
            (
                NodePlan::List { element, .. },
                DatumReadState::List { buffer, child },
            ) => {
                let array = array.as_list::<i32>();
                ReaderState::List {
                    // SAFETY: the list contract includes its element type.
                    child: Box::new(unsafe {
                        Self::bind_unchecked(element, array.values().as_ref(), child)
                    }),
                    array: array.clone(),
                    buffer: Rc::clone(buffer),
                }
            }
            (
                NodePlan::Struct(fields),
                DatumReadState::Struct { codec, children },
            ) => {
                let array = array.as_struct();
                ReaderState::Struct {
                    children: fields
                        .iter()
                        .zip(array.columns())
                        .zip(children)
                        .map(|(((index, plan), array), state)| {
                            // SAFETY: the struct contract includes field order
                            // and each child's complete type.
                            (*index, unsafe {
                                Self::bind_unchecked(plan, array.as_ref(), state)
                            })
                        })
                        .collect(),
                    array: array.clone(),
                    codec: Rc::clone(codec),
                }
            }
            (NodePlan::Interval, DatumReadState::Interval) => {
                let array = array.as_struct();
                let parts = [0, 1, 2]
                    .map(|i| array.column(i).as_primitive::<Int64Type>().clone());
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
        })
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
