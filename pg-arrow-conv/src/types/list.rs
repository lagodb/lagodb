//! Single-level list conversion: read (`Arrow → Cell` and `Arrow → array
//! datum`) and write (bound datum / `Cell` → Arrow `ListArray`).
//!
//! The element kind is one concept expressed in three roles that are
//! deliberately kept distinct (mirroring the crate's read/write split): the
//! resolved [`ListElementRule`] (dispatch key, in `rule`), [`ListValues`] (read
//! views), and [`ListInner`] (write builders).

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, StringBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, ListArray, StringArray,
};
use arrow_schema::{DataType, FieldRef};
use pg_lakebase_core::tuple::{Cell, ColumnDatumCodec, StringView};
// Aliased to avoid colliding with the `arrow_array::Array` trait (used as
// `&dyn Array`); `PgArray` is the borrowed PostgreSQL array view.
use pgrx::pg_sys;

use super::{ColumnAppend, downcast};
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::rule::ListElementRule;

/// Expand `$body` once per [`ListInner`] element-builder variant, binding the
/// active builder to `$b`. Keeps the six element kinds in one place for the
/// uniform list operations (`finish`, `len`, null append).
macro_rules! dispatch_list_inner {
    ($self:expr, $b:ident => $body:expr) => {
        match $self {
            ListInner::Bool($b) => $body,
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

// ---------------------------------------------------------------------------
// Read (Arrow → Cell / array datum)
// ---------------------------------------------------------------------------

/// One list cell's element `values` array, downcast to its concrete Arrow type
/// per the resolved [`ListElementRule`]. This is the single read-path site that
/// inspects the physical Arrow element type, consumed by both the row-world
/// `Cell` reader ([`ListValues::into_cell`]) and the slot-first datum reader
/// ([`ListValues::into_array_datum`]).
///
/// This view captures only the *physical* Arrow element width (an `Int16`
/// source stays `i16`, an `Int32` source stays `i32`); the *target* PG element
/// type is applied later — [`ListValues::into_cell`] keeps the physical width
/// (row-world `Cell`), while [`ListValues::into_array_datum`] retargets each
/// element to the column's declared element OID (narrowing where needed). The
/// write/build path widens `Int16` to `Int32`. The accepted physical element
/// types here are the source of truth that the batch-boundary
/// [`ColumnRule::accepts`] defers to (it validates list-ness and lets this
/// decode reject a wrong element type).
///
/// [`ColumnRule::accepts`]: crate::rule::ColumnRule::accepts
enum ListValues<'a> {
    Bool(&'a BooleanArray),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

impl ListElementRule {
    /// Downcast a list cell's element `values` array to its concrete Arrow type.
    /// The `Int` rule accepts an `Int32` or `Int16` physical source (the read
    /// path keeps the narrower width); the `String` rule accepts `Utf8` or
    /// `LargeUtf8`. A mismatch is a clean [`ArrowConversionError::ArrowTypeMismatch`].
    ///
    /// The accepted physical types here are the source of truth for
    /// [`ListElementRule::accepts_data_type`], which the batch-boundary
    /// validation calls; the two must stay in lockstep.
    fn read_values<'a>(
        &self,
        values: &'a dyn Array,
    ) -> ArrowConversionResult<ListValues<'a>> {
        let v = match self {
            ListElementRule::Bool => {
                ListValues::Bool(downcast::<BooleanArray>(values, "BooleanArray")?)
            }
            ListElementRule::Int => match values.data_type() {
                DataType::Int32 => {
                    ListValues::I32(values.as_primitive::<Int32Type>())
                }
                DataType::Int16 => {
                    ListValues::I16(values.as_primitive::<Int16Type>())
                }
                other => {
                    return Err(ArrowConversionError::ArrowTypeMismatch(
                        format!("Int32 or Int16 list element (actual: {other:?})")
                            .into(),
                    ));
                }
            },
            ListElementRule::Long => {
                ListValues::I64(downcast::<Int64Array>(values, "Int64")?)
            }
            ListElementRule::Float => {
                ListValues::F32(downcast::<Float32Array>(values, "Float32")?)
            }
            ListElementRule::Double => {
                ListValues::F64(downcast::<Float64Array>(values, "Float64")?)
            }
            ListElementRule::String => match values.data_type() {
                DataType::Utf8 => ListValues::Utf8(values.as_string::<i32>()),
                DataType::LargeUtf8 => {
                    ListValues::LargeUtf8(values.as_string::<i64>())
                }
                other => {
                    return Err(ArrowConversionError::ArrowTypeMismatch(
                        format!("Utf8 or LargeUtf8 list element (actual: {other:?})")
                            .into(),
                    ));
                }
            },
        };
        Ok(v)
    }
}

impl ListValues<'_> {
    /// Collect into the matching array-valued [`Cell`] — the row-world / FDW
    /// read API. Allocates one `Vec` per cell (the cost of an owned `Cell`).
    ///
    // This preserves the Arrow *physical* element width (e.g.
    // `Int16 -> Cell::I16Array`). Destination-aware materialization is deferred
    // to the bound column datum codec, where the slot column supplies the
    // declared element OID; the row value itself remains a source representation.
    fn into_cell(self) -> Cell {
        match self {
            ListValues::Bool(a) => Cell::BoolArray(a.iter().collect()),
            ListValues::I16(a) => Cell::I16Array(a.iter().collect()),
            ListValues::I32(a) => Cell::I32Array(a.iter().collect()),
            ListValues::I64(a) => Cell::I64Array(a.iter().collect()),
            ListValues::F32(a) => Cell::F32Array(a.iter().collect()),
            ListValues::F64(a) => Cell::F64Array(a.iter().collect()),
            ListValues::Utf8(a) => Cell::StringArray(
                a.iter().map(|v| v.map(ToOwned::to_owned)).collect(),
            ),
            ListValues::LargeUtf8(a) => Cell::StringArray(
                a.iter().map(|v| v.map(ToOwned::to_owned)).collect(),
            ),
        }
    }

    /// Build the PostgreSQL array datum straight from the Arrow element array,
    /// targeting the declared element type `elem_oid` — no intermediate
    /// `Vec<Option<T>>` and no array `Cell`.
    ///
    /// Each element is materialized through the same target-aware Cell
    /// conversion path the scalar read path uses, keyed on
    /// `elem_oid`: an `Int32` source targeting `int2` is narrowed, the text
    /// family selects its precise varlena/`name`/`json` form, etc. This is why
    /// the element OID — not the Arrow physical type — decides the produced
    /// array's element type, fixing the mismatch where e.g. an `int2[]` column
    /// (widened to `Int32` on write) read back as `int4[]`.
    ///
    /// # Safety
    ///
    /// A backend must be active and the caller must have switched to the target
    /// memory context.
    unsafe fn into_array_datum(
        self,
        elem_codec: ColumnDatumCodec,
    ) -> ArrowConversionResult<pg_sys::Datum> {
        unsafe {
            match self {
                ListValues::Bool(a) => {
                    accum_array_datum(elem_codec, a.iter().map(|v| v.map(Cell::Bool)))
                }
                ListValues::I16(a) => {
                    accum_array_datum(elem_codec, a.iter().map(|v| v.map(Cell::I16)))
                }
                ListValues::I32(a) => {
                    accum_array_datum(elem_codec, a.iter().map(|v| v.map(Cell::I32)))
                }
                ListValues::I64(a) => {
                    accum_array_datum(elem_codec, a.iter().map(|v| v.map(Cell::I64)))
                }
                ListValues::F32(a) => {
                    accum_array_datum(elem_codec, a.iter().map(|v| v.map(Cell::F32)))
                }
                ListValues::F64(a) => {
                    accum_array_datum(elem_codec, a.iter().map(|v| v.map(Cell::F64)))
                }
                ListValues::Utf8(a) => {
                    accum_array_datum(elem_codec, a.iter().map(|v| v.map(str_view)))
                }
                ListValues::LargeUtf8(a) => {
                    accum_array_datum(elem_codec, a.iter().map(|v| v.map(str_view)))
                }
            }
        }
    }
}

/// Borrow an Arrow `&str` element as a zero-copy [`StringView`] `Cell` so the
/// shared Cell target conversion can produce the right text-family varlena
/// (or `name`/`json`) for the target element OID.
fn str_view(s: &str) -> Cell {
    // SAFETY: the list cell is consumed while the Arrow array owns s.
    Cell::StringView(unsafe { StringView::from_raw_parts(s.as_ptr(), s.len()) })
}

/// Accumulate a one-dimensional PostgreSQL array datum from an iterator of
/// optional element [`Cell`]s, each materialized into the declared `elem_oid`
/// through the target-aware Cell conversion (so the array's element
/// type and per-element representation match the column, with narrowing where
/// required). Fed from the Arrow array's own iterator, so no intermediate
/// `Vec` is materialized.
///
/// Returns [`ArrowConversionError::DatumConversion`] when an element cannot be
/// represented as `elem_oid` (e.g. an out-of-range value narrowing to `int2`).
///
/// # Safety
///
/// A backend must be active and the caller must have switched to the memory
/// context the array should be palloc'd into.
unsafe fn accum_array_datum(
    elem_codec: ColumnDatumCodec,
    elements: impl Iterator<Item = Option<Cell>>,
) -> ArrowConversionResult<pg_sys::Datum> {
    let ctx = unsafe { pg_sys::CurrentMemoryContext };
    let elem_oid = elem_codec.oid();
    let mut state = unsafe { pg_sys::initArrayResult(elem_oid, ctx, false) };
    for element in elements {
        let (datum, isnull) = match element {
            Some(cell) => {
                let datum = unsafe { elem_codec.cell_to_datum(cell) }
                    .map_err(ArrowConversionError::from)?;
                (datum, false)
            }
            None => (pg_sys::Datum::from(0usize), true),
        };
        state =
            unsafe { pg_sys::accumArrayResult(state, datum, isnull, elem_oid, ctx) };
    }
    Ok(unsafe { pg_sys::makeArrayResult(state, ctx) })
}

/// Read list cell `row_idx` into an array-valued [`Cell`] (row-world API). The
/// `ListArray` was already downcast when its batch was bound (see
/// `ColumnReader`), so this takes the typed array directly.
///
/// Keeps the Arrow *physical* element width (see the narrowing TODO on
/// [`ListValues::into_cell`]); the slot-first path retargets per element OID via
/// [`array_datum_at`] instead.
///
/// # Safety
///
/// `row_idx` must be within `list`. Any returned borrowed string views must not
/// outlive `list`.
pub(crate) unsafe fn cell_at(
    list: &ListArray,
    row_idx: usize,
    element: ListElementRule,
) -> ArrowConversionResult<Cell> {
    let values = unsafe { list.value_unchecked(row_idx) };
    Ok(element.read_values(values.as_ref())?.into_cell())
}

/// Read list cell `row_idx` straight into a PostgreSQL array datum (slot-first
/// read path), bypassing the owned `Cell`. The `ListArray` was already downcast
/// when its batch was bound (see `ArrowColumnDecoder`), so this takes the typed
/// array directly and does no per-cell list downcast. `elem_oid` is the array
/// column's declared element type OID, which the produced datum targets (see
/// [`ListValues::into_array_datum`]).
///
/// # Safety
///
/// `row_idx` must be within `list`. A backend must be active and the caller
/// must have switched to the target memory context the array payload should be
/// palloc'd into.
pub(crate) unsafe fn array_datum_at(
    list: &ListArray,
    row_idx: usize,
    element: ListElementRule,
    elem_codec: ColumnDatumCodec,
) -> ArrowConversionResult<pg_sys::Datum> {
    let values = unsafe { list.value_unchecked(row_idx) };
    unsafe {
        element
            .read_values(values.as_ref())?
            .into_array_datum(elem_codec)
    }
}
