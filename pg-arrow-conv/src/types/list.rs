//! Single-level list conversion: read (`Arrow → Cell` and `Arrow → array
//! datum`) and write (datum / `Cell` → Arrow `ListArray`).
//!
//! The element kind is one concept expressed in three roles that are
//! deliberately kept distinct (mirroring the crate's read/write split): the
//! resolved [`ListElementRule`] (dispatch key, in `rule`), [`ListValues`] (read
//! views), and [`ListInner`] (write builders).

use std::ffi::CStr;
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
use pg_lakebase_core::tuple::{Cell, PgDatumRef, StringView};
// Aliased to avoid colliding with the `arrow_array::Array` trait (used as
// `&dyn Array`); `PgArray` is the borrowed PostgreSQL array view.
use pgrx::{Array as PgArray, FromDatum, pg_sys};

use super::{ColumnAppend, downcast};
use crate::error::{ConvError, ConvResult};
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
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<usize> {
        let oid = datum.type_oid();
        let raw = datum.datum();
        let mut payload = 0usize;

        // Read a primitive PG array as a borrowed view (no intermediate
        // `Vec<Option<T>>`), append each element (optionally widened), and close
        // the list slot. `$width` is the appended element's Arrow width.
        macro_rules! prim {
            ($b:expr, $ty:ty, $width:expr, $map:expr) => {
                match PgArray::<$ty>::from_datum(raw, false) {
                    Some(arr) => {
                        payload = arr.len() * $width;
                        for v in arr.iter() {
                            $b.values().append_option(v.map($map));
                        }
                        $b.append(true);
                        true
                    }
                    None => false,
                }
            };
        }

        // String analogue of `prim!`. `$elem` is the borrowed element type pgrx
        // unboxes per the physical layout (`&str` for varlena
        // text/varchar/bpchar/json, `&CStr` for `name`'s C strings), and
        // `$to_str` maps one element to `&str`, validating UTF-8 where the
        // source can carry non-UTF-8 bytes (surfaces as `InvalidUtf8`).
        macro_rules! text {
            ($b:expr, $elem:ty, $to_str:expr) => {
                match PgArray::<$elem>::from_datum(raw, false) {
                    Some(arr) => {
                        for v in arr.iter() {
                            match v.map($to_str).transpose()? {
                                Some(s) => {
                                    payload += s.len();
                                    $b.values().append_value(s);
                                }
                                None => $b.values().append_null(),
                            }
                        }
                        $b.append(true);
                        true
                    }
                    None => false,
                }
            };
        }

        let matched = unsafe {
            match &mut self.inner {
                ListInner::Bool(b) if oid == pg_sys::BOOLARRAYOID => {
                    prim!(b, bool, std::mem::size_of::<bool>(), |x| x)
                }
                ListInner::Int(b) if oid == pg_sys::INT4ARRAYOID => {
                    prim!(b, i32, std::mem::size_of::<i32>(), |x| x)
                }
                // Widen an Int16 source to Int32, as the row-build path does.
                ListInner::Int(b) if oid == pg_sys::INT2ARRAYOID => {
                    prim!(b, i16, std::mem::size_of::<i32>(), |x| x as i32)
                }
                ListInner::Long(b) if oid == pg_sys::INT8ARRAYOID => {
                    prim!(b, i64, std::mem::size_of::<i64>(), |x| x)
                }
                ListInner::Float(b) if oid == pg_sys::FLOAT4ARRAYOID => {
                    prim!(b, f32, std::mem::size_of::<f32>(), |x| x)
                }
                ListInner::Double(b) if oid == pg_sys::FLOAT8ARRAYOID => {
                    prim!(b, f64, std::mem::size_of::<f64>(), |x| x)
                }
                // `name[]` elements are fixed `NameData` (NUL-terminated C
                // strings), not text varlenas, so they're read as `&CStr` —
                // mirroring the scalar `name` special-case in `string.rs`. pgrx
                // hops the array correctly (it picks the `CStr` layout from the
                // element OID), but `<&str>::unbox` would misread each element
                // as a varlena, so the Rust element type must match the layout.
                ListInner::String(b) if oid == pg_sys::NAMEARRAYOID => {
                    text!(b, &CStr, |c: &CStr| c.to_str().map_err(ConvError::from))
                }
                // Varlena text family (text/varchar/bpchar/json): each element
                // reads as a borrowed `&str` straight into the builder.
                ListInner::String(b) if is_string_array(oid) => {
                    text!(b, &str, Ok::<&str, ConvError>)
                }
                _ => false,
            }
        };

        if matched {
            Ok(payload)
        } else {
            Err(ConvError::InvariantViolated(
                "List encoder: datum array type does not match the list element rule",
            ))
        }
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        let mismatch = |element: &str| {
            ConvError::IncompatibleColumnType(
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
                // Widen an Int16 source to Int32, as the datum path does.
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

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        let array: ArrayRef =
            dispatch_list_inner!(&mut self.inner, b => Arc::new(b.finish()));
        Ok(array)
    }

    fn len(&self) -> usize {
        dispatch_list_inner!(&self.inner, b => b.len())
    }
}

/// Text-family array OIDs the list encoder accepts as a `String` element.
///
/// `name[]` (`NAMEARRAYOID`) is in this set but is *not* a varlena: its
/// elements are C strings, so `append_datum` reads it via `PgArray<&CStr>`
/// while the rest of the family is read via `PgArray<&str>`. This split mirrors
/// the scalar `name` vs text/varchar/bpchar/json special-case in `string.rs`;
/// the two must stay in lockstep.
fn is_string_array(oid: pg_sys::Oid) -> bool {
    oid == pg_sys::TEXTARRAYOID
        || oid == pg_sys::VARCHARARRAYOID
        || oid == pg_sys::BPCHARARRAYOID
        || oid == pg_sys::NAMEARRAYOID
        || oid == pg_sys::JSONARRAYOID
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
    /// `LargeUtf8`. A mismatch is a clean [`ConvError::ArrowTypeMismatch`].
    ///
    /// The accepted physical types here are the source of truth for
    /// [`ListElementRule::accepts_data_type`], which the batch-boundary
    /// validation calls; the two must stay in lockstep.
    fn read_values<'a>(&self, values: &'a dyn Array) -> ConvResult<ListValues<'a>> {
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
                    return Err(ConvError::ArrowTypeMismatch(
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
                    return Err(ConvError::ArrowTypeMismatch(
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
    // TODO(row-world-list-narrowing): this preserves the Arrow *physical*
    // element width (e.g. `Int16 -> Cell::I16Array`), but the array `Cell`
    // variants have no target-aware retarget in `Cell::into_datum_typed`
    // (cell.rs) — they fall through to `Vec<Option<T>>::into_datum()`, which
    // stamps the array datum with the physical element type. So a list column
    // widened on write (e.g. `int2[]` stored as Arrow `Int32`) reads back via
    // the row path as the wrong PG element type (`int4[]` instead of `int2[]`).
    // The slot-first path already side-steps this through `into_array_datum`,
    // which retargets each element by the declared `elem_oid`. Row-world has no
    // consumers yet, so this is documented rather than fixed; it must be closed
    // before the FDW/row-mode work in `columnar-datapath-refactor.md` §10
    // relies on `Row`/`Cell` as a first-class interface. The fix is an
    // array-aware arm in `into_datum_typed` (retarget per element OID), not a
    // change here.
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
    /// Each element is materialized through the *same* target-aware
    /// [`Cell::into_datum_typed`] the scalar read path uses, keyed on
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
        elem_oid: pg_sys::Oid,
    ) -> ConvResult<pg_sys::Datum> {
        unsafe {
            match self {
                ListValues::Bool(a) => {
                    accum_array_datum(elem_oid, a.iter().map(|v| v.map(Cell::Bool)))
                }
                ListValues::I16(a) => {
                    accum_array_datum(elem_oid, a.iter().map(|v| v.map(Cell::I16)))
                }
                ListValues::I32(a) => {
                    accum_array_datum(elem_oid, a.iter().map(|v| v.map(Cell::I32)))
                }
                ListValues::I64(a) => {
                    accum_array_datum(elem_oid, a.iter().map(|v| v.map(Cell::I64)))
                }
                ListValues::F32(a) => {
                    accum_array_datum(elem_oid, a.iter().map(|v| v.map(Cell::F32)))
                }
                ListValues::F64(a) => {
                    accum_array_datum(elem_oid, a.iter().map(|v| v.map(Cell::F64)))
                }
                ListValues::Utf8(a) => {
                    accum_array_datum(elem_oid, a.iter().map(|v| v.map(str_view)))
                }
                ListValues::LargeUtf8(a) => {
                    accum_array_datum(elem_oid, a.iter().map(|v| v.map(str_view)))
                }
            }
        }
    }
}

/// Borrow an Arrow `&str` element as a zero-copy [`StringView`] `Cell` so the
/// shared [`Cell::into_datum_typed`] can produce the right text-family varlena
/// (or `name`/`json`) for the target element OID.
fn str_view(s: &str) -> Cell {
    Cell::StringView(StringView {
        ptr: s.as_ptr(),
        len: s.len(),
    })
}

/// Accumulate a one-dimensional PostgreSQL array datum from an iterator of
/// optional element [`Cell`]s, each materialized into the declared `elem_oid`
/// through the target-aware [`Cell::into_datum_typed`] (so the array's element
/// type and per-element representation match the column, with narrowing where
/// required). Fed from the Arrow array's own iterator, so no intermediate
/// `Vec` is materialized.
///
/// Returns [`ConvError::DatumConversionError`] when an element cannot be
/// represented as `elem_oid` (e.g. an out-of-range value narrowing to `int2`).
///
/// # Safety
///
/// A backend must be active and the caller must have switched to the memory
/// context the array should be palloc'd into.
unsafe fn accum_array_datum(
    elem_oid: pg_sys::Oid,
    elements: impl Iterator<Item = Option<Cell>>,
) -> ConvResult<pg_sys::Datum> {
    let ctx = unsafe { pg_sys::CurrentMemoryContext };
    let mut state = unsafe { pg_sys::initArrayResult(elem_oid, ctx, false) };
    for element in elements {
        let (datum, isnull) = match element {
            Some(cell) => {
                let datum = unsafe { cell.into_datum_typed(elem_oid, -1) }
                    .ok_or_else(|| {
                        ConvError::DatumConversionError(format!(
                            "list element is not representable as PostgreSQL \
                             element type {}",
                            u32::from(elem_oid)
                        ))
                    })?;
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
pub(crate) fn cell_at(
    list: &ListArray,
    row_idx: usize,
    element: ListElementRule,
) -> ConvResult<Cell> {
    let values = list.value(row_idx);
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
/// A backend must be active and the caller must have switched to the target
/// memory context the array payload should be palloc'd into.
pub(crate) unsafe fn array_datum_at(
    list: &ListArray,
    row_idx: usize,
    element: ListElementRule,
    elem_oid: pg_sys::Oid,
) -> ConvResult<pg_sys::Datum> {
    let values = list.value(row_idx);
    unsafe {
        element
            .read_values(values.as_ref())?
            .into_array_datum(elem_oid)
    }
}
