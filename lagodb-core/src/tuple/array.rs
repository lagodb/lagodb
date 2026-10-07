//! PostgreSQL array layout and iteration shared by recursive storage codecs.

use std::ffi::{c_char, c_int};
use std::mem::{MaybeUninit, size_of};

use pgrx::pg_sys::{self, ffi::pg_guard_ffi_boundary};

use super::DetoastedVarlena;

// LagoDB's flat-array cursor ABI, shared with csrc/tuple/lagodb_array.h.
// This is not a mirror of PostgreSQL's private array_iter layout.
#[repr(C)]
struct ArrayCursor {
    data: *mut c_char,
    bitmap: *mut u8,
    bitmask: c_int,
}

unsafe extern "C-unwind" {
    fn lagodb_array_cursor_setup(
        array: *mut pg_sys::ArrayType,
        cursor: *mut ArrayCursor,
    ) -> c_int;
    fn lagodb_array_cursor_next(
        cursor: *mut ArrayCursor,
        len: c_int,
        by_value: bool,
        align: c_char,
        is_null: *mut bool,
    ) -> pg_sys::Datum;
}

#[derive(Debug, Clone, Copy)]
pub struct ArrayDatumLayout {
    pub element_oid: pg_sys::Oid,
    len: i16,
    by_value: bool,
    align: c_char,
}

impl ArrayDatumLayout {
    pub fn bind(element_oid: pg_sys::Oid) -> Self {
        let (mut len, mut by_value, mut align) = (0, false, 0);
        unsafe {
            pg_sys::get_typlenbyvalalign(
                element_oid,
                &mut len,
                &mut by_value,
                &mut align,
            )
        };
        Self {
            element_oid,
            len,
            by_value,
            align,
        }
    }

    /// # Safety
    /// `datum` is a non-NULL array of this layout's element type and remains
    /// live for the iterator's lifetime on the PostgreSQL backend thread.
    pub unsafe fn values(self, datum: pg_sys::Datum) -> ArrayDatumIter {
        // SAFETY: the caller supplies a live array datum. The guard retains
        // any detoasted copy until the iterator and its borrowed elements end.
        let value = unsafe { DetoastedVarlena::from_datum(datum) };
        let array = value.as_datum().cast_mut_ptr::<pg_sys::ArrayType>();
        let mut cursor = MaybeUninit::uninit();
        // SAFETY: the guard owns or borrows a flat array. The bridge initializes
        // every cursor field; ArrayGetNItems errors unwind through pgrx while
        // the Rust-owned detoast guard stays outside the FFI closure.
        let remaining = unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_array_cursor_setup(array, cursor.as_mut_ptr())
            })
        };
        ArrayDatumIter {
            // SAFETY: successful setup initialized all cursor fields.
            cursor: unsafe { cursor.assume_init() },
            remaining,
            layout: self,
            value,
        }
    }

    /// # Safety
    /// Values have the bound element type, fit in an i32 length, and remain live
    /// until this returns. `nulls` has the same length as `values`.
    pub unsafe fn form(
        self,
        values: &mut [pg_sys::Datum],
        nulls: &mut [bool],
    ) -> pg_sys::Datum {
        let array = if values.is_empty() {
            unsafe { pg_sys::construct_empty_array(self.element_oid) }
        } else {
            let mut length = values.len() as i32;
            let mut lower = 1;
            unsafe {
                pg_sys::construct_md_array(
                    values.as_mut_ptr(),
                    nulls.as_mut_ptr(),
                    1,
                    &mut length,
                    &mut lower,
                    self.element_oid,
                    self.len.into(),
                    self.by_value,
                    self.align,
                )
            }
        };
        pg_sys::Datum::from(array)
    }
}

/// Reusable construction workspace for one declared PostgreSQL array type.
/// Elements live in the caller's row context; forming the result copies them
/// directly into the final array, without an ArrayBuildState copy in between.
pub struct ArrayDatumBuffer {
    layout: ArrayDatumLayout,
    values: Vec<pg_sys::Datum>,
    nulls: Vec<bool>,
}

impl ArrayDatumBuffer {
    pub fn new(layout: ArrayDatumLayout) -> Self {
        Self {
            layout,
            values: Vec::new(),
            nulls: Vec::new(),
        }
    }

    pub fn reset(&mut self, len: usize) {
        self.values.clear();
        self.nulls.clear();
        self.values.reserve(len);
        self.nulls.reserve(len);
    }

    pub fn push(&mut self, value: Option<pg_sys::Datum>) {
        self.nulls.push(value.is_none());
        self.values.push(value.unwrap_or(pg_sys::Datum::from(0)));
    }

    /// # Safety
    /// All pushed datums have the bound element type and remain live. Their
    /// count fits i32, and the current row context owns the returned array.
    pub unsafe fn form(&mut self) -> pg_sys::Datum {
        unsafe { self.layout.form(&mut self.values, &mut self.nulls) }
    }
}

/// Allocation-free cursor over a detoasted array. By-reference element datums
/// borrow the array and must be consumed before the iterator is dropped.
pub struct ArrayDatumIter {
    cursor: ArrayCursor,
    remaining: c_int,
    layout: ArrayDatumLayout,
    value: DetoastedVarlena,
}

impl ArrayDatumIter {
    /// A flat Arrow list cannot retain PG dimensions or non-one lower bounds.
    pub fn is_linear(&self) -> bool {
        let array = self.value.as_datum().cast_mut_ptr::<pg_sys::ArrayType>();
        unsafe {
            match (*array).ndim {
                0 => true,
                1 => {
                    *array
                        .cast::<u8>()
                        .add(size_of::<pg_sys::ArrayType>())
                        .cast::<i32>()
                        .add(1)
                        == 1
                }
                _ => false,
            }
        }
    }
}

impl Iterator for ArrayDatumIter {
    type Item = Option<pg_sys::Datum>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut is_null = false;
        // SAFETY: setup established the cursor and bound element layout; the
        // remaining count establishes that another element exists. The detoast
        // guard keeps its backing array live. Unlike setup, this flat-array
        // step cannot raise PG ERROR with the catalog-bound element layout;
        // call directly to avoid a PG exception frame for every element. See
        // csrc/tuple/README.md for the reviewed PostgreSQL contract.
        let value = unsafe {
            lagodb_array_cursor_next(
                &mut self.cursor,
                self.layout.len.into(),
                self.layout.by_value,
                self.layout.align,
                &mut is_null,
            )
        };
        self.remaining -= 1;
        Some((!is_null).then_some(value))
    }
}
