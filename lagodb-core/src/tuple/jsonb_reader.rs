//! Borrowed binary JSONB access for schema-bound consumers.
//!
//! The callback owns one detoast guard. PG errors unwind to the enclosing
//! provider/FFI boundary. Container iterators never descend implicitly, so their allocation can be released on
//! early return as well as normal exhaustion. No JSON text or DOM is created.

mod datetime;
mod numeric;

use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::panic::AssertUnwindSafe;
use std::{slice, str};

use pgrx::PgTryBuilder;
use pgrx::pg_sys::{self, JsonbIteratorToken as Token, jbvType};

use super::{DetoastedVarlena, JsonbValue};
use crate::diag::PgError;

pub use datetime::JsonbDatetime;
pub use numeric::JsonbNumeric;

/// A value borrowing a detoasted JSONB document, valid only inside `read`.
#[derive(Clone, Copy)]
pub enum JsonbRef<'a> {
    Null,
    Boolean(bool),
    Numeric(JsonbNumeric<'a>),
    String(&'a str),
    Array(JsonbContainer<'a>),
    Object(JsonbObject<'a>),
}

pub struct JsonbReader;

impl JsonbReader {
    /// Cold adapter for a buffered semantic value, which already owns JSON
    /// text. Live tuple writes use `read` and never take this text path.
    /// Keep the checked adapter's Result contract and release its temporary
    /// input datum after either a visitor error or a caught PG error.
    ///
    /// # Safety
    /// Has the backend/encoding/context requirements of `read`.
    pub unsafe fn read_owned<R, E>(
        value: &JsonbValue,
        visit: impl for<'a> FnOnce(JsonbRef<'a>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<PgError>,
    {
        let mut input = None;
        let result = PgTryBuilder::new(AssertUnwindSafe(|| {
            // SAFETY: the caller establishes the backend/encoding/context
            // contract. Capture input errors at this operation's boundary too.
            let datum = unsafe { value.to_datum_checked() }
                .map_err(E::from)?
                .expect("jsonb_in returns non-NULL");
            input = Some(datum);
            // SAFETY: input stays live until the visitor and its borrows end.
            unsafe { Self::read(datum, visit) }
        }))
        .catch_others(|error| Err(PgError::from(error).into()))
        .execute();
        if let Some(datum) = input {
            // SAFETY: jsonb_in allocated this datum; read released all borrows
            // on both normal return and unwinding to the operation boundary.
            unsafe { pg_sys::pfree(datum.cast_mut_ptr()) };
        }
        result
    }

    /// Visit a non-NULL JSONB datum without exposing borrowed storage outside
    /// its detoast lifetime. PG errors unwind through the visitor to the caller.
    ///
    /// # Safety
    /// `datum` must be live JSONB on the backend thread. UTF-8 server encoding
    /// must have been validated by the column plan. A row context must be active.
    pub unsafe fn read<R, E>(
        datum: pg_sys::Datum,
        visit: impl for<'a> FnOnce(JsonbRef<'a>) -> Result<R, E>,
    ) -> Result<R, E> {
        // SAFETY: the caller supplies a live JSONB datum.
        let guard = unsafe { DetoastedVarlena::from_datum(datum) };
        let container = JsonbContainer {
            pointer: guard.bytes().as_ptr().cast_mut().cast(),
            borrowed: PhantomData,
        };
        // A scalar JSONB root is stored as a one-element raw-scalar array.
        let value = if container.header() & pg_sys::JB_FSCALAR != 0 {
            container
                .values()
                .next()
                .expect("JSONB scalar has one element")
        } else {
            container.value()
        };
        visit(value)
    }
}

#[derive(Clone, Copy)]
pub struct JsonbContainer<'a> {
    pointer: *mut pg_sys::JsonbContainer,
    borrowed: PhantomData<&'a pg_sys::JsonbContainer>,
}

impl<'a> JsonbContainer<'a> {
    fn header(self) -> u32 {
        // SAFETY: containers only originate from live, validated JSONB.
        unsafe { (*self.pointer).header }
    }

    pub fn len(self) -> usize {
        (self.header() & pg_sys::JB_CMASK) as usize
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    fn value(self) -> JsonbRef<'a> {
        // SAFETY: container views are constructed inside the reader boundary.
        // Check once at each container transition, including nested structs.
        unsafe { pg_sys::check_stack_depth() };
        if self.header() & pg_sys::JB_FOBJECT != 0 {
            JsonbRef::Object(JsonbObject(self))
        } else {
            JsonbRef::Array(self)
        }
    }

    /// Array elements, or alternating object keys and values.
    pub fn values(self) -> JsonbValues<'a> {
        // SAFETY: a valid container is live for 'a. The iterator is shallow,
        // hence has no parent allocations when dropped early.
        unsafe {
            let mut iterator = pg_sys::JsonbIteratorInit(self.pointer);
            let mut value = MaybeUninit::uninit();
            pg_sys::JsonbIteratorNext(&mut iterator, value.as_mut_ptr(), true);
            JsonbValues {
                iterator,
                borrowed: PhantomData,
            }
        }
    }
}

/// Object-only access keeps PG's object-container invariant out of field loops.
#[derive(Clone, Copy)]
pub struct JsonbObject<'a>(JsonbContainer<'a>);

impl<'a> JsonbObject<'a> {
    pub fn len(self) -> usize {
        self.0.len()
    }
    pub fn is_empty(self) -> bool {
        self.0.is_empty()
    }
    pub fn values(self) -> JsonbValues<'a> {
        self.0.values()
    }
    /// Look up an object field using PostgreSQL's binary search and a stack
    /// result slot (the same pattern as jsonb_populate_record).
    pub fn get(self, name: &str) -> Option<JsonbRef<'a>> {
        let mut value = MaybeUninit::uninit();
        // SAFETY: The object tag was established by the reader. The name and result
        // buffer are live for the call; payload pointers borrow the document.
        let found = unsafe {
            pg_sys::getKeyJsonValueFromContainer(
                self.0.pointer,
                name.as_ptr().cast(),
                name.len() as i32,
                value.as_mut_ptr(),
            )
        };
        if found.is_null() {
            None
        } else {
            Some(unsafe { JsonbRef::from_pg(value.assume_init()) })
        }
    }
}

pub struct JsonbValues<'a> {
    iterator: *mut pg_sys::JsonbIterator,
    borrowed: PhantomData<&'a pg_sys::JsonbContainer>,
}

impl<'a> Iterator for JsonbValues<'a> {
    type Item = JsonbRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut value = MaybeUninit::uninit();
        // SAFETY: iteration keeps nested containers as borrowed binary values.
        let token = unsafe {
            pg_sys::JsonbIteratorNext(&mut self.iterator, value.as_mut_ptr(), true)
        };
        match token {
            Token::WJB_KEY | Token::WJB_VALUE | Token::WJB_ELEM => {
                Some(unsafe { JsonbRef::from_pg(value.assume_init()) })
            }
            _ => None,
        }
    }
}

impl Drop for JsonbValues<'_> {
    fn drop(&mut self) {
        if !self.iterator.is_null() {
            // SAFETY: a shallow iterator has no parent; exhaustion already
            // frees it and sets this pointer to NULL.
            unsafe { pg_sys::pfree(self.iterator.cast()) };
        }
    }
}

impl<'a> JsonbRef<'a> {
    unsafe fn from_pg(value: pg_sys::JsonbValue) -> Self {
        // SAFETY: tags/payloads are produced by PostgreSQL's binary reader.
        unsafe {
            match value.type_ {
                jbvType::jbvNull => Self::Null,
                jbvType::jbvBool => Self::Boolean(value.val.boolean),
                jbvType::jbvNumeric => {
                    Self::Numeric(JsonbNumeric::new(value.val.numeric))
                }
                jbvType::jbvString => {
                    let s = value.val.string;
                    let bytes = slice::from_raw_parts(s.val.cast(), s.len as usize);
                    Self::String(str::from_utf8_unchecked(bytes))
                }
                jbvType::jbvBinary => JsonbContainer {
                    pointer: value.val.binary.data,
                    borrowed: PhantomData,
                }
                .value(),
                _ => unreachable!(
                    "shallow JSONB iterator returns scalars or binary containers"
                ),
            }
        }
    }
}
