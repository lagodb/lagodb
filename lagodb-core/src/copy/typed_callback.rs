//! Explicit callback contexts for Datum/slot COPY execution.

use std::ffi::{CString, c_char, c_void};
use std::marker::PhantomData;
use std::mem;
use std::ptr::NonNull;

use pgrx::{pg_guard, pg_sys};

use super::io::report;
use super::{
    CopyColumnLayout, CopyDatumSource, CopyError, CopyInputRow, CopyOutputRow,
    CopyRowOutcome, CopyRowRejection, CopyTupleDestination,
};

type SourcePointer = *mut dyn CopyDatumSource;
type DestinationPointer = *mut dyn CopyTupleDestination;

struct OwnedRejection {
    message: CString,
    location: Option<CString>,
    column: i32,
    sql_error_code: i32,
}

impl OwnedRejection {
    fn new(rejection: CopyRowRejection) -> Self {
        let column = rejection
            .column_index()
            .and_then(|index| i32::try_from(index + 1).ok())
            .unwrap_or(-1);
        Self {
            message: ffi_text(rejection.message()),
            location: rejection.location().map(ffi_text),
            column,
            sql_error_code: rejection.sql_error_code() as i32,
        }
    }
}

fn ffi_text(value: &str) -> CString {
    let bytes = value.as_bytes();
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    CString::new(&bytes[..end]).unwrap_or_else(|_| unreachable!())
}

struct TypedSourceContext {
    source: SourcePointer,
    layout: CopyColumnLayout,
    rejection: Option<OwnedRejection>,
}

struct TypedDestinationContext {
    destination: DestinationPointer,
    layout: CopyColumnLayout,
}

pub(super) struct TypedSourceGuard<'a> {
    context: TypedSourceContext,
    finished: bool,
    _lifetime: PhantomData<&'a mut dyn CopyDatumSource>,
}

impl<'a> TypedSourceGuard<'a> {
    pub(super) fn install(
        source: &'a mut dyn CopyDatumSource,
        layout: CopyColumnLayout,
    ) -> Self {
        // SAFETY: the guard retains the exclusive borrow until finalization.
        // The C executor receives a pointer to this context only for its
        // synchronous execute/end lifetime.
        let source = unsafe {
            mem::transmute::<*mut (dyn CopyDatumSource + 'a), SourcePointer>(
                source as *mut (dyn CopyDatumSource + 'a),
            )
        };
        Self {
            context: TypedSourceContext {
                source,
                layout,
                rejection: None,
            },
            finished: false,
            _lifetime: PhantomData,
        }
    }

    pub(super) fn callback_context(&mut self) -> NonNull<c_void> {
        NonNull::from(&mut self.context).cast()
    }

    pub(super) fn finish(&mut self) -> Result<(), CopyError> {
        // SAFETY: _lifetime keeps the exclusively borrowed source alive for
        // the guard's entire lifetime.
        let result = unsafe { (&mut *self.context.source).finish() };
        if result.is_ok() {
            self.finished = true;
        }
        result
    }
}

impl Drop for TypedSourceGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // SAFETY: the same lifetime invariant as finish applies in Drop.
            unsafe { (&mut *self.context.source).abort() };
        }
    }
}

pub(super) struct TypedDestinationGuard<'a> {
    context: TypedDestinationContext,
    finished: bool,
    _lifetime: PhantomData<&'a mut dyn CopyTupleDestination>,
}

impl<'a> TypedDestinationGuard<'a> {
    pub(super) fn install(
        destination: &'a mut dyn CopyTupleDestination,
        layout: CopyColumnLayout,
    ) -> Result<Self, CopyError> {
        destination.initialize(&layout)?;
        // SAFETY: identical lifetime and synchronous callback contract to the
        // typed source guard above.
        let destination = unsafe {
            mem::transmute::<*mut (dyn CopyTupleDestination + 'a), DestinationPointer>(
                destination as *mut (dyn CopyTupleDestination + 'a),
            )
        };
        Ok(Self {
            context: TypedDestinationContext {
                destination,
                layout,
            },
            finished: false,
            _lifetime: PhantomData,
        })
    }

    pub(super) fn callback_context(&mut self) -> NonNull<c_void> {
        NonNull::from(&mut self.context).cast()
    }

    pub(super) fn finish(&mut self) -> Result<u64, CopyError> {
        // SAFETY: _lifetime keeps the exclusively borrowed destination alive
        // for the guard's entire lifetime.
        let result = unsafe { (&mut *self.context.destination).finish() };
        if result.is_ok() {
            self.finished = true;
        }
        result.map(|()| {
            // SAFETY: the destination remains borrowed by this guard through
            // finalization.
            unsafe { (&*self.context.destination).bytes_produced() }
        })
    }
}

impl Drop for TypedDestinationGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // SAFETY: the same lifetime invariant as finish applies in Drop.
            unsafe { (&mut *self.context.destination).abort() };
        }
    }
}

pub(super) const fn source_callback() -> super::pg::TypedCopySourceCallback {
    typed_source_callback
}

pub(super) const fn destination_callback() -> super::pg::TypedCopyDestinationCallback
{
    typed_destination_callback
}

#[pg_guard]
unsafe extern "C-unwind" fn typed_source_callback(
    context: *mut c_void,
    values: *mut pg_sys::Datum,
    nulls: *mut bool,
    bytes_consumed: *mut u64,
    materialized_bytes: *mut u64,
    rejection_message: *mut *const c_char,
    rejection_location: *mut *const c_char,
    rejection_column: *mut std::ffi::c_int,
    rejection_sql_error_code: *mut std::ffi::c_int,
) -> std::ffi::c_int {
    // SAFETY: the routed executor passes the unique context pointer borrowed
    // from its live TypedSourceGuard and invokes this callback synchronously.
    let context = unsafe { &mut *context.cast::<TypedSourceContext>() };
    context.rejection = None;
    // SAFETY: the derived executor passes the live arrays for the descriptor
    // used to build context.layout before installing this callback.
    let mut row_bytes = 0;
    let row = unsafe {
        CopyInputRow::from_raw(values, nulls, &context.layout, &mut row_bytes)
    };
    // SAFETY: the guard retains exclusive access to the source while the
    // synchronous callback uses its erased-lifetime pointer.
    let outcome = unsafe { (&mut *context.source).next_row(row) }
        .unwrap_or_else(|error| report(error));

    // SAFETY: the bridge always supplies storage for every callback output.
    unsafe {
        *bytes_consumed = (&*context.source).bytes_consumed();
        *materialized_bytes = u64::try_from(row_bytes).expect(
            "PostgreSQL is supported only on platforms where usize fits in u64",
        );
    }
    match outcome {
        CopyRowOutcome::Row => 0,
        CopyRowOutcome::End => 1,
        CopyRowOutcome::Rejected(rejection) => {
            context.rejection = Some(OwnedRejection::new(rejection));
            let rejection = context.rejection.as_ref().expect("just stored");
            // SAFETY: the bridge always supplies storage for every callback
            // output and consumes these borrowed C strings before returning.
            unsafe {
                *rejection_message = rejection.message.as_ptr();
                *rejection_location = rejection
                    .location
                    .as_ref()
                    .map_or(std::ptr::null(), |location| location.as_ptr());
                *rejection_column = rejection.column;
                *rejection_sql_error_code = rejection.sql_error_code;
            }
            2
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn typed_destination_callback(
    context: *mut c_void,
    slot: *mut pg_sys::TupleTableSlot,
    bytes_produced: *mut u64,
) {
    // SAFETY: the routed executor passes the unique context pointer borrowed
    // from its live TypedDestinationGuard and invokes this callback synchronously.
    let context = unsafe { &mut *context.cast::<TypedDestinationContext>() };
    // SAFETY: CopyOneRowTo passes its live executor slot synchronously, and
    // context.layout was built from this COPY state's final descriptor.
    let row = unsafe { CopyOutputRow::from_raw(slot, &context.layout) };
    // SAFETY: the guard retains exclusive access to the destination while the
    // synchronous callback uses its erased-lifetime pointer.
    unsafe { (&mut *context.destination).write_slot(row) }
        .unwrap_or_else(|error| report(error));
    // SAFETY: the bridge supplies callback-scoped storage for this output.
    unsafe {
        *bytes_produced = (&*context.destination).bytes_produced();
    }
}
