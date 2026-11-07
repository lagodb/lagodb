//! Producer-side C ABI export for the typed table-scan adapters.
//!
//! Provider schemas and streams leave through the Arrow C Data/Stream
//! interfaces; provider handles leave as boxed pointers and return through
//! the descriptor release callbacks.

use std::ffi::{c_char, c_int, c_void};
use std::ptr::{self, NonNull};

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, StructArray};
use arrow_schema::Schema;
use arrow_schema::ffi::FFI_ArrowSchema;
use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{CALLBACK_FAILED, CallbackErrorReport};
use pgrx::prelude::PgSqlErrorCode;

use super::provider::TableScanStream;

/// Export a provider schema through the Arrow C Data Interface.
pub(super) fn schema(schema: &Schema) -> Result<FFI_ArrowSchema, PgReportError> {
    FFI_ArrowSchema::try_from(schema).map_err(|error| {
        PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("table scan returned an unexportable Arrow schema: {error}"),
        )
    })
}

/// Release callback for a handle the adapter created with
/// `Box::into_raw(Box::new(T))`. The destructor runs inside the callback
/// error capture so a panic is reported instead of unwinding across the ABI.
///
/// # Safety
///
/// The runtime passes the unique, not yet released `T` handle produced by the
/// matching adapter callback, together with live callback error storage.
pub(super) unsafe extern "C-unwind" fn release_boxed<T>(
    _context: *mut c_void,
    handle: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32 {
    // SAFETY: upheld by this callback's contract.
    let handle = unsafe { Box::from_raw(handle.cast::<T>()) };
    // SAFETY: the runtime supplies matching live callback storage.
    unsafe {
        (&mut *error).capture(move || {
            drop(handle);
            Ok(())
        })
    }
}

struct StreamState<S> {
    stream: S,
    error: NonNull<CallbackErrorReport>,
}

/// Export one provider stream through the Arrow C Stream Interface, using
/// callbacks that contain both PostgreSQL errors and Rust panics.
///
/// # Safety
///
/// `error` must remain writable until the returned Arrow stream is released.
pub(super) unsafe fn stream<S: TableScanStream>(
    provider_stream: S,
    error: NonNull<CallbackErrorReport>,
) -> FFI_ArrowArrayStream {
    let state = Box::new(StreamState {
        stream: provider_stream,
        error,
    });
    FFI_ArrowArrayStream {
        get_schema: Some(get_schema::<S>),
        get_next: Some(get_next::<S>),
        get_last_error: Some(get_last_error),
        release: Some(release::<S>),
        private_data: Box::into_raw(state).cast(),
    }
}

unsafe extern "C" fn get_schema<S: TableScanStream>(
    stream: *mut FFI_ArrowArrayStream,
    output: *mut FFI_ArrowSchema,
) -> c_int {
    // SAFETY: Arrow invokes this callback only on the live stream produced by
    // `stream` and has not released its private data.
    let state = unsafe { state::<S>(stream) };
    // SAFETY: `stream` ties this pointer to the stream's release lifetime and
    // Arrow invokes callbacks synchronously on the PostgreSQL backend thread.
    let error = unsafe { state.error.as_mut() };
    // SAFETY: the engine-owned record is live and this is a PostgreSQL backend
    // thread with a live current memory context.
    let result = unsafe { error.capture_result(|| schema(&state.stream.schema())) };
    match result {
        Ok(schema) => {
            // SAFETY: the Arrow C Stream contract supplies initialized,
            // correctly aligned caller-owned output storage.
            unsafe { output.write(schema) };
            0
        }
        Err(_) => libc::EIO,
    }
}

unsafe extern "C" fn get_next<S: TableScanStream>(
    stream: *mut FFI_ArrowArrayStream,
    output: *mut FFI_ArrowArray,
) -> c_int {
    // SAFETY: Arrow invokes this callback only on the live stream produced by
    // `stream` and has not released its private data.
    let state = unsafe { state::<S>(stream) };
    // SAFETY: the error slot follows the same lifetime and backend-thread
    // contract documented by `stream`.
    let error = unsafe { state.error.as_mut() };
    // SAFETY: the engine-owned record is live and this is a PostgreSQL backend
    // thread with a live current memory context.
    let result = unsafe {
        error.capture_result(|| {
            let batch = state
                .stream
                .next_batch()
                .map_err(PgReportError::from_domain_error)?;
            Ok(batch.map(|batch| {
                // C arrays carry buffers, not Field metadata. Import assigns
                // the stream's get_schema contract (including nested logical
                // annotations) to these unchanged physical buffers.
                FFI_ArrowArray::new(&StructArray::from(batch).to_data())
            }))
        })
    };
    match result {
        Ok(Some(array)) => {
            // SAFETY: the Arrow consumer supplies output storage for exactly
            // one FFI_ArrowArray and assumes ownership of the written value.
            unsafe { output.write(array) };
            0
        }
        Ok(None) => {
            // SAFETY: the Arrow consumer supplies output storage; a released
            // array is the C Stream end-of-stream sentinel.
            unsafe { output.write(FFI_ArrowArray::empty()) };
            0
        }
        Err(_) => libc::EIO,
    }
}

unsafe extern "C" fn get_last_error(
    _stream: *mut FFI_ArrowArrayStream,
) -> *const c_char {
    c"table scan stream callback failed".as_ptr()
}

unsafe extern "C" fn release<S: TableScanStream>(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    // SAFETY: Arrow calls release with the live stream value created by
    // `stream`; the null case was handled above.
    let stream = unsafe { &mut *stream };
    // Clear the release callback before invoking provider Drop. Even when Drop
    // panics, ownership has been consumed and a second release is forbidden.
    let private_data = stream.private_data.cast::<StreamState<S>>();
    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;
    stream.release = None;
    stream.private_data = ptr::null_mut::<c_void>();

    // Preserve an earlier schema/batch error as the primary diagnostic. A
    // release panic is recorded only when release itself is the first failure.
    // SAFETY: `private_data` still owns the live state installed by `stream`.
    let error = unsafe { (*private_data).error };
    let mut cleanup_error = CallbackErrorReport::default();
    let release_state = || {
        // SAFETY: release took the unique private-data ownership above and
        // cleared the callback before reconstructing the box.
        drop(unsafe { Box::from_raw(private_data) });
        Ok(())
    };
    // SAFETY: release runs on the PostgreSQL backend thread and the temporary
    // record remains live through the contained Drop operation.
    let status = unsafe { cleanup_error.capture(release_state) };
    // SAFETY: the engine-owned slot outlives stream release by `stream`'s
    // contract and callback/consumer access is serialized.
    if status == CALLBACK_FAILED && !unsafe { error.as_ref() }.is_set() {
        // SAFETY: same live, uniquely written error slot as the condition.
        unsafe { error.write(cleanup_error) };
    }
}

unsafe fn state<'a, S>(stream: *mut FFI_ArrowArrayStream) -> &'a mut StreamState<S> {
    // SAFETY: every callback is installed only by `stream`, which stores a
    // live `StreamState<S>` in `private_data` until `release` clears it.
    unsafe { &mut *(*stream).private_data.cast::<StreamState<S>>() }
}
