//! Project-owned Arrow C Stream exporter for provider query sources.

use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{FFI_OPERATION_FAILED, FfiErrorRecord};
use pgrx::prelude::PgSqlErrorCode;

use super::QuerySourceStream;

struct StreamState<S> {
    stream: S,
    error: *mut FfiErrorRecord,
}

/// Export one provider stream using callbacks that contain both PostgreSQL
/// errors and Rust panics.
///
/// # Safety
///
/// `error` must remain writable until the returned Arrow stream is released.
pub(super) unsafe fn export<S: QuerySourceStream>(
    stream: S,
    error: *mut FfiErrorRecord,
) -> FFI_ArrowArrayStream {
    let state = Box::new(StreamState { stream, error });
    FFI_ArrowArrayStream {
        get_schema: Some(get_schema::<S>),
        get_next: Some(get_next::<S>),
        get_last_error: Some(get_last_error),
        release: Some(release::<S>),
        private_data: Box::into_raw(state).cast(),
    }
}

unsafe extern "C" fn get_schema<S: QuerySourceStream>(
    stream: *mut FFI_ArrowArrayStream,
    output: *mut FFI_ArrowSchema,
) -> c_int {
    // SAFETY: Arrow invokes this callback only on the live stream produced by
    // `export` and has not released its private data.
    let state = unsafe { state::<S>(stream) };
    // SAFETY: `export` ties this pointer to the stream's release lifetime and
    // Arrow invokes callbacks synchronously on the PostgreSQL backend thread.
    let error = unsafe { &mut *state.error };
    // SAFETY: the engine-owned record is live and this is a PostgreSQL backend
    // thread with a live current memory context.
    let result = unsafe {
        error.capture_result(|| {
            let schema = FFI_ArrowSchema::try_from(state.stream.schema().as_ref())
                .map_err(|error| {
                    PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        format!("query source returned an unexportable Arrow schema: {error}"),
                    )
                })?;
            Ok(schema)
        })
    };
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

unsafe extern "C" fn get_next<S: QuerySourceStream>(
    stream: *mut FFI_ArrowArrayStream,
    output: *mut FFI_ArrowArray,
) -> c_int {
    // SAFETY: Arrow invokes this callback only on the live stream produced by
    // `export` and has not released its private data.
    let state = unsafe { state::<S>(stream) };
    // SAFETY: the error slot follows the same lifetime and backend-thread
    // contract documented by `export`.
    let error = unsafe { &mut *state.error };
    // SAFETY: the engine-owned record is live and this is a PostgreSQL backend
    // thread with a live current memory context.
    let result = unsafe {
        error.capture_result(|| {
            let batch = state
                .stream
                .next_batch()
                .map_err(PgReportError::from_domain_error)?;
            Ok(batch.map(|batch| {
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
    c"query source stream callback failed".as_ptr()
}

unsafe extern "C" fn release<S: QuerySourceStream>(
    stream: *mut FFI_ArrowArrayStream,
) {
    if stream.is_null() {
        return;
    }
    // SAFETY: Arrow calls release with the live stream value created by
    // `export`; the null case was handled above.
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
    // SAFETY: `private_data` still owns the live state installed by `export`.
    let error = unsafe { (*private_data).error };
    let mut cleanup_error = FfiErrorRecord::default();
    let release_state = || {
        // SAFETY: release took the unique private-data ownership above and
        // cleared the callback before reconstructing the box.
        drop(unsafe { Box::from_raw(private_data) });
        Ok(())
    };
    // SAFETY: release runs on the PostgreSQL backend thread and the temporary
    // record remains live through the contained Drop operation.
    let status = unsafe { cleanup_error.capture(release_state) };
    // SAFETY: the engine-owned slot outlives stream release by `export`'s
    // contract and callback/consumer access is serialized.
    if status == FFI_OPERATION_FAILED && !unsafe { (*error).is_set() } {
        // SAFETY: same live, uniquely written error slot as the condition.
        unsafe { *error = cleanup_error };
    }
}

unsafe fn state<'a, S>(stream: *mut FFI_ArrowArrayStream) -> &'a mut StreamState<S> {
    // SAFETY: every callback is installed only by `export`, which stores a
    // live `StreamState<S>` in `private_data` until `release` clears it.
    unsafe { &mut *(*stream).private_data.cast::<StreamState<S>>() }
}
