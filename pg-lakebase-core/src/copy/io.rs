//! PostgreSQL COPY byte callback adapters.
//!
//! PostgreSQL's callback ABI has no user-data pointer and cannot return a Rust
//! error. The guards below install one backend-local callback state for the
//! synchronous `BeginCopy*`/`DoCopy*` call. The callbacks are the only place
//! where a byte-adapter error is reported as a PostgreSQL ERROR.

use std::cell::RefCell;
use std::ffi::{c_int, c_void};
use std::marker::PhantomData;
use std::mem;

use pgrx::{pg_guard, pg_sys};

use super::error::CopyError;
use super::layout::CopyColumnLayout;
use crate::diag::PgReportError;

pub trait CopyDataSource {
    /// Fill PostgreSQL's input buffer.
    ///
    /// A successful return below `min_read` means EOF to PostgreSQL. Sources
    /// that encounter a short non-EOF read must continue until they have read
    /// at least `min_read` bytes or have reached actual EOF.
    fn read(
        &mut self,
        output: &mut [u8],
        min_read: usize,
    ) -> Result<usize, CopyError>;
}

pub trait CopyDataDestination {
    fn initialize(&mut self, _layout: &CopyColumnLayout) -> Result<(), CopyError> {
        Ok(())
    }

    /// Write one complete row produced by PostgreSQL.
    ///
    /// `COPY_CALLBACK` omits the file/frontend row terminator. Destinations
    /// that serialize a line-oriented format must add its framing themselves;
    /// binary and other provider-specific destinations may use different
    /// framing.
    fn write_row(&mut self, data: &[u8]) -> Result<(), CopyError>;
}

type SourcePointer = *mut dyn CopyDataSource;
type DestinationPointer = *mut dyn CopyDataDestination;

thread_local! {
    static SOURCES: RefCell<Vec<SourcePointer>> = const { RefCell::new(Vec::new()) };
    static DESTINATIONS: RefCell<Vec<DestinationPointer>> =
        const { RefCell::new(Vec::new()) };
}

pub(super) struct SourceGuard<'a> {
    _lifetime: PhantomData<&'a mut dyn CopyDataSource>,
}

impl<'a> SourceGuard<'a> {
    pub(super) fn install(source: &'a mut dyn CopyDataSource) -> Self {
        // SAFETY: PostgreSQL's callback ABI has no user-data pointer, so the
        // backend-local stack must erase the borrow lifetime. SourceGuard's
        // PhantomData retains the exclusive borrow, and Drop removes this
        // exact pointer. BeginCopyFrom/CopyFrom invoke callbacks synchronously
        // while the guard is owned by CopyFromDriver; nested COPY operations
        // are isolated by the stack.
        let source = unsafe {
            mem::transmute::<*mut (dyn CopyDataSource + 'a), SourcePointer>(
                source as *mut (dyn CopyDataSource + 'a),
            )
        };
        SOURCES.with_borrow_mut(|sources| sources.push(source));
        Self {
            _lifetime: PhantomData,
        }
    }
}

impl Drop for SourceGuard<'_> {
    fn drop(&mut self) {
        SOURCES.with_borrow_mut(|sources| {
            debug_assert!(sources.pop().is_some());
        });
    }
}

pub(super) struct DestinationGuard<'a> {
    _lifetime: PhantomData<&'a mut dyn CopyDataDestination>,
}

impl<'a> DestinationGuard<'a> {
    pub(super) fn install(destination: &'a mut dyn CopyDataDestination) -> Self {
        // SAFETY: this erases only the trait object's borrow lifetime. The
        // guard retains the exclusive borrow and removes the pointer on Drop;
        // PostgreSQL invokes the destination callback synchronously while the
        // owning CopyToDriver and destination remain live.
        let destination = unsafe {
            mem::transmute::<*mut (dyn CopyDataDestination + 'a), DestinationPointer>(
                destination as *mut (dyn CopyDataDestination + 'a),
            )
        };
        DESTINATIONS.with_borrow_mut(|destinations| destinations.push(destination));
        Self {
            _lifetime: PhantomData,
        }
    }

    pub(super) fn initialize(
        &mut self,
        layout: &CopyColumnLayout,
    ) -> Result<(), CopyError> {
        let destination =
            DESTINATIONS.with_borrow(|destinations| destinations.last().copied());
        let Some(destination) = destination else {
            return Err(CopyError::MissingCallbackState);
        };
        unsafe { (&mut *destination).initialize(layout) }
    }
}

impl Drop for DestinationGuard<'_> {
    fn drop(&mut self) {
        DESTINATIONS.with_borrow_mut(|destinations| {
            debug_assert!(destinations.pop().is_some());
        });
    }
}

pub(super) const fn source_callback() -> pg_sys::copy_data_source_cb {
    Some(copy_source_callback)
}

pub(super) const fn destination_callback() -> pg_sys::copy_data_dest_cb {
    Some(copy_destination_callback)
}

fn report(error: CopyError) -> ! {
    match error {
        CopyError::Postgres(error) => error.report(),
        error => PgReportError::from_domain_error(error).report(),
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn copy_source_callback(
    outbuf: *mut c_void,
    minread: c_int,
    maxread: c_int,
) -> c_int {
    let minread = minread as usize;
    let maxread = maxread as usize;
    // PostgreSQL treats a return value below minread as EOF. CopyDataSource
    // owns the retry policy because only the provider can distinguish a short
    // transport/decoder read from decoded EOF.
    debug_assert!(!outbuf.is_null());
    let output =
        unsafe { std::slice::from_raw_parts_mut(outbuf.cast::<u8>(), maxread) };
    let source = SOURCES.with_borrow(|sources| sources.last().copied());
    let Some(source) = source else {
        report(CopyError::MissingCallbackState);
    };
    let result = unsafe { (&mut *source).read(output, minread) };
    match result {
        Ok(read) if read <= maxread => read as c_int,
        Ok(read) => report(CopyError::invalid_byte_count(read, maxread)),
        Err(error) => report(error),
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn copy_destination_callback(data: *mut c_void, len: c_int) {
    let len = len as usize;
    debug_assert!(!data.is_null());
    let bytes = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) };
    let destination =
        DESTINATIONS.with_borrow(|destinations| destinations.last().copied());
    let Some(destination) = destination else {
        report(CopyError::MissingCallbackState);
    };
    if let Err(error) = unsafe { (&mut *destination).write_row(bytes) } {
        report(error);
    }
}
