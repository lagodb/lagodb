//! PostgreSQL COPY byte callback adapters.
//!
//! PostgreSQL's callback ABI has no user-data pointer and cannot return a Rust
//! error. Each input guard owns its source binding and activates it only for a
//! synchronous parser call. The callbacks are the only place where a
//! byte-adapter error is reported as a PostgreSQL ERROR.

use std::cell::{Cell, RefCell};
use std::ffi::{c_int, c_void};
use std::marker::PhantomData;
use std::mem;

use pgrx::{pg_guard, pg_sys};

use super::error::CopyError;
use super::layout::CopyColumnLayout;
use super::progress::CopyOutputProgress;
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
    /// Bind relation layout, PostgreSQL's parsed HEADER choice, and physical-byte
    /// progress before any output. Publish progress at file boundaries.
    fn initialize(
        &mut self,
        _layout: &CopyColumnLayout,
        _has_header: bool,
        _progress: CopyOutputProgress,
    ) -> Result<(), CopyError> {
        Ok(())
    }

    /// Write one complete row produced by PostgreSQL.
    ///
    /// `COPY_CALLBACK` omits the file/frontend row terminator. Destinations
    /// that serialize a line-oriented format must add its framing themselves;
    /// binary and other provider-specific destinations may use different
    /// framing.
    fn write_row(&mut self, data: &[u8]) -> Result<(), CopyError>;

    /// Physical output bytes, including encoder buffering, for COPY progress.
    fn bytes_produced(&self) -> u64;

    /// Finish encoding and publish output after successful executor shutdown.
    fn finish(&mut self) -> Result<(), CopyError>;

    /// Release unfinished output. Must not report a PostgreSQL ERROR.
    fn abort(&mut self) {}
}

type SourcePointer = *mut dyn CopyDataSource;
type DestinationPointer = *mut dyn CopyDataDestination;

thread_local! {
    static ACTIVE_SOURCE: Cell<Option<SourcePointer>> = const { Cell::new(None) };
    static DESTINATIONS: RefCell<Vec<DestinationPointer>> =
        const { RefCell::new(Vec::new()) };
}

pub(super) struct SourceGuard<'a> {
    source: SourcePointer,
    _lifetime: PhantomData<&'a mut dyn CopyDataSource>,
}

impl<'a> SourceGuard<'a> {
    pub(super) fn new(source: &'a mut dyn CopyDataSource) -> Self {
        // SAFETY: PostgreSQL's callback ABI has no user-data pointer, so the
        // binding erases the borrow lifetime. PhantomData retains the exclusive
        // borrow; with_active lends this pointer only during a synchronous call.
        let source = unsafe {
            mem::transmute::<*mut (dyn CopyDataSource + 'a), SourcePointer>(
                source as *mut (dyn CopyDataSource + 'a),
            )
        };
        Self {
            source,
            _lifetime: PhantomData,
        }
    }

    /// Adapt the native callback ABI for this parser invocation. PostgreSQL
    /// errors must be caught inside `call` before control returns to Rust.
    /// Nested parser/COPY invocations restore the caller's source on return,
    /// including Rust unwinding; inactive parsers retain no TLS entry.
    pub(super) fn with_active<T>(&mut self, call: impl FnOnce() -> T) -> T {
        let previous = ACTIVE_SOURCE.replace(Some(self.source));
        let _activation = SourceActivation(previous);
        call()
    }
}

struct SourceActivation(Option<SourcePointer>);

impl Drop for SourceActivation {
    fn drop(&mut self) {
        ACTIVE_SOURCE.set(self.0);
    }
}

pub(super) struct DestinationGuard<'a> {
    destination: DestinationPointer,
    finished: bool,
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
            destination,
            finished: false,
            _lifetime: PhantomData,
        }
    }

    pub(super) fn initialize(
        &mut self,
        layout: &CopyColumnLayout,
        has_header: bool,
        progress: CopyOutputProgress,
    ) -> Result<(), CopyError> {
        // SAFETY: the guard retains the exclusive destination borrow.
        unsafe { (&mut *self.destination).initialize(layout, has_header, progress) }
    }

    pub(super) fn finish(&mut self) -> Result<u64, CopyError> {
        // SAFETY: the destination remains exclusively borrowed until Drop.
        let destination = unsafe { &mut *self.destination };
        destination.finish()?;
        self.finished = true;
        Ok(destination.bytes_produced())
    }
}

impl Drop for DestinationGuard<'_> {
    fn drop(&mut self) {
        DESTINATIONS.with_borrow_mut(|destinations| {
            let destination = destinations.pop();
            debug_assert!(destination.is_some());
        });
        if !self.finished {
            // SAFETY: uninstall callbacks first; the exclusive borrow is still live.
            unsafe { (&mut *self.destination).abort() };
        }
    }
}

pub(super) const fn source_callback() -> pg_sys::copy_data_source_cb {
    Some(copy_source_callback)
}

pub(super) const fn destination_callback() -> pg_sys::copy_data_dest_cb {
    Some(copy_destination_callback)
}

pub(super) fn report(error: CopyError) -> ! {
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
    let source = ACTIVE_SOURCE.get();
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
