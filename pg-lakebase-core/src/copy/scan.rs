//! PostgreSQL COPY parser state for relation scans.
//!
//! The parser is deliberately separate from [`super::driver::CopyFromDriver`].
//! It only converts one input document into a relation-shaped virtual slot;
//! it never invokes PostgreSQL's COPY insertion executor. A document source is
//! owned by this state so the callback guard remains installed for the whole
//! scan instead of being pushed and popped on every row.

use std::marker::PhantomData;
use std::mem;
use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;
use crate::fdw::ScanSlotWriter;

use super::error::CopyError;
use super::io::{CopyDataSource, SourceGuard, source_callback};
use super::pg;

/// A source containing a sequence of independent COPY input documents.
///
/// `next_document` must make the next document readable through [`read`]. It
/// returns `false` only after the complete input set has been consumed. The
/// source owns decompression and object boundaries; PostgreSQL owns parsing.
pub trait CopyDocumentSource: CopyDataSource {
    /// Borrow this document source through the COPY byte-source interface.
    ///
    /// This explicit object-safe conversion avoids relying on unstable trait
    /// object upcasting when the scan installs its PostgreSQL callback guard.
    fn copy_data_source(&mut self) -> &mut dyn CopyDataSource;

    /// Advance to the next independently parsed COPY document.
    fn next_document(&mut self) -> Result<bool, CopyError>;

    /// Reset the document sequence without listing or resolving it again.
    fn reset(&mut self) -> Result<(), CopyError>;
}

/// COPY parser state used by a Foreign Table scan.
pub struct CopyFromScan {
    state: Option<pg_sys::CopyFromState>,
    _source_guard: SourceGuard<'static>,
    source: Box<dyn CopyDocumentSource>,
    relation: pg_sys::Relation,
    options: *mut pg_sys::List,
    econtext: *mut pg_sys::ExprContext,
    _not_send_sync: PhantomData<*mut ()>,
}

impl CopyFromScan {
    /// Start the first document parser.
    ///
    /// `relation` must be the live executor relation and `options` must remain
    /// valid for the scan lifetime. The source is boxed to keep its callback
    /// address stable while parser states are replaced at object boundaries.
    ///
    /// # Safety
    ///
    /// The relation, expression context, and option list must be live for the
    /// returned scan. `source` must obey the [`CopyDocumentSource`] contract.
    pub unsafe fn begin(
        relation: pg_sys::Relation,
        econtext: *mut pg_sys::ExprContext,
        options: *mut pg_sys::List,
        mut source: Box<dyn CopyDocumentSource>,
    ) -> Result<Self, CopyError> {
        let has_document = source.next_document()?;

        // SAFETY: the boxed allocation never moves. The guard is dropped before
        // `source` (field order), and the parser owns the only mutable borrow
        // for the entire callback lifetime.
        let source_guard = unsafe { install_boxed_source(&mut source) };
        let mut scan = Self {
            state: None,
            _source_guard: source_guard,
            source,
            relation,
            options,
            econtext,
            _not_send_sync: PhantomData,
        };
        if has_document {
            scan.start_parser()?;
        }
        Ok(scan)
    }

    fn begin_state(&mut self) -> Result<pg_sys::CopyFromState, CopyError> {
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_from(
                    std::ptr::null_mut(),
                    self.relation,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    false,
                    source_callback(),
                    std::ptr::null_mut(),
                    self.options,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;
        Ok(result)
    }

    fn start_parser(&mut self) -> Result<(), CopyError> {
        self.state = Some(self.begin_state()?);
        Ok(())
    }

    fn end_state(state: pg_sys::CopyFromState) -> Result<(), CopyError> {
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::end_from(state);
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)
    }

    /// Decode one row into the relation-shaped scan slot.
    pub fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, CopyError> {
        loop {
            let Some(state) = self.state else {
                return Ok(false);
            };
            let (values, nulls) = unsafe { output.prepare_copy_input() };
            let found = unsafe {
                PgTryBuilder::new(AssertUnwindSafe(|| {
                    Ok(pg::CopyBridge::next_from(
                        state,
                        self.econtext,
                        values,
                        nulls,
                    ))
                }))
                .catch_others(|error| Err(PgError::from_caught(error)))
                .execute()
            }
            .map_err(CopyError::from)?;
            if found {
                unsafe { output.store_copy_input() };
                return Ok(true);
            }

            let state = self
                .state
                .take()
                .expect("the active COPY parser was present above");
            Self::end_state(state)?;
            if !self.source.next_document()? {
                return Ok(false);
            }
            self.start_parser()?;
        }
    }

    /// Reset the decoder to the first retained input document.
    pub fn rescan(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), CopyError> {
        if let Some(state) = self.state.take() {
            Self::end_state(state)?;
        }
        self.source.reset()?;
        self.econtext = econtext;
        if !self.source.next_document()? {
            return Ok(());
        }
        self.start_parser()
    }

    /// End the active parser state while retaining normal Rust cleanup order.
    pub fn end(&mut self) -> Result<(), CopyError> {
        if let Some(state) = self.state.take() {
            Self::end_state(state)?;
        }
        Ok(())
    }
}

impl Drop for CopyFromScan {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            let _ = Self::end_state(state);
        }
    }
}

/// Install the callback guard for a heap-stable source.
///
/// # Safety
///
/// `source` must stay in the same boxed allocation until the returned guard is
/// dropped, and the guard must be dropped before the source.
unsafe fn install_boxed_source(
    source: &mut Box<dyn CopyDocumentSource>,
) -> SourceGuard<'static> {
    let guard = SourceGuard::install(source.copy_data_source());
    // SAFETY: the source's boxed allocation remains stable while the guard is
    // stored, and the guard is dropped before the source. No callback can
    // outlive that ordering.
    unsafe { mem::transmute(guard) }
}
