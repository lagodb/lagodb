//! PostgreSQL COPY parser state for relation scans.
//!
//! The parser is deliberately separate from [`super::driver::CopyFromDriver`].
//! It only converts one input document into a relation-shaped virtual slot;
//! it never invokes PostgreSQL's COPY insertion executor. A document source is
//! owned by this state. Only the parser being invoked activates its input
//! binding, so other live scans cannot redirect its reads.

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
    /// object upcasting when the scan creates its PostgreSQL source binding.
    fn copy_data_source(&mut self) -> &mut dyn CopyDataSource;

    /// Advance to the next independently parsed COPY document.
    fn next_document(&mut self) -> Result<bool, CopyError>;

    /// Reset the document sequence without listing or resolving it again.
    fn reset(&mut self) -> Result<(), CopyError>;
}

/// COPY parser state used by a Foreign Table scan.
///
/// PostgreSQL owns the parser allocations in the executor query context.
/// Normal scan termination calls [`Self::end`], while query abort reclaims
/// those allocations before dropping the Rust scan state. Rust destruction
/// therefore only releases the source and its binding; it must not
/// access the PostgreSQL parser after context deletion.
pub struct CopyFromScan {
    state: Option<pg_sys::CopyFromState>,
    source_guard: SourceGuard<'static>,
    source: Box<dyn CopyDocumentSource>,
    relation: pg_sys::Relation,
    options: *mut pg_sys::List,
    econtext: *mut pg_sys::ExprContext,
    parser_context: pg_sys::MemoryContext,
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

        // SAFETY: the boxed allocation never moves. The scan activates this
        // binding only during its own synchronous parser calls; transitions
        // between documents access the source while the binding is inactive.
        let source_guard = unsafe { Self::bind_source(&mut source) };
        let mut scan = Self {
            state: None,
            source_guard,
            source,
            relation,
            options,
            econtext,
            // CopyFromScan::begin is invoked while the FDW framework has
            // switched to the executor query context. Parser replacements at
            // prefix object boundaries happen later from the per-tuple
            // context, so retain this parent for every BeginCopyFrom call.
            parser_context: unsafe { pg_sys::CurrentMemoryContext },
            _not_send_sync: PhantomData,
        };
        if has_document {
            scan.start_parser()?;
        }
        Ok(scan)
    }

    fn begin_state(&mut self) -> Result<pg_sys::CopyFromState, CopyError> {
        let relation = self.relation;
        let options = self.options;
        let result = self.source_guard.with_active(|| unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_from(
                    std::ptr::null_mut(),
                    relation,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    false,
                    source_callback(),
                    std::ptr::null_mut(),
                    options,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        })?;
        Ok(result)
    }

    fn start_parser(&mut self) -> Result<(), CopyError> {
        // BeginCopyFrom allocates the CopyFromState itself in the current
        // context and its workspace in a child context. A prefix scan can
        // replace this parser while IterateForeignScan is running in the
        // executor's resettable per-tuple context, so create every parser in
        // the query-lifetime context captured by begin().
        let prior_context =
            unsafe { pg_sys::MemoryContextSwitchTo(self.parser_context) };
        let result = self.begin_state();
        unsafe { pg_sys::MemoryContextSwitchTo(prior_context) };
        self.state = Some(result?);
        Ok(())
    }

    fn end_state(state: pg_sys::CopyFromState) {
        unsafe { pg::CopyBridge::end_from(state) }
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
            let econtext = self.econtext;
            let found = self
                .source_guard
                .with_active(|| unsafe {
                    PgTryBuilder::new(AssertUnwindSafe(|| {
                        Ok(pg::CopyBridge::next_from(state, econtext, values, nulls))
                    }))
                    .catch_others(|error| Err(PgError::from_caught(error)))
                    .execute()
                })
                .map_err(CopyError::from)?;
            if found {
                unsafe { output.store_copy_input() };
                return Ok(true);
            }

            let state = self
                .state
                .take()
                .expect("the active COPY parser was present above");
            Self::end_state(state);
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
            Self::end_state(state);
        }
        self.source.reset()?;
        self.econtext = econtext;
        if !self.source.next_document()? {
            return Ok(());
        }
        self.start_parser()
    }

    /// End the active parser state while retaining normal Rust cleanup order.
    pub fn end(&mut self) {
        if let Some(state) = self.state.take() {
            Self::end_state(state);
        }
    }

    /// # Safety
    ///
    /// The returned binding must be owned by the scan containing this box.
    unsafe fn bind_source(
        source: &mut Box<dyn CopyDocumentSource>,
    ) -> SourceGuard<'static> {
        let guard = SourceGuard::new(source.copy_data_source());
        // SAFETY: the source's boxed allocation remains stable while the guard
        // is stored. Calls activate this binding only while the scan is live;
        // document transitions access the source only outside those calls.
        unsafe { mem::transmute(guard) }
    }
}
