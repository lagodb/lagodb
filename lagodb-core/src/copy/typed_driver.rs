//! PostgreSQL-derived COPY drivers for provider-native Datum and slot paths.

use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use super::from_execution::CopyFromExecution;
use crate::diag::PgError;

use super::context::{
    CopyFromPreparation, CopyParseState, CopyStatement, CopyToPreparation,
};
use super::pg::CopyToShutdown;
use super::typed_callback::{TypedDestinationGuard, TypedSourceGuard};
use super::{
    CopyColumnLayout, CopyDatumSource, CopyError, CopyOnError, CopyTupleDestination,
    pg,
};

pub struct TypedCopyFromSpec<'statement, 'parse> {
    statement: &'statement CopyStatement<'statement>,
    parse_state: &'parse CopyParseState,
    preparation: CopyFromPreparation<'statement, 'parse>,
    options: *mut pg_sys::List,
}

impl<'statement, 'parse> TypedCopyFromSpec<'statement, 'parse> {
    /// # Safety
    ///
    /// The preparation and option list must belong to this statement and parse
    /// state.
    pub unsafe fn new(
        statement: &'statement CopyStatement<'statement>,
        parse_state: &'parse CopyParseState,
        preparation: CopyFromPreparation<'statement, 'parse>,
        options: *mut pg_sys::List,
    ) -> Self {
        Self {
            statement,
            parse_state,
            preparation,
            options,
        }
    }
}

pub struct TypedCopyFromDriver<'statement, 'parse> {
    execution: CopyFromExecution,
    _preparation: CopyFromPreparation<'statement, 'parse>,
    _statement: PhantomData<&'statement pg_sys::CopyStmt>,
    _parse: PhantomData<&'parse CopyParseState>,
}

impl<'statement, 'parse> TypedCopyFromDriver<'statement, 'parse> {
    /// Validates options and prepares the target before provider input is opened.
    pub fn begin(
        spec: TypedCopyFromSpec<'statement, 'parse>,
    ) -> Result<Self, CopyError> {
        let relation = spec.preparation.relation();
        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_routed_from(
                    spec.parse_state.as_raw(),
                    relation,
                    spec.preparation.where_clause(),
                    std::ptr::null(),
                    false,
                    None,
                    spec.statement.attlist(),
                    spec.options,
                    true,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;
        Ok(Self {
            execution: CopyFromExecution::new(state),
            _preparation: spec.preparation,
            _statement: PhantomData,
            _parse: PhantomData,
        })
    }

    /// Constructs a source bound to PostgreSQL's accepted column layout and
    /// `ON_ERROR` choice before starting the executor. Construction errors,
    /// including PostgreSQL ERRORs, release the COPY state through Drop.
    pub fn execute(
        mut self,
        open_source: impl FnOnce(
            &CopyColumnLayout,
            CopyOnError,
        ) -> Result<Box<dyn CopyDatumSource>, CopyError>,
    ) -> Result<u64, CopyError> {
        let state = self.execution.state;
        let bridge = self.execution.bridge();
        // SAFETY: begin prepared a live COPY state; its descriptor and attnums
        // remain valid until this driver ends the state.
        let layout = unsafe {
            CopyColumnLayout::from_descriptor(
                pg::CopyBridge::routed_from_tuple_desc(state),
                pg::CopyBridge::routed_from_attnums(state),
            )
        }?;
        // SAFETY: begin prepared a live COPY state.
        let soft_errors = unsafe { pg::CopyBridge::routed_from_escontext(state) };
        let on_error = if soft_errors.is_null() {
            CopyOnError::Stop
        } else {
            CopyOnError::Ignore
        };
        let mut source = self
            .execution
            .open_source(|| open_source(&layout, on_error))?;
        let mut source_guard =
            TypedSourceGuard::install(source.as_mut(), layout, soft_errors);
        let source_context = source_guard.callback_context();
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::execute_routed_from_typed(
                    state,
                    TypedSourceGuard::callback(),
                    source_context,
                    &bridge,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };

        self.execution.finish(result, || source_guard.finish())
    }
}

pub struct TypedCopyToSpec<'statement, 'parse, 'destination> {
    statement: &'statement CopyStatement<'statement>,
    parse_state: &'parse CopyParseState,
    preparation: CopyToPreparation<'statement, 'parse>,
    options: *mut pg_sys::List,
    destination: &'destination mut dyn CopyTupleDestination,
}

impl<'statement, 'parse, 'destination>
    TypedCopyToSpec<'statement, 'parse, 'destination>
{
    /// # Safety
    ///
    /// The preparation and option list must belong to this statement and parse
    /// state. The destination is synchronously borrowed through finalization.
    pub unsafe fn new(
        statement: &'statement CopyStatement<'statement>,
        parse_state: &'parse CopyParseState,
        preparation: CopyToPreparation<'statement, 'parse>,
        options: *mut pg_sys::List,
        destination: &'destination mut dyn CopyTupleDestination,
    ) -> Self {
        Self {
            statement,
            parse_state,
            preparation,
            options,
            destination,
        }
    }
}

pub struct TypedCopyToDriver<'statement, 'parse, 'destination> {
    state: pg_sys::CopyToState,
    state_ended: bool,
    destination_guard: TypedDestinationGuard<'destination>,
    _preparation: CopyToPreparation<'statement, 'parse>,
    _statement: PhantomData<&'statement pg_sys::CopyStmt>,
    _parse: PhantomData<&'parse CopyParseState>,
}

impl<'statement, 'parse, 'destination>
    TypedCopyToDriver<'statement, 'parse, 'destination>
{
    fn end_state(
        state: pg_sys::CopyToState,
        shutdown: CopyToShutdown,
    ) -> Result<(), PgError> {
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::end_routed_to(state, shutdown);
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }

    pub fn begin(
        spec: TypedCopyToSpec<'statement, 'parse, 'destination>,
    ) -> Result<Self, CopyError> {
        let relation = spec.preparation.relation();
        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_routed_to(
                    spec.parse_state.as_raw(),
                    relation,
                    spec.preparation.raw_query(),
                    spec.preparation.query_relation(),
                    std::ptr::null(),
                    false,
                    None,
                    spec.statement.attlist(),
                    spec.options,
                    true,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;
        let layout = unsafe {
            super::CopyColumnLayout::from_descriptor(
                pg::CopyBridge::routed_to_tuple_desc(state),
                pg::CopyBridge::routed_to_attnums(state),
            )
        };
        // The destination binds its output plan through PostgreSQL catalogs.
        // Capture those errors like the other phases, so the state ends below
        // before this function returns and the preparation is disposed.
        let destination = spec.destination;
        let initialized = layout.and_then(|layout| {
            PgTryBuilder::new(AssertUnwindSafe(|| destination.initialize(&layout)))
                .catch_others(|error| Err(PgError::from_caught(error).into()))
                .execute()
                .map(|()| layout)
        });
        let layout = match initialized {
            Ok(layout) => layout,
            Err(error) => {
                let _ = Self::end_state(state, CopyToShutdown::Abort);
                return Err(error);
            }
        };
        let destination_guard = TypedDestinationGuard::install(destination, layout);

        Ok(Self {
            state,
            state_ended: false,
            destination_guard,
            _preparation: spec.preparation,
            _statement: PhantomData,
            _parse: PhantomData,
        })
    }

    pub fn execute(mut self) -> Result<u64, CopyError> {
        let state = self.state;
        let destination_context = self.destination_guard.callback_context();
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                let processed = pg::CopyBridge::execute_routed_to_typed(
                    state,
                    TypedDestinationGuard::callback(),
                    destination_context,
                );
                // Query exports run ExecutorFinish/ExecutorEnd before the
                // destination commits its upload. Keep COPY progress alive
                // until the writer has produced its footer and final bytes.
                pg::CopyBridge::finish_routed_to(state);
                Ok(processed)
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };

        self.state_ended = true;
        match result {
            Ok(processed) => match self.destination_guard.finish() {
                Ok(bytes_produced) => {
                    unsafe {
                        pg::CopyBridge::update_routed_to_progress(
                            state,
                            bytes_produced,
                        );
                    }
                    Self::end_state(state, CopyToShutdown::Complete)?;
                    Ok(processed)
                }
                Err(error) => {
                    let _ = Self::end_state(state, CopyToShutdown::Abort);
                    Err(error)
                }
            },
            Err(error) => {
                let _ = Self::end_state(state, CopyToShutdown::Abort);
                Err(error.into())
            }
        }
    }
}

impl Drop for TypedCopyToDriver<'_, '_, '_> {
    fn drop(&mut self) {
        if !self.state_ended {
            let _ = Self::end_state(self.state, CopyToShutdown::Abort);
        }
    }
}
