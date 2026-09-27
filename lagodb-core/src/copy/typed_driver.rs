//! PostgreSQL-derived COPY drivers for provider-native Datum and slot paths.

use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

use super::context::{
    CopyFromPreparation, CopyParseState, CopyStatement, CopyToPreparation,
};
use super::pg::CopyToShutdown;
use super::route::CopyTargetRoute;
use super::typed_callback::{
    TypedDestinationGuard, TypedSourceGuard, destination_callback, source_callback,
};
use super::{CopyDatumSource, CopyError, CopyTupleDestination, pg};

pub struct TypedCopyFromSpec<'statement, 'parse, 'source> {
    statement: &'statement CopyStatement<'statement>,
    parse_state: &'parse CopyParseState,
    preparation: CopyFromPreparation<'statement, 'parse>,
    options: *mut pg_sys::List,
    source: &'source mut dyn CopyDatumSource,
}

impl<'statement, 'parse, 'source> TypedCopyFromSpec<'statement, 'parse, 'source> {
    /// # Safety
    ///
    /// The preparation and option list must belong to this statement and parse
    /// state. The source is synchronously borrowed until execution completes.
    pub unsafe fn new(
        statement: &'statement CopyStatement<'statement>,
        parse_state: &'parse CopyParseState,
        preparation: CopyFromPreparation<'statement, 'parse>,
        options: *mut pg_sys::List,
        source: &'source mut dyn CopyDatumSource,
    ) -> Self {
        Self {
            statement,
            parse_state,
            preparation,
            options,
            source,
        }
    }
}

pub struct TypedCopyFromDriver<'statement, 'parse, 'source> {
    state: pg_sys::CopyFromState,
    target_route: CopyTargetRoute,
    state_ended: bool,
    source_guard: TypedSourceGuard<'source>,
    _preparation: CopyFromPreparation<'statement, 'parse>,
    _statement: PhantomData<&'statement pg_sys::CopyStmt>,
    _parse: PhantomData<&'parse CopyParseState>,
}

impl<'statement, 'parse, 'source> TypedCopyFromDriver<'statement, 'parse, 'source> {
    fn end_state(state: pg_sys::CopyFromState) {
        unsafe { pg::CopyBridge::end_routed_from(state) }
    }

    pub fn begin(
        spec: TypedCopyFromSpec<'statement, 'parse, 'source>,
    ) -> Result<Self, CopyError> {
        let relation = spec.preparation.relation();
        let target_route = spec.preparation.target_route();
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
        let layout = unsafe {
            super::CopyColumnLayout::from_descriptor(
                pg::CopyBridge::routed_from_tuple_desc(state),
                pg::CopyBridge::routed_from_attnums(state),
            )
        };
        let source_guard = match layout
            .and_then(|layout| TypedSourceGuard::install(spec.source, layout))
        {
            Ok(guard) => guard,
            Err(error) => {
                Self::end_state(state);
                return Err(error);
            }
        };

        Ok(Self {
            state,
            target_route,
            state_ended: false,
            source_guard,
            _preparation: spec.preparation,
            _statement: PhantomData,
            _parse: PhantomData,
        })
    }

    pub fn execute(mut self) -> Result<u64, CopyError> {
        let state = self.state;
        let provider_owned_partitioned_table =
            self.target_route.provider_owned_partitioned_table();
        let source_context = self.source_guard.callback_context();
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::execute_routed_from_typed(
                    state,
                    source_callback(),
                    source_context,
                    provider_owned_partitioned_table,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };

        self.state_ended = true;
        match result {
            Ok(processed) => {
                Self::end_state(state);
                self.source_guard.finish()?;
                Ok(processed)
            }
            Err(error) => {
                Self::end_state(state);
                Err(error.into())
            }
        }
    }
}

impl Drop for TypedCopyFromDriver<'_, '_, '_> {
    fn drop(&mut self) {
        if !self.state_ended {
            Self::end_state(self.state);
        }
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
        let target_route = spec.preparation.target_route()?;
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
                    target_route.provider_owned_partitioned_table(),
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
        let destination_guard = match layout.and_then(|layout| {
            TypedDestinationGuard::install(spec.destination, layout)
        }) {
            Ok(guard) => guard,
            Err(error) => {
                let _ = Self::end_state(state, CopyToShutdown::Abort);
                return Err(error);
            }
        };

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
                    destination_callback(),
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
