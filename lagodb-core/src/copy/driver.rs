use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

use super::context::{
    CopyFromPreparation, CopyParseState, CopyStatement, CopyToPreparation,
};
use super::io::{
    DestinationGuard, SourceGuard, destination_callback, source_callback,
};
use super::pg::CopyToShutdown;
use super::route::CopyTargetRoute;
use super::{CopyDataDestination, CopyDataSource, CopyError, pg};

/// Parameters for a standard PostgreSQL COPY FROM execution.
pub struct CopyFromSpec<'statement, 'parse, 'source> {
    statement: &'statement CopyStatement<'statement>,
    parse_state: &'parse CopyParseState,
    _preparation: CopyFromPreparation<'statement, 'parse>,
    options: *mut pg_sys::List,
    data_source: &'source mut dyn CopyDataSource,
}

impl<'statement, 'parse, 'source> CopyFromSpec<'statement, 'parse, 'source> {
    /// # Safety
    ///
    /// `preparation` must have been created by
    /// [`super::context::CopyContext::prepare_from`] for this statement and
    /// parse state.
    /// `options` must be the original option list or a list produced by
    /// [`CopyOptionView::without_names`]. PostgreSQL's `CopyFrom` uses the
    /// preparation's range table and permission metadata.
    pub unsafe fn new(
        statement: &'statement CopyStatement<'statement>,
        parse_state: &'parse CopyParseState,
        preparation: CopyFromPreparation<'statement, 'parse>,
        options: *mut pg_sys::List,
        data_source: &'source mut dyn CopyDataSource,
    ) -> Self {
        Self {
            statement,
            parse_state,
            _preparation: preparation,
            options,
            data_source,
        }
    }
}

/// RAII wrapper around PostgreSQL's `CopyFromState`.
pub struct CopyFromDriver<'statement, 'parse, 'source> {
    state: pg_sys::CopyFromState,
    target_route: CopyTargetRoute,
    finished: bool,
    _source_guard: SourceGuard<'source>,
    _preparation: CopyFromPreparation<'statement, 'parse>,
    _statement_lifetime: PhantomData<&'statement pg_sys::CopyStmt>,
    _parse_lifetime: PhantomData<&'parse CopyParseState>,
}

impl<'statement, 'parse, 'source> CopyFromDriver<'statement, 'parse, 'source> {
    fn end_state(state: pg_sys::CopyFromState) {
        unsafe { pg::CopyBridge::end_routed_from(state) }
    }

    /// Starts PostgreSQL's COPY FROM parser and executor state.
    ///
    /// The caller must use [`Self::execute`] to finish the PostgreSQL COPY
    /// state. Execution errors are captured, the opaque PostgreSQL state is
    /// ended, and the original error is returned to the outer utility report
    /// boundary.
    ///
    /// # Safety
    ///
    /// The [`CopyFromSpec`] must satisfy the lifetime and PostgreSQL-state
    /// invariants documented by [`CopyFromSpec::new`].
    pub unsafe fn begin(
        spec: CopyFromSpec<'statement, 'parse, 'source>,
    ) -> Result<Self, CopyError> {
        let statement = spec.statement;
        let pstate = spec.parse_state.as_raw();
        let preparation = spec._preparation;
        let relation = preparation.relation();
        let where_clause = preparation.where_clause();
        let source_guard = SourceGuard::install(spec.data_source);
        let attlist = statement.attlist();
        let options = spec.options;
        let target_route = preparation.target_route();

        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(move || {
                Ok(pg::CopyBridge::begin_routed_from(
                    pstate,
                    relation,
                    where_clause,
                    // The provider's object URI is not a PostgreSQL server
                    // file path. A non-null filename would make BeginCopyFrom
                    // open that path before it uses the source callback.
                    std::ptr::null(),
                    false,
                    source_callback(),
                    attlist,
                    options,
                    false,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;

        Ok(Self {
            state,
            target_route,
            finished: false,
            _source_guard: source_guard,
            _preparation: preparation,
            _statement_lifetime: PhantomData,
            _parse_lifetime: PhantomData,
        })
    }

    pub fn execute(mut self) -> Result<u64, CopyError> {
        let state = self.state;
        let provider_owned_partitioned_table =
            self.target_route.provider_owned_partitioned_table();
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(move || {
                Ok(pg::CopyBridge::execute_routed_from_bytes(
                    state,
                    provider_owned_partitioned_table,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };
        match result {
            Ok(processed) => {
                self.finished = true;
                Self::end_state(state);
                Ok(processed)
            }
            Err(error) => {
                // The error has been caught by PgTryBuilder, so PostgreSQL's
                // normal longjmp cleanup will not release this opaque state.
                // EndCopyFrom is required here to release its COPY context;
                self.finished = true;
                Self::end_state(state);
                Err(error.into())
            }
        }
    }
}

impl Drop for CopyFromDriver<'_, '_, '_> {
    fn drop(&mut self) {
        if !self.finished {
            // Normal callers use execute(); Drop covers Rust-side early
            // returns before execution begins.
            Self::end_state(self.state);
        }
    }
}

/// Parameters for a standard PostgreSQL COPY TO execution.
pub struct CopyToSpec<'statement, 'parse, 'destination> {
    statement: &'statement CopyStatement<'statement>,
    parse_state: &'parse CopyParseState,
    _preparation: CopyToPreparation<'statement, 'parse>,
    options: *mut pg_sys::List,
    data_destination: &'destination mut dyn CopyDataDestination,
}

impl<'statement, 'parse, 'destination> CopyToSpec<'statement, 'parse, 'destination> {
    /// # Safety
    ///
    /// `preparation` must have been created by
    /// [`super::context::CopyContext::prepare_to`] for this statement and
    /// parse state.
    /// `options` must be the original option list or a list produced by
    /// [`CopyOptionView::without_names`].
    pub unsafe fn new(
        statement: &'statement CopyStatement<'statement>,
        parse_state: &'parse CopyParseState,
        preparation: CopyToPreparation<'statement, 'parse>,
        options: *mut pg_sys::List,
        data_destination: &'destination mut dyn CopyDataDestination,
    ) -> Self {
        Self {
            statement,
            parse_state,
            _preparation: preparation,
            options,
            data_destination,
        }
    }
}

/// RAII wrapper around PostgreSQL's `CopyToState`.
pub struct CopyToDriver<'statement, 'parse, 'destination> {
    state: pg_sys::CopyToState,
    finished: bool,
    _destination_guard: DestinationGuard<'destination>,
    _preparation: CopyToPreparation<'statement, 'parse>,
    _statement_lifetime: PhantomData<&'statement pg_sys::CopyStmt>,
    _parse_lifetime: PhantomData<&'parse CopyParseState>,
}

impl<'statement, 'parse, 'destination>
    CopyToDriver<'statement, 'parse, 'destination>
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

    /// Starts PostgreSQL's COPY TO executor state.
    ///
    /// # Safety
    ///
    /// The [`CopyToSpec`] must satisfy the lifetime and PostgreSQL-state
    /// invariants documented by [`CopyToSpec::new`].
    pub unsafe fn begin(
        spec: CopyToSpec<'statement, 'parse, 'destination>,
    ) -> Result<Self, CopyError> {
        let statement = spec.statement;
        let pstate = spec.parse_state.as_raw();
        let preparation = spec._preparation;
        let relation = preparation.relation();
        let raw_query = preparation.raw_query();
        let query_relation = preparation.query_relation();
        let mut destination_guard = DestinationGuard::install(spec.data_destination);
        let attlist = statement.attlist();
        let options = spec.options;
        let target_route = preparation.target_route()?;

        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(move || {
                Ok(pg::CopyBridge::begin_routed_to(
                    pstate,
                    relation,
                    raw_query,
                    query_relation,
                    // The provider's object URI is not a PostgreSQL server
                    // file path. A non-null filename would select file I/O
                    // instead of the destination callback.
                    std::ptr::null(),
                    false,
                    destination_callback(),
                    attlist,
                    options,
                    target_route.provider_owned_partitioned_table(),
                    false,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;

        let layout = unsafe {
            super::layout::CopyColumnLayout::from_descriptor(
                pg::CopyBridge::routed_to_tuple_desc(state),
                pg::CopyBridge::routed_to_attnums(state),
            )
        };
        if let Err(error) =
            layout.and_then(|layout| destination_guard.initialize(&layout))
        {
            let _ = Self::end_state(state, CopyToShutdown::Abort);
            return Err(error);
        }

        Ok(Self {
            state,
            finished: false,
            _destination_guard: destination_guard,
            _preparation: preparation,
            _statement_lifetime: PhantomData,
            _parse_lifetime: PhantomData,
        })
    }

    pub fn execute(mut self) -> Result<u64, CopyError> {
        let state = self.state;
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(move || {
                Ok(pg::CopyBridge::execute_routed_to_bytes(state))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };
        match result {
            Ok(processed) => {
                self.finished = true;
                Self::end_state(state, CopyToShutdown::Complete)?;
                Ok(processed)
            }
            Err(error) => {
                // Release COPY storage without running a failed executor's
                // shutdown callbacks. Transaction abort owns its resources;
                // preserve the original error if COPY cleanup also fails.
                self.finished = true;
                let _ = Self::end_state(state, CopyToShutdown::Abort);
                Err(error.into())
            }
        }
    }
}

impl Drop for CopyToDriver<'_, '_, '_> {
    fn drop(&mut self) {
        if !self.finished {
            // Drop is a best-effort guard for Rust-side early returns. Keep
            // PostgreSQL cleanup under the same FFI error boundary as the
            // normal EndCopyTo path; a PG ERROR must not escape Drop.
            let _ = Self::end_state(self.state, CopyToShutdown::Abort);
        }
    }
}
