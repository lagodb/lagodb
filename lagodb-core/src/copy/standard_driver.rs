//! PostgreSQL-encoded COPY using the derived executors.
//! FROM supports the shared table fallback; TO retains provider-owned partition
//! binding. File, PROGRAM and frontend I/O remain PostgreSQL's responsibility.

use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use super::from_execution::CopyFromExecution;
use crate::diag::PgError;

use super::context::{
    CopyFromPreparation, CopyParseState, CopyStatement, CopyToPreparation,
};
use super::pg::CopyToShutdown;
use super::route::{CopyTargetRoute, PartitionedTableCopyToPreparation};
use super::{CopyError, pg};

pub struct RoutedCopyFromDriver<'statement, 'parse> {
    execution: CopyFromExecution,
    target_route: CopyTargetRoute,
    _preparation: CopyFromPreparation<'statement, 'parse>,
}

impl<'statement, 'parse> RoutedCopyFromDriver<'statement, 'parse> {
    /// # Safety
    ///
    /// `preparation` must belong to this statement and parse state. Its table
    /// binding already established the execution-locked target's ownership.
    pub unsafe fn begin(
        statement: &'statement CopyStatement<'statement>,
        parse_state: &'parse CopyParseState,
        preparation: CopyFromPreparation<'statement, 'parse>,
    ) -> Result<Self, CopyError> {
        let target_route = preparation.target_route();
        let relation = preparation.relation();
        let filename = statement
            .filename()
            .map_or(std::ptr::null(), |filename| filename.as_ptr());
        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_routed_from(
                    parse_state.as_raw(),
                    relation,
                    preparation.where_clause(),
                    filename,
                    statement.is_program(),
                    None,
                    statement.attlist(),
                    statement.options(),
                    false,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;
        Ok(Self {
            execution: CopyFromExecution::new(state),
            target_route,
            _preparation: preparation,
        })
    }

    pub fn execute(mut self) -> Result<u64, CopyError> {
        let state = self.execution.state;
        let bridge = self.execution.bridge();
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::execute_routed_from_bytes(
                    state,
                    self.target_route.provider_owned_partitioned_table(),
                    &bridge,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };
        self.execution.finish(result, || Ok(()))
    }
}

pub struct RoutedCopyToDriver<'statement, 'parse> {
    state: pg_sys::CopyToState,
    state_ended: bool,
    _preparation: CopyToPreparation<'statement, 'parse>,
}

impl<'statement, 'parse> RoutedCopyToDriver<'statement, 'parse> {
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

    /// # Safety
    ///
    /// `preparation` must belong to this statement and parse state. Its table
    /// binding covers relation mode and named targets rewritten for RLS.
    pub unsafe fn begin(
        statement: &'statement CopyStatement<'statement>,
        parse_state: &'parse CopyParseState,
        preparation: PartitionedTableCopyToPreparation<'statement, 'parse>,
    ) -> Result<Self, CopyError> {
        let preparation = preparation.into_preparation();
        let relation = preparation.relation();
        let filename = statement
            .filename()
            .map_or(std::ptr::null(), |filename| filename.as_ptr());
        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_routed_to(
                    parse_state.as_raw(),
                    relation,
                    preparation.raw_query(),
                    preparation.query_relation(),
                    filename,
                    statement.is_program(),
                    None,
                    statement.attlist(),
                    statement.options(),
                    true,
                    false,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;
        Ok(Self {
            state,
            state_ended: false,
            _preparation: preparation,
        })
    }

    pub fn execute(mut self) -> Result<u64, CopyError> {
        let state = self.state;
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::execute_routed_to_bytes(state))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };
        self.state_ended = true;
        match result {
            Ok(processed) => {
                Self::end_state(state, CopyToShutdown::Complete)?;
                Ok(processed)
            }
            Err(error) => {
                let _ = Self::end_state(state, CopyToShutdown::Abort);
                Err(error.into())
            }
        }
    }
}

impl Drop for RoutedCopyToDriver<'_, '_> {
    fn drop(&mut self) {
        if !self.state_ended {
            let _ = Self::end_state(self.state, CopyToShutdown::Abort);
        }
    }
}
