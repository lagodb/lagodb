//! PostgreSQL-encoded COPY FROM for the shared table fallback, using the derived
//! executor. File, PROGRAM and frontend I/O remain PostgreSQL's responsibility.

use std::panic::AssertUnwindSafe;

use pgrx::PgTryBuilder;

use super::from_execution::CopyFromExecution;
use crate::diag::PgError;

use super::context::{CopyFromPreparation, CopyParseState, CopyStatement};
use super::{CopyError, pg};

pub struct RoutedCopyFromDriver<'statement, 'parse> {
    execution: CopyFromExecution,
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
            _preparation: preparation,
        })
    }

    pub fn execute(mut self) -> Result<u64, CopyError> {
        let state = self.execution.state;
        let bridge = self.execution.bridge();
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::execute_routed_from_bytes(state, &bridge))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };
        self.execution.finish(result, || Ok(()))
    }
}
