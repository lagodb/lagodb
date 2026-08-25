//! REST foreign-table scan planning and execution.

mod cursor;
mod mutation;
mod planner;
mod private;
mod state;

pub(crate) use mutation::ForeignMutationScan;
