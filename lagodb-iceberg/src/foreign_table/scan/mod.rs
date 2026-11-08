//! REST foreign-table scan planning and execution.

mod cursor;
mod mutation;
mod plan_private;
mod planner;
mod state;

pub(crate) use mutation::ForeignMutationScan;
