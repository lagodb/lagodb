//! PostgreSQL FDW adapter for the LagoDB connector.

mod ddl;
mod filter;
mod format_adapters;
mod maintenance;
mod modify;
mod options;
mod plan_private;
mod provider;
mod relation;
mod scan;

pub(crate) use options::{ResolvedTableOptions, resolve_table_options};
pub(crate) use plan_private::PlannedFormat;
pub(crate) use provider::LagodbConnectors;
pub(crate) use relation::ResolvedForeignRelation;

pub(crate) fn register_ddl_hooks() {
    ddl::register();
}
