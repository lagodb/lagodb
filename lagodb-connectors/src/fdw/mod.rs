//! PostgreSQL FDW adapter for the LagoDB connector.

mod ddl;
mod filter;
mod format_selection;
mod maintenance;
mod modify;
mod options;
mod provider;
mod scan;

pub(crate) use format_selection::ResolvedForeignRelation;
pub(crate) use options::{ResolvedTableOptions, resolve_table_options};
pub(crate) use provider::Lakebase;

pub(crate) fn register_ddl_hooks() {
    ddl::register();
}
