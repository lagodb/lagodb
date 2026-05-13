//! Per-connection session state: handle table and connection context.
//!
//! These types are shared between [`crate::connection`] (connection pipeline) and [`crate::service`]
//! (command dispatch), so they live at the crate root rather than inside either consumer.

pub mod handle_table;
mod context;

pub use context::StorageContext;
pub use handle_table::HandleTable;
