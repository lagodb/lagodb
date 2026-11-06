//! Diagnostics and PostgreSQL error bridging.

mod elog;
mod error;

pub use elog::*;
pub use error::*;
pub(crate) use error::{PgReportParts, PgReportableError};
