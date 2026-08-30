//! Diagnostics and PostgreSQL error bridging.

pub mod elog;
pub mod error;

pub use elog::*;
pub use error::*;
pub(crate) use error::{PgReportParts, PgReportableError};
