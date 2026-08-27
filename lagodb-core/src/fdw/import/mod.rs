//! Optional `IMPORT FOREIGN SCHEMA` capability.

mod callbacks;
mod contract;
mod error;

pub(crate) use callbacks::import_foreign_schema;
pub use contract::{FdwImportSchema, ForeignImportSchemaContext};
pub use error::ForeignImportError;
