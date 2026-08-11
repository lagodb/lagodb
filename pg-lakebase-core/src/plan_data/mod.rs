//! Stage-neutral, `copyObject`-safe PostgreSQL plan-data codec.

mod error;
mod reader;
mod writer;

pub use error::PlanDataError;
pub use reader::PlanDataReader;
pub use writer::PlanDataWriter;
