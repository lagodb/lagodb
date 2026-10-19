//! PostgreSQL binary JSONB construction and borrowed traversal.
//!
//! These codecs operate on PostgreSQL's in-memory JSONB representation. Arrow
//! schema mapping and provider-specific storage policy remain in their owning
//! crates and consume this module through the Datum layer.

mod builder;
mod reader;

pub use super::json::JsonbValue;
pub use builder::{JsonbBuilder, JsonbPosition};
pub use reader::{
    JsonbArray, JsonbNumeric, JsonbObject, JsonbReader, JsonbRef, JsonbTemporalParser,
};
