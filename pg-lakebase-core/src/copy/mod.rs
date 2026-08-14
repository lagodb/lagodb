//! PostgreSQL COPY execution primitives shared by utility consumers.
//!
//! This module owns the PostgreSQL-facing part of COPY. It deliberately does
//! not know about an object-store provider or a file format. A consumer chooses
//! a format and supplies PostgreSQL's documented COPY source/destination
//! callback; the drivers keep PostgreSQL's parser, executor, permission,
//! trigger, partition, RLS, and FDW semantics in charge of row execution.

mod context;
mod driver;
mod error;
mod io;
mod layout;
mod pg;
mod raw_fields;
mod row;
mod scan;

pub use context::{
    CopyCompletion, CopyContext, CopyFromPreparation, CopyOption, CopyOptionIter,
    CopyOptionView, CopyParseState, CopyProcessContext, CopyStatement,
    CopyToPreparation,
};
pub use driver::{CopyFromDriver, CopyFromSpec, CopyToDriver, CopyToSpec};
pub use error::CopyError;
pub use io::{CopyDataDestination, CopyDataSource};
pub use layout::{CopyColumn, CopyColumnLayout};
pub use row::CopyRowEncoder;
pub use raw_fields::{
    CopyRawFieldReader, CopyRawFields, CopyRawRecord, CopyTextInputValidator,
};
pub use scan::{CopyDocumentSource, CopyFromScan};
