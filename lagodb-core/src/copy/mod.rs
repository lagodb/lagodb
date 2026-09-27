//! PostgreSQL COPY execution primitives shared by utility consumers.
//!
//! This module owns the PostgreSQL-facing part of COPY. It deliberately does
//! not know about an object-store provider or a file format. A consumer chooses
//! a format and supplies PostgreSQL's documented COPY source/destination
//! callback; the drivers keep PostgreSQL's parser, executor, permission,
//! trigger, partition, RLS, and FDW semantics in charge of row execution.

mod context;
mod datum;
mod driver;
mod endpoint;
mod error;
mod io;
mod layout;
mod pg;
mod raw_fields;
mod route;
mod row;
mod scan;
mod standard_driver;
mod typed_callback;
mod typed_driver;
mod typed_io;

pub use context::{
    CopyCompletion, CopyContext, CopyFromPreparation, CopyOption, CopyOptionIter,
    CopyOptionView, CopyParseState, CopyProcessContext, CopyStatement,
    CopyToPreparation,
};
pub use datum::CopyDatumCoercion;
pub use driver::{CopyFromDriver, CopyFromSpec, CopyToDriver, CopyToSpec};
pub use endpoint::CopyEndpoint;
pub use error::CopyError;
pub use io::{CopyDataDestination, CopyDataSource};
pub use layout::{CopyColumn, CopyColumnLayout};
pub use raw_fields::{
    CopyRawFieldReader, CopyRawFields, CopyRawRecord, CopyTextInputValidator,
};
pub use route::{
    CopyTargetProbe, PartitionedTableCopyFromPreparation,
    PartitionedTableCopyToPreparation,
};
pub use row::CopyRowEncoder;
pub use scan::{CopyDocumentSource, CopyFromScan};
pub use standard_driver::{RoutedCopyFromDriver, RoutedCopyToDriver};
pub use typed_driver::{
    TypedCopyFromDriver, TypedCopyFromSpec, TypedCopyToDriver, TypedCopyToSpec,
};
pub use typed_io::{
    CopyDatumSource, CopyInputColumn, CopyInputColumns, CopyInputRow,
    CopyOutputDatum, CopyOutputDatums, CopyOutputRow, CopyRowOutcome,
    CopyRowRejection, CopyTupleDestination,
};
