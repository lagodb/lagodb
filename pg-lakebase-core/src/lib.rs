#![allow(clippy::too_many_arguments)]

//! pg-lakebase-core: A framework for building PostgreSQL Table Access Methods in Rust
//!
//! This library provides a safe, ergonomic API for implementing custom table access
//! methods for PostgreSQL using the pgrx framework.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use pg_lakebase_core::prelude::*;
//!
//! #[pg_table_am(
//!     version = "0.1.0",
//!     author = "Your Name",
//!     website = "https://github.com/your/repo"
//! )]
//! pub struct MyTableAm;
//!
//! impl TableAccessMethod for MyTableAm {
//!     type ScanSession = MyScan;
//!     type IndexFetchSession = MyIndexFetch;
//!     type DmlSession = MyModify;
//! }
//! ```

/// Core trait definitions for table access methods
pub mod api;

/// Safe wrapper types for PostgreSQL FFI types
pub mod handles;

/// PostgreSQL tuple value abstractions (Cell, Row)
pub mod tuple;

/// Table access implementation modules (scan, index, dml, ddl, relation)
pub mod access;

/// Registration logic for Table Access Method routines
pub mod registry;

/// PostgreSQL catalog option extraction, persistence, and caches
pub mod options;

/// PostgreSQL hooks framework (utility, object access, etc.)
pub mod hooks;

/// Custom WAL Resource Manager framework
pub mod wal;

/// ResourceOwner-scoped cleanup callbacks.  Distinct from transaction events.
pub mod resource;

/// Transaction lifecycle callbacks.  Distinct from ResourceOwner cleanup.
pub mod transaction;

/// Helper functions and diagnostics
pub mod diag;

/// Internal wrapper for PostgreSQL functions
mod wrapper;

/// Catalog access and caching
pub mod catalog;

/// PostgreSQL background worker modules.
pub mod worker;

/// The prelude includes all necessary imports to make pg_lakebase_core work
pub mod prelude {
    pub use crate::api::*;
    pub use crate::diag::{
        PgError, PgErrorReport, PgErrorSource, PgReportError, ReportableError,
        SqlStateError,
    };
    pub use crate::handles::*;
    pub use crate::pg_table_am;
    pub use crate::tuple::*;
}

use pgrx::AllocatedByPostgres;
use pgrx::prelude::*;

/// PgBox'ed `TableAmRoutine`, used in [`am_routine`](api::TableAccessMethod::am_routine)
pub type TableAmRoutine<A = AllocatedByPostgres> = PgBox<pg_sys::TableAmRoutine, A>;

/// Procedural macro for generating table access method boilerplate
pub use pg_lakebase_macros::pg_table_am;
