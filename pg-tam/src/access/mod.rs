//! Table Access Method implementation modules
//!
//! This module groups together the core implementation modules for different
//! aspects of the Table Access Method interface:
//! - `ddl`: DDL operations
//! - `dml`: Data modification (INSERT/UPDATE/DELETE)
//! - `index`: Index access
//! - `relation`: Relation-level operations
//! - `scan`: Scan operations

pub mod ddl;
pub mod dml;
pub mod index;
pub mod relation;
pub mod scan;
pub mod xact;
