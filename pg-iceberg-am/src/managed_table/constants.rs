//! Constants used across the catalog module.

/// The access method name for Iceberg tables.
///
/// Stored as a `CStr` so it can be passed directly to PostgreSQL C APIs
/// (e.g. `get_table_am_oid`) and compared byte-for-byte against C strings
/// from parse trees without re-allocating. Using a single source of truth
/// keeps the SQL-level `CREATE ACCESS METHOD <name>` contract aligned with
/// every Rust-side AM lookup and name check.
pub const ICEBERG_AM_NAME: &std::ffi::CStr = c"iceberg";
