//! PostgreSQL background worker modules.
//!
//! Each submodule owns one type of background worker: registration, GUCs,
//! lifecycle, and logging.

pub mod storage;
