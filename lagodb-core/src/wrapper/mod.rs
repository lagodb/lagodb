//! PostgreSQL FFI boundary.
//!
//! This module intentionally contains `unsafe` code because PostgreSQL exposes
//! backend-owned objects as raw C pointers. The architectural goal is not to
//! eliminate `unsafe`; it is to keep raw-pointer preconditions in a narrow
//! boundary and expose safe Rust APIs only when ownership and lifetime can be
//! represented by guard or handle types.
//!
//! New wrappers should follow this policy:
//!
//! - raw PG pointer operations stay crate-private and `unsafe`;
//! - public safe APIs should encode ownership/lifetime through types such as
//!   `RelationGuard`, `CatalogRelation`, `CatalogScan`, `SysCacheTuple`, or
//!   `SysCacheTupleCopy`;
//! - public `unsafe fn` is acceptable only when callers must uphold PostgreSQL
//!   invariants that cannot be expressed in the type system.

mod catalog;
mod composite;
mod cstring;
mod json;
mod namespace;
mod relation;
mod syscache;
mod systable;
mod wal;

pub(crate) use cstring::PgOutputCString;

/// PostgreSQL FFI boundary.
///
/// Methods on this type are crate-private implementation details. Public
/// callers should use typed modules such as `catalog`, `handles`, `data`, or
/// `wal`.
pub struct PgWrapper;

// Manually declare CacheRegisterSyscacheCallback because it is not in pg_sys.
unsafe extern "C" {
    pub(crate) fn CacheRegisterSyscacheCallback(
        cacheid: std::os::raw::c_int,
        func: Option<
            unsafe extern "C" fn(
                arg: pgrx::pg_sys::Datum,
                cacheid: std::os::raw::c_int,
                hashvalue: u32,
            ),
        >,
        arg: pgrx::pg_sys::Datum,
    );
}
