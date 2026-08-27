//! Identity of the Iceberg table access method.
//!
//! This module owns the single source of truth for "what is the Iceberg AM"
//! and exposes it in two complementary forms:
//!
//! * [`IcebergAccessMethod`] — a marker type with associated functions for
//!   matching by AM OID or by AM name. Used in catalog and hook code that has
//!   already resolved (or is in the process of resolving) an AM identifier.
//! * [`IcebergRelationExt`] — an extension trait on `RelationHandle` that lets
//!   callers ask `rel.is_iceberg()` directly, instead of importing a free
//!   function. This keeps the predicate close to the type it inspects.
//!
//! Statement-level probes that look at PostgreSQL parse-tree nodes (e.g.
//! `CreateStmt`) live in the `hooks` layer; this module deliberately does not
//! depend on parse-tree types so that the catalog layer stays decoupled from
//! the utility-hook entry points.

use std::ffi::CStr;

use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::managed_table::constants::ICEBERG_AM_NAME;

/// Iceberg table access-method identity.
pub struct IcebergAccessMethod;

impl IcebergAccessMethod {
    /// Resolve the OID of the Iceberg AM in `pg_am`, or `None` if it has not
    /// been registered yet.
    ///
    /// We deliberately do **not** cache the OID in a Rust static. The hook
    /// library is loaded at backend start (via `shared_preload_libraries`),
    /// but `CREATE ACCESS METHOD iceberg` only runs later as part of
    /// `CREATE EXTENSION lagodb_iceberg`. Caching the first lookup would pin
    /// `InvalidOid` for the rest of the session and silently misclassify all
    /// subsequent relations as non-Iceberg. Likewise, a `DROP EXTENSION` /
    /// `CREATE EXTENSION` cycle in the same backend would leave a stale OID
    /// behind.
    ///
    /// `get_table_am_oid` already goes through PostgreSQL's `AMNAME` syscache,
    /// which has its own invalidation tied to catalog changes; that is the
    /// correct caching layer for AM identity.
    #[inline]
    pub fn oid() -> Option<pg_sys::Oid> {
        // SAFETY: `get_table_am_oid` is a syscache lookup; it is safe to call
        // from any backend context where catalog access is permitted, which
        // covers every site that can reach an Iceberg hook.
        let oid = unsafe { pg_sys::get_table_am_oid(ICEBERG_AM_NAME.as_ptr(), true) };
        (oid != pg_sys::InvalidOid).then_some(oid)
    }

    /// True iff `am` is the Iceberg AM OID and the AM is currently registered.
    #[inline]
    pub fn matches_oid(am: pg_sys::Oid) -> bool {
        Self::oid().is_some_and(|iceberg| am == iceberg)
    }

    /// True iff `name` matches the Iceberg AM name.
    ///
    /// Used in DDL hooks where the AM has been spelled out by the user but
    /// not yet resolved against `pg_am` (e.g. inside `CREATE TABLE ... USING
    /// iceberg`). Callers are responsible for converting from raw FFI
    /// pointers; this signature stays in safe Rust.
    #[inline]
    pub fn matches_name(name: &CStr) -> bool {
        name.to_bytes() == ICEBERG_AM_NAME.to_bytes()
    }
}

/// Predicates that ask whether a relation belongs to the Iceberg AM.
pub trait IcebergRelationExt {
    /// True iff this relation's access method is Iceberg.
    fn is_iceberg(&self) -> bool;
}

impl IcebergRelationExt for RelationHandle<'_> {
    #[inline]
    fn is_iceberg(&self) -> bool {
        IcebergAccessMethod::matches_oid(self.access_method_oid())
    }
}
