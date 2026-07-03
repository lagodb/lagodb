//! The provider → scan column-projection descriptor.
//!
//! [`Projection`] is the resolved column projection handed from the
//! CustomScan provider's `resolve_projection` (Layer 2) to the scan layer
//! (Layer 3). It carries selected columns in base-schema read order, each as a
//! `(base attno, scan destination, name)` triple so neither the scan layer nor
//! the converter has to
//! re-derive one from the other:
//!
//! - `name` drives `builder.select(...)` (the Iceberg field to read).
//! - `destination` is the zero-based custom scan-slot cell where the decoded
//!   value lands.

use pgrx::pg_sys;

/// One selected column: its 1-based PG attribute number and the Iceberg
/// field name resolved from it.
///
/// `attno` identifies the Iceberg source field; `destination` identifies the
/// actual raw scan-slot cell. `name` is owned once at scan construction.
#[derive(Debug, Clone)]
pub(crate) struct ProjectedName {
    /// 1-based PG attribute number of a live (non-dropped) user column.
    pub(crate) attno: pg_sys::AttrNumber,
    /// Zero-based destination in the actual executor scan slot.
    pub(crate) destination: usize,
    /// Iceberg field name resolved from `attno`.
    pub(crate) name: String,
}

impl ProjectedName {
    pub(crate) fn new(
        attno: pg_sys::AttrNumber,
        destination: usize,
        name: String,
    ) -> Self {
        Self {
            attno,
            destination,
            name,
        }
    }
}

/// A resolved column projection in stable base-schema read order.
///
/// Select-all is represented by a `None` projection on
/// [`ScanSpec`](super::scan::ScanSpec). An empty projection is valid for a
/// Modify identity-only scan, where Iceberg metadata columns still drive one
/// output row but no business column is decoded.
#[derive(Debug, Clone)]
pub(crate) struct Projection {
    columns: Vec<ProjectedName>,
}

impl Projection {
    /// Build a projection from resolved source/destination/name entries in
    /// storage read order.
    pub(crate) fn new(columns: Vec<ProjectedName>) -> Self {
        Self { columns }
    }

    /// Selected columns in storage read order. Each entry independently carries
    /// its compact scan-slot destination.
    pub(crate) fn columns(&self) -> &[ProjectedName] {
        &self.columns
    }

    /// Selected column names in storage read order, for `builder.select(...)`.
    ///
    /// Iceberg preserves this request order in the produced Arrow batch;
    /// `ColumnMapping` binds source indices against the same sequence.
    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|c| c.name.as_str())
    }
}
