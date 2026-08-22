//! The provider → scan column-projection descriptor.
//!
//! [`Projection`] is the resolved column projection handed from a PostgreSQL
//! scan adapter (CustomScan or FDW) to the shared Iceberg scan layer. It carries
//! selected columns in base-schema read order, each as a `(base attno, scan
//! destination)` pair. The scan layer binds the attno to an Iceberg field id
//! exactly once through `RelationFieldMap`, then all execution paths use field
//! ids instead of names.

use pgrx::pg_sys;

/// One selected column: its 1-based PG attribute number and destination in the
/// scan tuple.
///
/// `attno` identifies the PostgreSQL source attribute; it is later resolved to
/// an Iceberg field id by `RelationFieldMap`. `destination` identifies the
/// actual raw scan-slot cell.
#[derive(Debug, Clone)]
pub(crate) struct ProjectedField {
    /// 1-based PG attribute number of a live (non-dropped) user column.
    pub(crate) attno: pg_sys::AttrNumber,
    /// Zero-based destination in the actual executor scan slot.
    pub(crate) destination: usize,
}

impl ProjectedField {
    pub(crate) fn new(attno: pg_sys::AttrNumber, destination: usize) -> Self {
        Self { attno, destination }
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
    columns: Vec<ProjectedField>,
}

impl Projection {
    /// Build a projection from resolved source/destination entries in storage
    /// read order.
    pub(crate) fn new(columns: Vec<ProjectedField>) -> Self {
        Self { columns }
    }

    /// Selected columns in storage read order. Each entry independently
    /// carries its compact scan-slot destination.
    pub(crate) fn columns(&self) -> &[ProjectedField] {
        &self.columns
    }
}
