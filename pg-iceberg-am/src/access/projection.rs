//! The provider → scan column-projection descriptor.
//!
//! [`Projection`] is the resolved column projection handed from the
//! CustomScan provider's `resolve_projection` (Layer 2) to the scan layer
//! (Layer 3). It carries the selected columns in *scan order*, each as a
//! `(attno, name)` pair so neither the scan layer nor the converter has to
//! re-derive one from the other:
//!
//! - `name` drives `builder.select(...)` (the Iceberg field to read).
//! - `attno` drives the [`ColumnPlan`](super::conversion) `dest = attno - 1`
//!   mapping (where the decoded value lands in the PG `Row`).
//!
//! Select-all is represented by [`ScanSpec`](super::scan::ScanSpec) holding
//! `Option<Projection>` = `None`, **not** by an empty `Projection`: an empty
//! `Projection` would be a zero-column scan, which v1 never constructs (the
//! `count(*)` policy maps an empty subset to a single-column `Projection`).
//!
//! This module owns no Arrow-column-to-slot-position arithmetic — that lives
//! exclusively in `ColumnPlan` (Requirement 8.1). `Projection` only records
//! the resolved `(attno, name)` pairs; the converter turns them into `dest`
//! indices.

use pgrx::pg_sys;

/// One selected column: its 1-based PG attribute number and the Iceberg
/// field name resolved from it.
///
/// `attno` becomes `dest = attno - 1` inside `ColumnPlan`; `name` is what the
/// Iceberg scan builder selects. Both are owned so the descriptor outlives
/// the catalog lookups that produced it.
#[derive(Debug, Clone)]
pub(crate) struct ProjectedName {
    /// 1-based PG attribute number of a live (non-dropped) user column.
    pub(crate) attno: pg_sys::AttrNumber,
    /// Iceberg field name resolved from `attno`.
    pub(crate) name: String,
}

impl ProjectedName {
    pub(crate) fn new(attno: pg_sys::AttrNumber, name: String) -> Self {
        Self { attno, name }
    }
}

/// A resolved column projection: the selected columns in scan order.
///
/// Invariant (enforced by the provider's `resolve_projection`): a `Projection`
/// v1 builds always has **≥ 1** column. Select-all is represented by a `None`
/// projection on [`ScanSpec`](super::scan::ScanSpec), never by an empty
/// `Projection`.
#[derive(Debug, Clone)]
pub(crate) struct Projection {
    columns: Vec<ProjectedName>,
}

impl Projection {
    /// Build a projection from the resolved `(attno, name)` pairs in scan
    /// order. Callers (the provider) guarantee `columns` is non-empty.
    pub(crate) fn new(columns: Vec<ProjectedName>) -> Self {
        Self { columns }
    }

    /// The selected columns in scan order. Consumed by the converter to build
    /// the projected `ColumnPlan` (each pair's `attno` → `dest`).
    pub(crate) fn columns(&self) -> &[ProjectedName] {
        &self.columns
    }

    /// The selected column names in scan order, for `builder.select(...)`.
    ///
    /// The Iceberg `select(names)` call preserves the passed name order all
    /// the way to the produced Arrow batch's column order, which is exactly
    /// the order the projected `ColumnPlan` entries are built in — so Arrow
    /// column `j` always lines up with `ColumnPlan.entries[j]`.
    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|c| c.name.as_str())
    }
}
