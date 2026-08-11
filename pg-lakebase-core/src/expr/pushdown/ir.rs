//! Provider-neutral, owned predicate representation constructed at plan time.

use pgrx::pg_sys;

use crate::expr::contract::PgComparisonOp;

/// Index of a value expression in one [`FilterFragment`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterValueSlotId(usize);

impl FilterValueSlotId {
    #[inline]
    pub(crate) const fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0
    }

    /// Reconstruct a slot id from validated provider plan data.
    #[inline]
    pub fn from_plan_data(index: usize, binding_count: usize) -> Option<Self> {
        (index < binding_count).then_some(Self(index))
    }
}

/// Origin of a value evaluated by PostgreSQL at Begin/ReScan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterValueSourceKind {
    Constant,
    ExternalParam,
    ExecParam,
    OuterValue,
}

impl FilterValueSourceKind {
    #[inline]
    pub const fn is_rescan_stable(self) -> bool {
        matches!(self, Self::Constant | Self::ExternalParam)
    }

    #[inline]
    pub const fn is_static(self) -> bool {
        matches!(self, Self::Constant)
    }
}

/// PostgreSQL type metadata for one scalar expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterTypeMetadata {
    pub type_oid: pg_sys::Oid,
    pub typmod: i32,
    pub collation: pg_sys::Oid,
}

/// Plan-time type metadata for one value slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterValueSlot {
    pub value_type: FilterTypeMetadata,
    pub source_kind: FilterValueSourceKind,
}

/// Scan-relation column identity required by a provider planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterColumn {
    pub rel_oid: pg_sys::Oid,
    pub attno: pg_sys::AttrNumber,
    /// Declared relation-attribute type used to identify storage semantics.
    pub declared_type: FilterTypeMetadata,
    /// Type after PostgreSQL's binary-compatible relabels around the Var.
    pub value_type: FilterTypeMetadata,
}

/// Scalar operand in a filter node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterScalar {
    Column(FilterColumn),
    Value(FilterValueSlotId),
}

/// Complete provider-neutral predicate tree.
#[derive(Debug, Clone)]
pub enum FilterNode {
    Comparison {
        operator: PgComparisonOp,
        left: FilterScalar,
        right: FilterScalar,
    },
    IsNull(FilterScalar),
    IsNotNull(FilterScalar),
    And(Box<[FilterNode]>),
    Or(Box<[FilterNode]>),
    Not(Box<FilterNode>),
}

/// Owned filter tree and its local value-slot table.
#[derive(Debug, Clone)]
pub struct FilterFragment {
    root: FilterNode,
    values: Box<[FilterValueSlot]>,
}

impl FilterFragment {
    pub(crate) fn new(root: FilterNode, values: Vec<FilterValueSlot>) -> Self {
        Self {
            root,
            values: values.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn root(&self) -> &FilterNode {
        &self.root
    }

    #[inline]
    pub fn values(&self) -> &[FilterValueSlot] {
        &self.values
    }

    #[inline]
    pub fn value(&self, id: FilterValueSlotId) -> &FilterValueSlot {
        &self.values[id.index()]
    }
}
