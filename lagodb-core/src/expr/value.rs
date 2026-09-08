//! Expression identities and runtime-value metadata shared by query execution
//! and provider pruning.

use pgrx::pg_sys;

use crate::query_contract::ScanId;

/// PostgreSQL type metadata for one scalar expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExprType {
    pub type_oid: pg_sys::Oid,
    pub typmod: i32,
    pub collation: pg_sys::Oid,
}

/// A column occurrence identified by query input, not relation OID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub scan: ScanId,
    pub attno: pg_sys::AttrNumber,
    /// Declared relation-attribute type used to identify storage semantics.
    pub declared_type: ExprType,
    /// Type after PostgreSQL's binary-compatible relabels around the Var.
    pub value_type: ExprType,
}

impl ColumnRef {
    /// Whether two references read the same physical input column.
    #[inline]
    pub fn same_storage_column(self, other: Self) -> bool {
        self.scan == other.scan
            && self.attno == other.attno
            && self.declared_type == other.declared_type
    }

    /// Validate that Arrow lowering may read the declared column without a
    /// value transformation.
    #[inline]
    pub fn has_binary_compatible_value(self) -> bool {
        self.declared_type.type_oid == self.value_type.type_oid
            || unsafe {
                pg_sys::IsBinaryCoercible(
                    self.declared_type.type_oid,
                    self.value_type.type_oid,
                )
            }
    }
}

/// Index of a value evaluated by PostgreSQL before query execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuntimeValueId(usize);

impl RuntimeValueId {
    #[inline]
    pub(crate) const fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn from_index(index: usize) -> Self {
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
pub enum RuntimeValueSource {
    Constant,
    ExternalParam,
    ExecParam,
    OuterValue,
}

impl RuntimeValueSource {
    #[inline]
    pub const fn is_rescan_stable(self) -> bool {
        matches!(self, Self::Constant | Self::ExternalParam)
    }

    #[inline]
    pub const fn is_static(self) -> bool {
        matches!(self, Self::Constant)
    }
}

/// Plan-time type and lifetime metadata for one value slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuntimeValueSpec {
    /// PostgreSQL evaluation type (including a compact array-valued set), or a
    /// narrower comparison-domain type after the normalizer has proved a
    /// direct scalar constant is exactly bindable to it.
    pub value_type: ExprType,
    pub source_kind: RuntimeValueSource,
}

/// Dense metadata table shared by plan codecs and executor binding state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeValueLayout {
    values: Box<[RuntimeValueSpec]>,
}

impl RuntimeValueLayout {
    pub fn new(values: Box<[RuntimeValueSpec]>) -> Self {
        Self { values }
    }

    #[inline]
    pub fn values(&self) -> &[RuntimeValueSpec] {
        &self.values
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline]
    pub fn value(&self, id: RuntimeValueId) -> RuntimeValueSpec {
        self.values[id.index()]
    }
}
