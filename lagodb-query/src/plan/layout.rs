//! Physical PostgreSQL output contract for a query fragment.

use pgrx::pg_sys;

use super::OutputId;

/// Metadata for one physical PostgreSQL output slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTupleSlot {
    output: OutputId,
    type_oid: pg_sys::Oid,
    typmod: i32,
    collation: pg_sys::Oid,
    nullable: bool,
}

impl QueryTupleSlot {
    pub const fn new(
        output: OutputId,
        type_oid: pg_sys::Oid,
        typmod: i32,
        collation: pg_sys::Oid,
        nullable: bool,
    ) -> Self {
        Self {
            output,
            type_oid,
            typmod,
            collation,
            nullable,
        }
    }

    #[inline]
    pub const fn output(&self) -> OutputId {
        self.output
    }

    #[inline]
    pub const fn type_oid(&self) -> pg_sys::Oid {
        self.type_oid
    }

    #[inline]
    pub const fn typmod(&self) -> i32 {
        self.typmod
    }

    #[inline]
    pub const fn collation(&self) -> pg_sys::Oid {
        self.collation
    }

    #[inline]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// Dense physical output layout indexed by PostgreSQL slot position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTupleLayout {
    slots: Box<[QueryTupleSlot]>,
}

impl QueryTupleLayout {
    pub(crate) fn scalar_count(output: OutputId, type_oid: pg_sys::Oid) -> Self {
        Self {
            slots: Box::new([QueryTupleSlot {
                output,
                type_oid,
                typmod: -1,
                collation: pg_sys::InvalidOid,
                nullable: false,
            }]),
        }
    }

    pub fn from_slots(slots: Box<[QueryTupleSlot]>) -> Self {
        // PostgreSQL permits a zero-width SELECT target. Arrow preserves its
        // batch row count even when the projection has no physical columns.
        Self { slots }
    }

    #[inline]
    pub fn slots(&self) -> &[QueryTupleSlot] {
        &self.slots
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}
