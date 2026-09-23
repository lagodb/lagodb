//! Borrowed access to PostgreSQL's opaque `PartitionKeyData`.
//!
//! `pgrx-pg-sys` represents this backend-private structure as a zero-field
//! opaque type and does not bind `RelationGetPartitionKey`. Re-declaring the
//! PG structure with `repr(C)` here would leave its ABI unchecked. The narrow
//! C accessor is compiled against the target PostgreSQL headers; this module restores
//! Rust lifetimes and typed field access. Provider-specific interpretation is
//! intentionally absent from both layers.

use core::ffi::{c_char, c_void};
use core::marker::PhantomData;
use core::ptr::NonNull;

use pgrx::pg_sys;

use crate::expr::pg::PgExprRef;

use super::RelationHandle;

unsafe extern "C" {
    fn lagodb_relation_partition_key(relation: pg_sys::Relation) -> *mut c_void;
    fn lagodb_partition_key_strategy(key: *const c_void) -> c_char;
    fn lagodb_partition_key_natts(key: *const c_void) -> i16;
    fn lagodb_partition_key_attr(
        key: *const c_void,
        index: i16,
    ) -> pg_sys::AttrNumber;
    fn lagodb_partition_key_type(key: *const c_void, index: i16) -> pg_sys::Oid;
    fn lagodb_partition_key_typmod(key: *const c_void, index: i16) -> i32;
    fn lagodb_partition_key_collation(key: *const c_void, index: i16) -> pg_sys::Oid;
    fn lagodb_partition_key_expr(key: *const c_void, index: i16)
    -> *mut pg_sys::Expr;
}

/// A PostgreSQL partitioning strategy copied from `PartitionKeyData`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionStrategy {
    List,
    Range,
    Hash,
}

/// A borrowed, relcache-owned partition key.
///
/// The handle exposes PostgreSQL metadata only. Interpretation as a provider's
/// physical partitioning scheme belongs to that provider.
#[derive(Clone, Copy, Debug)]
pub struct PartitionKeyHandle<'a> {
    key: NonNull<c_void>,
    _relation: PhantomData<&'a ()>,
}

/// One partition-key entry, indexed in PostgreSQL key order.
#[derive(Clone, Copy, Debug)]
pub struct PartitionKeyField<'a> {
    key: PartitionKeyHandle<'a>,
    index: i16,
}

impl<'a> PartitionKeyHandle<'a> {
    /// Borrow the analyzed partition key cached by PostgreSQL for `relation`.
    ///
    /// Returns `None` for a relation without a partition key. PostgreSQL owns
    /// the returned object and keeps it live while the relation is open.
    pub fn for_relation(relation: &'a RelationHandle<'_>) -> Option<Self> {
        // SAFETY: `RelationHandle` guarantees a live Relation for `'a`.
        let key = unsafe { lagodb_relation_partition_key(relation.as_raw()) };
        NonNull::new(key).map(|key| Self {
            key,
            _relation: PhantomData,
        })
    }

    pub fn strategy(self) -> PartitionStrategy {
        // SAFETY: construction established a live PartitionKeyData.
        match unsafe { lagodb_partition_key_strategy(self.key.as_ptr()) } as u8 {
            value
                if value
                    == pg_sys::PartitionStrategy::PARTITION_STRATEGY_LIST as u8 =>
            {
                PartitionStrategy::List
            }
            value
                if value
                    == pg_sys::PartitionStrategy::PARTITION_STRATEGY_RANGE as u8 =>
            {
                PartitionStrategy::Range
            }
            value
                if value
                    == pg_sys::PartitionStrategy::PARTITION_STRATEGY_HASH as u8 =>
            {
                PartitionStrategy::Hash
            }
            _ => unreachable!("PostgreSQL returned an invalid partition strategy"),
        }
    }

    #[inline]
    pub fn len(self) -> usize {
        // SAFETY: construction established a live PartitionKeyData.
        unsafe { lagodb_partition_key_natts(self.key.as_ptr()) as usize }
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub fn field(self, index: usize) -> Option<PartitionKeyField<'a>> {
        let index = i16::try_from(index).ok()?;
        (usize::from(index as u16) < self.len())
            .then_some(PartitionKeyField { key: self, index })
    }
}

impl<'a> PartitionKeyField<'a> {
    #[inline]
    pub fn attno(self) -> Option<pg_sys::AttrNumber> {
        // SAFETY: `PartitionKeyHandle::field` proved the index is in bounds.
        let attno =
            unsafe { lagodb_partition_key_attr(self.key.key.as_ptr(), self.index) };
        (attno != 0).then_some(attno)
    }

    #[inline]
    pub fn type_oid(self) -> pg_sys::Oid {
        // SAFETY: `PartitionKeyHandle::field` proved the index is in bounds.
        unsafe { lagodb_partition_key_type(self.key.key.as_ptr(), self.index) }
    }

    #[inline]
    pub fn typmod(self) -> i32 {
        // SAFETY: `PartitionKeyHandle::field` proved the index is in bounds.
        unsafe { lagodb_partition_key_typmod(self.key.key.as_ptr(), self.index) }
    }

    #[inline]
    pub fn collation(self) -> pg_sys::Oid {
        // SAFETY: `PartitionKeyHandle::field` proved the index is in bounds.
        unsafe { lagodb_partition_key_collation(self.key.key.as_ptr(), self.index) }
    }

    #[inline]
    pub fn expression(self) -> Option<PgExprRef<'a>> {
        // SAFETY: `PartitionKeyHandle::field` proved the index is in bounds;
        // the expression is relcache-owned and therefore lives for `'a`.
        unsafe {
            PgExprRef::from_raw_opt(lagodb_partition_key_expr(
                self.key.key.as_ptr(),
                self.index,
            ))
        }
    }
}
