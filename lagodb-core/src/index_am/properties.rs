//! Access method properties: the static `IndexAmRoutine` members, and
//! `amproperty`, which `amutils.c` consults before answering from them.

use std::ffi::{CStr, c_char, c_int};

use pgrx::{pg_guard, pg_sys};

use super::IndexAmResult;
use crate::diag::ReportableError;

/// `amproperty`.
pub trait IndexPropertyProvider {
    /// `attno == 0` asks about the index itself.
    fn property(
        index_oid: pg_sys::Oid,
        attno: i32,
        prop: pg_sys::IndexAMProperty::Type,
        name: &CStr,
    ) -> IndexAmResult<PropertyAnswer>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyAnswer {
    /// Let PostgreSQL's generic logic answer.
    Default,
    /// The property is NULL.
    Null,
    Value(bool),
}

/// `amparallelvacuumoptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParallelVacuumOptions(u8);

impl ParallelVacuumOptions {
    pub const NONE: Self = Self(pg_sys::VACUUM_OPTION_NO_PARALLEL as u8);
    pub const BULK_DELETE: Self = Self(pg_sys::VACUUM_OPTION_PARALLEL_BULKDEL as u8);
    /// Cleanup in parallel only when no bulk delete ran.
    pub const CONDITIONAL_CLEANUP: Self =
        Self(pg_sys::VACUUM_OPTION_PARALLEL_COND_CLEANUP as u8);
    /// Cleanup in parallel even after a bulk delete.
    pub const CLEANUP: Self = Self(pg_sys::VACUUM_OPTION_PARALLEL_CLEANUP as u8);

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[inline]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

/// `IndexAmRoutine` members the provider declares. `amcanparallel` and the
/// callback pointers come from [`super::IndexAmRoutineBuilder`] instead.
#[derive(Debug, Clone, Copy)]
pub struct IndexAmProperties {
    pub strategies: u16,
    pub support: u16,
    pub opts_proc: u16,
    pub can_order: bool,
    pub can_order_by_op: bool,
    #[cfg(feature = "pg18")]
    pub can_hash: bool,
    #[cfg(feature = "pg18")]
    pub consistent_equality: bool,
    #[cfg(feature = "pg18")]
    pub consistent_ordering: bool,
    pub can_backward: bool,
    pub can_unique: bool,
    pub can_multi_col: bool,
    pub optional_key: bool,
    pub search_array: bool,
    pub search_nulls: bool,
    pub storage: bool,
    pub clusterable: bool,
    pub predicate_locks: bool,
    #[cfg(any(feature = "pg17", feature = "pg18"))]
    pub can_build_parallel: bool,
    pub can_include: bool,
    pub uses_maintenance_work_mem: bool,
    pub summarizing: bool,
    pub parallel_vacuum: ParallelVacuumOptions,
    pub key_type: pg_sys::Oid,
}

impl IndexAmProperties {
    pub const DEFAULT: Self = Self {
        strategies: 0,
        support: 0,
        opts_proc: 0,
        can_order: false,
        can_order_by_op: false,
        #[cfg(feature = "pg18")]
        can_hash: false,
        #[cfg(feature = "pg18")]
        consistent_equality: false,
        #[cfg(feature = "pg18")]
        consistent_ordering: false,
        can_backward: false,
        can_unique: false,
        can_multi_col: false,
        optional_key: false,
        search_array: false,
        search_nulls: false,
        storage: false,
        clusterable: false,
        predicate_locks: false,
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        can_build_parallel: false,
        can_include: false,
        uses_maintenance_work_mem: false,
        summarizing: false,
        parallel_vacuum: ParallelVacuumOptions::NONE,
        key_type: pg_sys::InvalidOid,
    };

    pub(super) fn write_to(&self, routine: &mut pg_sys::IndexAmRoutine) {
        routine.amstrategies = self.strategies;
        routine.amsupport = self.support;
        routine.amoptsprocnum = self.opts_proc;
        routine.amcanorder = self.can_order;
        routine.amcanorderbyop = self.can_order_by_op;
        #[cfg(feature = "pg18")]
        {
            routine.amcanhash = self.can_hash;
            routine.amconsistentequality = self.consistent_equality;
            routine.amconsistentordering = self.consistent_ordering;
        }
        routine.amcanbackward = self.can_backward;
        routine.amcanunique = self.can_unique;
        routine.amcanmulticol = self.can_multi_col;
        routine.amoptionalkey = self.optional_key;
        routine.amsearcharray = self.search_array;
        routine.amsearchnulls = self.search_nulls;
        routine.amstorage = self.storage;
        routine.amclusterable = self.clusterable;
        routine.ampredlocks = self.predicate_locks;
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        {
            routine.amcanbuildparallel = self.can_build_parallel;
        }
        routine.amcaninclude = self.can_include;
        routine.amusemaintenanceworkmem = self.uses_maintenance_work_mem;
        routine.amsummarizing = self.summarizing;
        routine.amparallelvacuumoptions = self.parallel_vacuum.bits();
        routine.amkeytype = self.key_type;
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn amproperty<A: IndexPropertyProvider>(
    index_oid: pg_sys::Oid,
    attno: c_int,
    prop: pg_sys::IndexAMProperty::Type,
    propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    // SAFETY: `pg_index_column_has_property` and friends pass the property
    // name and both output locations.
    unsafe {
        let name = CStr::from_ptr(propname);
        match A::property(index_oid, attno, prop, name).report_unwrap() {
            PropertyAnswer::Default => false,
            PropertyAnswer::Null => {
                *isnull = true;
                true
            }
            PropertyAnswer::Value(value) => {
                *res = value;
                true
            }
        }
    }
}
