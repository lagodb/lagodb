//! Operator class and family callbacks: `amvalidate`, `amadjustmembers` and,
//! on PostgreSQL 18, strategy translation.

use std::marker::PhantomData;

use pgrx::{PgList, pg_guard, pg_sys};

use super::IndexAmResult;
use crate::diag::ReportableError;

/// `amvalidate`.
pub trait IndexOpclass {
    /// Report problems with `INFO` and return `false`, as PostgreSQL's own
    /// access methods do.
    fn validate(opclass: pg_sys::Oid) -> IndexAmResult<bool>;
}

/// `amadjustmembers`.
pub trait IndexAdjustMembers {
    fn adjust_members(
        opfamily: pg_sys::Oid,
        opclass: pg_sys::Oid,
        operators: OpFamilyMembers<'_>,
        functions: OpFamilyMembers<'_>,
    ) -> IndexAmResult<()>;
}

/// `amtranslatestrategy` / `amtranslatecmptype`.
#[cfg(feature = "pg18")]
pub trait IndexStrategyTranslation {
    /// The comparison a strategy implements, or `COMPARE_INVALID`.
    fn translate_strategy(
        strategy: pg_sys::StrategyNumber,
        opfamily: pg_sys::Oid,
    ) -> IndexAmResult<pg_sys::CompareType::Type>;

    /// The strategy implementing a comparison, or `InvalidStrategy`.
    fn translate_compare_type(
        compare: pg_sys::CompareType::Type,
        opfamily: pg_sys::Oid,
    ) -> IndexAmResult<pg_sys::StrategyNumber>;
}

pub struct OpFamilyMembers<'a> {
    members: PgList<pg_sys::OpFamilyMember>,
    next: usize,
    _members: PhantomData<&'a mut pg_sys::OpFamilyMember>,
}

impl OpFamilyMembers<'_> {
    /// # Safety
    ///
    /// `list` must be NIL or a List of OpFamilyMember nodes owned by the
    /// caller of `amadjustmembers` for the call.
    unsafe fn from_pg(list: *mut pg_sys::List) -> Self {
        Self {
            members: unsafe { PgList::from_pg(list) },
            next: 0,
            _members: PhantomData,
        }
    }
}

impl<'a> Iterator for OpFamilyMembers<'a> {
    type Item = OpFamilyMemberRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let member = self.members.get_ptr(self.next)?;
        self.next += 1;
        // SAFETY: each list element is a distinct OpFamilyMember owned by
        // the caller of amadjustmembers.
        Some(OpFamilyMemberRef {
            member: unsafe { &mut *member },
        })
    }
}

/// An access method may only change a new member's dependency.
pub struct OpFamilyMemberRef<'a> {
    member: &'a mut pg_sys::OpFamilyMember,
}

impl OpFamilyMemberRef<'_> {
    #[inline]
    pub fn is_function(&self) -> bool {
        self.member.is_func
    }

    #[inline]
    pub fn object(&self) -> pg_sys::Oid {
        self.member.object
    }

    #[inline]
    pub fn number(&self) -> i32 {
        self.member.number
    }

    #[inline]
    pub fn left_type(&self) -> pg_sys::Oid {
        self.member.lefttype
    }

    #[inline]
    pub fn right_type(&self) -> pg_sys::Oid {
        self.member.righttype
    }

    #[inline]
    pub fn sort_family(&self) -> pg_sys::Oid {
        self.member.sortfamily
    }

    #[inline]
    pub fn set_dependency(
        &mut self,
        hard: bool,
        on_family: bool,
        referenced: pg_sys::Oid,
    ) {
        self.member.ref_is_hard = hard;
        self.member.ref_is_family = on_family;
        self.member.refobjid = referenced;
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn amvalidate<A: IndexOpclass>(
    opclass: pg_sys::Oid,
) -> bool {
    A::validate(opclass).report_unwrap()
}

#[pg_guard]
pub(super) extern "C-unwind" fn amadjustmembers<A: IndexAdjustMembers>(
    opfamily: pg_sys::Oid,
    opclass: pg_sys::Oid,
    operators: *mut pg_sys::List,
    functions: *mut pg_sys::List,
) {
    // SAFETY: both lists hold OpFamilyMember nodes owned by the caller and
    // are NIL when empty.
    let (operators, functions) = unsafe {
        (
            OpFamilyMembers::from_pg(operators),
            OpFamilyMembers::from_pg(functions),
        )
    };
    A::adjust_members(opfamily, opclass, operators, functions).report_unwrap();
}

#[cfg(feature = "pg18")]
#[pg_guard]
pub(super) extern "C-unwind" fn amtranslatestrategy<A: IndexStrategyTranslation>(
    strategy: pg_sys::StrategyNumber,
    opfamily: pg_sys::Oid,
) -> pg_sys::CompareType::Type {
    A::translate_strategy(strategy, opfamily).report_unwrap()
}

#[cfg(feature = "pg18")]
#[pg_guard]
pub(super) extern "C-unwind" fn amtranslatecmptype<A: IndexStrategyTranslation>(
    compare: pg_sys::CompareType::Type,
    opfamily: pg_sys::Oid,
) -> pg_sys::StrategyNumber {
    A::translate_compare_type(compare, opfamily).report_unwrap()
}
