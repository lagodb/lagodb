//! Validation of a base relation as a CustomScan planning candidate.

use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::expr::inspect::{RelationExprAnalyzer, RelationExprUsage, RelationScope};

/// Reason a relation cannot participate in CustomScan planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomScanRejection {
    NotARegularRelation,
    UnsupportedRelKind { relkind: u8 },
    HasRowMark,
    SystemColumnReference,
}

/// A relation that passed core's CustomScan eligibility checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CustomScanCandidate {
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rte: *mut pg_sys::RangeTblEntry,
    purpose: ScanPurpose,
}

impl CustomScanCandidate {
    /// # Safety
    ///
    /// Pointers must be non-NULL live planner nodes for the same pathlist
    /// callback invocation.
    pub unsafe fn inspect(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
        rte: *mut pg_sys::RangeTblEntry,
    ) -> Result<Self, CustomScanRejection> {
        let purpose = if unsafe { Self::is_modify_target(root, rel) } {
            ScanPurpose::Modify
        } else {
            ScanPurpose::Query
        };
        let candidate = Self {
            root,
            rel,
            rte,
            purpose,
        };
        unsafe { candidate.validate()? };
        Ok(candidate)
    }

    unsafe fn validate(self) -> Result<(), CustomScanRejection> {
        debug_assert!(!self.root.is_null(), "candidate root must be non-null");
        debug_assert!(!self.rel.is_null(), "candidate rel must be non-null");
        debug_assert!(!self.rte.is_null(), "candidate rte must be non-null");

        let rtekind = unsafe { (*self.rte).rtekind };
        if rtekind != pg_sys::RTEKind::RTE_RELATION {
            return Err(CustomScanRejection::NotARegularRelation);
        }

        let relkind = unsafe { (*self.rte).relkind } as u8;
        if !Self::is_supported_storage_relkind(relkind) {
            return Err(CustomScanRejection::UnsupportedRelKind { relkind });
        }

        let relid = unsafe { (*self.rel).relid };
        let row_marks = unsafe { (*self.root).rowMarks };
        if unsafe { Self::has_rowmark_for(row_marks, relid) } {
            return Err(CustomScanRejection::HasRowMark);
        }

        let usage = unsafe { self.collect_usage() };
        if self.purpose == ScanPurpose::Query
            && usage
                .system_attnos()
                .iter()
                .any(|&attno| Self::is_rejected_system_attno(attno))
        {
            return Err(CustomScanRejection::SystemColumnReference);
        }

        Ok(())
    }

    #[inline]
    pub(super) fn root(self) -> *mut pg_sys::PlannerInfo {
        self.root
    }

    #[inline]
    pub(super) fn rel(self) -> *mut pg_sys::RelOptInfo {
        self.rel
    }

    #[inline]
    pub(super) fn purpose(self) -> ScanPurpose {
        self.purpose
    }

    pub(super) unsafe fn provider_context(
        self,
    ) -> crate::customscan::provider::RelPathContext {
        unsafe {
            crate::customscan::provider::RelPathContext::with_planner(
                self.rte, self.root, self.rel,
            )
        }
    }

    unsafe fn is_modify_target(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
    ) -> bool {
        let parse = unsafe { (*root).parse };
        debug_assert!(
            !parse.is_null(),
            "CustomScanCandidate: root->parse must be non-null"
        );
        let command_type = unsafe { (*parse).commandType };
        if !matches!(
            command_type,
            pg_sys::CmdType::CMD_UPDATE
                | pg_sys::CmdType::CMD_DELETE
                | pg_sys::CmdType::CMD_MERGE
        ) {
            return false;
        }

        let relid = unsafe { (*rel).relid } as core::ffi::c_int;
        let all_result_relids = unsafe { (*root).all_result_relids };
        let leaf_result_relids = unsafe { (*root).leaf_result_relids };
        unsafe {
            pg_sys::bms_is_member(relid, all_result_relids)
                || pg_sys::bms_is_member(relid, leaf_result_relids)
        }
    }

    /// Relation-local expression usage consulted by path-stage gates.
    unsafe fn collect_usage(self) -> RelationExprUsage {
        let relids = unsafe { (*self.rel).relids };
        let scope = if relids.is_null() {
            RelationScope::exact(unsafe { (*self.rel).relid })
        } else {
            RelationScope::relids(relids)
        };
        let analyzer = RelationExprAnalyzer::new(scope);
        let mut usage = RelationExprUsage::default();

        let reltarget = unsafe { (*self.rel).reltarget };
        if !reltarget.is_null() {
            let exprs = unsafe { (*reltarget).exprs };
            usage.extend(unsafe { analyzer.collect_expr_list(exprs) });
        }

        let baserestrict = unsafe { (*self.rel).baserestrictinfo };
        usage.extend(unsafe { analyzer.collect_restrictinfo_list(baserestrict) });

        let joininfo = unsafe { (*self.rel).joininfo };
        usage.extend(unsafe { analyzer.collect_restrictinfo_list(joininfo) });

        usage
    }

    /// Heap-shaped relkind supported in v1 (`r`, `m`, `t`).
    #[inline]
    fn is_supported_storage_relkind(relkind: u8) -> bool {
        matches!(
            relkind,
            pg_sys::RELKIND_RELATION
                | pg_sys::RELKIND_MATVIEW
                | pg_sys::RELKIND_TOASTVALUE
        )
    }

    /// True if `rowMarks` lists `relid`; `rowMarks` may be NULL.
    unsafe fn has_rowmark_for(
        row_marks: *mut pg_sys::List,
        relid: pg_sys::Index,
    ) -> bool {
        if row_marks.is_null() {
            return false;
        }
        let len = unsafe { pg_sys::list_length(row_marks) };
        for i in 0..len {
            let mark =
                unsafe { pg_sys::list_nth(row_marks, i) } as *mut pg_sys::PlanRowMark;
            if mark.is_null() {
                continue;
            }
            if unsafe { (*mark).rti } == relid {
                return true;
            }
        }
        false
    }

    /// Rejected system attnos (ctid/xmin/cmax/etc.); tableoid is allowed.
    #[inline]
    fn is_rejected_system_attno(attno: pg_sys::AttrNumber) -> bool {
        let n = attno as i32;
        n == pg_sys::SelfItemPointerAttributeNumber
            || n == pg_sys::MinTransactionIdAttributeNumber
            || n == pg_sys::MinCommandIdAttributeNumber
            || n == pg_sys::MaxTransactionIdAttributeNumber
            || n == pg_sys::MaxCommandIdAttributeNumber
    }
}
