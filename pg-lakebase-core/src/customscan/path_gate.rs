//! Path-stage relation gates before provider matching.

use pgrx::pg_sys;

use crate::expr::inspect::{RelationExprAnalyzer, RelationExprUsage, RelationScope};

/// First path-stage gate that rejected a CustomPath for this rel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathStageRejection {
    NotARegularRelation,
    UnsupportedRelKind { relkind: u8 },
    DmlTarget,
    HasRowMark,
    SystemColumnReference,
    WholeRowVarNotMaterializable,
}

/// Path-stage gates before `supports_relation`; valid planner pointers.
///
/// # Safety
///
/// `root`, `rel`, and `rte` must be non-NULL planner-owned pointers for the same
/// relation pathlist callback invocation.
pub unsafe fn path_stage_gates(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rte: *mut pg_sys::RangeTblEntry,
) -> Result<(), PathStageRejection> {
    unsafe { PathStageGate::new(root, rel, rte).check() }
}

#[derive(Debug, Clone, Copy)]
struct PathStageGate {
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rte: *mut pg_sys::RangeTblEntry,
}

impl PathStageGate {
    /// # Safety
    ///
    /// Pointers must be live planner nodes for the same pathlist callback.
    unsafe fn new(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
        rte: *mut pg_sys::RangeTblEntry,
    ) -> Self {
        Self { root, rel, rte }
    }

    /// # Safety
    ///
    /// Pointers captured in [`Self::new`] must still be live.
    unsafe fn check(self) -> Result<(), PathStageRejection> {
        debug_assert!(
            !self.root.is_null(),
            "path_stage_gates: root must be non-null"
        );
        debug_assert!(
            !self.rel.is_null(),
            "path_stage_gates: rel must be non-null"
        );
        debug_assert!(
            !self.rte.is_null(),
            "path_stage_gates: rte must be non-null"
        );

        // Gate 1: RTE_RELATION only.
        let rtekind = unsafe { (*self.rte).rtekind };
        if rtekind != pg_sys::RTEKind::RTE_RELATION {
            return Err(PathStageRejection::NotARegularRelation);
        }

        // Gate 2: heap-shaped relkind ('r', 'm', 't') only.
        let relkind = unsafe { (*self.rte).relkind } as u8;
        if !Self::is_supported_storage_relkind(relkind) {
            return Err(PathStageRejection::UnsupportedRelKind { relkind });
        }

        // Gate 3: DML target.
        if unsafe { self.is_dml_target() } {
            return Err(PathStageRejection::DmlTarget);
        }

        // Gate 4: rowmark.
        let relid = unsafe { (*self.rel).relid };
        let row_marks = unsafe { (*self.root).rowMarks };
        if unsafe { Self::has_rowmark_for(row_marks, relid) } {
            return Err(PathStageRejection::HasRowMark);
        }

        let usage = unsafe { self.collect_usage() };

        // Gate 5: relation-local system-column references except tableoid.
        if usage
            .system_attnos()
            .iter()
            .any(|&attno| Self::is_rejected_system_attno(attno))
        {
            return Err(PathStageRejection::SystemColumnReference);
        }

        // Gate 6: whole-row Var.
        //
        // Whole-row references require the runtime to populate every live user
        // column in the scan slot. The plan-time tuple contract represents that
        // requirement as `NeededColumns::All`, which disables storage projection
        // for that scan.
        if usage.has_whole_row()
            && !unsafe { self.runtime_can_materialize_all_user_attrs() }
        {
            return Err(PathStageRejection::WholeRowVarNotMaterializable);
        }

        Ok(())
    }

    unsafe fn is_dml_target(self) -> bool {
        let parse = unsafe { (*self.root).parse };
        debug_assert!(
            !parse.is_null(),
            "path_stage_gates: root->parse must be non-null"
        );
        let command_type = unsafe { (*parse).commandType };
        if command_type == pg_sys::CmdType::CMD_SELECT {
            return false;
        }

        let relid = unsafe { (*self.rel).relid } as core::ffi::c_int;
        let all_result_relids = unsafe { (*self.root).all_result_relids };
        unsafe { pg_sys::bms_is_member(relid, all_result_relids) }
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

    #[inline]
    unsafe fn runtime_can_materialize_all_user_attrs(self) -> bool {
        let _rel = self.rel;
        // Current CustomScan providers are required to support the select-all path:
        // when core reports `NeededColumns::All`, the provider must read and write
        // every live user column. This makes whole-row Var materialization safe
        // without falling back at path stage. If a future provider cannot satisfy
        // that contract, this should become a provider capability check.
        true
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
