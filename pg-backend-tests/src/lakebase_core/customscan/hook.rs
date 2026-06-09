//! Backend tests for `path_stage_gates` and join-parameterized emit decisions.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ptr;

    use crate::lakebase_core::support::pg::{
        OpExprSpec, PgNodeBuilder, PlannerRelFixture,
    };
    use pg_lakebase_core::customscan::hook::{PathStageRejection, path_stage_gates};
    use pgrx::pg_sys;
    use pgrx::pg_test;

    const PSG_RELID: u32 = 1;
    const PSG_REL_OID: u32 = 50_500;

    /// Default fixture: all gates pass (`RTE_RELATION`, `CMD_SELECT`, no rowmarks/sysattrs).
    unsafe fn make_psg_state() -> (
        *mut pg_sys::PlannerInfo,
        *mut pg_sys::RelOptInfo,
        *mut pg_sys::RangeTblEntry,
    ) {
        let fixture = unsafe { PlannerRelFixture::relation(PSG_RELID, PSG_REL_OID) };
        (fixture.root, fixture.baserel, fixture.rte)
    }

    struct HookExprFixture;

    impl HookExprFixture {
        fn nodes() -> PgNodeBuilder {
            PgNodeBuilder::new(PSG_RELID as core::ffi::c_int)
        }

        unsafe fn var_at(
            varno: core::ffi::c_int,
            varattno: pg_sys::AttrNumber,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_var_at(varno, varattno) }
        }

        unsafe fn var(varattno: pg_sys::AttrNumber) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_var(varattno) }
        }

        unsafe fn func_expr(arg: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_func_expr(arg) }
        }

        unsafe fn int4_const(value: i32) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_const(value) }
        }

        unsafe fn op_expr(
            opno: u32,
            lhs: *mut pg_sys::Expr,
            rhs: *mut pg_sys::Expr,
        ) -> *mut pg_sys::Expr {
            unsafe {
                Self::nodes().op_expr(
                    OpExprSpec::int4_comparison(opno)
                        .with_collations(pg_sys::Oid::INVALID, pg_sys::Oid::INVALID),
                    &[lhs, rhs],
                )
            }
        }

        unsafe fn restrictinfo(
            clause: *mut pg_sys::Expr,
        ) -> *mut pg_sys::RestrictInfo {
            unsafe { Self::nodes().restrictinfo(clause, false, false, 0) }
        }

        unsafe fn singleton_restrictinfo_list(
            rinfo: *mut pg_sys::RestrictInfo,
        ) -> *mut pg_sys::List {
            unsafe { Self::nodes().restrictinfo_list(&[rinfo]) }
        }

        unsafe fn outer_var(varattno: pg_sys::AttrNumber) -> *mut pg_sys::Expr {
            unsafe { Self::var_at(JP_OUTER_RELID as core::ffi::c_int, varattno) }
        }
    }

    unsafe fn psg_singleton_bitmap(member: i32) -> *mut pg_sys::Bitmapset {
        unsafe { pg_sys::bms_make_singleton(member) }
    }

    #[pg_test]
    fn path_stage_rejects_partitioned_relkind() {
        unsafe {
            let (root, baserel, rte) = make_psg_state();
            (*rte).relkind = pg_sys::RELKIND_PARTITIONED_TABLE as core::ffi::c_char;

            let result = path_stage_gates(root, baserel, rte);
            match result {
                Err(PathStageRejection::UnsupportedRelKind { relkind }) => {
                    assert_eq!(
                        relkind,
                        pg_sys::RELKIND_PARTITIONED_TABLE,
                        "partitioned-table rejection must surface the actual relkind byte",
                    );
                }
                other => panic!(
                    "expected UnsupportedRelKind for relkind 'p', got {:?}",
                    other
                ),
            }
        }
    }

    #[pg_test]
    fn path_stage_rejects_foreign_relkind() {
        unsafe {
            let (root, baserel, rte) = make_psg_state();
            (*rte).relkind = pg_sys::RELKIND_FOREIGN_TABLE as core::ffi::c_char;

            let result = path_stage_gates(root, baserel, rte);
            match result {
                Err(PathStageRejection::UnsupportedRelKind { relkind }) => {
                    assert_eq!(
                        relkind,
                        pg_sys::RELKIND_FOREIGN_TABLE,
                        "foreign-table rejection must surface the actual relkind byte",
                    );
                }
                other => {
                    panic!(
                        "expected UnsupportedRelKind for relkind 'f', got {:?}",
                        other
                    )
                }
            }
        }
    }

    #[pg_test]
    fn path_stage_rejects_dml_target() {
        unsafe {
            let (root, baserel, rte) = make_psg_state();
            (*(*root).parse).commandType = pg_sys::CmdType::CMD_UPDATE;
            (*root).all_result_relids = psg_singleton_bitmap(PSG_RELID as i32);

            let result = path_stage_gates(root, baserel, rte);
            assert_eq!(
                result,
                Err(PathStageRejection::DmlTarget),
                "DML target gate must reject when commandType != SELECT and \
                 rel->relid is in root->all_result_relids",
            );
        }
    }

    #[pg_test]
    fn path_stage_rejects_rowmark() {
        unsafe {
            let (root, baserel, rte) = make_psg_state();

            let mark = pg_sys::palloc0(core::mem::size_of::<pg_sys::PlanRowMark>())
                as *mut pg_sys::PlanRowMark;
            (*mark).type_ = pg_sys::NodeTag::T_PlanRowMark;
            (*mark).rti = PSG_RELID as pg_sys::Index;

            let mut row_marks: *mut pg_sys::List = ptr::null_mut();
            row_marks = pg_sys::lappend(row_marks, mark.cast());
            (*root).rowMarks = row_marks;

            let result = path_stage_gates(root, baserel, rte);
            assert_eq!(
                result,
                Err(PathStageRejection::HasRowMark),
                "rowmark gate must reject when root->rowMarks has an entry whose \
                 rti matches rel->relid",
            );
        }
    }

    #[pg_test]
    fn path_stage_rejects_system_column_reference() {
        unsafe {
            let (root, baserel, rte) = make_psg_state();

            let var_ctid =
                HookExprFixture::var(pg_sys::SelfItemPointerAttributeNumber as i16);
            let rinfo = HookExprFixture::restrictinfo(var_ctid);
            (*baserel).baserestrictinfo =
                HookExprFixture::singleton_restrictinfo_list(rinfo);

            let result = path_stage_gates(root, baserel, rte);
            assert_eq!(
                result,
                Err(PathStageRejection::SystemColumnReference),
                "system-column gate must reject a Var(ctid) reachable from \
                 baserestrictinfo",
            );
        }
    }

    #[pg_test]
    fn path_stage_rejects_system_column_nested_under_func_expr() {
        unsafe {
            let (root, baserel, rte) = make_psg_state();

            let var_ctid =
                HookExprFixture::var(pg_sys::SelfItemPointerAttributeNumber as i16);
            let nested = HookExprFixture::func_expr(var_ctid);
            let rinfo = HookExprFixture::restrictinfo(nested);
            (*baserel).baserestrictinfo =
                HookExprFixture::singleton_restrictinfo_list(rinfo);

            let result = path_stage_gates(root, baserel, rte);
            assert_eq!(
                result,
                Err(PathStageRejection::SystemColumnReference),
                "system-column gate must inspect Vars nested under common \
                 expression nodes",
            );
        }
    }

    #[pg_test]
    fn path_stage_ignores_system_column_from_other_relation() {
        unsafe {
            let (root, baserel, rte) = make_psg_state();

            let other_ctid = HookExprFixture::var_at(
                /* varno */ 2,
                pg_sys::SelfItemPointerAttributeNumber as i16,
            );
            let rinfo = HookExprFixture::restrictinfo(other_ctid);
            (*baserel).joininfo = HookExprFixture::singleton_restrictinfo_list(rinfo);

            let result = path_stage_gates(root, baserel, rte);
            assert_eq!(
                result,
                Ok(()),
                "system columns on another relation must not reject this rel's \
                 CustomPath",
            );
        }
    }

    #[pg_test]
    fn path_stage_accepts_normal_relation() {
        unsafe {
            let (root, baserel, rte) = make_psg_state();
            let result = path_stage_gates(root, baserel, rte);
            assert_eq!(
                result,
                Ok(()),
                "the default fixture (RTE_RELATION, RELKIND_RELATION, CMD_SELECT, \
                 no rowmark, no sysattrs) must pass every gate",
            );
        }
    }

    use pg_lakebase_core::customscan::hook::join_parameterized_variant_pushes_nothing;
    use pg_lakebase_core::expr::predicate::PlanPredicate;
    use pg_lakebase_core::expr::split::{
        PlanPushdownSplitter, PushdownContract, PushdownCosting,
        QualPushdownDecision, ScanClauseSource,
    };

    const INT4GE_OPNO: u32 = 525;
    const JP_INT4EQ_OPNO: u32 = 96;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum JpEmitDecision {
        Emit,
        Skip,
    }

    /// Pushable base + non-pushable join qual → skip redundant `JoinParameterized`.
    #[pg_test]
    fn join_parameterized_noop_path_skipped_after_fix() {
        unsafe {
            let (root, baserel, _rte) = make_psg_state();

            let base_var = HookExprFixture::var(1);
            let base_const = HookExprFixture::int4_const(0);
            let base_clause =
                HookExprFixture::op_expr(INT4GE_OPNO, base_var, base_const);
            let base_rinfo = HookExprFixture::restrictinfo(base_clause);
            let base_list = HookExprFixture::singleton_restrictinfo_list(base_rinfo);

            let mut base_classify =
                |_p: &PlanPredicate<'_>| QualPushdownDecision::Pushable {
                    contract: PushdownContract::ExactRowFilter,
                    costing: PushdownCosting::CostedPruning,
                };
            let mut base_splitter = PlanPushdownSplitter::new(
                root,
                baserel,
                base_list,
                ScanClauseSource::BaseRestriction,
                &mut base_classify,
            );
            let base_split = base_splitter.split();

            let ppi_var = HookExprFixture::var(2);
            let ppi_rhs = HookExprFixture::var(3);
            let ppi_clause =
                HookExprFixture::op_expr(JP_INT4EQ_OPNO, ppi_var, ppi_rhs);
            let ppi_rinfo = HookExprFixture::restrictinfo(ppi_clause);
            let ppi_list = HookExprFixture::singleton_restrictinfo_list(ppi_rinfo);

            let mut ppi_classify =
                |_p: &PlanPredicate<'_>| QualPushdownDecision::Unsupported;
            let mut ppi_splitter = PlanPushdownSplitter::new(
                root,
                baserel,
                ppi_list,
                ScanClauseSource::BaseRestriction,
                &mut ppi_classify,
            );
            let ppi_split = ppi_splitter.split();

            assert_eq!(
                base_split.pushed.len(),
                1,
                "base baserestrictinfo filter `a >= 0` must be pushable \
                 (non-empty base.pushed) — the masking precondition for the bug",
            );
            assert!(
                ppi_split.pushed.is_empty(),
                "join qual `b = c` must be non-pushable (empty ppi_split.pushed) \
                 — this is isBugCondition(group)",
            );

            let decision = if join_parameterized_variant_pushes_nothing(&ppi_split) {
                JpEmitDecision::Skip
            } else {
                JpEmitDecision::Emit
            };
            assert_eq!(
                decision,
                JpEmitDecision::Skip,
                "ppi_split.pushed is empty (base.pushed.len={}) \
                 so the JoinParameterized variant pushes nothing beyond Plain and \
                 MUST be skipped, but the emit decision is Emit — a redundant \
                 parameterized custom path that pushes nothing extra over the Plain \
                 variant yet carries a required_outer",
                base_split.pushed.len(),
            );
        }
    }

    /// Skipping empty ppi must not drop base pushdown on the `Plain` path.
    #[pg_test]
    fn join_parameterized_skip_keeps_base_pushdown_on_plain_path() {
        unsafe {
            let (root, baserel, _rte) = make_psg_state();

            let base_var = HookExprFixture::var(1);
            let base_const = HookExprFixture::int4_const(0);
            let base_clause =
                HookExprFixture::op_expr(INT4GE_OPNO, base_var, base_const);
            let base_rinfo = HookExprFixture::restrictinfo(base_clause);
            let base_list = HookExprFixture::singleton_restrictinfo_list(base_rinfo);

            let mut base_classify =
                |_p: &PlanPredicate<'_>| QualPushdownDecision::Pushable {
                    contract: PushdownContract::ExactRowFilter,
                    costing: PushdownCosting::CostedPruning,
                };
            let mut base_splitter = PlanPushdownSplitter::new(
                root,
                baserel,
                base_list,
                ScanClauseSource::BaseRestriction,
                &mut base_classify,
            );
            let base_split = base_splitter.split();

            let ppi_var = HookExprFixture::var(2);
            let ppi_rhs = HookExprFixture::var(3);
            let ppi_clause =
                HookExprFixture::op_expr(JP_INT4EQ_OPNO, ppi_var, ppi_rhs);
            let ppi_rinfo = HookExprFixture::restrictinfo(ppi_clause);
            let ppi_list = HookExprFixture::singleton_restrictinfo_list(ppi_rinfo);

            let mut ppi_classify =
                |_p: &PlanPredicate<'_>| QualPushdownDecision::Unsupported;
            let mut ppi_splitter = PlanPushdownSplitter::new(
                root,
                baserel,
                ppi_list,
                ScanClauseSource::BaseRestriction,
                &mut ppi_classify,
            );
            let ppi_split = ppi_splitter.split();

            assert!(
                join_parameterized_variant_pushes_nothing(&ppi_split),
                "the JoinParameterized group must be skipped (empty ppi_split.pushed) \
                 for this 'no coverage lost' scenario to apply",
            );

            assert_eq!(
                base_split.pushed.len(),
                1,
                "no coverage lost: the skipped JoinParameterized group must not \
                 affect the base pushdown — the `Plain` path still carries the \
                 single pushable base clause `a >= 0` (base_split.pushed.len == 1), \
                 so pushed predicates and query results are unchanged",
            );

            assert_eq!(
                base_split.pushed[0].expr, base_clause,
                "the base pushdown carried by the `Plain` path must be the original \
                 `a >= 0` baserestrictinfo clause — unchanged by skipping the \
                 redundant JoinParameterized variant",
            );
        }
    }

    const JP_OUTER_RELID: u32 = 2;

    /// Pushable equijoin → still emit `JoinParameterized` with `required_outer = {outer}`.
    #[pg_test]
    fn join_parameterized_equijoin_path_still_emitted_after_fix() {
        unsafe {
            let (root, baserel, _rte) = make_psg_state();

            let lake_k = HookExprFixture::var(1);
            let outer_id = HookExprFixture::outer_var(1);
            let join_clause =
                HookExprFixture::op_expr(JP_INT4EQ_OPNO, lake_k, outer_id);
            let join_rinfo = HookExprFixture::restrictinfo(join_clause);
            let ppi_list = HookExprFixture::singleton_restrictinfo_list(join_rinfo);

            let mut ppi_classify =
                |_p: &PlanPredicate<'_>| QualPushdownDecision::Pushable {
                    contract: PushdownContract::ExactRowFilter,
                    costing: PushdownCosting::CostedPruning,
                };
            let mut ppi_splitter = PlanPushdownSplitter::new(
                root,
                baserel,
                ppi_list,
                ScanClauseSource::BaseRestriction,
                &mut ppi_classify,
            );
            let ppi_split = ppi_splitter.split();

            let required_outer = psg_singleton_bitmap(JP_OUTER_RELID as i32);

            assert_eq!(
                ppi_split.pushed.len(),
                1,
                "equijoin join qual `k = outer.id` must be pushable \
                 (non-empty ppi_split.pushed) — this is NOT isBugCondition(group)",
            );

            let pushes_nothing =
                join_parameterized_variant_pushes_nothing(&ppi_split);
            assert!(
                !pushes_nothing,
                "join_parameterized_variant_pushes_nothing must return FALSE for a \
                 useful equijoin variant (ppi_split.pushed.len={}); a useful \
                 JoinParameterized variant pushes a join qual beyond Plain and must \
                 NOT be gated out",
                ppi_split.pushed.len(),
            );

            let decision = if pushes_nothing {
                JpEmitDecision::Skip
            } else {
                JpEmitDecision::Emit
            };
            assert_eq!(
                decision,
                JpEmitDecision::Emit,
                "ppi_split.pushed is non-empty \
                 (len={}) so the JoinParameterized variant pushes a join qual \
                 beyond Plain and MUST be emitted, but the emit decision is Skip",
                ppi_split.pushed.len(),
            );

            assert!(
                pg_sys::bms_is_member(JP_OUTER_RELID as i32, required_outer),
                "emitted JoinParameterized variant must carry required_outer = \
                 {{outer rel}} (RTI {})",
                JP_OUTER_RELID,
            );
            assert_eq!(
                pg_sys::bms_num_members(required_outer),
                1,
                "required_outer must be exactly the single outer rel the equijoin \
                 parameterizes on ({{outer rel}})",
            );
        }
    }
}
