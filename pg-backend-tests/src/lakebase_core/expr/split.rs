//! Backend tests for `PlanPushdownSplitter`.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::lakebase_core::support::pg::{
        INT4_EQ_OPNO, OpExprSpec, PgNodeBuilder, PlannerRelFixture,
    };
    use pg_lakebase_core::expr::predicate::{PlanPredicate, PlanScalar};
    use pg_lakebase_core::expr::split::{
        PlanPushdownSplitter, PushdownContract, PushdownCosting, PushedExpr,
        QualPushdownDecision, ScanClauseSource,
    };
    use pgrx::pg_sys;
    use pgrx::pg_test;

    fn pushed_entry(
        expr: *mut pg_sys::Expr,
        contract: PushdownContract,
    ) -> PushedExpr {
        PushedExpr {
            expr,
            contract,
            costing: PushdownCosting::CostedPruning,
        }
    }

    const SYNTH_REL_OID: u32 = 16_500;

    /// 1-based RTI for the synthetic scan relation (`varno == scan_relid`).
    const SYNTH_RELID: u32 = 1;

    /// `pg_operator.oid` of `texteq`.
    const TEXTEQ_OPNO: u32 = 98;

    /// Non-default collation OID for exercising `(opno, opcollid, inputcollid)` keys.
    const NON_DEFAULT_COLLATION_OID: u32 = 50_000;

    struct SplitFixture {
        planner: PlannerRelFixture,
        nodes: PgNodeBuilder,
    }

    impl SplitFixture {
        unsafe fn new() -> Self {
            Self {
                planner: unsafe {
                    PlannerRelFixture::relation(SYNTH_RELID, SYNTH_REL_OID)
                },
                nodes: PgNodeBuilder::new(SYNTH_RELID as core::ffi::c_int),
            }
        }

        fn root(&self) -> *mut pg_sys::PlannerInfo {
            self.planner.root
        }

        fn baserel(&self) -> *mut pg_sys::RelOptInfo {
            self.planner.baserel
        }

        unsafe fn int4_var(&self, attno: i16) -> *mut pg_sys::Expr {
            unsafe { self.nodes.int4_var(attno as pg_sys::AttrNumber) }
        }

        unsafe fn int4_const(&self, value: i32) -> *mut pg_sys::Expr {
            unsafe { self.nodes.int4_const(value) }
        }

        unsafe fn op_expr(
            &self,
            opno: u32,
            opcollid: pg_sys::Oid,
            inputcollid: pg_sys::Oid,
            args: &[*mut pg_sys::Expr],
        ) -> *mut pg_sys::Expr {
            unsafe {
                self.nodes.op_expr(
                    OpExprSpec::int4_comparison(opno)
                        .with_collations(opcollid, inputcollid),
                    args,
                )
            }
        }

        unsafe fn restrictinfo(
            &self,
            clause: *mut pg_sys::Expr,
            pseudoconstant: bool,
        ) -> *mut pg_sys::RestrictInfo {
            unsafe { self.nodes.restrictinfo(clause, pseudoconstant, true, 0) }
        }

        unsafe fn restrictinfo_with_security(
            &self,
            clause: *mut pg_sys::Expr,
            leakproof: bool,
            security_level: u32,
        ) -> *mut pg_sys::RestrictInfo {
            unsafe {
                self.nodes.restrictinfo(
                    clause,
                    /* pseudoconstant */ false,
                    leakproof,
                    security_level,
                )
            }
        }

        unsafe fn restrictinfo_list(
            &self,
            rinfos: &[*mut pg_sys::RestrictInfo],
        ) -> *mut pg_sys::List {
            unsafe { self.nodes.restrictinfo_list(rinfos) }
        }

        unsafe fn bool_expr(
            &self,
            boolop: pg_sys::BoolExprType::Type,
            args: &[*mut pg_sys::Expr],
        ) -> *mut pg_sys::Expr {
            unsafe { self.nodes.bool_expr(boolop, args) }
        }
    }

    /// Exact pushdown: omitted from residual, present in pushed and recheck.
    #[pg_test]
    fn split_exact_pushdown_omits_residual_adds_recheck() {
        unsafe {
            let fixture = SplitFixture::new();

            let var = fixture.int4_var(1);
            let lit = fixture.int4_const(1);
            let opexpr = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[var, lit],
            );

            let rinfo = fixture.restrictinfo(opexpr, /* pseudoconstant */ false);
            let scan_clauses = fixture.restrictinfo_list(&[rinfo]);

            let mut classify_leaf =
                |_p: &PlanPredicate| QualPushdownDecision::Pushable {
                    contract: PushdownContract::ExactRowFilter,
                    costing: PushdownCosting::CostedPruning,
                };

            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert!(
                split.residual.is_empty(),
                "exact clause must be omitted from residual",
            );
            assert_eq!(split.pushed.len(), 1, "exact clause must be in pushed");
            assert_eq!(split.recheck.len(), 1, "exact clause must be in recheck");
            assert_eq!(
                split.pushed_contracts().collect::<Vec<_>>(),
                vec![PushdownContract::ExactRowFilter],
                "contract must be ExactRowFilter",
            );

            assert_eq!(
                split.pushed[0].expr, opexpr,
                "pushed[0] must be the bare clause Expr* from RestrictInfo.clause",
            );
            assert_eq!(
                split.recheck[0], opexpr,
                "recheck[0] must be the bare clause Expr* from RestrictInfo.clause",
            );

            assert_eq!(split.column_refs.len(), 1);
            let cr = &split.column_refs[0];
            assert_eq!(cr.expr_index, 0);
            assert_eq!(cr.rel_oid, pg_sys::Oid::from(SYNTH_REL_OID));
            assert_eq!(cr.attno, 1);
            assert_eq!(cr.atttypid, pg_sys::INT4OID);
        }
    }

    /// ConservativePruning pushdown: stays in residual and pushed, not in recheck.
    #[pg_test]
    fn split_conservative_pruning_pushdown_keeps_residual_and_adds_pushed() {
        unsafe {
            let fixture = SplitFixture::new();

            let var = fixture.int4_var(1);
            let lit = fixture.int4_const(7);
            let opexpr = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[var, lit],
            );

            let rinfo = fixture.restrictinfo(opexpr, /* pseudoconstant */ false);
            let scan_clauses = fixture.restrictinfo_list(&[rinfo]);

            let mut classify_leaf =
                |_p: &PlanPredicate| QualPushdownDecision::Pushable {
                    contract: PushdownContract::ConservativePruning,
                    costing: PushdownCosting::CostedPruning,
                };

            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert_eq!(
                split.residual.len(),
                1,
                "ConservativePruning clause must remain in residual",
            );
            assert_eq!(
                split.pushed.len(),
                1,
                "ConservativePruning clause must enter pushed",
            );
            assert!(
                split.recheck.is_empty(),
                "ConservativePruning clause must NOT enter recheck",
            );
            assert_eq!(
                split.pushed_contracts().collect::<Vec<_>>(),
                vec![PushdownContract::ConservativePruning],
                "contract must be ConservativePruning",
            );

            assert_eq!(split.residual[0], opexpr);
            assert_eq!(split.pushed[0].expr, opexpr);
        }
    }

    /// One AND clause mixing Exact + Conservative: residual is the conservative leaf only.
    #[pg_test]
    fn split_mixed_and_clause_residual_is_minimal() {
        const INT4LT_OPNO: u32 = 97;

        unsafe {
            let fixture = SplitFixture::new();

            let exact_leaf = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[fixture.int4_var(1), fixture.int4_const(1)],
            );
            let conservative_leaf = fixture.op_expr(
                INT4LT_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[fixture.int4_var(2), fixture.int4_const(2)],
            );
            let and_clause = fixture.bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[exact_leaf, conservative_leaf],
            );
            let and_ri = fixture.restrictinfo(and_clause, false);
            let scan_clauses = fixture.restrictinfo_list(&[and_ri]);

            let mut classify_leaf = |pred: &PlanPredicate| {
                let attno = match pred {
                    PlanPredicate::Comparison {
                        left: PlanScalar::Column(c),
                        ..
                    } => c.attno,
                    _ => return QualPushdownDecision::Unsupported,
                };
                match attno {
                    1 => QualPushdownDecision::Pushable {
                        contract: PushdownContract::ExactRowFilter,
                        costing: PushdownCosting::CostedPruning,
                    },
                    2 => QualPushdownDecision::Pushable {
                        contract: PushdownContract::ConservativePruning,
                        costing: PushdownCosting::CostedPruning,
                    },
                    _ => QualPushdownDecision::Unsupported,
                }
            };

            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert_eq!(
                split.residual,
                vec![conservative_leaf],
                "mixed AND must keep only conservative leaf in residual",
            );
            assert_eq!(
                split.pushed,
                vec![
                    pushed_entry(exact_leaf, PushdownContract::ExactRowFilter),
                    pushed_entry(
                        conservative_leaf,
                        PushdownContract::ConservativePruning,
                    ),
                ],
            );
            assert_eq!(split.recheck, vec![exact_leaf]);
            assert!(
                !split.residual.contains(&exact_leaf),
                "ExactRowFilter sibling must not be duplicated into residual",
            );
            assert!(
                !split.residual.contains(&and_clause),
                "original AND must not be widened into residual",
            );
        }
    }

    /// Unsupported: stays in residual only.
    #[pg_test]
    fn split_unsupported_stays_in_residual_only() {
        unsafe {
            let fixture = SplitFixture::new();

            let var = fixture.int4_var(1);
            let lit = fixture.int4_const(13);
            let opexpr = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[var, lit],
            );

            let rinfo = fixture.restrictinfo(opexpr, /* pseudoconstant */ false);
            let scan_clauses = fixture.restrictinfo_list(&[rinfo]);

            let mut classify_leaf =
                |_p: &PlanPredicate| QualPushdownDecision::Unsupported;

            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert_eq!(split.residual.len(), 1, "unsupported stays in residual");
            assert!(split.pushed.is_empty(), "unsupported never in pushed");
            assert!(split.pushed_contracts().next().is_none());
            assert!(split.recheck.is_empty(), "unsupported never in recheck");
            assert!(
                split.column_refs.is_empty(),
                "unsupported has nothing pushed -> no column refs",
            );

            assert_eq!(split.residual[0], opexpr);
        }
    }

    /// Pseudoconstant RestrictInfo dropped before classification.
    #[pg_test]
    fn split_drops_pseudoconstant_restrictinfo() {
        unsafe {
            let fixture = SplitFixture::new();

            let lhs = fixture.int4_const(1);
            let rhs = fixture.int4_const(1);
            let opexpr = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[lhs, rhs],
            );

            // Classifier would push Exact if reached; pseudoconstant must drop first.
            let rinfo = fixture.restrictinfo(opexpr, /* pseudoconstant */ true);
            let scan_clauses = fixture.restrictinfo_list(&[rinfo]);

            let mut classify_leaf =
                |_p: &PlanPredicate| QualPushdownDecision::Pushable {
                    contract: PushdownContract::ExactRowFilter,
                    costing: PushdownCosting::CostedPruning,
                };

            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert!(
                split.residual.is_empty(),
                "pseudoconstant must NOT appear in residual ",
            );
            assert!(
                split.pushed.is_empty(),
                "pseudoconstant must NOT appear in pushed ",
            );
            assert!(
                split.recheck.is_empty(),
                "pseudoconstant must NOT appear in recheck",
            );
            assert!(split.pushed_contracts().next().is_none());
            assert!(split.column_refs.is_empty());
        }
    }

    /// A non-leakproof clause above the relation's minimum security level must
    /// remain in PostgreSQL's residual qual without reaching the provider.
    #[pg_test]
    fn split_security_gate_keeps_non_leakproof_clause_residual() {
        unsafe {
            let fixture = SplitFixture::new();
            let clause = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[fixture.int4_var(1), fixture.int4_const(1)],
            );
            let rinfo = fixture.restrictinfo_with_security(
                clause,
                /* leakproof */ false,
                /* security_level */ 1,
            );
            let scan_clauses = fixture.restrictinfo_list(&[rinfo]);

            let mut classify_leaf =
                |_p: &PlanPredicate| -> QualPushdownDecision {
                    panic!(
                        "security-gated clause must not reach provider classification"
                    )
                };
            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert_eq!(split.residual, vec![clause]);
            assert!(split.pushed.is_empty());
            assert!(split.recheck.is_empty());
            assert!(split.column_refs.is_empty());
        }
    }

    /// Leakproof clauses may be promoted even when their security level is
    /// above the relation minimum.
    #[pg_test]
    fn split_security_gate_allows_leakproof_clause() {
        unsafe {
            let fixture = SplitFixture::new();
            let clause = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[fixture.int4_var(1), fixture.int4_const(1)],
            );
            let rinfo = fixture.restrictinfo_with_security(
                clause,
                /* leakproof */ true,
                /* security_level */ 1,
            );
            let scan_clauses = fixture.restrictinfo_list(&[rinfo]);

            let mut classify_leaf =
                |_p: &PlanPredicate| QualPushdownDecision::Pushable {
                    contract: PushdownContract::ExactRowFilter,
                    costing: PushdownCosting::CostedPruning,
                };
            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert!(split.residual.is_empty());
            assert_eq!(
                split.pushed,
                vec![pushed_entry(clause, PushdownContract::ExactRowFilter)],
            );
            assert_eq!(split.recheck, vec![clause]);
        }
    }

    /// A join/PPI clause that PostgreSQL says is not movable to the scan
    /// relation must remain residual without reaching provider classification.
    #[pg_test]
    fn split_movable_source_keeps_unmovable_clause_residual() {
        unsafe {
            let fixture = SplitFixture::new();
            let clause = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[fixture.int4_var(1), fixture.int4_const(1)],
            );
            let rinfo = fixture.restrictinfo_with_security(
                clause,
                /* leakproof */ true,
                /* security_level */ 0,
            );
            (*rinfo).clause_relids =
                pg_sys::bms_make_singleton(SYNTH_RELID as core::ffi::c_int);
            (*rinfo).outer_relids =
                pg_sys::bms_make_singleton(SYNTH_RELID as core::ffi::c_int);
            assert!(
                !pg_sys::join_clause_is_movable_to(rinfo, fixture.baserel()),
                "fixture must represent an unmovable join clause",
            );
            let scan_clauses = fixture.restrictinfo_list(&[rinfo]);

            let mut classify_leaf =
                |_p: &PlanPredicate| -> QualPushdownDecision {
                    panic!(
                        "unmovable clause must not reach provider classification"
                    )
                };
            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::Movable,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert_eq!(split.residual, vec![clause]);
            assert!(split.pushed.is_empty());
            assert!(split.recheck.is_empty());
            assert!(split.column_refs.is_empty());
        }
    }

    /// Split holds bare `(*rinfo).clause` pointers, not RestrictInfo wrappers.
    #[pg_test]
    fn split_unwraps_restrictinfo_to_bare_expr() {
        unsafe {
            let fixture = SplitFixture::new();

            let mk = |attno: i16, lit: i32| -> *mut pg_sys::Expr {
                fixture.op_expr(
                    INT4_EQ_OPNO,
                    pg_sys::Oid::INVALID,
                    pg_sys::Oid::INVALID,
                    &[fixture.int4_var(attno), fixture.int4_const(lit)],
                )
            };
            let exact_clause = mk(1, 1);
            let conservative_pruning_clause = mk(2, 2);
            let unsupported_clause = mk(3, 3);

            let exact_ri = fixture.restrictinfo(exact_clause, false);
            let conservative_pruning_ri =
                fixture.restrictinfo(conservative_pruning_clause, false);
            let unsupported_ri = fixture.restrictinfo(unsupported_clause, false);
            let scan_clauses = fixture.restrictinfo_list(&[
                exact_ri,
                conservative_pruning_ri,
                unsupported_ri,
            ]);

            let mut classify_leaf = |pred: &PlanPredicate| {
                let attno = match pred {
                    PlanPredicate::Comparison {
                        left: PlanScalar::Column(c),
                        ..
                    } => c.attno,
                    _ => return QualPushdownDecision::Unsupported,
                };
                match attno {
                    1 => QualPushdownDecision::Pushable {
                        contract: PushdownContract::ExactRowFilter,
                        costing: PushdownCosting::CostedPruning,
                    },
                    2 => QualPushdownDecision::Pushable {
                        contract: PushdownContract::ConservativePruning,
                        costing: PushdownCosting::CostedPruning,
                    },
                    _ => QualPushdownDecision::Unsupported,
                }
            };

            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert_eq!(
                split.residual,
                vec![conservative_pruning_clause, unsupported_clause],
            );
            assert_eq!(
                split.pushed,
                vec![
                    pushed_entry(exact_clause, PushdownContract::ExactRowFilter),
                    pushed_entry(
                        conservative_pruning_clause,
                        PushdownContract::ConservativePruning,
                    ),
                ],
            );
            assert_eq!(split.recheck, vec![exact_clause]);

            let ri_ptrs = [
                exact_ri as *mut pg_sys::Expr,
                conservative_pruning_ri as *mut pg_sys::Expr,
                unsupported_ri as *mut pg_sys::Expr,
            ];
            for &p in split
                .residual
                .iter()
                .chain(split.pushed.iter().map(|e| &e.expr))
                .chain(split.recheck.iter())
            {
                assert!(
                    !ri_ptrs.contains(&p),
                    "split must hold bare Expr pointers, never the RestrictInfo wrapper",
                );
            }
        }
    }

    /// Classification keys on `(opno, opcollid, inputcollid)`, not operator name.
    #[pg_test]
    fn split_operator_identity_keys_on_opno_and_collation_triple() {
        unsafe {
            let fixture = SplitFixture::new();

            let a_var = fixture.int4_var(1);
            let a_lit = fixture.int4_const(1);
            let clause_a = fixture.op_expr(
                INT4_EQ_OPNO,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                &[a_var, a_lit],
            );

            let b_var = fixture.int4_var(2);
            let b_lit = fixture.int4_const(2);
            let clause_b = fixture.op_expr(
                TEXTEQ_OPNO,
                pg_sys::Oid::from(NON_DEFAULT_COLLATION_OID),
                pg_sys::Oid::from(NON_DEFAULT_COLLATION_OID),
                &[b_var, b_lit],
            );

            let ri_a = fixture.restrictinfo(clause_a, false);
            let ri_b = fixture.restrictinfo(clause_b, false);
            let scan_clauses = fixture.restrictinfo_list(&[ri_a, ri_b]);

            let mut classify_leaf = |pred: &PlanPredicate| {
                let PlanPredicate::Comparison { op, .. } = pred else {
                    return QualPushdownDecision::Unsupported;
                };
                let is_default_int_eq = op.opno == pg_sys::Oid::from(INT4_EQ_OPNO)
                    && op.opcollid == pg_sys::Oid::INVALID
                    && op.inputcollid == pg_sys::Oid::INVALID;
                if is_default_int_eq {
                    QualPushdownDecision::Pushable {
                        contract: PushdownContract::ExactRowFilter,
                        costing: PushdownCosting::CostedPruning,
                    }
                } else {
                    QualPushdownDecision::Unsupported
                }
            };

            let mut splitter = PlanPushdownSplitter::new(
                fixture.root(),
                fixture.baserel(),
                scan_clauses,
                ScanClauseSource::BaseRestriction,
                &mut classify_leaf,
            );
            let split = splitter.split();

            assert_eq!(
                split.residual,
                vec![clause_b],
                "non-default-collation text= must remain in residual",
            );
            assert_eq!(
                split.pushed,
                vec![pushed_entry(clause_a, PushdownContract::ExactRowFilter)],
                "only the auto-Exact int4= must enter pushed",
            );
            assert_eq!(
                split.recheck,
                vec![clause_a],
                "only the Exact clause is recorded in recheck",
            );
            assert_eq!(
                split.pushed_contracts().collect::<Vec<_>>(),
                vec![PushdownContract::ExactRowFilter],
            );

            assert_eq!(split.column_refs.len(), 1);
            assert_eq!(split.column_refs[0].expr_index, 0);
            assert_eq!(split.column_refs[0].attno, 1);
        }
    }
}
