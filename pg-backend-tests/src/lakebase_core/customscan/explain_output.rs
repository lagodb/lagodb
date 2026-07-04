//! Backend tests for `ExplainCustomScan` TEXT/VERBOSE/JSON output (trampoline only).
//! Full `EXPLAIN` SQL coverage is in pg-iceberg-am `customscan_*` regressions.
//!
//! The fixtures below are deliberately object-shaped: an [`ExplainHarness`]
//! owns the per-test backend identity (temp table OID, varno, alias) and is the
//! single entry point for building a [`BuiltScenario`] and rendering it. A
//! [`ScenarioSpec`] is the format-agnostic description of a scan that both the
//! example tests and the proptest strategies share, so adding a render mode or
//! changing the scan shape touches exactly one place.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::{CStr, c_char, c_int};
    use core::ops::RangeInclusive;
    use std::ffi::CString;
    use std::ptr;

    use crate::lakebase_core::support::pg::{OpExprSpec, PgNodeBuilder};
    use pg_lakebase_core::customscan::codec::{PrivateDataReader, PrivateDataWriter};
    use pg_lakebase_core::customscan::custom_private::{
        CustomScanPrivate, encode_split,
    };
    use pg_lakebase_core::customscan::explain::explain_custom_scan_trampoline;
    use pg_lakebase_core::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
        PathVariant, PlanTranslateContext, ReScanContext, RelPathContext,
    };
    use pg_lakebase_core::customscan::state::CustomScanStateWrapper;
    use pg_lakebase_core::expr::split::{
        ColumnRef, PushdownContract, QualPushdownDecision,
    };
    use pgrx::pg_sys;
    use pgrx::pg_test;
    use proptest::prelude::*;
    use proptest::test_runner::TestRunner;

    // ----------------------------------------------------------------------
    // Provider stub: the explain trampoline only needs NAME / PrivateData /
    // State, so every execution callback is `unreachable!`.
    // ----------------------------------------------------------------------

    /// Stub provider; explain trampoline only needs NAME / PrivateData / State.
    struct ExplainPrivate;

    impl CustomScanPrivate for ExplainPrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(ExplainPrivate)
        }
    }

    struct ExplainState;

    struct ExplainProvider;

    impl LakebaseCustomScanProvider for ExplainProvider {
        const NAME: &'static CStr = c"explain-output-test-provider";
        type PrivateData = ExplainPrivate;
        type State = ExplainState;

        fn supports_relation(_ctx: &RelPathContext) -> bool {
            false
        }

        fn classify_predicate(
            _ctx: &PlanTranslateContext,
            _predicate: &pg_lakebase_core::expr::predicate::PlanPredicate,
        ) -> QualPushdownDecision {
            QualPushdownDecision::Unsupported
        }

        fn create_path(
            _ctx: &RelPathContext,
            _variant: &PathVariant<'_>,
            _builder: CustomPathBuilder<Self>,
        ) -> Option<CustomPathPlan<Self>> {
            None
        }

        fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
            ExplainState
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("explain output tests do not call begin")
        }

        fn next_slot(
            _ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            unreachable!("explain output tests do not call next_slot")
        }

        fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("explain output tests do not call rescan")
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("explain output tests do not call end")
        }
    }

    // ----------------------------------------------------------------------
    // Leaf PG-node constructors (analogous to `support::pg`): pure builders
    // with no per-test state. The harness orchestrates them.
    // ----------------------------------------------------------------------

    struct ExplainExprFixture;

    impl ExplainExprFixture {
        fn nodes(varno: c_int) -> PgNodeBuilder {
            PgNodeBuilder::new(varno)
        }

        /// Build an INT4 `Const` leaf for predictable deparse text.
        unsafe fn int4_const(value: i32) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(1).int4_const(value) }
        }

        /// Build an INT4 `Var`; tests pass `varno` matching `scan.scanrelid`.
        unsafe fn var_int4(
            varno: c_int,
            attno: pg_sys::AttrNumber,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(varno).int4_var(attno) }
        }

        /// `OpExpr` with real `int4eq` OIDs so `deparse_expression` renders `=`.
        unsafe fn op_int4eq(
            left: *mut pg_sys::Expr,
            right: *mut pg_sys::Expr,
        ) -> *mut pg_sys::Expr {
            unsafe {
                Self::nodes(1).op_expr(OpExprSpec::int4_eq_deparse(), &[left, right])
            }
        }

        /// `attr = const` predicate with real `int4eq` OIDs (deparses to `=`).
        ///
        /// This is the single shape every scenario uses, so both the harness
        /// and the no-context smoke test build predicates through it.
        unsafe fn eq_pred(
            varno: c_int,
            attno: pg_sys::AttrNumber,
            value: i32,
        ) -> *mut pg_sys::Expr {
            unsafe {
                Self::op_int4eq(Self::var_int4(varno, attno), Self::int4_const(value))
            }
        }

        unsafe fn expr_list(cells: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
            unsafe { Self::nodes(1).expr_list(cells) }
        }
    }

    /// Synthetic `CustomScan`; `custom_exprs = pushed ++ recheck`, `plan.qual = residual`.
    unsafe fn make_custom_scan_plan(
        relation_oid: pg_sys::Oid,
        varno: pg_sys::Index,
        pushed_exprs: &[*mut pg_sys::Expr],
        recheck_exprs: &[*mut pg_sys::Expr],
        residual_exprs: &[*mut pg_sys::Expr],
        pushed_contracts: &[PushdownContract],
        column_refs: &[ColumnRef],
    ) -> *mut pg_sys::CustomScan {
        unsafe {
            let mut custom_exprs_vec: Vec<*mut pg_sys::Expr> = Vec::new();
            custom_exprs_vec.extend_from_slice(pushed_exprs);
            custom_exprs_vec.extend_from_slice(recheck_exprs);
            let custom_exprs = ExplainExprFixture::expr_list(&custom_exprs_vec);

            let plan_qual = ExplainExprFixture::expr_list(residual_exprs);

            let custom_private = encode_split(
                ExplainProvider::NAME,
                relation_oid,
                pushed_exprs.len(),
                recheck_exprs.len(),
                pushed_contracts,
                column_refs,
                ptr::null_mut(),
            )
            .expect("encode_split: synthetic counts are well within i32::MAX");

            let cscan = pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomScan>())
                as *mut pg_sys::CustomScan;
            (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
            (*cscan).scan.plan.qual = plan_qual;
            (*cscan).scan.scanrelid = varno;
            (*cscan).custom_exprs = custom_exprs;
            (*cscan).custom_private = custom_private;

            cscan
        }
    }

    /// Wrapper with only `base.ss.ps.plan` set; enough for the explain trampoline.
    unsafe fn make_wrapper(
        cscan: *mut pg_sys::CustomScan,
    ) -> *mut CustomScanStateWrapper<ExplainProvider> {
        unsafe {
            let wrapper_ptr = pg_sys::palloc0(core::mem::size_of::<
                CustomScanStateWrapper<ExplainProvider>,
            >())
                as *mut CustomScanStateWrapper<ExplainProvider>;
            assert!(!wrapper_ptr.is_null());
            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
            (*wrapper_ptr).base.ss.ps.plan = cscan.cast::<pg_sys::Plan>();
            wrapper_ptr
        }
    }

    /// Fresh TEXT-format `ExplainState`; mirrors `ExplainQuery` defaults.
    unsafe fn new_explain_state(verbose: bool) -> *mut pg_sys::ExplainState {
        unsafe {
            let es = pg_sys::NewExplainState();
            (*es).format = pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT;
            (*es).verbose = verbose;
            (*es).costs = false;
            (*es).summary = false;
            (*es).analyze = false;
            (*es).buffers = false;
            (*es).timing = false;
            es
        }
    }

    /// Like [`new_explain_state`] but `FORMAT JSON` for the structured path.
    unsafe fn new_explain_state_json(verbose: bool) -> *mut pg_sys::ExplainState {
        unsafe {
            let es = new_explain_state(verbose);
            (*es).format = pg_sys::ExplainFormat::EXPLAIN_FORMAT_JSON;
            es
        }
    }

    /// Wrap JSON output like `ExplainPrintPlan` so the labeled group parses.
    unsafe fn drive_trampoline_json_document(
        wrapper: *mut CustomScanStateWrapper<ExplainProvider>,
        es: *mut pg_sys::ExplainState,
    ) -> String {
        unsafe {
            pg_sys::ExplainBeginOutput(es);
            // Unlabeled outer object hosts the trampoline's labeled group.
            pg_sys::ExplainOpenGroup(c"Plan".as_ptr(), ptr::null(), true, es);

            explain_custom_scan_trampoline::<ExplainProvider>(
                wrapper.cast::<pg_sys::CustomScanState>(),
                ptr::null_mut(),
                es,
            );

            pg_sys::ExplainCloseGroup(c"Plan".as_ptr(), ptr::null(), true, es);
            pg_sys::ExplainEndOutput(es);

            read_explain_str(es)
        }
    }

    unsafe fn read_explain_str(es: *mut pg_sys::ExplainState) -> String {
        unsafe {
            let buf = (*es).str_;
            let data = (*buf).data as *const c_char;
            CStr::from_ptr(data).to_string_lossy().into_owned()
        }
    }

    /// Seed deparse context like `ExplainPrintPlan` before walking the plan tree.
    unsafe fn install_deparse_context(
        es: *mut pg_sys::ExplainState,
        cscan: *mut pg_sys::CustomScan,
        relation_oid: pg_sys::Oid,
        alias: &CStr,
    ) {
        unsafe {
            // Minimal RTE; deparse only needs relid, alias, eref, rtekind, relkind.
            let rte_alias = pg_sys::makeAlias(alias.as_ptr(), ptr::null_mut());
            let rte = pg_sys::palloc0(core::mem::size_of::<pg_sys::RangeTblEntry>())
                as *mut pg_sys::RangeTblEntry;
            (*rte).type_ = pg_sys::NodeTag::T_RangeTblEntry;
            (*rte).rtekind = pg_sys::RTEKind::RTE_RELATION;
            (*rte).relid = relation_oid;
            (*rte).relkind = b'r' as c_char;
            (*rte).rellockmode = pg_sys::AccessShareLock as c_int;
            (*rte).alias = rte_alias;
            (*rte).eref = rte_alias;
            (*rte).lateral = false;
            (*rte).inh = false;
            (*rte).inFromCl = true;

            let mut rtable: *mut pg_sys::List = ptr::null_mut();
            rtable = pg_sys::lappend(rtable, rte.cast::<core::ffi::c_void>());

            let rtable_names =
                pg_sys::select_rtable_names_for_explain(rtable, ptr::null_mut());

            let pstmt = pg_sys::palloc0(core::mem::size_of::<pg_sys::PlannedStmt>())
                as *mut pg_sys::PlannedStmt;
            (*pstmt).type_ = pg_sys::NodeTag::T_PlannedStmt;
            (*pstmt).commandType = pg_sys::CmdType::CMD_SELECT;
            (*pstmt).rtable = rtable;
            (*pstmt).planTree = cscan.cast::<pg_sys::Plan>();

            let deparse_cxt =
                pg_sys::deparse_context_for_plan_tree(pstmt, rtable_names);

            (*es).rtable = rtable;
            (*es).rtable_names = rtable_names;
            (*es).pstmt = pstmt;
            (*es).deparse_cxt = deparse_cxt;
        }
    }

    fn lookup_relation_oid(qualified_name: &str) -> pg_sys::Oid {
        let raw = pgrx::Spi::get_one::<i64>(&format!(
            "SELECT '{qualified_name}'::regclass::oid::int8",
        ))
        .expect("regclass cast failed")
        .expect("regclass returned NULL");
        pg_sys::Oid::from(raw as u32)
    }

    // ----------------------------------------------------------------------
    // Scenario model: a format-agnostic description of a scan, the materialized
    // PG structures it builds, and the per-test backend harness that renders
    // them. This is what collapses the ~75-line setup block that used to be
    // copy-pasted into every test.
    // ----------------------------------------------------------------------

    /// Format-agnostic scan description shared by example tests and proptest.
    ///
    /// `pushed` carries `(attno, const_value, is_exact)`; `recheck` and
    /// `residual` carry `(attno, const_value)`. Every predicate is `attr = N`.
    #[derive(Clone, Debug)]
    struct ScenarioSpec {
        pushed: Vec<(i16, i32, bool)>,
        recheck: Vec<(i16, i32)>,
        residual: Vec<(i16, i32)>,
    }

    impl ScenarioSpec {
        /// The canonical mixed scenario shared by the four format example
        /// tests: one exact + one conservative pushed predicate, one recheck,
        /// one residual. Deparses to `a = 1`, `b = 2`, `a = 3`, `b = 4`.
        fn example() -> Self {
            Self {
                pushed: vec![(1, 1, true), (2, 2, false)],
                recheck: vec![(1, 3)],
                residual: vec![(2, 4)],
            }
        }
    }

    /// A materialized [`ScenarioSpec`]: the synthetic `CustomScan` + wrapper,
    /// plus the expression vectors (already classified by contract) that serve
    /// as the deparse oracle for assertions.
    struct BuiltScenario {
        cscan: *mut pg_sys::CustomScan,
        wrapper: *mut CustomScanStateWrapper<ExplainProvider>,
        /// `custom_exprs` pushed segment, in plan order.
        pushed: Vec<*mut pg_sys::Expr>,
        /// Pushed subsequence with `ExactRowFilter` contract, in plan order.
        exact: Vec<*mut pg_sys::Expr>,
        /// Pushed subsequence with `ConservativePruning` contract, in plan order.
        conservative: Vec<*mut pg_sys::Expr>,
        recheck: Vec<*mut pg_sys::Expr>,
        residual: Vec<*mut pg_sys::Expr>,
    }

    /// Per-test backend fixture: a TEMP table plus the deparse identity
    /// (`relation_oid`, `varno`, `alias`) that every render path threads. Owns
    /// scenario construction and all three render entry points so callers never
    /// re-pass the `(cscan, relation_oid, alias)` tuple.
    struct ExplainHarness {
        relation_oid: pg_sys::Oid,
        varno: c_int,
        alias: CString,
    }

    impl ExplainHarness {
        /// `CREATE TEMP TABLE <table>(a int4, b int4)` and resolve its OID.
        ///
        /// The table name doubles as the relation alias; it never appears in
        /// single-relation deparse output, so it does not affect expected text.
        fn create(table: &str) -> Self {
            pgrx::Spi::run(&format!("CREATE TEMP TABLE {table}(a int4, b int4)"))
                .expect("CREATE TEMP TABLE failed");
            let relation_oid = lookup_relation_oid(&format!("pg_temp.{table}"));
            let alias =
                CString::new(table).expect("table name contains no interior NUL");
            Self {
                relation_oid,
                varno: 1,
                alias,
            }
        }

        fn alias(&self) -> &CStr {
            self.alias.as_c_str()
        }

        /// Materialize a spec into PG nodes and classify the pushed segment.
        unsafe fn build(&self, spec: &ScenarioSpec) -> BuiltScenario {
            unsafe {
                let pushed: Vec<*mut pg_sys::Expr> = spec
                    .pushed
                    .iter()
                    .map(|&(attno, value, _)| {
                        ExplainExprFixture::eq_pred(self.varno, attno, value)
                    })
                    .collect();
                let contracts: Vec<PushdownContract> = spec
                    .pushed
                    .iter()
                    .map(|&(_, _, exact)| {
                        if exact {
                            PushdownContract::ExactRowFilter
                        } else {
                            PushdownContract::ConservativePruning
                        }
                    })
                    .collect();

                let mut exact = Vec::new();
                let mut conservative = Vec::new();
                for (&expr, &(_, _, is_exact)) in
                    pushed.iter().zip(spec.pushed.iter())
                {
                    if is_exact {
                        exact.push(expr);
                    } else {
                        conservative.push(expr);
                    }
                }

                let recheck: Vec<*mut pg_sys::Expr> = spec
                    .recheck
                    .iter()
                    .map(|&(attno, value)| {
                        ExplainExprFixture::eq_pred(self.varno, attno, value)
                    })
                    .collect();
                let residual: Vec<*mut pg_sys::Expr> = spec
                    .residual
                    .iter()
                    .map(|&(attno, value)| {
                        ExplainExprFixture::eq_pred(self.varno, attno, value)
                    })
                    .collect();

                let cscan = make_custom_scan_plan(
                    self.relation_oid,
                    self.varno as pg_sys::Index,
                    &pushed,
                    &recheck,
                    &residual,
                    &contracts,
                    &[],
                );
                let wrapper = make_wrapper(cscan);

                BuiltScenario {
                    cscan,
                    wrapper,
                    pushed,
                    exact,
                    conservative,
                    recheck,
                    residual,
                }
            }
        }

        /// Drive TEXT EXPLAIN once with a fresh `ExplainState` (so repeated
        /// calls exercise the determinism contract).
        unsafe fn render_text(&self, built: &BuiltScenario, verbose: bool) -> String {
            unsafe {
                let es = new_explain_state(verbose);
                install_deparse_context(
                    es,
                    built.cscan,
                    self.relation_oid,
                    self.alias(),
                );
                explain_custom_scan_trampoline::<ExplainProvider>(
                    built.wrapper.cast::<pg_sys::CustomScanState>(),
                    ptr::null_mut(),
                    es,
                );
                read_explain_str(es)
            }
        }

        /// Drive `FORMAT JSON` EXPLAIN once; returns a complete PG-shaped document.
        unsafe fn render_json(&self, built: &BuiltScenario, verbose: bool) -> String {
            unsafe {
                let es = new_explain_state_json(verbose);
                install_deparse_context(
                    es,
                    built.cscan,
                    self.relation_oid,
                    self.alias(),
                );
                drive_trampoline_json_document(built.wrapper, es)
            }
        }

        /// Deparse `exprs` exactly like the trampoline; the oracle for the
        /// `Pushed Filter` / `Recheck` text and JSON list members.
        unsafe fn deparse_parts(
            &self,
            built: &BuiltScenario,
            exprs: &[*mut pg_sys::Expr],
        ) -> Vec<String> {
            unsafe {
                let es = new_explain_state(/* verbose */ false);
                install_deparse_context(
                    es,
                    built.cscan,
                    self.relation_oid,
                    self.alias(),
                );
                let plan = built.cscan.cast::<pg_sys::Plan>();
                let dpcontext = pg_sys::set_deparse_context_plan(
                    (*es).deparse_cxt,
                    plan,
                    ptr::null_mut(),
                );
                exprs
                    .iter()
                    .map(|&expr| {
                        let exprstr = pg_sys::deparse_expression(
                            expr.cast::<pg_sys::Node>(),
                            dpcontext,
                            false,
                            false,
                        );
                        assert!(
                            !exprstr.is_null(),
                            "deparse_expression returned NULL for an expr",
                        );
                        CStr::from_ptr(exprstr).to_string_lossy().into_owned()
                    })
                    .collect()
            }
        }
    }

    /// Tunable bounds for a [`ScenarioSpec`] proptest strategy.
    ///
    /// Defaults match the common case (attno in `1..=2`, values in `0..=9999`,
    /// each section `0..=4` rows); tests override only the axes they care about
    /// via struct-update syntax.
    struct ScenarioStrategy {
        /// Value range for pushed and recheck constants.
        value: RangeInclusive<i32>,
        /// Value range for residual constants (kept disjoint where a test must
        /// prove residual text never leaks into output).
        residual_value: RangeInclusive<i32>,
        pushed_len: RangeInclusive<usize>,
        recheck_len: RangeInclusive<usize>,
        residual_len: RangeInclusive<usize>,
    }

    impl Default for ScenarioStrategy {
        fn default() -> Self {
            Self {
                value: 0..=9999,
                residual_value: 0..=9999,
                pushed_len: 0..=4,
                recheck_len: 0..=4,
                residual_len: 0..=4,
            }
        }
    }

    impl ScenarioStrategy {
        fn into_strategy(self) -> impl Strategy<Value = ScenarioSpec> {
            let attno = 1i16..=2;
            let value = self.value;
            let residual_value = self.residual_value;
            (
                proptest::collection::vec(
                    (attno.clone(), value.clone(), any::<bool>()),
                    self.pushed_len,
                ),
                proptest::collection::vec((attno.clone(), value), self.recheck_len),
                proptest::collection::vec((attno, residual_value), self.residual_len),
            )
                .prop_map(|(pushed, recheck, residual)| ScenarioSpec {
                    pushed,
                    recheck,
                    residual,
                })
        }
    }

    /// Run `case` over `strategy` with the shared deterministic 256-case config.
    /// Centralizes the `ProptestConfig` + `TestRunner` boilerplate every
    /// property test would otherwise repeat verbatim.
    fn run_property<S>(
        label: &str,
        strategy: S,
        case: impl Fn(S::Value) -> Result<(), TestCaseError>,
    ) where
        S: Strategy,
        S::Value: core::fmt::Debug,
    {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);
        if let Err(err) = runner.run(&strategy, case) {
            panic!("{label} failed: {err}");
        }
    }

    /// Snapshot of a PG list strong enough to detect mutation of the plan tree.
    ///
    /// Pointer identity alone (the old approach) misses in-place edits to node
    /// payloads — a changed `Const` datum, `Var` attno, or the `Integer` /
    /// `String` contents inside `custom_private`. The `nodeToString`
    /// serialization captures the full node structure including those payloads,
    /// while `list_ptr` / `cells` still catch list-header or cell reassignment
    /// that an equal serialization would not.
    #[derive(Clone, Debug, PartialEq)]
    struct ListSnapshot {
        /// List header identity: catches the field being reassigned to a
        /// different list, even a structurally equal one.
        list_ptr: *const pg_sys::List,
        /// Per-cell payload pointers: catches a cell being repointed.
        cells: Vec<*mut core::ffi::c_void>,
        /// Full textual serialization: catches in-place payload edits that
        /// preserve pointers (datum/attno/string-content changes).
        serialized: String,
    }

    /// Snapshot a list's header identity, cell pointers, and `nodeToString`
    /// serialization; a NULL list is treated as empty.
    ///
    /// Safe on every list these tests build: `custom_exprs` / `plan.qual` hold
    /// `OpExpr` / `Var` / `Const`, and `custom_private` holds `String` /
    /// `Integer` / `IntList` / nested `List` / NIL cells — all `outNode`-able.
    unsafe fn snapshot_list(list: *mut pg_sys::List) -> ListSnapshot {
        unsafe {
            if list.is_null() {
                return ListSnapshot {
                    list_ptr: ptr::null(),
                    cells: Vec::new(),
                    serialized: String::new(),
                };
            }
            let len = pg_sys::list_length(list);
            let mut cells = Vec::with_capacity(len.max(0) as usize);
            for i in 0..len {
                cells.push(pg_sys::list_nth(list, i));
            }
            let raw = pg_sys::nodeToString(list.cast::<core::ffi::c_void>());
            assert!(
                !raw.is_null(),
                "nodeToString returned NULL for a non-null list",
            );
            let serialized = CStr::from_ptr(raw).to_string_lossy().into_owned();
            ListSnapshot {
                list_ptr: list as *const pg_sys::List,
                cells,
                serialized,
            }
        }
    }

    // ----------------------------------------------------------------------
    // Example tests: a single concrete scenario per render mode, asserting the
    // exact bytes / JSON shape. They share `ScenarioSpec::example()`.
    // ----------------------------------------------------------------------

    /// Default TEXT: single ` AND `-joined `Pushed Filter:` line in plan order.
    #[pg_test]
    fn explain_text_pushed_filter_line() {
        let harness = ExplainHarness::create("explain_pushed_filter_line_t");
        unsafe {
            let built = harness.build(&ScenarioSpec::example());
            let output = harness.render_text(&built, /* verbose */ false);
            // PG parenthesizes every `OpExpr` in deparse output (`ruleutils.c`).
            let expected = "Pushed Filter: (a = 1) AND (b = 2)\n";
            assert_eq!(
                output, expected,
                "default TEXT must emit a single ` AND `-joined `Pushed \
                 Filter:` line in custom_exprs order \
                 (got {output:?}, expected {expected:?})",
            );
        }
    }

    /// Default TEXT with no pushed predicate emits nothing. This also pins the
    /// `need_deparse == false` short-circuit: the callback is driven with NO
    /// deparse context installed and must still produce empty output (the
    /// distinct value over the empty-pushed property test, which installs one).
    #[pg_test]
    fn explain_text_no_pushed_emits_nothing() {
        unsafe {
            // Const-only predicates so that, even if some future change tried to
            // deparse them, no Var would require the (absent) deparse context.
            let recheck = [ExplainExprFixture::op_int4eq(
                ExplainExprFixture::int4_const(1),
                ExplainExprFixture::int4_const(1),
            )];
            let residual = [ExplainExprFixture::op_int4eq(
                ExplainExprFixture::int4_const(2),
                ExplainExprFixture::int4_const(2),
            )];

            let cscan = make_custom_scan_plan(
                pg_sys::Oid::from(1259u32),
                1,
                /* pushed */ &[],
                &recheck,
                &residual,
                /* pushed_contracts */ &[],
                /* column_refs */ &[],
            );
            let wrapper = make_wrapper(cscan);

            let es = new_explain_state(/* verbose */ false);
            explain_custom_scan_trampoline::<ExplainProvider>(
                wrapper.cast::<pg_sys::CustomScanState>(),
                ptr::null_mut(),
                es,
            );

            let output = read_explain_str(es);
            assert_eq!(
                output, "",
                "default TEXT with no pushed predicate must emit nothing and \
                 must not require a deparse context (got {output:?})",
            );
        }
    }

    /// VERBOSE TEXT: purpose/provider lines plus per-class labelled predicates.
    #[pg_test]
    fn explain_verbose_classified_predicates() {
        let harness = ExplainHarness::create("explain_classified_predicates_t");
        unsafe {
            let built = harness.build(&ScenarioSpec::example());
            let output = harness.render_text(&built, /* verbose */ true);

            let expected = "Scan Purpose: Query\n\
                            Provider: explain-output-test-provider\n\
                            Pushed Filter Exact: (a = 1)\n\
                            Pushed Filter Conservative Pruning: (b = 2)\n\
                            Recheck: (a = 3)\n";
            assert_eq!(
                output, expected,
                "VERBOSE output must carry Scan Purpose + Provider + per-class labelled \
                 predicate lines with no count lines \
                 (got {output:?}, expected {expected:?})",
            );
        }
    }

    /// JSON non-VERBOSE: well-formed document with `Pushed Filter` list only.
    #[pg_test]
    fn explain_json_well_formed() {
        let harness = ExplainHarness::create("explain_json_well_formed_t");
        unsafe {
            let built = harness.build(&ScenarioSpec::example());
            let document = harness.render_json(&built, /* verbose */ false);

            let parsed: serde_json::Value = serde_json::from_str(&document)
                .unwrap_or_else(|err| {
                    panic!(
                        "structured JSON output must parse without error \
                         (err = {err}, document = {document:?})",
                    )
                });

            let group = &parsed[0]["Lakebase Pushdown"];
            assert!(
                group.is_object(),
                "expected a `Lakebase Pushdown` object group \
                 (document = {document:?})",
            );

            let pushed_filter = &group["Pushed Filter"];
            assert_eq!(
                pushed_filter,
                &serde_json::json!(["(a = 1)", "(b = 2)"]),
                "`Pushed Filter` list members must match the deparsed \
                 predicates in custom_exprs order (document = {document:?})",
            );

            let obj = group.as_object().expect("group is an object");
            assert_eq!(
                obj.len(),
                1,
                "non-VERBOSE structured output must contain exactly the \
                 `Pushed Filter` list (document = {document:?})",
            );
            assert!(
                pushed_filter.is_array(),
                "`Pushed Filter` must be a list, never a scalar \
                 (document = {document:?})",
            );
        }
    }

    /// JSON VERBOSE: `Provider` scalar plus per-class list properties.
    #[pg_test]
    fn explain_json_well_formed_verbose() {
        let harness = ExplainHarness::create("explain_json_well_formed_verbose_t");
        unsafe {
            let built = harness.build(&ScenarioSpec::example());
            let document = harness.render_json(&built, /* verbose */ true);

            let parsed: serde_json::Value = serde_json::from_str(&document)
                .unwrap_or_else(|err| {
                    panic!(
                        "structured VERBOSE JSON output must parse without \
                         error (err = {err}, document = {document:?})",
                    )
                });

            let group = &parsed[0]["Lakebase Pushdown"];
            let obj = group.as_object().unwrap_or_else(|| {
                panic!(
                    "expected a `Lakebase Pushdown` object group \
                     (document = {document:?})",
                )
            });

            let provider = &group["Provider"];
            assert_eq!(
                provider,
                &serde_json::json!("explain-output-test-provider"),
                "VERBOSE structured output must carry a `Provider` scalar \
                 (document = {document:?})",
            );
            assert!(
                provider.is_string(),
                "`Provider` must be a scalar string, never a list \
                 (document = {document:?})",
            );

            assert_eq!(
                &group["Pushed Filter Exact"],
                &serde_json::json!(["(a = 1)"]),
                "`Pushed Filter Exact` list mismatch (document = {document:?})",
            );
            assert_eq!(
                &group["Pushed Filter Conservative Pruning"],
                &serde_json::json!(["(b = 2)"]),
                "`Pushed Filter Conservative Pruning` list mismatch (document = {document:?})",
            );
            assert_eq!(
                &group["Recheck"],
                &serde_json::json!(["(a = 3)"]),
                "`Recheck` list mismatch (document = {document:?})",
            );

            assert_eq!(
                obj.len(),
                4,
                "VERBOSE structured output must contain exactly \
                 {{Provider, Pushed Filter Exact, Pushed Filter Conservative Pruning, \
                 Recheck}} (document = {document:?})",
            );
            for (key, value) in obj {
                if key == "Provider" {
                    assert!(
                        value.is_string(),
                        "`{key}` must stay a scalar (document = {document:?})",
                    );
                } else {
                    assert!(
                        value.is_array(),
                        "`{key}` must be a list, never a scalar \
                         (document = {document:?})",
                    );
                }
            }
        }
    }

    // ----------------------------------------------------------------------
    // Property-based tests. Each builds a scenario via the harness, then makes
    // its own distinct assertion; all setup boilerplate lives in `build`.
    // ----------------------------------------------------------------------

    /// Repeated trampoline calls with fresh `ExplainState` are
    /// byte-for-byte identical across TEXT, VERBOSE, and JSON (both verbosities).
    #[pg_test]
    fn explain_output_is_deterministic() {
        let harness = ExplainHarness::create("explain_determinism_t");
        let strategy = ScenarioStrategy {
            value: i32::MIN..=i32::MAX,
            residual_value: i32::MIN..=i32::MAX,
            ..ScenarioStrategy::default()
        }
        .into_strategy();

        run_property("explain output determinism", strategy, |spec| unsafe {
            let built = harness.build(&spec);
            for verbose in [false, true] {
                let text_a = harness.render_text(&built, verbose);
                let text_b = harness.render_text(&built, verbose);
                prop_assert_eq!(
                    &text_a,
                    &text_b,
                    "TEXT (verbose={}) must be byte-for-byte identical \
                         across two fresh ExplainState runs",
                    verbose
                );

                let json_a = harness.render_json(&built, verbose);
                let json_b = harness.render_json(&built, verbose);
                prop_assert_eq!(
                    &json_a,
                    &json_b,
                    "FORMAT JSON (verbose={}) must be byte-for-byte \
                         identical across two fresh ExplainState runs",
                    verbose
                );
            }
            Ok(())
        });
    }

    /// Non-empty pushed yields exactly one ` AND `-joined
    /// `Pushed Filter:` line in plan order, with no legacy tokens.
    #[pg_test]
    fn explain_text_pushed_filter_shape() {
        let harness = ExplainHarness::create("explain_pushed_filter_shape_t");
        let strategy = ScenarioStrategy {
            pushed_len: 1..=5,
            ..ScenarioStrategy::default()
        }
        .into_strategy();

        run_property(
            "default TEXT Pushed Filter shape",
            strategy,
            |spec| unsafe {
                let built = harness.build(&spec);
                let output = harness.render_text(&built, /* verbose */ false);

                let parts = harness.deparse_parts(&built, &built.pushed);
                let expected = format!("Pushed Filter: {}\n", parts.join(" AND "));
                prop_assert_eq!(
                    &output,
                    &expected,
                    "default TEXT with non-empty pushed must be exactly a \
                     single ` AND `-joined `Pushed Filter:` line in \
                     custom_exprs order"
                );

                for token in [
                    "Lakebase Pushdown",
                    "Pushed Exact",
                    "Pushed Conservative Pruning",
                    "Residual",
                    "Provider:",
                ] {
                    prop_assert!(
                        !output.contains(token),
                        "default TEXT output must not contain legacy token \
                         {:?} (output={:?})",
                        token,
                        output
                    );
                }
                Ok(())
            },
        );
    }

    /// Empty pushed emits nothing in default TEXT mode, regardless
    /// of recheck/residual content.
    #[pg_test]
    fn explain_text_empty_pushed_emits_nothing_property() {
        let harness = ExplainHarness::create("explain_empty_pushed_t");
        let strategy = ScenarioStrategy {
            value: i32::MIN..=i32::MAX,
            residual_value: i32::MIN..=i32::MAX,
            pushed_len: 0..=0,
            ..ScenarioStrategy::default()
        }
        .into_strategy();

        run_property(
            "default TEXT empty pushed emits nothing",
            strategy,
            |spec| unsafe {
                let built = harness.build(&spec);
                let output = harness.render_text(&built, /* verbose */ false);

                prop_assert_eq!(
                    &output,
                    "",
                    "default TEXT with no pushed predicate must emit nothing \
                     (output={:?})",
                    output
                );

                for token in [
                    "Pushed Filter",
                    "Lakebase Pushdown",
                    "Pushed Exact",
                    "Pushed Conservative Pruning",
                    "Recheck",
                    "Residual",
                    "Provider:",
                ] {
                    prop_assert!(
                        !output.contains(token),
                        "default TEXT output must not contain token {:?} \
                         (output={:?})",
                        token,
                        output
                    );
                }
                Ok(())
            },
        );
    }

    /// VERBOSE TEXT carries exactly one purpose and Provider line plus one
    /// ` AND `-joined labelled line per non-empty class, matching the oracle.
    #[pg_test]
    fn explain_verbose_classification_property() {
        let harness = ExplainHarness::create("explain_verbose_classification_t");
        let strategy = ScenarioStrategy {
            pushed_len: 0..=5,
            ..ScenarioStrategy::default()
        }
        .into_strategy();

        run_property("VERBOSE classification", strategy, |spec| unsafe {
            let built = harness.build(&spec);
            let output = harness.render_text(&built, /* verbose */ true);

            let mut expected = String::from(
                "Scan Purpose: Query\nProvider: explain-output-test-provider\n",
            );
            if !built.exact.is_empty() {
                let parts = harness.deparse_parts(&built, &built.exact);
                expected.push_str(&format!(
                    "Pushed Filter Exact: {}\n",
                    parts.join(" AND ")
                ));
            }
            if !built.conservative.is_empty() {
                let parts = harness.deparse_parts(&built, &built.conservative);
                expected.push_str(&format!(
                    "Pushed Filter Conservative Pruning: {}\n",
                    parts.join(" AND ")
                ));
            }
            if !built.recheck.is_empty() {
                let parts = harness.deparse_parts(&built, &built.recheck);
                expected.push_str(&format!("Recheck: {}\n", parts.join(" AND ")));
            }

            prop_assert_eq!(
                &output,
                &expected,
                "VERBOSE output must carry exactly one purpose and Provider line plus \
                     one ` AND `-joined labelled line per non-empty class \
                     (ExactRowFilter / ConservativePruning / Recheck) in \
                     custom_exprs order"
            );

            for token in
                ["Pushed Exact:", "Pushed Conservative Pruning:", "Residual:"]
            {
                prop_assert!(
                    !output.contains(token),
                    "VERBOSE output must not contain legacy count label \
                         {:?} (output={:?})",
                    token,
                    output
                );
            }
            for line in output.lines() {
                if let Some((_label, value)) = line.split_once(": ") {
                    prop_assert!(
                        value.trim().parse::<i64>().is_err(),
                        "VERBOSE output line {:?} looks like a numeric \
                             count line (output={:?})",
                        line,
                        output
                    );
                }
            }
            Ok(())
        });
    }

    /// Residual (`scan.plan.qual`) deparse text never appears in any
    /// render mode; empty pushed + empty recheck + residual still emits nothing.
    #[pg_test]
    fn explain_does_not_render_residual_property() {
        let harness = ExplainHarness::create("explain_no_residual_t");
        // Residual constants live in a disjoint range so their deparse text
        // cannot accidentally collide with pushed/recheck text.
        let strategy = ScenarioStrategy {
            value: 0..=999,
            residual_value: 100000..=100999,
            residual_len: 1..=4,
            ..ScenarioStrategy::default()
        }
        .into_strategy();

        run_property(
            "callback does not render residual",
            strategy,
            |spec| unsafe {
                let built = harness.build(&spec);
                let residual_texts = harness.deparse_parts(&built, &built.residual);

                let outputs = [
                    ("TEXT default", harness.render_text(&built, false)),
                    ("TEXT VERBOSE", harness.render_text(&built, true)),
                    ("JSON non-VERBOSE", harness.render_json(&built, false)),
                    ("JSON VERBOSE", harness.render_json(&built, true)),
                ];

                for (mode, output) in &outputs {
                    for residual_text in &residual_texts {
                        prop_assert!(
                            !output.contains(residual_text.as_str()),
                            "{} output must not render residual predicate text \
                             {:?} (output={:?})",
                            mode,
                            residual_text,
                            output
                        );
                    }
                }

                if built.pushed.is_empty() && built.recheck.is_empty() {
                    prop_assert_eq!(
                        &outputs[0].1,
                        "",
                        "default TEXT with empty pushed + empty recheck and a \
                         non-empty residual must emit nothing (output={:?})",
                        &outputs[0].1
                    );
                }
                Ok(())
            },
        );
    }

    /// Rendering leaves `custom_exprs`, `custom_private`, and
    /// `scan.plan.qual` unchanged — list header identity, cell pointers, and
    /// the full `nodeToString` serialization (node payloads included) all stay
    /// identical across every render mode.
    #[pg_test]
    fn explain_does_not_mutate_plan_tree_property() {
        let harness = ExplainHarness::create("explain_no_mutate_t");
        let strategy = ScenarioStrategy::default().into_strategy();

        run_property(
            "render does not mutate plan tree",
            strategy,
            |spec| unsafe {
                let built = harness.build(&spec);

                let baseline_exprs = snapshot_list((*built.cscan).custom_exprs);
                let baseline_private = snapshot_list((*built.cscan).custom_private);
                let baseline_qual = snapshot_list((*built.cscan).scan.plan.qual);

                let configs: [(&str, bool, bool); 4] = [
                    ("TEXT default", false, false),
                    ("TEXT VERBOSE", true, false),
                    ("JSON non-VERBOSE", false, true),
                    ("JSON VERBOSE", true, true),
                ];

                for (mode, verbose, json) in configs {
                    if json {
                        let _ = harness.render_json(&built, verbose);
                    } else {
                        let _ = harness.render_text(&built, verbose);
                    }

                    prop_assert_eq!(
                        &snapshot_list((*built.cscan).custom_exprs),
                        &baseline_exprs,
                        "{} render must not mutate custom_exprs (list header, \
                         cell pointers, or serialized node payload)",
                        mode
                    );
                    prop_assert_eq!(
                        &snapshot_list((*built.cscan).custom_private),
                        &baseline_private,
                        "{} render must not mutate custom_private (list \
                         header, cell pointers, or serialized node payload)",
                        mode
                    );
                    prop_assert_eq!(
                        &snapshot_list((*built.cscan).scan.plan.qual),
                        &baseline_qual,
                        "{} render must not mutate scan.plan.qual (list \
                         header, cell pointers, or serialized node payload)",
                        mode
                    );
                }
                Ok(())
            },
        );
    }

    /// JSON output parses, and each class list/scalar matches the
    /// deparse oracle in plan order, for both verbosities.
    #[pg_test]
    fn explain_json_information_equivalent_property() {
        let harness = ExplainHarness::create("explain_json_information_t");
        let strategy = ScenarioStrategy {
            pushed_len: 0..=5,
            ..ScenarioStrategy::default()
        }
        .into_strategy();

        run_property(
            "structured JSON well-formed + information equivalent",
            strategy,
            |spec| unsafe {
                let built = harness.build(&spec);

                // --- non-VERBOSE: `Pushed Filter` list only, no scalars. ---
                let document = harness.render_json(&built, /* verbose */ false);
                let parsed: serde_json::Value = match serde_json::from_str(&document)
                {
                    Ok(value) => value,
                    Err(err) => {
                        return Err(TestCaseError::fail(format!(
                            "non-VERBOSE structured JSON must parse without \
                             error (err={err}, document={document:?})",
                        )));
                    }
                };

                let group = &parsed[0]["Lakebase Pushdown"];
                let obj = match group.as_object() {
                    Some(obj) => obj,
                    None => {
                        return Err(TestCaseError::fail(format!(
                            "expected a `Lakebase Pushdown` object group \
                             (document={document:?})",
                        )));
                    }
                };

                let pushed_parts = harness.deparse_parts(&built, &built.pushed);
                let mut expected_keys: Vec<&str> = Vec::new();
                if !built.pushed.is_empty() {
                    expected_keys.push("Pushed Filter");
                    prop_assert_eq!(
                        &group["Pushed Filter"],
                        &serde_json::json!(pushed_parts),
                        "non-VERBOSE `Pushed Filter` list members must match \
                         the deparsed predicates in custom_exprs order \
                         (document={:?})",
                        document
                    );
                }

                // Strict key-set equivalence: non-VERBOSE must expose exactly
                // the non-empty `Pushed Filter` list and nothing else — no
                // stray `Recheck: []`, no `Provider` scalar.
                let mut actual_keys: Vec<&str> =
                    obj.keys().map(|key| key.as_str()).collect();
                actual_keys.sort_unstable();
                expected_keys.sort_unstable();
                prop_assert_eq!(
                    &actual_keys,
                    &expected_keys,
                    "non-VERBOSE structured output must contain exactly the \
                     expected key set (document={:?})",
                    document
                );

                for (key, value) in obj {
                    prop_assert!(
                        value.is_array(),
                        "non-VERBOSE structured key {:?} must be a list, never \
                         a scalar (document={:?})",
                        key,
                        document
                    );
                }

                // --- VERBOSE: `Provider` scalar + per-class lists. ---
                let vdocument = harness.render_json(&built, /* verbose */ true);
                let vparsed: serde_json::Value =
                    match serde_json::from_str(&vdocument) {
                        Ok(value) => value,
                        Err(err) => {
                            return Err(TestCaseError::fail(format!(
                                "VERBOSE structured JSON must parse without \
                                 error (err={err}, document={vdocument:?})",
                            )));
                        }
                    };

                let vgroup = &vparsed[0]["Lakebase Pushdown"];
                let vobj = match vgroup.as_object() {
                    Some(obj) => obj,
                    None => {
                        return Err(TestCaseError::fail(format!(
                            "expected a `Lakebase Pushdown` object group \
                             (document={vdocument:?})",
                        )));
                    }
                };

                prop_assert_eq!(
                    &vgroup["Provider"],
                    &serde_json::json!("explain-output-test-provider"),
                    "VERBOSE structured output must carry a `Provider` scalar \
                     (document={:?})",
                    vdocument
                );

                let exact_parts = harness.deparse_parts(&built, &built.exact);
                let conservative_parts =
                    harness.deparse_parts(&built, &built.conservative);
                let recheck_parts = harness.deparse_parts(&built, &built.recheck);

                // `Provider` is always present; each class key appears iff its
                // segment is non-empty. Build the expected key set while
                // checking each present key's content, then assert exact
                // equivalence so a stray empty-class key cannot slip through.
                let mut expected_keys: Vec<&str> = vec!["Provider"];
                if !built.exact.is_empty() {
                    expected_keys.push("Pushed Filter Exact");
                    prop_assert_eq!(
                        &vgroup["Pushed Filter Exact"],
                        &serde_json::json!(exact_parts),
                        "`Pushed Filter Exact` list must match the exact \
                         subsequence in custom_exprs order (document={:?})",
                        vdocument
                    );
                }
                if !built.conservative.is_empty() {
                    expected_keys.push("Pushed Filter Conservative Pruning");
                    prop_assert_eq!(
                        &vgroup["Pushed Filter Conservative Pruning"],
                        &serde_json::json!(conservative_parts),
                        "`Pushed Filter Conservative Pruning` list must match \
                         the conservative-pruning subsequence in custom_exprs \
                         order (document={:?})",
                        vdocument
                    );
                }
                if !built.recheck.is_empty() {
                    expected_keys.push("Recheck");
                    prop_assert_eq!(
                        &vgroup["Recheck"],
                        &serde_json::json!(recheck_parts),
                        "`Recheck` list must match the recheck segment in \
                         custom_exprs order (document={:?})",
                        vdocument
                    );
                }

                let mut actual_keys: Vec<&str> =
                    vobj.keys().map(|key| key.as_str()).collect();
                actual_keys.sort_unstable();
                expected_keys.sort_unstable();
                prop_assert_eq!(
                    &actual_keys,
                    &expected_keys,
                    "VERBOSE structured output must contain exactly the \
                     expected key set ({{Provider}} plus one key per non-empty \
                     class) (document={:?})",
                    vdocument
                );

                for (key, value) in vobj {
                    if key == "Provider" {
                        prop_assert!(
                            value.is_string(),
                            "`{}` must stay a scalar string (document={:?})",
                            key,
                            vdocument
                        );
                    } else {
                        prop_assert!(
                            value.is_array(),
                            "VERBOSE structured key {:?} must be a list, never \
                             a scalar (document={:?})",
                            key,
                            vdocument
                        );
                    }
                }
                Ok(())
            },
        );
    }
}
