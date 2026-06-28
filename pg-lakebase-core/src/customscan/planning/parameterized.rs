//! Enumeration and resolution of parameterized CustomPath variants.

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::customscan::paths::PredicatePlanner;
use crate::customscan::provider::ErasedProvider;
use crate::expr::split::{PlanPushdownSplit, PlannerClauseGate, ScanClauseSource};

#[derive(Clone, Copy)]
pub(super) struct ParameterizedPathResolver {
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    provider: &'static dyn ErasedProvider,
}

impl ParameterizedPathResolver {
    pub(super) fn new(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
        provider: &'static dyn ErasedProvider,
    ) -> Self {
        Self {
            root,
            rel,
            provider,
        }
    }

    /// Resolve PPI and classify `ppi_clauses`; non-empty `outer_relids`.
    ///
    /// # Safety
    ///
    /// Captured planner pointers and `outer_relids` must be live.
    pub(super) unsafe fn resolve_and_split(
        self,
        outer_relids: *mut pg_sys::Bitmapset,
    ) -> PlanPushdownSplit {
        debug_assert!(
            !self.root.is_null(),
            "resolve_and_split_ppi_clauses: root must be non-null"
        );
        debug_assert!(
            !self.rel.is_null(),
            "resolve_and_split_ppi_clauses: rel must be non-null"
        );
        debug_assert!(
            !outer_relids.is_null(),
            "resolve_and_split_ppi_clauses: outer_relids must be non-empty \
             (PG17 represents an empty Bitmapset as NULL)"
        );

        let lateral_relids = unsafe { (*self.rel).lateral_relids };
        let rel_relids = unsafe { (*self.rel).relids };
        debug_assert!(
            unsafe { pg_sys::bms_is_subset(lateral_relids, outer_relids) },
            "resolve_and_split_ppi_clauses: outer_relids must be a superset of lateral_relids"
        );
        debug_assert!(
            !unsafe { pg_sys::bms_overlap(rel_relids, outer_relids) },
            "resolve_and_split_ppi_clauses: outer_relids must not overlap rel->relids"
        );

        let param_info = unsafe {
            pg_sys::get_baserel_parampathinfo(self.root, self.rel, outer_relids)
        };
        debug_assert!(
            !param_info.is_null(),
            "get_baserel_parampathinfo returned NULL for non-empty outer_relids \
             (PG17 contract — see relnode.c)"
        );

        let ppi_clauses = unsafe { (*param_info).ppi_clauses };
        unsafe {
            PredicatePlanner::new(self.root, self.rel, self.provider)
                .split(ppi_clauses, ScanClauseSource::Movable)
        }
    }
}

/// Join-parameterized group; PG owns `outer_relids` and `param_info`.
pub(super) struct ParameterizedPathGroup {
    pub(super) outer_relids: *mut pg_sys::Bitmapset,
    pub(super) ppi_split: PlanPushdownSplit,
}

#[derive(Clone, Copy)]
pub(super) struct ParameterizedPathPlanner {
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    provider: &'static dyn ErasedProvider,
}

impl ParameterizedPathPlanner {
    pub(super) fn new(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
        provider: &'static dyn ErasedProvider,
    ) -> Self {
        Self {
            root,
            rel,
            provider,
        }
    }

    /// JoinParameterized variants from `joininfo`; PG owns returned bitmaps/PPI.
    ///
    /// # Safety
    ///
    /// Captured planner pointers and `joininfo` must be live in one planner context.
    pub(super) unsafe fn enumerate(
        self,
        joininfo: *mut pg_sys::List,
    ) -> Vec<ParameterizedPathGroup> {
        debug_assert!(
            !self.rel.is_null(),
            "parameterized-path rel must be non-null"
        );
        debug_assert!(
            !self.root.is_null(),
            "parameterized-path root must be non-null"
        );

        let lateral_relids = unsafe { (*self.rel).lateral_relids };
        let rel_relids = unsafe { (*self.rel).relids };

        let mut candidates = unsafe { ParameterizationCandidates::new(self.rel) };
        unsafe {
            candidates.collect_joininfo(joininfo);
            candidates.collect_implied_equalities(self.root);
        }

        let candidates = candidates.into_vec();
        let mut groups: Vec<ParameterizedPathGroup> =
            Vec::with_capacity(candidates.len());
        let resolver =
            ParameterizedPathResolver::new(self.root, self.rel, self.provider);

        for required_outer in candidates {
            debug_assert!(
                unsafe { pg_sys::bms_is_subset(lateral_relids, required_outer) },
                "required_outer must be a superset of lateral_relids"
            );
            debug_assert!(
                !unsafe { pg_sys::bms_overlap(rel_relids, required_outer) },
                "required_outer must not overlap rel->relids"
            );
            debug_assert!(
                !required_outer.is_null(),
                "JoinParameterized required_outer must be non-empty (PG17 represents empty Bitmapsets as NULL)"
            );

            let ppi_split = unsafe { resolver.resolve_and_split(required_outer) };
            groups.push(ParameterizedPathGroup {
                outer_relids: required_outer,
                ppi_split,
            });
        }

        groups
    }
}

struct ParameterizationCandidates {
    rel: *mut pg_sys::RelOptInfo,
    lateral_relids: *mut pg_sys::Bitmapset,
    rel_relids: *mut pg_sys::Bitmapset,
    clause_gate: PlannerClauseGate,
    candidates: Vec<*mut pg_sys::Bitmapset>,
}

impl ParameterizationCandidates {
    /// # Safety
    ///
    /// `rel` must be a live planner-owned `RelOptInfo`.
    unsafe fn new(rel: *mut pg_sys::RelOptInfo) -> Self {
        Self {
            rel,
            lateral_relids: unsafe { (*rel).lateral_relids },
            rel_relids: unsafe { (*rel).relids },
            clause_gate: PlannerClauseGate::for_relation(rel),
            candidates: Vec::new(),
        }
    }

    unsafe fn collect_joininfo(&mut self, joininfo: *mut pg_sys::List) {
        let len = if joininfo.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(joininfo) }
        };
        for i in 0..len {
            let rinfo =
                unsafe { pg_sys::list_nth(joininfo, i) } as *mut pg_sys::RestrictInfo;
            unsafe { self.consider_clause(rinfo) };
        }
    }

    unsafe fn collect_implied_equalities(&mut self, root: *mut pg_sys::PlannerInfo) {
        let implied_eqs = unsafe {
            pg_sys::generate_implied_equalities_for_column(
                root,
                self.rel,
                Some(ec_member_is_scan_var),
                core::ptr::null_mut(),
                (*self.rel).lateral_referencers,
            )
        };
        let implied_len = if implied_eqs.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(implied_eqs) }
        };
        for i in 0..implied_len {
            let rinfo = unsafe { pg_sys::list_nth(implied_eqs, i) }
                as *mut pg_sys::RestrictInfo;
            unsafe { self.consider_clause(rinfo) };
        }
    }

    unsafe fn consider_clause(&mut self, rinfo: *mut pg_sys::RestrictInfo) {
        if rinfo.is_null() {
            return;
        }

        if unsafe { (*rinfo).pseudoconstant } {
            return;
        }

        if !unsafe { self.clause_gate.is_securely_promotable(rinfo) } {
            return;
        }
        if !unsafe { self.clause_gate.is_movable_to_relation(rinfo) } {
            return;
        }

        // outer_relids = lateral ∪ (clause_relids - relids)
        let clause_outer = unsafe {
            pg_sys::bms_difference((*rinfo).clause_relids, self.rel_relids)
        };
        let lateral_copy = unsafe { pg_sys::bms_copy(self.lateral_relids) };
        let candidate = unsafe { pg_sys::bms_union(lateral_copy, clause_outer) };

        // Skip if no new outer rels beyond Plain variant.
        if unsafe { pg_sys::bms_equal(candidate, self.lateral_relids) } {
            return;
        }

        // Dedupe by exact outer_relids equality.
        if self
            .candidates
            .iter()
            .any(|&c| unsafe { pg_sys::bms_equal(c, candidate) })
        {
            return;
        }

        self.candidates.push(candidate);
    }

    fn into_vec(self) -> Vec<*mut pg_sys::Bitmapset> {
        self.candidates
    }
}

/// EC callback: bare scan-column `Var` on this rel.
#[pg_guard]
unsafe extern "C-unwind" fn ec_member_is_scan_var(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    _ec: *mut pg_sys::EquivalenceClass,
    em: *mut pg_sys::EquivalenceMember,
    _arg: *mut core::ffi::c_void,
) -> bool {
    if em.is_null() || rel.is_null() {
        return false;
    }
    let rel_relids = unsafe { (*rel).relids };

    // Strip RelabelType; require bare scan Var on this rel.
    let mut node = unsafe { (*em).em_expr } as *mut pg_sys::Node;
    while !node.is_null()
        && unsafe { (*node).type_ } == pg_sys::NodeTag::T_RelabelType
    {
        node =
            unsafe { (*(node as *mut pg_sys::RelabelType)).arg } as *mut pg_sys::Node;
    }
    if node.is_null() || unsafe { (*node).type_ } != pg_sys::NodeTag::T_Var {
        return false;
    }
    let var = node as *mut pg_sys::Var;
    let varattno = unsafe { (*var).varattno };
    if varattno <= 0 {
        return false;
    }
    let varno = unsafe { (*var).varno };
    unsafe { pg_sys::bms_is_member(varno, rel_relids) }
}
