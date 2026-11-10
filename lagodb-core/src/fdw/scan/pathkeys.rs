//! PostgreSQL pathkey analysis for ordered foreign base-relation paths.

use pgrx::pg_sys;

use crate::expr::{
    PgOrderingDirection,
    inspect::{RelationExprAnalyzer, RelationScope},
};

use super::error::ForeignScanError;

/// One PostgreSQL pathkey member selected for a foreign scan.
///
/// The expression pointer is planner-owned and is valid only while the
/// planner callback that supplied this value is running.  Providers may use it
/// to deparse their remote ordering expression, but must not retain it in
/// planner state or plan private data.
#[derive(Debug, Clone, Copy)]
pub struct ForeignPathKey {
    expression: *mut pg_sys::Expr,
    data_type: pg_sys::Oid,
    collation: pg_sys::Oid,
    opfamily: pg_sys::Oid,
    direction: PgOrderingDirection,
    nulls_first: bool,
}

impl ForeignPathKey {
    /// Expression represented by the selected `EquivalenceMember`.
    #[inline]
    pub fn expression(&self) -> *mut pg_sys::Expr {
        self.expression
    }

    /// Nominal datatype used by PostgreSQL when looking up the ordering
    /// operator in the pathkey's operator family.
    #[inline]
    pub fn data_type(&self) -> pg_sys::Oid {
        self.data_type
    }

    /// Collation attached to the pathkey's equivalence class.
    #[inline]
    pub fn collation(&self) -> pg_sys::Oid {
        self.collation
    }

    /// B-tree operator family defining the ordering.
    #[inline]
    pub fn opfamily(&self) -> pg_sys::Oid {
        self.opfamily
    }

    /// Direction represented by the PostgreSQL pathkey.
    #[inline]
    pub fn direction(&self) -> PgOrderingDirection {
        self.direction
    }

    /// Whether NULL values precede non-NULL values.
    #[inline]
    pub fn nulls_first(&self) -> bool {
        self.nulls_first
    }
}

/// Validated pathkeys selected for one foreign scan path.
///
/// Each pathkey retains every non-constant, non-system-column EC member that
/// belongs only to the scanned relation.  PostgreSQL's own early-sort gate
/// also confirms that the relation target can represent the ordering locally.
/// The provider selects one candidate after applying its remote shippability
/// rules. Only the selected candidate is exposed by [`iter`]. Candidate order
/// is the order of the canonical PostgreSQL `EquivalenceClass` member list;
/// it does not depend on the relation target and is rebuilt in each planner
/// phase.
#[derive(Debug, Default)]
pub struct ForeignPathKeys {
    candidates: Vec<Vec<ForeignPathKey>>,
    selected: Vec<usize>,
}

impl ForeignPathKeys {
    /// Number of pathkeys in the selected ordering.
    #[inline]
    pub fn len(&self) -> usize {
        self.candidates.len()
    }

    /// Whether the path has no known ordering.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    /// Iterate over the provider-selected pathkey metadata.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &ForeignPathKey> {
        self.candidates
            .iter()
            .zip(&self.selected)
            .map(|(candidates, &selected)| &candidates[selected])
    }

    /// Number of relation-local EC member candidates for one pathkey.
    #[inline]
    pub fn candidate_count(&self, pathkey_index: usize) -> usize {
        self.candidates
            .get(pathkey_index)
            .map_or(0, |candidates| candidates.len())
    }

    /// Return one relation-local EC member candidate for provider inspection.
    #[inline]
    pub fn candidate(
        &self,
        pathkey_index: usize,
        candidate_index: usize,
    ) -> Option<&ForeignPathKey> {
        self.candidates
            .get(pathkey_index)
            .and_then(|candidates| candidates.get(candidate_index))
    }

    /// Select the EC member that the provider can express remotely.
    ///
    /// Providers should inspect [`candidate`](Self::candidate) and apply their
    /// own expression, operator-family, collation, and NULL-ordering rules
    /// before selecting a candidate.
    ///
    /// # Errors
    ///
    /// Returns a framework error when either index is outside the analyzed
    /// pathkey candidate set.
    pub fn select_candidate(
        &mut self,
        pathkey_index: usize,
        candidate_index: usize,
    ) -> Result<(), ForeignScanError> {
        let Some(candidate_count) = self
            .candidates
            .get(pathkey_index)
            .map(|candidates| candidates.len())
        else {
            return Err(ForeignScanError::framework(
                "FDW pathkey candidate index is outside the pathkey list",
            ));
        };
        if candidate_index >= candidate_count {
            return Err(ForeignScanError::framework(
                "FDW pathkey member candidate index is outside the EC member list",
            ));
        }
        self.selected[pathkey_index] = candidate_index;
        Ok(())
    }

    /// Analyze a PostgreSQL pathkey list for a base relation.
    ///
    /// `None` means that PostgreSQL supplied a structurally valid pathkey list
    /// for which this framework could not find a relation-local EC member or
    /// PostgreSQL could not represent the ordering from the relation target.
    /// The caller should reject that path candidate before `add_path()`.
    ///
    /// # Safety
    ///
    /// `root`, `baserel`, and `pathkeys` must be planner-owned nodes from the
    /// same planning invocation. `pathkeys` may be NULL, which represents an
    /// unordered path. Every non-NULL pointer reachable from `pathkeys` must
    /// remain live for the returned value's lifetime.
    pub(crate) unsafe fn analyze(
        root: *mut pg_sys::PlannerInfo,
        baserel: *mut pg_sys::RelOptInfo,
        pathkeys: *mut pg_sys::List,
    ) -> Result<Option<Self>, ForeignScanError> {
        unsafe { Self::analyze_with_local_sort(root, baserel, pathkeys, true) }
    }

    /// Rebuild the planner-only candidate view for `GetForeignPlan`.
    ///
    /// The path has already passed PostgreSQL's early-sort gate.  PostgreSQL
    /// may replace `baserel->reltarget` before plan construction, so this
    /// phase must not use that target to identify the provider's remote EC
    /// member.  It still validates the pathkey structure and relation-local
    /// candidate contract before the provider repeats its remote validation.
    ///
    /// # Safety
    ///
    /// `root`, `baserel`, and `pathkeys` must be planner-owned nodes from the
    /// same planning invocation. `pathkeys` may be NULL, which represents an
    /// unordered path. Every non-NULL pointer reachable from `pathkeys` must
    /// remain live for the returned value's lifetime.
    pub(crate) unsafe fn reanalyze_for_plan(
        root: *mut pg_sys::PlannerInfo,
        baserel: *mut pg_sys::RelOptInfo,
        pathkeys: *mut pg_sys::List,
    ) -> Result<Option<Self>, ForeignScanError> {
        unsafe { Self::analyze_with_local_sort(root, baserel, pathkeys, false) }
    }

    /// Analyze the pathkey list and optionally apply PostgreSQL's early-sort
    /// gate used while adding a path.
    ///
    /// # Safety
    ///
    /// `root`, `baserel`, and `pathkeys` must be planner-owned nodes from the
    /// same planning invocation. `pathkeys` may be NULL, which represents an
    /// unordered path. Every non-NULL pointer reachable from `pathkeys` must
    /// remain live for the returned value's lifetime.
    unsafe fn analyze_with_local_sort(
        root: *mut pg_sys::PlannerInfo,
        baserel: *mut pg_sys::RelOptInfo,
        pathkeys: *mut pg_sys::List,
        check_local_sort: bool,
    ) -> Result<Option<Self>, ForeignScanError> {
        if pathkeys.is_null() {
            return Ok(Some(Self::default()));
        }
        let relation_relids = unsafe { (*baserel).relids };

        let pathkey_list = unsafe { &*pathkeys };
        let length = pathkey_list.length;
        if length == 0 {
            return Ok(Some(Self::default()));
        }
        let mut candidates = Vec::with_capacity(length as usize);
        for index in 0..length {
            let pathkey =
                unsafe { pg_sys::list_nth(pathkeys, index) } as *mut pg_sys::PathKey;

            let mut eclass = unsafe { (*pathkey).pk_eclass };
            while unsafe { !(*eclass).ec_merged.is_null() } {
                eclass = unsafe { (*eclass).ec_merged };
            }
            if unsafe { (*eclass).ec_has_volatile } {
                return Ok(None);
            }
            // `prepare_sort_from_pathkeys` may need to build a local sort key
            // when this ordered path is consumed by Sort, MergeAppend, or
            // Gather Merge.  Use PostgreSQL's own target/member test for that
            // contract; the complete candidate list below remains the remote
            // provider's shippability choice.
            if check_local_sort {
                let can_be_sorted_early = unsafe {
                    pg_sys::relation_can_be_sorted_early(root, baserel, eclass, false)
                };
                if !can_be_sorted_early {
                    return Ok(None);
                }
            }
            let Some(direction) =
                PgOrderingDirection::from_path_key(unsafe { &*pathkey })
            else {
                return Ok(None);
            };
            if unsafe { (*pathkey).pk_opfamily } == pg_sys::InvalidOid {
                return Ok(None);
            }
            if !unsafe {
                pg_sys::list_member_oid(
                    (*eclass).ec_opfamilies,
                    (*pathkey).pk_opfamily,
                )
            } {
                return Ok(None);
            }

            let members =
                unsafe { Self::find_relation_members(eclass, relation_relids) };
            if members.is_empty() {
                return Ok(None);
            }
            let pathkey_candidates = members
                .into_iter()
                .map(|member| ForeignPathKey {
                    expression: unsafe { (*member).em_expr },
                    data_type: unsafe { (*member).em_datatype },
                    collation: unsafe { (*eclass).ec_collation },
                    opfamily: unsafe { (*pathkey).pk_opfamily },
                    direction,
                    nulls_first: unsafe { (*pathkey).pk_nulls_first },
                })
                .collect::<Vec<_>>();
            candidates.push(pathkey_candidates);
        }

        let selected = vec![0; candidates.len()];
        Ok(Some(Self {
            candidates,
            selected,
        }))
    }

    pub(crate) fn expressions(&self) -> impl Iterator<Item = *mut pg_sys::Expr> + '_ {
        self.iter().map(|pathkey| pathkey.expression())
    }

    /// Find relation-local EC members for remote ordering in PostgreSQL's
    /// canonical EC order. The complete EC scan retains remote-only
    /// expressions that a provider can deparse even when PostgreSQL need not
    /// evaluate them locally. The local-sort capability gate is performed by
    /// `relation_can_be_sorted_early` before this method is called.
    ///
    /// # Safety
    ///
    /// All arguments must be live planner nodes from one planning invocation.
    unsafe fn find_relation_members(
        eclass: *mut pg_sys::EquivalenceClass,
        relation_relids: *mut pg_sys::Bitmapset,
    ) -> Vec<*mut pg_sys::EquivalenceMember> {
        let mut members = Vec::new();
        let analyzer =
            RelationExprAnalyzer::new(RelationScope::relids(relation_relids));

        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        {
            let ec_members = unsafe { (*eclass).ec_members };
            let length = unsafe { pg_sys::list_length(ec_members) };
            for index in 0..length {
                let member = unsafe { pg_sys::list_nth(ec_members, index) }
                    as *mut pg_sys::EquivalenceMember;
                if unsafe { Self::usable_member(member, relation_relids, &analyzer) }
                {
                    members.push(member);
                }
            }
        }

        #[cfg(feature = "pg18")]
        {
            let mut iterator = pg_sys::EquivalenceMemberIterator::default();
            unsafe {
                pg_sys::setup_eclass_member_iterator(
                    &mut iterator,
                    eclass,
                    relation_relids,
                )
            };
            loop {
                let member =
                    unsafe { pg_sys::eclass_member_iterator_next(&mut iterator) };
                if member.is_null() {
                    break;
                }
                if unsafe { Self::usable_member(member, relation_relids, &analyzer) }
                {
                    members.push(member);
                }
            }
        }

        members
    }

    /// # Safety
    ///
    /// `member` and `relation_relids` must be live planner nodes.
    unsafe fn usable_member(
        member: *mut pg_sys::EquivalenceMember,
        relation_relids: *mut pg_sys::Bitmapset,
        analyzer: &RelationExprAnalyzer,
    ) -> bool {
        let member = unsafe { &*member };
        if member.em_is_const
            || member.em_datatype == pg_sys::InvalidOid
            || member.em_relids.is_null()
            || unsafe {
                pg_sys::bms_membership(member.em_relids)
                    == pg_sys::BMS_Membership::BMS_EMPTY_SET
            }
        {
            return false;
        }
        let usage = unsafe { analyzer.collect_expr(member.em_expr) };
        if !usage.system_attnos().is_empty() {
            return false;
        }
        unsafe { pg_sys::bms_is_subset(member.em_relids, relation_relids) }
    }
}
