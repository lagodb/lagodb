//! FDW planner callbacks and base-relation path enumeration.

use core::ffi::c_void;
use core::ptr;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::expr::pushdown::{
    FilterNegotiator, FilterPlanningContext, NegotiatedFilterSet, PathFilterSet,
    ScanClauseSource,
};

use super::super::row_identity::{ForeignRowIdentityRequirement, RowIdentityLayout};
use super::context::{
    ForeignPathContext, ForeignPlanContext, ForeignRelContext, ForeignRelSize,
    ForeignRelSizeContext, PathVariantKind,
};
use super::contract::FdwScan;
use super::error::{ForeignScanError, ForeignScanPhase};
use super::explain::ForeignScanExplain;
use super::filter::ForeignScanFilters;
use super::path_builder::{build_path_variants, expr_list_from_ptrs};
use super::pathkeys::ForeignPathKeys;
use super::private::{DecodedPathPrivate, decode_path_private, encode_scan_private};
use super::projection::{ScanProjectionPolicy, plan_projection};

use super::parameterized::ParameterizedCandidates;

struct PlannerState<P: FdwScan> {
    provider_state: P::PlannerState,
    filter_planner: P::Planner,
    base_filters: PathFilterSet,
}

/// # Safety
///
/// PostgreSQL invokes this callback with a live planner root and base-relation
/// object.  Their planner memory, including `baserel->baserestrictinfo`, must
/// remain live for the duration of the callback.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn get_foreign_rel_size<P: FdwScan>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        if root.is_null() || baserel.is_null() {
            return Err(ForeignScanError::framework(
                "GetForeignRelSize received a NULL planner pointer",
            ));
        }
        if !unsafe { (*baserel).fdw_private }.is_null() {
            return Err(ForeignScanError::framework(
                "GetForeignRelSize was called with an already initialized fdw_private",
            ));
        }
        let relation = unsafe {
            ForeignRelContext::from_raw(&*root, &*baserel, foreigntableid)
        }?;
        let mut provider_state = P::init_planner(&relation)?;
        let filter_context = FilterPlanningContext::new(
            relation.relation_oid(),
            relation.scan_relid(),
            unsafe { pg_sys::get_rel_tablespace(relation.relation_oid()) },
        );
        let mut filter_planner = P::begin_filter_planning(&filter_context)
            .map_err(ForeignScanError::provider)?;
        let base_filters = unsafe {
            negotiate_clauses::<P>(
                &mut filter_planner,
                &relation,
                relation.baserestrictinfo(),
                ScanClauseSource::BaseRestriction,
            )
        }?;
        let rel_size = P::estimate(
            &mut provider_state,
            &ForeignRelSizeContext::new(relation, &base_filters),
        )?;
        validate_rel_size(rel_size)?;

        let planner_state = PlannerState::<P> {
            provider_state,
            filter_planner,
            base_filters,
        };
        let state_ptr = PgMemoryContexts::CurrentMemoryContext
            .leak_and_drop_on_delete(planner_state);
        unsafe {
            (*baserel).fdw_private = state_ptr.cast::<c_void>();
            (*baserel).rows = rel_size.rows;
            if !(*baserel).reltarget.is_null() {
                (*(*baserel).reltarget).width = rel_size.width;
            }
        }
        Ok::<(), ForeignScanError>(())
    })();

    if let Err(error) = result {
        error
            .with_callback_phase::<P>(ForeignScanPhase::RelSize)
            .report_after_switch(prior_ctx);
    }
}

/// # Safety
///
/// PostgreSQL invokes this callback with a live planner root, base relation,
/// relation restriction lists, and `baserel->fdw_private` created by the
/// matching rel-size callback.  Those planner objects must remain live for
/// the duration of the callback.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn get_foreign_paths<P: FdwScan>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        if root.is_null() || baserel.is_null() {
            return Err(ForeignScanError::framework(
                "GetForeignPaths received a NULL planner pointer",
            ));
        }
        let relation = unsafe {
            ForeignRelContext::from_raw(&*root, &*baserel, foreigntableid)
        }?;
        let state_ptr = unsafe { planner_state::<P>(baserel)? };
        // SAFETY: rel-size allocated this planner state in the current
        // planner context and no other callback accesses it concurrently.
        let state = unsafe { &mut *state_ptr };
        let mut emitted = 0usize;

        let lateral_relids = relation.lateral_relids();
        let (plain_required_outer, plain_param_info, plain_filters) =
            if lateral_relids.is_null() {
                (ptr::null_mut(), ptr::null_mut(), state.base_filters.clone())
            } else {
                let required_outer = unsafe { pg_sys::bms_copy(lateral_relids) };
                let param_info = unsafe {
                    pg_sys::get_baserel_parampathinfo(root, baserel, required_outer)
                };
                if param_info.is_null() {
                    return Err(ForeignScanError::framework(
                        "PostgreSQL returned no ParamPathInfo for a non-empty lateral dependency",
                    ));
                }
                let lateral_filters = unsafe {
                    negotiate_clauses::<P>(
                        &mut state.filter_planner,
                        &relation,
                        (*param_info).ppi_clauses,
                        ScanClauseSource::Movable,
                    )
                }?;
                (
                    required_outer,
                    param_info,
                    state.base_filters.merged(&lateral_filters),
                )
            };

        emitted += unsafe {
            build_path_variants::<P>(
                root,
                baserel,
                &relation,
                &state.provider_state,
                PathVariantKind::Plain,
                plain_required_outer,
                plain_param_info,
                &plain_filters,
            )
        }?;

        let candidates =
            unsafe { ParameterizedCandidates::new(baserel).enumerate(root) }?;
        for required_outer in candidates {
            let param_info = unsafe {
                pg_sys::get_baserel_parampathinfo(root, baserel, required_outer)
            };
            if param_info.is_null() {
                return Err(ForeignScanError::framework(
                    "PostgreSQL returned no ParamPathInfo for a parameterized FDW path",
                ));
            }
            let ppi_filters = unsafe {
                negotiate_clauses::<P>(
                    &mut state.filter_planner,
                    &relation,
                    (*param_info).ppi_clauses,
                    ScanClauseSource::Movable,
                )
            }?;
            if !ppi_filters.has_planned_filters() {
                continue;
            }
            let filters = state.base_filters.merged(&ppi_filters);
            emitted += unsafe {
                build_path_variants::<P>(
                    root,
                    baserel,
                    &relation,
                    &state.provider_state,
                    PathVariantKind::JoinParameterized,
                    required_outer,
                    param_info,
                    &filters,
                )
            }?;
        }

        if emitted == 0 {
            return Err(ForeignScanError::unsupported(
                "FDW provider did not build a base-relation foreign path",
            ));
        }
        Ok::<(), ForeignScanError>(())
    })();

    if let Err(error) = result {
        error
            .with_callback_phase::<P>(ForeignScanPhase::Paths)
            .report_after_switch(prior_ctx);
    }
}

/// # Safety
///
/// PostgreSQL invokes this callback with live planner objects and lists from
/// the selected ForeignPath.  The selected path must have been produced by
/// this framework, and all input planner memory must remain live while the
/// ForeignScan is constructed.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn get_foreign_plan<P: FdwScan>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
    best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        if root.is_null() || baserel.is_null() || best_path.is_null() {
            return Err(ForeignScanError::framework(
                "GetForeignPlan received a NULL planner pointer",
            ));
        }
        if !outer_plan.is_null() {
            return Err(ForeignScanError::unsupported(
                "FDW base-scan framework does not accept an outer plan",
            ));
        }
        let relation = unsafe {
            ForeignRelContext::from_raw(&*root, &*baserel, foreigntableid)
        }?;
        let state_ptr = unsafe { planner_state::<P>(baserel)? };
        // SAFETY: rel-size allocated this planner state in the current
        // planner context and no other callback accesses it concurrently.
        let state = unsafe { &mut *state_ptr };
        let path_private: DecodedPathPrivate<P::PrivateData> =
            unsafe { decode_path_private::<P>((*best_path).fdw_private) }?;

        let path_param_info = unsafe { (*best_path).path.param_info };
        if path_private.kind == PathVariantKind::JoinParameterized
            && path_param_info.is_null()
        {
            return Err(ForeignScanError::framework(
                "parameterized FDW path has no ParamPathInfo",
            ));
        }
        let required_outer = if path_param_info.is_null() {
            ptr::null_mut()
        } else {
            unsafe { (*path_param_info).ppi_req_outer }
        };
        if path_private.kind == PathVariantKind::Plain
            && unsafe {
                !pg_sys::bms_equal(required_outer, relation.lateral_relids())
            }
        {
            return Err(ForeignScanError::framework(
                "plain FDW path required_outer differs from the relation's lateral dependencies",
            ));
        }
        let relids = relation.relids();
        if unsafe { pg_sys::bms_overlap(relids, required_outer) } {
            return Err(ForeignScanError::framework(
                "FDW path required_outer overlaps the scanned relation",
            ));
        }
        if !unsafe {
            pg_sys::bms_is_subset(relation.lateral_relids(), required_outer)
        } {
            return Err(ForeignScanError::framework(
                "FDW path required_outer does not include the relation's lateral dependencies",
            ));
        }
        if path_private.kind == PathVariantKind::JoinParameterized
            && unsafe {
                path_param_info.is_null()
                    || (*path_param_info).ppi_req_outer.is_null()
                    || pg_sys::bms_membership((*path_param_info).ppi_req_outer)
                        == pg_sys::BMS_Membership::BMS_EMPTY_SET
            }
        {
            return Err(ForeignScanError::framework(
                "join-parameterized FDW path has an empty required_outer set",
            ));
        }

        let mut pathkeys = unsafe {
            ForeignPathKeys::reanalyze_for_plan(
                root,
                relation.baserel(),
                (*best_path).path.pathkeys,
            )
        }?
        .ok_or_else(|| {
            ForeignScanError::framework(
                "selected FDW path does not satisfy the relation-local pathkey dependency contract",
            )
        })?;
        if !pathkeys.is_empty()
            && path_private.kind == PathVariantKind::JoinParameterized
        {
            return Err(ForeignScanError::framework(
                "selected join-parameterized FDW path declares pathkeys",
            ));
        }

        let path_target_exprs = unsafe {
            let target = (*best_path).path.pathtarget;
            if target.is_null() {
                ptr::null_mut()
            } else {
                (*target).exprs
            }
        };
        let final_filters = unsafe {
            negotiate_final_clauses::<P>(
                &mut state.filter_planner,
                &relation,
                scan_clauses,
            )
        }?;
        let final_path_filters = final_filters.path_set();
        if !pathkeys.is_empty() {
            let path_context = ForeignPathContext::new(
                relation,
                &final_path_filters,
                path_private.kind,
                required_outer,
                path_param_info,
            );
            if !P::supports_pathkeys(
                &state.provider_state,
                &path_context,
                &mut pathkeys,
            )? {
                return Err(ForeignScanError::framework(
                    "provider rejected the selected pathkeys during GetForeignPlan",
                ));
            }
        }
        let residual_quals = unsafe { expr_list_from_ptrs(&final_filters.residual) }?;
        let binding_exprs = unsafe {
            expr_list_from_ptrs(
                &final_filters
                    .bindings
                    .iter()
                    .map(|binding| binding.expr)
                    .collect::<Vec<_>>(),
            )
        }?;
        let recheck_quals = unsafe { expr_list_from_ptrs(&final_filters.recheck) }?;
        let row_identity_requirement = if relation.is_modify_target() {
            let processed_tlist = unsafe { (*relation.root()).processed_tlist };
            let has_item_pointer_identity = unsafe {
                RowIdentityLayout::has_item_pointer_identity_in_targetlist(
                    processed_tlist,
                    relation.scan_relid(),
                )
            }
            .map_err(ForeignScanError::framework)?;
            if has_item_pointer_identity {
                ForeignRowIdentityRequirement::ItemPointer
            } else {
                ForeignRowIdentityRequirement::None
            }
        } else {
            ForeignRowIdentityRequirement::None
        };
        let plan_context = ForeignPlanContext::new(
            relation,
            &final_filters,
            tlist,
            outer_plan,
            path_private.kind,
            required_outer,
            &path_private.private_data,
            &pathkeys,
            row_identity_requirement,
        );
        let plan_spec = P::build_plan(&mut state.provider_state, &plan_context)?;
        unsafe {
            plan_spec
                .fdw_exprs
                .validate_for_scan(relation.root(), relation.scan_relid())?
        };
        let fdw_exprs = unsafe { plan_spec.fdw_exprs.append_to(binding_exprs) };
        let required_columns = plan_spec.required_columns;
        let projection_policy = if pathkeys.is_empty() {
            plan_spec.projection_policy
        } else {
            // `prepare_sort_from_pathkeys` and `setrefs.c` may need relation
            // Vars used by the local EC member.  Relation shape makes those
            // Vars directly addressable without adding a second
            // framework-specific resjunk mapping contract.
            ScanProjectionPolicy::RequireRelationShape
        };
        let private_data = plan_spec.private_data;
        let planned_projection = unsafe {
            plan_projection(
                relation.relation_oid(),
                relation.scan_relid(),
                tlist,
                path_target_exprs,
                &pathkeys,
                residual_quals,
                fdw_exprs,
                recheck_quals,
                projection_policy,
                row_identity_requirement,
                required_columns,
            )
        }?;
        let encoded_filters = ForeignScanFilters::<P>::encode(&final_filters)?;
        let encoded_explain =
            unsafe { ForeignScanExplain::build::<P>(&final_filters) }?.encode()?;
        let private_data = encode_scan_private::<P>(
            P::NAME,
            relation.relation_oid(),
            &private_data,
            &planned_projection.projection,
            &planned_projection.write_plan,
            row_identity_requirement,
            &planned_projection.requirements,
            final_filters.planned.len(),
            final_filters.bindings.len(),
            encoded_filters.planned,
            encoded_filters.bindings,
            encoded_explain,
        )?;
        let scan = unsafe {
            pg_sys::make_foreignscan(
                tlist,
                residual_quals,
                relation.scan_relid(),
                fdw_exprs,
                private_data,
                planned_projection.fdw_scan_tlist,
                recheck_quals,
                outer_plan,
            )
        };
        if scan.is_null() {
            return Err(ForeignScanError::framework(
                "PostgreSQL returned NULL from make_foreignscan",
            ));
        }
        Ok::<*mut pg_sys::ForeignScan, ForeignScanError>(scan)
    })();

    match result {
        Ok(scan) => scan,
        Err(error) => error
            .with_callback_phase::<P>(ForeignScanPhase::Plan)
            .report_after_switch(prior_ctx),
    }
}

/// # Safety
///
/// `baserel` must be the live base relation for a callback whose matching
/// rel-size phase stored a `PlannerState<P>` in `fdw_private`.  No other
/// mutable or immutable reference to the stored planner state may be used
/// while the returned pointer is dereferenced.
unsafe fn planner_state<P: FdwScan>(
    baserel: *mut pg_sys::RelOptInfo,
) -> Result<*mut PlannerState<P>, ForeignScanError> {
    let raw = unsafe { (*baserel).fdw_private };
    if raw.is_null() {
        return Err(ForeignScanError::framework(
            "FDW callback has no planner state in baserel->fdw_private",
        ));
    }
    // The handler's generic callback table is the type witness for this cast;
    // the pointer is allocated in the planner memory context by rel-size.
    Ok(raw.cast::<PlannerState<P>>())
}

/// # Safety
///
/// `relation`, `clauses`, and their planner nodes must remain live for the
/// synchronous splitter traversal.  `clauses` must be NULL or a PostgreSQL
/// list of the `RestrictInfo` nodes identified by `source`.
unsafe fn negotiate_clauses<P: FdwScan>(
    planner: &mut P::Planner,
    relation: &ForeignRelContext<'_>,
    clauses: *mut pg_sys::List,
    source: ScanClauseSource,
) -> Result<PathFilterSet, ForeignScanError> {
    let mut negotiator =
        FilterNegotiator::new(planner, relation.relation_oid(), relation.baserel());
    unsafe { negotiator.negotiate(clauses, source) }
        .map(NegotiatedFilterSet::into_path_set)
        .map_err(ForeignScanError::provider)
}

/// # Safety
///
/// `relation` and `clauses` must be live planner objects for the current
/// callback.  The source-classification callback only observes the live
/// `RestrictInfo` nodes while PostgreSQL performs the split.
unsafe fn negotiate_final_clauses<P: FdwScan>(
    planner: &mut P::Planner,
    relation: &ForeignRelContext<'_>,
    clauses: *mut pg_sys::List,
) -> Result<NegotiatedFilterSet<P::PlannedPredicate>, ForeignScanError> {
    let mut negotiator =
        FilterNegotiator::new(planner, relation.relation_oid(), relation.baserel());
    let baserestrictinfo = relation.baserestrictinfo();
    unsafe {
        negotiator.negotiate_with_source(clauses, |rinfo| {
            if pg_sys::list_member_ptr(baserestrictinfo, rinfo.cast::<c_void>()) {
                ScanClauseSource::BaseRestriction
            } else {
                ScanClauseSource::Movable
            }
        })
    }
    .map_err(ForeignScanError::provider)
}

fn validate_rel_size(size: ForeignRelSize) -> Result<(), ForeignScanError> {
    if !size.rows.is_finite() || size.rows < 0.0 {
        return Err(ForeignScanError::framework(
            "FDW relation estimate has a negative or non-finite row count",
        ));
    }
    if size.width < 0 {
        return Err(ForeignScanError::framework(
            "FDW relation estimate has a negative tuple width",
        ));
    }
    Ok(())
}
