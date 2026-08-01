//! Base-relation scan projection and provider read-set planning.

use core::ffi::c_void;
use core::ptr;
use core::slice;
use std::collections::{BTreeMap, BTreeSet};

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::expr::inspect::{RelationExprAnalyzer, RelationExprUsage, RelationScope};

use super::super::row_identity::ForeignRowIdentityRequirement;
use super::super::system_column::SystemColumnRequirement;
use super::error::ForeignScanError;
use super::pathkeys::ForeignPathKeys;

/// Provider read-set contract, kept separate from the executor scan tuple.
/// PostgreSQL system columns are intentionally absent: `tableoid` is
/// synthesized by the executor, `ctid` is represented by the row-identity
/// contract, and unsupported header fields are rejected during analysis.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnRequirements {
    all_columns: bool,
    user_attnos: BTreeSet<pg_sys::AttrNumber>,
}

impl ColumnRequirements {
    /// Require one positive base-table attribute.
    pub fn require_column(
        &mut self,
        attno: pg_sys::AttrNumber,
    ) -> Result<(), ForeignScanError> {
        if attno <= 0 {
            return Err(ForeignScanError::framework(
                "FDW column requirements must use positive user attributes",
            ));
        }
        self.user_attnos.insert(attno);
        Ok(())
    }

    /// Require the provider to read the complete relation row.
    pub fn require_all_columns(&mut self) {
        self.all_columns = true;
    }

    #[inline]
    pub fn needs_all_columns(&self) -> bool {
        self.all_columns
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        !self.all_columns && self.user_attnos.is_empty()
    }

    /// User attributes requested by the provider in ascending order.
    pub fn user_columns(&self) -> impl Iterator<Item = pg_sys::AttrNumber> + '_ {
        self.user_attnos.iter().copied()
    }

    pub(crate) fn user_columns_slice(&self) -> &BTreeSet<pg_sys::AttrNumber> {
        &self.user_attnos
    }

    #[inline]
    pub(crate) fn contains_user_column(&self, attno: pg_sys::AttrNumber) -> bool {
        self.all_columns || self.user_attnos.contains(&attno)
    }
}

/// Provider policy for the tuple shape exposed by the base ForeignScan.
///
/// This policy is independent from [`ColumnRequirements`], which describes
/// the provider's physical read set.  `AllowColumnPruning` may still fall back
/// to a relation-shaped tuple when PostgreSQL's plan contract requires it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanProjectionPolicy {
    /// Let the framework derive the narrowest safe scan tuple.
    #[default]
    AllowColumnPruning,
    /// Require `fdw_scan_tlist = NIL` and the base relation tuple descriptor.
    RequireRelationShape,
}

/// Shape of the tuple placed in PostgreSQL's ForeignScan scan slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanProjection {
    /// `fdw_scan_tlist = NIL`; the slot has the foreign relation rowtype.
    Relation,
    /// `fdw_scan_tlist` contains one base `Var` per slot position.
    Projected { attnos: Vec<pg_sys::AttrNumber> },
    /// `fdw_scan_tlist` contains one synthetic NULL slot column.  It carries
    /// no relation attribute and is used when the executor needs no base
    /// relation column, for example for a constant-only scan.  In PostgreSQL
    /// 17 ordinary ForeignPaths, aggregate queries such as COUNT(*) use a
    /// physical targetlist and are represented as `Projected` instead.
    SyntheticNull,
}

/// Contract for the columns a provider must place in the executor scan slot.
/// This is deliberately separate from [`ColumnRequirements`], which describes
/// the provider's physical read set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SlotWritePlan {
    /// Every position in the scan slot must be written, except dropped
    /// relation-column gaps which the framework initializes to NULL.
    Complete,
    /// Only these relation attributes are required in a relation-shaped slot.
    /// Other positions are initialized to NULL by the framework.
    RequiredAttributes(Vec<pg_sys::AttrNumber>),
}

impl SlotWritePlan {
    #[inline]
    pub(crate) const fn complete() -> Self {
        Self::Complete
    }

    #[inline]
    pub(crate) fn required_attributes(
        attributes: impl IntoIterator<Item = pg_sys::AttrNumber>,
    ) -> Self {
        Self::RequiredAttributes(attributes.into_iter().collect())
    }

    #[inline]
    pub(crate) fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    #[inline]
    pub(crate) fn attributes(&self) -> &[pg_sys::AttrNumber] {
        match self {
            Self::Complete => &[],
            Self::RequiredAttributes(attributes) => attributes,
        }
    }

    pub(crate) fn from_wire(
        complete: bool,
        attributes: Vec<pg_sys::AttrNumber>,
    ) -> Result<Self, ForeignScanError> {
        let mut seen = BTreeSet::new();
        for &attno in &attributes {
            if attno <= 0 || !seen.insert(attno) {
                return Err(ForeignScanError::framework(
                    "FDW private data encodes an invalid slot write attribute list",
                ));
            }
        }
        if complete {
            if !attributes.is_empty() {
                return Err(ForeignScanError::framework(
                    "FDW private data encodes attributes for a complete slot write plan",
                ));
            }
            Ok(Self::Complete)
        } else {
            Ok(Self::RequiredAttributes(attributes))
        }
    }
}

impl ScanProjection {
    #[inline]
    pub const fn relation() -> Self {
        Self::Relation
    }

    #[inline]
    pub(crate) fn projected(attnos: Vec<pg_sys::AttrNumber>) -> Self {
        Self::Projected { attnos }
    }

    #[inline]
    pub(crate) const fn synthetic_null() -> Self {
        Self::SyntheticNull
    }

    #[inline]
    pub fn is_relation(&self) -> bool {
        matches!(self, Self::Relation)
    }

    #[inline]
    pub fn attnos(&self) -> &[pg_sys::AttrNumber] {
        match self {
            Self::Relation => &[],
            Self::Projected { attnos, .. } => attnos,
            Self::SyntheticNull => &[],
        }
    }

    pub(crate) fn wire_kind(&self) -> i32 {
        match self {
            Self::Relation => 0,
            Self::Projected { .. } => 1,
            Self::SyntheticNull => 2,
        }
    }

    pub(crate) fn from_wire(
        kind: i32,
        attnos: Vec<pg_sys::AttrNumber>,
    ) -> Result<Self, ForeignScanError> {
        match kind {
            0 if attnos.is_empty() => Ok(Self::Relation),
            0 => Err(ForeignScanError::framework(
                "FDW private data encodes relation projection with attributes",
            )),
            1 if attnos.is_empty() => Err(ForeignScanError::framework(
                "FDW private data encodes an empty projected scan tuple",
            )),
            1 => {
                let mut seen = BTreeSet::new();
                for attno in &attnos {
                    if *attno <= 0 || !seen.insert(*attno) {
                        return Err(ForeignScanError::framework(
                            "FDW private data encodes an invalid projected attribute list",
                        ));
                    }
                }
                Ok(Self::projected(attnos))
            }
            2 if attnos.is_empty() => Ok(Self::SyntheticNull),
            2 => Err(ForeignScanError::framework(
                "FDW private data encodes synthetic-null projection with attributes",
            )),
            _ => Err(ForeignScanError::framework(
                "FDW private data encodes an unknown scan projection kind",
            )),
        }
    }
}

/// Planner output consumed by the final private-data encoder.
pub(crate) struct PlannedProjection {
    pub(crate) fdw_scan_tlist: *mut pg_sys::List,
    pub(crate) projection: ScanProjection,
    pub(crate) write_plan: SlotWritePlan,
    pub(crate) requirements: ColumnRequirements,
}

/// Planner-only dependency maps.  No map is retained in executor state or
/// consulted by the per-row writer; the finalized relation mapping is an
/// array indexed by attribute number.
struct ProjectionAnalysis {
    vars_by_attno: BTreeMap<pg_sys::AttrNumber, *mut pg_sys::Var>,
    executor_vars_by_attno: BTreeMap<pg_sys::AttrNumber, *mut pg_sys::Var>,
    direct_outputs: Vec<DirectOutput>,
    can_narrow: bool,
    provider_requires_all_columns: bool,
    executor_requires_all_columns: bool,
    system_columns: Vec<SystemColumnRequirement>,
}

#[derive(Clone, Copy)]
enum DependencyScope {
    Executor,
    Provider,
}

impl Default for ProjectionAnalysis {
    fn default() -> Self {
        Self {
            vars_by_attno: BTreeMap::new(),
            executor_vars_by_attno: BTreeMap::new(),
            direct_outputs: Vec::new(),
            can_narrow: true,
            provider_requires_all_columns: false,
            executor_requires_all_columns: false,
            system_columns: Vec::new(),
        }
    }
}

impl ProjectionAnalysis {
    fn absorb(&mut self, usage: RelationExprUsage, scope: DependencyScope) {
        if usage.has_whole_row() {
            self.provider_requires_all_columns = true;
            if matches!(scope, DependencyScope::Executor) {
                self.executor_requires_all_columns = true;
                self.can_narrow = false;
            }
        }
        if !usage.system_attnos().is_empty() {
            self.system_columns.extend(
                usage
                    .system_attnos()
                    .iter()
                    .copied()
                    .map(SystemColumnRequirement::from_attno),
            );
            self.can_narrow = false;
        }
        for var in usage.user_vars() {
            if let Some(existing) = self.vars_by_attno.get(&var.attno) {
                let same_nullingrels = unsafe {
                    pg_sys::bms_equal(
                        (*(*existing)).varnullingrels,
                        var.raw.as_ref().varnullingrels,
                    )
                };
                if !same_nullingrels {
                    self.can_narrow = false;
                }
            } else {
                self.vars_by_attno.insert(var.attno, var.raw.as_ptr());
            }
            if matches!(scope, DependencyScope::Executor) {
                if let Some(existing) = self.executor_vars_by_attno.get(&var.attno) {
                    let same_nullingrels = unsafe {
                        pg_sys::bms_equal(
                            (*(*existing)).varnullingrels,
                            var.raw.as_ref().varnullingrels,
                        )
                    };
                    if !same_nullingrels {
                        self.can_narrow = false;
                    }
                } else {
                    self.executor_vars_by_attno
                        .insert(var.attno, var.raw.as_ptr());
                }
            }
        }
    }
}

struct DirectOutput {
    attno: pg_sys::AttrNumber,
    var: *mut pg_sys::Var,
    resjunk: bool,
}

/// Build the relation's executor projection and the provider read set.
///
/// All inputs are pre-setrefs planner nodes.  The returned `fdw_scan_tlist`
/// is either NIL, a Var-only list, or a one-column synthetic NULL list
/// allocated in the current planner context;
/// `projection_policy` controls whether the provider permits narrowing it.
///
/// # Safety
///
/// Every non-NULL list argument must be a live PostgreSQL planner `T_List`
/// owned by the current planner memory context.  Its cells must remain live
/// for this call, and the nodes must be the pre-setrefs nodes supplied by the
/// current `GetForeignPlan` callback.
pub(crate) unsafe fn plan_projection(
    relation_oid: pg_sys::Oid,
    scan_relid: pg_sys::Index,
    targetlist: *mut pg_sys::List,
    path_target_exprs: *mut pg_sys::List,
    pathkeys: &ForeignPathKeys,
    residual_quals: *mut pg_sys::List,
    pushed_quals: *mut pg_sys::List,
    recheck_quals: *mut pg_sys::List,
    projection_policy: ScanProjectionPolicy,
    row_identity_requirement: ForeignRowIdentityRequirement,
    mut requirements: ColumnRequirements,
) -> Result<PlannedProjection, ForeignScanError> {
    let analyzer = RelationExprAnalyzer::new(RelationScope::exact(scan_relid));
    let mut analysis = ProjectionAnalysis::default();

    unsafe {
        inspect_targetlist(
            targetlist,
            &analyzer,
            &mut analysis,
            DependencyScope::Executor,
        );
        inspect_expr_list(
            path_target_exprs,
            &analyzer,
            &mut analysis,
            DependencyScope::Executor,
        );
        for expr in pathkeys.expressions() {
            inspect_expr(
                expr,
                &analyzer,
                &mut analysis,
                // PostgreSQL selects the local sort member from the relation
                // target through the EC.  The provider-selected member is a
                // remote read dependency and need not be written to the
                // executor slot unless the target/qual analysis also needs it.
                DependencyScope::Provider,
            );
        }
        inspect_expr_list(
            residual_quals,
            &analyzer,
            &mut analysis,
            DependencyScope::Executor,
        );
        inspect_expr_list(
            pushed_quals,
            &analyzer,
            &mut analysis,
            DependencyScope::Provider,
        );
        inspect_expr_list(
            recheck_quals,
            &analyzer,
            &mut analysis,
            DependencyScope::Executor,
        );
    }

    for &attno in analysis.vars_by_attno.keys() {
        requirements.require_column(attno)?;
    }
    if analysis.provider_requires_all_columns {
        requirements.require_all_columns();
    }
    if analysis
        .system_columns
        .iter()
        .copied()
        .any(SystemColumnRequirement::is_unsupported)
        || (analysis
            .system_columns
            .iter()
            .copied()
            .any(SystemColumnRequirement::requires_item_pointer)
            && !row_identity_requirement.needs_item_pointer())
    {
        return Err(ForeignScanError::unsupported(
            "FDW framework v1 does not support system-column expressions",
        ));
    }
    let require_relation_shape = row_identity_requirement.needs_item_pointer()
        || matches!(
            projection_policy,
            ScanProjectionPolicy::RequireRelationShape
        );
    // Tuple shape policy does not change the provider's physical read set.
    // Full-column reads come only from explicit requirements or analyzed
    // provider dependencies above.

    unsafe {
        add_required_columns(relation_oid, &requirements)?;
    }

    let relation_shape = require_relation_shape || !analysis.can_narrow;
    let write_plan = if analysis.executor_requires_all_columns {
        SlotWritePlan::complete()
    } else {
        SlotWritePlan::required_attributes(
            analysis.executor_vars_by_attno.keys().copied(),
        )
    };

    if relation_shape {
        return Ok(PlannedProjection {
            fdw_scan_tlist: ptr::null_mut(),
            projection: ScanProjection::Relation,
            write_plan,
            requirements,
        });
    }

    if analysis.executor_vars_by_attno.is_empty() {
        let fdw_scan_tlist = unsafe { append_synthetic_null_tlist() }?;
        return Ok(PlannedProjection {
            fdw_scan_tlist,
            projection: ScanProjection::synthetic_null(),
            write_plan: SlotWritePlan::required_attributes(
                Vec::<pg_sys::AttrNumber>::new(),
            ),
            requirements,
        });
    }

    let mut tlist = ptr::null_mut();
    let mut attnos = Vec::with_capacity(analysis.executor_vars_by_attno.len());
    let mut emitted = BTreeSet::new();

    for direct in analysis.direct_outputs {
        if !emitted.insert(direct.attno) {
            continue;
        }
        attnos.push(direct.attno);
        tlist = unsafe {
            append_tlist_entry(tlist, direct.var, attnos.len(), direct.resjunk)?
        };
    }

    for (&attno, &var) in &analysis.executor_vars_by_attno {
        if !emitted.insert(attno) {
            continue;
        }
        attnos.push(attno);
        tlist = unsafe { append_tlist_entry(tlist, var, attnos.len(), true)? };
    }

    if tlist.is_null() {
        return Ok(PlannedProjection {
            fdw_scan_tlist: ptr::null_mut(),
            projection: ScanProjection::Relation,
            write_plan,
            requirements,
        });
    }

    requirements.user_attnos.extend(attnos.iter().copied());
    Ok(PlannedProjection {
        fdw_scan_tlist: tlist,
        projection: ScanProjection::projected(attnos),
        write_plan: SlotWritePlan::complete(),
        requirements,
    })
}

/// # Safety
///
/// `targetlist` must be NULL or a live planner targetlist whose cells contain
/// live `TargetEntry` nodes for the duration of this call.
unsafe fn inspect_targetlist(
    targetlist: *mut pg_sys::List,
    analyzer: &RelationExprAnalyzer,
    analysis: &mut ProjectionAnalysis,
    scope: DependencyScope,
) {
    if targetlist.is_null() {
        return;
    }
    let length = unsafe { pg_sys::list_length(targetlist) };
    for index in 0..length {
        let entry = unsafe { pg_sys::list_nth(targetlist, index) }
            as *mut pg_sys::TargetEntry;
        if entry.is_null()
            || unsafe { (*entry).xpr.type_ } != pg_sys::NodeTag::T_TargetEntry
        {
            analysis.can_narrow = false;
            continue;
        }
        let expr = unsafe { (*entry).expr };
        unsafe { inspect_expr(expr, analyzer, analysis, scope) };
        if !expr.is_null() && unsafe { (*expr).type_ } == pg_sys::NodeTag::T_Var {
            let var = expr.cast::<pg_sys::Var>();
            if unsafe { is_local_user_var(var, analyzer) } {
                analysis.direct_outputs.push(DirectOutput {
                    attno: unsafe { (*var).varattno },
                    var,
                    resjunk: unsafe { (*entry).resjunk },
                });
            }
        }
    }
}

/// # Safety
///
/// `list` must be NULL or a live planner list of expression nodes whose cells
/// remain valid for the duration of this call.
unsafe fn inspect_expr_list(
    list: *mut pg_sys::List,
    analyzer: &RelationExprAnalyzer,
    analysis: &mut ProjectionAnalysis,
    scope: DependencyScope,
) {
    if list.is_null() {
        return;
    }
    let length = unsafe { pg_sys::list_length(list) };
    for index in 0..length {
        let expr = unsafe { pg_sys::list_nth(list, index) } as *mut pg_sys::Expr;
        unsafe { inspect_expr(expr, analyzer, analysis, scope) };
    }
}

/// # Safety
///
/// `expr` must be NULL or a live planner expression node.  `analyzer` and
/// `analysis` must remain exclusively borrowed for the duration of the call.
unsafe fn inspect_expr(
    expr: *mut pg_sys::Expr,
    analyzer: &RelationExprAnalyzer,
    analysis: &mut ProjectionAnalysis,
    scope: DependencyScope,
) {
    if expr.is_null() {
        analysis.can_narrow = false;
        return;
    }
    if matches!(scope, DependencyScope::Executor)
        && unsafe { contains_placeholder(expr.cast()) }
    {
        analysis.can_narrow = false;
    }
    let usage = unsafe { analyzer.collect_expr(expr) };
    analysis.absorb(usage, scope);
}

/// # Safety
///
/// `var` must be a live PostgreSQL `Var` node from the planner expression tree
/// represented by `analyzer`.
unsafe fn is_local_user_var(
    var: *mut pg_sys::Var,
    analyzer: &RelationExprAnalyzer,
) -> bool {
    // The analyzer already owns the relation-scope rule.  Re-run its tiny
    // public observation here rather than exposing another raw scope getter.
    let usage = unsafe { analyzer.collect_expr(var.cast()) };
    usage.user_vars().len() == 1
        && usage.system_attnos().is_empty()
        && !usage.has_whole_row()
        && usage.user_vars()[0].attno == unsafe { (*var).varattno }
}

/// # Safety
///
/// `relation_oid` must identify the live base relation being planned.
/// `requirements` must remain valid for the duration of the validation.  The
/// relation descriptor returned by PostgreSQL must remain live until it is
/// closed before return.
unsafe fn add_required_columns(
    relation_oid: pg_sys::Oid,
    requirements: &ColumnRequirements,
) -> Result<(), ForeignScanError> {
    if requirements.user_columns_slice().is_empty() {
        return Ok(());
    }
    let relation =
        unsafe { pg_sys::relation_open(relation_oid, pg_sys::NoLock as i32) };
    if relation.is_null() {
        return Err(ForeignScanError::framework(
            "FDW projection planning could not open the base relation",
        ));
    }

    let tuple_desc = unsafe { (*relation).rd_att };
    let natts = if tuple_desc.is_null() {
        0
    } else {
        let natts = unsafe { (*tuple_desc).natts };
        if natts < 0 {
            unsafe { pg_sys::relation_close(relation, pg_sys::NoLock as i32) };
            return Err(ForeignScanError::framework(
                "FDW projection planning received a TupleDesc with a negative width",
            ));
        }
        natts as usize
    };
    let attrs = if tuple_desc.is_null() {
        &[][..]
    } else {
        unsafe { slice::from_raw_parts((*tuple_desc).attrs.as_ptr(), natts) }
    };

    let mut result = Ok(());
    for &attno in requirements.user_columns_slice() {
        let index = (attno as i32 - 1) as usize;
        if index >= attrs.len() {
            result = Err(ForeignScanError::framework(
                "FDW provider requested an attribute outside the relation descriptor",
            ));
            break;
        }
        let attr = &attrs[index];
        if attr.attisdropped {
            result = Err(ForeignScanError::framework(
                "FDW projection planning requested a dropped relation attribute",
            ));
            break;
        }
    }
    unsafe { pg_sys::relation_close(relation, pg_sys::NoLock as i32) };
    result
}

/// # Safety
///
/// `list` must be a PostgreSQL list in the current planner memory context and
/// `source_var` must be a live base-relation `Var` node.  `resno` must fit in a
/// PostgreSQL `AttrNumber`.
unsafe fn append_tlist_entry(
    list: *mut pg_sys::List,
    source_var: *mut pg_sys::Var,
    resno: usize,
    resjunk: bool,
) -> Result<*mut pg_sys::List, ForeignScanError> {
    let resno = pg_sys::AttrNumber::try_from(resno).map_err(|_| {
        ForeignScanError::framework(
            "FDW projected scan tuple has more entries than AttrNumber can represent",
        )
    })?;
    let copied = unsafe { pg_sys::copyObjectImpl(source_var.cast::<c_void>()) }
        as *mut pg_sys::Expr;
    if copied.is_null() {
        return Err(ForeignScanError::framework(
            "FDW projection planning failed to copy a Var",
        ));
    }
    let entry =
        unsafe { pg_sys::makeTargetEntry(copied, resno, ptr::null_mut(), resjunk) };
    if entry.is_null() {
        return Err(ForeignScanError::framework(
            "FDW projection planning failed to construct a TargetEntry",
        ));
    }
    Ok(unsafe { pg_sys::lappend(list, entry.cast()) })
}

/// Build the one-column executor shape used when no relation attribute is
/// needed by PostgreSQL.  The column is deliberately synthetic rather than a
/// first real relation column: provider read requirements and executor tuple
/// shape must remain independent.
///
/// # Safety
///
/// The returned list is allocated in PostgreSQL's current planner memory
/// context and is valid for the remainder of the current plan construction.
unsafe fn append_synthetic_null_tlist() -> Result<*mut pg_sys::List, ForeignScanError>
{
    let expr =
        unsafe { pg_sys::makeNullConst(pg_sys::INT4OID, -1, pg_sys::InvalidOid) };
    if expr.is_null() {
        return Err(ForeignScanError::framework(
            "FDW projection planning failed to construct a synthetic NULL",
        ));
    }
    let resno = pg_sys::AttrNumber::try_from(1).map_err(|_| {
        ForeignScanError::framework(
            "FDW projection planning failed to represent synthetic TargetEntry resno",
        )
    })?;
    let entry = unsafe {
        pg_sys::makeTargetEntry(
            expr.cast::<pg_sys::Expr>(),
            resno,
            ptr::null_mut(),
            true,
        )
    };
    if entry.is_null() {
        return Err(ForeignScanError::framework(
            "FDW projection planning failed to construct a synthetic TargetEntry",
        ));
    }
    Ok(unsafe { pg_sys::lappend(ptr::null_mut(), entry.cast()) })
}

/// Return true when an expression tree contains a PlaceholderVar.  The
/// framework deliberately falls back to the relation-shaped tuple for this
/// shape because its executor/setrefs contract is not a plain base-Var map.
/// # Safety
///
/// `node` must be NULL or a live planner expression node whose tree remains
/// valid while PostgreSQL invokes the walker callback.
unsafe fn contains_placeholder(node: *mut pg_sys::Node) -> bool {
    let mut found = false;
    unsafe {
        pg_sys::expression_tree_walker(
            node,
            Some(placeholder_walker),
            (&mut found as *mut bool).cast(),
        );
    }
    found
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL invokes this callback synchronously with a live expression node
/// and the non-NULL `context` pointer supplied by `contains_placeholder` or a
/// recursive call from PostgreSQL's expression walker.
unsafe extern "C-unwind" fn placeholder_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let found = unsafe { &mut *(context.cast::<bool>()) };
    if unsafe { (*node).type_ } == pg_sys::NodeTag::T_PlaceHolderVar {
        *found = true;
        return true;
    }
    unsafe { pg_sys::expression_tree_walker(node, Some(placeholder_walker), context) }
}
