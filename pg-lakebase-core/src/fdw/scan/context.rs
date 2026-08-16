//! Public provider API for the FDW planner/executor seam.

use core::ffi::c_int;
use core::marker::PhantomData;
use core::ptr;

use pgrx::pg_sys;

use crate::expr::pushdown::{FilterPlanSummary, NegotiatedFilterSet, PathFilterSet};
use crate::expr::relation::PlanRelationResolver;

use super::super::row_identity::ForeignRowIdentityRequirement;
use super::error::ForeignScanError;
use super::pathkeys::ForeignPathKeys;
use super::plan_filter::ForeignPlanFilters;
use super::projection::{ColumnRequirements, ScanProjectionPolicy};
use super::pushdown::ForeignExprs;
use crate::fdw::{ForeignPrivateReader, ForeignPrivateWriter};

/// Nullable PostgreSQL `Bitmapset *` used for required outer relations.
pub type Relids = *mut pg_sys::Bitmapset;

/// Plain or join-parameterized base-relation path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathVariantKind {
    /// The ordinary base path, including any lateral dependency required by
    /// the relation itself.
    Plain,
    /// A path whose `ParamPathInfo` contains movable clauses from an outer
    /// relation.
    JoinParameterized,
}

/// Planner context shared by relation-size, path, and plan callbacks.
#[derive(Debug, Clone, Copy)]
pub struct ForeignRelContext<'a> {
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreign_table_id: pg_sys::Oid,
    relation_oid: pg_sys::Oid,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ForeignRelContext<'a> {
    /// Construct a context from live planner nodes.
    ///
    /// # Safety
    ///
    /// `root` and `baserel` must be live planner-owned nodes from the same
    /// planning invocation.  The returned context cannot outlive those
    /// borrows, which prevents a provider's safe planner state from retaining
    /// the raw planner context after the callback.
    pub(crate) unsafe fn from_raw(
        root: &'a pg_sys::PlannerInfo,
        baserel: &'a pg_sys::RelOptInfo,
        foreign_table_id: pg_sys::Oid,
    ) -> Result<Self, ForeignScanError> {
        let root = root as *const pg_sys::PlannerInfo as *mut pg_sys::PlannerInfo;
        let baserel = baserel as *const pg_sys::RelOptInfo as *mut pg_sys::RelOptInfo;
        let relid = unsafe { (*baserel).relid };
        let relation_oid = unsafe { PlanRelationResolver::new(root).rel_oid(relid) };
        if relation_oid == pg_sys::Oid::INVALID {
            return Err(ForeignScanError::framework(
                "FDW planner context could not resolve the base relation OID",
            ));
        }
        if foreign_table_id != relation_oid {
            return Err(ForeignScanError::framework(
                "FDW planner callback foreign-table OID does not match its base relation",
            ));
        }
        let row_marks = unsafe { (*root).rowMarks };
        if !row_marks.is_null() {
            let row_mark_count = unsafe { pg_sys::list_length(row_marks) };
            for index in 0..row_mark_count {
                let row_mark = unsafe { pg_sys::list_nth(row_marks, index) }
                    as *mut pg_sys::PlanRowMark;
                if !row_mark.is_null()
                    && unsafe { (*row_mark).rti } == relid
                    && unsafe { (*row_mark).strength }
                        != pg_sys::LockClauseStrength::LCS_NONE
                {
                    return Err(ForeignScanError::unsupported(
                        "FDW framework v1 does not support explicit foreign row locking",
                    ));
                }
            }
        }
        Ok(Self {
            root,
            baserel,
            foreign_table_id,
            relation_oid,
            _marker: PhantomData,
        })
    }

    /// Live planner root.
    #[inline]
    pub fn root(&self) -> *mut pg_sys::PlannerInfo {
        self.root
    }

    /// Live base-relation planner node.
    #[inline]
    pub fn baserel(&self) -> *mut pg_sys::RelOptInfo {
        self.baserel
    }

    /// Foreign-table OID passed by PostgreSQL to the callback.
    #[inline]
    pub fn foreign_table_id(&self) -> pg_sys::Oid {
        self.foreign_table_id
    }

    /// Base relation's `pg_class` OID.
    #[inline]
    pub fn relation_oid(&self) -> pg_sys::Oid {
        self.relation_oid
    }

    /// Base relation's range-table index.
    #[inline]
    pub fn scan_relid(&self) -> pg_sys::Index {
        unsafe { (*self.baserel).relid }
    }

    /// Planner relids represented by this base relation.
    #[inline]
    pub fn relids(&self) -> Relids {
        unsafe { (*self.baserel).relids }
    }

    /// Planner-visible base restriction list.
    #[inline]
    pub fn baserestrictinfo(&self) -> *mut pg_sys::List {
        unsafe { (*self.baserel).baserestrictinfo }
    }

    /// Planner-visible join restriction list.
    #[inline]
    pub fn joininfo(&self) -> *mut pg_sys::List {
        unsafe { (*self.baserel).joininfo }
    }

    /// Relation-local lateral dependency.
    #[inline]
    pub fn lateral_relids(&self) -> Relids {
        unsafe { (*self.baserel).lateral_relids }
    }

    /// Planner path target expressions.
    #[inline]
    pub fn path_target_exprs(&self) -> *mut pg_sys::List {
        let target = unsafe { (*self.baserel).reltarget };
        if target.is_null() {
            ptr::null_mut()
        } else {
            unsafe { (*target).exprs }
        }
    }

    /// Query-level ordering requested by PostgreSQL, if any.
    #[inline]
    pub fn query_pathkeys(&self) -> *mut pg_sys::List {
        unsafe { (*self.root).query_pathkeys }
    }

    /// Unfiltered tuple population from `pg_class`, or PostgreSQL's provider
    /// fallback established during `GetForeignRelSize`.
    #[inline]
    pub fn base_tuples(&self) -> f64 {
        unsafe { (*self.baserel).tuples }
    }

    /// Unfiltered relation size in PostgreSQL blocks.
    #[inline]
    pub fn base_pages(&self) -> f64 {
        unsafe { (*self.baserel).pages as f64 }
    }

    /// Rows after all base restrictions, as established by relation sizing.
    #[inline]
    pub fn rows(&self) -> f64 {
        unsafe { (*self.baserel).rows }
    }

    fn filter_estimate_for_exprs(
        &self,
        exprs: impl IntoIterator<Item = *mut pg_sys::Expr>,
    ) -> ForeignFilterEstimate {
        let mut clauses = ptr::null_mut();
        for expr in exprs {
            clauses = unsafe { pg_sys::lappend(clauses, expr.cast()) };
        }
        if clauses.is_null() {
            return ForeignFilterEstimate::NONE;
        }
        let selectivity = unsafe {
            pg_sys::clauselist_selectivity(
                self.root,
                clauses,
                (*self.baserel).relid as c_int,
                pg_sys::JoinType::JOIN_INNER,
                ptr::null_mut(),
            )
        };
        let mut cost = pg_sys::QualCost {
            startup: 0.0,
            per_tuple: 0.0,
        };
        unsafe { pg_sys::cost_qual_eval(&mut cost, clauses, self.root) };
        ForeignFilterEstimate {
            selectivity: selectivity.clamp(0.0, 1.0),
            startup_cost: cost.startup,
            per_tuple_cost: cost.per_tuple,
        }
    }

    /// Whether PostgreSQL is planning this base relation as the target of an
    /// UPDATE or DELETE statement.
    ///
    /// This is planner metadata, not an executor-time check. Providers use it
    /// to limit explicit modify row-identity registration to the target scan;
    /// the framework does not derive an ItemPointer identity from this fact.
    #[inline]
    pub fn is_modify_target(&self) -> bool {
        let parse = unsafe { (*self.root).parse };
        !parse.is_null()
            && matches!(
                unsafe { (*parse).commandType },
                pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE
            )
            && unsafe { (*parse).resultRelation as pg_sys::Index }
                == self.scan_relid()
    }
}

/// Result of the provider's relation-size estimate.
#[derive(Debug, Clone, Copy)]
pub struct ForeignRelSize {
    /// Estimated number of rows returned by the base relation.
    pub rows: f64,
    /// Estimated width of the ordinary relation output tuple.
    pub width: i32,
}

impl ForeignRelSize {
    #[inline]
    pub const fn new(rows: f64, width: i32) -> Self {
        Self { rows, width }
    }
}

/// Relation-size callback context.  `pushdown` is an estimate-time hint; the
/// final plan always re-splits PostgreSQL's final `scan_clauses` list.
pub struct ForeignRelSizeContext<'a> {
    relation: ForeignRelContext<'a>,
    pushdown: &'a PathFilterSet,
}

impl<'a> ForeignRelSizeContext<'a> {
    pub(crate) fn new(
        relation: ForeignRelContext<'a>,
        pushdown: &'a PathFilterSet,
    ) -> Self {
        Self { relation, pushdown }
    }

    #[inline]
    pub fn relation(&self) -> &ForeignRelContext<'a> {
        &self.relation
    }

    #[inline]
    pub fn pushdown(&self) -> FilterPlanSummary {
        FilterPlanSummary::from_path_set(self.pushdown)
    }

    /// Estimate a foreign relation from locally persisted PostgreSQL
    /// statistics. This is the same fallback used by `postgres_fdw` when it
    /// does not request a remote estimate: an unANALYZEd relation is assigned
    /// a small page population, then PostgreSQL applies type widths, column
    /// statistics, and base-qual selectivity itself.
    pub fn local_statistics_estimate(
        &self,
        fallback_pages: pg_sys::BlockNumber,
    ) -> ForeignRelSize {
        let baserel = self.relation.baserel;
        unsafe {
            if (*baserel).tuples < 0.0 {
                (*baserel).pages = fallback_pages;
                let width = if (*baserel).reltarget.is_null() {
                    0
                } else {
                    (*(*baserel).reltarget).width.max(0)
                };
                let header_bytes = core::mem::offset_of!(
                    pg_sys::HeapTupleHeaderData,
                    t_bits
                );
                let alignment = pg_sys::MAXIMUM_ALIGNOF as usize;
                let aligned_header = header_bytes
                    .saturating_add(alignment - 1)
                    & !(alignment - 1);
                let tuple_bytes = (width as usize)
                    .saturating_add(aligned_header)
                    .max(1);
                let relation_bytes = (fallback_pages as usize)
                    .saturating_mul(pg_sys::BLCKSZ as usize);
                (*baserel).tuples = relation_bytes as f64 / tuple_bytes as f64;
            }
            pg_sys::set_baserel_size_estimates(self.relation.root, baserel);
            let width = if (*baserel).reltarget.is_null() {
                0
            } else {
                (*(*baserel).reltarget).width
            };
            ForeignRelSize::new((*baserel).rows, width)
        }
    }
}

/// Path callback context.  The split is used for path costing and selection;
/// `GetForeignPlan` recomputes the authoritative final split.
pub struct ForeignPathContext<'a> {
    relation: ForeignRelContext<'a>,
    pushdown: &'a PathFilterSet,
    kind: PathVariantKind,
    required_outer: Relids,
    param_info: *mut pg_sys::ParamPathInfo,
}

/// PostgreSQL's selectivity and evaluation cost for provider-side filters.
#[derive(Debug, Clone, Copy)]
pub struct ForeignFilterEstimate {
    /// Fraction of input rows expected to survive the provider filter.
    pub selectivity: f64,
    /// One-time expression evaluation cost.
    pub startup_cost: f64,
    /// Expression evaluation cost charged for each provider input row.
    pub per_tuple_cost: f64,
}

impl ForeignFilterEstimate {
    const NONE: Self = Self {
        selectivity: 1.0,
        startup_cost: 0.0,
        per_tuple_cost: 0.0,
    };
}

impl<'a> ForeignPathContext<'a> {
    pub(crate) fn new(
        relation: ForeignRelContext<'a>,
        pushdown: &'a PathFilterSet,
        kind: PathVariantKind,
        required_outer: Relids,
        param_info: *mut pg_sys::ParamPathInfo,
    ) -> Self {
        Self {
            relation,
            pushdown,
            kind,
            required_outer,
            param_info,
        }
    }

    #[inline]
    pub fn relation(&self) -> &ForeignRelContext<'a> {
        &self.relation
    }

    #[inline]
    pub fn pushdown(&self) -> FilterPlanSummary {
        FilterPlanSummary::from_path_set(self.pushdown)
    }

    /// Selectivity and execution cost of provider filters that are allowed to
    /// reduce scan-volume costing. Unsupported/local quals are excluded.
    pub fn pruning_estimate(&self) -> ForeignFilterEstimate {
        self.relation.filter_estimate_for_exprs(
            self.pushdown.costed_pruning_exprs(),
        )
    }

    /// Rows emitted by this path after all base and parameterized local quals.
    #[inline]
    pub fn rows(&self) -> f64 {
        self.param_info()
            .map_or_else(|| self.relation.rows(), |info| info.ppi_rows)
    }

    #[inline]
    pub fn kind(&self) -> PathVariantKind {
        self.kind
    }

    #[inline]
    pub fn required_outer(&self) -> Relids {
        self.required_outer
    }

    /// `ParamPathInfo` for this variant, or `None` for an unparameterized path.
    /// The pointer is planner-owned and valid for this callback.
    #[inline]
    pub fn param_info(&self) -> Option<&pg_sys::ParamPathInfo> {
        (!self.param_info.is_null()).then(|| unsafe { &*self.param_info })
    }
}

/// A path's provider-private data and planner costs.
pub struct ForeignPathSpec<D> {
    /// Estimated rows emitted after PostgreSQL applies local scan quals.
    pub rows: f64,
    /// Estimated rows materialized by the ForeignScan before local scan quals.
    /// The framework uses this value, bounded below by `rows`, for local
    /// residual-qual evaluation and tuple-processing costs.
    pub retrieved_rows: f64,
    /// Provider-owned startup cost.  The framework adds PostgreSQL-local
    /// residual-qual and target-list startup costs to the path startup cost.
    pub provider_startup_cost: f64,
    /// Provider-owned total cost before the framework adds PostgreSQL-local
    /// residual-qual, target-list, and tuple-processing costs.
    pub provider_total_cost: f64,
    /// PostgreSQL pathkeys promised by this provider path. The framework
    /// validates the EC member and dependency contract before adding the path;
    /// the provider must use the same ordering in its remote plan. The value
    /// is private so a provider cannot submit an arbitrary pointer through a
    /// safe struct literal.
    pathkeys: *mut pg_sys::List,
    pub private_data: D,
}

impl<D> ForeignPathSpec<D> {
    #[inline]
    pub fn new(
        rows: f64,
        provider_startup_cost: f64,
        provider_total_cost: f64,
        private_data: D,
    ) -> Self {
        Self {
            rows,
            retrieved_rows: rows,
            provider_startup_cost,
            provider_total_cost,
            pathkeys: ptr::null_mut(),
            private_data,
        }
    }

    /// Associate planner-owned PostgreSQL pathkeys with this path alternative.
    ///
    /// The default path spec is unordered (`NIL`). The provider must call this
    /// method only for an ordered alternative and must use the same ordering in
    /// the remote plan represented by `private_data`.
    ///
    /// # Safety
    ///
    /// `pathkeys` must be NULL or a live PostgreSQL `List` allocated in the
    /// current planner memory context. A non-NULL list must remain valid until
    /// PostgreSQL finishes consuming the path and must contain valid
    /// `PathKey` nodes in every list cell. The framework does not copy or take
    /// ownership of the list.
    pub unsafe fn set_pathkeys(&mut self, pathkeys: *mut pg_sys::List) {
        self.pathkeys = pathkeys;
    }

    #[inline]
    pub(crate) fn pathkeys_ptr(&self) -> *mut pg_sys::List {
        self.pathkeys
    }
}

/// Final plan callback context.
pub struct ForeignPlanContext<'a, P: super::contract::FdwScan> {
    relation: ForeignRelContext<'a>,
    filters: ForeignPlanFilters<'a, P>,
    tlist: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
    kind: PathVariantKind,
    required_outer: Relids,
    path_private: &'a P::PrivateData,
    pathkeys: &'a ForeignPathKeys,
    row_identity_requirement: ForeignRowIdentityRequirement,
}

impl<'a, P: super::contract::FdwScan> ForeignPlanContext<'a, P> {
    pub(crate) fn new(
        relation: ForeignRelContext<'a>,
        filters: &'a NegotiatedFilterSet<P::PlannedPredicate>,
        tlist: *mut pg_sys::List,
        outer_plan: *mut pg_sys::Plan,
        kind: PathVariantKind,
        required_outer: Relids,
        path_private: &'a P::PrivateData,
        pathkeys: &'a ForeignPathKeys,
        row_identity_requirement: ForeignRowIdentityRequirement,
    ) -> Self {
        Self {
            relation,
            filters: ForeignPlanFilters::new(filters),
            tlist,
            outer_plan,
            kind,
            required_outer,
            path_private,
            pathkeys,
            row_identity_requirement,
        }
    }

    #[inline]
    pub fn relation(&self) -> &ForeignRelContext<'a> {
        &self.relation
    }

    #[inline]
    pub fn pushdown(&self) -> FilterPlanSummary {
        self.filters.summary()
    }

    /// Finalized typed filter plan. This is the only filter authority at the
    /// final FDW planning stage.
    #[inline]
    pub fn filters(&self) -> &ForeignPlanFilters<'a, P> {
        &self.filters
    }

    #[inline]
    pub fn targetlist(&self) -> *mut pg_sys::List {
        self.tlist
    }

    #[inline]
    pub fn outer_plan(&self) -> *mut pg_sys::Plan {
        self.outer_plan
    }

    #[inline]
    pub fn kind(&self) -> PathVariantKind {
        self.kind
    }

    #[inline]
    pub fn required_outer(&self) -> Relids {
        self.required_outer
    }

    #[inline]
    pub fn path_private(&self) -> &P::PrivateData {
        self.path_private
    }

    /// Validated ordering selected for the chosen foreign path.
    #[inline]
    pub fn pathkeys(&self) -> &ForeignPathKeys {
        self.pathkeys
    }

    /// Whether this scan is the foreign relation that PostgreSQL is modifying.
    ///
    /// Providers use this method to distinguish the modify-purpose scan from
    /// ordinary scans. The provider still chooses the concrete row identity in
    /// `AddForeignUpdateTargets`.
    #[inline]
    pub fn is_modify_target(&self) -> bool {
        self.relation.is_modify_target()
    }

    /// Special scan output required by the provider's explicit UPDATE/DELETE row
    /// identity registration. Positive attribute identities use ordinary
    /// relation columns and therefore return [`ForeignRowIdentityRequirement::None`].
    #[inline]
    pub fn row_identity_requirement(&self) -> ForeignRowIdentityRequirement {
        self.row_identity_requirement
    }
}

/// Provider output of the final plan callback.
pub struct ForeignPlanSpec<D> {
    pub private_data: D,
    pub fdw_exprs: ForeignExprs,
    pub required_columns: ColumnRequirements,
    /// Controls the executor scan tuple shape independently of the provider
    /// read set.  The default allows the framework to prune columns safely.
    pub projection_policy: ScanProjectionPolicy,
}

impl<D> ForeignPlanSpec<D> {
    #[inline]
    pub fn new(private_data: D) -> Self {
        Self {
            private_data,
            fdw_exprs: ForeignExprs::new(),
            required_columns: ColumnRequirements::default(),
            projection_policy: ScanProjectionPolicy::default(),
        }
    }
}

/// Copy-object-safe codec implemented by provider plan data.
pub trait ForeignPlanPrivate: Sized + 'static {
    /// Encode provider control data as PostgreSQL Nodes in the current planner
    /// memory context.  The framework adds its own envelope around this data.
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignScanError>;

    /// Decode provider control data from a validated framework envelope.
    ///
    /// # Safety
    ///
    /// The reader must refer to a PostgreSQL-owned plan list that remains live
    /// for the returned value's construction.
    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignScanError>;
}
