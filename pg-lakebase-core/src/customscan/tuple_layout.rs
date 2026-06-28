//! Base-scan tuple-layout planning and the executor-side layout contract.

use core::ffi::{c_char, c_int, c_void};
use core::ptr;
use std::collections::{BTreeMap, HashSet};

use pgrx::pg_sys;

use crate::customscan::custom_private::DecodeError;
use crate::customscan::error::CustomScanError;
use crate::expr::inspect::{RelationExprAnalyzer, RelationExprUsage, RelationScope};
use crate::expr::translator::ScanVarResolver;

const LAYOUT_RELATION: i32 = 0;
const LAYOUT_PROJECTED_BASE: i32 = 1;
const LAYOUT_RELATION_PRUNED: i32 = 2;

/// Borrowed storage-column requirement exposed to scan providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeededColumns<'a> {
    /// The provider must materialize every live user column.
    All,
    /// Base-relation attribute numbers in raw scan-tuple order.
    Subset(&'a [pg_sys::AttrNumber]),
}

/// Opaque raw scan-tuple contract.
///
/// The representation stays private so future metadata/computed/join layouts
/// can be added without spreading enum matches across builders, translators,
/// and providers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanTupleLayout {
    kind: ScanTupleLayoutKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScanTupleLayoutKind {
    Relation {
        /// When `Some`, only these base attnos are referenced by expressions;
        /// the provider may read just this subset even though the slot is
        /// full-width. When `None`, the provider must read all columns
        /// (whole-row Var or analysis could not determine the set).
        storage_attnos: Option<Box<[pg_sys::AttrNumber]>>,
    },
    ProjectedBase {
        attnos_by_resno: Box<[pg_sys::AttrNumber]>,
    },
}

impl ScanTupleLayout {
    pub(crate) fn relation() -> Self {
        Self {
            kind: ScanTupleLayoutKind::Relation {
                storage_attnos: None,
            },
        }
    }

    fn relation_with_storage_hint(attnos: Option<Vec<pg_sys::AttrNumber>>) -> Self {
        Self {
            kind: ScanTupleLayoutKind::Relation {
                storage_attnos: attnos.map(Vec::into_boxed_slice),
            },
        }
    }

    fn projected_base(attnos_by_resno: Vec<pg_sys::AttrNumber>) -> Self {
        debug_assert!(!attnos_by_resno.is_empty());
        Self {
            kind: ScanTupleLayoutKind::ProjectedBase {
                attnos_by_resno: attnos_by_resno.into_boxed_slice(),
            },
        }
    }

    /// Columns the provider must read, without rebuilding expression usage at
    /// executor start.
    pub fn required_columns(&self) -> NeededColumns<'_> {
        match &self.kind {
            ScanTupleLayoutKind::Relation {
                storage_attnos: Some(attnos),
            } => NeededColumns::Subset(attnos),
            ScanTupleLayoutKind::Relation {
                storage_attnos: None,
            } => NeededColumns::All,
            ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                NeededColumns::Subset(attnos_by_resno)
            }
        }
    }

    /// Resolve a base attribute to its zero-based destination in the raw scan
    /// slot. Bounds against the actual descriptor are applied by
    /// [`ScanTupleDescriptor::destination_for_attno`].
    fn destination_for_attno(&self, attno: pg_sys::AttrNumber) -> Option<usize> {
        if attno <= 0 {
            return None;
        }
        match &self.kind {
            ScanTupleLayoutKind::Relation { .. } => Some(attno as usize - 1),
            ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                attnos_by_resno.iter().position(|&source| source == attno)
            }
        }
    }

    pub(crate) fn var_resolver(&self, scan_relid: c_int) -> ScanVarResolver<'_> {
        match &self.kind {
            ScanTupleLayoutKind::Relation { .. } => {
                ScanVarResolver::relation(scan_relid)
            }
            ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                ScanVarResolver::mapped(pg_sys::INDEX_VAR, attnos_by_resno)
            }
        }
    }

    pub(crate) unsafe fn encode_wire(&self) -> *mut pg_sys::List {
        unsafe {
            let mut wire = ptr::null_mut();
            match &self.kind {
                ScanTupleLayoutKind::Relation {
                    storage_attnos: None,
                } => {
                    wire = pg_sys::lappend_int(wire, LAYOUT_RELATION);
                }
                ScanTupleLayoutKind::Relation {
                    storage_attnos: Some(attnos),
                } => {
                    wire = pg_sys::lappend_int(wire, LAYOUT_RELATION_PRUNED);
                    for &attno in attnos.iter() {
                        wire = pg_sys::lappend_int(wire, attno as i32);
                    }
                }
                ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                    wire = pg_sys::lappend_int(wire, LAYOUT_PROJECTED_BASE);
                    for &attno in attnos_by_resno.iter() {
                        wire = pg_sys::lappend_int(wire, attno as i32);
                    }
                }
            }
            wire
        }
    }

    pub(crate) unsafe fn decode_wire(
        wire: *mut pg_sys::List,
        field: i32,
    ) -> Result<Self, DecodeError> {
        if wire.is_null() {
            return Err(DecodeError::MalformedTupleLayout {
                reason: "layout list is NULL",
            });
        }
        if unsafe { (*wire).type_ } != pg_sys::NodeTag::T_IntList {
            return Err(DecodeError::WrongNodeTag {
                field,
                expected: pg_sys::NodeTag::T_IntList,
                found: unsafe { (*wire).type_ },
            });
        }
        let len = unsafe { pg_sys::list_length(wire) } as usize;
        if len == 0 {
            return Err(DecodeError::MalformedTupleLayout {
                reason: "layout list has no kind tag",
            });
        }
        let kind = unsafe { pg_sys::list_nth_int(wire, 0) };
        match kind {
            LAYOUT_RELATION if len == 1 => Ok(Self::relation()),
            LAYOUT_RELATION => Err(DecodeError::MalformedTupleLayout {
                reason: "relation layout contains trailing data",
            }),
            LAYOUT_RELATION_PRUNED if len == 1 => {
                Err(DecodeError::MalformedTupleLayout {
                    reason: "relation-pruned layout has no storage attnos",
                })
            }
            LAYOUT_RELATION_PRUNED => {
                let attnos = unsafe { decode_attno_tail(wire, len)? };
                Ok(Self::relation_with_storage_hint(Some(attnos)))
            }
            LAYOUT_PROJECTED_BASE if len == 1 => {
                Err(DecodeError::MalformedTupleLayout {
                    reason: "projected base layout is empty",
                })
            }
            LAYOUT_PROJECTED_BASE => {
                let attnos = unsafe { decode_attno_tail(wire, len)? };
                Ok(Self::projected_base(attnos))
            }
            value => Err(DecodeError::UnknownTupleLayoutKind { value }),
        }
    }

    pub(crate) unsafe fn validate_executor(
        &self,
        cscan: *mut pg_sys::CustomScan,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<(), CustomScanError> {
        if cscan.is_null() || slot.is_null() {
            return Err(CustomScanError::internal(LayoutInvariantError(
                "plan or scan slot is NULL",
            )));
        }
        let tlist = unsafe { (*cscan).custom_scan_tlist };
        let tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
        if tuple_desc.is_null() {
            return Err(CustomScanError::internal(LayoutInvariantError(
                "scan slot tuple descriptor is NULL",
            )));
        }

        match &self.kind {
            ScanTupleLayoutKind::Relation { .. } => {
                if !tlist.is_null() {
                    return Err(CustomScanError::internal(LayoutInvariantError(
                        "relation layout has a non-NIL custom_scan_tlist",
                    )));
                }
            }
            ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                if tlist.is_null() {
                    return Err(CustomScanError::internal(LayoutInvariantError(
                        "projected base layout has a NIL custom_scan_tlist",
                    )));
                }
                let tlist_len = unsafe { pg_sys::list_length(tlist) } as usize;
                let slot_width = unsafe { (*tuple_desc).natts } as usize;
                if tlist_len != attnos_by_resno.len()
                    || slot_width != attnos_by_resno.len()
                {
                    return Err(CustomScanError::internal(LayoutInvariantError(
                        "layout, custom_scan_tlist, and scan slot widths differ",
                    )));
                }
                let scan_relid = unsafe { (*cscan).scan.scanrelid } as c_int;
                for (index, &attno) in attnos_by_resno.iter().enumerate() {
                    let tle = unsafe { pg_sys::list_nth(tlist, index as i32) }
                        as *mut pg_sys::TargetEntry;
                    if tle.is_null()
                        || unsafe { (*tle).xpr.type_ }
                            != pg_sys::NodeTag::T_TargetEntry
                        || unsafe { (*tle).resno as usize } != index + 1
                    {
                        return Err(CustomScanError::internal(LayoutInvariantError(
                            "custom_scan_tlist entries are not contiguous TargetEntry nodes",
                        )));
                    }
                    let expr = unsafe { (*tle).expr };
                    if expr.is_null()
                        || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var
                    {
                        return Err(CustomScanError::internal(LayoutInvariantError(
                            "custom_scan_tlist contains a non-Var expression",
                        )));
                    }
                    let var = expr.cast::<pg_sys::Var>();
                    if unsafe { (*var).varno } != scan_relid
                        || unsafe { (*var).varattno } != attno
                        || unsafe { (*var).varlevelsup } != 0
                    {
                        return Err(CustomScanError::internal(LayoutInvariantError(
                            "custom_scan_tlist Var does not match the encoded base attribute",
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Decode the `[1..len]` tail of a wire IntList as deduplicated positive AttrNumbers.
unsafe fn decode_attno_tail(
    wire: *mut pg_sys::List,
    len: usize,
) -> Result<Vec<pg_sys::AttrNumber>, DecodeError> {
    let mut seen = HashSet::with_capacity(len - 1);
    let mut attnos = Vec::with_capacity(len - 1);
    for index in 1..len {
        let raw = unsafe { pg_sys::list_nth_int(wire, index as i32) };
        let attno = pg_sys::AttrNumber::try_from(raw).map_err(|_| {
            DecodeError::InvalidTupleLayoutAttno {
                index: index - 1,
                value: raw,
            }
        })?;
        if attno <= 0 {
            return Err(DecodeError::InvalidTupleLayoutAttno {
                index: index - 1,
                value: raw,
            });
        }
        if !seen.insert(attno) {
            return Err(DecodeError::DuplicateTupleLayoutAttno { attno });
        }
        attnos.push(attno);
    }
    Ok(attnos)
}

impl Default for ScanTupleLayout {
    fn default() -> Self {
        Self::relation()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("customscan tuple-layout invariant violated: {0}")]
struct LayoutInvariantError(&'static str);

/// Read-only view of the actual executor scan slot descriptor paired with the
/// decoded plan-time layout contract.
#[derive(Clone, Copy)]
pub struct ScanTupleDescriptor<'a> {
    tuple_desc: pg_sys::TupleDesc,
    layout: &'a ScanTupleLayout,
}

impl<'a> ScanTupleDescriptor<'a> {
    pub(crate) unsafe fn new(
        tuple_desc: pg_sys::TupleDesc,
        layout: &'a ScanTupleLayout,
    ) -> Self {
        debug_assert!(!tuple_desc.is_null());
        Self { tuple_desc, layout }
    }

    /// Number of physical cells in the raw scan slot.
    pub fn len(&self) -> usize {
        unsafe { (*self.tuple_desc).natts as usize }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Base attribute to zero-based raw scan-slot destination.
    pub fn destination_for_attno(&self, attno: pg_sys::AttrNumber) -> Option<usize> {
        self.layout
            .destination_for_attno(attno)
            .filter(|&destination| destination < self.len())
    }

    /// Actual scan-slot target types, indexed by raw scan position.
    pub fn attr_types(&self) -> Vec<(pg_sys::Oid, i32)> {
        let len = self.len();
        let attrs = unsafe {
            std::slice::from_raw_parts((*self.tuple_desc).attrs.as_ptr(), len)
        };
        attrs
            .iter()
            .map(|attr| (attr.atttypid, attr.atttypmod))
            .collect()
    }
}

/// Output of [`BaseScanTuplePlanner`].
pub(crate) struct PlannedScanTuple {
    pub(crate) custom_scan_tlist: *mut pg_sys::List,
    pub(crate) layout: ScanTupleLayout,
}

impl PlannedScanTuple {
    fn relation() -> Self {
        Self {
            custom_scan_tlist: ptr::null_mut(),
            layout: ScanTupleLayout::relation(),
        }
    }

    fn relation_with_storage_hint(attnos: Option<Vec<pg_sys::AttrNumber>>) -> Self {
        Self {
            custom_scan_tlist: ptr::null_mut(),
            layout: ScanTupleLayout::relation_with_storage_hint(attnos),
        }
    }
}

/// Cohesive pre-setrefs planner for a base-relation CustomScan tuple.
pub(crate) struct BaseScanTuplePlanner {
    scan_relid: pg_sys::Index,
    relation_oid: pg_sys::Oid,
    analyzer: RelationExprAnalyzer,
}

impl BaseScanTuplePlanner {
    pub(crate) fn new(scan_relid: pg_sys::Index, relation_oid: pg_sys::Oid) -> Self {
        Self {
            scan_relid,
            relation_oid,
            analyzer: RelationExprAnalyzer::new(RelationScope::exact(scan_relid)),
        }
    }

    /// Analyze every executor-visible expression before setrefs, then build a
    /// Var-only custom tlist plus the matching base-attno mapping. Any shape
    /// that cannot be proven safe falls back atomically to relation layout.
    pub(crate) unsafe fn plan(
        &self,
        targetlist: *mut pg_sys::List,
        path_target_exprs: *mut pg_sys::List,
        qual: *mut pg_sys::List,
        custom_exprs: *mut pg_sys::List,
    ) -> PlannedScanTuple {
        let mut analysis = LayoutAnalysis::default();
        unsafe { self.inspect_targetlist(targetlist, &mut analysis) };
        unsafe { self.inspect_path_target(path_target_exprs, &mut analysis) };
        unsafe { self.inspect_expr_list(qual, &mut analysis) };
        unsafe { self.inspect_expr_list(custom_exprs, &mut analysis) };

        if !analysis.can_narrow_tuple {
            let storage_attnos =
                if analysis.can_prune_storage && !analysis.vars_by_attno.is_empty() {
                    Some(analysis.vars_by_attno.keys().copied().collect::<Vec<_>>())
                } else {
                    None
                };
            return PlannedScanTuple::relation_with_storage_hint(storage_attnos);
        }

        if analysis.vars_by_attno.is_empty() {
            let Some(dummy) = (unsafe { self.first_live_user_var() }) else {
                return PlannedScanTuple::relation();
            };
            analysis
                .vars_by_attno
                .insert(unsafe { (*dummy).varattno }, dummy);
        }

        let mut custom_scan_tlist = ptr::null_mut();
        let mut attnos_by_resno = Vec::with_capacity(analysis.vars_by_attno.len());
        let mut emitted = HashSet::with_capacity(analysis.vars_by_attno.len());

        for direct in &analysis.direct_outputs {
            if !emitted.insert(direct.attno) {
                continue;
            }
            attnos_by_resno.push(direct.attno);
            custom_scan_tlist = unsafe {
                Self::append_tlist_entry(
                    custom_scan_tlist,
                    direct.var,
                    attnos_by_resno.len(),
                    direct.resname,
                    direct.resjunk,
                )
            };
        }

        for (&attno, &var) in &analysis.vars_by_attno {
            if !emitted.insert(attno) {
                continue;
            }
            attnos_by_resno.push(attno);
            custom_scan_tlist = unsafe {
                Self::append_tlist_entry(
                    custom_scan_tlist,
                    var,
                    attnos_by_resno.len(),
                    ptr::null_mut(),
                    true,
                )
            };
        }

        debug_assert!(!custom_scan_tlist.is_null());
        PlannedScanTuple {
            custom_scan_tlist,
            layout: ScanTupleLayout::projected_base(attnos_by_resno),
        }
    }

    /// `CUSTOMPATH_SUPPORT_PROJECTION` lets PostgreSQL call PlanCustomPath with
    /// a NIL tlist and replace `plan.targetlist` afterwards. The PathTarget is
    /// therefore part of the authoritative pre-setrefs dependency input.
    unsafe fn inspect_path_target(
        &self,
        exprs: *mut pg_sys::List,
        analysis: &mut LayoutAnalysis,
    ) {
        if exprs.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(exprs) };
        for index in 0..len {
            let expr = unsafe { pg_sys::list_nth(exprs, index) } as *mut pg_sys::Expr;
            unsafe { self.inspect_expr(expr, analysis) };
            if !expr.is_null() && unsafe { (*expr).type_ } == pg_sys::NodeTag::T_Var {
                let var = expr.cast::<pg_sys::Var>();
                if unsafe { self.is_local_user_var(var) } {
                    analysis.direct_outputs.push(DirectOutput {
                        attno: unsafe { (*var).varattno },
                        var,
                        resname: ptr::null_mut(),
                        resjunk: false,
                    });
                }
            }
        }
    }

    unsafe fn inspect_targetlist(
        &self,
        targetlist: *mut pg_sys::List,
        analysis: &mut LayoutAnalysis,
    ) {
        if targetlist.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(targetlist) };
        for index in 0..len {
            let tle = unsafe { pg_sys::list_nth(targetlist, index) }
                as *mut pg_sys::TargetEntry;
            if tle.is_null()
                || unsafe { (*tle).xpr.type_ } != pg_sys::NodeTag::T_TargetEntry
            {
                analysis.can_narrow_tuple = false;
                continue;
            }
            let expr = unsafe { (*tle).expr };
            unsafe { self.inspect_expr(expr, analysis) };

            if !expr.is_null() && unsafe { (*expr).type_ } == pg_sys::NodeTag::T_Var {
                let var = expr.cast::<pg_sys::Var>();
                if unsafe { self.is_local_user_var(var) } {
                    analysis.direct_outputs.push(DirectOutput {
                        attno: unsafe { (*var).varattno },
                        var,
                        resname: unsafe { (*tle).resname },
                        resjunk: unsafe { (*tle).resjunk },
                    });
                }
            }
        }
    }

    unsafe fn inspect_expr_list(
        &self,
        list: *mut pg_sys::List,
        analysis: &mut LayoutAnalysis,
    ) {
        if list.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(list) };
        for index in 0..len {
            let expr = unsafe { pg_sys::list_nth(list, index) } as *mut pg_sys::Expr;
            unsafe { self.inspect_expr(expr, analysis) };
        }
    }

    unsafe fn inspect_expr(
        &self,
        expr: *mut pg_sys::Expr,
        analysis: &mut LayoutAnalysis,
    ) {
        if expr.is_null() {
            return;
        }
        // PostgreSQL's set_customscan_references() rewrites plan.qual and
        // custom_exprs against custom_scan_tlist through fix_upper_expr().
        // Its expression mutator descends into SubPlan.testexpr and
        // SubPlan.args, exactly as the walker used by RelationExprAnalyzer
        // does. A SubPlan therefore needs its relation-local Vars in the
        // Var-only tlist, not a relation-shaped scan tuple.
        let usage = unsafe { self.analyzer.collect_expr(expr) };
        analysis.absorb(usage);
    }

    unsafe fn is_local_user_var(&self, var: *mut pg_sys::Var) -> bool {
        unsafe {
            (*var).varlevelsup == 0
                && (*var).varno == self.scan_relid as c_int
                && (*var).varattno > 0
        }
    }

    unsafe fn first_live_user_var(&self) -> Option<*mut pg_sys::Var> {
        if self.relation_oid == pg_sys::Oid::INVALID {
            return None;
        }
        let relation = unsafe {
            pg_sys::relation_open(self.relation_oid, pg_sys::NoLock as i32)
        };
        if relation.is_null() {
            return None;
        }
        let tuple_desc = unsafe { (*relation).rd_att };
        let natts = unsafe { (*tuple_desc).natts as usize };
        let attrs = unsafe {
            std::slice::from_raw_parts((*tuple_desc).attrs.as_ptr(), natts)
        };
        let result = attrs.iter().enumerate().find_map(|(index, attr)| {
            if attr.attisdropped {
                return None;
            }
            let attno = pg_sys::AttrNumber::try_from(index + 1).ok()?;
            Some(unsafe {
                pg_sys::makeVar(
                    self.scan_relid as c_int,
                    attno,
                    attr.atttypid,
                    attr.atttypmod,
                    attr.attcollation,
                    0,
                )
            })
        });
        unsafe { pg_sys::relation_close(relation, pg_sys::NoLock as i32) };
        result
    }

    unsafe fn append_tlist_entry(
        list: *mut pg_sys::List,
        source_var: *mut pg_sys::Var,
        resno: usize,
        resname: *mut c_char,
        resjunk: bool,
    ) -> *mut pg_sys::List {
        let copied = unsafe { pg_sys::copyObjectImpl(source_var.cast::<c_void>()) }
            .cast::<pg_sys::Expr>();
        let resno = pg_sys::AttrNumber::try_from(resno)
            .expect("custom scan tlist cannot exceed AttrNumber::MAX entries");
        let tle = unsafe { pg_sys::makeTargetEntry(copied, resno, resname, resjunk) };
        unsafe { pg_sys::lappend(list, tle.cast()) }
    }
}

struct LayoutAnalysis {
    vars_by_attno: BTreeMap<pg_sys::AttrNumber, *mut pg_sys::Var>,
    direct_outputs: Vec<DirectOutput>,
    /// The scan slot can be narrowed to a custom_scan_tlist (ProjectedBase).
    can_narrow_tuple: bool,
    /// The provider may read only the referenced columns even when the slot
    /// must stay full-width. Only `false` for whole-row Var, which
    /// legitimately needs every column.
    can_prune_storage: bool,
}

impl Default for LayoutAnalysis {
    fn default() -> Self {
        Self {
            vars_by_attno: BTreeMap::new(),
            direct_outputs: Vec::new(),
            can_narrow_tuple: true,
            can_prune_storage: true,
        }
    }
}

impl LayoutAnalysis {
    fn absorb(&mut self, usage: RelationExprUsage) {
        if usage.has_whole_row() {
            self.can_narrow_tuple = false;
            self.can_prune_storage = false;
            return;
        }
        if !usage.system_attnos().is_empty() {
            self.can_narrow_tuple = false;
        }
        for var in usage.user_vars() {
            if let Some(&existing) = self.vars_by_attno.get(&var.attno) {
                let equal = unsafe {
                    pg_sys::bms_equal(
                        (*existing).varnullingrels,
                        (*var.raw).varnullingrels,
                    )
                };
                if !equal {
                    self.can_narrow_tuple = false;
                }
            } else {
                self.vars_by_attno.insert(var.attno, var.raw);
            }
        }
    }
}

#[derive(Clone, Copy)]
struct DirectOutput {
    attno: pg_sys::AttrNumber,
    var: *mut pg_sys::Var,
    resname: *mut c_char,
    resjunk: bool,
}

/// Backend-test view of a planned base-scan tuple contract.
///
/// This adapter is compiled only for the dedicated `pg-backend-tests`
/// extension. Keeping construction here lets those tests exercise the real
/// private planner without widening the production CustomScan API.
#[cfg(feature = "pg_test")]
#[doc(hidden)]
pub struct ScanTuplePlanProbe {
    custom_scan_tlist: *mut pg_sys::List,
    layout: ScanTupleLayout,
}

/// Raw scan-tuple shape reported by [`ScanTuplePlanProbe`].
#[cfg(feature = "pg_test")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanTupleShape<'a> {
    Relation,
    ProjectedBase(&'a [pg_sys::AttrNumber]),
}

#[cfg(feature = "pg_test")]
impl ScanTuplePlanProbe {
    /// Run the production base-scan tuple planner over backend-owned nodes.
    ///
    /// # Safety
    ///
    /// Every pointer must be NULL or a live PostgreSQL planner node of the
    /// documented list shape. `scan_relid` and `relation_oid` must identify
    /// the same base relation whenever planning needs a count-only dummy Var.
    pub unsafe fn plan_base_scan(
        scan_relid: pg_sys::Index,
        relation_oid: pg_sys::Oid,
        targetlist: *mut pg_sys::List,
        path_target_exprs: *mut pg_sys::List,
        qual: *mut pg_sys::List,
        custom_exprs: *mut pg_sys::List,
    ) -> Self {
        let planned = unsafe {
            BaseScanTuplePlanner::new(scan_relid, relation_oid).plan(
                targetlist,
                path_target_exprs,
                qual,
                custom_exprs,
            )
        };
        Self {
            custom_scan_tlist: planned.custom_scan_tlist,
            layout: planned.layout,
        }
    }

    pub fn shape(&self) -> ScanTupleShape<'_> {
        match &self.layout.kind {
            ScanTupleLayoutKind::Relation { .. } => ScanTupleShape::Relation,
            ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                ScanTupleShape::ProjectedBase(attnos_by_resno)
            }
        }
    }

    pub fn required_columns(&self) -> NeededColumns<'_> {
        self.layout.required_columns()
    }

    pub fn custom_scan_tlist(&self) -> *mut pg_sys::List {
        self.custom_scan_tlist
    }
}
