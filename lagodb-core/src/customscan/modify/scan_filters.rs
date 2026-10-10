//! Rescan-stable predicates for native modification target scans. Constants and
//! external parameters support scan pruning and reader filtering; providers
//! retain their separate static-only conflict scope. PostgreSQL retains every
//! SeqScan qual for final row qualification.
//!
//! PG17's SeqNext (nodeSeqscan.c) passes zero scan keys to table_beginscan;
//! ExecScan (execScan.c) evaluates quals after fetching each row. These predicates
//! instead come from baserestrictinfo at planning time and travel through the
//! outer ModifyTable CustomScan's plan data to prepare_modify_seq_scan. They are
//! an explicit LagoDB channel, not qual pushdown supplied by native SeqScan.

use std::{marker::PhantomData, mem, ptr};

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::provider::RelationContext;
use crate::expr::pushdown::{
    FilterDataCodec, FilterNegotiator, FilterPlanningContext, RelationFilterBinding,
    ScanClauseSource,
};
use crate::expr::relation::PlanRelationResolver;
use crate::plan_data::{PlanDataReader, PlanDataWriter};

use super::LagodbCustomModifyProvider;

pub(super) struct ModifyScanFilters<P>(PhantomData<fn() -> P>);

impl<P: LagodbCustomModifyProvider> ModifyScanFilters<P> {
    /// Store records by resultRelations position, not RTI: setrefs renumbers RTIs
    /// when flattening subqueries but preserves the result-relation ordering.
    ///
    /// # Safety
    /// All nodes belong to the current planner invocation, before setrefs.
    pub(super) unsafe fn plan(
        root: *mut pg_sys::PlannerInfo,
        modify: *mut pg_sys::ModifyTable,
        outer: *mut pg_sys::CustomScan,
    ) -> Result<(), CustomScanError> {
        let relations = unsafe { (*modify).resultRelations };
        // Match the planner's RTI-indexed relation array. Collect once, including
        // when every target uses CustomScan and no SeqScan filters are needed.
        let mut seq_scans =
            vec![false; unsafe { (*root).simple_rel_array_size } as usize];
        unsafe { Self::collect_seq_scans((*modify).plan.lefttree, &mut seq_scans) };
        let mut records = ptr::null_mut();
        let mut expressions = ptr::null_mut();
        for index in 0..unsafe { pg_sys::list_length(relations) } {
            let rti =
                unsafe { pg_sys::list_nth_int(relations, index) } as pg_sys::Index;
            let record = if seq_scans[rti as usize] {
                unsafe { Self::plan_relation(root, rti, &mut expressions) }?
            } else {
                ptr::null_mut()
            };
            records = unsafe { pg_sys::lappend(records, record.cast()) };
        }
        unsafe {
            (*outer).custom_private = records;
            (*outer).custom_exprs = expressions;
        }
        Ok(())
    }

    unsafe fn plan_relation(
        root: *mut pg_sys::PlannerInfo,
        rti: pg_sys::Index,
        expressions: &mut *mut pg_sys::List,
    ) -> Result<*mut pg_sys::List, CustomScanError> {
        let relation = unsafe { *(*root).simple_rel_array.add(rti as usize) };
        let resolver = PlanRelationResolver::new(root);
        let rte = unsafe { resolver.query_rte(rti) };
        let context = RelationContext::from_ref(unsafe { &*rte });
        if !P::supports_modify_target(&context) {
            return Ok(ptr::null_mut());
        }
        let user = unsafe { (*relation).userid };
        let context = FilterPlanningContext::new(
            unsafe { resolver.rel_oid(rti) },
            rti,
            if user == pg_sys::InvalidOid {
                unsafe { pg_sys::GetUserId() }
            } else {
                user
            },
        );
        let mut planner =
            P::begin_filter_planning(&context).map_err(CustomScanError::provider)?;
        let mut filters = unsafe {
            FilterNegotiator::new(&mut planner, relation).negotiate(
                (*relation).baserestrictinfo,
                ScanClauseSource::BaseRestriction,
            )
        }
        .map_err(CustomScanError::provider)?;
        // Bind constants and external parameters once per execution, using the
        // same rescan-stable classification as CustomScan task planning. Exec
        // parameters and outer values stay in PG quals: they can change on
        // rescan. Preserve source metadata for the provider's conflict scope.
        let bindings = mem::take(&mut filters.bindings);
        filters.planned.retain_mut(|filter| {
            let values = &bindings
                [filter.binding_start..filter.binding_start + filter.binding_count];
            if !values
                .iter()
                .all(|value| value.metadata().source_kind.is_rescan_stable())
            {
                return false;
            }
            filter.binding_start = filters.bindings.len();
            filters.bindings.extend_from_slice(values);
            true
        });
        let offset = unsafe { pg_sys::list_length(*expressions) } as usize;
        for binding in &filters.bindings {
            *expressions =
                unsafe { pg_sys::lappend(*expressions, binding.expr().cast()) };
        }
        let encoded = FilterDataCodec::<P>::encode(&filters)?;
        PlanDataWriter::encode_list(|writer| {
            writer
                .append_count(filters.planned.len())
                .append_count(filters.bindings.len())
                .append_count(offset);
            unsafe {
                writer.append_list(encoded.planned);
                writer.append_list(encoded.bindings);
            }
            Ok(())
        })
    }

    /// # Safety
    /// `outer` owns the plan data and `scan` is its initialized target SeqScan.
    pub(super) unsafe fn bind(
        outer: *mut pg_sys::CustomScan,
        index: i32,
        scan: *mut pg_sys::PlanState,
    ) -> Result<RelationFilterBinding<P>, CustomScanError> {
        let record =
            unsafe { pg_sys::list_nth((*outer).custom_private, index).cast() };
        let (planned, bindings, offset, count) = unsafe {
            PlanDataReader::decode_checked_list::<_, CustomScanError>(
                record,
                0,
                |reader| {
                    let planned_count = reader.read_count()?;
                    let count = reader.read_count()?;
                    let offset = reader.read_count()?;
                    let planned =
                        reader.read_optional_list(pg_sys::NodeTag::T_List)?;
                    let bindings = reader.read_encoded_list()?;
                    let (planned, bindings) = FilterDataCodec::<P>::decode(
                        planned,
                        planned_count,
                        bindings,
                        count,
                    )?;
                    Ok((planned, bindings, offset, count))
                },
            )
        }?;
        // Copy only this target's cells; expressions remain owned by the plan.
        // PG lists provide constant-time indexed access.
        let mut expressions = ptr::null_mut();
        for index in offset..offset + count {
            let expr =
                unsafe { pg_sys::list_nth((*outer).custom_exprs, index as i32) };
            expressions = unsafe { pg_sys::lappend(expressions, expr) };
        }
        let runtime = unsafe {
            RelationFilterBinding::<P>::initialize(
                planned,
                bindings,
                expressions,
                scan,
            )
        };
        // Initialization retains ExprStates and parameter metadata, not these
        // temporary list cells. Release them on both success and returned error.
        unsafe { pg_sys::list_free(expressions) };
        let mut runtime = runtime?;
        unsafe { runtime.bind_stable((*scan).ps_ExprContext) }?;
        Ok(runtime)
    }

    unsafe fn collect_seq_scans(plan: *mut pg_sys::Plan, seq_scans: &mut [bool]) {
        if plan.is_null() {
            return;
        }
        let children = match unsafe { (*plan).type_ } {
            pg_sys::NodeTag::T_SeqScan => {
                let rti = unsafe { (*plan.cast::<pg_sys::SeqScan>()).scan.scanrelid };
                seq_scans[rti as usize] = true;
                return;
            }
            pg_sys::NodeTag::T_Append => unsafe {
                (*plan.cast::<pg_sys::Append>()).appendplans
            },
            pg_sys::NodeTag::T_MergeAppend => unsafe {
                (*plan.cast::<pg_sys::MergeAppend>()).mergeplans
            },
            pg_sys::NodeTag::T_CustomScan => unsafe {
                (*plan.cast::<pg_sys::CustomScan>()).custom_plans
            },
            _ => ptr::null_mut(),
        };
        for index in 0..unsafe { pg_sys::list_length(children) } {
            let child = unsafe { pg_sys::list_nth(children, index).cast() };
            unsafe { Self::collect_seq_scans(child, seq_scans) };
        }
        unsafe {
            Self::collect_seq_scans((*plan).lefttree, seq_scans);
            Self::collect_seq_scans((*plan).righttree, seq_scans);
        }
    }
}
