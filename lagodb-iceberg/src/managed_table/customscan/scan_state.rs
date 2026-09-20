//! Iceberg-specific runtime state and scan lifecycle.

use std::sync::Arc;

use crate::error::IcebergError;
use crate::managed_table::access::mutation::{
    IcebergModifyQueryState, IcebergModifyScanContext,
};
use crate::managed_table::access::scan::{BatchCursor, ScanSpec};
use crate::predicate::BoundIcebergPredicate;
use crate::scan::ScanError;
use crate::scan::parallel::{TaskGrouping, WorkerSource};
use crate::schema::relation::RelationShape;
use iceberg_lite::expr::Predicate;
use lagodb_core::access::mutation::ModifyScanBinding;
use lagodb_core::customscan::modify::ModifyBindContext;
use lagodb_core::customscan::provider::{
    BeginContext, CustomScanError, EndContext, NextSlotContext, ReScanContext,
    ScanPurpose,
};
use lagodb_core::parallel_scan::{ParallelScanCoordinator, PreparedParallelScan};
use pgrx::pg_sys;

use super::IcebergCustomScanProvider;
use super::projection::ProjectionResolver;

/// Per-scan runtime state inside the framework's `CustomScanStateWrapper`.
pub(crate) struct IcebergScanState {
    active_scan: Option<ScanSpec>,
    cursor: Option<BatchCursor>,
    conflict_filter: Predicate,
    purpose: ScanPurpose,
    modify_binding: Option<ModifyScanBinding<IcebergModifyQueryState>>,
    parallel: ParallelScanCoordinator,
    parallel_source: Option<WorkerSource>,
    parallel_aware: bool,
}

impl Default for IcebergScanState {
    fn default() -> Self {
        Self {
            active_scan: None,
            cursor: None,
            conflict_filter: Predicate::AlwaysTrue,
            purpose: ScanPurpose::Query,
            modify_binding: None,
            parallel: ParallelScanCoordinator::default(),
            parallel_source: None,
            parallel_aware: false,
        }
    }
}

impl IcebergScanState {
    /// Build [`ScanSpec`]/[`BatchCursor`] and install already-bound
    /// planned predicates.
    pub(super) fn begin(
        ctx: BeginContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let rel_oid = ctx.relation.oid();
        let spc_oid = ctx.relation.tablespace_oid();
        let scan_tuple = ctx.scan_tuple();
        let projection =
            ProjectionResolver.resolve(ctx.required_columns(), scan_tuple)?;
        let shape = RelationShape::from_relation(&ctx.relation)?;

        let mut spec = match projection {
            None => {
                ScanSpec::build_for_custom_scan(rel_oid, spc_oid, None, None, &shape)?
            }
            Some(proj) => {
                let scan_attr_types = scan_tuple.attr_types();
                ScanSpec::build_with_projection(
                    rel_oid,
                    spc_oid,
                    proj,
                    None,
                    None,
                    &shape,
                    &scan_attr_types,
                )?
            }
        };

        BoundIcebergPredicate::validate_schema(ctx.filters.iter(), spec.schema_id())
            .map_err(CustomScanError::provider)?;
        let row_filter = BoundIcebergPredicate::conjoin(ctx.filters.iter());
        let planning_filter =
            BoundIcebergPredicate::conjoin(ctx.filters.rescan_stable());
        let conflict_filter = if ctx.purpose.is_modify() {
            BoundIcebergPredicate::conjoin(ctx.filters.static_values())
                .unwrap_or(Predicate::AlwaysTrue)
        } else {
            Predicate::AlwaysTrue
        };
        spec.set_predicates(planning_filter, row_filter);

        let purpose = ctx.purpose;
        let parallel_aware = ctx.parallel_aware;
        let state = ctx.state;
        state.purpose = purpose;
        state.parallel_aware = parallel_aware;
        state.conflict_filter = conflict_filter;
        if purpose.is_modify() {
            spec.prepare_mutation_tasks()?;
        }

        let cursor = if purpose.is_modify() || parallel_aware {
            None
        } else {
            Some(spec.open_batch_cursor()?)
        };
        if parallel_aware && unsafe { pg_sys::ParallelWorkerNumber } < 0 {
            let tasks = spec.planned_query_tasks()?;
            let grouped = TaskGrouping::from_properties(spec.table_properties())?
                .group(&tasks)?;
            let work_count = u32::try_from(grouped.group_count()).map_err(|_| {
                CustomScanError::provider(ScanError::WorkerPayload(
                    "native parallel group count exceeds u32".to_owned(),
                ))
            })?;
            let bytes =
                WorkerSource::encode(&spec.query_arrow_schema()?, &tasks, grouped)?;
            state.parallel.prepare(
                PreparedParallelScan::new(bytes, work_count)
                    .map_err(CustomScanError::internal)?,
            );
        }
        state.active_scan = Some(spec);
        state.cursor = cursor;
        state.modify_binding = None;

        Ok(())
    }

    pub(super) fn modify_scan_context(&self) -> Option<IcebergModifyScanContext> {
        self.active_scan.as_ref().and_then(|scan| {
            let scan_tasks = scan.prepared_mutation_tasks()?;
            Some(IcebergModifyScanContext::new(
                scan.starting_snapshot_id(),
                self.conflict_filter.clone(),
                scan_tasks,
            ))
        })
    }

    pub(super) fn bind_modify(
        ctx: ModifyBindContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let binding = ctx.binding;
        let state = ctx.state;
        match state.modify_binding.as_ref() {
            Some(existing) if existing == &binding => return Ok(()),
            Some(_) => {
                return Err(CustomScanError::provider(
                    IcebergError::InvariantViolated(
                        "Modify scan was bound to two relation states",
                    ),
                ));
            }
            None => {}
        }
        state.active_scan.as_ref().ok_or_else(|| {
            CustomScanError::provider(IcebergError::InvariantViolated(
                "Modify scan binding has no scan specification",
            ))
        })?;
        state.modify_binding = Some(binding);
        Ok(())
    }

    /// Drive the slot-first cursor straight into the scan slot via
    /// [`NextSlotContext::emit_columns`]. Returns `Ok(false)` at end-of-scan
    /// without touching the slot.
    pub(super) fn next_slot(
        mut ctx: NextSlotContext<'_, IcebergCustomScanProvider>,
    ) -> Result<bool, CustomScanError> {
        if ctx.state.parallel_aware {
            return Self::next_parallel_slot(ctx);
        }
        let purpose = ctx.state.purpose;
        let mut cursor = match ctx.state.cursor.take() {
            Some(cursor) => cursor,
            None if purpose.is_modify() => {
                let binding = ctx.state.modify_binding.clone().ok_or_else(|| {
                    CustomScanError::provider(IcebergError::InvariantViolated(
                        "Modify scan executed before outer binding",
                    ))
                })?;
                ctx.state
                    .active_scan
                    .as_mut()
                    .ok_or_else(|| {
                        CustomScanError::provider(IcebergError::InvariantViolated(
                            "Modify scan has no scan specification",
                        ))
                    })?
                    .open_mutation_batch_cursor(binding, ctx.relation.oid())?
            }
            None => return Ok(false),
        };

        let result = ctx.emit_columns(&mut cursor);
        ctx.state.cursor = Some(cursor);
        result
    }

    fn next_parallel_slot(
        mut ctx: NextSlotContext<'_, IcebergCustomScanProvider>,
    ) -> Result<bool, CustomScanError> {
        loop {
            if let Some(mut cursor) = ctx.state.cursor.take() {
                let produced = ctx.emit_columns(&mut cursor)?;
                if produced {
                    ctx.state.cursor = Some(cursor);
                    return Ok(true);
                }
            }
            let Some(work_id) = ctx
                .state
                .parallel
                .claim()
                .map_err(CustomScanError::internal)?
            else {
                return Ok(false);
            };
            let tasks = ctx
                .state
                .parallel_source
                .as_ref()
                .ok_or_else(|| {
                    CustomScanError::internal(std::io::Error::other(
                        "parallel Iceberg source is not attached",
                    ))
                })?
                .take_tasks(work_id)?;
            let spec = ctx.state.active_scan.as_ref().ok_or_else(|| {
                CustomScanError::provider(IcebergError::InvariantViolated(
                    "parallel scan has no scan specification",
                ))
            })?;
            ctx.state.cursor =
                Some(spec.open_batch_cursor_with_tasks(Arc::from(
                    tasks.into_boxed_slice(),
                ))?);
        }
    }

    /// Replace the complete row filter when values changed; always reopen the
    /// cursor without replanning stable file tasks.
    pub(super) fn rescan(
        ctx: ReScanContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let relation_oid = ctx.relation.oid();
        let replacement = ctx
            .filters_changed
            .then(|| BoundIcebergPredicate::conjoin(ctx.filters.iter()));

        let state = ctx.state;
        let Some(spec) = state.active_scan.as_mut() else {
            return Ok(());
        };
        if let Some(predicate) = replacement {
            spec.set_row_filter(predicate);
        }

        if state.parallel_aware {
            state.cursor = None;
            state.attach_parallel_source()?;
            return Ok(());
        }
        state.cursor = Some(if state.purpose.is_modify() {
            let binding = state.modify_binding.clone().ok_or_else(|| {
                CustomScanError::provider(IcebergError::InvariantViolated(
                    "Modify rescan occurred before outer binding",
                ))
            })?;
            spec.open_mutation_batch_cursor(binding, relation_oid)?
        } else {
            spec.open_batch_cursor()?
        });
        Ok(())
    }

    pub(super) fn end(
        ctx: EndContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let state = ctx.state;
        // Drop cursor before the active scan so IO closes before
        // metadata/predicate teardown.
        let _ = state.cursor.take();
        let _ = state.active_scan.take();
        state.modify_binding = None;
        state.parallel_source = None;
        state.parallel.detach();
        Ok(())
    }

    pub(super) fn estimate_dsm(&mut self) -> Result<pg_sys::Size, CustomScanError> {
        self.parallel.estimate().map_err(CustomScanError::internal)
    }

    pub(super) unsafe fn initialize_dsm(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.parallel.initialize(coordinate) }
            .map_err(CustomScanError::internal)?;
        self.attach_parallel_source()
    }

    pub(super) unsafe fn reinitialize_dsm(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.parallel.attach(coordinate) }
            .map_err(CustomScanError::internal)?;
        self.parallel
            .reinitialize()
            .map_err(CustomScanError::internal)?;
        Ok(())
    }

    pub(super) unsafe fn initialize_worker(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.parallel.attach(coordinate) }
            .map_err(CustomScanError::internal)?;
        self.attach_parallel_source()
    }

    fn attach_parallel_source(&mut self) -> Result<(), CustomScanError> {
        // SAFETY: `parallel_source` is cleared before the coordinator detaches
        // this immutable native-parallel DSM payload on every lifecycle path.
        let source = unsafe {
            WorkerSource::decode_shared(
                self.parallel.payload().map_err(CustomScanError::internal)?,
                self.active_scan
                    .as_ref()
                    .ok_or_else(|| {
                        CustomScanError::provider(IcebergError::InvariantViolated(
                            "parallel scan has no scan specification",
                        ))
                    })?
                    .file_io(),
            )
        }?;
        let expected = self
            .active_scan
            .as_ref()
            .ok_or_else(|| {
                CustomScanError::provider(IcebergError::InvariantViolated(
                    "parallel scan has no scan specification",
                ))
            })?
            .query_arrow_schema()?;
        if source.schema().as_ref() != expected.as_ref() {
            return Err(CustomScanError::provider(ScanError::WorkerPayload(
                "worker relation view does not match the leader scan schema"
                    .to_owned(),
            )));
        }
        self.parallel_source = Some(source);
        Ok(())
    }

    pub(super) fn shutdown_parallel(&mut self) {
        self.cursor = None;
        self.parallel_source = None;
        self.parallel.detach();
    }
}
