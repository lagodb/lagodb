//! Provider-neutral relation execution owned by one Custom ModifyTable node.

use std::collections::HashMap;
use std::ptr::NonNull;

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::access::mutation::ModifyScanBinding;
use crate::api::{
    AmModifyState, AmResult, ModifyActions, ModifyQueryState, ModifyStateContext,
    MutationDeleteContext, MutationOutcome, MutationUpdateContext,
    MutationWriteContext, TableAccessMethod,
};
use crate::customscan::modify::LagodbCustomModifyProvider;
use crate::diag::PgReportError;
use crate::handles::{ItemPointer, RelationHandle};
use crate::tuple::TupleSlotRow;

#[derive(Debug)]
enum RelationAccess<C> {
    Unbound,
    TargetRead(C),
    InsertOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelationPhase {
    Ready,
    Finished,
    Aborted,
}

/// Derive the concrete operation set from the executor's immutable plan.
///
/// PostgreSQL stores one MERGE action list per target relation. The relation
/// AM needs only their union: a sink is necessary if any target can execute the
/// corresponding action.
unsafe fn modify_actions(plan: *mut pg_sys::ModifyTable) -> AmResult<ModifyActions> {
    match unsafe { (*plan).operation } {
        pg_sys::CmdType::CMD_INSERT => Ok(ModifyActions::INSERT),
        pg_sys::CmdType::CMD_UPDATE => Ok(ModifyActions::UPDATE),
        pg_sys::CmdType::CMD_DELETE => Ok(ModifyActions::DELETE),
        pg_sys::CmdType::CMD_MERGE => {
            let action_lists = unsafe { (*plan).mergeActionLists };
            let mut result = ModifyActions::NONE;
            for list_index in 0..unsafe { pg_sys::list_length(action_lists) } {
                let actions = unsafe {
                    pg_sys::list_nth(action_lists, list_index).cast::<pg_sys::List>()
                };
                for action_index in 0..unsafe { pg_sys::list_length(actions) } {
                    let action = unsafe {
                        pg_sys::list_nth(actions, action_index)
                            .cast::<pg_sys::MergeAction>()
                    };
                    match unsafe { (*action).commandType } {
                        pg_sys::CmdType::CMD_INSERT => {
                            result = result.union(ModifyActions::INSERT);
                        }
                        pg_sys::CmdType::CMD_UPDATE => {
                            result = result.union(ModifyActions::UPDATE);
                        }
                        pg_sys::CmdType::CMD_DELETE => {
                            result = result.union(ModifyActions::DELETE);
                        }
                        pg_sys::CmdType::CMD_NOTHING => {}
                        _ => {
                            return Err(internal_error(
                                "MERGE plan contains an unsupported action",
                            ));
                        }
                    }
                }
            }
            Ok(result)
        }
        _ => Err(internal_error("unsupported ModifyTable command")),
    }
}

pub(super) struct ResultRelationState<P: LagodbCustomModifyProvider> {
    relation: NonNull<pg_sys::RelationData>,
    command: pg_sys::CmdType::Type,
    actions: ModifyActions,
    query_state: ModifyQueryState<
        <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
    >,
    access: RelationAccess<
        <<P::AccessMethod as TableAccessMethod>::ModifyState as AmModifyState>::ScanContext,
    >,
    modify_state: Option<<P::AccessMethod as TableAccessMethod>::ModifyState>,
    phase: RelationPhase,
}

impl<P: LagodbCustomModifyProvider> ResultRelationState<P> {
    unsafe fn new(
        result_rel_info: NonNull<pg_sys::ResultRelInfo>,
        command: pg_sys::CmdType::Type,
        actions: ModifyActions,
        query_state: ModifyQueryState<
            <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
        >,
    ) -> AmResult<Self> {
        let relation = unsafe {
            NonNull::new_unchecked(result_rel_info.as_ref().ri_RelationDesc)
        };
        if !P::MODIFY_CAPABILITIES.postgres_indexes()
            && unsafe { (*relation.as_ref().rd_rel).relhasindex }
        {
            return Err(feature_not_supported(
                "the Custom ModifyTable provider does not support PostgreSQL indexes",
            ));
        }
        let trigger_desc = unsafe { result_rel_info.as_ref().ri_TrigDesc };
        if !trigger_desc.is_null() {
            let trigger_count =
                usize::try_from(unsafe { (*trigger_desc).numtriggers })
                    .map_err(|_| internal_error("invalid trigger count"))?;
            let triggers = unsafe {
                std::slice::from_raw_parts(
                    if trigger_count == 0 {
                        NonNull::<pg_sys::Trigger>::dangling().as_ptr()
                    } else {
                        (*trigger_desc).triggers
                    },
                    trigger_count,
                )
            };
            let has_deferred_row_trigger = triggers.iter().any(|trigger| {
                let trigger_type = u32::from(trigger.tgtype as u16);
                let can_fire_for_command = match command {
                    pg_sys::CmdType::CMD_INSERT => {
                        trigger_type
                            & (pg_sys::TRIGGER_TYPE_INSERT
                                | pg_sys::TRIGGER_TYPE_UPDATE)
                            != 0
                    }
                    pg_sys::CmdType::CMD_DELETE => {
                        trigger_type & pg_sys::TRIGGER_TYPE_DELETE != 0
                    }
                    pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_MERGE => {
                        trigger_type
                            & (pg_sys::TRIGGER_TYPE_INSERT
                                | pg_sys::TRIGGER_TYPE_UPDATE
                                | pg_sys::TRIGGER_TYPE_DELETE)
                            != 0
                    }
                    _ => false,
                };
                trigger.tgdeferrable
                    && can_fire_for_command
                    && trigger_type & pg_sys::TRIGGER_TYPE_ROW != 0
                    && trigger_type & pg_sys::TRIGGER_TYPE_TIMING_MASK
                        == pg_sys::TRIGGER_TYPE_AFTER
            });
            if has_deferred_row_trigger {
                return Err(feature_not_supported(
                    "LagoDB does not support deferrable AFTER ROW triggers; \
                     retained OLD/NEW rows have statement lifetime",
                ));
            }
        }
        Ok(Self {
            relation,
            command,
            actions,
            query_state,
            access: RelationAccess::Unbound,
            modify_state: None,
            phase: RelationPhase::Ready,
        })
    }

    fn relation_oid(&self) -> pg_sys::Oid {
        // SAFETY: the owning ModifyTable keeps the result relation open.
        unsafe { self.relation.as_ref().rd_id }
    }

    fn ensure_ready(&self) -> AmResult<()> {
        match self.phase {
            RelationPhase::Ready => Ok(()),
            RelationPhase::Finished => Err(internal_error(
                "mutation relation execution is already finished",
            )),
            RelationPhase::Aborted => {
                Err(internal_error("mutation relation execution was aborted"))
            }
        }
    }

    fn create_modify_state(
        &mut self,
        context: ModifyStateContext<
            <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
            <<P::AccessMethod as TableAccessMethod>::ModifyState as AmModifyState>::ScanContext,
        >,
    ) -> AmResult<()> {
        // SAFETY: the owning ModifyTableState keeps the relation open; the AM
        // contract requires the state to retain only derived owned data.
        let relation = unsafe { RelationHandle::from_raw(self.relation.as_ptr()) };
        let modify_state =
            <P::AccessMethod as TableAccessMethod>::ModifyState::begin_modify(
                &relation, context,
            )?;
        self.modify_state = Some(modify_state);
        Ok(())
    }

    fn bind_scan(
        &mut self,
        context: <<P::AccessMethod as TableAccessMethod>::ModifyState as AmModifyState>::ScanContext,
    ) -> AmResult<
        ModifyScanBinding<<P::AccessMethod as TableAccessMethod>::ModifyQueryState>,
    > {
        self.ensure_ready()?;
        match &self.access {
            RelationAccess::Unbound => {
                self.access = RelationAccess::TargetRead(context);
            }
            RelationAccess::TargetRead(existing) if existing == &context => {}
            RelationAccess::TargetRead(_) => {
                return Err(internal_error(
                    "Modify scans for one result relation captured different contexts",
                ));
            }
            RelationAccess::InsertOnly => {
                return Err(internal_error(
                    "Modify scan attempted to bind an insert-only relation",
                ));
            }
        }
        Ok(ModifyScanBinding::new(
            self.query_state.clone(),
            self.relation_oid(),
        ))
    }

    fn prepare_insert(&mut self) {
        if matches!(self.access, RelationAccess::Unbound) {
            self.access = RelationAccess::InsertOnly;
        }
    }

    // The mutation entry points perform the phase check once before reaching
    // this state-construction path. Keeping that check outside avoids a second
    // phase read for every INSERT callback.
    fn modify_state(
        &mut self,
    ) -> AmResult<&mut <P::AccessMethod as TableAccessMethod>::ModifyState> {
        if self.modify_state.is_none() {
            let context = match &self.access {
                RelationAccess::Unbound => {
                    return Err(internal_error(
                        "row-level mutation reached a relation before its target scan was bound",
                    ));
                }
                RelationAccess::TargetRead(context) => {
                    ModifyStateContext::target_read(
                        self.query_state.clone(),
                        self.command,
                        self.actions,
                        context.clone(),
                    )
                }
                RelationAccess::InsertOnly => ModifyStateContext::independent(
                    self.query_state.clone(),
                    if self.command == pg_sys::CmdType::CMD_MERGE {
                        pg_sys::CmdType::CMD_INSERT
                    } else {
                        self.command
                    },
                    ModifyActions::INSERT,
                ),
            };
            self.create_modify_state(context)?;
        }
        self.modify_state
            .as_mut()
            .ok_or_else(|| internal_error("AM modify state construction failed"))
    }

    fn finish(&mut self) -> AmResult<()> {
        match self.phase {
            RelationPhase::Finished => return Ok(()),
            RelationPhase::Aborted => {
                return Err(internal_error(
                    "cannot finish an aborted relation execution",
                ));
            }
            RelationPhase::Ready => {}
        }
        if let Some(modify_state) = self.modify_state.as_mut() {
            modify_state.end_modify()?;
        }
        self.phase = RelationPhase::Finished;
        Ok(())
    }

    fn abort(&mut self) {
        if self.phase != RelationPhase::Ready {
            return;
        }
        if let Some(modify_state) = self.modify_state.as_mut() {
            modify_state.abort_modify();
        }
        self.phase = RelationPhase::Aborted;
    }

    pub(super) unsafe fn insert(
        &mut self,
        new_slot: *mut pg_sys::TupleTableSlot,
        context: MutationWriteContext,
    ) -> AmResult<()> {
        self.ensure_ready()?;
        self.prepare_insert();
        let new = unsafe { TupleSlotRow::from_raw(new_slot) };
        self.modify_state()?.insert_slot(new, context)
    }

    pub(super) unsafe fn update(
        &mut self,
        row_id: ItemPointer,
        old_slot: *mut pg_sys::TupleTableSlot,
        new_slot: *mut pg_sys::TupleTableSlot,
        context: MutationUpdateContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let old = unsafe { TupleSlotRow::from_raw(old_slot) };
        let new = unsafe { TupleSlotRow::from_raw(new_slot) };
        self.ensure_ready()?;
        self.modify_state()?.update_slot(row_id, old, new, context)
    }

    pub(super) fn delete(
        &mut self,
        row_id: ItemPointer,
        context: MutationDeleteContext<'_>,
    ) -> AmResult<MutationOutcome> {
        self.ensure_ready()?;
        self.modify_state()?.delete_slot(row_id, context)
    }

    pub(super) unsafe fn preserve_trigger_row(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> AmResult<ItemPointer> {
        self.ensure_ready()?;
        unsafe {
            self.query_state.preserve_trigger_row::<P::AccessMethod>(
                self.relation_oid(),
                self.relation.as_ref().rd_att,
                slot,
            )
        }
    }
}

/// Generic owner behind one Custom ModifyTable execution.
pub(super) struct ModifyNodeState<P: LagodbCustomModifyProvider> {
    query_state:
        ModifyQueryState<<P::AccessMethod as TableAccessMethod>::ModifyQueryState>,
    relations: Vec<Box<ResultRelationState<P>>>,
    relation_by_oid: HashMap<pg_sys::Oid, NonNull<ResultRelationState<P>>>,
    relation_by_info:
        HashMap<NonNull<pg_sys::ResultRelInfo>, NonNull<ResultRelationState<P>>>,
    access_method_oid: pg_sys::Oid,
    command: pg_sys::CmdType::Type,
    actions: ModifyActions,
    phase: RelationPhase,
}

pub(super) struct PreparedUpdateTriggerRows {
    pub old_tid: Option<ItemPointer>,
    pub new_tid: Option<ItemPointer>,
}

impl<P: LagodbCustomModifyProvider> ModifyNodeState<P> {
    /// # Safety
    ///
    /// `mtstate`, its result relation array, and all relation descriptors must
    /// remain live until this execution is ended.
    pub(super) unsafe fn from_modify_table_state(
        mtstate: *mut pg_sys::ModifyTableState,
        query_state: ModifyQueryState<
            <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
        >,
    ) -> AmResult<Self> {
        let state = unsafe { &*mtstate };
        let count = usize::try_from(state.mt_nrels).map_err(|_| {
            internal_error("invalid ModifyTable result relation count")
        })?;
        if count == 0 {
            return Err(internal_error(
                "ModifyTableState has no initialized result relations",
            ));
        }

        let plan = state.ps.plan.cast::<pg_sys::ModifyTable>();
        let actions = unsafe { modify_actions(plan) }?;
        if !P::MODIFY_CAPABILITIES.speculative_insert()
            && unsafe { (*plan).onConflictAction }
                != pg_sys::OnConflictAction::ONCONFLICT_NONE
        {
            return Err(feature_not_supported(
                "the Custom ModifyTable provider does not support speculative insertion",
            ));
        }

        let mut relations = Vec::with_capacity(count);
        let mut relation_by_oid = HashMap::with_capacity(count);
        let mut relation_by_info = HashMap::with_capacity(count);
        let access_method_oid = P::AccessMethod::access_method_oid()
            .ok_or_else(|| internal_error("registered access method is missing"))?;
        for index in 0..count {
            let info =
                unsafe { NonNull::new_unchecked(state.resultRelInfo.add(index)) };
            let relation_desc = unsafe { info.as_ref().ri_RelationDesc };
            if unsafe { (*(*relation_desc).rd_rel).relam } != access_method_oid {
                continue;
            }
            let relation_oid = unsafe { (*relation_desc).rd_id };
            if let Some(&existing) = relation_by_oid.get(&relation_oid) {
                relation_by_info.insert(info, existing);
                continue;
            }
            let mut relation = Box::new(unsafe {
                ResultRelationState::<P>::new(
                    info,
                    state.operation,
                    actions,
                    query_state.clone(),
                )
            }?);
            let pointer = NonNull::from(relation.as_mut());
            relation_by_oid.insert(relation_oid, pointer);
            relation_by_info.insert(info, pointer);
            relations.push(relation);
        }

        Ok(Self {
            query_state,
            relations,
            relation_by_oid,
            relation_by_info,
            access_method_oid,
            command: state.operation,
            actions,
            phase: RelationPhase::Ready,
        })
    }

    pub(super) fn bind_scan(
        &mut self,
        relation_oid: pg_sys::Oid,
        context: <<P::AccessMethod as TableAccessMethod>::ModifyState as AmModifyState>::ScanContext,
    ) -> AmResult<
        ModifyScanBinding<<P::AccessMethod as TableAccessMethod>::ModifyQueryState>,
    > {
        let pointer = self
            .relation_by_oid
            .get(&relation_oid)
            .copied()
            .ok_or_else(|| {
                internal_error("Modify scan relation is not a result relation")
            })?;
        // SAFETY: relation states are individually boxed and owned until this
        // execution is dropped.
        unsafe { &mut *pointer.as_ptr() }.bind_scan(context)
    }

    /// Resolve the provider-owned relation for a PostgreSQL result relation.
    ///
    /// # Safety
    ///
    /// `result_rel_info` is the live `ResultRelInfo` supplied by PostgreSQL's
    /// ModifyTable executor.
    pub(super) unsafe fn resolve_relation(
        &mut self,
        result_rel_info: *mut pg_sys::ResultRelInfo,
    ) -> AmResult<Option<NonNull<ResultRelationState<P>>>> {
        let info = unsafe { NonNull::new_unchecked(result_rel_info) };
        if let Some(&pointer) = self.relation_by_info.get(&info) {
            return Ok(Some(pointer));
        }
        let relation_desc = unsafe { info.as_ref().ri_RelationDesc };
        if unsafe { (*(*relation_desc).rd_rel).relam } != self.access_method_oid {
            return Ok(None);
        }
        let relation_oid = unsafe { (*relation_desc).rd_id };
        if let Some(&pointer) = self.relation_by_oid.get(&relation_oid) {
            self.relation_by_info.insert(info, pointer);
            return Ok(Some(pointer));
        }
        let mut relation = Box::new(unsafe {
            ResultRelationState::<P>::new(
                info,
                self.command,
                self.actions,
                self.query_state.clone(),
            )
        }?);
        let pointer = NonNull::from(relation.as_mut());
        self.relations.push(relation);
        self.relation_by_oid.insert(relation_oid, pointer);
        self.relation_by_info.insert(info, pointer);
        Ok(Some(pointer))
    }

    /// Preserve both sides of one UPDATE trigger event under the exact leaf
    /// relations PostgreSQL will later use to fetch OLD and NEW.
    ///
    /// A cross-partition update may cross AM boundaries, so either side can
    /// remain a native PostgreSQL TID. Keeping the pair in one operation
    /// prevents the C executor's one-entry relation cache from accidentally
    /// associating both rows with the same partition.
    pub(super) unsafe fn prepare_update_trigger_rows(
        &mut self,
        source_info: *mut pg_sys::ResultRelInfo,
        destination_info: *mut pg_sys::ResultRelInfo,
        old_slot: *mut pg_sys::TupleTableSlot,
        new_slot: *mut pg_sys::TupleTableSlot,
    ) -> AmResult<PreparedUpdateTriggerRows> {
        let source = unsafe { self.resolve_relation(source_info) }?;
        let destination = unsafe { self.resolve_relation(destination_info) }?;

        let old_tid = if let Some(mut source) = source {
            Some(unsafe { source.as_mut().preserve_trigger_row(old_slot) }?)
        } else {
            None
        };
        let new_tid = if let Some(mut destination) = destination {
            Some(unsafe { destination.as_mut().preserve_trigger_row(new_slot) }?)
        } else {
            None
        };

        Ok(PreparedUpdateTriggerRows { old_tid, new_tid })
    }

    pub(super) fn finish(&mut self) -> AmResult<()> {
        match self.phase {
            RelationPhase::Finished => return Ok(()),
            RelationPhase::Aborted => {
                return Err(internal_error(
                    "cannot finish an aborted Modify execution",
                ));
            }
            RelationPhase::Ready => {}
        }
        for relation in &mut self.relations {
            relation.finish()?;
        }
        self.phase = RelationPhase::Finished;
        Ok(())
    }

    pub(super) fn abort(&mut self) {
        if self.phase != RelationPhase::Ready {
            return;
        }
        for relation in &mut self.relations {
            relation.abort();
        }
        self.phase = RelationPhase::Aborted;
    }

    pub(super) fn is_finished(&self) -> bool {
        self.phase == RelationPhase::Finished
    }
}

impl<P: LagodbCustomModifyProvider> Drop for ModifyNodeState<P> {
    fn drop(&mut self) {
        self.abort();
    }
}

fn internal_error(message: impl Into<String>) -> PgReportError {
    PgReportError::from_message(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
}

fn feature_not_supported(message: impl Into<String>) -> PgReportError {
    PgReportError::from_message(
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        message,
    )
}
