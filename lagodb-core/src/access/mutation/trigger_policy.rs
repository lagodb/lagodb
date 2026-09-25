//! Cold-path trigger capability checks shared by ModifyTable and COPY FROM.

use std::slice;

use pgrx::{PgSqlErrorCode, pg_sys};

use crate::api::{AmResult, ModifyActions};
use crate::diag::PgReportError;
use crate::handles::RelationHandle;

/// Trigger support for one locked provider relation.
///
/// Provider-owned partitioned tables cannot enter PostgreSQL's native AFTER ROW queue:
/// that path requires physical leaf relations for partitioned-table events.
/// Ordinary ModifyTable targets retain the existing immediate-trigger support
/// backed by core's query-scoped row store.
pub(crate) struct RelationTriggerPolicy<'relation> {
    triggers: &'relation [pg_sys::Trigger],
    partitioned_table: bool,
}

impl<'relation> RelationTriggerPolicy<'relation> {
    /// The caller must have selected the relation's provider and retain the
    /// execution lock while inspecting its trigger metadata.
    pub(crate) fn new(
        relation: &'relation RelationHandle<'_>,
        provider_owns_partitioned_table: bool,
    ) -> Self {
        // SAFETY: the relation handle retains live relcache metadata. PostgreSQL's
        // RelationBuildTriggers leaves trigdesc NULL for zero triggers and
        // otherwise supplies an initialized array of numtriggers elements.
        let triggers = unsafe {
            match (*relation.as_raw()).trigdesc.as_ref() {
                Some(descriptor) => slice::from_raw_parts(
                    descriptor.triggers,
                    descriptor.numtriggers as usize,
                ),
                None => &[],
            }
        };
        Self {
            triggers,
            partitioned_table: provider_owns_partitioned_table
                && relation.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE,
        }
    }

    pub(crate) fn validate_modify(
        &self,
        command: pg_sys::CmdType::Type,
        actions: ModifyActions,
    ) -> AmResult<()> {
        if self.partitioned_table {
            return self.validate_partitioned_table(actions);
        }

        // Preserve the existing deferred-trigger restriction for ordinary
        // targets, including UPDATE's physical partition-movement events.
        let actions = match command {
            pg_sys::CmdType::CMD_INSERT => {
                ModifyActions::INSERT.union(ModifyActions::UPDATE)
            }
            pg_sys::CmdType::CMD_DELETE => ModifyActions::DELETE,
            pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_MERGE => {
                ModifyActions::INSERT
                    .union(ModifyActions::UPDATE)
                    .union(ModifyActions::DELETE)
            }
            _ => ModifyActions::NONE,
        };
        if self.triggers.iter().any(|trigger| {
            trigger.tgdeferrable && Self::is_after_row_for(trigger, actions)
        }) {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "LagoDB does not support deferrable AFTER ROW triggers; \
                 retained OLD/NEW rows have statement lifetime",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_copy_from(&self) -> AmResult<()> {
        self.validate_partitioned_table(ModifyActions::INSERT)
    }

    fn validate_partitioned_table(&self, actions: ModifyActions) -> AmResult<()> {
        // Check declared trigger shape even for disabled/WHEN-false triggers:
        // PostgreSQL validates partitioned table row events before TriggerEnabled.
        if self.partitioned_table
            && self
                .triggers
                .iter()
                .any(|trigger| Self::is_after_row_for(trigger, actions))
        {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "LagoDB does not support AFTER ROW triggers on provider-owned partitioned tables",
            ));
        }
        Ok(())
    }

    fn is_after_row_for(trigger: &pg_sys::Trigger, actions: ModifyActions) -> bool {
        let trigger_type = u32::from(trigger.tgtype as u16);
        trigger_type & pg_sys::TRIGGER_TYPE_ROW != 0
            && trigger_type & pg_sys::TRIGGER_TYPE_TIMING_MASK
                == pg_sys::TRIGGER_TYPE_AFTER
            && ((actions.may_insert()
                && trigger_type & pg_sys::TRIGGER_TYPE_INSERT != 0)
                || (actions.may_update()
                    && trigger_type & pg_sys::TRIGGER_TYPE_UPDATE != 0)
                || (actions.may_delete()
                    && trigger_type & pg_sys::TRIGGER_TYPE_DELETE != 0))
    }
}
