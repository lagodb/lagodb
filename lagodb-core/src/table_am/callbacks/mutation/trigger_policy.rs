//! Cold-path trigger capability checks shared by ModifyTable and COPY FROM.

use std::slice;

use pgrx::{PgSqlErrorCode, pg_sys};

use crate::diag::PgReportError;
use crate::handles::RelationHandle;
use crate::table_am::{AmResult, ModifyActions};

/// Trigger support for one locked provider relation.
///
/// ModifyTable and COPY targets support immediate triggers backed by core's
/// statement-scoped row stores.
pub(crate) struct RelationTriggerPolicy<'relation> {
    triggers: &'relation [pg_sys::Trigger],
}

impl<'relation> RelationTriggerPolicy<'relation> {
    /// The caller must have selected the relation's provider and retain the
    /// execution lock while inspecting its trigger metadata.
    pub(crate) fn new(relation: &'relation RelationHandle<'_>) -> Self {
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
        Self { triggers }
    }

    /// Called only when binding an actual LagoDB AM COPY session.
    /// Returns whether successful inserts need a retained trigger row.
    pub(super) fn bind_copy_session(&self) -> AmResult<bool> {
        self.validate_immediate_rows(ModifyActions::INSERT)?;
        Ok(self
            .triggers
            .iter()
            .any(|trigger| Self::is_after_row_for(trigger, ModifyActions::INSERT)))
    }

    pub(crate) fn validate_immediate_rows(
        &self,
        actions: ModifyActions,
    ) -> AmResult<()> {
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
