//! Cancel transaction-local staged creation when its PostgreSQL relation drops.

use lagodb_core::hooks::{
    OBJECT_ACCESS_DROP, ObjectAccessEvent, ObjectAccessFilter, ObjectAccessHook,
    ObjectAccessHookError,
};
use pgrx::pg_sys;

use super::super::transaction::ForeignTransaction;

pub(super) struct IcebergForeignTableDropHook;

impl ObjectAccessHook for IcebergForeignTableDropHook {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(OBJECT_ACCESS_DROP)
            .for_class(pg_sys::RelationRelationId)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        let ObjectAccessEvent::Drop {
            class_id,
            object_id,
            sub_id: 0,
            ..
        } = event
        else {
            return Ok(());
        };
        if *class_id == pg_sys::RelationRelationId {
            ForeignTransaction::drop_staged_relation(*object_id)?;
        }
        Ok(())
    }
}
