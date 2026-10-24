//! Keep a transaction-staged foreign-table definition immutable until publish.
//!
//! The staged schema guard deliberately rejects every column-level create,
//! alter, or drop event, including local-only settings such as SET STATISTICS,
//! SET/RESET (n_distinct), and SET STORAGE. A column PostAlter event does not
//! imply an Iceberg schema change; rejecting these local settings is an
//! intentional restriction during staged creation. Configure them after the
//! CREATE transaction commits. Published foreign tables are unaffected, even
//! when they have uncommitted data writes.
//!
//! There is no current requirement to configure these attributes before
//! publication. Uniform rejection keeps this boundary simple: allowing a
//! subset would require classifying changes and inspecting old/new catalog
//! state solely for that convenience. Revisit this tradeoff only with an
//! explicit requirement to support local column changes during staged creation.
//!
//! After publication, the PostgreSQL and Iceberg schemas may evolve
//! independently. This module deliberately does not contact the catalog to
//! synchronize or validate local DDL. Reads and writes validate their referenced
//! columns when they reach the FDW boundary. PostgreSQL foreign-table constraints
//! such as NOT NULL remain user-maintained planner assertions: if one no longer
//! describes the remote data, the planner can eliminate a scan before any FDW
//! callback can report the mismatch.
//!
//! PostgreSQL reports column creation, alteration, and deletion against the
//! relation that actually changed, including changes propagated through
//! inheritance and typed-table dependencies. This is the final authorization
//! boundary; utility statement shape does not determine the affected relation.

use lagodb_core::hooks::{
    HookError, OBJECT_ACCESS_DROP, OBJECT_ACCESS_POST_ALTER,
    OBJECT_ACCESS_POST_CREATE, ObjectAccessEvent, ObjectAccessFilter,
    ObjectAccessHook, ObjectAccessHookError,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::super::transaction::ForeignTransaction;

pub(super) struct IcebergStagedSchemaGuard;
pub(super) struct IcebergStagedOptionsGuard;

impl ObjectAccessHook for IcebergStagedSchemaGuard {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(
            OBJECT_ACCESS_POST_CREATE | OBJECT_ACCESS_POST_ALTER | OBJECT_ACCESS_DROP,
        )
        .for_class(pg_sys::RelationRelationId)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        let relation_oid = match event {
            ObjectAccessEvent::PostCreate {
                class_id,
                object_id,
                sub_id,
                ..
            }
            | ObjectAccessEvent::PostAlter {
                class_id,
                object_id,
                sub_id,
                ..
            }
            | ObjectAccessEvent::Drop {
                class_id,
                object_id,
                sub_id,
                ..
            } if *class_id == pg_sys::RelationRelationId && *sub_id > 0 => *object_id,
            _ => return Ok(()),
        };
        if ForeignTransaction::is_staged_relation(relation_oid) {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot change the schema of an Iceberg foreign table before its staged creation is published",
            ));
        }
        Ok(())
    }
}

impl ObjectAccessHook for IcebergStagedOptionsGuard {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(OBJECT_ACCESS_POST_ALTER)
            .for_class(pg_sys::ForeignTableRelationId)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        let ObjectAccessEvent::PostAlter {
            class_id,
            object_id,
            sub_id: 0,
            ..
        } = event
        else {
            return Ok(());
        };
        if *class_id == pg_sys::ForeignTableRelationId
            && ForeignTransaction::is_staged_relation(*object_id)
        {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot change the options of an Iceberg foreign table before its staged creation is published",
            ));
        }
        Ok(())
    }
}
