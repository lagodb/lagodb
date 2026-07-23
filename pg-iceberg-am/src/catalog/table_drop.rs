//! Transactional Iceberg DROP TABLE orchestration.

use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::maintenance::{
    MaintenanceContext, MaintenanceItemRef, MaintenanceQueue, ObjectTreeTarget,
};
use pg_lakebase_core::options::{TableOptions, get_tablespace};

use super::metadata_table::IcebergMetadata;
use super::metadata_tracker::TxMetadata;
use super::table_lifecycle::{compute_table_location, distributed_table_key};
use crate::error::IcebergResult;
use crate::storage::StorageContext;
use crate::storage::transaction_resources::register_local_table_root_dropped;

enum TableRootCleanup {
    Local {
        location: String,
        context: StorageContext,
    },
    Remote(ObjectTreeTarget),
}

impl TableRootCleanup {
    fn resolve(rel: &RelationHandle<'_>) -> IcebergResult<Self> {
        if let Some(opts) = get_tablespace(rel.tablespace_oid())? {
            let object_path = opts.rooted_object_key(&distributed_table_key(rel));
            return Ok(Self::Remote(ObjectTreeTarget::new(
                opts.store_id().as_str(),
                opts.object_namespace(),
                object_path,
            )?));
        }

        let context = StorageContext::for_tablespace_with_wal(
            rel.tablespace_oid(),
            rel.needs_wal(),
        )?;
        let location = compute_table_location(rel, context.base_path(), false);
        Ok(Self::Local { location, context })
    }
}

pub(crate) struct IcebergTableDrop<'a> {
    rel: &'a RelationHandle<'a>,
    cleanup: TableRootCleanup,
}

impl<'a> IcebergTableDrop<'a> {
    pub(crate) fn for_relation(rel: &'a RelationHandle<'a>) -> IcebergResult<Self> {
        Ok(Self {
            rel,
            cleanup: TableRootCleanup::resolve(rel)?,
        })
    }

    pub(crate) fn stage(self) -> IcebergResult<()> {
        TxMetadata::stage_drop(self.rel.oid())?;

        match self.cleanup {
            TableRootCleanup::Local { location, context } => {
                register_local_table_root_dropped(location, context.into_file_io())?;
            }
            TableRootCleanup::Remote(target) => {
                let source_name = self.rel.relation_name();
                let _ = MaintenanceQueue::enqueue(MaintenanceItemRef::DeleteTree {
                    target: &target,
                    context: MaintenanceContext {
                        producer: "iceberg-drop",
                        source_relid: Some(self.rel.oid()),
                        source_name: Some(&source_name),
                    },
                })?;
            }
        }

        IcebergMetadata::delete_if_exists(self.rel.oid())?;
        TableOptions::delete_from_catalog(self.rel.oid())?;
        Ok(())
    }
}
