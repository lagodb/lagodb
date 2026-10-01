//! Transactional Iceberg DROP TABLE orchestration.

use lagodb_core::handles::RelationHandle;
use lagodb_core::object_cleanup::{
    ObjectCleanupContext, ObjectCleanupItemRef, ObjectCleanupQueue, ObjectTreeTarget,
};
use lagodb_core::options::TableOptions;

use super::local_storage::LocalTableRoot;
use super::metadata_table::IcebergMetadata;
use super::metadata_tracker::TxMetadata;
use super::table_location::ManagedTableLocation;
use crate::error::IcebergResult;
use crate::managed_table::storage::StorageContext;

enum TableRootCleanup {
    Local {
        root: LocalTableRoot,
        context: StorageContext,
    },
    Remote(ObjectTreeTarget),
}

impl TableRootCleanup {
    fn resolve(rel: &RelationHandle<'_>) -> IcebergResult<Self> {
        let location = ManagedTableLocation::for_relation(rel)?;
        match location {
            ManagedTableLocation::Remote { cleanup_target, .. } => {
                Ok(Self::Remote(cleanup_target))
            }
            ManagedTableLocation::Local(root) => Ok(Self::Local {
                root,
                context: StorageContext::for_write(rel)?,
            }),
        }
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
            TableRootCleanup::Local { root, context } => {
                root.retire(context.into_file_io())?;
            }
            TableRootCleanup::Remote(target) => {
                // TODO(drop-database): this queue is database-local, so DROP
                // DATABASE needs a cluster-visible handoff that survives
                // removal of the source database before remote cleanup runs.
                let relation_name = self.rel.relation_name();
                let source_name = relation_name.as_c_str().to_string_lossy();
                let _ =
                    ObjectCleanupQueue::enqueue(ObjectCleanupItemRef::DeleteTree {
                        target: &target,
                        context: ObjectCleanupContext {
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
