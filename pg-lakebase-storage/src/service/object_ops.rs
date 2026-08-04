//! Object metadata, staging publication, invalidation, and delete handlers.

use futures::StreamExt;
use tracing::{info, warn};

use super::StorageService;
use super::command::{
    DeleteCommand, DeleteObjectsCommand, DeletePrefixCommand, HeadCommand,
    InvalidateObjectCacheCommand, ProbeStoreCommand, UploadCommand,
};
use super::reply::{
    CommandOutput, DeleteObjectsOutput, DeletePrefixOutput, HeadOutput,
    InvalidateObjectCacheOutput, ServiceReply, UploadOutput,
};
use crate::backend::BackendDataIdentity;
use crate::cache::CacheIndex;
use crate::error::{StorageError, StorageResult};
use crate::object::ObjectLocation;
use crate::session::StorageContext;

impl<I: CacheIndex> StorageService<I> {
    pub(super) async fn handle_head(
        &self,
        context: &StorageContext<I>,
        command: HeadCommand,
    ) -> StorageResult<ServiceReply> {
        let attached = context.attached()?;
        let key = ObjectLocation::new(
            attached.identity().clone(),
            command.bucket,
            command.key,
        )?;
        if let Some(meta) = self.cache.index.get_meta(&key).await? {
            return Ok(ServiceReply::new(CommandOutput::Head(HeadOutput {
                size: meta.size(),
                etag: meta.etag().map(str::to_string),
            })));
        }

        let info = attached.backend().head(key.path()).await?;
        Ok(ServiceReply::new(CommandOutput::Head(HeadOutput {
            size: info.size,
            etag: info.etag,
        })))
    }

    pub(super) async fn handle_upload(
        &self,
        context: &StorageContext<I>,
        command: UploadCommand,
    ) -> StorageResult<ServiceReply> {
        let attached = context.attached()?;
        let key = ObjectLocation::new(
            attached.identity().clone(),
            command.bucket,
            command.key,
        )?;
        let backend = attached.backend();
        let info = self.staging_uploader.upload(&key, backend.as_ref()).await?;
        Ok(ServiceReply::new(CommandOutput::Upload(UploadOutput {
            size: info.size,
            etag: info.etag,
        })))
    }

    pub(super) async fn handle_probe_store(
        &self,
        context: &StorageContext<I>,
        command: ProbeStoreCommand,
    ) -> StorageResult<ServiceReply> {
        let backend = context.attached()?.backend();
        let result = backend.probe(&command.bucket, &command.root_prefix).await?;
        Ok(ServiceReply::new(CommandOutput::ProbeStore(result)))
    }

    pub(super) async fn handle_invalidate_object_cache(
        &self,
        context: &StorageContext<I>,
        command: InvalidateObjectCacheCommand,
    ) -> StorageResult<ServiceReply> {
        let key = ObjectLocation::new(
            context.attached()?.identity().clone(),
            command.bucket,
            command.key,
        )?;
        self.cache.validate_file_cache_paths(&key)?;
        let report = self.cache.invalidate_object_cache(&key).await?;
        info!(
            key = %key,
            removed = report.removed,
            bytes_removed = report.bytes_removed,
            "object cache invalidated",
        );
        Ok(ServiceReply::new(CommandOutput::InvalidateObjectCache(
            InvalidateObjectCacheOutput {
                removed: report.removed,
            },
        )))
    }

    pub(super) async fn handle_delete(
        &self,
        context: &StorageContext<I>,
        command: DeleteCommand,
    ) -> StorageResult<ServiceReply> {
        let attached = context.attached()?;
        let key = ObjectLocation::new(
            attached.identity().clone(),
            command.bucket,
            command.key,
        )?;
        attached.backend().delete(key.path()).await?;
        let outcome = self.cache.invalidate_object_cache_best_effort(&key).await;
        info!(key = %key, ?outcome, "delete completed");
        Ok(ServiceReply::new(CommandOutput::Delete))
    }

    pub(super) async fn handle_delete_prefix(
        &self,
        context: &StorageContext<I>,
        command: DeletePrefixCommand,
    ) -> StorageResult<ServiceReply> {
        if command.prefix.is_empty() {
            return Err(StorageError::invalid_path(
                "delete_prefix requires a non-empty prefix; pass an explicit prefix to scope the deletion",
            ));
        }
        let DeletePrefixCommand { bucket, prefix } = command;
        let attached = context.attached()?;
        let identity = attached.identity().clone();
        let backend = attached.backend();
        let key_stream = backend
            .list(&bucket, Some(&prefix))
            .map(|item| item.map(|entry| entry.key))
            .boxed();
        let mut deleted_stream = backend.delete_stream(&bucket, key_stream);

        let mut deleted = 0_u64;
        while let Some(result) = deleted_stream.next().await {
            self.try_invalidate_local_cache_for_key(&identity, &bucket, result?)
                .await;
            deleted = deleted.saturating_add(1);
        }
        info!(
            backend_identity = %identity,
            bucket = bucket.as_str(),
            prefix = prefix.as_str(),
            deleted,
            "delete_prefix completed",
        );
        Ok(ServiceReply::new(CommandOutput::DeletePrefix(
            DeletePrefixOutput { deleted },
        )))
    }

    pub(super) async fn handle_delete_objects(
        &self,
        context: &StorageContext<I>,
        command: DeleteObjectsCommand,
    ) -> StorageResult<ServiceReply> {
        const MAX_KEYS: usize = crate::protocol::MAX_BULK_DELETE_OBJECT_KEYS;
        if command.keys.len() > MAX_KEYS {
            return Err(StorageError::resource_exhausted(format!(
                "delete_objects accepts at most {MAX_KEYS} keys"
            )));
        }
        if command.keys.is_empty() {
            return Ok(ServiceReply::new(CommandOutput::DeleteObjects(
                DeleteObjectsOutput { deleted: 0 },
            )));
        }

        let DeleteObjectsCommand { bucket, keys } = command;
        let attached = context.attached()?;
        let identity = attached.identity().clone();
        let backend = attached.backend();
        let key_stream =
            futures::stream::iter(keys.into_iter().map(Ok::<_, StorageError>))
                .boxed();
        let mut deleted_stream = backend.delete_stream(&bucket, key_stream);
        let mut deleted = 0_u32;
        while let Some(result) = deleted_stream.next().await {
            self.try_invalidate_local_cache_for_key(&identity, &bucket, result?)
                .await;
            deleted = deleted.saturating_add(1);
        }
        Ok(ServiceReply::new(CommandOutput::DeleteObjects(
            DeleteObjectsOutput { deleted },
        )))
    }

    /// Best-effort cleanup of derived cache state after backend deletion.
    async fn try_invalidate_local_cache_for_key(
        &self,
        identity: &BackendDataIdentity,
        bucket: &str,
        key: String,
    ) {
        match ObjectLocation::new(identity.clone(), bucket, key) {
            Ok(location) => {
                let _ = self
                    .cache
                    .invalidate_object_cache_best_effort(&location)
                    .await;
            }
            Err(error) => {
                warn!(
                    backend_identity = %identity,
                    bucket = %bucket,
                    %error,
                    "skipping cache invalidation for an unrepresentable deleted object path",
                );
            }
        }
    }
}
