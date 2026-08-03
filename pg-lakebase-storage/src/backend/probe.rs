//! Explicit object-store connectivity probe.
//!
//! The probe runs against an attached backend, so it exercises the same
//! credential-bearing backend instance as normal storage operations without involving the
//! local object cache or staging directory.

use futures::StreamExt;
use uuid::Uuid;

use super::ObjectBackend;
use crate::error::{StorageError, StorageErrorKind};
use crate::object::ObjectPath;

const PROBE_PAYLOAD: &[u8] = b"pg-lakebase-storage-probe-v1";

/// Structured outcome of an explicit storage-backend probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageProbeResult {
    list_succeeded: bool,
    write_succeeded: bool,
    read_succeeded: bool,
    delete_succeeded: bool,
    error: Option<String>,
}

impl StorageProbeResult {
    fn new() -> Self {
        Self {
            list_succeeded: false,
            write_succeeded: false,
            read_succeeded: false,
            delete_succeeded: false,
            error: None,
        }
    }

    pub(crate) fn from_wire(
        list_succeeded: bool,
        write_succeeded: bool,
        read_succeeded: bool,
        delete_succeeded: bool,
        error: Option<String>,
    ) -> Self {
        Self {
            list_succeeded,
            write_succeeded,
            read_succeeded,
            delete_succeeded,
            error,
        }
    }

    pub fn list_succeeded(&self) -> bool {
        self.list_succeeded
    }

    pub fn write_succeeded(&self) -> bool {
        self.write_succeeded
    }

    pub fn read_succeeded(&self) -> bool {
        self.read_succeeded
    }

    pub fn delete_succeeded(&self) -> bool {
        self.delete_succeeded
    }

    pub fn succeeded(&self) -> bool {
        self.list_succeeded
            && self.write_succeeded
            && self.read_succeeded
            && self.delete_succeeded
            && self.error.is_none()
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    fn fail(&mut self, stage: &str, error: StorageError) {
        self.error = Some(format!("{stage}: {}", error.wire_message()));
    }
}

pub(super) struct BackendProbe<'a, B: ObjectBackend + ?Sized> {
    backend: &'a B,
    root_prefix: String,
    location: ObjectPath,
    result: StorageProbeResult,
}

impl<'a, B: ObjectBackend + ?Sized> BackendProbe<'a, B> {
    pub(super) fn new(
        backend: &'a B,
        bucket: &str,
        root_prefix: &str,
    ) -> Result<Self, StorageError> {
        if root_prefix.is_empty() {
            return Err(StorageError::invalid_path(
                "storage probe requires a non-empty root prefix",
            ));
        }
        let key = format!("{root_prefix}/.lakebase-probe/{}", Uuid::new_v4());
        Ok(Self {
            backend,
            root_prefix: root_prefix.to_owned(),
            location: ObjectPath::new(bucket, key)?,
            result: StorageProbeResult::new(),
        })
    }

    pub(super) async fn run(mut self) -> StorageProbeResult {
        let list_prefix = format!("{}/", self.root_prefix);
        let mut objects = self
            .backend
            .list(self.location.bucket(), Some(&list_prefix));
        if let Some(Err(error)) = objects.next().await {
            self.result.fail("list", error);
            return self.result;
        }
        drop(objects);
        self.result.list_succeeded = true;

        if let Err(error) = self
            .backend
            .put_if_absent(&self.location, bytes::Bytes::from_static(PROBE_PAYLOAD))
            .await
        {
            // A transport/backend error can be an indeterminate write: the provider may have
            // committed the object before the response was lost. Best-effort delete it. `Busy`
            // denotes a create-only collision and must not delete the pre-existing object.
            let cleanup_indeterminate_write = matches!(
                error.kind(),
                StorageErrorKind::Backend | StorageErrorKind::Io
            );
            self.result.fail("write", error);
            if cleanup_indeterminate_write {
                self.cleanup_after_failure().await;
            }
            return self.result;
        }
        self.result.write_succeeded = true;

        if let Err(error) = self.verify_read().await {
            self.result.fail("read", error);
            self.cleanup_after_failure().await;
            return self.result;
        }
        self.result.read_succeeded = true;

        match self.backend.delete(&self.location).await {
            Ok(()) => self.result.delete_succeeded = true,
            Err(error) => self.result.fail("delete", error),
        }
        self.result
    }

    async fn verify_read(&self) -> Result<(), StorageError> {
        let info = self.backend.head(&self.location).await?;
        if info.size != PROBE_PAYLOAD.len() as u64 {
            return Err(StorageError::backend(format!(
                "probe object reported size {}, expected {}",
                info.size,
                PROBE_PAYLOAD.len()
            )));
        }
        let data = self
            .backend
            .get_range(&self.location, 0..PROBE_PAYLOAD.len() as u64)
            .await?;
        if data.as_ref() != PROBE_PAYLOAD {
            return Err(StorageError::backend(
                "probe object read-back payload did not match",
            ));
        }
        Ok(())
    }

    async fn cleanup_after_failure(&mut self) {
        match self.backend.delete(&self.location).await {
            Ok(()) => self.result.delete_succeeded = true,
            Err(cleanup_error) => {
                let primary = self.result.error.take().expect(
                    "cleanup is only attempted after a recorded probe failure",
                );
                self.result.error = Some(format!(
                    "{primary}; cleanup: {}",
                    cleanup_error.wire_message()
                ));
            }
        }
    }
}
