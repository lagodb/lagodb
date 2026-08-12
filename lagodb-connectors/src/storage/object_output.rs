//! Object allocation and key generation for one write statement.

use std::num::NonZeroU64;

use chrono::{Datelike, Utc};
use pg_lakebase_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use uuid::Uuid;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::{ObjectLocationKind, StorageTarget};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObjectFileSuffix(&'static str);

impl ObjectFileSuffix {
    pub(crate) const fn new(value: &'static str) -> Self {
        Self(value)
    }

    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}

pub(crate) enum ObjectOutput {
    Exact {
        object: Option<ObjectAccess>,
    },
    Prefix {
        access: ObjectPrefixAccess,
        keys: PartitionedKeyGenerator,
        target_file_bytes: NonZeroU64,
    },
}

/// One allocated output object together with its transaction disposition.
pub(crate) struct AllocatedObject {
    object: ObjectAccess,
    delete_on_abort: bool,
}

impl AllocatedObject {
    fn exact(object: ObjectAccess) -> Self {
        Self {
            object,
            delete_on_abort: false,
        }
    }

    fn created(object: ObjectAccess) -> Self {
        Self {
            object,
            delete_on_abort: true,
        }
    }

    pub(super) fn into_parts(self) -> (ObjectAccess, bool) {
        (self.object, self.delete_on_abort)
    }
}

impl ObjectOutput {
    pub(crate) fn resolve(
        target: &StorageTarget,
        manager: &StorageManager,
        format: FormatKind,
        prefix_target_file_bytes: impl FnOnce() -> NonZeroU64,
    ) -> Result<Self, ConnectorError> {
        match ObjectLocationKind::classify(target.object_key(), format)? {
            ObjectLocationKind::Exact => Ok(Self::Exact {
                object: Some(target.acquire_object_access(manager)?),
            }),
            ObjectLocationKind::Prefix => {
                let prefix = target.normalized_prefix();
                Ok(Self::Prefix {
                    access: target.acquire_prefix_access(manager, &prefix)?,
                    keys: PartitionedKeyGenerator::new(prefix),
                    target_file_bytes: prefix_target_file_bytes(),
                })
            }
        }
    }

    pub(crate) const fn should_roll(&self, estimated_file_bytes: u64) -> bool {
        match self {
            Self::Exact { .. } => false,
            Self::Prefix {
                target_file_bytes,
                ..
            } => estimated_file_bytes >= target_file_bytes.get(),
        }
    }

    pub(crate) fn allocate_next(
        &mut self,
        suffix: ObjectFileSuffix,
    ) -> Result<AllocatedObject, ConnectorError> {
        match self {
            Self::Exact { object } => Ok(AllocatedObject::exact(
                object
                    .take()
                    .expect("an exact output is allocated only once"),
            )),
            Self::Prefix { access, keys, .. } => Ok(AllocatedObject::created(
                access.object(&keys.next_key(suffix))?,
            )),
        }
    }
}

/// One operation-scoped, collision-resistant object-key sequence.
pub(crate) struct PartitionedKeyGenerator {
    directory: Box<str>,
    writer_id: Uuid,
    sequence: u32,
}

impl PartitionedKeyGenerator {
    fn new(prefix: String) -> Self {
        let today = Utc::now().date_naive();
        let directory = format!(
            "{}{}/{:02}/{:02}/",
            prefix,
            today.year(),
            today.month(),
            today.day()
        );
        Self {
            directory: directory.into(),
            writer_id: Uuid::now_v7(),
            sequence: 0,
        }
    }

    fn next_key(&mut self, suffix: ObjectFileSuffix) -> String {
        let sequence = self.sequence;
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("one statement cannot create more than u32::MAX objects");
        format!(
            "{}part-{}-{sequence:05}.{}",
            self.directory,
            self.writer_id,
            suffix.as_str()
        )
    }
}
