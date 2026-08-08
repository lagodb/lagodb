//! Object-cleanup queue domain types.

use pgrx::datum::Uuid as PgUuid;
use pgrx::pg_sys;

use super::target::{ObjectTarget, ObjectTreeTarget};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i16)]
pub enum ObjectCleanupOperation {
    DeleteObject = 1,
    DeleteTree = 2,
}

impl TryFrom<i16> for ObjectCleanupOperation {
    type Error = i16;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::DeleteObject),
            2 => Ok(Self::DeleteTree),
            other => Err(other),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ObjectCleanupItemId(pub(crate) PgUuid);

impl ObjectCleanupItemId {
    pub(crate) fn new() -> Self {
        Self(PgUuid::from_bytes(*uuid::Uuid::now_v7().as_bytes()))
    }

    pub fn from_pg_uuid(value: PgUuid) -> Self {
        Self(value)
    }

    pub fn as_pg_uuid(self) -> PgUuid {
        self.0
    }
}

impl std::fmt::Display for ObjectCleanupItemId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug)]
pub struct ObjectCleanupContext<'a> {
    pub producer: &'a str,
    pub source_relid: Option<pg_sys::Oid>,
    pub source_name: Option<&'a str>,
}

pub enum ObjectCleanupItemRef<'a> {
    DeleteObject {
        target: &'a ObjectTarget,
        context: ObjectCleanupContext<'a>,
    },
    DeleteTree {
        target: &'a ObjectTreeTarget,
        context: ObjectCleanupContext<'a>,
    },
}

impl ObjectCleanupItemRef<'_> {
    pub(crate) fn operation(&self) -> ObjectCleanupOperation {
        match self {
            Self::DeleteObject { .. } => ObjectCleanupOperation::DeleteObject,
            Self::DeleteTree { .. } => ObjectCleanupOperation::DeleteTree,
        }
    }

    pub(crate) fn fields(&self) -> (u64, &str, &str, &ObjectCleanupContext<'_>) {
        match self {
            Self::DeleteObject { target, context } => (
                target.volume_id().get(),
                target.namespace(),
                target.path(),
                context,
            ),
            Self::DeleteTree { target, context } => (
                target.volume_id().get(),
                target.namespace(),
                target.prefix(),
                context,
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectCleanupItem {
    pub(crate) id: ObjectCleanupItemId,
    pub(crate) target: ObjectCleanupTarget,
    pub(crate) attempt_count: i32,
    pub(crate) revision: i64,
}

impl ObjectCleanupItem {
    pub(crate) const fn volume_id(&self) -> u64 {
        match &self.target {
            ObjectCleanupTarget::Object { volume_id, .. }
            | ObjectCleanupTarget::Tree { volume_id, .. } => *volume_id,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ObjectCleanupTarget {
    Object {
        volume_id: u64,
        namespace: String,
        path: String,
    },
    Tree {
        volume_id: u64,
        namespace: String,
        prefix: String,
    },
}
