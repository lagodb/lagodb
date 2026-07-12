//! Maintenance queue domain types.

use pgrx::datum::Uuid as PgUuid;
use pgrx::pg_sys;

use super::target::{ObjectTarget, ObjectTreeTarget};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i16)]
pub enum MaintenanceOperation {
    DeleteObject = 1,
    DeleteTree = 2,
}

impl TryFrom<i16> for MaintenanceOperation {
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
pub struct MaintenanceItemId(pub(crate) PgUuid);

impl MaintenanceItemId {
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

impl std::fmt::Display for MaintenanceItemId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug)]
pub struct MaintenanceContext<'a> {
    pub producer: &'a str,
    pub source_relid: Option<pg_sys::Oid>,
    pub source_name: Option<&'a str>,
}

pub enum MaintenanceItemRef<'a> {
    DeleteObject {
        target: &'a ObjectTarget,
        context: MaintenanceContext<'a>,
    },
    DeleteTree {
        target: &'a ObjectTreeTarget,
        context: MaintenanceContext<'a>,
    },
}

impl MaintenanceItemRef<'_> {
    pub(crate) fn operation(&self) -> MaintenanceOperation {
        match self {
            Self::DeleteObject { .. } => MaintenanceOperation::DeleteObject,
            Self::DeleteTree { .. } => MaintenanceOperation::DeleteTree,
        }
    }

    pub(crate) fn fields(&self) -> (&str, &str, &str, &MaintenanceContext<'_>) {
        match self {
            Self::DeleteObject { target, context } => (
                target.store_id().as_str(),
                target.namespace(),
                target.path(),
                context,
            ),
            Self::DeleteTree { target, context } => (
                target.store_id().as_str(),
                target.namespace(),
                target.prefix(),
                context,
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MaintenanceItem {
    pub(crate) id: MaintenanceItemId,
    pub(crate) target: MaintenanceTarget,
    pub(crate) producer: String,
    pub(crate) attempt_count: i32,
}

#[derive(Clone, Debug)]
pub(crate) enum MaintenanceTarget {
    Object {
        store_id: String,
        namespace: String,
        path: String,
    },
    Tree {
        store_id: String,
        namespace: String,
        prefix: String,
    },
}

impl MaintenanceTarget {
    pub(crate) fn operation(&self) -> MaintenanceOperation {
        match self {
            Self::Object { .. } => MaintenanceOperation::DeleteObject,
            Self::Tree { .. } => MaintenanceOperation::DeleteTree,
        }
    }
}
