use pgrx::pg_sys;
use serde::{Deserialize, Serialize};

use super::error::StorageVolumeError;

/// A Unix-epoch timestamp with millisecond precision.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub(crate) struct UnixMillis(i64);

impl UnixMillis {
    const POSTGRES_TO_UNIX_EPOCH_MS: i64 = 10_957 * 86_400_000;

    pub(crate) fn now() -> Result<Self, StorageVolumeError> {
        let postgres_timestamp_us = unsafe { pg_sys::GetCurrentTimestamp() };
        let postgres_timestamp_ms = postgres_timestamp_us.div_euclid(1_000);
        postgres_timestamp_ms
            .checked_add(Self::POSTGRES_TO_UNIX_EPOCH_MS)
            .map(Self)
            .ok_or(StorageVolumeError::TimestampOverflow)
    }

    pub(crate) const fn get(self) -> i64 {
        self.0
    }

    pub(crate) const fn is_positive(self) -> bool {
        self.0 > 0
    }

    pub(crate) fn ttl_millis(seconds: i64) -> Result<i64, StorageVolumeError> {
        if seconds <= 0 {
            return Err(StorageVolumeError::InvalidTtl);
        }
        let seconds =
            u64::try_from(seconds).map_err(|_| StorageVolumeError::InvalidTtl)?;
        let millis = seconds
            .checked_mul(1_000)
            .ok_or(StorageVolumeError::InvalidTtl)?;
        i64::try_from(millis).map_err(|_| StorageVolumeError::InvalidTtl)
    }

    pub(crate) fn checked_add_millis(
        self,
        millis: u64,
    ) -> Result<Self, StorageVolumeError> {
        let millis = i64::try_from(millis)
            .map_err(|_| StorageVolumeError::TimestampOverflow)?;
        self.0
            .checked_add(millis)
            .map(Self)
            .ok_or(StorageVolumeError::TimestampOverflow)
    }

    pub(crate) fn checked_add_seconds(
        self,
        seconds: i64,
    ) -> Result<Self, StorageVolumeError> {
        let millis = Self::ttl_millis(seconds)?;
        self.0
            .checked_add(millis)
            .map(Self)
            .ok_or(StorageVolumeError::TimestampOverflow)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StorageVolumeLifecycle {
    Unbound {
        expires_at_ms: Option<UnixMillis>,
    },
    Bound {
        tablespace_oid: u32,
    },
    Retiring {
        former_tablespace_oid: u32,
        marked_at_ms: UnixMillis,
        purge_after_ms: UnixMillis,
    },
}

impl StorageVolumeLifecycle {
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Unbound { .. } => "unbound",
            Self::Bound { .. } => "bound",
            Self::Retiring { .. } => "retiring",
        }
    }

    pub(crate) const fn is_unbound(&self) -> bool {
        matches!(self, Self::Unbound { .. })
    }

    pub(crate) const fn is_retiring(&self) -> bool {
        matches!(self, Self::Retiring { .. })
    }

    pub(crate) fn is_expired_at(&self, now: UnixMillis) -> bool {
        self.expires_at_ms()
            .is_some_and(|expires_at| expires_at <= now)
    }

    pub(crate) const fn expires_at_ms(&self) -> Option<UnixMillis> {
        match self {
            Self::Unbound { expires_at_ms } => *expires_at_ms,
            Self::Bound { .. } | Self::Retiring { .. } => None,
        }
    }

    pub(crate) const fn bound_tablespace_oid(&self) -> Option<u32> {
        match self {
            Self::Bound { tablespace_oid } => Some(*tablespace_oid),
            Self::Unbound { .. } | Self::Retiring { .. } => None,
        }
    }

    pub(crate) const fn retired_tablespace_oid(&self) -> Option<u32> {
        match self {
            Self::Retiring {
                former_tablespace_oid,
                ..
            } => Some(*former_tablespace_oid),
            Self::Unbound { .. } | Self::Bound { .. } => None,
        }
    }

    pub(crate) const fn marked_at_ms(&self) -> Option<UnixMillis> {
        match self {
            Self::Retiring { marked_at_ms, .. } => Some(*marked_at_ms),
            Self::Unbound { .. } | Self::Bound { .. } => None,
        }
    }

    pub(crate) const fn purge_after_ms(&self) -> Option<UnixMillis> {
        match self {
            Self::Retiring { purge_after_ms, .. } => Some(*purge_after_ms),
            Self::Unbound { .. } | Self::Bound { .. } => None,
        }
    }

    pub(crate) fn bind(
        &mut self,
        tablespace_oid: u32,
        now: UnixMillis,
    ) -> Result<bool, StorageVolumeError> {
        if tablespace_oid == pg_sys::InvalidOid.to_u32() {
            return Err(StorageVolumeError::InvalidTablespaceOid);
        }
        let expired = self.is_expired_at(now);
        match self {
            Self::Unbound { .. } if expired => Err(StorageVolumeError::Expired),
            Self::Unbound { .. } => {
                *self = Self::Bound { tablespace_oid };
                Ok(true)
            }
            Self::Bound {
                tablespace_oid: existing,
            } if *existing == tablespace_oid => Ok(false),
            Self::Bound {
                tablespace_oid: existing,
            } => Err(StorageVolumeError::AlreadyBound(*existing)),
            Self::Retiring { .. } => {
                Err(StorageVolumeError::LifecycleOperation { operation: "bound" })
            }
        }
    }

    pub(crate) fn retire(
        &mut self,
        tablespace_oid: u32,
        created_at_ms: UnixMillis,
        marked_at_ms: UnixMillis,
        retirement_grace_ms: u64,
    ) -> Result<bool, StorageVolumeError> {
        if tablespace_oid == pg_sys::InvalidOid.to_u32() {
            return Err(StorageVolumeError::InvalidTablespaceOid);
        }
        match self {
            Self::Bound {
                tablespace_oid: existing,
            } if *existing == tablespace_oid => {
                let marked_at_ms = marked_at_ms.max(created_at_ms);
                let purge_after_ms =
                    marked_at_ms.checked_add_millis(retirement_grace_ms)?;
                *self = Self::Retiring {
                    former_tablespace_oid: tablespace_oid,
                    marked_at_ms,
                    purge_after_ms,
                };
                Ok(true)
            }
            Self::Bound {
                tablespace_oid: existing,
            } => Err(StorageVolumeError::AlreadyBound(*existing)),
            Self::Unbound { .. } => Err(StorageVolumeError::NotBound),
            Self::Retiring {
                former_tablespace_oid,
                ..
            } if *former_tablespace_oid == tablespace_oid => Ok(false),
            Self::Retiring { .. } => Err(StorageVolumeError::LifecycleOperation {
                operation: "retired again",
            }),
        }
    }
}
