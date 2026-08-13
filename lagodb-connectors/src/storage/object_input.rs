//! Object collection resolution for COPY FROM and foreign scans.

use pg_lakebase_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::{ObjectLocationKind, ResolvedStorageLocation};

const LIST_PAGE_SIZE: u32 = 1_024;

pub(crate) enum ObjectInput {
    Exact(ObjectAccess),
    Prefix {
        access: ObjectPrefixAccess,
        keys: Box<[String]>,
    },
}

impl ObjectInput {
    /// Resolve the declared location once and retain stable prefix membership
    /// for rescans.
    pub(crate) fn resolve(
        location: &ResolvedStorageLocation,
        manager: &StorageManager,
        format: FormatKind,
    ) -> Result<Self, ConnectorError> {
        match ObjectLocationKind::classify(location.object_key(), format)? {
            ObjectLocationKind::Exact => {
                let exact = location.acquire_object_access(manager)?;
                exact.head()?;
                return Ok(Self::Exact(exact));
            }
            ObjectLocationKind::Prefix => {}
        }

        let prefix = location.normalized_prefix();
        let access = location.acquire_prefix_access(manager, &prefix)?;
        let mut keys = Vec::new();
        // Prefix scans intentionally materialize and sort one complete LIST.
        // Object stores do not provide a snapshot across independent LISTs;
        // retaining this set gives FDW ReScan stable membership and a stable
        // first object for format-specific schema inference.
        let mut listing = access.listing(LIST_PAGE_SIZE)?;
        loop {
            pg_sys::check_for_interrupts!();
            let Some(entries) = listing.next_page()? else {
                break;
            };
            keys.extend(entries.into_iter().filter_map(|entry| {
                format.matches_object_key(&entry.key).then_some(entry.key)
            }));
            if listing.is_exhausted() {
                break;
            }
        }
        drop(listing);
        pg_sys::check_for_interrupts!();
        keys.sort_unstable();
        Ok(Self::Prefix {
            access,
            keys: keys.into_boxed_slice(),
        })
    }

    pub(crate) fn open(self) -> ObjectFiles {
        match self {
            Self::Exact(access) => ObjectFiles::Exact {
                access,
                emitted: false,
            },
            Self::Prefix { access, keys } => ObjectFiles::Prefix {
                access,
                keys,
                index: 0,
            },
        }
    }
}

pub(crate) enum ObjectFiles {
    Exact {
        access: ObjectAccess,
        emitted: bool,
    },
    Prefix {
        access: ObjectPrefixAccess,
        keys: Box<[String]>,
        index: usize,
    },
}

impl ObjectFiles {
    pub(crate) fn reset(&mut self) {
        match self {
            Self::Exact { emitted, .. } => *emitted = false,
            Self::Prefix { index, .. } => *index = 0,
        }
    }
}

impl Iterator for ObjectFiles {
    type Item = Result<StorageFile, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Exact { access, emitted } if !*emitted => {
                *emitted = true;
                Some(access.open().map_err(ConnectorError::from))
            }
            Self::Exact { .. } => None,
            Self::Prefix {
                access,
                keys,
                index,
            } => keys.get(*index).map(|key| {
                *index += 1;
                access
                    .object(key)
                    .and_then(|object| object.open())
                    .map_err(ConnectorError::from)
            }),
        }
    }
}
