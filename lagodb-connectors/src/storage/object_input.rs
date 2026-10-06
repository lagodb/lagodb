//! Object collection resolution for COPY FROM and foreign scans.

use lagodb_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::local_input::{LocalFiles, LocalInput};
use super::{InputFile, ObjectLocationKind, ResolvedStorageLocation};

const LIST_PAGE_SIZE: u32 = 1_024;

pub(crate) enum ObjectInput {
    Local(LocalInput),
    Exact {
        access: ObjectAccess,
        key: Box<str>,
        size: u64,
    },
    Prefix {
        access: ObjectPrefixAccess,
        keys: Box<[String]>,
        total_bytes: u64,
    },
}

impl ObjectInput {
    /// Resolve the declared location once and retain stable prefix membership
    /// for rescans.
    pub(crate) fn resolve(
        location: &ResolvedStorageLocation,
        kind: ObjectLocationKind,
        matches: impl Fn(&str) -> bool,
    ) -> Result<Self, ConnectorError> {
        if let Some(path) = location.local_path() {
            return Ok(Self::Local(LocalInput::resolve(path, kind, matches)?));
        }
        let manager = StorageManager::from_pg_gucs()?;
        match kind {
            ObjectLocationKind::Exact => {
                let exact = location.acquire_object_access(&manager)?;
                let size = exact.head()?.size;
                return Ok(Self::Exact {
                    access: exact,
                    key: location.object_key().into(),
                    size,
                });
            }
            ObjectLocationKind::Prefix => {}
        }

        let prefix = location.normalized_prefix();
        let access = location.acquire_prefix_access(&manager, &prefix)?;
        let mut keys = Vec::new();
        let mut total_bytes = 0_u64;
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
            for entry in entries {
                if matches(&entry.key) {
                    total_bytes = total_bytes.saturating_add(entry.size);
                    keys.push(entry.key);
                }
            }
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
            total_bytes,
        })
    }

    pub(crate) const fn total_bytes(&self) -> u64 {
        match self {
            Self::Local(input) => input.total_bytes(),
            Self::Exact { size, .. } => *size,
            Self::Prefix { total_bytes, .. } => *total_bytes,
        }
    }

    pub(crate) fn open(self) -> ObjectFiles {
        match self {
            Self::Local(input) => ObjectFiles::Local(input.open()),
            Self::Exact { access, key, .. } => ObjectFiles::Exact {
                access,
                key,
                emitted: false,
            },
            Self::Prefix { access, keys, .. } => ObjectFiles::Prefix {
                access,
                keys,
                index: 0,
            },
        }
    }
}

pub(crate) enum ObjectFiles {
    Local(LocalFiles),
    Exact {
        access: ObjectAccess,
        key: Box<str>,
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
            Self::Local(files) => files.reset(),
            Self::Exact { emitted, .. } => *emitted = false,
            Self::Prefix { index, .. } => *index = 0,
        }
    }
}

impl Iterator for ObjectFiles {
    type Item = Result<InputFile, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Local(files) => files.next(),
            Self::Exact {
                access,
                key,
                emitted,
            } if !*emitted => {
                *emitted = true;
                Some(
                    access
                        .open()
                        .map(|file| InputFile::object(file, key))
                        .map_err(ConnectorError::from),
                )
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
                    .map(|file| InputFile::object(file, key))
                    .map_err(ConnectorError::from)
            }),
        }
    }
}
