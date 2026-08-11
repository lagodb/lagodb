//! Exact-object and prefix resolution for format adapters.

use chrono::{Datelike, Utc};
use pg_lakebase_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;
use uuid::Uuid;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::StorageTarget;

const LIST_PAGE_SIZE: u32 = 1_024;

pub(crate) enum ObjectInput {
    Exact(ObjectAccess),
    Prefix {
        access: ObjectPrefixAccess,
        keys: Box<[String]>,
    },
}

impl ObjectInput {
    /// Resolve layout from the declared location syntax, then perform only the
    /// operation for that layout. A selected-format suffix denotes one exact
    /// object and is confirmed with HEAD; a location without that suffix is a
    /// prefix and is listed directly. A missing exact object is never silently
    /// reinterpreted as a dataset prefix.
    pub(crate) fn resolve(
        target: &StorageTarget,
        manager: &StorageManager,
        format: FormatKind,
    ) -> Result<Self, ConnectorError> {
        match ObjectLocationKind::classify(target.object_key(), format)? {
            ObjectLocationKind::Exact => {
                let exact = target.acquire_object_access(manager)?;
                exact.head()?;
                return Ok(Self::Exact(exact));
            }
            ObjectLocationKind::Prefix => {}
        }

        let prefix = target.normalized_prefix();
        let access = target.acquire_prefix_access(manager, &prefix)?;
        let mut keys = Vec::new();
        // A prefix denotes a raw external-file collection, not transactionally
        // published table membership, so every matching object is visible.
        // Materializing and sorting the complete key set before the first row is
        // intentional. Object stores do not provide a snapshot shared by independent
        // LIST operations, while FDW ReScan reuses this ObjectFiles and only resets
        // its index. Re-listing on every ReScan could therefore change membership;
        // retaining one list gives the scan a stable file set. Sorting canonicalizes
        // the backend's unspecified list order and deterministically selects the first
        // file from which the Parquet reader establishes its expected schema. This
        // deliberately accepts full-LIST first-row latency, key-set memory, and
        // O(n log n) sorting. Do not make this lazy without an operation-scoped
        // snapshot, manifest, or spool that preserves the same ReScan contract.
        //
        // The connection-bound session stays local because eager resolution completes
        // before the FDW publishes provider state. It owns both the cursor and its
        // client generation through this lifecycle window.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectLocationKind {
    Exact,
    Prefix,
}

impl ObjectLocationKind {
    pub(crate) fn classify(
        key: &str,
        format: FormatKind,
    ) -> Result<Self, ConnectorError> {
        match FormatKind::infer_from_key(key) {
            Some(found) if found != format => Err(ConnectorError::invalid_option(
                "path",
                "object suffix conflicts with the selected format",
            )),
            Some(_) if format.matches_object_key(key) && !key.ends_with('/') => {
                Ok(Self::Exact)
            }
            Some(_) => Err(ConnectorError::invalid_option(
                "path",
                "stream compression suffixes are not valid for Parquet or Avro objects",
            )),
            _ => Ok(Self::Prefix),
        }
    }
}

pub(crate) enum ObjectOutput {
    Exact(Option<ObjectAccess>),
    Prefix {
        access: ObjectPrefixAccess,
        keys: PartitionedKeyGenerator,
    },
}

/// One exact output capability together with its transaction disposition.
pub(crate) struct ObjectWriteTarget {
    object: ObjectAccess,
    delete_on_abort: bool,
}

impl ObjectWriteTarget {
    pub(crate) fn exact(object: ObjectAccess) -> Self {
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
    ) -> Result<Self, ConnectorError> {
        let key = target.object_key();
        match ObjectLocationKind::classify(key, format)? {
            ObjectLocationKind::Exact => {
                Ok(Self::Exact(Some(target.acquire_object_access(manager)?)))
            }
            ObjectLocationKind::Prefix => {
                let prefix = target.normalized_prefix();
                Ok(Self::Prefix {
                    access: target.acquire_prefix_access(manager, &prefix)?,
                    keys: PartitionedKeyGenerator::new(prefix, format),
                })
            }
        }
    }

    pub(crate) const fn kind(&self) -> ObjectLocationKind {
        match self {
            Self::Exact(_) => ObjectLocationKind::Exact,
            Self::Prefix { .. } => ObjectLocationKind::Prefix,
        }
    }

    pub(crate) fn next_object(
        &mut self,
    ) -> Result<ObjectWriteTarget, ConnectorError> {
        match self {
            Self::Exact(object) => {
                object.take().map(ObjectWriteTarget::exact).ok_or_else(|| {
                    ConnectorError::invalid_option(
                        "path",
                        "an exact output cannot create a second object",
                    )
                })
            }
            Self::Prefix { access, keys } => {
                Ok(ObjectWriteTarget::created(access.object(&keys.next_key())?))
            }
        }
    }
}

/// One operation-scoped, collision-resistant object-key sequence.
pub(crate) struct PartitionedKeyGenerator {
    directory: Box<str>,
    extension: &'static str,
    writer_id: Uuid,
    sequence: u32,
}

impl PartitionedKeyGenerator {
    pub(crate) fn new(prefix: String, format: FormatKind) -> Self {
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
            extension: format.as_str(),
            writer_id: Uuid::now_v7(),
            sequence: 0,
        }
    }

    pub(crate) fn next_key(&mut self) -> String {
        let sequence = self.sequence;
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("one statement cannot create more than u32::MAX objects");
        format!(
            "{}part-{}-{sequence:05}.{}",
            self.directory, self.writer_id, self.extension
        )
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod object_location_tests {
    use super::ObjectLocationKind;
    use crate::format::FormatKind;
    use pgrx::pg_test;

    #[pg_test]
    fn selected_format_suffix_is_exact() {
        assert_eq!(
            ObjectLocationKind::classify("data/file.parquet", FormatKind::Parquet)
                .unwrap(),
            ObjectLocationKind::Exact,
        );
        assert_eq!(
            ObjectLocationKind::classify("data/file.csv.gz", FormatKind::Csv)
                .unwrap(),
            ObjectLocationKind::Exact,
        );
    }

    #[pg_test]
    fn suffixless_and_directory_locations_are_prefixes() {
        for key in ["data/table", "data/table/", "data/table.parquet/"] {
            assert_eq!(
                ObjectLocationKind::classify(key, FormatKind::Parquet).unwrap(),
                ObjectLocationKind::Prefix,
            );
        }
    }

    #[pg_test]
    fn conflicting_or_stream_wrapped_container_suffixes_are_rejected() {
        assert!(
            ObjectLocationKind::classify("data/file.csv", FormatKind::Parquet)
                .is_err()
        );
        assert!(
            ObjectLocationKind::classify(
                "data/file.parquet.gz",
                FormatKind::Parquet,
            )
            .is_err()
        );
    }
}
