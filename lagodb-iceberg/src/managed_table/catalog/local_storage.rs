//! Local storage generations follow PostgreSQL's relation-file identity.

use iceberg_lite::io::FileIO;
use lagodb_core::handles::{RelFileLocator, RelationHandle};

use crate::error::IcebergResult;
use crate::storage::LocalTableRetirement;

pub(crate) const LOCAL_TABLE_SUFFIX: &str = "_iceberg";

pub(crate) struct LocalTableRoot {
    directory: String,
}

impl LocalTableRoot {
    pub(crate) fn for_relation(rel: &RelationHandle<'_>) -> Self {
        Self::for_locator(rel, rel.locator())
    }

    pub(crate) fn for_locator(
        rel: &RelationHandle<'_>,
        locator: RelFileLocator,
    ) -> Self {
        let path = locator.path(rel.storage_backend());
        Self {
            directory: format!("{path}{LOCAL_TABLE_SUFFIX}"),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.directory
    }

    /// Whether a generated file path belongs to this storage generation.
    /// Managed metadata cannot override its directory with write.metadata.path;
    /// compare the PG-generated root without filesystem access or normalization.
    pub(crate) fn contains_path(&self, path: &str) -> bool {
        path.strip_prefix(self.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
    }

    pub(crate) fn into_string(self) -> String {
        self.directory
    }

    /// Retire this generation after commit and transaction lock release.
    ///
    /// Known limitation: a crash before cleanup or a failed deletion can leave
    /// the retired directory behind. Table VACUUM scans only the current
    /// metadata location, so it cannot reclaim retired generations. There is no
    /// durable retirement record for retry; these leftovers require external
    /// cleanup.
    pub(crate) fn retire(self, file_io: FileIO) -> IcebergResult<()> {
        LocalTableRetirement::register(self.directory, file_io)
    }
}
