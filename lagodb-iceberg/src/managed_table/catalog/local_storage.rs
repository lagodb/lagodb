//! Local storage generations follow PostgreSQL's relation-file identity.

use iceberg_lite::io::FileIO;
use lagodb_core::handles::{RelFileLocator, RelationHandle};
use lagodb_core::options::TableOptions;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::options::RELFILENUMBER_OPTION_DEF;
use crate::storage::{LocalStorage, LocalTableReservation, LocalTableRetirement};

pub(crate) const LOCAL_TABLE_SUFFIX: &str = "_iceberg";

pub(crate) struct LocalTableRoot {
    locator: RelFileLocator,
    directory: String,
    reservation: Option<LocalTableReservation>,
}

impl LocalTableRoot {
    pub(crate) fn for_create(
        rel: &RelationHandle<'_>,
        storage: &LocalStorage,
    ) -> IcebergResult<Self> {
        if rel.relkind() == pg_sys::RELKIND_PARTITIONED_TABLE as i8 {
            let locator = rel.allocate_storage_locator()?;
            let root = Self::for_locator(rel, locator);
            storage.reserve_file(
                root.reservation
                    .as_ref()
                    .expect("partitioned table reservation")
                    .as_str(),
            )?;
            Ok(root)
        } else {
            Ok(Self::for_locator(rel, rel.locator()))
        }
    }

    pub(crate) fn for_relation(rel: &RelationHandle<'_>) -> IcebergResult<Self> {
        if rel.relkind() != pg_sys::RELKIND_PARTITIONED_TABLE as i8 {
            return Ok(Self::for_locator(rel, rel.locator()));
        }
        let options = TableOptions::load_from_catalog(rel.oid())?;
        let number = options
            .as_ref()
            .and_then(|options| options.get_value(&RELFILENUMBER_OPTION_DEF))
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or(IcebergError::InvalidLocalStorageIdentity { relid: rel.oid() })?;
        // Partitioned relations have no native locator. Only their file number
        // is AM-owned; database, tablespace and temporary backend remain PG-owned.
        let locator = RelFileLocator {
            spc_oid: rel.tablespace().resolved_oid(),
            db_oid: unsafe { pg_sys::MyDatabaseId },
            rel_number: number.into(),
        };
        Ok(Self::for_locator(rel, locator))
    }

    pub(crate) fn for_locator(
        rel: &RelationHandle<'_>,
        locator: RelFileLocator,
    ) -> Self {
        let backend = rel.storage_backend();
        let path = locator.path(backend);
        let directory = format!("{path}{LOCAL_TABLE_SUFFIX}");
        let reservation = (rel.relkind() == pg_sys::RELKIND_PARTITIONED_TABLE as i8)
            .then(|| LocalTableReservation::new(locator, backend, path));
        Self {
            locator,
            directory,
            reservation,
        }
    }

    pub(crate) fn persist_identity(
        &self,
        options: &mut TableOptions,
    ) -> IcebergResult<()> {
        if self.reservation.is_some() {
            options.set_access_method_value(
                &RELFILENUMBER_OPTION_DEF,
                u32::from(self.locator.rel_number).to_string(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.directory
    }

    pub(crate) fn into_string(self) -> String {
        self.directory
    }

    /// Retire this generation after commit and transaction lock release.
    ///
    /// Known limitation: a crash before cleanup or a failed deletion can leave
    /// the retired directory or reservation file behind. Table VACUUM scans
    /// only the current metadata location, so it cannot reclaim these retired
    /// generations or their adjacent reservation files. There is no durable
    /// retirement record for retry; these leftovers require external cleanup.
    pub(crate) fn retire(self, file_io: FileIO) -> IcebergResult<()> {
        LocalTableRetirement::register(self.directory, self.reservation, file_io)
    }
}
