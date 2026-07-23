use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{FromDatum, IntoDatum};
use thiserror::Error;

use super::defs::{
    self, INTERNAL_STORAGE_VOLUME_ID_OPTION, PUBLIC_STORAGE_VOLUME_OPTION,
};
use crate::catalog::{CatalogRelation, search_syscache_copy};
use crate::diag::{PgError, SqlStateError};
use crate::options::schema::OptionSchemaError;
use crate::storage_volume::{StorageVolumeId, StorageVolumeIdError};

#[derive(Debug, Error)]
pub enum TablespaceError {
    #[error("invalid tablespace option: {0}")]
    InvalidSchema(#[from] OptionSchemaError),

    #[error("{name} is an internal Lakebase option")]
    UserSuppliedInternalOption { name: &'static str },

    #[error("{PUBLIC_STORAGE_VOLUME_OPTION} requires a non-empty value")]
    MissingVolumeName,

    #[error("invalid internal storage volume id")]
    InvalidVolumeId(#[from] StorageVolumeIdError),

    #[error("invalid internal storage volume id {value:?}")]
    InvalidVolumeIdText { value: String },

    #[error("duplicate internal Lakebase tablespace option {name}")]
    DuplicateInternalOption { name: &'static str },

    #[error("public storage volume name leaked into pg_tablespace.spcoptions")]
    PublicNameInCatalog,

    #[error("failed to update tablespace catalog: {0}")]
    UpdateFailed(#[source] PgError),

    #[error("tablespace OID {0} not found in pg_tablespace")]
    NotFound(pg_sys::Oid),
}

impl SqlStateError for TablespaceError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::UpdateFailed(error) => error.sql_error_code(),
            Self::NotFound(_) => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            Self::DuplicateInternalOption { .. }
            | Self::InvalidVolumeId(_)
            | Self::InvalidVolumeIdText { .. }
            | Self::PublicNameInCatalog => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
            _ => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
        }
    }
}

/// User-facing storage option removed from a `CREATE TABLESPACE` parse tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTablespaceStorageOptions {
    volume_name: String,
}

impl CreateTablespaceStorageOptions {
    pub fn extract_from_stmt(
        stmt: &mut pg_sys::CreateTableSpaceStmt,
    ) -> Result<Option<Self>, TablespaceError> {
        // SAFETY: the caller owns the mutable parse tree for this hook phase.
        let options = unsafe { defs::extract_and_remove_options(stmt)? };
        let mut volume_name = None;
        for (name, value) in options {
            match name.as_str() {
                INTERNAL_STORAGE_VOLUME_ID_OPTION => {
                    return Err(TablespaceError::UserSuppliedInternalOption {
                        name: INTERNAL_STORAGE_VOLUME_ID_OPTION,
                    });
                }
                PUBLIC_STORAGE_VOLUME_OPTION => {
                    let value = value
                        .filter(|value| !value.is_empty())
                        .ok_or(TablespaceError::MissingVolumeName)?;
                    volume_name = Some(value);
                }
                _ => unreachable!("option schema returned an unknown option"),
            }
        }
        Ok(volume_name.map(|volume_name| Self { volume_name }))
    }

    pub fn volume_name(&self) -> &str {
        &self.volume_name
    }
}

/// Validated internal binding persisted in `pg_tablespace.spcoptions`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TablespaceBinding {
    volume_id: StorageVolumeId,
}

impl TablespaceBinding {
    pub const fn new(volume_id: StorageVolumeId) -> Self {
        Self { volume_id }
    }

    pub const fn volume_id(&self) -> StorageVolumeId {
        self.volume_id
    }

    pub fn persist_to_catalog(
        &self,
        spcoid: pg_sys::Oid,
    ) -> Result<(), TablespaceError> {
        unsafe {
            let relation = CatalogRelation::open(
                pg_sys::TableSpaceRelationId,
                pg_sys::RowExclusiveLock as _,
            )
            .map_err(TablespaceError::UpdateFailed)?;
            let tuple = search_syscache_copy(
                pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
                spcoid.into_datum().expect("Oid has a Datum representation"),
                0.into(),
                0.into(),
                0.into(),
            )
            .map_err(TablespaceError::UpdateFailed)?
            .ok_or(TablespaceError::NotFound(spcoid))?;

            let existing = tuple
                .get_attr(pg_sys::Anum_pg_tablespace_spcoptions as i16)
                .map_err(TablespaceError::UpdateFailed)?;
            let mut options = existing
                .and_then(|datum| Vec::<String>::from_datum(datum, false))
                .unwrap_or_default();
            if options.iter().any(|option| {
                option.split_once('=').is_some_and(|(option_name, _)| {
                    option_name == INTERNAL_STORAGE_VOLUME_ID_OPTION
                })
            }) {
                return Err(TablespaceError::DuplicateInternalOption {
                    name: INTERNAL_STORAGE_VOLUME_ID_OPTION,
                });
            }
            options.push(format!(
                "{INTERNAL_STORAGE_VOLUME_ID_OPTION}={}",
                self.volume_id
            ));

            let new_options = options.into_datum();
            let tuple_desc =
                relation.as_raw().as_ref().expect("open relation").rd_att;
            let natts = usize::try_from((*tuple_desc).natts)
                .expect("tuple descriptor attribute count is non-negative");
            let mut values = vec![0.into(); natts];
            let mut nulls = vec![false; natts];
            let mut replacements = vec![false; natts];
            let index = usize::try_from(pg_sys::Anum_pg_tablespace_spcoptions - 1)
                .expect("positive catalog attribute number");
            values[index] = new_options.unwrap_or(0.into());
            nulls[index] = new_options.is_none();
            replacements[index] = true;

            let updated = pg_sys::heap_modify_tuple(
                tuple.as_raw(),
                tuple_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                replacements.as_mut_ptr(),
            );
            let updated = crate::handles::HeapTupleGuard::new(updated);
            relation
                .catalog_update(
                    crate::handles::HeapTupleRef::from_raw(tuple.as_raw()),
                    &updated,
                )
                .map_err(TablespaceError::UpdateFailed)?;
        }
        Ok(())
    }
}

pub(crate) fn parse_catalog_binding(
    options: &[String],
) -> Result<Option<TablespaceBinding>, TablespaceError> {
    let mut volume_id = None;
    for option in options {
        let Some((name, value)) = option.split_once('=') else {
            continue;
        };
        match name {
            PUBLIC_STORAGE_VOLUME_OPTION => {
                return Err(TablespaceError::PublicNameInCatalog);
            }
            INTERNAL_STORAGE_VOLUME_ID_OPTION => {
                if volume_id.is_some() {
                    return Err(TablespaceError::DuplicateInternalOption {
                        name: INTERNAL_STORAGE_VOLUME_ID_OPTION,
                    });
                }
                let raw = value.parse::<i64>().map_err(|_| {
                    TablespaceError::InvalidVolumeIdText {
                        value: value.to_owned(),
                    }
                })?;
                volume_id = Some(StorageVolumeId::try_from(raw)?);
            }
            _ => {}
        }
    }
    Ok(volume_id.map(TablespaceBinding::new))
}
