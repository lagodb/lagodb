//! Managed-table storage identity.

use lagodb_core::catalog::{current_database_name_bytes, get_namespace_name_bytes};
use lagodb_core::handles::RelationHandle;
use lagodb_core::object_cleanup::ObjectTreeTarget;
use lagodb_core::options::{CachedTablespaceOpts, TableOptions, get_tablespace};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};

use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::storage::StorageContext;
use crate::storage::object_uri::resolve_object_uri;

use super::local_storage::LocalTableRoot;
use crate::managed_table::options::LOCATION_OPTION_DEF;

// RFC 3986 unreserved bytes remain readable, except `.`. Encoding `.` makes
// even quoted PostgreSQL identifiers named `.` or `..` safe path segments.
const TABLE_NAME_ENCODE_SET: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'~');

/// Local roots follow PG storage generations; object roots retain their
/// catalog-owned identity across PostgreSQL relfilenumber changes.
pub(crate) enum ManagedTableLocation {
    Local(LocalTableRoot),
    Remote {
        location: String,
        cleanup_target: ObjectTreeTarget,
    },
}

impl ManagedTableLocation {
    pub(crate) fn for_create(
        rel: &RelationHandle<'_>,
        storage: &StorageContext,
    ) -> IcebergResult<Self> {
        match storage.object_tablespace() {
            Some(tablespace) => {
                let schema = get_namespace_name_bytes(rel.namespace_oid())
                    .ok_or(IcebergError::NamespaceNull)?;
                let encoded_schema = Self::encode_name(&schema);
                let encoded_table = Self::encode_name(rel.relation_name().as_bytes());
                let relation_oid = u32::from(rel.oid());
                let location = Self::remote_default(
                    tablespace,
                    &encoded_schema,
                    &encoded_table,
                    relation_oid,
                );
                let cleanup_target =
                    Self::build_remote_cleanup_target(tablespace, &location)?;
                Ok(Self::Remote {
                    location,
                    cleanup_target,
                })
            }
            None => Ok(Self::Local(LocalTableRoot::for_create(
                rel,
                storage.local_storage().expect("local storage selected"),
            )?)),
        }
    }

    /// Resolve the authoritative root for an existing relation.
    pub(crate) fn for_relation(rel: &RelationHandle<'_>) -> IcebergResult<Self> {
        let Some(tablespace) = get_tablespace(rel.tablespace().resolved_oid())?
        else {
            return Ok(Self::Local(LocalTableRoot::for_relation(rel)?));
        };
        let options = TableOptions::load_from_catalog(rel.oid())?
            .ok_or(IcebergError::ManagedTableLocationMissing { relid: rel.oid() })?;
        let value = options
            .get_value(&LOCATION_OPTION_DEF)
            .ok_or(IcebergError::ManagedTableLocationMissing { relid: rel.oid() })?;
        if value.is_empty() {
            return Err(IcebergError::InvalidManagedTableLocation {
                location: String::new(),
                reason: "persisted location must not be empty".to_owned(),
            });
        }

        let cleanup_target = Self::build_remote_cleanup_target(&tablespace, value)?;
        if value.rsplit_once('/').is_none_or(|(_, relation_oid)| {
            relation_oid.parse::<u32>() != Ok(u32::from(rel.oid()))
        }) {
            return Err(IcebergError::InvalidManagedTableLocation {
                location: value.to_owned(),
                reason: format!(
                    "object-backed location must end in relation OID {}",
                    u32::from(rel.oid()),
                ),
            });
        }
        Ok(Self::Remote {
            location: value.to_owned(),
            cleanup_target,
        })
    }

    pub(crate) fn persist_option(
        &self,
        options: &mut TableOptions,
    ) -> IcebergResult<()> {
        match self {
            Self::Local(root) => root.persist_identity(options),
            Self::Remote { location, .. } => {
                options.insert_access_method_value(
                    &LOCATION_OPTION_DEF,
                    location.as_str(),
                )?;
                Ok(())
            }
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Local(location) => location.as_str(),
            Self::Remote { location, .. } => location,
        }
    }

    pub(crate) fn into_string(self) -> String {
        match self {
            Self::Local(location) => location.into_string(),
            Self::Remote { location, .. } => location,
        }
    }

    pub(crate) fn remote_cleanup_target(&self) -> Option<&ObjectTreeTarget> {
        match self {
            Self::Local(_) => None,
            Self::Remote { cleanup_target, .. } => Some(cleanup_target),
        }
    }

    fn remote_default(
        tablespace: &CachedTablespaceOpts,
        schema: &str,
        table: &str,
        relation_oid: u32,
    ) -> String {
        format!(
            "{}/{}/{schema}/{table}/{relation_oid}",
            tablespace.effective_base_uri().trim_end_matches('/'),
            Self::encode_name(&current_database_name_bytes()),
        )
    }

    fn build_remote_cleanup_target(
        tablespace: &CachedTablespaceOpts,
        location: &str,
    ) -> IcebergResult<ObjectTreeTarget> {
        let object_key_offset =
            resolve_object_uri(tablespace.effective_base_uri(), location).map_err(
                |error| IcebergError::InvalidManagedTableLocation {
                    location: location.to_owned(),
                    reason: error.to_string(),
                },
            )?;
        ObjectTreeTarget::new(
            tablespace.volume_id(),
            tablespace.object_namespace(),
            &location[object_key_offset..],
        )
        .map_err(|error| IcebergError::InvalidManagedTableLocation {
            location: location.to_owned(),
            reason: error.to_string(),
        })
    }

    fn encode_name(name: &[u8]) -> String {
        percent_encode(name, TABLE_NAME_ENCODE_SET).to_string()
    }
}
