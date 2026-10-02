//! Query-offload worker FileIO reopen plan around the shared task inventory.

use bincode::Options;
use iceberg_lite::io::FileIO;
use lagodb_core::handles::RelationGuard;
use pgrx::pg_sys;
use serde::{Deserialize, Serialize};

use crate::error::IcebergError;
use crate::foreign_table::{
    ForeignTableIdentity, ForeignTableMode, IcebergFdwError, PlanSourceIdentity,
    RestForeignTable,
};
use crate::managed_table::StorageContext;
use crate::scan::ScanError;

use super::error::Error;

const MAGIC: &[u8; 8] = b"LAGOQWK\0";
const VERSION: u32 = 1;
const HEADER_BYTES: usize = 24;

#[derive(Debug, Clone)]
pub(super) enum ReopenPlan {
    Managed {
        relation_oid: pg_sys::Oid,
    },
    Foreign {
        relation_oid: pg_sys::Oid,
        effective_user_oid: pg_sys::Oid,
        identity: ForeignTableIdentity,
        generation: PlanSourceIdentity,
    },
}

#[derive(Serialize)]
enum EncodedWorkerPlan<'a> {
    Managed {
        relation_oid: u32,
    },
    Foreign {
        relation_oid: u32,
        effective_user_oid: u32,
        catalog_name: &'a str,
        namespace: &'a str,
        table_name: &'a str,
        writable: bool,
        table_uuid: [u8; 16],
        schema_id: i32,
    },
}

#[derive(Deserialize)]
enum DecodedWorkerPlan {
    Managed {
        relation_oid: u32,
    },
    Foreign {
        relation_oid: u32,
        effective_user_oid: u32,
        catalog_name: String,
        namespace: String,
        table_name: String,
        writable: bool,
        table_uuid: [u8; 16],
        schema_id: i32,
    },
}

impl ReopenPlan {
    pub(super) fn encode_prefix(&self) -> Result<Vec<u8>, Error> {
        let encoded = match self {
            Self::Managed { relation_oid } => EncodedWorkerPlan::Managed {
                relation_oid: u32::from(*relation_oid),
            },
            Self::Foreign {
                relation_oid,
                effective_user_oid,
                identity,
                generation,
            } => {
                let (table_uuid, schema_id) = generation.parts();
                EncodedWorkerPlan::Foreign {
                    relation_oid: u32::from(*relation_oid),
                    effective_user_oid: u32::from(*effective_user_oid),
                    catalog_name: identity.catalog_name(),
                    namespace: identity.namespace(),
                    table_name: identity.table_name(),
                    writable: identity.mode().is_writable(),
                    table_uuid,
                    schema_id,
                }
            }
        };
        let payload = codec()
            .serialize(&encoded)
            .map_err(IcebergError::from)
            .map_err(Error::from)?;
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            Error::Scan(ScanError::WorkerPayload(
                "query worker reopen plan exceeds u64".to_owned(),
            ))
        })?;
        let mut prefix = vec![0_u8; HEADER_BYTES];
        prefix[..MAGIC.len()].copy_from_slice(MAGIC);
        prefix[8..12].copy_from_slice(&VERSION.to_le_bytes());
        prefix[16..24].copy_from_slice(&payload_len.to_le_bytes());
        prefix.extend_from_slice(&payload);
        Ok(prefix)
    }

    pub(super) fn decode(data: &[u8]) -> Result<(Self, &[u8]), Error> {
        if data.len() < HEADER_BYTES || data.get(..MAGIC.len()) != Some(MAGIC) {
            return Err(Self::payload_error(
                "query worker payload has an invalid header",
            ));
        }
        let version =
            u32::from_le_bytes(data[8..12].try_into().expect("fixed slice"));
        if version != VERSION {
            return Err(Self::payload_error(
                "query worker payload has an unsupported version",
            ));
        }
        let encoded_len =
            u64::from_le_bytes(data[16..24].try_into().expect("fixed slice"));
        let encoded_len = usize::try_from(encoded_len).map_err(|_| {
            Self::payload_error("query worker reopen plan exceeds this platform")
        })?;
        let inventory_offset =
            HEADER_BYTES.checked_add(encoded_len).ok_or_else(|| {
                Self::payload_error(
                    "query worker reopen plan length overflowed usize",
                )
            })?;
        let encoded = data.get(HEADER_BYTES..inventory_offset).ok_or_else(|| {
            Self::payload_error("query worker reopen plan is truncated")
        })?;
        let inventory = data.get(inventory_offset..).ok_or_else(|| {
            Self::payload_error("query worker task inventory is missing")
        })?;
        let plan = match codec()
            .deserialize(encoded)
            .map_err(IcebergError::from)
            .map_err(Error::from)?
        {
            DecodedWorkerPlan::Managed { relation_oid } => Self::Managed {
                relation_oid: pg_sys::Oid::from(relation_oid),
            },
            DecodedWorkerPlan::Foreign {
                relation_oid,
                effective_user_oid,
                catalog_name,
                namespace,
                table_name,
                writable,
                table_uuid,
                schema_id,
            } => Self::Foreign {
                relation_oid: pg_sys::Oid::from(relation_oid),
                effective_user_oid: pg_sys::Oid::from(effective_user_oid),
                identity: ForeignTableIdentity::with_mode(
                    catalog_name,
                    namespace,
                    table_name,
                    if writable {
                        ForeignTableMode::ReadWrite
                    } else {
                        ForeignTableMode::ReadOnly
                    },
                ),
                generation: PlanSourceIdentity::from_parts(table_uuid, schema_id),
            },
        };
        Ok((plan, inventory))
    }

    pub(super) fn file_io(&self) -> Result<FileIO, Error> {
        match self {
            Self::Managed { relation_oid } => {
                // PostgreSQL parallel workers take their own local relation
                // lock even after joining the leader's lock group, so the
                // relation remains protected if the leader exits first.
                let relation = RelationGuard::open_table(
                    *relation_oid,
                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                )?;
                Ok(StorageContext::for_read(&relation.as_handle())?.into_file_io())
            }
            Self::Foreign {
                relation_oid,
                effective_user_oid,
                identity,
                generation,
            } => {
                let resolved =
                    RestForeignTable::resolve(*relation_oid, *effective_user_oid)?;
                if resolved.identity() != identity {
                    return Err(IcebergFdwError::PlanIdentityChanged.into());
                }
                if &PlanSourceIdentity::from_table(resolved.table()) != generation {
                    return Err(IcebergFdwError::PlanSourceChanged.into());
                }
                Ok(resolved.table().file_io().clone())
            }
        }
    }

    fn payload_error(message: &'static str) -> Error {
        Error::Scan(ScanError::WorkerPayload(message.to_owned()))
    }
}

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}
