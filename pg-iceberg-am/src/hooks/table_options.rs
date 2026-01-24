use crate::catalog::iceberg_metadata::IcebergMetadata;
use crate::catalog::init_table_storage_metadata;
use crate::constants::ICEBERG_AM_NAME;
use pg_lakebase_core::option::{OptionKind, StorageCategory, TamOptionDef};
use pg_lakebase_core::pg_wrapper::PgWrapper;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;
use std::ffi::CStr;

// ============================================================================
//  Option Name Constants - Single source of truth for all option names
// ============================================================================

/// Iceberg table format version (1 or 2)
pub const OPT_FORMAT_VERSION: &str = "format-version";
/// Default format version
pub const OPT_FORMAT_VERSION_DEFAULT: i32 = 2;

/// Parquet compression codec (snappy, zstd, etc.)
pub const OPT_COMPRESSION_CODEC: &str = "write.parquet.compression-codec";
/// Default compression codec
pub const OPT_COMPRESSION_CODEC_DEFAULT: &str = "zstd";

/// Default file format for writing (parquet, avro, orc)
pub const OPT_WRITE_FORMAT: &str = "write.format.default";
/// Default write format
pub const OPT_WRITE_FORMAT_DEFAULT: &str = "parquet";
/// Allowed write format values
pub const OPT_WRITE_FORMAT_VALUES: &[&str] = &["parquet", "avro", "orc"];

// ============================================================================
//  Option Definitions
// ============================================================================

/// Iceberg-specific table options definition.
static ICEBERG_TABLE_OPTIONS: &[TamOptionDef] = &[
    TamOptionDef {
        name: OPT_FORMAT_VERSION,
        category: StorageCategory::Common,
        kind: OptionKind::Int {
            default: OPT_FORMAT_VERSION_DEFAULT,
            min: Some(1),
            max: Some(2),
        },
        description: "Iceberg table format version (1 or 2)",
    },
    TamOptionDef {
        name: OPT_COMPRESSION_CODEC,
        category: StorageCategory::Common,
        kind: OptionKind::String {
            default: Some(OPT_COMPRESSION_CODEC_DEFAULT),
        },
        description: "Parquet compression codec (snappy, zstd)",
    },
    TamOptionDef {
        name: OPT_WRITE_FORMAT,
        category: StorageCategory::Common,
        kind: OptionKind::Enum {
            default: OPT_WRITE_FORMAT_DEFAULT,
            values: OPT_WRITE_FORMAT_VALUES,
        },
        description: "Default file format (parquet, avro, orc)",
    },
];

struct IcebergTableHook;

/// Check if the CREATE TABLE statement uses the 'iceberg' access method.
fn is_iceberg_access_method(stmt: &pg_sys::CreateStmt) -> bool {
    unsafe {
        let am = stmt.accessMethod;
        if am.is_null() {
            return false;
        }
        CStr::from_ptr(am).to_string_lossy() == ICEBERG_AM_NAME
    }
}

impl UtilityHook for IcebergTableHook {
    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .is_a_mut::<pg_sys::CreateStmt>(pg_sys::NodeTag::T_CreateStmt)
            .expect("Hook registered for T_CreateStmt");

        if !is_iceberg_access_method(stmt) {
            return Ok(());
        }

        // Extract and validate options (but don't persist yet, just validate)
        TableOptions::extract_from_stmt(stmt, Some(ICEBERG_TABLE_OPTIONS)).map_err(
            |e| {
                UtilityHookError::Message(format!(
                    "table: option extraction failed - {}",
                    e
                ))
            },
        )?;
        Ok(())
    }

    fn on_post(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .is_a_mut::<pg_sys::CreateStmt>(pg_sys::NodeTag::T_CreateStmt)
            .expect("Hook registered for T_CreateStmt");

        if !is_iceberg_access_method(stmt) {
            return Ok(());
        }

        // Resolve the OID of the newly created table using RangeVarGetRelidExtended
        // This correctly handles search_path resolution when schema is not specified
        let oid = PgWrapper::range_var_get_relid(
            stmt.relation,
            pg_sys::NoLock as pg_sys::LOCKMODE,
            false, // fail if missing (shouldn't happen in on_post of CreateStmt)
        )
        .map_err(|e| {
            UtilityHookError::Message(format!(
                "table: failed to get table OID - {}",
                e
            ))
        })?;

        if let Some(opts) =
            TableOptions::extract_from_stmt(stmt, Some(ICEBERG_TABLE_OPTIONS))
                .map_err(|e| {
                    UtilityHookError::Message(format!(
                        "table: option extraction failed - {}",
                        e
                    ))
                })?
        {
            opts.persist_to_catalog(oid).map_err(|e| {
                UtilityHookError::Message(format!(
                    "table: failed to persist options to catalog - {}",
                    e
                ))
            })?;
        }

        // Use RAII guard to ensure table is properly closed
        let guard =
            TableGuard::open(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
                .map_err(|e| {
                    UtilityHookError::Message(format!(
                        "table: failed to open table - {}",
                        e
                    ))
                })?;

        // Create Iceberg metadata files on storage and get the metadata location
        let metadata_location = init_table_storage_metadata(&guard.as_handle())
            .map_err(|e| {
                UtilityHookError::Message(format!(
                    "table: failed to create iceberg metadata - {}",
                    e
                ))
            })?;

        // Insert metadata record into lakebase.iceberg_metadata catalog table
        IcebergMetadata::new(oid)
            .with_metadata_location(metadata_location)
            .with_default_spec_id(0) // New table starts with default spec_id 0
            .insert()
            .map_err(|e| {
                UtilityHookError::Message(format!(
                    "table: failed to insert iceberg metadata record - {}",
                    e
                ))
            })?;
        Ok(())
    }
}

pub fn init_hook() {
    register_utility_hook(pg_sys::NodeTag::T_CreateStmt, Box::new(IcebergTableHook));
}
