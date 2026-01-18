//! Low-level storage options extraction and validation.
//!
//! This module provides the core functionality for extracting custom options
//! from PostgreSQL DDL statements and validating them against a schema.

use pgrx::pg_sys;
use std::ffi::CStr;

// ============================================================================
//  Option Name Constants
// ============================================================================
pub const OPT_PROTOCOL: &str = "protocol";
pub const OPT_BUCKET: &str = "bucket";
pub const OPT_REGION: &str = "region";
pub const OPT_ENDPOINT: &str = "endpoint";
pub const OPT_ALLOW_HTTP: &str = "allow_http";
pub const OPT_ACCESS_KEY_ID: &str = "access_key_id";
pub const OPT_SECRET_ACCESS_KEY: &str = "secret_access_key";
// ============================================================================
//  Default Value Constants
// ============================================================================
pub const DEFAULT_PROTOCOL: &str = "s3";
pub const DEFAULT_S3_REGION: &str = "us-east-1";
pub const DEFAULT_ALLOW_HTTP: bool = false;

// ============================================================================
//  Common Lakehouse Tablespace Option Definitions
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageCategory {
    Common,
    S3,
    GCS,
    Azure,
}

#[derive(Debug, Clone)]
pub enum OptionKind {
    Bool {
        default: bool,
    },
    Int {
        default: i32,
        min: Option<i32>,
        max: Option<i32>,
    },
    String {
        default: Option<&'static str>,
    },
    Enum {
        default: &'static str,
        values: &'static [&'static str],
    },
}

pub struct TamOptionDef {
    pub name: &'static str, // Core name without prefix, e.g., "bucket", "region"
    pub category: StorageCategory,
    pub kind: OptionKind,
    pub description: &'static str,
}

// Define all standard options common to storage extensions.
static STANDARD_STORAGE_OPTIONS: &[TamOptionDef] = &[
    // --- Common ---
    TamOptionDef {
        name: OPT_PROTOCOL,
        category: StorageCategory::Common,
        kind: OptionKind::String {
            default: Some(DEFAULT_PROTOCOL),
        },
        description: "Storage protocol (s3, gcs, azure)",
    },
    // --- S3 ---
    TamOptionDef {
        name: OPT_BUCKET,
        category: StorageCategory::S3,
        kind: OptionKind::String { default: None },
        description: "S3 Bucket name",
    },
    TamOptionDef {
        name: OPT_REGION,
        category: StorageCategory::S3,
        kind: OptionKind::String {
            default: Some(DEFAULT_S3_REGION),
        },
        description: "AWS Region",
    },
    TamOptionDef {
        name: OPT_ENDPOINT,
        category: StorageCategory::S3,
        kind: OptionKind::String { default: None },
        description: "Custom S3 Endpoint (for MinIO etc)",
    },
    TamOptionDef {
        name: OPT_ALLOW_HTTP,
        category: StorageCategory::S3,
        kind: OptionKind::Bool {
            default: DEFAULT_ALLOW_HTTP,
        },
        description: "Allow HTTP connections (insecure)",
    },
    TamOptionDef {
        name: OPT_ACCESS_KEY_ID,
        category: StorageCategory::S3,
        kind: OptionKind::String { default: None },
        description: "S3 Access Key ID",
    },
    TamOptionDef {
        name: OPT_SECRET_ACCESS_KEY,
        category: StorageCategory::S3,
        kind: OptionKind::String { default: None },
        description: "S3 Secret Access Key",
    },
];

// ============================================================================
//  Extraction Logic
// ============================================================================

/// Extract and remove custom options from a generic list of DefElem.
///
/// This function iterates through the options provided in a DDL statement.
/// If an option matches one of the `valid_options`, it is validated, extracted,
/// and removed from the statement's options list.
///
/// This serves both CreateStmt (tables) and CreateTableSpaceStmt.
///
/// # Arguments
/// * `options_list_ptr` - Pointer to the options list pointer
/// * `valid_options` - List of valid option definitions to match against
///
/// # Returns
/// * `Ok(Vec<(String, Option<String>)>)` - A list of validated custom options.
/// * `Err(String)` - If validation fails for any option.
pub unsafe fn extract_and_remove_options(
    options_list_ptr: *mut *mut pg_sys::List,
    valid_options: &[TamOptionDef],
) -> Result<Vec<(String, Option<String>)>, String> {
    unsafe {
        let mut custom_opts = Vec::new();
        let mut new_pg_opts: *mut pg_sys::List = std::ptr::null_mut();

        if (*options_list_ptr).is_null() {
            return Ok(custom_opts);
        }

        let cell = (*(*options_list_ptr)).elements;
        let length = (*(*options_list_ptr)).length;

        for i in 0..length {
            let def_elem_ptr =
                (*cell.add(i as usize)).ptr_value as *mut pg_sys::DefElem;
            let def_name_cstr = CStr::from_ptr((*def_elem_ptr).defname);
            let def_name = def_name_cstr.to_string_lossy().to_string();

            // Check if this option is one of our valid options
            if let Some(def) = valid_options.iter().find(|opt| opt.name == def_name) {
                // 1. Extract raw value
                // Use Postgres internal helper `defGetString` which handles T_String, T_Integer, T_Float, and T_A_Const (PG15+)
                let raw_val = if (*def_elem_ptr).arg.is_null() {
                    None
                } else {
                    let val_ptr = pg_sys::defGetString(def_elem_ptr);
                    (!val_ptr.is_null()).then(|| {
                        CStr::from_ptr(val_ptr).to_string_lossy().into_owned()
                    })
                };

                // 2. Check for duplicate options
                if custom_opts.iter().any(|(k, _)| k == &def_name) {
                    return Err(format!(
                        "option '{}' specified more than once",
                        def_name
                    ));
                }

                // 3. Validate and normalize value
                match validate_option_value(def, raw_val) {
                    Ok(validated_val) => {
                        custom_opts.push((def_name, validated_val));
                    }
                    Err(e) => {
                        return Err(format!(
                            "Invalid value for option '{}': {}",
                            def_name, e
                        ));
                    }
                }
            } else {
                // Not a known storage option, leave it for Postgres to handle.
                new_pg_opts = pg_sys::lappend(
                    new_pg_opts,
                    def_elem_ptr as *mut std::ffi::c_void,
                );
            }
        }

        *options_list_ptr = new_pg_opts;
        Ok(custom_opts)
    }
}

/// Legacy wrapper for tablespace options
pub unsafe fn extract_and_remove_custom_options(
    stmt: *mut pg_sys::CreateTableSpaceStmt,
) -> Result<Vec<(String, Option<String>)>, String> {
    unsafe {
        extract_and_remove_options(&mut (*stmt).options, STANDARD_STORAGE_OPTIONS)
    }
}

fn validate_option_value(
    def: &TamOptionDef,
    raw_val: Option<String>,
) -> Result<Option<String>, String> {
    match &def.kind {
        OptionKind::Bool { default: _ } => {
            // Postgres convention: omitting the value for a boolean option means "true"
            // regardless of the default value defined in our schema.
            let v = raw_val.unwrap_or_else(|| "true".to_string());
            let bool_res = parse_bool(&v)
                .ok_or_else(|| format!("invalid boolean value \"{}\"", v))?;
            Ok(Some(bool_res.to_string()))
        }
        OptionKind::Int {
            default: _,
            min,
            max,
        } => {
            // Int requires explicit value, standard "option" syntax without value is not supported for ints here
            let v = raw_val.ok_or("numeric option requires a value")?;
            let int_val = v
                .parse::<i32>()
                .map_err(|_| format!("invalid integer value \"{}\"", v))?;

            if let Some(min_val) = min {
                if int_val < *min_val {
                    return Err(format!(
                        "value {} is less than minimum {}",
                        int_val, min_val
                    ));
                }
            }
            if let Some(max_val) = max {
                if int_val > *max_val {
                    return Err(format!(
                        "value {} is greater than maximum {}",
                        int_val, max_val
                    ));
                }
            }
            Ok(Some(int_val.to_string()))
        }
        OptionKind::String { default } => {
            if let Some(val) = raw_val {
                Ok(Some(val))
            } else {
                // If user provided the key without value (arg is NULL), use default if present
                Ok(default.map(|s| s.to_string()))
            }
        }
        OptionKind::Enum { default, values } => {
            let val = raw_val.unwrap_or_else(|| default.to_string());
            if !values.contains(&val.as_str()) {
                return Err(format!(
                    "invalid value \"{}\". Allowed values are: {}",
                    val,
                    values.join(", ")
                ));
            }
            Ok(Some(val))
        }
    }
}

/// Parse a boolean value from a string, supporting various common formats.
/// Returns `None` if the string is not a recognized boolean representation.
pub fn parse_bool(s: &str) -> Option<bool> {
    let s = s.to_lowercase();
    match s.as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
}
