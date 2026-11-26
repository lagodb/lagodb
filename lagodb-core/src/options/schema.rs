//! Shared DDL option schema and `DefElem` extraction.
//!
//! Table access methods and tablespaces define their own option schemas, while
//! this module owns the PostgreSQL list-walking and value-normalization logic.

use std::ffi::{CStr, c_void};
use std::ptr;

use crate::diag::SqlStateError;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OptionSchemaError {
    #[error("option '{option}' specified more than once")]
    Duplicate { option: String },

    #[error("option '{option}' requires a value")]
    MissingValue { option: String },

    #[error("invalid boolean value \"{value}\" for option '{option}'")]
    InvalidBool { option: String, value: String },

    #[error("invalid integer value \"{value}\" for option '{option}'")]
    InvalidInteger { option: String, value: String },

    #[error("value {value} for option '{option}' is less than minimum {min}")]
    IntBelowMin {
        option: String,
        value: i32,
        min: i32,
    },

    #[error("value {value} for option '{option}' is greater than maximum {max}")]
    IntAboveMax {
        option: String,
        value: i32,
        max: i32,
    },

    #[error(
        "invalid value \"{value}\" for option '{option}'. Allowed values are: {allowed}"
    )]
    InvalidEnum {
        option: String,
        value: String,
        allowed: String,
    },

    #[error("table option '{option}' is managed by the table access method")]
    AccessMethodManaged { option: String },
}
impl SqlStateError for OptionSchemaError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionAccess {
    /// Accepted on CREATE but rejected by ALTER SET and RESET.
    UserCreateOnly,
    /// Accepted on CREATE and by ALTER SET and RESET.
    UserMutable,
    /// Declared by an AM but never accepted from user DDL.
    AccessMethodManaged,
}

pub struct OptionDef {
    pub name: &'static str,
    pub kind: OptionKind,
    pub access: OptionAccess,
    pub description: &'static str,
}

impl OptionDef {
    /// Whether `def_elem` names this unqualified option (`defnamespace == NULL`).
    ///
    /// # Safety
    ///
    /// `def_elem` must reference a live PostgreSQL `DefElem` parse-tree node.
    pub(crate) unsafe fn matches_def_elem(&self, def_elem: &pg_sys::DefElem) -> bool {
        def_elem.defnamespace.is_null()
            && unsafe { CStr::from_ptr(def_elem.defname) }.to_bytes()
                == self.name.as_bytes()
    }

    /// Find the definition matching `def_elem` in `options`.
    ///
    /// # Safety
    ///
    /// `def_elem` must reference a live PostgreSQL `DefElem` parse-tree node.
    pub(crate) unsafe fn find_matching<'a>(
        options: &'a [Self],
        def_elem: &pg_sys::DefElem,
    ) -> Option<&'a Self> {
        options
            .iter()
            .find(|option| unsafe { option.matches_def_elem(def_elem) })
    }
}

/// Try to match a single `DefElem` against the valid option definitions.
///
/// Returns `Ok(true)` if the option matched, `Ok(false)` if not.
///
/// # Safety
///
/// `def_elem_ptr` must point at a valid PostgreSQL `DefElem` node.
unsafe fn try_extract_single_option(
    def_elem_ptr: *mut pg_sys::DefElem,
    valid_options: &[OptionDef],
    custom_opts: &mut Vec<(String, Option<String>)>,
) -> Result<bool, OptionSchemaError> {
    let def_elem = unsafe { &*def_elem_ptr };
    let Some(def) = (unsafe { OptionDef::find_matching(valid_options, def_elem) })
    else {
        return Ok(false);
    };

    if def.access == OptionAccess::AccessMethodManaged {
        return Err(OptionSchemaError::AccessMethodManaged {
            option: def.name.to_owned(),
        });
    }

    // SAFETY: defGetString reads from a valid DefElem node with non-null arg.
    let raw_val = unsafe {
        if def_elem.arg.is_null() {
            None
        } else {
            let val_ptr = pg_sys::defGetString(def_elem_ptr);
            Some(CStr::from_ptr(val_ptr).to_string_lossy().into_owned())
        }
    };

    if custom_opts.iter().any(|(k, _)| k == def.name) {
        return Err(OptionSchemaError::Duplicate {
            option: def.name.to_owned(),
        });
    }

    let validated_val = validate_option_value(def, def.name, raw_val)?;
    custom_opts.push((def.name.to_owned(), validated_val));
    Ok(true)
}

/// Extract and remove custom options from a generic list of DefElem.
///
/// Iterates the options list attached to a DDL statement, extracts entries
/// that match `valid_options`, validates them, and removes them from the
/// statement so PostgreSQL only sees its own native options.
///
/// # Safety
///
/// * `options_list_ptr` must point at a valid `*mut pg_sys::List` (or a
///   pointer to NULL for an empty list). The list and every `DefElem` in it
///   must have been allocated by PostgreSQL in the current memory context.
/// * The caller must ensure no concurrent access to the statement node while
///   this function runs (satisfied by the single-threaded backend model).
pub unsafe fn extract_and_remove_options(
    options_list_ptr: *mut *mut pg_sys::List,
    valid_options: &[OptionDef],
) -> Result<Vec<(String, Option<String>)>, OptionSchemaError> {
    let mut custom_opts = Vec::new();
    let mut new_pg_opts: *mut pg_sys::List = ptr::null_mut();

    // SAFETY: caller guarantees `options_list_ptr` is valid.
    if unsafe { (*options_list_ptr).is_null() } {
        return Ok(custom_opts);
    }

    // SAFETY: non-null list; elements/length are valid per pg_sys::List layout.
    let (cell, length) = unsafe {
        let list = *options_list_ptr;
        ((*list).elements, (*list).length)
    };

    for i in 0..length {
        // SAFETY: i < length, so cell.add(i) is within the list allocation.
        let def_elem_ptr =
            unsafe { (*cell.add(i as usize)).ptr_value.cast::<pg_sys::DefElem>() };

        let matched = unsafe {
            try_extract_single_option(def_elem_ptr, valid_options, &mut custom_opts)?
        };

        if !matched {
            // SAFETY: lappend is safe for a valid (or null) list and node.
            new_pg_opts = unsafe {
                pg_sys::lappend(new_pg_opts, def_elem_ptr.cast::<c_void>())
            };
        }
    }

    // SAFETY: caller guarantees `options_list_ptr` is writable.
    unsafe { *options_list_ptr = new_pg_opts };
    Ok(custom_opts)
}

/// Extract custom options from a generic list of DefElem without modifying it.
///
/// # Safety
///
/// `options_list` must be either NULL or a valid PostgreSQL `List` of
/// `DefElem` nodes allocated by PostgreSQL.
pub unsafe fn extract_options(
    options_list: *mut pg_sys::List,
    valid_options: &[OptionDef],
) -> Result<Vec<(String, Option<String>)>, OptionSchemaError> {
    let mut custom_opts = Vec::new();

    if options_list.is_null() {
        return Ok(custom_opts);
    }

    // SAFETY: caller guarantees `options_list` is valid.
    let (cell, length) =
        unsafe { ((*options_list).elements, (*options_list).length) };

    for i in 0..length {
        // SAFETY: i < length, so cell.add(i) is within the list allocation.
        let def_elem_ptr =
            unsafe { (*cell.add(i as usize)).ptr_value.cast::<pg_sys::DefElem>() };

        unsafe {
            try_extract_single_option(def_elem_ptr, valid_options, &mut custom_opts)?;
        }
    }

    Ok(custom_opts)
}

/// Extract and remove recognized option names from a `RESET (...)` list.
///
/// Unlike [`extract_and_remove_options`], RESET entries carry no value, so
/// this path validates only membership and duplicate names.
///
/// # Safety
///
/// The safety requirements are identical to [`extract_and_remove_options`].
pub unsafe fn extract_and_remove_option_names(
    options_list_ptr: *mut *mut pg_sys::List,
    valid_options: &[OptionDef],
) -> Result<Vec<String>, OptionSchemaError> {
    let mut custom_names = Vec::new();
    let mut new_pg_opts: *mut pg_sys::List = ptr::null_mut();

    if unsafe { (*options_list_ptr).is_null() } {
        return Ok(custom_names);
    }

    let (cell, length) = unsafe {
        let list = *options_list_ptr;
        ((*list).elements, (*list).length)
    };

    for i in 0..length {
        let def_elem_ptr =
            unsafe { (*cell.add(i as usize)).ptr_value.cast::<pg_sys::DefElem>() };
        let def_elem = unsafe { &*def_elem_ptr };

        if let Some(def) =
            unsafe { OptionDef::find_matching(valid_options, def_elem) }
        {
            if custom_names.iter().any(|name| name == def.name) {
                return Err(OptionSchemaError::Duplicate {
                    option: def.name.to_owned(),
                });
            }
            custom_names.push(def.name.to_owned());
        } else {
            new_pg_opts = unsafe {
                pg_sys::lappend(new_pg_opts, def_elem_ptr.cast::<c_void>())
            };
        }
    }

    unsafe { *options_list_ptr = new_pg_opts };
    Ok(custom_names)
}

pub(crate) fn validate_option_value(
    def: &OptionDef,
    option: &str,
    raw_val: Option<String>,
) -> Result<Option<String>, OptionSchemaError> {
    match &def.kind {
        OptionKind::Bool { .. } => {
            let v = raw_val.as_deref().unwrap_or("true");
            let parsed =
                parse_bool(v).ok_or_else(|| OptionSchemaError::InvalidBool {
                    option: option.to_owned(),
                    value: v.to_owned(),
                })?;
            Ok(Some(parsed.to_string()))
        }
        OptionKind::Int { min, max, .. } => {
            let v = raw_val.ok_or_else(|| OptionSchemaError::MissingValue {
                option: option.to_owned(),
            })?;
            let int_val =
                v.parse::<i32>()
                    .map_err(|_| OptionSchemaError::InvalidInteger {
                        option: option.to_owned(),
                        value: v,
                    })?;

            if let Some(min_val) = min
                && int_val < *min_val
            {
                return Err(OptionSchemaError::IntBelowMin {
                    option: option.to_owned(),
                    value: int_val,
                    min: *min_val,
                });
            }
            if let Some(max_val) = max
                && int_val > *max_val
            {
                return Err(OptionSchemaError::IntAboveMax {
                    option: option.to_owned(),
                    value: int_val,
                    max: *max_val,
                });
            }
            Ok(Some(int_val.to_string()))
        }
        OptionKind::String { .. } => {
            let val = raw_val.ok_or_else(|| OptionSchemaError::MissingValue {
                option: option.to_owned(),
            })?;
            Ok(Some(val))
        }
        OptionKind::Enum { values, .. } => {
            let val = raw_val.ok_or_else(|| OptionSchemaError::MissingValue {
                option: option.to_owned(),
            })?;
            if !values.contains(&val.as_str()) {
                return Err(OptionSchemaError::InvalidEnum {
                    option: option.to_owned(),
                    value: val,
                    allowed: values.join(", "),
                });
            }
            Ok(Some(val))
        }
    }
}

/// Parse a boolean value from a string, supporting various common formats.
pub(crate) fn parse_bool(s: &str) -> Option<bool> {
    const TRUE_VALUES: &[&str] = &["true", "t", "yes", "y", "on", "1"];
    const FALSE_VALUES: &[&str] = &["false", "f", "no", "n", "off", "0"];

    if TRUE_VALUES.iter().any(|v| s.eq_ignore_ascii_case(v)) {
        Some(true)
    } else if FALSE_VALUES.iter().any(|v| s.eq_ignore_ascii_case(v)) {
        Some(false)
    } else {
        None
    }
}
