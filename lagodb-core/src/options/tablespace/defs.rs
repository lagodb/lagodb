use crate::options::schema::{self, OptionDef, OptionKind, OptionMutability};
use pgrx::pg_sys;

pub const PUBLIC_STORAGE_VOLUME_OPTION: &str = "storage_volume";
pub const INTERNAL_STORAGE_VOLUME_ID_OPTION: &str = "lagodb_volume_id";

static TABLESPACE_OPTION_DEFS: &[OptionDef] = &[
    OptionDef {
        name: PUBLIC_STORAGE_VOLUME_OPTION,
        mutability: OptionMutability::CreateOnly,
        kind: OptionKind::String { default: None },
        description: "LagoDB storage volume name",
    },
    OptionDef {
        name: INTERNAL_STORAGE_VOLUME_ID_OPTION,
        mutability: OptionMutability::CreateOnly,
        kind: OptionKind::String { default: None },
        description: "LagoDB internal storage volume id",
    },
];

pub fn is_lagodb_tablespace_option(name: &str) -> bool {
    matches!(
        name,
        PUBLIC_STORAGE_VOLUME_OPTION | INTERNAL_STORAGE_VOLUME_ID_OPTION
    )
}

pub(crate) unsafe fn extract_and_remove_options(
    stmt: *mut pg_sys::CreateTableSpaceStmt,
) -> Result<Vec<(String, Option<String>)>, schema::OptionSchemaError> {
    unsafe {
        schema::extract_and_remove_options(
            &mut (*stmt).options,
            TABLESPACE_OPTION_DEFS,
        )
    }
}
