//! Tablespace catalog option definitions.

use super::storage::StorageProtocol;
use crate::options::schema::{self, OptionDef, OptionKind};
use pgrx::pg_sys;

pub(crate) const OPT_PROTOCOL: &str = "protocol";
pub(crate) const OPT_BUCKET: &str = "bucket";
pub(crate) const OPT_REGION: &str = "region";
pub(crate) const OPT_ENDPOINT: &str = "endpoint";
pub(crate) const OPT_ALLOW_HTTP: &str = "allow_http";
pub(crate) const OPT_ACCESS_KEY_ID: &str = "access_key_id";
pub(crate) const OPT_SECRET_ACCESS_KEY: &str = "secret_access_key";
pub(crate) const OPT_TOKEN: &str = "token";
pub(crate) const OPT_VIRTUAL_HOSTED_STYLE_REQUEST: &str =
    "virtual_hosted_style_request";
pub(crate) const OPT_SKIP_SIGNATURE: &str = "skip_signature";
pub(crate) const OPT_BASE_URL: &str = "base_url";
pub(crate) const OPT_SERVICE_ACCOUNT_PATH: &str = "service_account_path";
pub(crate) const OPT_SERVICE_ACCOUNT_KEY: &str = "service_account_key";
pub(crate) const OPT_APPLICATION_CREDENTIALS_PATH: &str =
    "application_credentials_path";
pub(crate) const OPT_CONTAINER: &str = "container";
pub(crate) const OPT_ACCOUNT: &str = "account";
pub(crate) const OPT_ACCESS_KEY: &str = "access_key";
pub(crate) const OPT_BEARER_TOKEN: &str = "bearer_token";
pub(crate) const OPT_CLIENT_ID: &str = "client_id";
pub(crate) const OPT_CLIENT_SECRET: &str = "client_secret";
pub(crate) const OPT_TENANT_ID: &str = "tenant_id";
pub(crate) const OPT_USE_EMULATOR: &str = "use_emulator";

pub(crate) const DEFAULT_PROTOCOL: &str = "s3";
pub(crate) const DEFAULT_S3_REGION: &str = "us-east-1";
pub(crate) const DEFAULT_ALLOW_HTTP: bool = false;

static TABLESPACE_OPTION_DEFS: &[OptionDef] = &[
    OptionDef {
        name: OPT_PROTOCOL,
        kind: OptionKind::Enum {
            default: DEFAULT_PROTOCOL,
            values: StorageProtocol::ALL,
        },
        description: "Storage protocol (s3, gcs, azure)",
    },
    OptionDef {
        name: OPT_BUCKET,
        kind: OptionKind::String { default: None },
        description: "Object storage bucket name",
    },
    OptionDef {
        name: OPT_REGION,
        kind: OptionKind::String {
            default: Some(DEFAULT_S3_REGION),
        },
        description: "AWS Region",
    },
    OptionDef {
        name: OPT_ENDPOINT,
        kind: OptionKind::String { default: None },
        description: "S3 or Azure object storage endpoint",
    },
    OptionDef {
        name: OPT_ALLOW_HTTP,
        kind: OptionKind::Bool {
            default: DEFAULT_ALLOW_HTTP,
        },
        description: "Allow HTTP connections for S3 or Azure endpoints",
    },
    OptionDef {
        name: OPT_ACCESS_KEY_ID,
        kind: OptionKind::String { default: None },
        description: "S3 access key id",
    },
    OptionDef {
        name: OPT_SECRET_ACCESS_KEY,
        kind: OptionKind::String { default: None },
        description: "S3 secret access key",
    },
    OptionDef {
        name: OPT_TOKEN,
        kind: OptionKind::String { default: None },
        description: "S3 session token",
    },
    OptionDef {
        name: OPT_VIRTUAL_HOSTED_STYLE_REQUEST,
        kind: OptionKind::Bool { default: false },
        description: "Use virtual-hosted-style S3 requests",
    },
    OptionDef {
        name: OPT_SKIP_SIGNATURE,
        kind: OptionKind::Bool { default: false },
        description: "Skip object-store request signing",
    },
    OptionDef {
        name: OPT_BASE_URL,
        kind: OptionKind::String { default: None },
        description: "GCS API base URL",
    },
    OptionDef {
        name: OPT_SERVICE_ACCOUNT_PATH,
        kind: OptionKind::String { default: None },
        description: "GCS service account JSON path",
    },
    OptionDef {
        name: OPT_SERVICE_ACCOUNT_KEY,
        kind: OptionKind::String { default: None },
        description: "GCS service account JSON",
    },
    OptionDef {
        name: OPT_APPLICATION_CREDENTIALS_PATH,
        kind: OptionKind::String { default: None },
        description: "GCS application credentials path",
    },
    OptionDef {
        name: OPT_CONTAINER,
        kind: OptionKind::String { default: None },
        description: "Azure Blob Storage container",
    },
    OptionDef {
        name: OPT_ACCOUNT,
        kind: OptionKind::String { default: None },
        description: "Azure storage account",
    },
    OptionDef {
        name: OPT_ACCESS_KEY,
        kind: OptionKind::String { default: None },
        description: "Azure storage access key",
    },
    OptionDef {
        name: OPT_BEARER_TOKEN,
        kind: OptionKind::String { default: None },
        description: "Azure bearer token",
    },
    OptionDef {
        name: OPT_CLIENT_ID,
        kind: OptionKind::String { default: None },
        description: "Azure client id",
    },
    OptionDef {
        name: OPT_CLIENT_SECRET,
        kind: OptionKind::String { default: None },
        description: "Azure client secret",
    },
    OptionDef {
        name: OPT_TENANT_ID,
        kind: OptionKind::String { default: None },
        description: "Azure tenant id",
    },
    OptionDef {
        name: OPT_USE_EMULATOR,
        kind: OptionKind::Bool { default: false },
        description: "Use Azure storage emulator",
    },
];

pub(crate) fn is_tablespace_option(name: &str) -> bool {
    TABLESPACE_OPTION_DEFS
        .iter()
        .any(|option| option.name == name)
}

pub(crate) unsafe fn extract_and_remove_options(
    stmt: *mut pg_sys::CreateTableSpaceStmt,
) -> Result<Vec<(String, Option<String>)>, String> {
    unsafe {
        schema::extract_and_remove_options(
            &mut (*stmt).options,
            TABLESPACE_OPTION_DEFS,
        )
    }
}

pub(crate) unsafe fn extract_options(
    stmt: *const pg_sys::CreateTableSpaceStmt,
) -> Result<Vec<(String, Option<String>)>, String> {
    unsafe { schema::extract_options((*stmt).options, TABLESPACE_OPTION_DEFS) }
}
