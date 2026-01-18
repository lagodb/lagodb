use crate::option::storage_option;
use crate::pg_wrapper::{CacheRegisterSyscacheCallback, PgWrapper, PgWrapperError};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Once;
use thiserror::Error;

// ============================================================================
//  Error Type
// ============================================================================

/// Errors that can occur when looking up tablespace options.
#[derive(Error, Debug)]
pub enum TablespaceCacheError {
    #[error("failed to lookup tablespace: {0}")]
    LookupFailed(#[from] PgWrapperError),
}

// ============================================================================
//  Protocol-specific Option Structs
// ============================================================================

/// S3-compatible storage options
#[derive(Debug, Clone)]
pub struct S3Options {
    pub bucket: Option<String>,
    pub region: String,
    pub endpoint: Option<String>,
    pub allow_http: bool,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

impl S3Options {
    /// Converts S3 options into a HashMap of properties.
    pub fn to_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        if let Some(bucket) = &self.bucket {
            props.insert(storage_option::OPT_BUCKET.to_string(), bucket.clone());
        }
        props.insert(storage_option::OPT_REGION.to_string(), self.region.clone());
        if let Some(endpoint) = &self.endpoint {
            props.insert(storage_option::OPT_ENDPOINT.to_string(), endpoint.clone());
        }
        props.insert(
            storage_option::OPT_ALLOW_HTTP.to_string(),
            self.allow_http.to_string(),
        );
        if let Some(ak) = &self.access_key_id {
            props.insert(storage_option::OPT_ACCESS_KEY_ID.to_string(), ak.clone());
        }
        if let Some(sk) = &self.secret_access_key {
            props.insert(
                storage_option::OPT_SECRET_ACCESS_KEY.to_string(),
                sk.clone(),
            );
        }
        props
    }
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            bucket: None,
            region: storage_option::DEFAULT_S3_REGION.to_string(),
            endpoint: None,
            allow_http: storage_option::DEFAULT_ALLOW_HTTP,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

/// Google Cloud Storage options
#[derive(Debug, Clone)]
pub struct GcsOptions {
    pub bucket: Option<String>,
    pub project_id: Option<String>,
    // TODO: Add more GCS-specific options as needed
}

impl GcsOptions {
    /// Converts GCS options into a HashMap of properties.
    pub fn to_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        if let Some(bucket) = &self.bucket {
            props.insert(storage_option::OPT_BUCKET.to_string(), bucket.clone());
        }
        if let Some(project_id) = &self.project_id {
            props.insert("project_id".to_string(), project_id.clone());
        }
        props
    }
}

impl Default for GcsOptions {
    fn default() -> Self {
        Self {
            bucket: None,
            project_id: None,
        }
    }
}

/// Azure Blob Storage options
#[derive(Debug, Clone)]
pub struct AzureOptions {
    pub container: Option<String>,
    pub account_name: Option<String>,
    // TODO: Add more Azure-specific options as needed
}

impl AzureOptions {
    /// Converts Azure options into a HashMap of properties.
    pub fn to_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        if let Some(container) = &self.container {
            props.insert("container".to_string(), container.clone());
        }
        if let Some(account) = &self.account_name {
            props.insert("account_name".to_string(), account.clone());
        }
        props
    }
}

impl Default for AzureOptions {
    fn default() -> Self {
        Self {
            container: None,
            account_name: None,
        }
    }
}

// ============================================================================
//  StorageOptions - Tagged Union (Rust enum)
// ============================================================================

/// Storage options as a tagged union, similar to C union but type-safe.
#[derive(Debug, Clone)]
pub enum StorageOptions {
    S3(S3Options),
    Gcs(GcsOptions),
    Azure(AzureOptions),
}

impl StorageOptions {
    /// Returns true if the storage is a distributed filesystem (S3, GCS, or Azure).
    pub fn is_distributed_filesystem(&self) -> bool {
        match self {
            StorageOptions::S3(_)
            | StorageOptions::Gcs(_)
            | StorageOptions::Azure(_) => true,
        }
    }

    /// Returns the protocol name for this storage type.
    pub fn protocol(&self) -> &'static str {
        match self {
            StorageOptions::S3(_) => "s3",
            StorageOptions::Gcs(_) => "gcs",
            StorageOptions::Azure(_) => "azure",
        }
    }

    /// Returns the base URL for this storage configuration.
    ///
    /// Format:
    /// - S3: `s3://{bucket}`
    /// - GCS: `gs://{bucket}`
    /// - Azure: `az://{account}/{container}`
    pub fn to_base_url(&self) -> String {
        match self {
            StorageOptions::S3(s3) => {
                let bucket = s3.bucket.as_deref().unwrap_or("default");
                format!("s3://{}", bucket)
            }
            StorageOptions::Gcs(gcs) => {
                let bucket = gcs.bucket.as_deref().unwrap_or("default");
                format!("gs://{}", bucket)
            }
            StorageOptions::Azure(azure) => {
                let account = azure.account_name.as_deref().unwrap_or("default");
                let container = azure.container.as_deref().unwrap_or("default");
                format!("az://{}/{}", account, container)
            }
        }
    }

    /// Converts the storage options into a HashMap of properties.
    pub fn to_props(&self) -> HashMap<String, String> {
        match self {
            StorageOptions::S3(s3) => s3.to_props(),
            StorageOptions::Gcs(gcs) => gcs.to_props(),
            StorageOptions::Azure(azure) => azure.to_props(),
        }
    }
}

impl Default for StorageOptions {
    fn default() -> Self {
        StorageOptions::S3(S3Options::default())
    }
}

// ============================================================================
//  CachedTableSpaceOpts
// ============================================================================

/// Structure to hold cached tablespace options.
#[derive(Debug, Clone)]
pub struct CachedTableSpaceOpts {
    pub storage: StorageOptions,
}

impl CachedTableSpaceOpts {
    /// Returns true if the tablespace is on a distributed filesystem.
    pub fn is_distributed_filesystem(&self) -> bool {
        self.storage.is_distributed_filesystem()
    }
}

impl Default for CachedTableSpaceOpts {
    fn default() -> Self {
        Self {
            storage: StorageOptions::default(),
        }
    }
}

// ============================================================================
//  Cache Infrastructure
// ============================================================================
thread_local! {
    static TABLESPACE_CACHE: RefCell<HashMap<pg_sys::Oid, Option<Rc<CachedTableSpaceOpts>>>> =
        RefCell::new(HashMap::new());
}

static INIT: Once = Once::new();

/// Invalidate callback function called by Postgres
unsafe extern "C" fn invalidate_tablespace_cache_callback(
    _arg: pg_sys::Datum,
    _cacheid: std::os::raw::c_int,
    _hashvalue: u32,
) {
    // Clear the thread-local cache
    TABLESPACE_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Initialize the cache and register the callback
fn initialize_tablespace_cache() {
    INIT.call_once(|| unsafe {
        CacheRegisterSyscacheCallback(
            pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
            Some(invalidate_tablespace_cache_callback),
            0.into(),
        );
    });
}

// ============================================================================
//  Public API
// ============================================================================

/// Check if a tablespace is a distributed filesystem.
pub fn is_distributed_tablespace(spcid: pg_sys::Oid) -> bool {
    get_tablespace(spcid)
        .ok()
        .flatten()
        .map(|opts| opts.is_distributed_filesystem())
        .unwrap_or(false)
}

/// Get tablespace options for a given OID.
///
/// Returns an `Rc` to avoid cloning the entire struct on each access.
/// Since PostgreSQL backend is single-threaded, we use `Rc` instead of `Arc`
/// to avoid atomic operation overhead.
///
/// # Errors
///
/// Returns an error if the tablespace lookup fails due to a Postgres error.
pub fn get_tablespace(
    spcid: pg_sys::Oid,
) -> Result<Option<Rc<CachedTableSpaceOpts>>, TablespaceCacheError> {
    initialize_tablespace_cache();

    TABLESPACE_CACHE.with(|cache| {
        // Fast path: check cache
        if let Some(entry) = cache.borrow().get(&spcid) {
            return Ok(entry.clone());
        }

        // Slow path: lookup and update cache
        let opts: Option<Rc<CachedTableSpaceOpts>> =
            lookup_tablespace_options(spcid)?.map(Rc::new);
        cache.borrow_mut().insert(spcid, opts.clone());
        Ok(opts)
    })
}

// ============================================================================
//  Internal Parsing
// ============================================================================

fn lookup_tablespace_options(
    spcid: pg_sys::Oid,
) -> Result<Option<CachedTableSpaceOpts>, TablespaceCacheError> {
    let cache_id = pg_sys::SysCacheIdentifier::TABLESPACEOID as i32;

    let tp = PgWrapper::search_sys_cache1(
        cache_id,
        pg_sys::Datum::from(u32::from(spcid) as usize),
    )?;

    let Some(tp) = tp else {
        return Ok(None);
    };

    let mut is_null = false;
    let datum = PgWrapper::sys_cache_get_attr(
        cache_id,
        tp,
        pg_sys::Anum_pg_tablespace_spcoptions as i16,
        &mut is_null,
    )?;

    let result = if is_null {
        None
    } else {
        // SAFETY: We must copy the data from the Datum (which points into Postgres memory)
        // into owned Rust Strings BEFORE we release the SysCache tuple.
        // Vec::from_datum performs this copy.
        // The datum is valid because we obtained it from SysCacheGetAttr on a valid tuple.
        let options_vec =
            unsafe { Vec::<String>::from_datum(datum, false) }.unwrap_or_default();
        if options_vec.is_empty() {
            None
        } else {
            Some(parse_options_to_cached(options_vec))
        }
    };

    // Release the syscache tuple. If this fails, PostgreSQL's resource owner
    // will clean it up at transaction end anyway.
    let _ = PgWrapper::release_sys_cache(tp);

    Ok(result)
}

/// Parse options vector and build CachedTableSpaceOpts with the appropriate StorageOptions variant
fn parse_options_to_cached(options_vec: Vec<String>) -> CachedTableSpaceOpts {
    // First pass: extract protocol to determine which variant to use
    let mut protocol = "s3".to_string();
    let mut opts_map: HashMap<&str, String> = HashMap::new();

    for opt_str in &options_vec {
        if let Some((key, value)) = opt_str.split_once('=') {
            if key == storage_option::OPT_PROTOCOL {
                protocol = value.to_string();
            }
            opts_map.insert(key, value.to_string());
        }
    }

    // Build the appropriate StorageOptions variant based on protocol
    let storage = match protocol.as_str() {
        "s3" => StorageOptions::S3(parse_s3_options(&opts_map)),
        "gcs" => StorageOptions::Gcs(parse_gcs_options(&opts_map)),
        "azure" => StorageOptions::Azure(parse_azure_options(&opts_map)),
        unknown => panic!(
            "unsupported storage protocol: {}. Supported protocols: s3, gcs, azure",
            unknown
        ),
    };

    CachedTableSpaceOpts { storage }
}

fn parse_s3_options(opts: &HashMap<&str, String>) -> S3Options {
    S3Options {
        bucket: opts.get(storage_option::OPT_BUCKET).cloned(),
        region: opts
            .get(storage_option::OPT_REGION)
            .cloned()
            .unwrap_or_else(|| storage_option::DEFAULT_S3_REGION.to_string()),
        endpoint: opts.get(storage_option::OPT_ENDPOINT).cloned(),
        allow_http: opts
            .get(storage_option::OPT_ALLOW_HTTP)
            .and_then(|v| storage_option::parse_bool(v))
            .unwrap_or(storage_option::DEFAULT_ALLOW_HTTP),
        access_key_id: opts.get(storage_option::OPT_ACCESS_KEY_ID).cloned(),
        secret_access_key: opts.get(storage_option::OPT_SECRET_ACCESS_KEY).cloned(),
    }
}

fn parse_gcs_options(opts: &HashMap<&str, String>) -> GcsOptions {
    GcsOptions {
        bucket: opts.get(storage_option::OPT_BUCKET).cloned(),
        project_id: None, // TODO: Add constant when GCS options are defined
    }
}

fn parse_azure_options(_opts: &HashMap<&str, String>) -> AzureOptions {
    AzureOptions {
        container: None, // TODO: Add constant when Azure options are defined
        account_name: None,
    }
}
