//! Credential-free identity of a physical object-storage service.

use std::fmt;
use std::sync::Arc;

use super::config::StoreConfig;
use crate::error::{StorageError, StorageResult};

/// Addressing fields that distinguish one physical object-storage service.
///
/// Authentication material is deliberately absent: the cache is shared across
/// credentials that address the same physical object. The canonical cache key
/// is computed once when the identity is created, so object-path construction
/// neither clones provider strings nor rebuilds the serialization.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackendDataIdentity {
    kind: BackendDataIdentityKind,
    cache_key: Arc<str>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum BackendDataIdentityKind {
    S3 {
        region: Option<Arc<str>>,
        endpoint: Option<Arc<str>>,
    },
    S3Compatible {
        endpoint: Arc<str>,
    },
    Gcs {
        base_url: Option<Arc<str>>,
    },
    Azure {
        account: Option<Arc<str>>,
        endpoint: Option<Arc<str>>,
        use_emulator: bool,
    },
    /// In-process backends supplied by embedders and tests.
    Memory {
        name: Arc<str>,
    },
}

impl BackendDataIdentity {
    pub fn from_config(config: &StoreConfig) -> Self {
        let kind = match config {
            StoreConfig::S3(config) => BackendDataIdentityKind::S3 {
                region: config.region.as_deref().map(Arc::from),
                endpoint: config.endpoint.as_deref().map(Arc::from),
            },
            StoreConfig::S3Compatible(config) => {
                BackendDataIdentityKind::S3Compatible {
                    endpoint: Arc::from(config.endpoint.as_str()),
                }
            }
            StoreConfig::Gcs(config) => BackendDataIdentityKind::Gcs {
                base_url: config.base_url.as_deref().map(Arc::from),
            },
            StoreConfig::Azure(config) => BackendDataIdentityKind::Azure {
                account: config.account.as_deref().map(Arc::from),
                endpoint: config.endpoint.as_deref().map(Arc::from),
                use_emulator: config.use_emulator,
            },
        };
        Self::from_kind(kind)
    }

    pub fn memory(name: impl Into<Arc<str>>) -> Self {
        Self::from_kind(BackendDataIdentityKind::Memory { name: name.into() })
    }

    /// Stable, credential-free serialization used by persistent cache keys and paths.
    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }

    pub fn from_cache_key(value: &str) -> StorageResult<Self> {
        let Some((tag, mut input)) = value.split_at_checked(1) else {
            return Err(StorageError::cache("missing backend identity tag"));
        };
        let kind = match tag {
            "s" => BackendDataIdentityKind::S3 {
                region: take_optional(&mut input)?,
                endpoint: take_optional(&mut input)?,
            },
            "c" => BackendDataIdentityKind::S3Compatible {
                endpoint: take_required(&mut input)?,
            },
            "g" => BackendDataIdentityKind::Gcs {
                base_url: take_optional(&mut input)?,
            },
            "a" => {
                let account = take_optional(&mut input)?;
                let endpoint = take_optional(&mut input)?;
                let Some((flag, rest)) = input.split_at_checked(1) else {
                    return Err(StorageError::cache(
                        "missing Azure emulator identity flag",
                    ));
                };
                input = rest;
                let use_emulator = match flag {
                    "0" => false,
                    "1" => true,
                    _ => {
                        return Err(StorageError::cache(
                            "invalid Azure emulator identity flag",
                        ));
                    }
                };
                BackendDataIdentityKind::Azure {
                    account,
                    endpoint,
                    use_emulator,
                }
            }
            "m" => BackendDataIdentityKind::Memory {
                name: take_required(&mut input)?,
            },
            _ => return Err(StorageError::cache("unknown backend identity tag")),
        };
        if !input.is_empty() {
            return Err(StorageError::cache("trailing bytes in backend identity"));
        }
        Ok(Self {
            kind,
            cache_key: Arc::from(value),
        })
    }

    fn from_kind(kind: BackendDataIdentityKind) -> Self {
        let mut output = String::new();
        match &kind {
            BackendDataIdentityKind::S3 { region, endpoint } => {
                output.push('s');
                push_optional(&mut output, region.as_deref());
                push_optional(&mut output, endpoint.as_deref());
            }
            BackendDataIdentityKind::S3Compatible { endpoint } => {
                output.push('c');
                push_required(&mut output, endpoint);
            }
            BackendDataIdentityKind::Gcs { base_url } => {
                output.push('g');
                push_optional(&mut output, base_url.as_deref());
            }
            BackendDataIdentityKind::Azure {
                account,
                endpoint,
                use_emulator,
            } => {
                output.push('a');
                push_optional(&mut output, account.as_deref());
                push_optional(&mut output, endpoint.as_deref());
                output.push(if *use_emulator { '1' } else { '0' });
            }
            BackendDataIdentityKind::Memory { name } => {
                output.push('m');
                push_required(&mut output, name);
            }
        }
        Self {
            kind,
            cache_key: Arc::from(output.into_boxed_str()),
        }
    }
}

impl fmt::Display for BackendDataIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            BackendDataIdentityKind::S3 { endpoint, .. } => {
                write!(f, "s3:{}", endpoint.as_deref().unwrap_or("aws"))
            }
            BackendDataIdentityKind::S3Compatible { endpoint } => {
                write!(f, "s3:{endpoint}")
            }
            BackendDataIdentityKind::Gcs { base_url } => {
                write!(f, "gs:{}", base_url.as_deref().unwrap_or("google"))
            }
            BackendDataIdentityKind::Azure {
                account, endpoint, ..
            } => write!(
                f,
                "az:{}",
                endpoint
                    .as_deref()
                    .or(account.as_deref())
                    .unwrap_or("emulator")
            ),
            BackendDataIdentityKind::Memory { name } => {
                write!(f, "memory:{name}")
            }
        }
    }
}

impl From<&str> for BackendDataIdentity {
    fn from(value: &str) -> Self {
        Self::memory(value)
    }
}

impl From<String> for BackendDataIdentity {
    fn from(value: String) -> Self {
        Self::memory(value)
    }
}

fn push_optional(output: &mut String, value: Option<&str>) {
    match value {
        Some(value) => {
            output.push('1');
            push_required(output, value);
        }
        None => output.push('0'),
    }
}

fn push_required(output: &mut String, value: &str) {
    use std::fmt::Write;

    write!(output, "{:08x}", value.len()).expect("infallible write to String");
    output.push_str(value);
}

fn take_optional(input: &mut &str) -> StorageResult<Option<Arc<str>>> {
    let Some((tag, rest)) = input.split_at_checked(1) else {
        return Err(StorageError::cache("missing optional identity field tag"));
    };
    *input = rest;
    match tag {
        "0" => Ok(None),
        "1" => take_required(input).map(Some),
        _ => Err(StorageError::cache("invalid optional identity field tag")),
    }
}

fn take_required(input: &mut &str) -> StorageResult<Arc<str>> {
    let Some((length, rest)) = input.split_at_checked(8) else {
        return Err(StorageError::cache("truncated backend identity length"));
    };
    let length = usize::from_str_radix(length, 16).map_err(|error| {
        StorageError::cache_source("invalid backend identity length", error)
    })?;
    let Some((value, rest)) = rest.split_at_checked(length) else {
        return Err(StorageError::cache("truncated backend identity field"));
    };
    *input = rest;
    Ok(Arc::from(value))
}
