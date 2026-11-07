//! Postmaster-time loading and identity registry for AM and FDW providers.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::mem::take;

use lagodb_core::runtime_api::{
    ProviderIdentity, ProviderKind, REGISTER_DUPLICATE_NAME,
    REGISTER_OUTSIDE_PROVIDER_BOOTSTRAP, REGISTER_PROVIDER_LIBRARY_MISMATCH,
};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, pg_sys};
use thiserror::Error;

static PROVIDER_LIBRARIES: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

#[derive(Debug, Error)]
enum ProviderBootstrapError {
    #[error("provider library list contains an empty entry")]
    EmptyEntry,
    #[error("provider library name {name:?} is not a PostgreSQL library basename")]
    InvalidLibraryName { name: String },
    #[error("provider library {name:?} is configured more than once")]
    DuplicateLibrary { name: String },
    #[error("lagodb_base cannot be configured as its own provider library")]
    RuntimeLibrary,
    #[error("provider library {name:?} did not register a LagoDB provider")]
    MissingRegistration { name: String },
}

struct BootstrapState {
    expected_library: Option<CString>,
    registration_count: usize,
}

impl BootstrapState {
    const fn new() -> Self {
        Self {
            expected_library: None,
            registration_count: 0,
        }
    }

    fn begin(&mut self, library: &CStr) {
        assert!(
            self.expected_library.is_none(),
            "provider bootstrap cannot load nested provider libraries"
        );
        self.expected_library = Some(library.to_owned());
        self.registration_count = 0;
    }

    fn validate(&self, library: &CStr) -> Result<(), u32> {
        let Some(expected) = self.expected_library.as_deref() else {
            return Err(REGISTER_OUTSIDE_PROVIDER_BOOTSTRAP);
        };
        if expected != library {
            return Err(REGISTER_PROVIDER_LIBRARY_MISMATCH);
        }
        Ok(())
    }

    fn confirm_registration(&mut self) {
        assert!(
            self.expected_library.is_some(),
            "provider registration committed outside bootstrap"
        );
        self.registration_count += 1;
    }

    fn finish(&mut self) -> usize {
        self.expected_library = None;
        take(&mut self.registration_count)
    }
}

struct StoredProviderIdentity {
    name: CString,
    extension_name: CString,
    library_name: CString,
    kind: ProviderKind,
}

pub(crate) struct ValidatedProviderIdentity<'a> {
    name: &'a CStr,
    extension_name: &'a CStr,
    library_name: &'a CStr,
    kind: ProviderKind,
}

impl<'a> ValidatedProviderIdentity<'a> {
    /// Validate the provider-declared names of one identity descriptor.
    pub(crate) fn new(identity: &'a ProviderIdentity) -> Option<Self> {
        let name = identity.name();
        let extension_name = identity.extension_name();
        let library_name = identity.library_name();
        if name.is_empty()
            || !is_extension_name(extension_name.to_bytes())
            || !is_library_basename(library_name.to_bytes())
        {
            return None;
        }
        Some(Self {
            name,
            extension_name,
            library_name,
            kind: identity.kind(),
        })
    }
}

pub(crate) struct PreparedProviderIdentity {
    identity: Option<StoredProviderIdentity>,
}

thread_local! {
    static BOOTSTRAP_STATE: RefCell<BootstrapState> =
        const { RefCell::new(BootstrapState::new()) };
    static PROVIDERS: RefCell<Vec<StoredProviderIdentity>> =
        const { RefCell::new(Vec::new()) };
}

/// Define `lagodb.provider_libraries` on every load of the runtime library.
pub(crate) fn define_gucs() {
    GucRegistry::define_string_guc(
        c"lagodb.provider_libraries",
        c"LagoDB AM and FDW provider libraries",
        c"Comma-separated PostgreSQL library basenames loaded once by lagodb_base during shared preload. Restart required.",
        &PROVIDER_LIBRARIES,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

/// Load every configured provider library. `_PG_init` calls this only during
/// a normal shared preload.
pub(crate) fn load_configured() {
    let Some(configured) = PROVIDER_LIBRARIES.get() else {
        return;
    };
    let libraries = parse_library_names(&configured)
        .unwrap_or_else(|error| panic!("invalid lagodb.provider_libraries: {error}"));
    for library in libraries {
        BOOTSTRAP_STATE.with_borrow_mut(|state| state.begin(&library));

        let mut path =
            Vec::with_capacity(b"$libdir/".len() + library.to_bytes().len());
        path.extend_from_slice(b"$libdir/");
        path.extend_from_slice(library.to_bytes());
        let path = CString::new(path).expect("validated provider path has no NUL");
        // SAFETY: the path is a live NUL-terminated `$libdir/<basename>` string.
        // PostgreSQL owns DSO loading and invokes the provider's `_PG_init`
        // synchronously before returning.
        unsafe { pg_sys::load_file(path.as_ptr(), false) };

        let registration_count =
            BOOTSTRAP_STATE.with_borrow_mut(BootstrapState::finish);
        if registration_count == 0 {
            let error = ProviderBootstrapError::MissingRegistration {
                name: library.to_string_lossy().into_owned(),
            };
            panic!("cannot bootstrap LagoDB provider: {error}");
        }
    }
}

impl PreparedProviderIdentity {
    pub(crate) fn prepare(
        identity: ValidatedProviderIdentity<'_>,
    ) -> Result<Self, u32> {
        BOOTSTRAP_STATE.with_borrow(|state| state.validate(identity.library_name))?;
        PROVIDERS.with_borrow_mut(|providers| {
            if let Some(existing) = providers
                .iter()
                .find(|existing| existing.name.as_c_str() == identity.name)
            {
                let identical = existing.extension_name.as_c_str()
                    == identity.extension_name
                    && existing.library_name.as_c_str() == identity.library_name
                    && existing.kind == identity.kind;
                return if identical {
                    Ok(Self { identity: None })
                } else {
                    Err(REGISTER_DUPLICATE_NAME)
                };
            }
            providers.reserve(1);
            let identity = StoredProviderIdentity {
                name: identity.name.to_owned(),
                extension_name: identity.extension_name.to_owned(),
                library_name: identity.library_name.to_owned(),
                kind: identity.kind,
            };
            Ok(Self {
                identity: Some(identity),
            })
        })
    }

    pub(crate) fn commit(self) {
        if let Some(identity) = self.identity {
            PROVIDERS.with_borrow_mut(|providers| providers.push(identity));
        }
        BOOTSTRAP_STATE.with_borrow_mut(BootstrapState::confirm_registration);
    }
}

fn parse_library_names(
    configured: &CStr,
) -> Result<Vec<CString>, ProviderBootstrapError> {
    if configured.is_empty() {
        return Ok(Vec::new());
    }
    let mut libraries = Vec::<CString>::new();
    for raw_name in configured.to_bytes().split(|byte| *byte == b',') {
        let name = raw_name.trim_ascii();
        if name.is_empty() {
            return Err(ProviderBootstrapError::EmptyEntry);
        }
        if !is_library_basename(name) {
            return Err(ProviderBootstrapError::InvalidLibraryName {
                name: String::from_utf8_lossy(name).into_owned(),
            });
        }
        if name == b"lagodb_base" {
            return Err(ProviderBootstrapError::RuntimeLibrary);
        }
        if libraries.iter().any(|existing| existing.as_bytes() == name) {
            return Err(ProviderBootstrapError::DuplicateLibrary {
                name: String::from_utf8_lossy(name).into_owned(),
            });
        }
        libraries
            .push(CString::new(name).expect("validated library name has no NUL"));
    }
    Ok(libraries)
}

fn is_extension_name(name: &[u8]) -> bool {
    name.len()
        < usize::try_from(pg_sys::NAMEDATALEN)
            .expect("PostgreSQL NAMEDATALEN fits usize")
        && is_library_basename(name)
}

fn is_library_basename(name: &[u8]) -> bool {
    !name.is_empty()
        && name[0].is_ascii_alphanumeric()
        && name
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-'))
}
