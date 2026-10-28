//! Runtime registry and C-bridge callbacks for table-provider descriptors.

use std::ops::ControlFlow;
use std::ptr;

use lagodb_core::runtime_api::{
    REGISTER_DUPLICATE_ACCESS_METHOD, REGISTER_DUPLICATE_NAME, TableProvider,
};
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{pg_guard, pg_sys};

use crate::descriptor_registry::{
    DescriptorNode, DescriptorRegistry, DescriptorSnapshot,
};

thread_local! {
    static TABLE_PROVIDERS: DescriptorRegistry<TableProvider> =
        const { DescriptorRegistry::new() };
}

/// Backend-local owner of table-provider descriptors.
///
/// Descriptor nodes are never reclaimed, so the addresses returned by
/// `provider_for_am` stay valid for the backend lifetime.
pub(crate) struct TableProviderRegistry;

impl TableProviderRegistry {
    fn snapshot() -> DescriptorSnapshot<TableProvider> {
        TABLE_PROVIDERS.with(|registry| registry.snapshot())
    }

    fn prepare(
        descriptor: &TableProvider,
    ) -> Result<Option<Box<DescriptorNode<TableProvider>>>, u32> {
        let conflict = Self::snapshot().walk(|existing| {
            if existing.name() == descriptor.name() {
                return ControlFlow::Break(
                    if existing.is_same_provider(descriptor) {
                        Ok(None)
                    } else {
                        Err(REGISTER_DUPLICATE_NAME)
                    },
                );
            }
            if existing.access_method_name() == descriptor.access_method_name() {
                return ControlFlow::Break(Err(REGISTER_DUPLICATE_ACCESS_METHOD));
            }
            ControlFlow::Continue(())
        });
        match conflict {
            ControlFlow::Break(result) => result,
            // Allocate before any runtime registry is changed. The later commit
            // is therefore allocation-free and cannot leave provider and hook
            // registries partially published.
            ControlFlow::Continue(()) => Ok(Some(DescriptorNode::new(*descriptor))),
        }
    }

    fn commit(entry: Option<Box<DescriptorNode<TableProvider>>>) {
        let _ = TABLE_PROVIDERS.with(|registry| registry.commit(entry));
    }

    /// Resolve one provider's database-local access method OID.
    ///
    /// AM OIDs do not exist yet during shared-preload registration, so this
    /// catalog-reading provider callback runs only at lookup time.
    fn resolve_access_method(descriptor: &TableProvider) -> pg_sys::Oid {
        // SAFETY: registration validated this backend-lifetime descriptor. The
        // callback has pg_guard, so this barrier turns its ERROR into an
        // unwind through the caller's Rust frames.
        unsafe { pg_guard_ffi_boundary(|| (descriptor.access_method_oid())()) }
    }

    /// Whether any registered provider resolves in the connected database.
    pub(crate) fn has_available_provider() -> bool {
        Self::snapshot()
            .walk(|descriptor| {
                if Self::resolve_access_method(descriptor) == pg_sys::InvalidOid {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            })
            .is_break()
    }

    /// Whether a catalog access method belongs to a registered provider.
    pub(crate) fn is_registered_am(access_method_oid: pg_sys::Oid) -> bool {
        Self::lookup(access_method_oid).is_some()
    }

    fn lookup(access_method_oid: pg_sys::Oid) -> Option<&'static TableProvider> {
        // InvalidOid means no database-local AM, both for an unavailable provider
        // and for relations without an AM. It is not an identity.
        if access_method_oid == pg_sys::InvalidOid {
            return None;
        }
        let mut matched = None;
        let _: ControlFlow<()> = Self::snapshot().walk(|descriptor| {
            // Unavailable providers cannot match the valid requested AM identity.
            if Self::resolve_access_method(descriptor) == access_method_oid {
                if matched.is_some() {
                    panic!(
                        "multiple table providers resolved to access method OID {access_method_oid}"
                    );
                }
                matched = Some(descriptor);
            }
            ControlFlow::Continue(())
        });
        matched
    }
}

pub(crate) struct ValidatedTableProvider<'a>(&'a TableProvider);

impl<'a> ValidatedTableProvider<'a> {
    /// Validate the provider-declared names of one table-provider descriptor.
    pub(crate) fn new(descriptor: &'a TableProvider) -> Option<Self> {
        let access_method_name = descriptor.access_method_name();
        if descriptor.name().is_empty()
            || access_method_name.is_empty()
            || access_method_name.to_bytes().len()
                >= usize::try_from(pg_sys::NAMEDATALEN)
                    .expect("PostgreSQL NAMEDATALEN fits usize")
        {
            return None;
        }
        Some(Self(descriptor))
    }
}

pub(crate) struct PreparedTableProviderRegistration {
    entry: Option<Box<DescriptorNode<TableProvider>>>,
}

impl PreparedTableProviderRegistration {
    pub(crate) fn prepare(
        provider: Option<ValidatedTableProvider<'_>>,
    ) -> Result<Self, u32> {
        let entry = match provider {
            Some(ValidatedTableProvider(descriptor)) => {
                TableProviderRegistry::prepare(descriptor)?
            }
            None => None,
        };
        Ok(Self { entry })
    }

    pub(crate) fn commit(self) {
        TableProviderRegistry::commit(self.entry);
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn provider_for_am(
    access_method_oid: pg_sys::Oid,
) -> *const TableProvider {
    TableProviderRegistry::lookup(access_method_oid)
        .map_or(ptr::null(), ptr::from_ref)
}
