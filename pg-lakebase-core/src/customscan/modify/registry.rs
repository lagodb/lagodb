//! Type-erased registry for providers using the Custom ModifyTable framework.

use std::any::TypeId;
use std::cell::RefCell;
use std::marker::PhantomData;

use pgrx::pg_sys;

use crate::customscan::modify::LakebaseCustomModifyProvider;
use crate::customscan::provider::RelationContext;

use super::methods;

pub(super) trait ErasedModifyProvider: Sync {
    fn type_id(&self) -> TypeId;
    fn name(&self) -> &'static std::ffi::CStr;
    fn supports_relation(&self, context: &RelationContext<'_>) -> bool;
    fn path_methods(&self) -> *const pg_sys::CustomPathMethods;
    fn scan_methods(&self) -> *const pg_sys::CustomScanMethods;
}

struct ModifyProviderEntry<P: LakebaseCustomModifyProvider> {
    _marker: PhantomData<fn() -> P>,
}

impl<P: LakebaseCustomModifyProvider> ModifyProviderEntry<P> {
    const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

unsafe impl<P: LakebaseCustomModifyProvider> Sync for ModifyProviderEntry<P> {}

impl<P: LakebaseCustomModifyProvider> ErasedModifyProvider
    for ModifyProviderEntry<P>
{
    fn type_id(&self) -> TypeId {
        TypeId::of::<P>()
    }

    fn name(&self) -> &'static std::ffi::CStr {
        P::MODIFY_NAME
    }

    fn supports_relation(&self, context: &RelationContext<'_>) -> bool {
        P::supports_modify_target(context)
    }

    fn path_methods(&self) -> *const pg_sys::CustomPathMethods {
        &methods::tables::<P>().modify_path
    }

    fn scan_methods(&self) -> *const pg_sys::CustomScanMethods {
        &methods::tables::<P>().modify_scan
    }
}

thread_local! {
    static REGISTRY: RefCell<Vec<&'static dyn ErasedModifyProvider>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn register<P: LakebaseCustomModifyProvider>() {
    let entry: &'static dyn ErasedModifyProvider =
        Box::leak(Box::new(ModifyProviderEntry::<P>::new()));
    assert!(
        P::MODIFY_NAME != P::NAME,
        "Custom ModifyTable name {:?} conflicts with its scan provider name",
        P::MODIFY_NAME,
    );
    REGISTRY.with_borrow_mut(|registry| {
        assert!(
            registry.iter().all(|existing| {
                existing.type_id() != TypeId::of::<P>()
                    && existing.name() != P::MODIFY_NAME
            }),
            "Custom ModifyTable provider/name {:?} is already registered",
            P::MODIFY_NAME,
        );
        registry.push(entry);
    });
    methods::register::<P>();
}

pub(super) fn matching(
    context: &RelationContext<'_>,
) -> Option<&'static dyn ErasedModifyProvider> {
    REGISTRY.with_borrow(|registry| {
        let mut matches = registry
            .iter()
            .copied()
            .filter(|provider| provider.supports_relation(context));
        let first = matches.next();
        assert!(
            matches.next().is_none(),
            "multiple Custom ModifyTable providers claim relation {}",
            context.rel_oid(),
        );
        first
    })
}

pub(super) fn is_modify_scan_methods(
    methods: *const pg_sys::CustomScanMethods,
) -> bool {
    REGISTRY.with_borrow(|registry| {
        registry
            .iter()
            .any(|provider| provider.scan_methods() == methods)
    })
}

pub(crate) fn has_provider(context: &RelationContext<'_>) -> bool {
    matching(context).is_some()
}
