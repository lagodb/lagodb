use lagodb_core::fdw::prelude::*;
use lagodb_core::handles::RelationHandle;
use lagodb_core::pg_fdw;

#[pg_fdw]
pub struct FrameworkTestFdw;

impl ForeignDataWrapper for FrameworkTestFdw {
    const NAME: &'static core::ffi::CStr = c"framework_test_fdw";

    fn register(routine: &mut FdwRoutine) {
        register_scan::<Self>(routine);
        register_modify::<Self>(routine);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IdentityMode {
    Attribute,
    ItemPointer,
}

impl IdentityMode {
    pub(super) fn for_relation(relation: &RelationHandle<'_>) -> Self {
        if relation.relation_name().starts_with("fdw_test_tid_") {
            Self::ItemPointer
        } else {
            Self::Attribute
        }
    }
}
