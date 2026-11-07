//! Binding of ModifyTarget-purpose CustomScans to the outer ModifyTable state.

use core::marker::PhantomData;

use crate::handles::RelationHandle;
use crate::table_am::{ModifyScanBinding, TableAccessMethod};

use super::contract::LagodbCustomModifyProvider;

/// One-time outer-executor binding for a provider scan in `ModifyTarget` purpose.
pub struct ModifyBindContext<'a, P: LagodbCustomModifyProvider + ?Sized> {
    pub relation: RelationHandle<'a>,
    pub binding:
        ModifyScanBinding<<P::AccessMethod as TableAccessMethod>::ModifyQueryState>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LagodbCustomModifyProvider> ModifyBindContext<'a, P> {
    pub(crate) fn new(
        relation: RelationHandle<'a>,
        binding: ModifyScanBinding<
            <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
        >,
    ) -> Self {
        Self {
            relation,
            binding,
            _marker: PhantomData,
        }
    }
}
