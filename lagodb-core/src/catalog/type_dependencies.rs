//! Stored type dependencies, following PG's find_composite_type_dependencies.

use std::collections::HashSet;
use std::ffi::c_char;
use std::slice;

use pgrx::pg_sys;

use super::{CatalogRelation, CatalogScanKey, CatalogSnapshot, get_relation_oid};
use crate::diag::PgError;
use crate::handles::RelationGuard;

// pg_depend.h's fixed-width form is not exposed by pgrx.
#[repr(C)]
struct DependencyForm {
    classid: pg_sys::Oid,
    objid: pg_sys::Oid,
    objsubid: i32,
    _refclassid: pg_sys::Oid,
    _refobjid: pg_sys::Oid,
    _refobjsubid: i32,
    _deptype: c_char,
}

pub struct TypeDependencies;

impl TypeDependencies {
    /// Find a stored column of this type, including arrays, domains and
    /// composite members, on a relation using the specified access method.
    pub fn used_by_access_method(
        type_oid: pg_sys::Oid,
        am: pg_sys::Oid,
    ) -> Result<bool, PgError> {
        let namespace = pg_sys::Oid::from(pg_sys::PG_CATALOG_NAMESPACE);
        let catalog = CatalogRelation::open(
            get_relation_oid(c"pg_depend", namespace),
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )?;
        let index = get_relation_oid(c"pg_depend_reference_index", namespace);
        let mut pending = vec![type_oid];
        let mut visited = HashSet::new();
        while let Some(oid) = pending.pop() {
            if !visited.insert(oid) {
                continue;
            }
            let mut scan = catalog.begin_scan(
                index,
                true,
                CatalogSnapshot::Default,
                [
                    CatalogScanKey::oid_eq(4, pg_sys::TypeRelationId),
                    CatalogScanKey::oid_eq(5, oid),
                ],
            )?;
            while let Some(tuple) = scan.get_next()? {
                // SAFETY: the scan pins a pg_depend tuple with this fixed layout.
                let dependency = unsafe {
                    &*pg_sys::GETSTRUCT(tuple.as_raw()).cast::<DependencyForm>()
                };
                if dependency.classid == pg_sys::TypeRelationId {
                    pending.push(dependency.objid);
                } else if dependency.classid == pg_sys::RelationRelationId {
                    let relation = RelationGuard::open_relation(
                        dependency.objid,
                        pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                    )?;
                    let handle = relation.as_handle();
                    let desc = handle.tuple_desc();
                    // Like PG, ignore view/expression dependencies without a
                    // stored attribute of this type. Attribute positions are
                    // descriptor indices, not a separate map.
                    let attrs = unsafe {
                        slice::from_raw_parts(
                            (*desc).attrs.as_ptr(),
                            (*desc).natts as usize,
                        )
                    };
                    let stored = if dependency.objsubid > 0
                        && dependency.objsubid as usize <= attrs.len()
                    {
                        let attr = &attrs[dependency.objsubid as usize - 1];
                        !attr.attisdropped && attr.atttypid == oid
                    } else {
                        attrs
                            .iter()
                            .any(|attr| !attr.attisdropped && attr.atttypid == oid)
                    };
                    if !stored {
                        continue;
                    }
                    if handle.access_method_oid() == am {
                        return Ok(true);
                    }
                    if handle.relkind() as u8 == pg_sys::RELKIND_COMPOSITE_TYPE {
                        // SAFETY: the guard keeps rd_rel live for this read.
                        pending
                            .push(unsafe { (*(*relation.as_raw()).rd_rel).reltype });
                    }
                }
            }
        }
        Ok(false)
    }
}
