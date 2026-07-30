//! Backend tests for `RuntimeParamResolver` (EXTERN/EXEC param resolution).

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_int;
    use std::ptr;

    use crate::lakebase_core::customscan::exec::support::make_estate_stub;
    use pg_lakebase_core::expr::ParamKey;
    use pg_lakebase_core::expr::{
        ExecParamRef, ExternParamRef, ResolvedParam, RuntimeParamResolver,
    };
    use pgrx::{pg_guard, pg_sys, pg_test};

    #[pg_guard]
    unsafe extern "C-unwind" fn fetch_param_from_workspace(
        _params: pg_sys::ParamListInfo,
        param_id: c_int,
        _speculative: bool,
        workspace: *mut pg_sys::ParamExternData,
    ) -> *mut pg_sys::ParamExternData {
        unsafe {
            (*workspace).ptype = pg_sys::INT4OID;
            (*workspace).value = pg_sys::Datum::from(param_id * 10);
            (*workspace).isnull = false;
            (*workspace).pflags = 0;
        }
        workspace
    }

    /// `ParamListInfo` with one INT4 slot (paramid 1) holding `value`.
    unsafe fn make_param_list_int4_value(value: i32) -> pg_sys::ParamListInfo {
        unsafe {
            let pli = pg_sys::makeParamList(1);
            let slot: *mut pg_sys::ParamExternData = (*pli).params.as_mut_ptr();
            (*slot).ptype = pg_sys::INT4OID;
            (*slot).value = pg_sys::Datum::from(value);
            (*slot).isnull = false;
            (*slot).pflags = 0;
            pli
        }
    }

    /// Two-slot `es_param_exec_vals` array with slot 1 populated (PARAM_EXEC side).
    unsafe fn make_param_exec_vals_slot1(
        slot1_value: i32,
    ) -> *mut pg_sys::ParamExecData {
        unsafe {
            let vals =
                pg_sys::palloc0(2 * core::mem::size_of::<pg_sys::ParamExecData>())
                    as *mut pg_sys::ParamExecData;
            let slot1 = vals.add(1);
            (*slot1).execPlan = ptr::null_mut();
            (*slot1).value = pg_sys::Datum::from(slot1_value);
            (*slot1).isnull = false;
            vals
        }
    }

    #[pg_test]
    fn runtime_param_resolver_uses_fetch_hook_before_flexible_array() {
        unsafe {
            let pli = pg_sys::makeParamList(0);
            (*pli).numParams = 2;
            (*pli).paramFetch = Some(fetch_param_from_workspace);
            let estate = make_estate_stub(pli);
            let refs = [ExternParamRef {
                param_id: 2,
                expected_type: pg_sys::INT4OID,
                collid: pg_sys::Oid::INVALID,
            }];

            let resolved = RuntimeParamResolver::new(estate)
                .resolve(&refs, &[])
                .expect("paramFetch-backed parameter must resolve");

            assert_eq!(resolved.len(), 1);
            assert_eq!(resolved[0].value().datum().as_raw().value(), 20);
        }
    }

    /// Colliding EXTERN/EXEC ids resolve by ParamKey, not numeric id alone.
    #[pg_test]
    fn runtime_param_resolver_mixed_colliding_ids_resolve_by_param_key() {
        unsafe {
            let pli = make_param_list_int4_value(100);
            let estate = make_estate_stub(pli);
            let exec_vals = make_param_exec_vals_slot1(200);
            (*estate).es_param_exec_vals = exec_vals;
            let extern_refs = [ExternParamRef {
                param_id: 1,
                expected_type: pg_sys::INT4OID,
                collid: pg_sys::Oid::INVALID,
            }];
            let exec_refs = [ExecParamRef {
                param_id: 1,
                expected_type: pg_sys::INT4OID,
                collid: pg_sys::Oid::INVALID,
            }];

            let extern_key = ParamKey {
                paramkind: pg_sys::ParamKind::PARAM_EXTERN,
                param_id: 1,
            };
            let exec_key = ParamKey {
                paramkind: pg_sys::ParamKind::PARAM_EXEC,
                param_id: 1,
            };
            // Same numeric id, distinct ParamKey — kind must disambiguate.
            assert_ne!(
                extern_key, exec_key,
                "colliding numeric ids must produce distinct ParamKeys \
                 (the kind disambiguates them)",
            );

            fn lookup(
                values: &[ResolvedParam],
                key: ParamKey,
            ) -> Option<&ResolvedParam> {
                values.iter().find(|v| v.key() == key)
            }

            let resolved = RuntimeParamResolver::new(estate)
                .resolve(&extern_refs, &exec_refs)
                .expect("mixed runtime parameters must resolve");
            assert_eq!(
                resolved.len(),
                2,
                "exactly one value per ParamKey: one EXTERN + one EXEC \
                 ",
            );

            let extern_val = lookup(&resolved, extern_key)
                .expect("EXTERN $1 must resolve by its ParamKey ");
            let exec_val = lookup(&resolved, exec_key)
                .expect("EXEC slot 1 must resolve by its ParamKey ");

            assert_eq!(
                extern_val.key().paramkind,
                pg_sys::ParamKind::PARAM_EXTERN,
                "the EXTERN-keyed value must be stamped PARAM_EXTERN",
            );
            assert_eq!(
                exec_val.key().paramkind,
                pg_sys::ParamKind::PARAM_EXEC,
                "the EXEC-keyed value must be stamped PARAM_EXEC",
            );
            assert_eq!(
                extern_val.value().datum().as_raw().value(),
                100,
                "EXTERN $1 must resolve to its own value (100), not the EXEC \
                 slot's value ",
            );
            assert_eq!(
                exec_val.value().datum().as_raw().value(),
                200,
                "EXEC slot 1 must resolve to its own value (200), not the \
                 EXTERN $1 value ",
            );
            // Regression guard: pre-fix bug collapsed both ids onto the EXTERN value.
            assert_ne!(
                extern_val.value().datum().as_raw().value(),
                exec_val.value().datum().as_raw().value(),
                "colliding (EXTERN $1) and (EXEC slot 1) must resolve to \
                 DISTINCT values — collapsing them to one value is the \
                 param-kind-collision data-loss bug ",
            );

            (*exec_vals.add(1)).value = pg_sys::Datum::from(300i32);

            let resolved_rescan = RuntimeParamResolver::new(estate)
                .resolve(&extern_refs, &exec_refs)
                .expect("mixed runtime parameters must re-resolve");
            assert_eq!(
                resolved_rescan.len(),
                2,
                "re-resolution still yields exactly one value per ParamKey",
            );

            let extern_val2 = lookup(&resolved_rescan, extern_key)
                .expect("EXTERN $1 must still resolve after ReScan");
            let exec_val2 = lookup(&resolved_rescan, exec_key)
                .expect("EXEC slot 1 must still resolve after ReScan");

            assert_eq!(
                exec_val2.value().datum().as_raw().value(),
                300,
                "the changed EXEC slot must re-resolve to its new value (300) \
                 by ParamKey after ReScan ",
            );
            assert_eq!(
                extern_val2.value().datum().as_raw().value(),
                100,
                "the EXTERN $1 value must be unaffected by the EXEC change — \
                 the two ParamKeys never alias ",
            );
        }
    }
}
