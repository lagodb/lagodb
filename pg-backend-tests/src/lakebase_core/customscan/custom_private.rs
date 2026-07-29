//! Backend tests for customscan `custom_private` encode/decode round-trips.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {

    use core::ffi::CStr;
    use std::ptr;

    use crate::lakebase_core::support::pg::{INT4_EQ_OPNO, PgNodeBuilder};
    use pg_lakebase_core::customscan::custom_private::{
        assert_provider_name_matches, decode_private, encode_split,
    };
    use pg_lakebase_core::customscan::provider::NeededColumns;
    use pg_lakebase_core::diag::ReportableError;
    use pg_lakebase_core::expr::PushdownContract;
    use pg_lakebase_core::expr::{ColumnRef, PredicateBuilder, ResolvedParam};
    use pgrx::pg_sys;
    use pgrx::pg_test;

    /// `T_List` of one `T_Integer` (42) and one `T_String` ("generic-metadata").
    unsafe fn make_provider_metadata() -> *mut pg_sys::List {
        unsafe {
            let mut list: *mut pg_sys::List = ptr::null_mut();

            let int_node = pg_sys::makeInteger(42);
            list = pg_sys::lappend(list, int_node.cast());

            // `makeString` does not copy — pstrdup first.
            let raw = c"generic-metadata".as_ptr();
            let owned = pg_sys::pstrdup(raw);
            let str_node = pg_sys::makeString(owned);
            list = pg_sys::lappend(list, str_node.cast());

            list
        }
    }

    unsafe fn replace_tuple_layout_wire(
        envelope: *mut pg_sys::List,
        kind: i32,
        attnos: &[pg_sys::AttrNumber],
    ) {
        let mut wire: *mut pg_sys::List = ptr::null_mut();
        wire = unsafe { pg_sys::lappend_int(wire, kind) };
        for &attno in attnos {
            wire = unsafe { pg_sys::lappend_int(wire, attno as i32) };
        }
        let cell = unsafe { pg_sys::list_nth_cell(envelope, 8) };
        assert!(!cell.is_null(), "tuple-layout envelope cell must exist");
        unsafe {
            (*cell).ptr_value = wire.cast();
        }
    }

    fn synthetic_inputs() -> (
        pg_sys::Oid,
        usize,
        usize,
        Vec<PushdownContract>,
        Vec<ColumnRef>,
    ) {
        let relation_oid = pg_sys::Oid::from(16_384u32);
        let pushed_count = 2usize;
        let recheck_count = 1usize;
        let pushed_contracts = vec![
            PushdownContract::ExactRowFilter,
            PushdownContract::ConservativePruning,
        ];
        let column_refs = vec![
            ColumnRef {
                expr_index: 0,
                rel_oid: relation_oid,
                attno: 1,
                atttypid: pg_sys::Oid::from(23u32),
                attcollation: pg_sys::Oid::INVALID,
                name: Some("col_a".to_string()),
            },
            ColumnRef {
                expr_index: 0,
                rel_oid: relation_oid,
                attno: 2,
                atttypid: pg_sys::Oid::from(25u32),
                attcollation: pg_sys::Oid::from(100u32),
                name: Some("col_b".to_string()),
            },
            ColumnRef {
                expr_index: 1,
                rel_oid: relation_oid,
                attno: 3,
                atttypid: pg_sys::Oid::from(20u32),
                attcollation: pg_sys::Oid::INVALID,
                name: Some("col_c".to_string()),
            },
        ];
        (
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
        )
    }

    #[pg_test]
    fn custom_private_encode_decode_copyobject_roundtrip() {
        let provider_name = c"core-private-roundtrip-provider";
        let (
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
        ) = synthetic_inputs();

        unsafe {
            let provider_metadata = make_provider_metadata();
            let original = encode_split(
                provider_name,
                relation_oid,
                pushed_count,
                recheck_count,
                &pushed_contracts,
                &column_refs,
                provider_metadata,
            )
            .expect("encode_split: synthetic counts are within i32::MAX");
            assert!(!original.is_null(), "encode_split returned NULL");

            let copied = pg_sys::copyObjectImpl(original.cast()) as *mut pg_sys::List;
            assert!(!copied.is_null(), "copyObjectImpl returned NULL");

            let decoded = decode_private(copied).report_unwrap();

            assert_eq!(
                decoded.provider_id_or_name.as_c_str(),
                CStr::from_ptr(provider_name.as_ptr()),
                "provider_id_or_name did not round-trip",
            );

            assert_eq!(
                decoded.relation_oid, relation_oid,
                "relation_oid did not round-trip",
            );

            assert_eq!(
                decoded.pushed_count, pushed_count,
                "pushed_count did not round-trip",
            );
            assert_eq!(
                decoded.recheck_count, recheck_count,
                "recheck_count did not round-trip",
            );

            assert_eq!(
                decoded.pushed_contracts, pushed_contracts,
                "pushed_contracts did not round-trip",
            );

            assert_eq!(
                decoded.column_refs, column_refs,
                "column_refs did not round-trip",
            );

            let raw = decoded.provider_metadata_raw;
            assert!(!raw.is_null(), "provider_metadata_raw was NULL after copy");
            assert_eq!((*raw).type_, pg_sys::NodeTag::T_List);
            assert_eq!((*raw).length, 2);

            let cell0 = pg_sys::list_nth(raw, 0) as *mut pg_sys::Node;
            assert!(!cell0.is_null());
            assert_eq!((*cell0).type_, pg_sys::NodeTag::T_Integer);
            let int_node = cell0.cast::<pg_sys::Integer>();
            assert_eq!((*int_node).ival, 42);

            let cell1 = pg_sys::list_nth(raw, 1) as *mut pg_sys::Node;
            assert!(!cell1.is_null());
            assert_eq!((*cell1).type_, pg_sys::NodeTag::T_String);
            let str_node = cell1.cast::<pg_sys::String>();
            let sval = (*str_node).sval;
            assert!(!sval.is_null());
            assert_eq!(CStr::from_ptr(sval), c"generic-metadata");
        }
    }

    /// The top-level envelope includes the framework tuple-layout contract.
    #[pg_test]
    fn custom_private_envelope_structural_identity() {
        let provider_name = c"envelope-shape-test-provider";
        let (
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
        ) = synthetic_inputs();

        unsafe {
            let provider_metadata = make_provider_metadata();
            let top = encode_split(
                provider_name,
                relation_oid,
                pushed_count,
                recheck_count,
                &pushed_contracts,
                &column_refs,
                provider_metadata,
            )
            .expect("encode_split: synthetic counts are within i32::MAX");
            assert!(!top.is_null(), "encode_split returned NULL");
            assert_eq!(
                (*top).type_,
                pg_sys::NodeTag::T_List,
                "top-level custom_private must be a T_List",
            );
            assert_eq!(
                (*top).length,
                9,
                "top-level custom_private must keep the 9-cell envelope layout",
            );

            let cell_tag = |i: i32| -> pg_sys::NodeTag {
                let node = pg_sys::list_nth(top, i) as *mut pg_sys::Node;
                assert!(!node.is_null(), "envelope cell {i} must be non-NULL");
                (*node).type_
            };

            assert_eq!(cell_tag(0), pg_sys::NodeTag::T_String);
            assert_eq!(cell_tag(1), pg_sys::NodeTag::T_Integer);
            assert_eq!(cell_tag(2), pg_sys::NodeTag::T_Integer);
            assert_eq!(cell_tag(3), pg_sys::NodeTag::T_Integer);
            assert_eq!(cell_tag(4), pg_sys::NodeTag::T_Integer);
            assert_eq!(cell_tag(5), pg_sys::NodeTag::T_IntList);
            assert_eq!(cell_tag(6), pg_sys::NodeTag::T_List);
            assert_eq!(cell_tag(7), pg_sys::NodeTag::T_List);
            assert_eq!(cell_tag(8), pg_sys::NodeTag::T_IntList);

            let metadata = pg_sys::list_nth(top, 7) as *mut pg_sys::List;
            assert!(
                !metadata.is_null(),
                "provider_metadata cell must be non-NULL"
            );
            assert_eq!(
                (*metadata).length,
                2,
                "generic provider metadata fixture must remain a 2-cell list",
            );

            let copied = pg_sys::copyObjectImpl(top.cast()) as *mut pg_sys::List;
            assert!(!copied.is_null(), "copyObjectImpl returned NULL");
            let copied_name = pg_sys::list_nth(copied, 0) as *mut pg_sys::String;
            assert_eq!(
                CStr::from_ptr((*copied_name).sval),
                provider_name,
                "provider name cell must survive copyObject unchanged",
            );

            let decoded = decode_private(copied).report_unwrap();
            assert_eq!(
                decoded.tuple_layout.required_columns(),
                NeededColumns::All,
                "legacy encode_split must carry a semantic relation layout",
            );
        }
    }

    #[pg_test]
    fn tuple_layout_projected_and_relation_pruned_wire_decode() {
        let provider_name = c"tuple-layout-wire-provider";
        let (
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
        ) = synthetic_inputs();

        for (kind, expected) in [(1, &[3, 1][..]), (2, &[1, 4][..]), (2, &[][..])] {
            unsafe {
                let envelope = encode_split(
                    provider_name,
                    relation_oid,
                    pushed_count,
                    recheck_count,
                    &pushed_contracts,
                    &column_refs,
                    ptr::null_mut(),
                )
                .expect("encode_split failed");
                replace_tuple_layout_wire(envelope, kind, expected);

                let copied =
                    pg_sys::copyObjectImpl(envelope.cast()) as *mut pg_sys::List;
                let decoded = decode_private(copied).report_unwrap();
                assert_eq!(
                    decoded.tuple_layout.required_columns(),
                    NeededColumns::Subset(expected),
                );
            }
        }
    }

    #[pg_test]
    fn tuple_layout_wire_rejects_malformed_shapes() {
        let provider_name = c"tuple-layout-malformed-provider";
        let (
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
        ) = synthetic_inputs();

        for (kind, attnos, expected_error) in [
            (99, &[][..], "unknown kind tag 99"),
            (1, &[][..], "projected base layout is empty"),
            (1, &[0][..], "invalid value 0"),
            (1, &[2, 2][..], "duplicate base attno 2"),
        ] {
            unsafe {
                let envelope = encode_split(
                    provider_name,
                    relation_oid,
                    pushed_count,
                    recheck_count,
                    &pushed_contracts,
                    &column_refs,
                    ptr::null_mut(),
                )
                .expect("encode_split failed");
                replace_tuple_layout_wire(envelope, kind, attnos);

                let error =
                    decode_private(envelope).expect_err("malformed layout must fail");
                assert!(
                    error.to_string().contains(expected_error),
                    "unexpected decode error for kind={kind}, attnos={attnos:?}: {error}",
                );
            }
        }
    }

    /// `ColumnRef.name` round-trips: `Some("named_col")` and `None`.
    #[pg_test]
    fn column_ref_name_survives_wire_roundtrip() {
        let provider_name = c"core-column-ref-name-provider";
        let relation_oid = pg_sys::Oid::from(16_384u32);
        let pushed_count = 2usize;
        let recheck_count = 0usize;
        let pushed_contracts = vec![
            PushdownContract::ExactRowFilter,
            PushdownContract::ExactRowFilter,
        ];

        let named = ColumnRef {
            expr_index: 0,
            rel_oid: relation_oid,
            attno: 1,
            atttypid: pg_sys::Oid::from(23u32),
            attcollation: pg_sys::Oid::INVALID,
            name: Some("named_col".to_string()),
        };
        let unnamed = ColumnRef {
            expr_index: 1,
            rel_oid: relation_oid,
            attno: 2,
            atttypid: pg_sys::Oid::from(25u32),
            attcollation: pg_sys::Oid::INVALID,
            name: None,
        };
        let column_refs = vec![named.clone(), unnamed.clone()];

        unsafe {
            let original = encode_split(
                provider_name,
                relation_oid,
                pushed_count,
                recheck_count,
                &pushed_contracts,
                &column_refs,
                ptr::null_mut(),
            )
            .expect("encode_split: synthetic counts are within i32::MAX");
            assert!(!original.is_null(), "encode_split returned NULL");

            let copied = pg_sys::copyObjectImpl(original.cast()) as *mut pg_sys::List;
            assert!(!copied.is_null(), "copyObjectImpl returned NULL");

            let decoded = decode_private(copied).report_unwrap();

            assert_eq!(
                decoded.column_refs.len(),
                2,
                "expected exactly two decoded column_refs",
            );

            let decoded_named = &decoded.column_refs[0];
            assert_eq!(
                decoded_named.name,
                Some("named_col".to_string()),
                "Some(name) did not round-trip to the same string",
            );
            assert_eq!(*decoded_named, named, "named ColumnRef did not round-trip",);

            let decoded_unnamed = &decoded.column_refs[1];
            assert_eq!(
                decoded_unnamed.name, None,
                "None name did not round-trip as None",
            );
            assert_eq!(
                *decoded_unnamed, unnamed,
                "unnamed ColumnRef did not round-trip",
            );
        }
    }

    #[derive(Debug)]
    struct CountingTranslatorError;

    impl core::fmt::Display for CountingTranslatorError {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("counting translator error")
        }
    }

    impl std::error::Error for CountingTranslatorError {}

    /// Mirrors Iceberg `resolve_column_name`: uses carried name or falls back to `get_attname`.
    struct CountingTranslator {
        get_attname_calls: usize,
    }

    impl pg_lakebase_core::expr::translator::PgPredicateTranslator
        for CountingTranslator
    {
        type Scalar = String;
        type Predicate = String;
        type Error = CountingTranslatorError;

        fn column(
            &mut self,
            col: pg_lakebase_core::expr::translator::PgColumnRef<'_>,
        ) -> Result<String, CountingTranslatorError> {
            if let Some(name) = col.name {
                return Ok(name.to_string());
            }

            self.get_attname_calls += 1;
            // SAFETY: temp-table OID; `missing_ok = true` avoids ereport on stale OID.
            let raw = unsafe {
                pg_sys::get_attname(col.rel_oid, col.attno, /*missing_ok=*/ true)
            };
            assert!(
                !raw.is_null(),
                "get_attname returned NULL on the fallback path \
                 (rel_oid={:?}, attno={})",
                col.rel_oid,
                col.attno,
            );
            let name = unsafe { CStr::from_ptr(raw) }
                .to_str()
                .expect("column name is valid UTF-8")
                .to_string();
            Ok(name)
        }

        fn literal(
            &mut self,
            _lit: pg_lakebase_core::expr::translator::PgLiteral<'_>,
        ) -> Result<String, CountingTranslatorError> {
            Ok("lit".to_string())
        }

        fn param_value(
            &mut self,
            _param: pg_lakebase_core::expr::translator::PgParamValue<'_>,
        ) -> Result<String, CountingTranslatorError> {
            Ok("param".to_string())
        }

        fn comparison(
            &mut self,
            _op: pg_lakebase_core::expr::translator::PgComparisonOp,
            left: String,
            right: String,
        ) -> Result<String, CountingTranslatorError> {
            Ok(format!("cmp({left},{right})"))
        }

        fn is_null(
            &mut self,
            value: String,
        ) -> Result<String, CountingTranslatorError> {
            Ok(format!("is_null[{value}]"))
        }

        fn is_not_null(
            &mut self,
            value: String,
        ) -> Result<String, CountingTranslatorError> {
            Ok(format!("is_not_null[{value}]"))
        }

        fn and(
            &mut self,
            items: Vec<String>,
        ) -> Result<String, CountingTranslatorError> {
            Ok(format!("and({})", items.join(",")))
        }

        fn or(
            &mut self,
            items: Vec<String>,
        ) -> Result<String, CountingTranslatorError> {
            Ok(format!("or({})", items.join(",")))
        }

        fn not(&mut self, item: String) -> Result<String, CountingTranslatorError> {
            Ok(format!("not({item})"))
        }
    }

    const COUNT_SCAN_RELID: core::ffi::c_int = 1;

    struct CountExprFixture;

    impl CountExprFixture {
        fn nodes() -> PgNodeBuilder {
            PgNodeBuilder::new(COUNT_SCAN_RELID)
        }

        unsafe fn int4_eq(
            attno: pg_sys::AttrNumber,
            value: i32,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_var_op_const(INT4_EQ_OPNO, attno, value) }
        }
    }

    fn count_lookup_relation_oid(qualified_name: &str) -> pg_sys::Oid {
        let raw = pgrx::Spi::get_one::<i64>(&format!(
            "SELECT '{qualified_name}'::regclass::oid::int8"
        ))
        .expect("regclass cast failed")
        .expect("regclass returned NULL");
        pg_sys::Oid::from(raw as u32)
    }

    /// Carried names skip `get_attname`; `None` triggers exactly one lookup per ref.
    #[pg_test]
    fn column_ref_name_bounds_get_attname_lookups_per_scan() {
        pgrx::Spi::run("CREATE TEMP TABLE col_ref_count_t(a int4, b int4)")
            .expect("CREATE TEMP TABLE failed");
        let rel_oid = count_lookup_relation_oid("pg_temp.col_ref_count_t");

        unsafe {
            let exprs: Vec<*mut pg_sys::Expr> = vec![
                CountExprFixture::int4_eq(/* attno (a) */ 1, 1),
                CountExprFixture::int4_eq(/* attno (b) */ 2, 2),
            ];

            let resolved_params: Vec<ResolvedParam> = Vec::new();

            let carried_refs = vec![
                ColumnRef {
                    expr_index: 0,
                    rel_oid,
                    attno: 1,
                    atttypid: pg_sys::INT4OID,
                    attcollation: pg_sys::Oid::INVALID,
                    name: Some("a".to_string()),
                },
                ColumnRef {
                    expr_index: 1,
                    rel_oid,
                    attno: 2,
                    atttypid: pg_sys::INT4OID,
                    attcollation: pg_sys::Oid::INVALID,
                    name: Some("b".to_string()),
                },
            ];

            let mut carried_translator = CountingTranslator {
                get_attname_calls: 0,
            };
            let carried_predicates = {
                let mut builder = PredicateBuilder::new(
                    &mut carried_translator,
                    &exprs,
                    &carried_refs,
                    &resolved_params,
                    COUNT_SCAN_RELID,
                );
                builder
                    .build_all()
                    .expect("build_all failed on the carried-name scan")
            };

            assert_eq!(
                carried_predicates.len(),
                2,
                "expected one native predicate per pushed expression",
            );
            assert_eq!(
                carried_translator.get_attname_calls, 0,
                "carried ColumnRef.name must short-circuit get_attname \
                 entirely (expected 0 lookups, found {})",
                carried_translator.get_attname_calls,
            );

            let uncarried_refs = vec![
                ColumnRef {
                    expr_index: 0,
                    rel_oid,
                    attno: 1,
                    atttypid: pg_sys::INT4OID,
                    attcollation: pg_sys::Oid::INVALID,
                    name: None,
                },
                ColumnRef {
                    expr_index: 1,
                    rel_oid,
                    attno: 2,
                    atttypid: pg_sys::INT4OID,
                    attcollation: pg_sys::Oid::INVALID,
                    name: None,
                },
            ];

            let mut uncarried_translator = CountingTranslator {
                get_attname_calls: 0,
            };
            let uncarried_predicates = {
                let mut builder = PredicateBuilder::new(
                    &mut uncarried_translator,
                    &exprs,
                    &uncarried_refs,
                    &resolved_params,
                    COUNT_SCAN_RELID,
                );
                builder
                    .build_all()
                    .expect("build_all failed on the fallback scan")
            };

            assert_eq!(
                uncarried_predicates.len(),
                2,
                "expected one native predicate per pushed expression",
            );
            assert_eq!(
                uncarried_translator.get_attname_calls,
                uncarried_refs.len(),
                "fallback path must call get_attname exactly once per column \
                 ref per scan (expected {}, found {})",
                uncarried_refs.len(),
                uncarried_translator.get_attname_calls,
            );
            assert!(
                uncarried_translator.get_attname_calls <= uncarried_refs.len(),
                "get_attname must never run more than once per column ref \
                 per scan",
            );
        }
    }

    #[pg_test(
        error = "customscan: provider name mismatch in custom_private (expected \"bar\", found \"foo\"); this indicates a corrupt plan tree or a stale cached plan referencing a renamed provider"
    )]
    fn assert_provider_name_matches_boundary_raises_on_mismatch() {
        let found = c"foo";
        let expected = c"bar";
        assert_provider_name_matches(found, expected).report_unwrap();
        panic!(
            "assert_provider_name_matches returned instead of raising \
             ereport(ERROR) for a provider-name mismatch"
        );
    }
}
