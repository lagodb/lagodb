//! Backend tests for PG literal to iceberg Datum decoding.

#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_int;

    use pg_lakebase_core::expr::pg::{PgConst, PgExprRef};
    use pg_lakebase_core::expr::translator::PgLiteral;
    use pg_lakebase_core::expr::translator::PgPredicateTranslator;
    use pgrx::pg_sys;

    use crate::predicate::translator::{
        IcebergPredicateTranslator, IcebergScalar, IcebergTranslationError,
        decode_datum,
    };

    /// `text` / `varchar` literals decode to iceberg `string` `Datum`s.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_text_and_varchar_build_string_datum() {
        use pgrx::IntoDatum;

        unsafe {
            let text_datum = "hello".into_datum().expect("text into_datum");
            let got = decode_datum(pg_sys::TEXTOID, text_datum)
                .expect("text must decode to a string Datum");
            assert_eq!(got, Datum::string("hello"));

            // `varchar` shares the branch (binary-coercible to `text`).
            let varchar_datum = "world".into_datum().expect("varchar into_datum");
            let got = decode_datum(pg_sys::VARCHAROID, varchar_datum)
                .expect("varchar must decode to a string Datum");
            assert_eq!(got, Datum::string("world"));
        }
    }

    /// NULL `Const` decodes to `Ok(IcebergScalar::Null { .. })`.
    #[pgrx::pg_test(schema = "tests")]
    fn literal_null_const_decodes_to_null() {
        unsafe {
            // A NULL int4 `Const`: `constisnull = true`, so `constvalue` is ignored.
            let c = pg_sys::makeConst(
                pg_sys::INT4OID,
                -1,
                pg_sys::Oid::INVALID,
                core::mem::size_of::<i32>() as c_int,
                pg_sys::Datum::from(0usize),
                true,
                true,
            );
            let expr: *mut pg_sys::Expr = c.cast();

            let leaf = PgExprRef::from_raw(expr);
            let pg_const = PgConst::try_from_expr(leaf)
                .expect("makeConst produced a T_Const node");
            let lit = PgLiteral::from_const(pg_const);
            assert!(
                lit.is_null(),
                "fixture Const must be NULL (constisnull = true)"
            );

            let mut translator = IcebergPredicateTranslator::new_unbound_for_tests();
            let result = translator.literal(lit);

            match result {
                Ok(IcebergScalar::Null { type_oid }) => {
                    assert_eq!(
                        type_oid,
                        pg_sys::INT4OID,
                        "Null scalar must carry the Const's PG type OID",
                    );
                }
                other => panic!(
                    "a NULL Const must decode to Ok(IcebergScalar::Null {{ .. }}); got {other:?}"
                ),
            }
        }
    }

    // -------------------------------------------------------------------------
    // Pass-by-value temporal decode.
    //
    // Scope note: the *happy-path* epoch-offset equivalence for date /
    // timestamp / timestamptz (decoded `Datum` == write-side stored bound) is
    // owned by the cross-crate write/translator consistency property tests in
    // `decode::epoch_consistency` (`pushed_*_bound_matches_write_side_offset`
    // and `*_epoch_consistency_at_unix_epoch`), which exercise the same
    // `decode_datum` path across the whole representable range. This module
    // keeps the ±infinity not-representable rejections excluded from those
    // property ranges.
    // -------------------------------------------------------------------------

    use iceberg_lite::spec::Datum;

    /// Build a PG `date` `Datum` directly from a raw `DateADT` (PG-epoch days).
    fn date_datum_from_raw(pg_days: i32) -> pg_sys::Datum {
        pg_sys::Datum::from(pg_days)
    }

    /// Build a PG `timestamp` / `timestamptz` `Datum` from raw PG-epoch micros.
    fn ts_datum_from_raw(pg_micros: i64) -> pg_sys::Datum {
        pg_sys::Datum::from(pg_micros)
    }

    #[pgrx::pg_test(schema = "tests")]
    fn decode_date_infinity_is_not_representable() {
        for raw in [i32::MAX, i32::MIN] {
            let datum = date_datum_from_raw(raw);
            assert!(
                matches!(
                    unsafe { decode_datum(pg_sys::DATEOID, datum) },
                    Err(IcebergTranslationError::ValueNotRepresentable { type_oid })
                        if type_oid == pg_sys::DATEOID
                ),
                "±infinity date (raw {raw}) must be ValueNotRepresentable",
            );
        }
    }

    #[pgrx::pg_test(schema = "tests")]
    fn decode_timestamp_infinity_is_not_representable() {
        for raw in [i64::MAX, i64::MIN] {
            let datum = ts_datum_from_raw(raw);
            assert!(
                matches!(
                    unsafe { decode_datum(pg_sys::TIMESTAMPOID, datum) },
                    Err(IcebergTranslationError::ValueNotRepresentable { type_oid })
                        if type_oid == pg_sys::TIMESTAMPOID
                ),
                "±infinity timestamp (raw {raw}) must be ValueNotRepresentable",
            );
        }
    }

    #[pgrx::pg_test(schema = "tests")]
    fn decode_timestamptz_infinity_is_not_representable() {
        for raw in [i64::MAX, i64::MIN] {
            let datum = ts_datum_from_raw(raw);
            assert!(
                matches!(
                    unsafe { decode_datum(pg_sys::TIMESTAMPTZOID, datum) },
                    Err(IcebergTranslationError::ValueNotRepresentable { type_oid })
                        if type_oid == pg_sys::TIMESTAMPTZOID
                ),
                "±infinity timestamptz (raw {raw}) must be ValueNotRepresentable",
            );
        }
    }

    #[pgrx::pg_test(schema = "tests")]
    fn decode_unsupported_type_is_rejected() {
        let datum = pg_sys::Datum::from(1usize);
        assert!(matches!(
            unsafe { decode_datum(pg_sys::BOOLOID, datum) },
            Err(IcebergTranslationError::UnsupportedType { type_oid })
                if type_oid == pg_sys::BOOLOID
        ));
    }
}
