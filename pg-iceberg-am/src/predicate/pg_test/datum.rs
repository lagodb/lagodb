//! Backend tests for PG literal to iceberg Datum decoding.

#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_int;

    use pg_lakebase_core::expr::pg::{PgConst, PgExprRef};
    use pg_lakebase_core::expr::translator::PgLiteral;
    use pg_lakebase_core::expr::translator::PgPredicateTranslator;
    use pg_lakebase_core::expr::{ParamKey, ResolvedParam};
    use pgrx::{IntoDatum, pg_sys};

    use crate::predicate::translator::{
        IcebergPredicateTranslator, IcebergScalar, IcebergTranslationError,
        decode_datum,
    };

    /// `text` / `varchar` literals decode to iceberg `string` `Datum`s.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_text_and_varchar_build_string_datum() {
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

    /// Each integer Datum representation reaches its corresponding Iceberg
    /// scalar width without losing signed values.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_integer_datums_preserves_width_and_sign() {
        let int8_value = i64::from(i32::MIN) - 1;
        let cases = [
            (
                pg_sys::INT2OID,
                (-123_i16).into_datum().expect("int2 into_datum"),
                Datum::int(-123),
            ),
            (
                pg_sys::INT4OID,
                (-123_456_i32).into_datum().expect("int4 into_datum"),
                Datum::int(-123_456),
            ),
            (
                pg_sys::INT8OID,
                int8_value.into_datum().expect("int8 into_datum"),
                Datum::long(int8_value),
            ),
        ];

        for (type_oid, datum, expected) in cases {
            // SAFETY: every Datum was produced by IntoDatum for the integer
            // type identified by the paired PostgreSQL OID and is non-NULL.
            assert_eq!(unsafe { decode_datum(type_oid, datum) }, Ok(expected));
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

    #[pgrx::pg_test(schema = "tests")]
    fn param_value_null_decodes_to_null() {
        let mut translator = IcebergPredicateTranslator::new_unbound_for_tests();
        let null_param = unsafe {
            ResolvedParam::from_raw_parts(
                ParamKey {
                    paramkind: pg_sys::ParamKind::PARAM_EXTERN,
                    param_id: 1,
                },
                pg_sys::INT4OID,
                pg_sys::Oid::INVALID,
                pg_sys::Datum::from(0usize),
                true,
            )
        };

        assert!(matches!(
            translator.param_value(null_param.value()),
            Ok(IcebergScalar::Null { type_oid }) if type_oid == pg_sys::INT4OID
        ));
    }

    use iceberg_lite::spec::Datum;
    use pg_lakebase_core::tuple::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};

    /// One raw-Datum smoke per temporal representation. Offset arithmetic and
    /// infinity boundaries are exhaustively owned by host tests in `datum.rs`.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_temporal_datums_at_unix_epoch() {
        unsafe {
            assert_eq!(
                decode_datum(
                    pg_sys::DATEOID,
                    pg_sys::Datum::from(-PG_EPOCH_DAYS_DIFF),
                ),
                Ok(Datum::date(0)),
            );
            assert_eq!(
                decode_datum(
                    pg_sys::TIMESTAMPOID,
                    pg_sys::Datum::from(-PG_EPOCH_USECS_DIFF),
                ),
                Ok(Datum::timestamp_micros(0)),
            );
            assert_eq!(
                decode_datum(
                    pg_sys::TIMESTAMPTZOID,
                    pg_sys::Datum::from(-PG_EPOCH_USECS_DIFF),
                ),
                Ok(Datum::timestamptz_micros(0)),
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
