//! Backend tests for PG literal to iceberg Datum decoding.

#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_int;

    use pg_lakebase_core::expr::nodes::{PgConst, PgExprRef, PgLiteral};
    use pg_lakebase_core::expr::translator::PgPredicateTranslator;
    use pgrx::pg_sys;

    use crate::customscan::predicate_translator::IcebergDatumDecoder;
    use crate::customscan::{
        IcebergPredicateTranslator, IcebergScalar, IcebergTranslationError,
    };

    /// In-range `numeric` decodes to an iceberg decimal `Datum`.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_numeric_in_range_builds_decimal_datum() {
        use iceberg_lite::spec::Datum;
        use pgrx::IntoDatum;
        use pgrx::prelude::AnyNumeric;
        use rust_decimal::Decimal;

        unsafe {
            let numeric = AnyNumeric::try_from(100.5_f64).expect("100.5 is valid");
            let datum = numeric.clone().into_datum().expect("numeric into_datum");

            let got = IcebergDatumDecoder::decode(pg_sys::NUMERICOID, datum)
                .expect("an in-range numeric must decode to a decimal Datum");

            // Independent oracle: PG canonical text -> rust_decimal -> Datum.
            let expected_decimal =
                Decimal::from_str_exact(numeric.normalize()).expect("decimal parse");
            let expected =
                Datum::decimal(expected_decimal).expect("decimal Datum builds");

            assert_eq!(got, expected, "numeric must decode to the scaled decimal");
        }
    }

    /// Negative fractional `numeric` decodes through the scaled-`i128` path.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_numeric_negative_fraction_round_trips() {
        use iceberg_lite::spec::Datum;
        use pgrx::IntoDatum;
        use pgrx::prelude::AnyNumeric;
        use rust_decimal::Decimal;

        unsafe {
            let numeric =
                AnyNumeric::try_from("-12345.6789").expect("valid numeric literal");
            let datum = numeric.clone().into_datum().expect("numeric into_datum");

            let got = IcebergDatumDecoder::decode(pg_sys::NUMERICOID, datum)
                .expect("a negative fractional numeric must decode");

            let expected_decimal =
                Decimal::from_str_exact(numeric.normalize()).expect("decimal parse");
            let expected =
                Datum::decimal(expected_decimal).expect("decimal Datum builds");
            assert_eq!(got, expected);
        }
    }

    /// `numeric 'NaN'` decodes to `ValueNotRepresentable`: NaN has no Iceberg
    /// decimal ordering, so the decoder refuses it instead of producing a bound.
    /// This is decoder-level defense-in-depth — numeric comparison pushdown is
    /// currently disabled (`NUMERIC_COMPARISON_PUSHDOWN_ENABLED`), so the
    /// production path never reaches this decode; the guard keeps the decoder
    /// correct for when numeric pushdown is re-enabled.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_numeric_nan_is_not_representable() {
        use pgrx::IntoDatum;
        use pgrx::prelude::AnyNumeric;

        unsafe {
            let nan = AnyNumeric::try_from("NaN").expect("NaN is a valid numeric");
            assert!(nan.is_nan(), "sanity: the literal must be NaN");
            let datum = nan.into_datum().expect("numeric NaN into_datum");

            let got = IcebergDatumDecoder::decode(pg_sys::NUMERICOID, datum);
            assert!(
                matches!(
                    got,
                    Err(IcebergTranslationError::ValueNotRepresentable { type_oid })
                        if type_oid == pg_sys::NUMERICOID
                ),
                "numeric NaN must be ValueNotRepresentable, got {got:?}",
            );
        }
    }

    /// Out-of-range `numeric` → `ValueNotRepresentable`.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_numeric_out_of_range_is_not_representable() {
        use pgrx::IntoDatum;
        use pgrx::prelude::AnyNumeric;

        unsafe {
            // 40 nines: well beyond rust_decimal's ~28-29 significant digits and
            // beyond Decimal128, but a perfectly valid PG numeric.
            let huge_text = "9".repeat(40);
            let huge = AnyNumeric::try_from(huge_text.as_str())
                .expect("a 40-digit integer is a valid PG numeric");
            let datum = huge.into_datum().expect("numeric into_datum");

            let got = IcebergDatumDecoder::decode(pg_sys::NUMERICOID, datum);
            assert!(
                matches!(
                    got,
                    Err(IcebergTranslationError::ValueNotRepresentable { type_oid })
                        if type_oid == pg_sys::NUMERICOID
                ),
                "an out-of-range numeric must be ValueNotRepresentable, got {got:?}",
            );
        }
    }

    /// `text` / `varchar` literals decode to iceberg `string` `Datum`s.
    #[pgrx::pg_test(schema = "tests")]
    fn decode_text_and_varchar_build_string_datum() {
        use iceberg_lite::spec::Datum;
        use pgrx::IntoDatum;

        unsafe {
            let text_datum = "hello".into_datum().expect("text into_datum");
            let got = IcebergDatumDecoder::decode(pg_sys::TEXTOID, text_datum)
                .expect("text must decode to a string Datum");
            assert_eq!(got, Datum::string("hello"));

            // `varchar` shares the branch (binary-coercible to `text`).
            let varchar_datum = "world".into_datum().expect("varchar into_datum");
            let got = IcebergDatumDecoder::decode(pg_sys::VARCHAROID, varchar_datum)
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
                lit.is_null,
                "fixture Const must be NULL (constisnull = true)"
            );

            let mut translator = IcebergPredicateTranslator::new();
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
}
