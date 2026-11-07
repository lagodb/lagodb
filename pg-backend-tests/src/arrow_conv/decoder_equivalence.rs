//! Backend tests for the standard scalar read path
//! (`PgDatumArrayReader::bind` + `datum_unchecked`, `Arrow → Datum`).
//!
//! These cannot run as host `#[test]`s: the bound reader dispatches over every
//! leaf conversion, so its compiled body references PostgreSQL symbols such as
//! the numeric decode and array construction. Linking those into an ordinary
//! Linux test binary fails with `undefined symbol: PG_exception_stack` (and
//! friends), so the tests must run inside a live backend as `#[pg_test]`s. The
//! pure resolution-table / codec-math tests stay as host tests in
//! `lagodb-arrow/tests/`.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::fmt::Debug;
    use std::sync::Arc;

    use arrow_array::builder::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, ListBuilder, StringBuilder, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    };
    use arrow_array::types::{Int16Type, Int32Type};
    use arrow_array::{Array, ArrayRef, ListArray};
    use lagodb_arrow::PgDatumArrayReader;
    use lagodb_core::datum::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
    use pgrx::pg_sys;
    use pgrx::prelude::*;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, TestCaseError, TestRunner};

    /// A generated column: the target PG type, the built array, and the
    /// expected value per row (None = null).
    #[derive(Debug)]
    struct GeneratedColumn {
        oid: pg_sys::Oid,
        array: ArrayRef,
        expected: Vec<Option<ExpectedValue>>,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum ExpectedValue {
        Bool(bool),
        I32(i32),
        I64(i64),
        F32(f32),
        F64(f64),
        Date(i32),      // pg-epoch days
        Time(i64),      // microseconds
        Timestamp(i64), // pg-epoch microseconds
    }

    impl ExpectedValue {
        /// Compare a decoded pass-by-value datum. Primitive types decode
        /// through pgrx's `FromDatum`; temporal types compare their PostgreSQL
        /// `DateADT` / `TimeADT` / `Timestamp` integer representation.
        fn matches_datum(&self, datum: pg_sys::Datum) -> bool {
            // SAFETY: the reader produced a non-null datum of the column's OID.
            unsafe {
                match self {
                    Self::Bool(v) => bool::from_datum(datum, false) == Some(*v),
                    Self::I32(v) => i32::from_datum(datum, false) == Some(*v),
                    Self::I64(v) => i64::from_datum(datum, false) == Some(*v),
                    Self::F32(v) => f32::from_datum(datum, false)
                        .is_some_and(|d| d.to_bits() == v.to_bits()),
                    Self::F64(v) => f64::from_datum(datum, false)
                        .is_some_and(|d| d.to_bits() == v.to_bits()),
                    Self::Date(pg_days) => datum.value() as i32 == *pg_days,
                    Self::Time(micros) | Self::Timestamp(micros) => {
                        datum.value() as i64 == *micros
                    }
                }
            }
        }
    }

    fn gen_bool_column(values: Vec<Option<bool>>) -> GeneratedColumn {
        let mut builder = BooleanBuilder::with_capacity(values.len());
        let expected: Vec<Option<ExpectedValue>> =
            values.iter().map(|v| v.map(ExpectedValue::Bool)).collect();
        for v in &values {
            match v {
                Some(b) => builder.append_value(*b),
                None => builder.append_null(),
            }
        }
        GeneratedColumn {
            oid: pg_sys::BOOLOID,
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn gen_i32_column(values: Vec<Option<i32>>) -> GeneratedColumn {
        let mut builder = Int32Builder::with_capacity(values.len());
        let expected: Vec<Option<ExpectedValue>> =
            values.iter().map(|v| v.map(ExpectedValue::I32)).collect();
        for v in &values {
            match v {
                Some(x) => builder.append_value(*x),
                None => builder.append_null(),
            }
        }
        GeneratedColumn {
            oid: pg_sys::INT4OID,
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn gen_i64_column(values: Vec<Option<i64>>) -> GeneratedColumn {
        let mut builder = Int64Builder::with_capacity(values.len());
        let expected: Vec<Option<ExpectedValue>> =
            values.iter().map(|v| v.map(ExpectedValue::I64)).collect();
        for v in &values {
            match v {
                Some(x) => builder.append_value(*x),
                None => builder.append_null(),
            }
        }
        GeneratedColumn {
            oid: pg_sys::INT8OID,
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn gen_f32_column(values: Vec<Option<f32>>) -> GeneratedColumn {
        let mut builder = Float32Builder::with_capacity(values.len());
        let expected: Vec<Option<ExpectedValue>> =
            values.iter().map(|v| v.map(ExpectedValue::F32)).collect();
        for v in &values {
            match v {
                Some(x) => builder.append_value(*x),
                None => builder.append_null(),
            }
        }
        GeneratedColumn {
            oid: pg_sys::FLOAT4OID,
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn gen_f64_column(values: Vec<Option<f64>>) -> GeneratedColumn {
        let mut builder = Float64Builder::with_capacity(values.len());
        let expected: Vec<Option<ExpectedValue>> =
            values.iter().map(|v| v.map(ExpectedValue::F64)).collect();
        for v in &values {
            match v {
                Some(x) => builder.append_value(*x),
                None => builder.append_null(),
            }
        }
        GeneratedColumn {
            oid: pg_sys::FLOAT8OID,
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn gen_date32_column(values: Vec<Option<i32>>) -> GeneratedColumn {
        let mut builder = Date32Builder::with_capacity(values.len());
        let expected: Vec<Option<ExpectedValue>> = values
            .iter()
            .map(|v| v.map(|d| ExpectedValue::Date(d - PG_EPOCH_DAYS_DIFF)))
            .collect();
        for v in &values {
            match v {
                Some(x) => builder.append_value(*x),
                None => builder.append_null(),
            }
        }
        GeneratedColumn {
            oid: pg_sys::DATEOID,
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn gen_time64_column(values: Vec<Option<i64>>) -> GeneratedColumn {
        let mut builder = Time64MicrosecondBuilder::with_capacity(values.len());
        let expected: Vec<Option<ExpectedValue>> =
            values.iter().map(|v| v.map(ExpectedValue::Time)).collect();
        for v in &values {
            match v {
                Some(x) => builder.append_value(*x),
                None => builder.append_null(),
            }
        }
        GeneratedColumn {
            oid: pg_sys::TIMEOID,
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn gen_timestamp_column(values: Vec<Option<i64>>) -> GeneratedColumn {
        let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
        let expected: Vec<Option<ExpectedValue>> = values
            .iter()
            .map(|v| {
                v.map(|unix_us| {
                    ExpectedValue::Timestamp(unix_us - PG_EPOCH_USECS_DIFF)
                })
            })
            .collect();
        for v in &values {
            match v {
                Some(x) => builder.append_value(*x),
                None => builder.append_null(),
            }
        }
        GeneratedColumn {
            oid: pg_sys::TIMESTAMPOID,
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn arb_nullable<T: Clone + Debug + 'static>(
        inner: impl Strategy<Value = T>,
    ) -> impl Strategy<Value = Vec<Option<T>>> {
        prop::collection::vec(prop::option::of(inner), 1..=16)
    }

    /// Unix-epoch days that survive the PG-epoch subtraction and Date::try_from.
    fn arb_date32() -> impl Strategy<Value = i32> {
        let lo = PG_EPOCH_DAYS_DIFF + (-2_000_000);
        let hi = PG_EPOCH_DAYS_DIFF + 2_000_000;
        lo..=hi
    }

    /// Microseconds within a day: [0, 86_400_000_000).
    fn arb_time64_micros() -> impl Strategy<Value = i64> {
        0i64..86_400_000_000i64
    }

    /// Unix-epoch microseconds that survive conversion to PG-epoch.
    fn arb_timestamp_micros() -> impl Strategy<Value = i64> {
        let lo = PG_EPOCH_USECS_DIFF + (-200_000_000_000_000i64);
        let hi = PG_EPOCH_USECS_DIFF + 200_000_000_000_000i64;
        lo..=hi
    }

    fn arb_generated_column() -> impl Strategy<Value = GeneratedColumn> {
        prop_oneof![
            arb_nullable(any::<bool>()).prop_map(gen_bool_column),
            arb_nullable(any::<i32>()).prop_map(gen_i32_column),
            arb_nullable(any::<i64>()).prop_map(gen_i64_column),
            arb_nullable(any::<f32>()).prop_map(gen_f32_column),
            arb_nullable(any::<f64>()).prop_map(gen_f64_column),
            arb_nullable(arb_date32()).prop_map(gen_date32_column),
            arb_nullable(arb_time64_micros()).prop_map(gen_time64_column),
            arb_nullable(arb_timestamp_micros()).prop_map(gen_timestamp_column),
        ]
    }

    fn check_extract(col: GeneratedColumn) -> Result<(), TestCaseError> {
        let reader = PgDatumArrayReader::bind(col.array.as_ref(), col.oid)
            .map_err(|e| TestCaseError::fail(format!("bind error: {e:?}")))?;
        for (row_idx, expected) in col.expected.iter().enumerate() {
            // SAFETY: `row_idx` is within the bound array, and every generated
            // type is pass-by-value.
            let datum = unsafe { reader.datum_unchecked(row_idx) }.map_err(|e| {
                TestCaseError::fail(format!("read error at row {row_idx}: {e:?}"))
            })?;
            match (expected, datum) {
                (None, None) => {}
                (Some(exp), Some(datum)) => prop_assert!(
                    exp.matches_datum(datum),
                    "row {}: expected {:?}, got raw datum {:#x}",
                    row_idx,
                    exp,
                    datum.value()
                ),
                (None, Some(_)) => {
                    prop_assert!(false, "row {row_idx}: expected null")
                }
                (Some(exp), None) => prop_assert!(
                    false,
                    "row {row_idx}: expected {exp:?} but got null"
                ),
            }
        }
        Ok(())
    }

    // Pins the bound scalar decode across all supported fixed-width types and
    // random nullability.
    #[pg_test]
    fn extract_produces_expected_datum() {
        let config = Config {
            cases: 256,
            failure_persistence: None,
            ..Config::default()
        };
        let mut runner = TestRunner::new(config);
        runner
            .run(&arb_generated_column(), check_extract)
            .expect("extract must produce the expected datum");
    }

    /// Decode every row of a native list column into its PostgreSQL array
    /// type and read the result back through pgrx's array `FromDatum`.
    fn read_list<V: FromDatum>(
        array: &ArrayRef,
        array_oid: pg_sys::Oid,
    ) -> Vec<Option<V>> {
        let reader =
            PgDatumArrayReader::bind(array.as_ref(), array_oid).expect("bind");
        (0..array.len())
            .map(|row| {
                // SAFETY: `row` is within the bound array; the array datum is
                // allocated in this test's current memory context.
                unsafe {
                    reader
                        .datum_unchecked(row)
                        .expect("read")
                        .and_then(|datum| V::from_datum(datum, false))
                }
            })
            .collect()
    }

    // A populated row keeps its interior NULL element, and a present-but-empty
    // row yields an empty array rather than a NULL row.
    #[pg_test]
    fn extract_int4_list_keeps_values_and_interior_null() {
        let array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None, Some(3)]),
                Some(vec![]),
                None,
            ]));
        assert_eq!(
            read_list::<Vec<Option<i32>>>(&array, pg_sys::INT4ARRAYOID),
            vec![Some(vec![Some(1), None, Some(3)]), Some(vec![]), None]
        );
    }

    #[pg_test]
    fn extract_int16_list_into_int2_array() {
        let array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int16Type, _, _>(vec![
                Some(vec![Some(7i16), None, Some(9)]),
            ]));
        assert_eq!(
            read_list::<Vec<Option<i16>>>(&array, pg_sys::INT2ARRAYOID),
            vec![Some(vec![Some(7), None, Some(9)])]
        );
    }

    #[pg_test]
    fn extract_bool_and_text_lists() {
        let mut bb = ListBuilder::new(BooleanBuilder::new());
        bb.values().append_value(true);
        bb.values().append_null();
        bb.append(true);
        let bool_array: ArrayRef = Arc::new(bb.finish());
        assert_eq!(
            read_list::<Vec<Option<bool>>>(&bool_array, pg_sys::BOOLARRAYOID),
            vec![Some(vec![Some(true), None])]
        );

        let mut sb = ListBuilder::new(StringBuilder::new());
        sb.values().append_value("a");
        sb.values().append_null();
        sb.values().append_value("c");
        sb.append(true);
        let str_array: ArrayRef = Arc::new(sb.finish());
        assert_eq!(
            read_list::<Vec<Option<String>>>(&str_array, pg_sys::TEXTARRAYOID),
            vec![Some(vec![Some("a".to_owned()), None, Some("c".to_owned())])]
        );
    }
}
