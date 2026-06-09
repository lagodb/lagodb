//! Backend tests for the row-world `ColumnRule::extract` (`Arrow → Cell`) path
//! and the encoder NULL-append path.
//!
//! These cannot run as host `#[test]`s: `ColumnRule::extract` dispatches over
//! every column rule, so its compiled body references `decimal::extract`
//! (`numeric_recv`) and `ArrowColumnEncoder::append_datum` references
//! `Decimal128Encoder`'s numeric encode path (`numeric_mul`, `numeric_floor`,
//! `pg_detoast_datum`, ...). Linking those into an ordinary Linux test binary
//! fails with `undefined symbol: PG_exception_stack` (and friends), so the
//! tests must run inside a live backend as `#[pg_test]`s. The pure
//! resolution-table / codec-math tests stay as host tests in
//! `pg-arrow-conv/tests/`.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, ListBuilder, StringBuilder, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    };
    use arrow_array::types::{Int16Type, Int32Type};
    use arrow_array::{Array, ArrayRef, ListArray};
    use pg_arrow_conv::{ColumnRule, PgColumnType, resolve_column_rule};
    use pg_lakebase_core::tuple::{Cell, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
    use pgrx::pg_sys;
    use pgrx::prelude::*;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, TestCaseError, TestRunner};

    /// A generated column: the rule, the built array, and the expected Cell per
    /// row (None = null).
    #[derive(Debug)]
    struct GeneratedColumn {
        rule: ColumnRule,
        array: ArrayRef,
        expected: Vec<Option<ExpectedValue>>,
    }

    /// Subset of Cell that can be compared without PartialEq on Cell itself.
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
        fn matches_cell(&self, cell: &Cell) -> bool {
            match (self, cell) {
                (ExpectedValue::Bool(a), Cell::Bool(b)) => a == b,
                (ExpectedValue::I32(a), Cell::I32(b)) => a == b,
                (ExpectedValue::I64(a), Cell::I64(b)) => a == b,
                (ExpectedValue::F32(a), Cell::F32(b)) => a.to_bits() == b.to_bits(),
                (ExpectedValue::F64(a), Cell::F64(b)) => a.to_bits() == b.to_bits(),
                (ExpectedValue::Date(pg_days), Cell::Date(d)) => {
                    d.to_pg_epoch_days() == *pg_days
                }
                (ExpectedValue::Time(micros), Cell::Time(t)) => {
                    i64::from(*t) == *micros
                }
                (ExpectedValue::Timestamp(pg_micros), Cell::Timestamp(ts)) => {
                    i64::from(*ts) == *pg_micros
                }
                _ => false,
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
            rule: ColumnRule::Bool,
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
            rule: ColumnRule::I32,
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
            rule: ColumnRule::I64,
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
            rule: ColumnRule::F32,
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
            rule: ColumnRule::F64,
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
            rule: ColumnRule::Date32,
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
            rule: ColumnRule::Time64Micros,
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
            rule: ColumnRule::Timestamp {
                nanos: false,
                tz: false,
            },
            array: Arc::new(builder.finish()),
            expected,
        }
    }

    fn arb_nullable<T: Clone + std::fmt::Debug + 'static>(
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
        let array = col.array.as_ref();
        for (row_idx, expected) in col.expected.iter().enumerate() {
            if array.is_null(row_idx) {
                prop_assert!(
                    expected.is_none(),
                    "row {row_idx}: array null but expected a value"
                );
                continue;
            }
            let cell = col
                .rule
                .extract(array, row_idx)
                .map_err(|e| {
                    TestCaseError::fail(format!(
                        "extract error at row {row_idx}: {e:?}"
                    ))
                })?
                .ok_or_else(|| {
                    TestCaseError::fail(format!(
                        "extract returned None for non-null row {row_idx}"
                    ))
                })?;
            match expected {
                Some(exp) => prop_assert!(
                    exp.matches_cell(&cell),
                    "row {}: expected {:?}, got {:?}",
                    row_idx,
                    exp,
                    cell
                ),
                None => {
                    prop_assert!(false, "row {row_idx}: expected null but got a cell")
                }
            }
        }
        Ok(())
    }

    // The slot-first read path (`ArrowColumnDecoder`) and the row-world
    // `extract` share the same per-type value logic, so this pins that shared
    // decode logic across all supported scalar types and random nullability.
    #[pg_test]
    fn extract_produces_expected_cell() {
        let config = Config {
            cases: 256,
            failure_persistence: None,
            ..Config::default()
        };
        let mut runner = TestRunner::new(config);
        runner
            .run(&arb_generated_column(), check_extract)
            .expect("extract must produce the expected Cell");
    }

    /// Resolve the rule for a list column from the built array's own Arrow type,
    /// pairing it with the canonical PG element OID for that element kind (the
    /// `extract` path ignores the OID, but resolution now validates the element
    /// kind against the target element OID).
    fn list_rule(array: &ArrayRef) -> ColumnRule {
        use arrow_schema::DataType;
        let elem_oid = match array.data_type() {
            DataType::List(field) => match field.data_type() {
                DataType::Boolean => pg_sys::BOOLOID,
                DataType::Int32 => pg_sys::INT4OID,
                DataType::Int64 => pg_sys::INT8OID,
                DataType::Float32 => pg_sys::FLOAT4OID,
                DataType::Float64 => pg_sys::FLOAT8OID,
                DataType::Utf8 | DataType::LargeUtf8 => pg_sys::TEXTOID,
                other => panic!("unsupported test list element: {other:?}"),
            },
            other => panic!("not a list type: {other:?}"),
        };
        resolve_column_rule(array.data_type(), PgColumnType::Array(elem_oid))
            .expect("list column rule")
    }

    // These pin `ListValues::into_cell`: the populated cell carries an interior
    // NULL element, and a present-but-empty cell yields an empty `Vec` (not a
    // NULL row). `extract`'s contract is that the caller has already checked
    // `is_null(row_idx)`, so only non-null rows are exercised here.
    #[pg_test]
    fn extract_int4_list_keeps_values_and_interior_null() {
        let array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None, Some(3)]),
                Some(vec![]),
            ]));
        let rule = list_rule(&array);

        match rule.extract(array.as_ref(), 0).unwrap().unwrap() {
            Cell::I32Array(v) => assert_eq!(v, vec![Some(1), None, Some(3)]),
            other => panic!("expected I32Array, got {other:?}"),
        }
        match rule.extract(array.as_ref(), 1).unwrap().unwrap() {
            Cell::I32Array(v) => assert_eq!(v, Vec::<Option<i32>>::new()),
            other => panic!("expected empty I32Array, got {other:?}"),
        }
    }

    // `ListElementRule::Int` only resolves from an `Int32` element schema, but
    // the physical batch array may be narrower (`Int16`). The read path must
    // then keep i16 (`Cell::I16Array` / `int2[]`); only the build path widens
    // Int16 to Int32. Resolve the rule from an Int32 list, then feed it an Int16
    // array.
    #[pg_test]
    fn extract_int16_list_stays_i16_no_widening() {
        let schema_array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1)]),
            ]));
        let rule = list_rule(&schema_array);

        let array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int16Type, _, _>(vec![
                Some(vec![Some(7i16), None, Some(9)]),
            ]));

        match rule.extract(array.as_ref(), 0).unwrap().unwrap() {
            Cell::I16Array(v) => assert_eq!(v, vec![Some(7), None, Some(9)]),
            other => panic!("expected I16Array, got {other:?}"),
        }
    }

    #[pg_test]
    fn extract_bool_and_text_lists() {
        let mut bb = ListBuilder::new(BooleanBuilder::new());
        bb.values().append_value(true);
        bb.values().append_null();
        bb.append(true);
        let bool_array: ArrayRef = Arc::new(bb.finish());
        match list_rule(&bool_array)
            .extract(bool_array.as_ref(), 0)
            .unwrap()
            .unwrap()
        {
            Cell::BoolArray(v) => assert_eq!(v, vec![Some(true), None]),
            other => panic!("expected BoolArray, got {other:?}"),
        }

        let mut sb = ListBuilder::new(StringBuilder::new());
        sb.values().append_value("a");
        sb.values().append_null();
        sb.values().append_value("c");
        sb.append(true);
        let str_array: ArrayRef = Arc::new(sb.finish());
        match list_rule(&str_array)
            .extract(str_array.as_ref(), 0)
            .unwrap()
            .unwrap()
        {
            Cell::StringArray(v) => {
                assert_eq!(
                    v,
                    vec![Some("a".to_string()), None, Some("c".to_string())]
                )
            }
            other => panic!("expected StringArray, got {other:?}"),
        }
    }

    // The NULL path of `ArrowColumnEncoder::append_datum` appends one slot
    // without reading a datum. It lives here (not as a host test) because the
    // `Some` dispatch arm references `Decimal128Encoder::append_datum`'s numeric
    // encode path, which pulls backend symbols into the test binary.
    #[pg_test]
    fn null_append_adds_one_slot_per_call() {
        use arrow_array::Array;
        use pg_arrow_conv::ArrowColumnEncoder;
        use pg_lakebase_core::batch::DatumColumnAppender;

        let mut encoder = ArrowColumnEncoder::new(&ColumnRule::I32, 4);
        encoder.append_datum(None).expect("null");
        encoder.append_datum(None).expect("null");
        assert_eq!(encoder.len(), 2);
        let array = encoder.finish().expect("finish");
        assert_eq!(array.len(), 2);
        assert_eq!(array.null_count(), 2);
    }
}
