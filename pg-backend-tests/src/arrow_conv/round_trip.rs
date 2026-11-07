//! Backend write→read round trip for declared column plans.
//!
//! Each generated PostgreSQL datum is written through `BoundWriteBuffer` (the
//! relation slot path) and read back through `ArrowColumnDecoder` from the
//! same declared `BoundDatumPlan`. The property asserts that the PostgreSQL
//! type-output text of every read-back value equals the original's, and that
//! NULLs stay NULL. Comparing output text keeps the check independent of
//! physical representation (numeric digit layout, jsonb binary form) while
//! still pinning every value, including timezone and decimal scale.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ffi::{CStr, CString};
    use std::ops::Range;
    use std::sync::Arc;
    use std::{ptr, slice};

    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use lagodb_arrow::{
        ArrowColumnDecoder, ArrowConversionError, BoundDatumPlan, BoundWriteBuffer,
        BoundWriteColumnPlan, DecodedColumn,
    };
    use lagodb_core::batch::{BatchBuffer, BatchRowDecoder};
    use lagodb_core::datum::PgType;
    use lagodb_core::tuple::{SlotDatumWriter, TupleSlotRow};
    use pgrx::prelude::*;
    use pgrx::{IntoDatum, pg_sys};
    use proptest::prelude::*;
    use proptest::strategy::{BoxedStrategy, Union};
    use proptest::test_runner::{Config, TestCaseError, TestRunner};

    type TsParts = (i32, u8, u8, u8, u8, u32);

    /// One generated column: the per-row source values of one
    /// provider-admitted PostgreSQL/Arrow mapping, including managed
    /// smallint's widening to Int32 storage.
    #[derive(Debug, Clone)]
    enum Case {
        Bool(Vec<Option<bool>>),
        Int2AsI32(Vec<Option<i16>>),
        Int4AsI32(Vec<Option<i32>>),
        Int8AsI64(Vec<Option<i64>>),
        Float4AsF32(Vec<Option<f32>>),
        Float8AsF64(Vec<Option<f64>>),
        Text(Vec<Option<String>>),
        Bytea(Vec<Option<Vec<u8>>>),
        Numeric {
            precision: u32,
            scale: u32,
            units: Vec<Option<i64>>,
        },
        Date(Vec<Option<(i32, u8, u8)>>),
        Time(Vec<Option<(u8, u8, u32)>>),
        Timestamp {
            nanos: bool,
            rows: Vec<Option<TsParts>>,
        },
        Timestamptz {
            nanos: bool,
            rows: Vec<Option<TsParts>>,
        },
        /// `name` is a fixed `NameData` cstring, not a varlena, stored as UTF-8.
        Name(Vec<Option<String>>),
        /// `json` keeps its validated input text, stored as UTF-8.
        Json(Vec<Option<String>>),
        /// Managed JSONB uses portable PostgreSQL output text.
        Jsonb(Vec<Option<String>>),
    }

    /// Lower a generated case to the stored Arrow type, source OID, and
    /// per-row datums. Runs in-backend because building numeric/temporal
    /// datums calls PG functions.
    fn into_parts(case: Case) -> (DataType, pg_sys::Oid, Vec<Option<pg_sys::Datum>>) {
        match case {
            Case::Bool(v) => (
                DataType::Boolean,
                pg_sys::BOOLOID,
                conv(v, |b| b.into_datum()),
            ),
            Case::Int2AsI32(v) => (
                DataType::Int32,
                pg_sys::INT2OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Int4AsI32(v) => (
                DataType::Int32,
                pg_sys::INT4OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Int8AsI64(v) => (
                DataType::Int64,
                pg_sys::INT8OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Float4AsF32(v) => (
                DataType::Float32,
                pg_sys::FLOAT4OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Float8AsF64(v) => (
                DataType::Float64,
                pg_sys::FLOAT8OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Text(v) => {
                (DataType::Utf8, pg_sys::TEXTOID, conv(v, |s| s.into_datum()))
            }
            Case::Bytea(v) => (
                DataType::LargeBinary,
                pg_sys::BYTEAOID,
                conv(v, |b| b.as_slice().into_datum()),
            ),
            Case::Numeric {
                precision,
                scale,
                units,
            } => (
                DataType::Decimal128(precision as u8, scale as i8),
                pg_sys::NUMERICOID,
                conv(units, |u| numeric_from_units(u, scale).into_datum()),
            ),
            Case::Date(v) => (
                DataType::Date32,
                pg_sys::DATEOID,
                conv(v, |(y, m, d)| {
                    Date::new(y, m, d).expect("valid date").into_datum()
                }),
            ),
            Case::Time(v) => (
                DataType::Time64(TimeUnit::Microsecond),
                pg_sys::TIMEOID,
                conv(v, |(h, m, us)| {
                    Time::new(h, m, us as f64 / 1_000_000.0)
                        .expect("valid time")
                        .into_datum()
                }),
            ),
            Case::Timestamp { nanos, rows } => (
                DataType::Timestamp(time_unit(nanos), None),
                pg_sys::TIMESTAMPOID,
                conv(rows, |(y, mo, d, h, mi, us)| {
                    Timestamp::new(y, mo, d, h, mi, us as f64 / 1_000_000.0)
                        .expect("valid timestamp")
                        .into_datum()
                }),
            ),
            Case::Timestamptz { nanos, rows } => (
                DataType::Timestamp(time_unit(nanos), Some("+00:00".into())),
                pg_sys::TIMESTAMPTZOID,
                conv(rows, |(y, mo, d, h, mi, us)| {
                    TimestampWithTimeZone::new(
                        y,
                        mo,
                        d,
                        h,
                        mi,
                        us as f64 / 1_000_000.0,
                    )
                    .expect("valid timestamptz")
                    .into_datum()
                }),
            ),
            Case::Name(v) => (
                DataType::Utf8,
                pg_sys::NAMEOID,
                conv(v, |s| unsafe { name_datum(&s) }),
            ),
            Case::Json(v) => (
                DataType::Utf8,
                pg_sys::JSONOID,
                conv(v, |s| unsafe { json_datum(&s) }),
            ),
            Case::Jsonb(v) => (
                DataType::Utf8,
                pg_sys::JSONBOID,
                conv(v, |s| unsafe { jsonb_datum(&s) }),
            ),
        }
    }

    fn time_unit(nanos: bool) -> TimeUnit {
        if nanos {
            TimeUnit::Nanosecond
        } else {
            TimeUnit::Microsecond
        }
    }

    /// Build a `name` datum (a palloc'd `NameData`) from text via `namein`. The
    /// result owns its storage, so the temporary `CString` can be dropped.
    unsafe fn name_datum(s: &str) -> Option<pg_sys::Datum> {
        let c = CString::new(s).expect("name has no interior NUL");
        unsafe {
            pgrx::fcinfo::direct_function_call_as_datum(
                pg_sys::namein,
                &[Some(pg_sys::Datum::from(c.as_ptr()))],
            )
        }
    }

    /// Build a `jsonb` datum from JSON text via `jsonb_in`.
    unsafe fn jsonb_datum(s: &str) -> Option<pg_sys::Datum> {
        let c = CString::new(s).expect("json has no interior NUL");
        unsafe {
            pgrx::fcinfo::direct_function_call_as_datum(
                pg_sys::jsonb_in,
                &[Some(pg_sys::Datum::from(c.as_ptr()))],
            )
        }
    }

    /// Build a `json` datum from JSON text via `json_in`.
    unsafe fn json_datum(s: &str) -> Option<pg_sys::Datum> {
        let c = CString::new(s).expect("json has no interior NUL");
        unsafe {
            pgrx::fcinfo::direct_function_call_as_datum(
                pg_sys::json_in,
                &[Some(pg_sys::Datum::from(c.as_ptr()))],
            )
        }
    }

    fn conv<T>(
        vals: Vec<Option<T>>,
        mut f: impl FnMut(T) -> Option<pg_sys::Datum>,
    ) -> Vec<Option<pg_sys::Datum>> {
        vals.into_iter().map(|o| o.and_then(&mut f)).collect()
    }

    /// Build a NUMERIC whose unscaled integer is `units` at the given `scale`,
    /// e.g. `(12345, 2) -> "123.45"`. Kept within precision by the generator so
    /// both encode paths succeed.
    fn numeric_from_units(units: i64, scale: u32) -> AnyNumeric {
        let neg = units < 0;
        let digits = units.unsigned_abs().to_string();
        let body = if scale == 0 {
            digits
        } else {
            let scale = scale as usize;
            let padded = if digits.len() <= scale {
                format!("{digits:0>width$}", width = scale + 1)
            } else {
                digits
            };
            let point = padded.len() - scale;
            format!("{}.{}", &padded[..point], &padded[point..])
        };
        let text = if neg { format!("-{body}") } else { body };
        AnyNumeric::try_from(text.as_str()).expect("valid numeric literal")
    }

    /// A single-column virtual slot of `oid`, reused across rows within a case.
    unsafe fn make_slot(oid: pg_sys::Oid) -> *mut pg_sys::TupleTableSlot {
        unsafe {
            let desc = pg_sys::CreateTemplateTupleDesc(1);
            pg_sys::TupleDescInitEntry(desc, 1, c"c".as_ptr(), oid, -1, 0);
            pg_sys::MakeTupleTableSlot(desc, ptr::addr_of!(pg_sys::TTSOpsVirtual))
        }
    }

    unsafe fn store_datum(
        slot: *mut pg_sys::TupleTableSlot,
        datum: Option<pg_sys::Datum>,
    ) {
        unsafe {
            pg_sys::ExecClearTuple(slot);
            let values = slice::from_raw_parts_mut((*slot).tts_values, 1);
            let isnull = slice::from_raw_parts_mut((*slot).tts_isnull, 1);
            match datum {
                Some(d) => {
                    values[0] = d;
                    isnull[0] = false;
                }
                None => {
                    values[0] = pg_sys::Datum::from(0usize);
                    isnull[0] = true;
                }
            }
            pg_sys::ExecStoreVirtualTuple(slot);
        }
    }

    /// PostgreSQL type-output text of a non-NULL datum.
    unsafe fn output_text(oid: pg_sys::Oid, datum: pg_sys::Datum) -> String {
        let mut output = pg_sys::InvalidOid;
        let mut is_varlena = false;
        unsafe {
            pg_sys::getTypeOutputInfo(oid, &mut output, &mut is_varlena);
            CStr::from_ptr(pg_sys::OidOutputFunctionCall(output, datum))
                .to_str()
                .expect("server encoding is UTF-8")
                .to_owned()
        }
    }

    unsafe fn slot_datum(slot: *mut pg_sys::TupleTableSlot) -> Option<pg_sys::Datum> {
        unsafe {
            if *(*slot).tts_isnull {
                None
            } else {
                Some(*(*slot).tts_values)
            }
        }
    }

    fn fail(stage: &str) -> impl Fn(ArrowConversionError) -> TestCaseError + '_ {
        move |error| TestCaseError::fail(format!("{stage} failed: {error:?}"))
    }

    fn run_case(case: Case) -> Result<(), TestCaseError> {
        let (data_type, oid, datums) = into_parts(case);
        let schema = Arc::new(Schema::new(vec![Field::new("c", data_type, true)]));
        let pg_type = PgType::new(oid, -1);
        let write_plan = BoundDatumPlan::bind(schema.field(0), pg_type)
            .and_then(|plan| BoundWriteColumnPlan::bind(plan, 0, 1))
            .map_err(fail("write bind"))?;
        let read_plan = BoundDatumPlan::bind(schema.field(0), pg_type)
            .map_err(fail("read bind"))?;
        let mut buffer = BoundWriteBuffer::new(schema, Box::new([write_plan]))
            .map_err(fail("buffer bind"))?;
        // SAFETY: the decoder reads only batches written from PostgreSQL datums
        // of the same declared type, so widened int2 values stay in domain and
        // TextIo values are NUL-free type-output text.
        let decoder = ArrowColumnDecoder::new(vec![
            unsafe { DecodedColumn::bind(read_plan, 0, 0) }
                .map_err(fail("decoder bind"))?,
        ]);

        unsafe {
            let source = make_slot(oid);
            for datum in &datums {
                store_datum(source, *datum);
                buffer
                    .append_slot_row(TupleSlotRow::from_raw(source))
                    .map_err(fail("append"))?;
            }
            let batch = buffer.finish_batch().map_err(fail("finish"))?;
            let bound = decoder.bind(batch).map_err(|error| {
                TestCaseError::fail(format!("batch bind failed: {error:?}"))
            })?;

            let target = make_slot(oid);
            for (row, datum) in datums.iter().enumerate() {
                let mut out =
                    SlotDatumWriter::new(target, pg_sys::CurrentMemoryContext);
                decoder.write_row(&bound, row, &mut out).map_err(|error| {
                    TestCaseError::fail(format!("read row {row} failed: {error:?}"))
                })?;
                let expected = datum.map(|datum| output_text(oid, datum));
                let actual = slot_datum(target).map(|datum| output_text(oid, datum));
                prop_assert_eq!(expected, actual, "row {} of type {:?}", row, oid);
            }
        }
        Ok(())
    }

    // --- generators (small, focused) ----------------------------------------

    fn finite_f32() -> impl Strategy<Value = f32> {
        -1.0e9f32..1.0e9
    }

    fn finite_f64() -> impl Strategy<Value = f64> {
        -1.0e9f64..1.0e9
    }

    fn text() -> impl Strategy<Value = String> {
        proptest::string::string_regex("[ -~]{0,16}").expect("valid regex")
    }

    fn numeric_case(rows: Range<usize>) -> impl Strategy<Value = Case> {
        (1u32..=18u32).prop_flat_map(move |precision| {
            let rows = rows.clone();
            (Just(precision), 0u32..=precision).prop_flat_map(
                move |(precision, scale)| {
                    let bound = 10i64.pow(precision) - 1;
                    prop::collection::vec(
                        prop::option::of(-bound..=bound),
                        rows.clone(),
                    )
                    .prop_map(move |units| Case::Numeric {
                        precision,
                        scale,
                        units,
                    })
                },
            )
        })
    }

    fn ts_parts() -> impl Strategy<Value = TsParts> {
        (
            2000i32..2100,
            1u8..=12,
            1u8..=28,
            0u8..24,
            0u8..60,
            0u32..60_000_000,
        )
    }

    fn cases() -> impl Strategy<Value = Case> {
        let n = 0usize..6;
        let arms: Vec<BoxedStrategy<Case>> = vec![
            prop::collection::vec(prop::option::of(any::<bool>()), n.clone())
                .prop_map(Case::Bool)
                .boxed(),
            prop::collection::vec(prop::option::of(any::<i16>()), n.clone())
                .prop_map(Case::Int2AsI32)
                .boxed(),
            prop::collection::vec(prop::option::of(any::<i32>()), n.clone())
                .prop_map(Case::Int4AsI32)
                .boxed(),
            prop::collection::vec(prop::option::of(any::<i64>()), n.clone())
                .prop_map(Case::Int8AsI64)
                .boxed(),
            prop::collection::vec(prop::option::of(finite_f32()), n.clone())
                .prop_map(Case::Float4AsF32)
                .boxed(),
            prop::collection::vec(prop::option::of(finite_f64()), n.clone())
                .prop_map(Case::Float8AsF64)
                .boxed(),
            prop::collection::vec(prop::option::of(text()), n.clone())
                .prop_map(Case::Text)
                .boxed(),
            prop::collection::vec(
                prop::option::of(prop::collection::vec(any::<u8>(), 0..8)),
                n.clone(),
            )
            .prop_map(Case::Bytea)
            .boxed(),
            numeric_case(n.clone()).boxed(),
            prop::collection::vec(
                prop::option::of((2000i32..2100, 1u8..=12, 1u8..=28)),
                n.clone(),
            )
            .prop_map(Case::Date)
            .boxed(),
            prop::collection::vec(
                prop::option::of((0u8..24, 0u8..60, 0u32..60_000_000)),
                n.clone(),
            )
            .prop_map(Case::Time)
            .boxed(),
            prop::collection::vec(prop::option::of(ts_parts()), n.clone())
                .prop_map(|rows| Case::Timestamp { nanos: false, rows })
                .boxed(),
            prop::collection::vec(prop::option::of(ts_parts()), n.clone())
                .prop_map(|rows| Case::Timestamp { nanos: true, rows })
                .boxed(),
            prop::collection::vec(prop::option::of(ts_parts()), n.clone())
                .prop_map(|rows| Case::Timestamptz { nanos: false, rows })
                .boxed(),
            prop::collection::vec(prop::option::of(ts_parts()), n)
                .prop_map(|rows| Case::Timestamptz { nanos: true, rows })
                .boxed(),
        ];
        Union::new(arms)
    }

    #[pg_test]
    fn write_read_round_trip_preserves_values() {
        let config = Config {
            cases: 48,
            failure_persistence: None,
            ..Config::default()
        };
        let mut runner = TestRunner::new(config);
        runner
            .run(&cases(), run_case)
            .expect("written values must read back unchanged");
    }

    // Type I/O-backed values also exercise deterministic NULL and text cases.
    #[pg_test]
    fn write_read_round_trip_for_name() {
        run_case(Case::Name(vec![
            Some("name_value".to_owned()),
            None,
            Some(String::new()),
            Some("another_name".to_owned()),
        ]))
        .expect("name values must read back unchanged");
    }

    #[pg_test]
    fn write_read_round_trip_for_json() {
        run_case(Case::Json(vec![
            Some(r#"{ "json_key": 1, "json_key": 2 }"#.to_owned()),
            None,
            Some("[true, null]".to_owned()),
        ]))
        .expect("json values must read back unchanged");
    }

    #[pg_test]
    fn write_read_round_trip_for_jsonb() {
        run_case(Case::Jsonb(vec![
            Some(r#"{"jsonb_key": "val"}"#.to_owned()),
            None,
            Some("[1, 2, 3]".to_owned()),
            Some("\"scalar\"".to_owned()),
        ]))
        .expect("jsonb values must read back unchanged");
    }
}
