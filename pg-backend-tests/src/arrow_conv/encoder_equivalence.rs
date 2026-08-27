//! Backend equivalence property between the slot/datum write path and the
//! buffered-row write path.
//!
//! Both paths read the *same* PostgreSQL datum: `ArrowColumnEncoder` reads it
//! through a tuple-slot view, while `ColumnRule::build` reads it after it has
//! been materialized into a `Cell`. The property asserts the two produce a
//! bit-identical Arrow array (physical type, values, null mask, timezone, and
//! decimal precision/scale), so the slot path can replace the row path without
//! changing stored values.
//!
//! Driving both sides from one datum makes the check robust to representation
//! quirks (session timezone, numeric text round-trips): it verifies the encoder
//! reproduces `build`, not any absolute encoding.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ops::Range;

    use arrow_array::Array;
    use arrow_schema::{Field, Schema};
    use lagodb_core::batch::BatchBuffer;
    use lagodb_core::tuple::{Row, RowDatumCodec, TupleSlotRow};
    use pg_arrow_conv::{
        ArrowColumnEncoder, BoundWriteBuffer, BoundWriteColumnPlan, ColumnRule,
    };
    use pgrx::prelude::*;
    use pgrx::{IntoDatum, pg_sys};
    use proptest::prelude::*;
    use proptest::strategy::{BoxedStrategy, Union};
    use proptest::test_runner::{Config, TestCaseError, TestRunner};

    type TsParts = (i32, u8, u8, u8, u8, u32);

    /// One generated column: the resolved rule plus the per-row source values.
    /// Each variant pins a source PostgreSQL type and an Arrow target so the
    /// integer/float widening arms are exercised both at and across widths.
    #[derive(Debug, Clone)]
    enum Case {
        Bool(Vec<Option<bool>>),
        Int2AsI32(Vec<Option<i16>>),
        Int4AsI64(Vec<Option<i32>>),
        Int8AsI64(Vec<Option<i64>>),
        Float4AsF64(Vec<Option<f32>>),
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
        /// `name` is a fixed `NameData` cstring, not a varlena, and maps to an
        /// Iceberg string (`ColumnRule::Utf8`).
        Name(Vec<Option<String>>),
        /// `json` keeps its validated input text and maps to an Iceberg string
        /// (`ColumnRule::Utf8`).
        Json(Vec<Option<String>>),
        /// `jsonb` is stored as Iceberg binary holding PostgreSQL's internal
        /// varlena verbatim (header included), so it maps to the explicit
        /// `ColumnRule::PostgresJsonbVarlena` codec.
        Jsonb(Vec<Option<String>>),
    }

    /// Lower a generated case to the rule, source OID, and per-row datums. Runs
    /// in-backend because building numeric/temporal datums calls PG functions.
    fn into_parts(
        case: Case,
    ) -> (ColumnRule, pg_sys::Oid, Vec<Option<pg_sys::Datum>>) {
        match case {
            Case::Bool(v) => (
                ColumnRule::Bool,
                pg_sys::BOOLOID,
                conv(v, |b| b.into_datum()),
            ),
            Case::Int2AsI32(v) => (
                ColumnRule::I32,
                pg_sys::INT2OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Int4AsI64(v) => (
                ColumnRule::I64,
                pg_sys::INT4OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Int8AsI64(v) => (
                ColumnRule::I64,
                pg_sys::INT8OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Float4AsF64(v) => (
                ColumnRule::F64,
                pg_sys::FLOAT4OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Float8AsF64(v) => (
                ColumnRule::F64,
                pg_sys::FLOAT8OID,
                conv(v, |x| x.into_datum()),
            ),
            Case::Text(v) => (
                ColumnRule::Utf8,
                pg_sys::TEXTOID,
                conv(v, |s| s.into_datum()),
            ),
            Case::Bytea(v) => (
                ColumnRule::Binary,
                pg_sys::BYTEAOID,
                conv(v, |b| b.as_slice().into_datum()),
            ),
            Case::Numeric {
                precision,
                scale,
                units,
            } => (
                ColumnRule::Decimal128 { precision, scale },
                pg_sys::NUMERICOID,
                conv(units, |u| numeric_from_units(u, scale).into_datum()),
            ),
            Case::Date(v) => (
                ColumnRule::Date32,
                pg_sys::DATEOID,
                conv(v, |(y, m, d)| {
                    Date::new(y, m, d).expect("valid date").into_datum()
                }),
            ),
            Case::Time(v) => (
                ColumnRule::Time64Micros,
                pg_sys::TIMEOID,
                conv(v, |(h, m, us)| {
                    Time::new(h, m, us as f64 / 1_000_000.0)
                        .expect("valid time")
                        .into_datum()
                }),
            ),
            Case::Timestamp { nanos, rows } => (
                ColumnRule::Timestamp { nanos, tz: false },
                pg_sys::TIMESTAMPOID,
                conv(rows, |(y, mo, d, h, mi, us)| {
                    Timestamp::new(y, mo, d, h, mi, us as f64 / 1_000_000.0)
                        .expect("valid timestamp")
                        .into_datum()
                }),
            ),
            Case::Timestamptz { nanos, rows } => (
                ColumnRule::Timestamp { nanos, tz: true },
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
                ColumnRule::Utf8,
                pg_sys::NAMEOID,
                conv(v, |s| unsafe { name_datum(&s) }),
            ),
            Case::Json(v) => (
                ColumnRule::Utf8,
                pg_sys::JSONOID,
                conv(v, |s| unsafe { json_datum(&s) }),
            ),
            Case::Jsonb(v) => (
                ColumnRule::PostgresJsonbVarlena,
                pg_sys::JSONBOID,
                conv(v, |s| unsafe { jsonb_datum(&s) }),
            ),
        }
    }

    /// Build a `name` datum (a palloc'd `NameData`) from text via `namein`. The
    /// result owns its storage, so the temporary `CString` can be dropped.
    unsafe fn name_datum(s: &str) -> Option<pg_sys::Datum> {
        let c = std::ffi::CString::new(s).expect("name has no interior NUL");
        unsafe {
            pgrx::fcinfo::direct_function_call_as_datum(
                pg_sys::namein,
                &[Some(pg_sys::Datum::from(c.as_ptr()))],
            )
        }
    }

    /// Build a `jsonb` datum from JSON text via `jsonb_in`.
    unsafe fn jsonb_datum(s: &str) -> Option<pg_sys::Datum> {
        let c = std::ffi::CString::new(s).expect("json has no interior NUL");
        unsafe {
            pgrx::fcinfo::direct_function_call_as_datum(
                pg_sys::jsonb_in,
                &[Some(pg_sys::Datum::from(c.as_ptr()))],
            )
        }
    }

    /// Build a `json` datum from JSON text via `json_in`.
    unsafe fn json_datum(s: &str) -> Option<pg_sys::Datum> {
        let c = std::ffi::CString::new(s).expect("json has no interior NUL");
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
            pg_sys::MakeTupleTableSlot(
                desc,
                std::ptr::addr_of!(pg_sys::TTSOpsVirtual),
            )
        }
    }

    unsafe fn store_datum(
        slot: *mut pg_sys::TupleTableSlot,
        datum: Option<pg_sys::Datum>,
    ) {
        unsafe {
            pg_sys::ExecClearTuple(slot);
            let values = std::slice::from_raw_parts_mut((*slot).tts_values, 1);
            let isnull = std::slice::from_raw_parts_mut((*slot).tts_isnull, 1);
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

    fn run_case(case: Case) -> Result<(), TestCaseError> {
        let (rule, oid, datums) = into_parts(case);
        unsafe {
            let slot = make_slot(oid);
            let row_codec = RowDatumCodec::from_slot(slot)
                .expect("test slot must produce a row datum codec");
            let mut type_encoder =
                ArrowColumnEncoder::new(&rule, 0).map_err(|e| {
                    TestCaseError::fail(format!(
                        "type encoder construction failed: {e:?}"
                    ))
                })?;
            let data_type = type_encoder
                .finish()
                .map_err(|e| {
                    TestCaseError::fail(format!("type finish failed: {e:?}"))
                })?
                .data_type()
                .clone();
            let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
                "c", data_type, true,
            )]));
            let plan =
                BoundWriteColumnPlan::bind(rule.clone(), Some(0), Some(oid), 1)
                    .map_err(|e| {
                        TestCaseError::fail(format!("bind failed: {e:?}"))
                    })?;
            let mut buffer =
                BoundWriteBuffer::new(schema, vec![plan].into_boxed_slice())
                    .map_err(|e| {
                        TestCaseError::fail(format!("buffer bind failed: {e:?}"))
                    })?;
            let mut rows: Vec<Row> = Vec::with_capacity(datums.len());

            for datum in datums {
                store_datum(slot, datum);
                let row = TupleSlotRow::from_raw(slot);
                buffer.append_slot_row(row).map_err(|e| {
                    TestCaseError::fail(format!("encoder append failed: {e:?}"))
                })?;
                let view = row.datum_at(0);
                let cell = match view.filter(|v| !v.is_null()) {
                    Some(value) => value.to_cell(&row_codec).map_err(|error| {
                        TestCaseError::fail(format!(
                            "cell conversion failed: {error}"
                        ))
                    })?,
                    None => None,
                };
                let mut row = Row::with_width(1);
                row.set_cell(0, cell);
                rows.push(row);
            }

            let encoded = buffer
                .finish_batch()
                .map_err(|e| {
                    TestCaseError::fail(format!("encoder finish failed: {e:?}"))
                })?
                .column(0)
                .clone();
            let reference = rule
                .build(&rows, 0)
                .map_err(|e| TestCaseError::fail(format!("build failed: {e:?}")))?;

            prop_assert_eq!(
                encoded.to_data(),
                reference.to_data(),
                "encoder/build mismatch for {:?}",
                rule
            );
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
                .prop_map(Case::Int4AsI64)
                .boxed(),
            prop::collection::vec(prop::option::of(any::<i64>()), n.clone())
                .prop_map(Case::Int8AsI64)
                .boxed(),
            prop::collection::vec(prop::option::of(finite_f32()), n.clone())
                .prop_map(Case::Float4AsF64)
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
    fn encoder_output_matches_column_rule_build() {
        let config = Config {
            cases: 48,
            failure_persistence: None,
            ..Config::default()
        };
        let mut runner = TestRunner::new(config);
        runner
            .run(&cases(), run_case)
            .expect("encoder output must match ColumnRule::build");
    }

    // `name`, `json`, and `jsonb` are not in the proptest generator (their
    // datums are built via input functions, not `IntoDatum`), so they get deterministic
    // equivalence checks. Both encoders previously diverged from `build`:
    // `name` was dropped to NULL (not a varlena) and `jsonb` had its varlena
    // header stripped (crashing the backend on read).
    #[pg_test]
    fn encoder_output_matches_build_for_name() {
        run_case(Case::Name(vec![
            Some("name_value".to_string()),
            None,
            Some(String::new()),
            Some("another_name".to_string()),
        ]))
        .expect("name encoder output must match ColumnRule::build");
    }

    #[pg_test]
    fn encoder_output_matches_build_for_json() {
        run_case(Case::Json(vec![
            Some(r#"{ "json_key": 1, "json_key": 2 }"#.to_string()),
            None,
            Some("[true, null]".to_string()),
        ]))
        .expect("json encoder output must match ColumnRule::build");
    }

    #[pg_test]
    fn encoder_output_matches_build_for_jsonb() {
        run_case(Case::Jsonb(vec![
            Some(r#"{"jsonb_key": "val"}"#.to_string()),
            None,
            Some("[1, 2, 3]".to_string()),
            Some("\"scalar\"".to_string()),
        ]))
        .expect("jsonb encoder output must match ColumnRule::build");
    }
}
