//! Tests for `lagodb_core::tuple::Cell` — RowDatumCodec and Display.
//!
//! These tests require a running PostgreSQL backend because they call PG internal
//! functions like `date_out`, `timestamp_out`, array I/O routines, and the
//! Datum allocation infrastructure.

use bytes::Bytes;
use lagodb_core::tuple::{
    Cell, ColumnDatumTarget, JsonText, JsonbValue, RowDatumCodec, TupleSlotRow,
    TupleSlotWriter,
};
use pgrx::pg_sys;
use pgrx::prelude::*;

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    fn recover_cell(
        datum: pg_sys::Datum,
        is_null: bool,
        target_oid: pg_sys::Oid,
    ) -> Option<Cell> {
        let targets = [ColumnDatumTarget::from_oid(target_oid)];
        let codec = RowDatumCodec::from_targets(&targets)
            .expect("test backend must support semantic UTF-8 conversion");
        unsafe { codec.datum_to_cell(0, datum, is_null) }
            .expect("datum should match its test target type")
    }

    fn cell_datum(cell: Cell, target_oid: pg_sys::Oid) -> pg_sys::Datum {
        let targets = [ColumnDatumTarget::from_oid(target_oid)];
        let codec = RowDatumCodec::from_targets(&targets)
            .expect("test backend must support semantic UTF-8 conversion");
        unsafe { codec.cell_to_datum(0, cell) }
            .expect("cell should convert to its test target type")
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_i32() {
        let cell = Cell::I32(42);
        let datum = cell_datum(cell, pg_sys::INT4OID);
        let recovered = recover_cell(datum, false, pg_sys::INT4OID);
        match recovered {
            Some(Cell::I32(v)) => assert_eq!(v, 42),
            other => panic!("expected Cell::I32(42), got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_i64() {
        let cell = Cell::I64(123_456_789_012);
        let datum = cell_datum(cell, pg_sys::INT8OID);
        let recovered = recover_cell(datum, false, pg_sys::INT8OID);
        match recovered {
            Some(Cell::I64(v)) => assert_eq!(v, 123_456_789_012),
            other => panic!("expected Cell::I64, got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_f64() {
        let expected = 12.375_f64;
        let cell = Cell::F64(expected);
        let datum = cell_datum(cell, pg_sys::FLOAT8OID);
        let recovered = recover_cell(datum, false, pg_sys::FLOAT8OID);
        match recovered {
            Some(Cell::F64(v)) => assert!((v - expected).abs() < 1e-10),
            other => panic!("expected Cell::F64, got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_bool() {
        let cell = Cell::Bool(true);
        let datum = cell_datum(cell, pg_sys::BOOLOID);
        let recovered = recover_cell(datum, false, pg_sys::BOOLOID);
        match recovered {
            Some(Cell::Bool(v)) => assert!(v),
            other => panic!("expected Cell::Bool(true), got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_text() {
        let cell = Cell::String("hello world".to_string());
        let datum = cell_datum(cell, pg_sys::TEXTOID);
        let recovered = recover_cell(datum, false, pg_sys::TEXTOID);
        match recovered {
            Some(Cell::String(v)) => assert_eq!(v, "hello world"),
            other => panic!("expected Cell::String, got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_date() {
        let date = Date::new(2024, 6, 15).unwrap();
        let cell = Cell::Date(date);
        let datum = cell_datum(cell, pg_sys::DATEOID);
        let recovered = recover_cell(datum, false, pg_sys::DATEOID);
        match recovered {
            Some(Cell::Date(v)) => assert_eq!(v, date),
            other => panic!("expected Cell::Date, got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_display_uses_pg_output_functions() {
        let date = Date::new(2024, 1, 15).unwrap();
        let cell = Cell::Date(date);
        let display = format!("{}", cell);
        assert_eq!(display, "'2024-01-15'");
    }

    #[pg_test]
    fn test_cell_display_timestamp() {
        let ts = Timestamp::new(2024, 3, 20, 10, 30, 0.0).unwrap();
        let cell = Cell::Timestamp(ts);
        let display = format!("{}", cell);
        assert!(
            display.contains("2024-03-20"),
            "timestamp display should contain date: {}",
            display,
        );
    }

    #[pg_test]
    fn test_cell_into_datum_for_attribute_int_widening() {
        let cell = Cell::I16(100);
        let datum = cell_datum(cell, pg_sys::INT4OID);
        let recovered = recover_cell(datum, false, pg_sys::INT4OID);
        match recovered {
            Some(Cell::I32(v)) => assert_eq!(v, 100),
            other => panic!("expected Cell::I32(100), got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_from_datum_null_returns_none() {
        let result = recover_cell(pg_sys::Datum::from(0usize), true, pg_sys::INT4OID);
        assert!(result.is_none(), "null datum should produce None");
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_bytea() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let cell = Cell::Bytea(Bytes::from(data.clone()));
        let datum = cell_datum(cell, pg_sys::BYTEAOID);
        let recovered = recover_cell(datum, false, pg_sys::BYTEAOID);
        match recovered {
            Some(Cell::Bytea(v)) => assert_eq!(v.as_ref(), &data[..]),
            other => panic!("expected Cell::Bytea, got {:?}", other),
        }
    }

    #[pg_test]
    fn jsonb_datum_row_slot_round_trip_preserves_semantic_value() {
        let input_text = r#"{"b": 2, "a": [true, null]}"#;
        let expected = JsonbValue::try_from(input_text)
            .expect("test input should be valid JSONB");
        let input = std::ffi::CString::new(input_text)
            .expect("JSON text has no interior NUL");
        let datum = unsafe {
            pgrx::fcinfo::direct_function_call_as_datum(
                pg_sys::jsonb_in,
                &[Some(pg_sys::Datum::from(input.as_ptr()))],
            )
        }
        .expect("jsonb_in should produce a datum");

        unsafe {
            let desc = pg_sys::CreateTemplateTupleDesc(1);
            pg_sys::TupleDescInitEntry(
                desc,
                1,
                c"payload".as_ptr(),
                pg_sys::JSONBOID,
                -1,
                0,
            );
            let input_slot = pg_sys::MakeTupleTableSlot(
                desc,
                std::ptr::addr_of!(pg_sys::TTSOpsVirtual),
            );
            let output_slot = pg_sys::MakeTupleTableSlot(
                desc,
                std::ptr::addr_of!(pg_sys::TTSOpsVirtual),
            );
            pg_sys::ExecClearTuple(input_slot);
            pg_sys::ExecClearTuple(output_slot);
            let values = std::slice::from_raw_parts_mut((*input_slot).tts_values, 1);
            let nulls = std::slice::from_raw_parts_mut((*input_slot).tts_isnull, 1);
            values[0] = datum;
            nulls[0] = false;
            pg_sys::ExecStoreVirtualTuple(input_slot);

            let codec = RowDatumCodec::from_slot(input_slot)
                .expect("test slot should produce a row codec");
            let mut row = TupleSlotRow::from_raw(input_slot)
                .to_owned_row(&codec)
                .expect("JSONB slot should materialize as a row");
            assert!(matches!(row.get_cell(0), Some(Cell::Jsonb(_))));

            TupleSlotWriter::new(output_slot, pg_sys::CurrentMemoryContext, &codec)
                .write_row(&mut row)
                .expect("JSONB row should write back to the slot");

            let recovered = TupleSlotRow::from_raw(output_slot)
                .to_owned_row(&codec)
                .expect("output JSONB slot should materialize as a row");
            match recovered.get_cell(0) {
                Some(Cell::Jsonb(value)) => assert_eq!(value, &expected),
                other => {
                    panic!("expected Cell::Jsonb after round-trip, got {other:?}")
                }
            }
        }
    }

    #[pg_test]
    fn cell_jsonb_into_datum_round_trips_as_jsonb() {
        let expected = JsonbValue::try_from(r#"{"b": 2, "a": [true, null]}"#)
            .expect("test input should be valid JSONB");
        let datum = cell_datum(Cell::Jsonb(expected.clone()), pg_sys::JSONBOID);
        let recovered = recover_cell(datum, false, pg_sys::JSONBOID);
        match recovered {
            Some(Cell::Jsonb(value)) => assert_eq!(value, expected),
            other => panic!("expected semantic Cell::Jsonb, got {other:?}"),
        }
    }

    #[pg_test]
    fn cell_json_into_datum_preserves_json_text() {
        let input = r#"{ "b": 2, "b": 3 }"#;
        let cell = Cell::Json(
            JsonText::try_from(input).expect("test input should be valid JSON"),
        );
        let datum = cell_datum(cell, pg_sys::JSONOID);
        let recovered = recover_cell(datum, false, pg_sys::JSONOID);
        match recovered {
            Some(Cell::Json(value)) => assert_eq!(value.as_str(), input),
            other => panic!("expected semantic Cell::Json, got {other:?}"),
        }
    }

    #[pg_test]
    fn jsonb_from_datum_preserves_large_numeric_and_depth() {
        let inputs = vec![
            r#"{"n":123456789012345678901234567890.123456789}"#.to_string(),
            format!("{}0{}", "[".repeat(130), "]".repeat(130)),
        ];

        for input_text in inputs {
            let expected = JsonbValue::try_from(input_text.as_str())
                .expect("test input should be valid JSONB");
            let input = std::ffi::CString::new(input_text)
                .expect("JSON text has no interior NUL");
            let datum = unsafe {
                pgrx::fcinfo::direct_function_call_as_datum(
                    pg_sys::jsonb_in,
                    &[Some(pg_sys::Datum::from(input.as_ptr()))],
                )
            }
            .expect("jsonb_in should produce a datum");

            let recovered = recover_cell(datum, false, pg_sys::JSONBOID);
            match recovered {
                Some(Cell::Jsonb(value)) => assert_eq!(value, expected),
                other => panic!("expected semantic Cell::Jsonb, got {other:?}"),
            }
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_i32_array() {
        let arr = vec![Some(1i32), None, Some(3), Some(42)];
        let cell = Cell::I32Array(arr.clone());
        let datum = cell_datum(cell, pg_sys::INT4ARRAYOID);
        let recovered = recover_cell(datum, false, pg_sys::INT4ARRAYOID);
        match recovered {
            Some(Cell::I32Array(v)) => assert_eq!(v, arr),
            other => panic!("expected Cell::I32Array, got {:?}", other),
        }
    }
}
