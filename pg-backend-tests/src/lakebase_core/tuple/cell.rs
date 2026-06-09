//! Tests for `pg_lakebase_core::tuple::Cell` — IntoDatum, FromDatum, and Display.
//!
//! These tests require a running PostgreSQL backend because they call PG internal
//! functions like `date_out`, `timestamp_out`, array I/O routines, and the
//! Datum allocation infrastructure.

use bytes::Bytes;
use pg_lakebase_core::tuple::Cell;
use pgrx::prelude::*;
use pgrx::{FromDatum, IntoDatum, pg_sys};

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_i32() {
        let cell = Cell::I32(42);
        let datum = cell.into_datum().expect("i32 should convert to datum");
        let recovered =
            unsafe { Cell::from_polymorphic_datum(datum, false, pg_sys::INT4OID) };
        match recovered {
            Some(Cell::I32(v)) => assert_eq!(v, 42),
            other => panic!("expected Cell::I32(42), got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_i64() {
        let cell = Cell::I64(123_456_789_012);
        let datum = cell.into_datum().expect("i64 should convert to datum");
        let recovered =
            unsafe { Cell::from_polymorphic_datum(datum, false, pg_sys::INT8OID) };
        match recovered {
            Some(Cell::I64(v)) => assert_eq!(v, 123_456_789_012),
            other => panic!("expected Cell::I64, got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_f64() {
        let expected = 12.375_f64;
        let cell = Cell::F64(expected);
        let datum = cell.into_datum().expect("f64 should convert to datum");
        let recovered =
            unsafe { Cell::from_polymorphic_datum(datum, false, pg_sys::FLOAT8OID) };
        match recovered {
            Some(Cell::F64(v)) => assert!((v - expected).abs() < 1e-10),
            other => panic!("expected Cell::F64, got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_bool() {
        let cell = Cell::Bool(true);
        let datum = cell.into_datum().expect("bool should convert to datum");
        let recovered =
            unsafe { Cell::from_polymorphic_datum(datum, false, pg_sys::BOOLOID) };
        match recovered {
            Some(Cell::Bool(v)) => assert!(v),
            other => panic!("expected Cell::Bool(true), got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_text() {
        let cell = Cell::String("hello world".to_string());
        let datum = cell.into_datum().expect("String should convert to datum");
        let recovered =
            unsafe { Cell::from_polymorphic_datum(datum, false, pg_sys::TEXTOID) };
        match recovered {
            Some(Cell::String(v)) => assert_eq!(v, "hello world"),
            other => panic!("expected Cell::String, got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_date() {
        let date = Date::new(2024, 6, 15).unwrap();
        let cell = Cell::Date(date);
        let datum = cell.into_datum().expect("Date should convert to datum");
        let recovered =
            unsafe { Cell::from_polymorphic_datum(datum, false, pg_sys::DATEOID) };
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
    fn test_cell_into_datum_typed_int_widening() {
        let cell = Cell::I16(100);
        let datum = unsafe { cell.into_datum_typed(pg_sys::INT4OID, -1) };
        assert!(datum.is_some(), "i16 -> int4 conversion should succeed");
        let recovered = unsafe {
            Cell::from_polymorphic_datum(datum.unwrap(), false, pg_sys::INT4OID)
        };
        match recovered {
            Some(Cell::I32(v)) => assert_eq!(v, 100),
            other => panic!("expected Cell::I32(100), got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_from_datum_null_returns_none() {
        let result = unsafe {
            Cell::from_polymorphic_datum(
                pg_sys::Datum::from(0usize),
                true,
                pg_sys::INT4OID,
            )
        };
        assert!(result.is_none(), "null datum should produce None");
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_bytea() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let cell = Cell::Bytea(Bytes::from(data.clone()));
        let datum = cell.into_datum().expect("Bytea should convert to datum");
        let recovered =
            unsafe { Cell::from_polymorphic_datum(datum, false, pg_sys::BYTEAOID) };
        match recovered {
            Some(Cell::Bytea(v)) => assert_eq!(v.as_ref(), &data[..]),
            other => panic!("expected Cell::Bytea, got {:?}", other),
        }
    }

    #[pg_test]
    fn test_cell_into_datum_and_from_datum_i32_array() {
        let arr = vec![Some(1i32), None, Some(3), Some(42)];
        let cell = Cell::I32Array(arr.clone());
        let datum = cell.into_datum().expect("I32Array should convert to datum");
        let recovered = unsafe {
            Cell::from_polymorphic_datum(datum, false, pg_sys::INT4ARRAYOID)
        };
        match recovered {
            Some(Cell::I32Array(v)) => assert_eq!(v, arr),
            other => panic!("expected Cell::I32Array, got {:?}", other),
        }
    }
}
