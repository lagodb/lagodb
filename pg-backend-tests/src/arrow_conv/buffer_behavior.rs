//! Backend tests for bound datum-path hazards and buffer behavior
//! that need a live PostgreSQL backend: the varlena detoast fix runs real toast
//! fetches, and the buffer's NULL-alignment / flush behavior is driven from
//! actual tuple slots.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, LargeBinaryArray};
    use arrow_schema::{DataType, Field, Schema};
    use pg_arrow_conv::{BoundWriteBuffer, BoundWriteColumnPlan, ColumnRule};
    use pg_lakebase_core::batch::BatchBuffer;
    use pg_lakebase_core::tuple::TupleSlotRow;
    use pgrx::prelude::*;
    use pgrx::{IntoDatum, pg_sys};

    /// A virtual slot whose tuple descriptor carries one column per `oids`
    /// entry, reused across rows within a test.
    unsafe fn make_slot(oids: &[pg_sys::Oid]) -> *mut pg_sys::TupleTableSlot {
        unsafe {
            let desc = pg_sys::CreateTemplateTupleDesc(oids.len() as i32);
            for (i, oid) in oids.iter().enumerate() {
                pg_sys::TupleDescInitEntry(
                    desc,
                    (i + 1) as pg_sys::AttrNumber,
                    c"c".as_ptr(),
                    *oid,
                    -1,
                    0,
                );
            }
            pg_sys::MakeTupleTableSlot(
                desc,
                std::ptr::addr_of!(pg_sys::TTSOpsVirtual),
            )
        }
    }

    unsafe fn store_row(
        slot: *mut pg_sys::TupleTableSlot,
        datums: &[Option<pg_sys::Datum>],
    ) {
        unsafe {
            pg_sys::ExecClearTuple(slot);
            let n = datums.len();
            let values = std::slice::from_raw_parts_mut((*slot).tts_values, n);
            let isnull = std::slice::from_raw_parts_mut((*slot).tts_isnull, n);
            for (i, datum) in datums.iter().enumerate() {
                match datum {
                    Some(d) => {
                        values[i] = *d;
                        isnull[i] = false;
                    }
                    None => {
                        values[i] = pg_sys::Datum::from(0usize);
                        isnull[i] = true;
                    }
                }
            }
            pg_sys::ExecStoreVirtualTuple(slot);
        }
    }

    fn bound_buffer(
        schema: Arc<Schema>,
        bindings: &[(ColumnRule, pg_sys::Oid)],
    ) -> BoundWriteBuffer {
        let slot_width = bindings.len();
        let plans = bindings
            .iter()
            .enumerate()
            .map(|(index, (rule, oid))| {
                BoundWriteColumnPlan::bind(
                    rule.clone(),
                    Some(index),
                    Some(*oid),
                    slot_width,
                )
                .expect("bind bound write column")
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        BoundWriteBuffer::new(schema, plans).expect("bind bound write buffer")
    }

    // A toasted `bytea`, read through the encoder, must append the fully
    // detoasted payload — the structural fix for the old write path that read
    // varlena bytes without detoasting. EXTERNAL storage forces an 8 KB value
    // out of line so the stored datum is a toast pointer, not the bytes.
    #[pg_test]
    fn toasted_bytea_appends_detoasted_payload() {
        let expected = vec![b'x'; 8000];
        Spi::run("CREATE TEMP TABLE toast_probe (b bytea)").expect("create");
        Spi::run("ALTER TABLE toast_probe ALTER COLUMN b SET STORAGE EXTERNAL")
            .expect("set storage");
        Spi::run("INSERT INTO toast_probe VALUES (repeat('x', 8000)::bytea)")
            .expect("insert");

        let array = unsafe {
            assert_eq!(pg_sys::SPI_connect(), pg_sys::SPI_OK_CONNECT as i32);
            let rc =
                pg_sys::SPI_execute(c"SELECT b FROM toast_probe".as_ptr(), false, 0);
            assert_eq!(rc, pg_sys::SPI_OK_SELECT as i32);
            let processed = std::ptr::addr_of!(pg_sys::SPI_processed).read();
            assert_eq!(processed, 1);

            let tuptable = std::ptr::addr_of!(pg_sys::SPI_tuptable).read();
            let tupdesc = (*tuptable).tupdesc;
            let tuple = *(*tuptable).vals;
            let mut isnull = false;
            // The datum stays a toast pointer here: SPI does not detoast.
            let datum = pg_sys::heap_getattr(tuple, 1, tupdesc, &mut isnull);
            assert!(!isnull);

            let slot = make_slot(&[pg_sys::BYTEAOID]);
            store_row(slot, &[Some(datum)]);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "b",
                DataType::LargeBinary,
                true,
            )]));
            let mut buffer =
                bound_buffer(schema, &[(ColumnRule::Binary, pg_sys::BYTEAOID)]);
            buffer
                .append_slot_row(TupleSlotRow::from_raw(slot))
                .expect("append toasted bytea");
            let array = buffer.finish_batch().expect("finish").column(0).clone();

            pg_sys::SPI_finish();
            array
        };

        let bytes = array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("LargeBinaryArray");
        assert_eq!(bytes.value(0), expected.as_slice());
    }

    // A `bytea` whose length differs from the fixed-binary width is rejected
    // rather than silently truncated or padded.
    #[pg_test]
    fn fixed_binary_width_mismatch_is_rejected() {
        unsafe {
            let slot = make_slot(&[pg_sys::BYTEAOID]);
            let wrong: &[u8] = &[1, 2, 3];
            store_row(slot, &[wrong.into_datum()]);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "b",
                DataType::FixedSizeBinary(16),
                true,
            )]));
            let mut buffer = bound_buffer(
                schema,
                &[(ColumnRule::FixedBinary { len: 16 }, pg_sys::BYTEAOID)],
            );
            let result = buffer.append_slot_row(TupleSlotRow::from_raw(slot));
            assert!(result.is_err(), "width mismatch must fail");
        }
    }

    // Appending rows with nulls scattered across columns keeps every column the
    // same length, so the flushed batch is rectangular and `try_new` succeeds.
    #[pg_test]
    fn null_alignment_yields_rectangular_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Boolean, true),
        ]));
        let rules = [ColumnRule::I32, ColumnRule::Utf8, ColumnRule::Bool];
        let mut buffer = bound_buffer(
            schema,
            &[
                (rules[0].clone(), pg_sys::INT4OID),
                (rules[1].clone(), pg_sys::TEXTOID),
                (rules[2].clone(), pg_sys::BOOLOID),
            ],
        );

        unsafe {
            let slot =
                make_slot(&[pg_sys::INT4OID, pg_sys::TEXTOID, pg_sys::BOOLOID]);
            let rows: [[Option<pg_sys::Datum>; 3]; 4] = [
                [1i32.into_datum(), "x".into_datum(), true.into_datum()],
                [None, "y".into_datum(), None],
                [3i32.into_datum(), None, false.into_datum()],
                [None, None, None],
            ];
            for row in rows {
                store_row(slot, &row);
                buffer
                    .append_slot_row(TupleSlotRow::from_raw(slot))
                    .expect("append row");
            }
        }

        assert_eq!(buffer.len(), 4);
        let batch = buffer.finish_batch().expect("finish batch");
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 3);
        for column in batch.columns() {
            assert_eq!(column.len(), 4);
        }
        assert_eq!(batch.column(0).null_count(), 2);
        assert_eq!(batch.column(1).null_count(), 2);
        assert_eq!(batch.column(2).null_count(), 2);
    }

    // Flushing an empty buffer yields a schema-only batch and touches nothing.
    #[pg_test]
    fn empty_buffer_flushes_schema_only_batch() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let mut buffer =
            bound_buffer(schema.clone(), &[(ColumnRule::I32, pg_sys::INT4OID)]);

        assert!(buffer.is_empty());
        let batch = buffer.finish_batch().expect("finish empty");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }

    // The byte estimate crosses the configured threshold exactly when the
    // appended fixed-width payload reaches it: 4 bytes per Int32 row.
    #[pg_test]
    fn flush_signals_at_configured_byte_threshold() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let mut buffer = bound_buffer(schema, &[(ColumnRule::I32, pg_sys::INT4OID)]);
        let threshold = 40; // 10 Int32 rows * 4 bytes

        unsafe {
            let slot = make_slot(&[pg_sys::INT4OID]);
            for i in 0..9i32 {
                store_row(slot, &[i.into_datum()]);
                buffer
                    .append_slot_row(TupleSlotRow::from_raw(slot))
                    .expect("append row");
            }
            assert!(
                !buffer.should_flush(threshold),
                "9 rows stay under threshold"
            );

            store_row(slot, &[9i32.into_datum()]);
            buffer
                .append_slot_row(TupleSlotRow::from_raw(slot))
                .expect("append row");
            assert!(buffer.should_flush(threshold), "10 rows reach threshold");
        }
    }
}
