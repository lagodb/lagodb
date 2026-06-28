//! Backend tests for the read path that decodes an Arrow column batch straight
//! into a tuple slot: `ArrowColumnDecoder` writing through `SlotColumns`, plus
//! the slot-non-empty / memory-context discipline the core scan shim relies on.
//!
//! These need a live PostgreSQL backend because datum construction for the
//! numeric/temporal/uuid/varlena arms calls into PG (`numeric_recv`, `palloc`,
//! detoast, ...), so they cannot run as host `#[test]`s.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::types::{Int16Type, Int32Type};
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array, Int64Array,
        ListArray, RecordBatch, StringArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{Field, Schema};
    use pg_arrow_conv::{
        ArrowBatchSource, ArrowColumnDecoder, ColumnReader, ColumnRule, ConvError,
        DecodedColumn, PgColumnType, resolve_column_rule,
    };
    use pg_lakebase_core::batch::{BatchRowCursor, BatchRowDecoder};
    use pg_lakebase_core::tuple::SlotColumns;
    use pgrx::prelude::*;
    use pgrx::{FromDatum, datum::Uuid, pg_sys};

    /// A virtual slot whose tuple descriptor carries one column per `oids`
    /// entry. The values/isnull arrays are PG-owned, so `SlotColumns` writes
    /// through the same pointers the real scan path uses.
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

    unsafe fn read<T: FromDatum>(
        slot: *mut pg_sys::TupleTableSlot,
        dest: usize,
    ) -> Option<T> {
        unsafe {
            let isnull = *(*slot).tts_isnull.add(dest);
            let datum = *(*slot).tts_values.add(dest);
            T::from_datum(datum, isnull)
        }
    }

    unsafe fn is_null(slot: *mut pg_sys::TupleTableSlot, dest: usize) -> bool {
        unsafe { *(*slot).tts_isnull.add(dest) }
    }

    unsafe fn is_empty(slot: *mut pg_sys::TupleTableSlot) -> bool {
        unsafe { ((*slot).tts_flags as u32) & pg_sys::TTS_FLAG_EMPTY != 0 }
    }

    /// Assemble a `RecordBatch` from arrays, deriving the schema from each
    /// array's own data type so decimal precision/scale and timestamp timezone
    /// stay attached.
    fn batch_of(arrays: Vec<ArrayRef>) -> RecordBatch {
        let fields: Vec<Field> = arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("record batch")
    }

    fn decimal(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .expect("decimal array"),
        )
    }

    fn uuid16(values: Vec<Option<[u8; 16]>>) -> ArrayRef {
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                values.into_iter(),
                16,
            )
            .expect("uuid array"),
        )
    }

    // Each supported scalar/varlena/temporal type, plus a second row that is
    // NULL in every column, decoded straight into a slot.
    #[pg_test]
    fn decodes_each_supported_type_and_null_cell_into_slot() {
        let uuid_bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66,
            0x55, 0x44, 0x00, 0x00,
        ];

        // Each column: (Arrow array with [value, NULL], rule, target oid).
        let plan: Vec<(ArrayRef, ColumnRule, pg_sys::Oid)> = vec![
            (
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                ColumnRule::Bool,
                pg_sys::BOOLOID,
            ),
            (
                Arc::new(Int32Array::from(vec![Some(42), None])),
                ColumnRule::I32,
                pg_sys::INT4OID,
            ),
            (
                Arc::new(Int64Array::from(vec![Some(9_000_000_000), None])),
                ColumnRule::I64,
                pg_sys::INT8OID,
            ),
            (
                Arc::new(Float32Array::from(vec![Some(1.5f32), None])),
                ColumnRule::F32,
                pg_sys::FLOAT4OID,
            ),
            (
                Arc::new(Float64Array::from(vec![Some(2.25f64), None])),
                ColumnRule::F64,
                pg_sys::FLOAT8OID,
            ),
            (
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                ColumnRule::Utf8,
                pg_sys::TEXTOID,
            ),
            (
                Arc::new(BinaryArray::from(vec![Some(&[1u8, 2, 3][..]), None])),
                ColumnRule::Binary,
                pg_sys::BYTEAOID,
            ),
            (
                decimal(vec![Some(12345), None], 10, 2),
                ColumnRule::Decimal128 {
                    precision: 10,
                    scale: 2,
                },
                pg_sys::NUMERICOID,
            ),
            (
                Arc::new(Date32Array::from(vec![Some(0), None])),
                ColumnRule::Date32,
                pg_sys::DATEOID,
            ),
            (
                Arc::new(Time64MicrosecondArray::from(vec![
                    Some(3_600_000_000),
                    None,
                ])),
                ColumnRule::Time64Micros,
                pg_sys::TIMEOID,
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![Some(0), None])),
                ColumnRule::Timestamp {
                    nanos: false,
                    tz: false,
                },
                pg_sys::TIMESTAMPOID,
            ),
            (
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(0), None])
                        .with_timezone("+00:00"),
                ),
                ColumnRule::Timestamp {
                    nanos: false,
                    tz: true,
                },
                pg_sys::TIMESTAMPTZOID,
            ),
            (
                uuid16(vec![Some(uuid_bytes), None]),
                ColumnRule::Uuid,
                pg_sys::UUIDOID,
            ),
        ];

        let arrays: Vec<ArrayRef> = plan.iter().map(|(a, _, _)| a.clone()).collect();
        let oids: Vec<pg_sys::Oid> = plan.iter().map(|(_, _, o)| *o).collect();
        let columns: Vec<DecodedColumn> = plan
            .iter()
            .enumerate()
            .map(|(i, (_, rule, oid))| DecodedColumn::new(rule.clone(), i, i, *oid))
            .collect();

        let batch = batch_of(arrays);
        let decoder = ArrowColumnDecoder::new(columns);
        let bound = decoder.bind(batch).expect("bind batch");
        let natts = oids.len();

        unsafe {
            let slot = make_slot(&oids);

            let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
            decoder
                .write_row(&bound, 0, &mut cols)
                .expect("decode value row");

            assert_eq!(read::<bool>(slot, 0), Some(true));
            assert_eq!(read::<i32>(slot, 1), Some(42));
            assert_eq!(read::<i64>(slot, 2), Some(9_000_000_000));
            assert_eq!(read::<f32>(slot, 3), Some(1.5));
            assert_eq!(read::<f64>(slot, 4), Some(2.25));
            assert_eq!(read::<String>(slot, 5).as_deref(), Some("hello"));
            assert_eq!(read::<Vec<u8>>(slot, 6), Some(vec![1, 2, 3]));
            assert_eq!(
                read::<AnyNumeric>(slot, 7),
                Some(AnyNumeric::try_from("123.45").unwrap())
            );
            assert_eq!(read::<Date>(slot, 8), Some(Date::new(1970, 1, 1).unwrap()));
            assert_eq!(read::<Time>(slot, 9), Some(Time::new(1, 0, 0.0).unwrap()));
            assert_eq!(
                read::<Timestamp>(slot, 10),
                Some(Timestamp::new(1970, 1, 1, 0, 0, 0.0).unwrap())
            );
            // The Arrow column carries tz `+00:00`, so the decoded value is the
            // absolute instant 1970-01-01T00:00:00Z. Pin the expected value to
            // UTC (rather than `new`, which interprets the parts in the session
            // time zone) so the assertion holds regardless of the test backend's
            // `timezone` GUC.
            assert_eq!(
                read::<TimestampWithTimeZone>(slot, 11),
                Some(
                    TimestampWithTimeZone::with_timezone(
                        1970, 1, 1, 0, 0, 0.0, "UTC"
                    )
                    .unwrap()
                )
            );
            assert_eq!(
                read::<Uuid>(slot, 12).map(|u| *u.as_bytes()),
                Some(uuid_bytes)
            );

            // The NULL row marks every mapped position SQL NULL.
            let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
            decoder
                .write_row(&bound, 1, &mut cols)
                .expect("decode null row");
            for dest in 0..natts {
                assert!(is_null(slot, dest), "column {dest} must be SQL NULL");
            }
        }
    }

    // Projection / dropped-column alignment: a mapped value lands at its dest,
    // a null Arrow cell yields a NULL slot, and positions the decoder never
    // touches stay SQL NULL.
    #[pg_test]
    fn unmapped_positions_and_null_cells_stay_null() {
        let batch = batch_of(vec![
            Arc::new(Int32Array::from(vec![Some(10)])),
            Arc::new(Int32Array::from(vec![None])),
        ]);
        // src 0 -> slot 0 (value); src 1 (null) -> slot 3; slots 1 and 2 unmapped.
        let decoder = ArrowColumnDecoder::new(vec![
            DecodedColumn::new(ColumnRule::I32, 0, 0, pg_sys::INT4OID),
            DecodedColumn::new(ColumnRule::I32, 1, 3, pg_sys::INT4OID),
        ]);
        let natts = 4;

        unsafe {
            let slot = make_slot(&[pg_sys::INT4OID; 4]);
            // The scan shim hands the decoder a cleared (all-NULL) slot; model
            // that so the unmapped-position check is meaningful.
            for dest in 0..natts {
                *(*slot).tts_isnull.add(dest) = true;
            }

            let bound = decoder.bind(batch).expect("bind");
            let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
            decoder.write_row(&bound, 0, &mut cols).expect("decode");

            assert_eq!(read::<i32>(slot, 0), Some(10));
            assert!(is_null(slot, 1), "unmapped slot 1 stays NULL");
            assert!(is_null(slot, 2), "unmapped slot 2 stays NULL");
            assert!(is_null(slot, 3), "null Arrow cell yields NULL slot");
        }
    }

    // A decoded varlena is palloc'd in the caller-switched target context, and
    // the held Arrow batch (Rust heap, not a PG context) survives a reset of
    // that context so the next row still decodes.
    #[pg_test]
    fn varlena_lands_in_target_ctx_and_batch_survives_reset() {
        let batch = batch_of(vec![Arc::new(StringArray::from(vec![
            Some("first"),
            Some("second"),
        ]))]);
        let decoder = ArrowColumnDecoder::new(vec![DecodedColumn::new(
            ColumnRule::Utf8,
            0,
            0,
            pg_sys::TEXTOID,
        )]);

        unsafe {
            let tmp_ctx = pg_sys::AllocSetContextCreateExtended(
                pg_sys::CurrentMemoryContext,
                c"decode tmp ctx".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            );
            let slot = make_slot(&[pg_sys::TEXTOID]);

            // Bind once: the bound batch holds Arc-backed Arrow arrays on the
            // Rust heap, so it survives the per-row reset of tmp_ctx below.
            let bound = decoder.bind(batch).expect("bind");

            // Mirror the shim: switch the current context to tmp_ctx so the
            // varlena palloc lands there, then decode.
            let prior = pg_sys::MemoryContextSwitchTo(tmp_ctx);
            let mut cols = SlotColumns::new(slot, tmp_ctx);
            decoder
                .write_row(&bound, 0, &mut cols)
                .expect("decode first");
            pg_sys::MemoryContextSwitchTo(prior);

            let datum = *(*slot).tts_values;
            let chunk_ctx = pg_sys::GetMemoryChunkContext(
                datum.cast_mut_ptr::<core::ffi::c_void>(),
            );
            assert_eq!(
                chunk_ctx, tmp_ctx,
                "varlena must be palloc'd in the target context"
            );

            // Reset tmp_ctx (per-row reset). The bound batch is not in any PG
            // context, so the next row still decodes from it.
            pg_sys::MemoryContextReset(tmp_ctx);

            let prior = pg_sys::MemoryContextSwitchTo(tmp_ctx);
            let mut cols = SlotColumns::new(slot, tmp_ctx);
            decoder
                .write_row(&bound, 1, &mut cols)
                .expect("decode after reset");
            pg_sys::MemoryContextSwitchTo(prior);

            assert_eq!(read::<String>(slot, 0).as_deref(), Some("second"));
        }
    }

    // Driving the cursor the way the shim does: the slot is non-empty after a
    // produced row and stays empty at end-of-scan, so `ExecStoreVirtualTuple`
    // runs exactly once per row and never on end-of-scan.
    #[pg_test]
    fn slot_marked_nonempty_once_per_row_never_at_end_of_scan() {
        let batch = batch_of(vec![Arc::new(Int32Array::from(vec![Some(7)]))]);
        let source = ArrowBatchSource::new(
            vec![Ok::<RecordBatch, ConvError>(batch)].into_iter(),
        );
        let decoder = ArrowColumnDecoder::new(vec![DecodedColumn::new(
            ColumnRule::I32,
            0,
            0,
            pg_sys::INT4OID,
        )]);
        let mut cursor = BatchRowCursor::new(source, decoder);

        unsafe {
            let slot = make_slot(&[pg_sys::INT4OID]);

            // Produced row: clear, decode, store.
            pg_sys::ExecClearTuple(slot);
            assert!(is_empty(slot), "slot is empty before a row is produced");
            let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
            assert!(cursor.next_into_slot(&mut cols).expect("first row"));
            pg_sys::ExecStoreVirtualTuple(slot);
            assert!(!is_empty(slot), "slot is non-empty after a produced row");
            assert_eq!(read::<i32>(slot, 0), Some(7));

            // End of scan: clear, decode returns false, no store.
            pg_sys::ExecClearTuple(slot);
            let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
            assert!(!cursor.next_into_slot(&mut cols).expect("end of scan"));
            assert!(is_empty(slot), "slot stays empty at end of scan");
        }
    }

    // List columns decode straight into the slot via the direct array-datum path
    // (no intermediate `Vec<Cell>`): a populated list (with an interior NULL
    // element), a NULL list cell, and a present-but-empty list each land
    // correctly. Covers int4[] and text[].
    #[pg_test]
    fn decodes_list_columns_into_slot() {
        // int4[]: [ [1, NULL, 3], NULL, [] ]
        let int_list: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None, Some(3)]),
                None,
                Some(vec![]),
            ]));
        let int_rule = resolve_column_rule(
            int_list.data_type(),
            PgColumnType::Array(pg_sys::INT4OID),
        )
        .expect("int list rule");

        // text[]: [ ["a", NULL], NULL, [] ]
        let mut sb = ListBuilder::new(StringBuilder::new());
        sb.values().append_value("a");
        sb.values().append_null();
        sb.append(true);
        sb.append(false);
        sb.append(true); // empty, non-null
        let str_list: ArrayRef = Arc::new(sb.finish());
        let str_rule = resolve_column_rule(
            str_list.data_type(),
            PgColumnType::Array(pg_sys::TEXTOID),
        )
        .expect("text list rule");

        let oids = [pg_sys::INT4ARRAYOID, pg_sys::TEXTARRAYOID];
        let batch = batch_of(vec![int_list, str_list]);
        let decoder = ArrowColumnDecoder::new(vec![
            DecodedColumn::new(int_rule, 0, 0, pg_sys::INT4ARRAYOID),
            DecodedColumn::new(str_rule, 1, 1, pg_sys::TEXTARRAYOID),
        ]);

        unsafe {
            let slot = make_slot(&oids);
            let bound = decoder.bind(batch).expect("bind");

            // Row 0: populated lists.
            let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
            decoder
                .write_row(&bound, 0, &mut cols)
                .expect("decode row 0");
            assert_eq!(
                read::<Vec<Option<i32>>>(slot, 0),
                Some(vec![Some(1), None, Some(3)])
            );
            assert_eq!(
                read::<Vec<Option<String>>>(slot, 1),
                Some(vec![Some("a".to_string()), None])
            );

            // Row 1: NULL list cells.
            let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
            decoder
                .write_row(&bound, 1, &mut cols)
                .expect("decode row 1");
            assert!(is_null(slot, 0), "NULL int4[] cell");
            assert!(is_null(slot, 1), "NULL text[] cell");

            // Row 2: present-but-empty lists decode to empty arrays, not NULL.
            let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
            decoder
                .write_row(&bound, 2, &mut cols)
                .expect("decode row 2");
            assert_eq!(read::<Vec<Option<i32>>>(slot, 0), Some(vec![]));
            assert_eq!(read::<Vec<Option<String>>>(slot, 1), Some(vec![]));
        }
    }

    // Canonical case: when the Arrow physical element representation already
    // matches the target element type (so no element-OID retarget is needed),
    // the slot-first direct array datum and the row-world `Cell` path
    // (`read_cell` + `into_datum_typed`) must agree. This pins that the direct
    // path is a faithful shortcut *for these cases*. The divergent retarget
    // cases are covered separately by `retarget_list_datum_targets_element_oid`,
    // where the Cell path is deliberately the wrong oracle.
    #[pg_test]
    fn canonical_list_datum_matches_cell_path() {
        unsafe fn assert_parity<T>(
            rule: &ColumnRule,
            array: ArrayRef,
            array_oid: pg_sys::Oid,
        ) where
            T: FromDatum + std::fmt::Debug + PartialEq,
        {
            unsafe {
                // Direct: bind the one-column batch and decode row 0 into a slot.
                let decoder = ArrowColumnDecoder::new(vec![DecodedColumn::new(
                    rule.clone(),
                    0,
                    0,
                    array_oid,
                )]);
                let bound =
                    decoder.bind(batch_of(vec![array.clone()])).expect("bind");
                let slot = make_slot(&[array_oid]);
                let mut cols = SlotColumns::new(slot, pg_sys::CurrentMemoryContext);
                decoder.write_row(&bound, 0, &mut cols).expect("write_row");
                let direct = read::<T>(slot, 0);

                // Cell path: read a `Cell` via the same bound reader and
                // materialize it for the same target oid.
                let cell = ColumnReader::bind(rule, array.as_ref())
                    .expect("bind cell reader")
                    .read_cell(0)
                    .expect("read_cell")
                    .expect("present cell");
                let via_cell = cell
                    .into_datum_typed(array_oid, -1)
                    .expect("cell into_datum_typed");
                assert_eq!(
                    direct,
                    T::from_datum(via_cell, false),
                    "direct array datum diverged from the Cell path"
                );
            }
        }

        // int4[]: Int32 physical -> int4 target (widths match).
        let int_list: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None, Some(3)]),
            ]));
        let int_rule = resolve_column_rule(
            int_list.data_type(),
            PgColumnType::Array(pg_sys::INT4OID),
        )
        .expect("int list rule");

        // int2[] backed by an Int16 element source: physical i16 already matches
        // the int2 target, so both paths agree. (Resolve from Int32 — the only
        // schema `Int` resolves from — then apply to a narrower Int16 array.)
        let i16_schema: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1)]),
            ]));
        let i16_rule = resolve_column_rule(
            i16_schema.data_type(),
            PgColumnType::Array(pg_sys::INT2OID),
        )
        .expect("int list rule for int16 source");
        let i16_list: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int16Type, _, _>(vec![
                Some(vec![Some(7i16), None, Some(9)]),
            ]));

        // text[]: Utf8 physical -> text target (no retarget).
        let mut sb = ListBuilder::new(StringBuilder::new());
        sb.values().append_value("x");
        sb.values().append_null();
        sb.values().append_value("z");
        sb.append(true);
        let str_list: ArrayRef = Arc::new(sb.finish());
        let str_rule = resolve_column_rule(
            str_list.data_type(),
            PgColumnType::Array(pg_sys::TEXTOID),
        )
        .expect("text list rule");

        unsafe {
            assert_parity::<Vec<Option<i32>>>(
                &int_rule,
                int_list,
                pg_sys::INT4ARRAYOID,
            );
            assert_parity::<Vec<Option<i16>>>(
                &i16_rule,
                i16_list,
                pg_sys::INT2ARRAYOID,
            );
            assert_parity::<Vec<Option<String>>>(
                &str_rule,
                str_list,
                pg_sys::TEXTARRAYOID,
            );
        }
    }

    // Retarget cases: the produced array's element type must come from the
    // rule's declared element OID, *not* the Arrow physical type. Here the Cell
    // path would be the wrong oracle (it keeps the physical element type), so
    // these assert `read_datum`'s output directly off the `ArrayType` header.
    //
    // - Physical `Int32` targeting `int2[]`: asserts both the element OID and
    //   the narrowed values, whereas `Cell::I32Array` -> `into_datum_typed`
    //   would yield `int4[]`.
    // - Physical `Utf8` targeting `name[]`: asserts the element OID only (the
    //   text-family retarget signal — `Cell::StringArray` -> `into_datum_typed`
    //   would yield `text[]`). The values are not read back here because `name`
    //   is a fixed 64-byte type, not a varlena, so a generic `String` array
    //   read-back is not reliable.
    #[pg_test]
    fn retarget_list_datum_targets_element_oid() {
        unsafe fn array_elem_oid(datum: pg_sys::Datum) -> pg_sys::Oid {
            let arr = datum.cast_mut_ptr::<pg_sys::ArrayType>();
            unsafe { (*arr).elemtype }
        }

        unsafe fn read_list_datum(
            rule: &ColumnRule,
            array: &ArrayRef,
        ) -> pg_sys::Datum {
            let reader =
                ColumnReader::bind(rule, array.as_ref()).expect("bind reader");
            // List ignores the (oid, typmod) args; the element type comes from
            // the rule's bound elem_oid.
            unsafe { reader.read_datum(0, pg_sys::InvalidOid, -1) }
                .expect("read_datum")
                .expect("present list cell")
        }

        // Physical Int32 -> int2[] (narrowing retarget).
        let i32_list: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None, Some(3)]),
            ]));
        let int2_rule = resolve_column_rule(
            i32_list.data_type(),
            PgColumnType::Array(pg_sys::INT2OID),
        )
        .expect("int2[] rule");

        // Physical Utf8 -> name[] (text-family element-OID retarget).
        let mut sb = ListBuilder::new(StringBuilder::new());
        sb.values().append_value("x");
        sb.values().append_null();
        sb.values().append_value("z");
        sb.append(true);
        let str_list: ArrayRef = Arc::new(sb.finish());
        let name_rule = resolve_column_rule(
            str_list.data_type(),
            PgColumnType::Array(pg_sys::NAMEOID),
        )
        .expect("name[] rule");

        unsafe {
            let int2_datum = read_list_datum(&int2_rule, &i32_list);
            assert_eq!(
                array_elem_oid(int2_datum),
                pg_sys::INT2OID,
                "Int32 source targeting int2[] must produce int2 elements"
            );
            assert_eq!(
                Vec::<Option<i16>>::from_datum(int2_datum, false),
                Some(vec![Some(1i16), None, Some(3i16)]),
                "int2[] values must be the narrowed elements"
            );

            let name_datum = read_list_datum(&name_rule, &str_list);
            assert_eq!(
                array_elem_oid(name_datum),
                pg_sys::NAMEOID,
                "Utf8 source targeting name[] must produce name elements, \
                 not text"
            );
        }
    }
}
