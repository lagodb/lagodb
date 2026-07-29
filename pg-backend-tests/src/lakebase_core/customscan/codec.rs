//! Backend proptest coverage for `PrivateDataWriter` / `PrivateDataReader`.
//! Host-side overflow and carrier tests live in `pg-lakebase-core`.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pg_lakebase_core::customscan::provider::CustomScanError;
    use pg_lakebase_core::customscan::provider::{
        PrivateDataReader, PrivateDataWriter,
    };

    fn assert_private_codec_message(err: CustomScanError, needles: &[&str]) {
        use pg_lakebase_core::diag::SqlStateError;
        use pgrx::prelude::PgSqlErrorCode;

        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
        let text = err.to_string();
        assert!(
            text.contains("custom_private codec error"),
            "expected codec wrapper in {text:?}"
        );
        for needle in needles {
            assert!(
                text.contains(needle),
                "expected message to contain {needle:?}, got {text:?}"
            );
        }
    }
    use pgrx::pg_sys;
    use pgrx::pg_test;

    use proptest::prelude::*;
    use proptest::test_runner::{TestCaseError, TestRunner};

    /// One logical codec field; a `Vec<Field>` is a full payload shape.
    #[derive(Clone, Debug, PartialEq)]
    enum Field {
        Oid(u32),
        I32(i32),
        Count(usize),
        I64(i64),
        Bool(bool),
        Str(String),
        Nested(Vec<Field>),
    }

    fn write_fields(writer: &mut PrivateDataWriter, fields: &[Field]) {
        for field in fields {
            match field {
                Field::Oid(v) => {
                    writer.append_oid(pg_sys::Oid::from(*v));
                }
                Field::I32(v) => {
                    writer.append_i32(*v);
                }
                Field::Count(v) => {
                    writer.append_count(*v);
                }
                Field::I64(v) => {
                    writer.append_i64(*v);
                }
                Field::Bool(v) => {
                    writer.append_bool(*v);
                }
                Field::Str(v) => {
                    writer.append_str(v);
                }
                Field::Nested(inner) => {
                    writer.append_nested(|child| write_fields(child, inner));
                }
            }
        }
    }

    fn read_fields(
        reader: &mut PrivateDataReader<'_>,
        shape: &[Field],
    ) -> Result<Vec<Field>, CustomScanError> {
        let mut out = Vec::with_capacity(shape.len());
        for field in shape {
            let value = match field {
                Field::Oid(_) => Field::Oid(reader.read_oid()?.to_u32()),
                Field::I32(_) => Field::I32(reader.read_i32()?),
                Field::Count(_) => Field::Count(reader.read_count()?),
                Field::I64(_) => Field::I64(reader.read_i64()?),
                Field::Bool(_) => Field::Bool(reader.read_bool()?),
                Field::Str(_) => Field::Str(reader.read_str()?),
                Field::Nested(inner) => {
                    let mut sub = reader.read_nested()?;
                    let nested = read_fields(&mut sub, inner)?;
                    sub.finish()?;
                    Field::Nested(nested)
                }
            };
            out.push(value);
        }
        Ok(out)
    }

    /// OID values biased toward round-trip corner cases (0, high bit, > i32::MAX).
    fn arb_oid() -> impl Strategy<Value = u32> {
        prop_oneof![
            Just(0u32),
            Just(u32::MAX),
            Just(0x8000_0000u32),
            Just(i32::MAX as u32 + 1),
            any::<u32>(),
        ]
    }

    /// `i64` values biased outside the `i32` range (exercises `T_Float` path).
    fn arb_i64() -> impl Strategy<Value = i64> {
        prop_oneof![
            Just(0i64),
            Just(i64::MAX),
            Just(i64::MIN),
            Just(i32::MAX as i64 + 1),
            Just(i32::MIN as i64 - 1),
            any::<i64>(),
        ]
    }

    fn arb_str() -> impl Strategy<Value = String> {
        prop_oneof![
            1 => Just(String::new()),
            4 => proptest::collection::vec(
                    any::<char>().prop_filter("no interior NUL", |c| *c != '\0'),
                    0..8,
                )
                .prop_map(|chars| chars.into_iter().collect::<String>()),
        ]
    }

    /// Recursive strategy for full payload shapes (nested up to 4 levels).
    fn arb_fields() -> impl Strategy<Value = Vec<Field>> {
        let leaf = prop_oneof![
            arb_oid().prop_map(Field::Oid),
            any::<i32>().prop_map(Field::I32),
            (0..=i32::MAX).prop_map(|v| Field::Count(v as usize)),
            arb_i64().prop_map(Field::I64),
            any::<bool>().prop_map(Field::Bool),
            arb_str().prop_map(Field::Str),
        ];
        let field = leaf.prop_recursive(4, 32, 4, |inner| {
            proptest::collection::vec(inner, 0..4).prop_map(Field::Nested)
        });
        proptest::collection::vec(field, 0..8)
    }

    fn roundtrip_case(fields: &[Field]) -> Result<(), TestCaseError> {
        // SAFETY: `#[pg_test]` — `List*` from `finish()` is valid for the read below.
        unsafe {
            let mut writer = PrivateDataWriter::new();
            write_fields(&mut writer, fields);
            let list = writer
                .finish()
                .map_err(|e| TestCaseError::fail(format!("encode failed: {e:?}")))?;

            let mut reader = PrivateDataReader::from_list(list);
            let decoded = read_fields(&mut reader, fields)
                .map_err(|e| TestCaseError::fail(format!("decode failed: {e:?}")))?;
            reader
                .finish()
                .map_err(|e| TestCaseError::fail(format!("finish failed: {e:?}")))?;

            prop_assert_eq!(
                &decoded,
                fields,
                "encode->decode did not reproduce the original field sequence"
            );
        }
        Ok(())
    }

    #[pg_test]
    fn codec_roundtrip_fidelity() {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);
        runner
            .run(&arb_fields(), |fields| roundtrip_case(&fields))
            .expect("codec round-trip fidelity failed");
    }

    fn copyobject_case(fields: &[Field]) -> Result<(), TestCaseError> {
        // SAFETY: `#[pg_test]` — `copyObjectImpl` deep-copies like cached-plan reuse.
        unsafe {
            let mut writer = PrivateDataWriter::new();
            write_fields(&mut writer, fields);
            let list = writer
                .finish()
                .map_err(|e| TestCaseError::fail(format!("encode failed: {e:?}")))?;

            // NULL payload stays NULL — `copyObjectImpl` rejects NULL input.
            let copied = if list.is_null() {
                std::ptr::null_mut()
            } else {
                pg_sys::copyObjectImpl(list.cast()) as *mut pg_sys::List
            };

            let mut reader = PrivateDataReader::from_list(copied);
            let decoded = read_fields(&mut reader, fields).map_err(|e| {
                TestCaseError::fail(format!("decode after copy failed: {e:?}"))
            })?;
            reader.finish().map_err(|e| {
                TestCaseError::fail(format!("finish after copy failed: {e:?}"))
            })?;

            prop_assert_eq!(
                &decoded,
                fields,
                "round-trip was not preserved across a copyObject boundary"
            );
        }
        Ok(())
    }

    #[pg_test]
    fn codec_copyobject_invariance() {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);
        runner
            .run(&arb_fields(), |fields| copyobject_case(&fields))
            .expect("codec copyObject-invariance failed");
    }

    #[pg_test]
    fn codec_append_count_rejects_overflow() {
        for value in [i32::MAX as usize + 1, usize::MAX] {
            let mut writer = PrivateDataWriter::new();
            writer.append_count(value);
            let err = writer
                .finish()
                .expect_err("overflow count must be rejected before finish");
            assert_private_codec_message(
                err,
                &["cannot encode count", "exceeds i32::MAX"],
            );
        }
    }

    #[pg_test]
    fn codec_empty_reader_reports_read_past_end() {
        unsafe {
            let reader = PrivateDataReader::from_list(std::ptr::null_mut());
            assert_eq!(reader.remaining(), 0);
            reader
                .finish()
                .expect("empty reader has no unread trailing cells");

            let mut reader = PrivateDataReader::from_list(std::ptr::null_mut());
            let err = reader
                .read_oid()
                .expect_err("reading from an empty payload must fail closed");
            assert_private_codec_message(
                err,
                &["read past end of payload", "position 0", "len 0"],
            );
        }
    }

    /// One representative per distinct PG value-node tag class.
    #[derive(Clone, Copy, Debug)]
    enum TagKind {
        Integer,
        Float,
        Boolean,
        Str,
        List,
    }

    const TAG_KINDS: [TagKind; 5] = [
        TagKind::Integer,
        TagKind::Float,
        TagKind::Boolean,
        TagKind::Str,
        TagKind::List,
    ];

    /// Write one cell; `List` uses a non-empty nested payload (empty → NIL/NullCell).
    fn write_one(writer: &mut PrivateDataWriter, kind: TagKind) {
        match kind {
            TagKind::Integer => {
                writer.append_i32(7);
            }
            TagKind::Float => {
                writer.append_i64(1_234_567_890_123_i64);
            }
            TagKind::Boolean => {
                writer.append_bool(true);
            }
            TagKind::Str => {
                writer.append_str("tag");
            }
            TagKind::List => {
                writer.append_nested(|child| {
                    child.append_i32(1);
                });
            }
        }
    }

    fn read_one(
        reader: &mut PrivateDataReader<'_>,
        kind: TagKind,
    ) -> Result<(), CustomScanError> {
        match kind {
            TagKind::Integer => reader.read_i32().map(|_| ()),
            TagKind::Float => reader.read_i64().map(|_| ()),
            TagKind::Boolean => reader.read_bool().map(|_| ()),
            TagKind::Str => reader.read_str().map(|_| ()),
            TagKind::List => reader.read_nested().map(|_| ()),
        }
    }

    fn wrong_tag_case(
        write_idx: usize,
        read_idx: usize,
    ) -> Result<(), TestCaseError> {
        let write_kind = TAG_KINDS[write_idx];
        let read_kind = TAG_KINDS[read_idx];

        // SAFETY: `#[pg_test]` — `List*` is valid for the read below.
        unsafe {
            let mut writer = PrivateDataWriter::new();
            write_one(&mut writer, write_kind);
            let list = writer
                .finish()
                .map_err(|e| TestCaseError::fail(format!("encode failed: {e:?}")))?;

            let mut reader = PrivateDataReader::from_list(list);
            match read_one(&mut reader, read_kind) {
                Err(err) => {
                    assert_private_codec_message(err, &["wrong NodeTag", "cell 0"])
                }
                Ok(()) => prop_assert!(
                    false,
                    "expected WrongNodeTag (wrote {:?}, read {:?}), got Ok",
                    write_kind,
                    read_kind,
                ),
            }
        }
        Ok(())
    }

    #[pg_test]
    fn codec_wrong_tag_read() {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);
        let strategy = (0usize..TAG_KINDS.len(), 0usize..TAG_KINDS.len())
            .prop_filter("distinct tag classes", |(w, r)| w != r);
        runner
            .run(&strategy, |(w, r)| wrong_tag_case(w, r))
            .expect("codec wrong-tag read failed");
    }

    fn length_mismatch_case(
        n: usize,
        over: usize,
        under: usize,
    ) -> Result<(), TestCaseError> {
        // SAFETY: `#[pg_test]` — both `List*`s are valid for the reads below.
        unsafe {
            {
                let mut writer = PrivateDataWriter::new();
                for i in 0..n {
                    writer.append_i32(i as i32);
                }
                let list = writer.finish().map_err(|e| {
                    TestCaseError::fail(format!("encode (a) failed: {e:?}"))
                })?;

                let mut reader = PrivateDataReader::from_list(list);
                for _ in 0..n {
                    reader.read_i32().map_err(|e| {
                        TestCaseError::fail(format!("in-bounds read failed: {e:?}"))
                    })?;
                }
                for _ in 0..over {
                    match reader.read_i32() {
                        Err(err) => assert_private_codec_message(
                            err,
                            &["read past end of payload"],
                        ),
                        Ok(_) => prop_assert!(
                            false,
                            "expected ReadPastEnd at end of {}-cell payload, got Ok",
                            n,
                        ),
                    }
                }
            }

            {
                let mut writer = PrivateDataWriter::new();
                for i in 0..n {
                    writer.append_i32(i as i32);
                }
                let list = writer.finish().map_err(|e| {
                    TestCaseError::fail(format!("encode (b) failed: {e:?}"))
                })?;

                let mut reader = PrivateDataReader::from_list(list);
                for _ in 0..under {
                    reader.read_i32().map_err(|e| {
                        TestCaseError::fail(format!("under-read failed: {e:?}"))
                    })?;
                }
                match reader.finish() {
                    Err(err) => assert_private_codec_message(
                        err,
                        &["unexpected trailing cells"],
                    ),
                    Ok(()) => prop_assert!(
                        false,
                        "expected UnexpectedTrailingCells (wrote {}, read {}), got Ok",
                        n,
                        under,
                    ),
                }
            }
        }
        Ok(())
    }

    #[pg_test]
    fn codec_length_mismatch() {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);
        let strategy = (1usize..=8, 1usize..=4, 0usize..8);
        runner
            .run(&strategy, |(n, over, under_raw)| {
                let under = under_raw % n;
                length_mismatch_case(n, over, under)
            })
            .expect("codec length-mismatch failed");
    }
}
