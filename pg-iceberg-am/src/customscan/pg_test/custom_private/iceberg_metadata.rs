//! Backend tests for Iceberg custom_private OID payload round-trip.

#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_sys;
    use proptest::prelude::*;
    use proptest::test_runner::TestRunner;

    /// Generator for `Oid` bit-patterns including corner cases.
    fn arb_oid_bits() -> impl Strategy<Value = u32> {
        prop_oneof![
            Just(0u32),                // InvalidOid
            Just(u32::MAX),            // all bits set
            Just(0x8000_0000u32),      // top bit only (> i32::MAX)
            Just(i32::MAX as u32),     // largest positive i32 pattern
            Just(i32::MAX as u32 + 1), // first pattern that bitcasts negative
            Just(1u32),                // smallest non-invalid OID
            any::<u32>(),              // uniform coverage of the whole space
        ]
    }

    /// One OID round-trip case; returns `Err` on assertion failure.
    fn iceberg_oid_round_trip_case(v: u32) -> Result<(), TestCaseError> {
        use pg_lakebase_core::customscan::codec::{
            PrivateDataReader, PrivateDataWriter,
        };
        use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;

        use crate::customscan::provider::IcebergPrivateData;

        // SAFETY: every codec call allocates/traverses PG value nodes in the
        // `#[pg_test]` backend's per-query memory context, which is cleaned up at
        // test exit.
        unsafe {
            let original = IcebergPrivateData {
                tablespace_oid: pg_sys::Oid::from(v),
            };

            // Encode through the typed writer.
            let mut writer = PrivateDataWriter::new();
            original.encode(&mut writer).map_err(|e| {
                TestCaseError::fail(format!("encode failed for oid={v}: {e:?}"))
            })?;
            let encoded = writer.finish().map_err(|e| {
                TestCaseError::fail(format!("finish failed for oid={v}: {e:?}"))
            })?;
            prop_assert!(
                !encoded.is_null(),
                "encode of a single OID must produce a non-NULL length-1 list (oid={})",
                v
            );

            // Decode through the typed reader.
            let mut reader = PrivateDataReader::from_list(encoded);
            let decoded = IcebergPrivateData::decode(&mut reader).map_err(|e| {
                TestCaseError::fail(format!("decode failed for oid={v}: {e:?}"))
            })?;

            // The payload must have had exactly one cell — no trailing data.
            reader.finish().map_err(|e| {
                TestCaseError::fail(format!(
                    "finish (reader) failed for oid={v}: {e:?}"
                ))
            })?;

            // Bitwise round-trip, including InvalidOid (0).
            prop_assert_eq!(
                decoded.tablespace_oid,
                pg_sys::Oid::from(v),
                "OID round-trip mismatch: encoded {} but decoded {}",
                v,
                decoded.tablespace_oid.to_u32()
            );
        }

        Ok(())
    }

    /// IcebergPrivateData tablespace OID round-trips for any `Oid` value.
    #[pgrx::pg_test(schema = "tests")]
    fn iceberg_private_oid_round_trip_property() {
        let config = ProptestConfig {
            // 256 cases; no file persistence in the PG backend harness.
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);

        runner
            .run(&arb_oid_bits(), iceberg_oid_round_trip_case)
            .expect("Iceberg OID round-trip property failed");
    }

    /// Iceberg provider metadata survives the full custom_private envelope path.
    #[pgrx::pg_test(schema = "tests")]
    fn iceberg_private_full_envelope_copyobject_round_trip() {
        use pg_lakebase_core::customscan::codec::{
            PrivateDataReader, PrivateDataWriter,
        };
        use pg_lakebase_core::customscan::custom_private::{
            CustomScanPrivate, encode_split,
        };
        use pg_lakebase_core::expr::split::{ColumnRef, PushdownContract};

        use crate::customscan::provider::IcebergPrivateData;

        let tablespace_oid = pg_sys::Oid::from(16_385u32);
        let private = IcebergPrivateData { tablespace_oid };

        let mut writer = PrivateDataWriter::new();
        private
            .encode(&mut writer)
            .expect("IcebergPrivateData::encode is infallible for a single OID");
        let provider_metadata = writer
            .finish()
            .expect("writer.finish is infallible for a single appended OID");

        let provider_name = c"pg-iceberg-am";
        let relation_oid = pg_sys::Oid::from(16_384u32);
        let pushed_contracts = vec![PushdownContract::ExactRowFilter];
        let column_refs = vec![ColumnRef {
            expr_index: 0,
            rel_oid: relation_oid,
            attno: 1,
            atttypid: pg_sys::INT4OID,
            attcollation: pg_sys::Oid::INVALID,
            name: Some("k".to_string()),
        }];

        unsafe {
            let envelope = encode_split(
                provider_name,
                relation_oid,
                1,
                0,
                &pushed_contracts,
                &column_refs,
                provider_metadata,
                1,
            )
            .expect("encode_split: synthetic counts are within i32::MAX");
            assert!(!envelope.is_null(), "encode_split returned NULL");

            let copied = pg_sys::copyObjectImpl(envelope.cast()) as *mut pg_sys::List;
            assert!(!copied.is_null(), "copyObjectImpl returned NULL");

            let metadata = pg_sys::list_nth(copied, 6) as *mut pg_sys::List;
            assert!(
                !metadata.is_null(),
                "provider_metadata cell must survive copyObject"
            );

            let mut reader = PrivateDataReader::from_list(metadata);
            let decoded = IcebergPrivateData::decode(&mut reader)
                .expect("Iceberg provider metadata must decode after copyObject");
            reader
                .finish()
                .expect("Iceberg provider metadata must have exactly one cell");

            assert_eq!(
                decoded.tablespace_oid, tablespace_oid,
                "tablespace OID must round-trip through IcebergPrivateData -> \
             encode_split -> copyObject -> IcebergPrivateData::decode",
            );
        }
    }
}
