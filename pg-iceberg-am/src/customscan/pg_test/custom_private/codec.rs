//! Backend tests for Iceberg private-data codec error surfaces.

#[pgrx::pg_schema]
mod tests {
    use core::ptr;

    use pgrx::pg_sys;

    /// Assert a codec-layer [`CustomScanError`] reports as INTERNAL with expected MESSAGE/DETAIL.
    ///
    /// AM tests must not match codec-internal variants; only the public error surface.
    fn assert_codec_custom_scan_error_report(
        err: pg_lakebase_core::customscan::provider::CustomScanError,
        message_needles: &[&str],
        detail_needles: &[&str],
    ) {
        use pg_lakebase_core::diag::SqlStateError;
        use pgrx::pg_sys::panic::ErrorReport;
        use pgrx::prelude::PgSqlErrorCode;

        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
        let report = ErrorReport::from(err);
        let message = report.message();
        for needle in message_needles {
            assert!(
                message.contains(needle),
                "expected MESSAGE to contain {needle:?}, got {message:?}"
            );
        }
        let detail = report
            .detail()
            .expect("codec errors should include DETAIL from the source chain");
        for needle in detail_needles {
            assert!(
                detail.contains(needle),
                "expected DETAIL to contain {needle:?}, got {detail:?}"
            );
        }
    }

    /// Extra trailing cells in decode payload → `UnexpectedTrailingCells`.
    #[pgrx::pg_test(schema = "tests")]
    fn iceberg_private_decode_rejects_extra_cells() {
        use pg_lakebase_core::customscan::codec::PrivateDataReader;
        use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;

        use crate::customscan::provider::IcebergPrivateData;

        unsafe {
            // Build a length-2 `T_List` via `lappend` + `makeInteger`.
            let mut list: *mut pg_sys::List = ptr::null_mut();
            list = pg_sys::lappend(list, pg_sys::makeInteger(11).cast());
            list = pg_sys::lappend(list, pg_sys::makeInteger(22).cast());
            assert_eq!((*list).length, 2, "fixture list must have length 2");

            let mut reader = PrivateDataReader::from_list(list);
            match IcebergPrivateData::decode(&mut reader) {
                Ok(_) => assert_codec_custom_scan_error_report(
                    reader.finish().unwrap_err(),
                    &[
                        "custom_private codec error",
                        "unexpected trailing cells",
                        "read 1",
                        "len 2",
                    ],
                    &["unexpected trailing cells", "read 1", "len 2"],
                ),
                Err(e) => panic!(
                    "decode of a valid first cell (T_Integer 11) should succeed, \
                 got {e:?}"
                ),
            }
        }
    }

    /// NULL decode payload fails closed with `ReadPastEnd`.
    #[pgrx::pg_test(schema = "tests")]
    fn iceberg_private_decode_null_fails_closed() {
        use pg_lakebase_core::customscan::codec::PrivateDataReader;
        use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;

        use crate::customscan::provider::IcebergPrivateData;

        unsafe {
            let mut reader = PrivateDataReader::from_list(ptr::null_mut());
            match IcebergPrivateData::decode(&mut reader) {
                Err(err) => assert_codec_custom_scan_error_report(
                    err,
                    &[
                        "custom_private codec error",
                        "read past end of payload",
                        "position 0",
                        "len 0",
                    ],
                    &["read past end of payload", "position 0", "len 0"],
                ),
                other => {
                    panic!("expected decode Err for a NULL payload, got {other:?}")
                }
            }
        }
    }
}
