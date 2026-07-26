//! Backend tests for the Arrow batch source and decoder-plan validation.
//!
//! These tests use the `AmError` callback boundary and the decoder constructor,
//! whose validation path can reference PostgreSQL backend symbols. Keep them in
//! the backend test extension instead of the `pg-arrow-conv` host test binary.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use pg_arrow_conv::{
        ArrowBatchSource, ArrowConversionError, ColumnRule, DatumCodec, DecodedColumn,
    };
    use pg_lakebase_core::batch::AmScanBatchSource;
    use pgrx::pg_sys;
    use pgrx::prelude::*;

    fn batch(values: &[i32]) -> RecordBatch {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .expect("record batch")
    }

    #[pg_test]
    fn prevalidated_json_text_codec_rejects_non_json_target() {
        let codec = unsafe { DatumCodec::prevalidated_json_text() };
        let result = unsafe {
            DecodedColumn::new(ColumnRule::Utf8, 0, 0, pg_sys::INT4OID, codec)
        };
        assert!(result.is_err());
    }

    #[pg_test]
    fn postgres_jsonb_varlena_codec_rejects_non_jsonb_target() {
        let codec = unsafe { DatumCodec::postgres_jsonb_varlena() };
        let result = unsafe {
            DecodedColumn::new(
                ColumnRule::PostgresJsonbVarlena,
                0,
                0,
                pg_sys::BYTEAOID,
                codec,
            )
        };
        assert!(result.is_err());
    }

    #[pg_test]
    fn forwards_batches_in_order_then_ends() {
        let items: Vec<Result<RecordBatch, ArrowConversionError>> =
            vec![Ok(batch(&[1, 2])), Ok(batch(&[3]))];
        let mut source = ArrowBatchSource::new(items.into_iter());

        assert_eq!(source.next_batch().unwrap().unwrap().num_rows(), 2);
        assert_eq!(source.next_batch().unwrap().unwrap().num_rows(), 1);
        assert!(source.next_batch().unwrap().is_none());
    }

    #[pg_test]
    fn lifts_iterator_error() {
        let items: Vec<Result<RecordBatch, ArrowConversionError>> = vec![Err(
            ArrowConversionError::InvariantViolated("batch source failure"),
        )];
        let mut source = ArrowBatchSource::new(items.into_iter());

        assert!(source.next_batch().is_err());
    }
}
