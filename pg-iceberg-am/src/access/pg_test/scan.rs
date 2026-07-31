//! Backend tests for Iceberg scan batch adaptation.
//!
//! Advancing [`IcebergArrowBatches`] calls PostgreSQL's interrupt checker, so
//! this test must execute inside a PostgreSQL backend.

#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use iceberg_lite::scan::ArrowRecordBatchIterator;

    use crate::access::scan::batch::IcebergArrowBatches;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]))
    }

    #[pgrx::pg_test(schema = "tests")]
    fn arrow_batch_adapter_preserves_empty_batches() {
        let empty = RecordBatch::new_empty(schema());
        let non_empty =
            RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vec![1]))])
                .expect("valid test batch");
        let batches: ArrowRecordBatchIterator =
            Box::new(vec![Ok(empty.clone()), Ok(non_empty.clone())].into_iter());
        let mut adapted = IcebergArrowBatches(batches);

        assert_eq!(adapted.next().unwrap().unwrap(), empty);
        assert_eq!(adapted.next().unwrap().unwrap(), non_empty);
        assert!(adapted.next().is_none());
    }
}
