//! Storage DDL callbacks: new file locators, truncate, copy, and CLUSTER.

use pgrx::pg_sys;

use super::{AmResult, unsupported_callback};
use crate::handles::{RelFileLocator, RelationHandle};

pub trait AmDdl {
    /// Remove all rows from one provider-owned logical table transactionally.
    ///
    /// PostgreSQL reaches this operation through its relfilenumber callbacks.
    fn truncate(rel: &RelationHandle) -> AmResult<()>
    where
        Self: Sized;

    fn relation_set_new_filelocator(
        rel: &RelationHandle,
        newrlocator: &RelFileLocator,
        persistence: u8,
    ) -> AmResult<(pg_sys::TransactionId, pg_sys::MultiXactId)>
    where
        Self: Sized;

    fn relation_nontransactional_truncate(rel: &RelationHandle) -> AmResult<()>
    where
        Self: Sized;

    fn relation_copy_data(
        rel: &RelationHandle,
        newrlocator: &RelFileLocator,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (rel, newrlocator);
        unsupported_callback("relation_copy_data")
    }

    fn relation_copy_for_cluster(
        old_table: &RelationHandle,
        new_table: &RelationHandle,
        old_index: Option<&RelationHandle>,
        use_sort: bool,
        oldest_xmin: pg_sys::TransactionId,
    ) -> AmResult<(pg_sys::TransactionId, pg_sys::MultiXactId, f64, f64, f64)>
    where
        Self: Sized,
    {
        let _ = (old_table, new_table, old_index, use_sort, oldest_xmin);
        unsupported_callback("relation_copy_for_cluster")
    }
}
