//! Backend-test construction support for relation binding fixtures.

use pgrx::pg_sys;

use super::{LiveColumn, RelationShape};

impl RelationShape {
    pub(crate) fn for_test(
        live_columns: Vec<LiveColumn>,
        slot_width: usize,
        attr_types: Vec<(pg_sys::Oid, i32)>,
    ) -> Self {
        Self {
            live_columns,
            slot_width,
            attr_types,
        }
    }
}
