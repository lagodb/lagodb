pub mod iceberg_catalog;
pub mod iceberg_metadata;
pub mod metadata_tracking;
pub mod schema_mapping;
pub mod table_handler;

use crate::get_iceberg_am_routine_ptr;
use pg_lakebase_core::handles::RelationHandle;

pub use metadata_tracking::{
    get_or_rebase_metadata_location, process_new_data_files, record_metadata_change,
    register_table_for_tracking,
};
pub use table_handler::{generate_table_location, init_table_storage_metadata};

#[inline]
pub fn is_iceberg_table(rel: &RelationHandle) -> bool {
    let rel_ptr = rel.as_raw();
    if rel_ptr.is_null() {
        return false;
    }

    unsafe {
        let rd_tableam = (*rel_ptr).rd_tableam;
        if rd_tableam.is_null() {
            return false;
        }

        std::ptr::eq(rd_tableam, get_iceberg_am_routine_ptr())
    }
}
