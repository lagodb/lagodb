//! PostgreSQL-backend tests for the `rd_amcache` table-option layout.

use super::*;
use pgrx::pg_sys;

// pgrx-tests invokes every `#[pg_test]` through the SQL schema `tests`.
#[pgrx::pg_schema]
mod tests {}

#[pgrx::pg_test(schema = "tests")]
fn table_option_cache_reads_strings_from_contiguous_pg_allocation() {
    let options = TableOptions::new(vec![
        (OPT_COMPRESSION_CODEC.to_owned(), Some("snappy".to_owned())),
        (OPT_WRITE_FORMAT.to_owned(), Some("parquet".to_owned())),
    ]);
    let (header, data) = IcebergTableOptionCache::from_options(Some(&options))
        .expect("valid options must build an AM cache");
    let header_size = std::mem::size_of::<IcebergTableOptionCache>();
    let allocation_size = header_size
        .checked_add(data.len())
        .expect("cache fixture allocation size overflowed");

    unsafe {
        // SAFETY: `palloc` returns MAXALIGN'd storage of `allocation_size`
        // bytes. The header and its complete nul-terminated tail are copied
        // into the same allocation before a typed reference is formed.
        let allocation = pg_sys::palloc(allocation_size).cast::<u8>();
        std::ptr::copy_nonoverlapping(
            (&header as *const IcebergTableOptionCache).cast::<u8>(),
            allocation,
            header_size,
        );
        std::ptr::copy_nonoverlapping(
            data.as_ptr(),
            allocation.add(header_size),
            data.len(),
        );

        let cached = &*allocation.cast::<IcebergTableOptionCache>();
        assert_eq!(cached.compression(), "snappy");
        assert_eq!(cached.write_format(), "parquet");
        assert_eq!(
            cached
                .parquet_compression()
                .expect("cached codec must remain semantically valid"),
            parquet::basic::Compression::SNAPPY,
        );
        assert_eq!(
            cached
                .to_properties()
                .expect("cached options must adapt to Iceberg properties")
                .get(OPT_COMPRESSION_CODEC)
                .map(String::as_str),
            Some("snappy"),
        );

        pg_sys::pfree(allocation.cast());
    }
}
