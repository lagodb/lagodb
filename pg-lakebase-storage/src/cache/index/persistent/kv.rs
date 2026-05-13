use crate::error::StorageResult;

/// Physical tables required by the persistent cache index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvTable {
    Meta,
    Small,
    Lru,
}

impl KvTable {
    pub const ALL: &'static [Self] = &[Self::Meta, Self::Small, Self::Lru];

    pub fn name(self) -> &'static str {
        match self {
            Self::Meta => "object_meta",
            Self::Small => "small_object",
            Self::Lru => "lru_by_access",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvPair {
    pub key: String,
    pub value: Vec<u8>,
}

/// Thin ordered-KV boundary under the cache-index logic.
///
/// This intentionally models only the persistence capabilities the index needs:
/// transactions, point reads/writes, deletes, and stable key-ordered page scans.
pub trait CacheKv: Send + Sync + 'static {
    type ReadTxn<'a>: KvReadTxn + 'a
    where
        Self: 'a;
    type WriteTxn<'a>: KvWriteTxn + 'a
    where
        Self: 'a;

    fn ensure_tables(&self, tables: &[KvTable]) -> StorageResult<()>;
    fn begin_read(&self) -> StorageResult<Self::ReadTxn<'_>>;
    fn begin_write(&self) -> StorageResult<Self::WriteTxn<'_>>;
}

pub trait KvReadTxn {
    fn get(&self, table: KvTable, key: &str) -> StorageResult<Option<Vec<u8>>>;
    fn get_len(&self, table: KvTable, key: &str) -> StorageResult<Option<u64>>;
    fn scan_page(&self, table: KvTable, after_exclusive: Option<&str>, limit: usize) -> StorageResult<Vec<KvPair>>;
}

pub trait KvWriteTxn: KvReadTxn {
    fn put(&mut self, table: KvTable, key: &str, value: &[u8]) -> StorageResult<()>;
    fn remove(&mut self, table: KvTable, key: &str) -> StorageResult<()>;
    fn commit(self) -> StorageResult<()>;
}
