use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use super::kv::{CacheKv, KvPair, KvReadTxn, KvTable, KvWriteTxn};
use crate::error::{StorageError, StorageResult};

pub(super) fn redb_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> StorageError {
    StorageError::cache_source("redb cache index operation failed", error)
}

pub struct RedbKv {
    db: Database,
}

impl RedbKv {
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path).map_err(redb_error)?;
        Ok(Self { db })
    }
}

impl CacheKv for RedbKv {
    type ReadTxn<'a>
        = RedbReadTxn
    where
        Self: 'a;
    type WriteTxn<'a>
        = RedbWriteTxn
    where
        Self: 'a;

    fn ensure_tables(&self, tables: &[KvTable]) -> StorageResult<()> {
        let txn = self.db.begin_write().map_err(redb_error)?;
        {
            for table in tables {
                let _ = txn
                    .open_table(table_definition(*table))
                    .map_err(redb_error)?;
            }
        }
        txn.commit().map_err(redb_error)
    }

    fn begin_read(&self) -> StorageResult<Self::ReadTxn<'_>> {
        Ok(RedbReadTxn {
            txn: self.db.begin_read().map_err(redb_error)?,
        })
    }

    fn begin_write(&self) -> StorageResult<Self::WriteTxn<'_>> {
        Ok(RedbWriteTxn {
            txn: self.db.begin_write().map_err(redb_error)?,
        })
    }
}

pub struct RedbReadTxn {
    txn: redb::ReadTransaction,
}

impl KvReadTxn for RedbReadTxn {
    fn get(&self, table: KvTable, key: &str) -> StorageResult<Option<Vec<u8>>> {
        let table = self
            .txn
            .open_table(table_definition(table))
            .map_err(redb_error)?;
        let value = table
            .get(key)
            .map_err(redb_error)?
            .map(|value| value.value().to_vec());
        Ok(value)
    }

    fn get_len(&self, table: KvTable, key: &str) -> StorageResult<Option<u64>> {
        let table = self
            .txn
            .open_table(table_definition(table))
            .map_err(redb_error)?;
        let len = table
            .get(key)
            .map_err(redb_error)?
            .map(|value| value.value().len() as u64);
        Ok(len)
    }

    fn scan_page(
        &self,
        table: KvTable,
        after_exclusive: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<KvPair>> {
        scan_page(&self.txn, table, after_exclusive, limit)
    }
}

pub struct RedbWriteTxn {
    txn: redb::WriteTransaction,
}

impl KvReadTxn for RedbWriteTxn {
    fn get(&self, table: KvTable, key: &str) -> StorageResult<Option<Vec<u8>>> {
        let table = self
            .txn
            .open_table(table_definition(table))
            .map_err(redb_error)?;
        let value = table
            .get(key)
            .map_err(redb_error)?
            .map(|value| value.value().to_vec());
        Ok(value)
    }

    fn get_len(&self, table: KvTable, key: &str) -> StorageResult<Option<u64>> {
        let table = self
            .txn
            .open_table(table_definition(table))
            .map_err(redb_error)?;
        let len = table
            .get(key)
            .map_err(redb_error)?
            .map(|value| value.value().len() as u64);
        Ok(len)
    }

    fn scan_page(
        &self,
        table: KvTable,
        after_exclusive: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<KvPair>> {
        scan_page(&self.txn, table, after_exclusive, limit)
    }
}

impl KvWriteTxn for RedbWriteTxn {
    fn put(&mut self, table: KvTable, key: &str, value: &[u8]) -> StorageResult<()> {
        let mut table = self
            .txn
            .open_table(table_definition(table))
            .map_err(redb_error)?;
        table.insert(key, value).map_err(redb_error)?;
        Ok(())
    }

    fn remove(&mut self, table: KvTable, key: &str) -> StorageResult<()> {
        let mut table = self
            .txn
            .open_table(table_definition(table))
            .map_err(redb_error)?;
        table.remove(key).map_err(redb_error)?;
        Ok(())
    }

    fn commit(self) -> StorageResult<()> {
        self.txn.commit().map_err(redb_error)
    }
}

fn table_definition(
    table: KvTable,
) -> TableDefinition<'static, &'static str, &'static [u8]> {
    TableDefinition::new(table.name())
}

fn scan_page<T>(
    txn: &T,
    table: KvTable,
    after_exclusive: Option<&str>,
    limit: usize,
) -> StorageResult<Vec<KvPair>>
where
    T: RedbTableReader,
{
    use std::ops::Bound::{Excluded, Unbounded};

    let table = txn.open_bytes_table(table)?;
    let limit = limit.max(1);
    let iter = match after_exclusive {
        Some(after) => table
            .range::<&str>((Excluded(after), Unbounded))
            .map_err(redb_error)?,
        None => table.range::<&str>(..).map_err(redb_error)?,
    };
    let mut rows = Vec::new();
    for entry in iter {
        let (key, value) = entry.map_err(redb_error)?;
        rows.push(KvPair {
            key: key.value().to_owned(),
            value: value.value().to_vec(),
        });
        if rows.len() >= limit {
            break;
        }
    }
    Ok(rows)
}

trait RedbTableReader {
    type Table<'a>: ReadableTable<&'static str, &'static [u8]>
    where
        Self: 'a;

    fn open_bytes_table(&self, table: KvTable) -> StorageResult<Self::Table<'_>>;
}

impl RedbTableReader for redb::ReadTransaction {
    type Table<'a>
        = redb::ReadOnlyTable<&'static str, &'static [u8]>
    where
        Self: 'a;

    fn open_bytes_table(&self, table: KvTable) -> StorageResult<Self::Table<'_>> {
        self.open_table(table_definition(table)).map_err(redb_error)
    }
}

impl RedbTableReader for redb::WriteTransaction {
    type Table<'a>
        = redb::Table<'a, &'static str, &'static [u8]>
    where
        Self: 'a;

    fn open_bytes_table(&self, table: KvTable) -> StorageResult<Self::Table<'_>> {
        self.open_table(table_definition(table)).map_err(redb_error)
    }
}
