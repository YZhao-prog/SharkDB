use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::{
        schema::Table,
        types::{Row, Value},
    },
    storage::{self, engine::Engine as StorageEngine},
};

use super::{Engine, Transaction};

// KV Engine 定义
pub struct KVEngine<E: StorageEngine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

// KV Transaction 定义，实际上对存储引擎中 MvccTransaction 的封装
pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }

    fn row_key(table_name: &str, id: &Value) -> Result<Vec<u8>> {
        let key = Key::Row(table_name.to_string(), id.clone());
        Ok(bincode::serialize(&key)?)
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(&self) -> Result<()> {
        self.txn.rollback()
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // 校验行的有效性
        table.check_row(&row)?;

        // 主键唯一性检查
        let id = table.primary_key_of(&row);
        if id.datatype().is_none() {
            return Err(Error::Internal(format!(
                "primary key of table {} cannot be null",
                table.name
            )));
        }
        if self.read_row(&table_name, id)?.is_some() {
            return Err(Error::Internal(format!(
                "duplicate primary key {} for table {}",
                id, table.name
            )));
        }

        // 存放数据，行的存储 key 为主键
        let key = Self::row_key(&table_name, id)?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)?;

        Ok(())
    }

    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()> {
        table.check_row(&row)?;
        let new_id = table.primary_key_of(&row);
        // 主键被修改时，相当于删除旧行、插入新行
        if new_id != id {
            if self.read_row(&table.name, new_id)?.is_some() {
                return Err(Error::Internal(format!(
                    "duplicate primary key {} for table {}",
                    new_id, table.name
                )));
            }
            self.txn.delete(Self::row_key(&table.name, id)?)?;
        }

        let key = Self::row_key(&table.name, table.primary_key_of(&row))?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)?;
        Ok(())
    }

    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()> {
        self.txn.delete(Self::row_key(&table.name, id)?)
    }

    fn read_row(&self, table_name: &str, id: &Value) -> Result<Option<Row>> {
        let key = Self::row_key(table_name, id)?;
        Ok(self
            .txn
            .get(key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        use crate::sql::executor::{parallelism, PARALLEL_THRESHOLD};

        let prefix = KeyPrefix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;

        // 行数多时并行反序列化（parallel scan 的解码阶段）
        let threads = parallelism();
        if results.len() < PARALLEL_THRESHOLD || threads <= 1 {
            let mut rows = Vec::with_capacity(results.len());
            for result in results {
                let row: Row = bincode::deserialize(&result.value)?;
                rows.push(row);
            }
            return Ok(rows);
        }

        // 分片数取线程数的 4 倍，缓解大小核负载不均
        let chunk_size = results.len().div_ceil(threads * 4).max(1);
        let decoded = std::thread::scope(|s| {
            let handles: Vec<_> = results
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        chunk
                            .iter()
                            .map(|r| Ok(bincode::deserialize::<Row>(&r.value)?))
                            .collect::<Result<Vec<Row>>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("decode worker panicked"))
                .collect::<Result<Vec<Vec<Row>>>>()
        })?;
        Ok(decoded.into_iter().flatten().collect())
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否已经存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists",
                table.name
            )));
        }

        // 判断表的有效性
        if table.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns",
                table.name
            )));
        }

        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;
        self.txn.set(bincode::serialize(&key)?, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        Ok(self
            .txn
            .get(bincode::serialize(&key)?)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String, Value),
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute("create table t1 (a int primary key, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        s.execute("select * from t1;")?;

        Ok(())
    }

    #[test]
    fn test_duplicate_primary_key() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute("create table t1 (a int primary key, b text);")?;
        s.execute("insert into t1 values(1, 'a');")?;
        assert!(s.execute("insert into t1 values(1, 'b');").is_err());
        Ok(())
    }
}
