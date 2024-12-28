
use serde::{Deserialize, Serialize};

use crate::{error::{Error, Result}, sql::{schema::Table, types::{Row, Value}}, storage::{self, engine::Engine as StorageEngine}};

use super::{Engine, Transaction};

pub struct KVEngine<E: StorageEngine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self { kv: self.kv.clone() }
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

pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self {txn}
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        // check row type validation
        let table = self.must_get_table(table_name.clone())?;
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {},
                None =>  return Err(Error::Internal(format!("column {} cannot be null", col.name))),
                Some(dt) if dt != col.datatype => return Err(Error::Internal(format!("column {} data type mismatch", col.name))),
                _ => {},
            }
        }
        // store data in memeory store engine
        // temporarily use row[0] (the first column) as primary key  (to be continue)
        let id = Key::Row(table_name.clone(), row[0].clone());
        let key = bincode::serialize(&id)?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)?;

        Ok(())
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        // 在 Key 枚举中，Row 类型的键是由 Key::Row(table_name, row) 表示的，包含了表名和行的具体数据。
        // 因此，KeyPrefix::Row(table_name) 作为前缀，可以用来定位所有以给定表名开头的行数据。
        let prefix = KeyPrefix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;
        let mut rows  = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // check if the table exists
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!("Table {} already exist.", table.name)));
        }
        // check validation
        if table.columns.is_empty() {
            return Err(Error::Internal(format!("Table {} has no columns.", table.name)));
        }
        let key = bincode::serialize(&Key::Table(table.name.clone()))?;
        let value = bincode::serialize(&table)?;
        self.txn.set(key, value)?;
        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = bincode::serialize(&Key::Table(table_name))?;
        // if exist, map; else return none
        let val = self.txn.get(key)?
                                .map(|val| bincode::deserialize(&val))
                                .transpose()?;
        Ok(val)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),// table name
    Row(String, Value), // table name, value
}

// KeyPrefix::Table 是为了与Key::Table对齐。在序列化后的字节中：
// 	•	Table 会以 0x01 开头。
// 	•	Row(String) 会以 0x02 开头。
#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table, // align
    Row(String), // table name
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute("create table t1 (a int, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        let v = s.execute("select * from t1;")?;
        println!("{:?}", v);
        Ok(())
    }
}