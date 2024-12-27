use serde::{Deserialize, Serialize};

use crate::{error::{Error, Result}, sql::{schema::Table, types::Row}, storage::{self, engine::Engine as StorageEngine}};

use super::{Engine, Transaction};

pub struct KVEngine<E: StorageEngine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self { kv: self.kv.clone() }
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

    fn create_row(&mut self, table: String, row: Row) -> Result<()> {
        todo!()
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        todo!()
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
    Row(String, String), // table name, row name
}