use crate::error::{Error, Result};

use super::{executor::ResultSet, parser::Parser, plan::Plan, schema::Table, types::Row};

mod kv;
pub trait Engine: Clone {
    // 这个关联类型 Transaction 表示：
	// •	每个实现 Engine 的类型都必须提供一个具体的类型作为 Transaction。
	// •	并且这个类型必须实现 Transaction trait
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}

pub trait Transaction {
    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()>;
    fn scan_table(&self, table_name: String) -> Result<Vec<Row>>;
    fn create_table(&mut self, table: Table) -> Result<()>;
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;
    // must get table info, otherwise return error (such as table not exist)
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?
            .ok_or(Error::Internal(format!(
                "Table {} does not exist",
                table_name
            )))
    }
}

pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine> Session<E> {
    // Session -> execute -> Parser -> AST -> PLAN
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        // get statement by parser
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                // build plan, execute sql
                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    },
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            },
        }
    }
}
