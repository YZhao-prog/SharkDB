use crate::error::{Error, Result};

use super::{
    executor::ResultSet,
    parser::Parser,
    plan::Plan,
    schema::Table,
    types::{Row, Value},
};

pub mod kv;

// 抽象的 SQL 引擎层定义，目前只有一个 KVEngine
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}

// 抽象的事务信息，包含了 DDL 和 DML 操作
// 底层可以接入普通的 KV 存储引擎，也可以接入分布式存储引擎
pub trait Transaction {
    // 提交事务
    fn commit(&self) -> Result<()>;
    // 回滚事务
    fn rollback(&self) -> Result<()>;

    // 创建行
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()>;
    // 更新行，id 是该行原来的主键
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()>;
    // 删除行
    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()>;
    // 按主键点查一行
    fn read_row(&self, table_name: &str, id: &Value) -> Result<Option<Row>>;
    // 扫描表
    fn scan_table(&self, table_name: String) -> Result<Vec<Row>>;

    // DDL 相关操作
    fn create_table(&mut self, table: Table) -> Result<()>;
    // 获取表信息
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;
    // 获取表信息，不存在则报错
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?
            .ok_or(Error::Internal(format!(
                "table {} does not exist",
                table_name
            )))
    }
}

// 客户端 session 定义
pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine> Session<E> {
    // 执行客户端 SQL 语句
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        let stmt = Parser::new(sql).parse()?;
        let mut txn = self.engine.begin()?;
        // 构建执行计划（构建和优化需要访问表结构），执行 SQL 语句
        match Plan::build(stmt, &mut txn).and_then(|plan| plan.execute(&mut txn)) {
            Ok(result) => {
                txn.commit()?;
                Ok(result)
            }
            Err(err) => {
                txn.rollback()?;
                Err(err)
            }
        }
    }
}
