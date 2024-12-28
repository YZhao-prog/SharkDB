use mutation::Insert;
use query::Scan;
use schema::CreateTable;

use crate::error::Result;

use super::{engine::Transaction, plan::Node, types::Row};

mod schema;
mod mutation;
mod query;
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

// 1.	抽象化实现:
// •	它表示我们并不关心 Executor<T> 的具体类型，而只要求返回的类型实现了 Executor<T> trait。
// 2.	动态分发:
// •	dyn 允许我们在运行时根据 Node 的类型决定具体返回哪个实现（如 CreateTable、Insert 或 Scan）。
// 3.	存储异构类型:
// •	通过 Box<dyn Executor<T>>，可以存储不同的类型（CreateTable、Insert 等），只要这些类型实现了 Executor<T> trait。
impl<T: Transaction> dyn Executor<T> {
    // convert plan node to executor struct
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert { table_name, columns, values } => Insert::new(table_name, columns, values),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}

#[derive(Debug)]
pub enum ResultSet {
    CreateTable {
        table_name: String,
    },
    Insert {
        count: usize,
    },
    Scan {
        columns: Vec<String>,
        row: Vec<Row>,
    }
}