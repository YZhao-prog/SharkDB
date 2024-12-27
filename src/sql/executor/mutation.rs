use std::{collections::HashMap, default};

use serde::de::value;

use crate::{error::{Error, Result}, sql::{engine::Transaction, parser::ast::Expression, schema::Table, types::{Row, Value}}};

use super::{Executor, ResultSet};

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table_name: String, columns: Vec<String>, values: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self {table_name, columns, values})
    }
}

// complete row, fill default value
// insert into tbl values(1, 2, 3);
// a          b           c           d
// 1          2           3       default value
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = &column.default {
            results.push(default.clone());
        } else {
            return Err(Error::Internal(format!("No default value for column {}", column.name)));
        }
    }
    Ok(results)
}

// insert into tbl(d, c) values(1, 2);
// a          b           c           d
// default   default      2           1
fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // check if value number equals columns number
    if columns.len() != columns.len() {
        return Err(Error::Internal(format!("columns and values number mismatch")));
    }
    // build hash map
    let mut inputs = HashMap::new();
    for (i, column_name) in columns.iter().enumerate() {
        inputs.insert(column_name, values[i].clone());
    }

    // insert each column
    let mut results = Vec::new();
    for col in table.columns.iter() {
        // •	容器要求所有权：Vec 等容器类型会要求你存储拥有所有权的元素（不是引用）。所以需要通过 clone() 来复制值并将其推入 results 向量中，而不是将借用的引用推入。
        // •	避免悬垂引用：借用的引用会在它们原本的作用域结束时失效，而通过 clone() 获得的值可以独立于原始数据的生命周期存在，避免了悬垂引用的问题
        if let Some(value) = inputs.get(&col.name) {
            results.push(value.clone());
        } else if let Some(default) = &col.default {
            results.push(default.clone());
        } else {
            // Err不会转移所有权
            return Err(Error::Internal(format!("No value given for the column {}", col.name)));
        }
    }
    Ok(results)
}

impl<T:Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // get information of table
        let table = txn.must_get_table(self.table_name.clone())?;
        // pub type Row = Vec<Value>; need to convert Expression to Value so we can use create_row func
        let mut count = 0;
        for exprs in self.values {
            let row = exprs.into_iter()
                                       .map(|e| Value::from_expression(e))
                                       .collect::<Vec<_>>();
            let insert_row = if self.columns.is_empty() {
                // if we don't know which column we need to insert
                pad_row(&table, &row)?
            } else {
                // if we know which column we need to insert
                make_row(&table, &self.columns, &row)?
            };
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        Ok(ResultSet::Insert { count })
    }
}