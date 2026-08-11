// 写操作执行：Insert / Update / Delete

use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::Expression,
        plan::Node,
        schema::Table,
        types::{Row, Value},
    },
};

use super::{expression, query, ResultSet};

// 列对齐
// tbl:
// insert into tbl values(1, 2, 3);
// a       b       c          d
// 1       2       3      default 填充
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = &column.default {
            results.push(default.clone());
        } else {
            return Err(Error::Internal(format!(
                "No default value for column {}",
                column.name
            )));
        }
    }

    Ok(results)
}

// tbl:
// insert into tbl(d, c) values(1, 2);
//    a          b       c          d
// default   default     2          1
fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // 判断列数是否和value数一致
    if columns.len() != values.len() {
        return Err(Error::Internal(format!("columns and values num mismatch")));
    }

    let mut inputs = HashMap::new();
    for (i, col_name) in columns.iter().enumerate() {
        inputs.insert(col_name, values[i].clone());
    }

    let mut results = Vec::new();
    for col in table.columns.iter() {
        if let Some(value) = inputs.get(&col.name) {
            results.push(value.clone());
        } else if let Some(value) = &col.default {
            results.push(value.clone());
        } else {
            return Err(Error::Internal(format!(
                "No value given for the column {}",
                col.name
            )));
        }
    }

    Ok(results)
}

pub fn insert<T: Transaction>(
    txn: &mut T,
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
) -> Result<ResultSet> {
    let mut count = 0;
    // 先取出表信息
    let table = txn.must_get_table(table_name.clone())?;
    for exprs in values {
        // 将表达式求值成 value（常量表达式，无行上下文）
        let row = exprs
            .iter()
            .map(|e| expression::evaluate(e, &[], &vec![]))
            .collect::<Result<Vec<_>>>()?;
        // 如果没有指定插入的列
        let insert_row = if columns.is_empty() {
            pad_row(&table, &row)?
        } else {
            // 指定了插入的列，需要对 value 信息进行整理
            make_row(&table, &columns, &row)?
        };

        // 插入数据
        txn.create_row(table_name.clone(), insert_row)?;
        count += 1;
    }

    Ok(ResultSet::Insert { count })
}

pub fn update<T: Transaction>(
    txn: &mut T,
    table_name: String,
    source: Node,
    set: Vec<(String, Expression)>,
) -> Result<ResultSet> {
    let table = txn.must_get_table(table_name)?;
    let pk_index = table.primary_key_index();

    // 预先解析 SET 的目标列下标
    let set_indexes = set
        .iter()
        .map(|(name, expr)| {
            let idx = table
                .columns
                .iter()
                .position(|c| &c.name == name)
                .ok_or(Error::Internal(format!("unknown column {}", name)))?;
            Ok((idx, expr))
        })
        .collect::<Result<Vec<_>>>()?;

    // 先物化出要更新的行，再逐行更新
    let matched = query::execute(source, txn)?;
    let mut count = 0;
    for row in matched.rows {
        let id = row[pk_index].clone();
        let mut new_row = row.clone();
        // SET 表达式在原行的上下文中求值（支持 a = a + 1）
        for (idx, expr) in &set_indexes {
            new_row[*idx] = expression::evaluate(expr, &matched.columns, &row)?;
        }
        txn.update_row(&table, &id, new_row)?;
        count += 1;
    }
    Ok(ResultSet::Update { count })
}

pub fn delete<T: Transaction>(
    txn: &mut T,
    table_name: String,
    source: Node,
) -> Result<ResultSet> {
    let table = txn.must_get_table(table_name)?;
    let pk_index = table.primary_key_index();

    let matched = query::execute(source, txn)?;
    let mut count = 0;
    for row in matched.rows {
        let id: &Value = &row[pk_index];
        txn.delete_row(&table, id)?;
        count += 1;
    }
    Ok(ResultSet::Delete { count })
}
