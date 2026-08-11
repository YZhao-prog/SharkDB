// 查询类算子的递归执行：Scan / PointLookup / Filter / Join / Order / Limit / Offset / Projection
//
// 过滤在数据量大时并行执行：按行分片用 std::thread::scope 并行求值谓词，
// 得到保留掩码后按序 retain，保证结果顺序与单线程一致

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::sql::engine::Transaction;
use crate::sql::parser::ast::{Expression, OrderDirection};
use crate::sql::plan::Node;
use crate::sql::types::{Row, Value};

use super::expression::{self, ColumnLabel};
use super::{aggregate, parallelism, PARALLEL_THRESHOLD};

// 查询算子的中间结果：带列标签的行集
pub struct Rows {
    pub columns: Vec<ColumnLabel>,
    pub rows: Vec<Row>,
}

pub fn execute<T: Transaction>(node: Node, txn: &mut T) -> Result<Rows> {
    match node {
        Node::Scan { table_name, filter } => {
            let table = txn.must_get_table(table_name.clone())?;
            let columns: Vec<ColumnLabel> = table
                .columns
                .iter()
                .map(|c| (Some(table_name.clone()), c.name.clone()))
                .collect();
            let mut rows = txn.scan_table(table_name)?;
            if let Some(predicate) = filter {
                filter_rows(&mut rows, &predicate, &columns)?;
            }
            Ok(Rows { columns, rows })
        }

        Node::PointLookup {
            table_name,
            value,
            filter,
        } => {
            let table = txn.must_get_table(table_name.clone())?;
            let columns: Vec<ColumnLabel> = table
                .columns
                .iter()
                .map(|c| (Some(table_name.clone()), c.name.clone()))
                .collect();
            let mut rows = match txn.read_row(&table_name, &value)? {
                Some(row) => vec![row],
                None => vec![],
            };
            if let Some(predicate) = filter {
                filter_rows(&mut rows, &predicate, &columns)?;
            }
            Ok(Rows { columns, rows })
        }

        Node::Filter { source, predicate } => {
            let mut result = execute(*source, txn)?;
            filter_rows(&mut result.rows, &predicate, &result.columns)?;
            Ok(result)
        }

        Node::NestedLoopJoin {
            left,
            right,
            predicate,
        } => {
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            let mut columns = left.columns.clone();
            columns.extend(right.columns.clone());

            let mut rows = Vec::new();
            for lrow in &left.rows {
                for rrow in &right.rows {
                    let mut row = lrow.clone();
                    row.extend(rrow.clone());
                    match &predicate {
                        Some(expr) => {
                            if expression::evaluate_predicate(expr, &columns, &row)? {
                                rows.push(row);
                            }
                        }
                        None => rows.push(row),
                    }
                }
            }
            Ok(Rows { columns, rows })
        }

        Node::HashJoin {
            left,
            left_field,
            right,
            right_field,
        } => {
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            let mut columns = left.columns.clone();
            columns.extend(right.columns.clone());

            // 构建侧：右表按连接键建哈希表
            let right_idx = field_index(&right_field, &right.columns)?;
            let mut table: HashMap<Value, Vec<&Row>> = HashMap::new();
            for row in &right.rows {
                let key = &row[right_idx];
                // NULL 不参与等值连接
                if key.datatype().is_none() {
                    continue;
                }
                table.entry(key.clone()).or_default().push(row);
            }

            // 探测侧：左表逐行查哈希表
            let left_idx = field_index(&left_field, &left.columns)?;
            let mut rows = Vec::new();
            for lrow in &left.rows {
                let key = &lrow[left_idx];
                if key.datatype().is_none() {
                    continue;
                }
                if let Some(matches) = table.get(key) {
                    for rrow in matches {
                        let mut row = lrow.clone();
                        row.extend((*rrow).clone());
                        rows.push(row);
                    }
                }
            }
            Ok(Rows { columns, rows })
        }

        Node::Aggregate {
            source,
            select,
            group_by,
        } => {
            let input = execute(*source, txn)?;
            aggregate::execute(input, select, group_by)
        }

        Node::Order { source, order_by } => {
            let mut result = execute(*source, txn)?;
            // 预先计算每一行的排序键，避免比较函数里重复求值
            let mut keyed: Vec<(Vec<Value>, Row)> = result
                .rows
                .drain(..)
                .map(|row| {
                    let keys = order_by
                        .iter()
                        .map(|(expr, _)| expression::evaluate(expr, &result.columns, &row))
                        .collect::<Result<Vec<_>>>()?;
                    Ok((keys, row))
                })
                .collect::<Result<Vec<_>>>()?;

            keyed.sort_by(|(a, _), (b, _)| {
                for (i, (_, direction)) in order_by.iter().enumerate() {
                    let ord = a[i].sort_cmp(&b[i]);
                    let ord = match direction {
                        OrderDirection::Asc => ord,
                        OrderDirection::Desc => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });

            result.rows = keyed.into_iter().map(|(_, row)| row).collect();
            Ok(result)
        }

        Node::Offset { source, offset } => {
            let mut result = execute(*source, txn)?;
            result.rows = result.rows.split_off(offset.min(result.rows.len()));
            Ok(result)
        }

        Node::Limit { source, limit } => {
            let mut result = execute(*source, txn)?;
            result.rows.truncate(limit);
            Ok(result)
        }

        Node::Projection { source, select } => {
            let input = execute(*source, txn)?;

            let mut columns = Vec::new();
            // (输出列, 对应的表达式)；* 展开为输入的全部列
            let mut exprs: Vec<Expression> = Vec::new();
            for (expr, alias) in select {
                match expr {
                    Expression::All => {
                        for (table, name) in &input.columns {
                            columns.push((table.clone(), name.clone()));
                            exprs.push(Expression::Field(table.clone(), name.clone()));
                        }
                    }
                    expr => {
                        let name = match (&alias, &expr) {
                            (Some(a), _) => a.clone(),
                            (None, Expression::Field(_, name)) => name.clone(),
                            (None, e) => e.to_string(),
                        };
                        columns.push((None, name));
                        exprs.push(expr);
                    }
                }
            }

            let rows = input
                .rows
                .iter()
                .map(|row| {
                    exprs
                        .iter()
                        .map(|e| expression::evaluate(e, &input.columns, row))
                        .collect::<Result<Row>>()
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Rows { columns, rows })
        }

        node => Err(Error::Internal(format!(
            "unexpected node in query executor: {:?}",
            node
        ))),
    }
}

// HashJoin 的连接键必须是列引用
fn field_index(expr: &Expression, columns: &[ColumnLabel]) -> Result<usize> {
    match expr {
        Expression::Field(table, name) => expression::resolve(columns, table, name),
        e => Err(Error::Internal(format!("invalid join key {}", e))),
    }
}

// 按谓词过滤行，大数据量时并行求值
pub fn filter_rows(rows: &mut Vec<Row>, predicate: &Expression, columns: &[ColumnLabel]) -> Result<()> {
    let threads = parallelism();
    if rows.len() < PARALLEL_THRESHOLD || threads <= 1 {
        let mut error = None;
        rows.retain(|row| {
            if error.is_some() {
                return false;
            }
            match expression::evaluate_predicate(predicate, columns, row) {
                Ok(keep) => keep,
                Err(e) => {
                    error = Some(e);
                    false
                }
            }
        });
        return match error {
            Some(e) => Err(e),
            None => Ok(()),
        };
    }

    // 并行：分片计算保留掩码，再按序 retain，结果与单线程完全一致
    // 分片数取线程数的 4 倍，缓解大小核负载不均
    let chunk_size = rows.len().div_ceil(threads * 4).max(1);
    let mask = std::thread::scope(|s| {
        let handles: Vec<_> = rows
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(move || {
                    chunk
                        .iter()
                        .map(|row| expression::evaluate_predicate(predicate, columns, row))
                        .collect::<Result<Vec<bool>>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("filter worker panicked"))
            .collect::<Result<Vec<Vec<bool>>>>()
    })?;

    let mut keep = mask.into_iter().flatten();
    rows.retain(|_| keep.next().unwrap_or(false));
    Ok(())
}
