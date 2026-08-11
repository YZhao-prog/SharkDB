// 表达式求值：在一行数据的上下文中计算表达式的值
//
// NULL 语义遵循 SQL 三值逻辑：
// - 与 NULL 的比较结果是 NULL
// - AND/OR 按三值逻辑传播（false AND NULL = false, true OR NULL = true）
// - WHERE 只保留结果为 true 的行（NULL 视为不满足）

use std::cmp::Ordering;

use crate::error::{Error, Result};
use crate::sql::parser::ast::{Consts, Expression, Operation};
use crate::sql::types::{Row, Value};

// 列标签：(所属表名, 列名)，join 之后用表名区分同名列
pub type ColumnLabel = (Option<String>, String);

// 把列名解析成行内下标；未限定表名时要求列名唯一
pub fn resolve(columns: &[ColumnLabel], table: &Option<String>, name: &str) -> Result<usize> {
    let mut found = None;
    for (i, (t, n)) in columns.iter().enumerate() {
        if n != name {
            continue;
        }
        if let Some(want) = table {
            if t.as_deref() != Some(want.as_str()) {
                continue;
            }
        }
        if found.is_some() {
            return Err(Error::Internal(format!("ambiguous column {}", name)));
        }
        found = Some(i);
    }
    found.ok_or(Error::Internal(format!(
        "unknown column {}{}",
        table.as_ref().map(|t| format!("{}.", t)).unwrap_or_default(),
        name
    )))
}

pub fn evaluate(expr: &Expression, columns: &[ColumnLabel], row: &Row) -> Result<Value> {
    use Operation::*;
    Ok(match expr {
        Expression::Consts(c) => match c {
            Consts::Null => Value::Null,
            Consts::Boolean(b) => Value::Boolean(*b),
            Consts::Integer(i) => Value::Integer(*i),
            Consts::Float(f) => Value::Float(*f),
            Consts::String(s) => Value::String(s.clone()),
        },
        Expression::Field(table, name) => row[resolve(columns, table, name)?].clone(),
        Expression::All => {
            return Err(Error::Internal("unexpected * in expression".into()));
        }
        Expression::Function(name, _) => {
            // 聚合函数由 Aggregate 算子预先计算，不会走到这里
            return Err(Error::Internal(format!(
                "aggregate function {} not allowed here",
                name
            )));
        }
        Expression::Operation(op) => match op {
            Equal(l, r) => compare_op(l, r, columns, row, |o| o == Ordering::Equal)?,
            NotEqual(l, r) => compare_op(l, r, columns, row, |o| o != Ordering::Equal)?,
            GreaterThan(l, r) => compare_op(l, r, columns, row, |o| o == Ordering::Greater)?,
            GreaterThanOrEqual(l, r) => {
                compare_op(l, r, columns, row, |o| o != Ordering::Less)?
            }
            LessThan(l, r) => compare_op(l, r, columns, row, |o| o == Ordering::Less)?,
            LessThanOrEqual(l, r) => compare_op(l, r, columns, row, |o| o != Ordering::Greater)?,
            Add(l, r) => evaluate(l, columns, row)?.checked_add(&evaluate(r, columns, row)?)?,
            Subtract(l, r) => {
                evaluate(l, columns, row)?.checked_sub(&evaluate(r, columns, row)?)?
            }
            Multiply(l, r) => {
                evaluate(l, columns, row)?.checked_mul(&evaluate(r, columns, row)?)?
            }
            Divide(l, r) => evaluate(l, columns, row)?.checked_div(&evaluate(r, columns, row)?)?,
            And(l, r) => match (
                to_bool(evaluate(l, columns, row)?)?,
                to_bool(evaluate(r, columns, row)?)?,
            ) {
                (Some(false), _) | (_, Some(false)) => Value::Boolean(false),
                (Some(true), Some(true)) => Value::Boolean(true),
                _ => Value::Null,
            },
            Or(l, r) => match (
                to_bool(evaluate(l, columns, row)?)?,
                to_bool(evaluate(r, columns, row)?)?,
            ) {
                (Some(true), _) | (_, Some(true)) => Value::Boolean(true),
                (Some(false), Some(false)) => Value::Boolean(false),
                _ => Value::Null,
            },
            Not(e) => match to_bool(evaluate(e, columns, row)?)? {
                Some(b) => Value::Boolean(!b),
                None => Value::Null,
            },
            Negate(e) => match evaluate(e, columns, row)? {
                Value::Integer(i) => Value::Integer(-i),
                Value::Float(f) => Value::Float(-f),
                Value::Null => Value::Null,
                v => return Err(Error::Internal(format!("cannot negate {}", v))),
            },
        },
    })
}

// 谓词判断：表达式求值结果是否为 true（NULL 和 false 都视为否）
pub fn evaluate_predicate(expr: &Expression, columns: &[ColumnLabel], row: &Row) -> Result<bool> {
    Ok(matches!(evaluate(expr, columns, row)?, Value::Boolean(true)))
}

fn compare_op(
    l: &Expression,
    r: &Expression,
    columns: &[ColumnLabel],
    row: &Row,
    check: impl Fn(Ordering) -> bool,
) -> Result<Value> {
    let lv = evaluate(l, columns, row)?;
    let rv = evaluate(r, columns, row)?;
    Ok(match lv.compare(&rv) {
        Some(ord) => Value::Boolean(check(ord)),
        None => Value::Null,
    })
}

fn to_bool(v: Value) -> Result<Option<bool>> {
    match v {
        Value::Boolean(b) => Ok(Some(b)),
        Value::Null => Ok(None),
        v => Err(Error::Internal(format!("expected boolean, got {}", v))),
    }
}
