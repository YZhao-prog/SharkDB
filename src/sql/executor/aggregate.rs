// 聚合算子：count / sum / avg / min / max + GROUP BY
//
// select 列表中的每一项要么是聚合函数，要么是 GROUP BY 中出现过的表达式。
// 数据量大时并行执行：分片做部分聚合（partial aggregation），再合并累加器，
// 这也是并行数据库经典的两阶段聚合思路。
//
// 输出按分组键排序，保证并行与单线程结果一致且确定。

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::sql::parser::ast::Expression;
use crate::sql::types::{Row, Value};

use super::expression::{self, ColumnLabel};
use super::query::Rows;
use super::{parallelism, PARALLEL_THRESHOLD};

// 聚合函数类型
#[derive(Clone, Copy, PartialEq)]
enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    fn from_name(name: &str) -> Result<Self> {
        Ok(match name {
            "count" => Self::Count,
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            _ => return Err(Error::Internal(format!("unknown function {}", name))),
        })
    }
}

// 累加器：avg 内部维护 (sum, count)，可以两两合并，支持并行部分聚合
#[derive(Clone)]
enum Accumulator {
    Count(i64),
    Sum(Option<Value>),
    Avg { sum: Option<Value>, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl Accumulator {
    fn new(func: AggFunc) -> Self {
        match func {
            AggFunc::Count => Self::Count(0),
            AggFunc::Sum => Self::Sum(None),
            AggFunc::Avg => Self::Avg {
                sum: None,
                count: 0,
            },
            AggFunc::Min => Self::Min(None),
            AggFunc::Max => Self::Max(None),
        }
    }

    // 累加一个值；NULL 不参与聚合（count(*) 的参数是非 NULL 常量，不受影响）
    fn update(&mut self, value: &Value) -> Result<()> {
        if value.datatype().is_none() {
            return Ok(());
        }
        match self {
            Self::Count(n) => *n += 1,
            Self::Sum(acc) => {
                *acc = Some(match acc {
                    Some(sum) => sum.checked_add(value)?,
                    None => value.clone(),
                })
            }
            Self::Avg { sum, count } => {
                *sum = Some(match sum {
                    Some(s) => s.checked_add(value)?,
                    None => value.clone(),
                });
                *count += 1;
            }
            Self::Min(acc) => match acc {
                Some(min) if value.sort_cmp(min) == std::cmp::Ordering::Less => {
                    *acc = Some(value.clone())
                }
                Some(_) => {}
                None => *acc = Some(value.clone()),
            },
            Self::Max(acc) => match acc {
                Some(max) if value.sort_cmp(max) == std::cmp::Ordering::Greater => {
                    *acc = Some(value.clone())
                }
                Some(_) => {}
                None => *acc = Some(value.clone()),
            },
        }
        Ok(())
    }

    // 合并另一个部分聚合的累加器（并行分片后归并）
    fn merge(&mut self, other: Accumulator) -> Result<()> {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => *a += b,
            (Self::Sum(a), Self::Sum(b)) => {
                if let Some(v) = b {
                    *a = Some(match a {
                        Some(sum) => sum.checked_add(&v)?,
                        None => v,
                    });
                }
            }
            (
                Self::Avg { sum, count },
                Self::Avg {
                    sum: bsum,
                    count: bcount,
                },
            ) => {
                if let Some(v) = bsum {
                    *sum = Some(match sum {
                        Some(s) => s.checked_add(&v)?,
                        None => v,
                    });
                }
                *count += bcount;
            }
            (Self::Min(a), Self::Min(b)) => {
                if let Some(v) = b {
                    match a {
                        Some(min) if v.sort_cmp(min) == std::cmp::Ordering::Less => *a = Some(v),
                        Some(_) => {}
                        None => *a = Some(v),
                    }
                }
            }
            (Self::Max(a), Self::Max(b)) => {
                if let Some(v) = b {
                    match a {
                        Some(max) if v.sort_cmp(max) == std::cmp::Ordering::Greater => {
                            *a = Some(v)
                        }
                        Some(_) => {}
                        None => *a = Some(v),
                    }
                }
            }
            _ => return Err(Error::Internal("accumulator type mismatch".into())),
        }
        Ok(())
    }

    fn finish(self) -> Result<Value> {
        Ok(match self {
            Self::Count(n) => Value::Integer(n),
            Self::Sum(acc) => acc.unwrap_or(Value::Null),
            Self::Avg { sum, count } => match sum {
                Some(s) => s.checked_div(&Value::Integer(count))?,
                None => Value::Null,
            },
            Self::Min(acc) => acc.unwrap_or(Value::Null),
            Self::Max(acc) => acc.unwrap_or(Value::Null),
        })
    }
}

// select 列表项：聚合函数或分组表达式
enum Item {
    Agg(AggFunc, Expression),
    Group(Expression),
}

pub fn execute(
    input: Rows,
    select: Vec<(Expression, Option<String>)>,
    group_by: Vec<Expression>,
) -> Result<Rows> {
    // 解析 select 列表
    let mut items = Vec::new();
    let mut columns: Vec<ColumnLabel> = Vec::new();
    for (expr, alias) in select {
        match expr {
            Expression::Function(name, arg) => {
                let func = AggFunc::from_name(&name)?;
                // count(*) 之外，参数不允许是 *
                if *arg == Expression::All && func != AggFunc::Count {
                    return Err(Error::Internal(format!("invalid argument * for {}", name)));
                }
                columns.push((None, alias.unwrap_or(name)));
                items.push(Item::Agg(func, *arg));
            }
            expr => {
                // 非聚合项必须出现在 GROUP BY 中
                if !group_by.contains(&expr) {
                    return Err(Error::Internal(format!(
                        "column {} must appear in GROUP BY or be used in an aggregate function",
                        expr
                    )));
                }
                let name = match (&alias, &expr) {
                    (Some(a), _) => a.clone(),
                    (None, Expression::Field(_, name)) => name.clone(),
                    (None, e) => e.to_string(),
                };
                columns.push((None, name));
                items.push(Item::Group(expr));
            }
        }
    }

    let agg_specs: Vec<(AggFunc, &Expression)> = items
        .iter()
        .filter_map(|item| match item {
            Item::Agg(f, e) => Some((*f, e)),
            Item::Group(_) => None,
        })
        .collect();

    // 分组聚合：group key -> 每个聚合函数一个累加器
    type Groups = HashMap<Vec<Value>, Vec<Accumulator>>;
    let aggregate_chunk = |rows: &[Row]| -> Result<Groups> {
        let mut groups: Groups = HashMap::new();
        for row in rows {
            let key = group_by
                .iter()
                .map(|e| expression::evaluate(e, &input.columns, row))
                .collect::<Result<Vec<_>>>()?;
            let accs = groups
                .entry(key)
                .or_insert_with(|| agg_specs.iter().map(|(f, _)| Accumulator::new(*f)).collect());
            for (acc, (_, arg)) in accs.iter_mut().zip(agg_specs.iter()) {
                // count(*) 每行都计数，其他函数对参数求值后累加
                let value = match arg {
                    Expression::All => Value::Integer(1),
                    e => expression::evaluate(e, &input.columns, row)?,
                };
                acc.update(&value)?;
            }
        }
        Ok(groups)
    };

    let threads = parallelism();
    let mut groups = if input.rows.len() < PARALLEL_THRESHOLD || threads <= 1 {
        aggregate_chunk(&input.rows)?
    } else {
        // 并行部分聚合：每个分片独立聚合，再合并累加器
        // 分片数取线程数的 4 倍：大小核异构下小分片让快核多干活，避免长尾
        let chunk_size = input.rows.len().div_ceil(threads * 4).max(1);
        let partials = std::thread::scope(|s| {
            let handles: Vec<_> = input
                .rows
                .chunks(chunk_size)
                .map(|chunk| s.spawn(|| aggregate_chunk(chunk)))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("aggregate worker panicked"))
                .collect::<Result<Vec<Groups>>>()
        })?;

        let mut merged: Groups = HashMap::new();
        for partial in partials {
            for (key, accs) in partial {
                match merged.entry(key) {
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(accs);
                    }
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        for (a, b) in e.get_mut().iter_mut().zip(accs) {
                            a.merge(b)?;
                        }
                    }
                }
            }
        }
        merged
    };

    // 没有 GROUP BY 时，即使输入为空也要输出一行（count = 0）
    if group_by.is_empty() && groups.is_empty() {
        groups.insert(
            vec![],
            agg_specs.iter().map(|(f, _)| Accumulator::new(*f)).collect(),
        );
    }

    // 按分组键排序输出，保证结果确定
    let mut entries: Vec<(Vec<Value>, Vec<Accumulator>)> = groups.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| x.sort_cmp(y))
            .find(|o| *o != std::cmp::Ordering::Equal)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut rows = Vec::with_capacity(entries.len());
    for (key, accs) in entries {
        let mut acc_iter = accs.into_iter();
        let mut row = Vec::with_capacity(items.len());
        for item in &items {
            match item {
                Item::Agg(_, _) => row.push(acc_iter.next().unwrap().finish()?),
                Item::Group(expr) => {
                    let idx = group_by.iter().position(|e| e == expr).unwrap();
                    row.push(key[idx].clone());
                }
            }
        }
        rows.push(row);
    }

    Ok(Rows { columns, rows })
}
