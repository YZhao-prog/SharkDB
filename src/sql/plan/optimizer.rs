// Rule-based 查询优化器，作用于执行计划树：
//
// 1. 常量折叠：1 + 2 * 3 在计划期折叠成 7
// 2. 谓词下推：Filter 尽量下推到 Scan；跨 Join 的条件按引用的列拆分到两侧
// 3. 主键点查：Scan + 主键等值条件 改写为 PointLookup（O(n) 全表扫描 → O(1) 点读）
// 4. Join 算法选择：等值连接条件的 NestedLoopJoin（O(n*m)）改写为 HashJoin（O(n+m)）
//
// 规则自底向上应用；改写产生的新 Filter 会再次递归优化，直到不动点

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        executor::expression::{self, ColumnLabel},
        parser::ast::{Consts, Expression, Operation},
        types::Value,
    },
};

use super::Node;

pub fn optimize<T: Transaction>(node: Node, txn: &T) -> Result<Node> {
    let node = fold_node_constants(node)?;
    optimize_node(node, txn)
}

fn optimize_node<T: Transaction>(node: Node, txn: &T) -> Result<Node> {
    // 先递归优化子节点
    let node = map_children(node, &mut |child| optimize_node(child, txn))?;

    // 再对当前节点应用规则
    match node {
        // 规则：谓词下推
        Node::Filter { source, predicate } => match *source {
            // Filter + Scan 合并：谓词进入 Scan 节点
            Node::Scan { table_name, filter } => {
                let merged = merge_predicates(filter, predicate);
                // 合并后可能触发主键点查规则
                optimize_node(
                    Node::Scan {
                        table_name,
                        filter: merged,
                    },
                    txn,
                )
            }
            Node::PointLookup {
                table_name,
                value,
                filter,
            } => Ok(Node::PointLookup {
                table_name,
                value,
                filter: merge_predicates(filter, predicate),
            }),
            // Filter + Join：把条件按引用的列拆分到左右两侧
            Node::NestedLoopJoin {
                left,
                right,
                predicate: join_predicate,
            } => {
                let left_columns = node_columns(&left, txn)?;
                let right_columns = node_columns(&right, txn)?;

                let mut left_conjuncts = Vec::new();
                let mut right_conjuncts = Vec::new();
                let mut keep = Vec::new();
                for conjunct in predicate.split_conjuncts() {
                    if all_fields_resolve(&conjunct, &left_columns) {
                        left_conjuncts.push(conjunct);
                    } else if all_fields_resolve(&conjunct, &right_columns) {
                        right_conjuncts.push(conjunct);
                    } else {
                        keep.push(conjunct);
                    }
                }

                let left = push_filter(*left, left_conjuncts);
                let right = push_filter(*right, right_conjuncts);
                let predicate = match join_predicate {
                    Some(p) => {
                        keep.insert(0, p);
                        Expression::join_conjuncts(keep)
                    }
                    None => Expression::join_conjuncts(keep),
                };
                // 下推后的子树重新优化（触发 Scan 合并、hash join 选择）
                optimize_node(
                    Node::NestedLoopJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        predicate,
                    },
                    txn,
                )
            }
            // Filter + HashJoin（join 已被前序规则改写）：同样按侧拆分下推
            Node::HashJoin {
                left,
                left_field,
                right,
                right_field,
            } => {
                let left_columns = node_columns(&left, txn)?;
                let right_columns = node_columns(&right, txn)?;

                let mut left_conjuncts = Vec::new();
                let mut right_conjuncts = Vec::new();
                let mut keep = Vec::new();
                for conjunct in predicate.split_conjuncts() {
                    if all_fields_resolve(&conjunct, &left_columns) {
                        left_conjuncts.push(conjunct);
                    } else if all_fields_resolve(&conjunct, &right_columns) {
                        right_conjuncts.push(conjunct);
                    } else {
                        keep.push(conjunct);
                    }
                }

                // 只重新优化被包了新 Filter 的子树，剩余条件留在 join 之上，避免无限递归
                let left = optimize_node(push_filter(*left, left_conjuncts), txn)?;
                let right = optimize_node(push_filter(*right, right_conjuncts), txn)?;
                let join = Node::HashJoin {
                    left: Box::new(left),
                    left_field,
                    right: Box::new(right),
                    right_field,
                };
                Ok(match Expression::join_conjuncts(keep) {
                    Some(predicate) => Node::Filter {
                        source: Box::new(join),
                        predicate,
                    },
                    None => join,
                })
            }
            source => Ok(Node::Filter {
                source: Box::new(source),
                predicate,
            }),
        },

        // 规则：主键等值条件改写为点查
        Node::Scan {
            table_name,
            filter: Some(filter),
        } => {
            let table = txn.must_get_table(table_name.clone())?;
            let pk_name = &table.columns[table.primary_key_index()].name;

            let mut remaining = Vec::new();
            let mut pk_value = None;
            for conjunct in filter.split_conjuncts() {
                if pk_value.is_none() {
                    if let Some(value) = match_pk_equality(&conjunct, &table_name, pk_name) {
                        pk_value = Some(value);
                        continue;
                    }
                }
                remaining.push(conjunct);
            }

            match pk_value {
                Some(value) => Ok(Node::PointLookup {
                    table_name,
                    value,
                    filter: Expression::join_conjuncts(remaining),
                }),
                None => Ok(Node::Scan {
                    table_name,
                    filter: Expression::join_conjuncts(remaining),
                }),
            }
        }

        // 规则：等值连接选择 hash join
        Node::NestedLoopJoin {
            left,
            right,
            predicate: Some(predicate),
        } => {
            let left_columns = node_columns(&left, txn)?;
            let right_columns = node_columns(&right, txn)?;

            let mut join_key = None;
            let mut remaining = Vec::new();
            for conjunct in predicate.split_conjuncts() {
                if join_key.is_none() {
                    if let Some(key) =
                        match_equi_join(&conjunct, &left_columns, &right_columns)
                    {
                        join_key = Some(key);
                        continue;
                    }
                }
                remaining.push(conjunct);
            }

            match join_key {
                Some((left_field, right_field)) => {
                    let join = Node::HashJoin {
                        left,
                        left_field,
                        right,
                        right_field,
                    };
                    // 剩余条件作为 join 之后的过滤
                    Ok(match Expression::join_conjuncts(remaining) {
                        Some(predicate) => Node::Filter {
                            source: Box::new(join),
                            predicate,
                        },
                        None => join,
                    })
                }
                // 没有可用的等值条件，remaining 里是全部条件
                None => Ok(Node::NestedLoopJoin {
                    left,
                    right,
                    predicate: Expression::join_conjuncts(remaining),
                }),
            }
        }

        node => Ok(node),
    }
}

// 对所有子节点应用变换，重建当前节点
fn map_children(node: Node, f: &mut impl FnMut(Node) -> Result<Node>) -> Result<Node> {
    Ok(match node {
        Node::Update {
            table_name,
            source,
            set,
        } => Node::Update {
            table_name,
            source: Box::new(f(*source)?),
            set,
        },
        Node::Delete { table_name, source } => Node::Delete {
            table_name,
            source: Box::new(f(*source)?),
        },
        Node::NestedLoopJoin {
            left,
            right,
            predicate,
        } => Node::NestedLoopJoin {
            left: Box::new(f(*left)?),
            right: Box::new(f(*right)?),
            predicate,
        },
        Node::HashJoin {
            left,
            left_field,
            right,
            right_field,
        } => Node::HashJoin {
            left: Box::new(f(*left)?),
            left_field,
            right: Box::new(f(*right)?),
            right_field,
        },
        Node::Filter { source, predicate } => Node::Filter {
            source: Box::new(f(*source)?),
            predicate,
        },
        Node::Aggregate {
            source,
            select,
            group_by,
        } => Node::Aggregate {
            source: Box::new(f(*source)?),
            select,
            group_by,
        },
        Node::Order { source, order_by } => Node::Order {
            source: Box::new(f(*source)?),
            order_by,
        },
        Node::Offset { source, offset } => Node::Offset {
            source: Box::new(f(*source)?),
            offset,
        },
        Node::Limit { source, limit } => Node::Limit {
            source: Box::new(f(*source)?),
            limit,
        },
        Node::Projection { source, select } => Node::Projection {
            source: Box::new(f(*source)?),
            select,
        },
        Node::Explain { source } => Node::Explain {
            source: Box::new(f(*source)?),
        },
        // 叶子节点
        node => node,
    })
}

// ---------------- 常量折叠 ----------------

fn fold_node_constants(node: Node) -> Result<Node> {
    let node = map_children(node, &mut fold_node_constants)?;
    Ok(match node {
        Node::Filter { source, predicate } => Node::Filter {
            source,
            predicate: fold_constants(predicate)?,
        },
        Node::Scan {
            table_name,
            filter: Some(filter),
        } => Node::Scan {
            table_name,
            filter: Some(fold_constants(filter)?),
        },
        Node::Projection { source, select } => Node::Projection {
            source,
            select: select
                .into_iter()
                .map(|(e, a)| Ok((fold_constants(e)?, a)))
                .collect::<Result<Vec<_>>>()?,
        },
        Node::Update {
            table_name,
            source,
            set,
        } => Node::Update {
            table_name,
            source,
            set: set
                .into_iter()
                .map(|(c, e)| Ok((c, fold_constants(e)?)))
                .collect::<Result<Vec<_>>>()?,
        },
        node => node,
    })
}

// 后序遍历表达式，纯常量子表达式直接求值
fn fold_constants(expr: Expression) -> Result<Expression> {
    fn is_const(expr: &Expression) -> bool {
        matches!(expr, Expression::Consts(_))
    }

    fn value_to_consts(value: Value) -> Expression {
        Expression::Consts(match value {
            Value::Null => Consts::Null,
            Value::Boolean(b) => Consts::Boolean(b),
            Value::Integer(i) => Consts::Integer(i),
            Value::Float(f) => Consts::Float(f),
            Value::String(s) => Consts::String(s),
        })
    }

    // 只折叠纯计算类运算，AND/OR/NOT 保持结构（用于谓词拆分）
    let expr = match expr {
        Expression::Operation(op) => {
            use Operation::*;
            let fold2 = |ctor: fn(Box<Expression>, Box<Expression>) -> Operation,
                         l: Box<Expression>,
                         r: Box<Expression>|
             -> Result<Expression> {
                let l = fold_constants(*l)?;
                let r = fold_constants(*r)?;
                let folded = ctor(Box::new(l), Box::new(r));
                if let Operation::And(l, r) | Operation::Or(l, r) = &folded {
                    // 布尔结构保留
                    let _ = (l, r);
                    return Ok(Expression::Operation(folded));
                }
                let candidate = Expression::Operation(folded);
                Ok(candidate)
            };
            match op {
                And(l, r) => fold2(And, l, r)?,
                Or(l, r) => fold2(Or, l, r)?,
                Equal(l, r) => fold2(Equal, l, r)?,
                NotEqual(l, r) => fold2(NotEqual, l, r)?,
                GreaterThan(l, r) => fold2(GreaterThan, l, r)?,
                GreaterThanOrEqual(l, r) => fold2(GreaterThanOrEqual, l, r)?,
                LessThan(l, r) => fold2(LessThan, l, r)?,
                LessThanOrEqual(l, r) => fold2(LessThanOrEqual, l, r)?,
                Add(l, r) => fold2(Add, l, r)?,
                Subtract(l, r) => fold2(Subtract, l, r)?,
                Multiply(l, r) => fold2(Multiply, l, r)?,
                Divide(l, r) => fold2(Divide, l, r)?,
                Not(e) => Expression::Operation(Not(Box::new(fold_constants(*e)?))),
                Negate(e) => Expression::Operation(Negate(Box::new(fold_constants(*e)?))),
            }
        }
        Expression::Function(name, arg) => {
            Expression::Function(name, Box::new(fold_constants(*arg)?))
        }
        expr => expr,
    };

    // 所有操作数都是常量时求值折叠
    if let Expression::Operation(op) = &expr {
        use Operation::*;
        let const_operands = match op {
            Not(e) | Negate(e) => is_const(e),
            And(l, r) | Or(l, r) | Equal(l, r) | NotEqual(l, r) | GreaterThan(l, r)
            | GreaterThanOrEqual(l, r) | LessThan(l, r) | LessThanOrEqual(l, r) | Add(l, r)
            | Subtract(l, r) | Multiply(l, r) | Divide(l, r) => is_const(l) && is_const(r),
        };
        if const_operands {
            // 求值失败（如除零）时保留原表达式，让错误在执行期抛出
            if let Ok(value) = expression::evaluate(&expr, &[], &vec![]) {
                return Ok(value_to_consts(value));
            }
        }
    }
    Ok(expr)
}

// ---------------- 规则辅助 ----------------

fn merge_predicates(existing: Option<Expression>, new: Expression) -> Option<Expression> {
    match existing {
        Some(e) => Expression::join_conjuncts(vec![e, new]),
        None => Some(new),
    }
}

fn push_filter(node: Node, conjuncts: Vec<Expression>) -> Node {
    match Expression::join_conjuncts(conjuncts) {
        Some(predicate) => Node::Filter {
            source: Box::new(node),
            predicate,
        },
        None => node,
    }
}

// 计算一个查询子树输出的列标签（谓词下推需要判断条件归属哪一侧）
fn node_columns<T: Transaction>(node: &Node, txn: &T) -> Result<Vec<ColumnLabel>> {
    match node {
        Node::Scan { table_name, .. } | Node::PointLookup { table_name, .. } => {
            let table = txn.must_get_table(table_name.clone())?;
            Ok(table
                .columns
                .iter()
                .map(|c| (Some(table_name.clone()), c.name.clone()))
                .collect())
        }
        Node::NestedLoopJoin { left, right, .. } | Node::HashJoin { left, right, .. } => {
            let mut columns = node_columns(left, txn)?;
            columns.extend(node_columns(right, txn)?);
            Ok(columns)
        }
        Node::Filter { source, .. }
        | Node::Order { source, .. }
        | Node::Offset { source, .. }
        | Node::Limit { source, .. } => node_columns(source, txn),
        node => Err(Error::Internal(format!(
            "cannot compute columns for node {:?}",
            node
        ))),
    }
}

// 表达式引用的所有列是否都能在给定列集中解析
fn all_fields_resolve(expr: &Expression, columns: &[ColumnLabel]) -> bool {
    let mut ok = true;
    expr.walk_fields(&mut |table, name| {
        if expression::resolve(columns, table, name).is_err() {
            ok = false;
        }
    });
    ok
}

// 匹配主键等值条件：pk = const 或 const = pk
fn match_pk_equality(expr: &Expression, table_name: &str, pk_name: &str) -> Option<Value> {
    let Expression::Operation(Operation::Equal(l, r)) = expr else {
        return None;
    };
    let is_pk = |e: &Expression| match e {
        Expression::Field(table, name) => {
            name == pk_name && table.as_deref().map_or(true, |t| t == table_name)
        }
        _ => false,
    };
    let as_const = |e: &Expression| match e {
        Expression::Consts(c) => Some(Value::from_expression(Expression::Consts(c.clone()))),
        _ => None,
    };
    if is_pk(l) {
        as_const(r)
    } else if is_pk(r) {
        as_const(l)
    } else {
        None
    }
}

// 匹配等值连接条件：左右两个列引用分属 join 的两侧
fn match_equi_join(
    expr: &Expression,
    left_columns: &[ColumnLabel],
    right_columns: &[ColumnLabel],
) -> Option<(Expression, Expression)> {
    let Expression::Operation(Operation::Equal(l, r)) = expr else {
        return None;
    };
    let (Expression::Field(_, _), Expression::Field(_, _)) = (l.as_ref(), r.as_ref()) else {
        return None;
    };
    if all_fields_resolve(l, left_columns) && all_fields_resolve(r, right_columns) {
        Some((l.as_ref().clone(), r.as_ref().clone()))
    } else if all_fields_resolve(r, left_columns) && all_fields_resolve(l, right_columns) {
        Some((r.as_ref().clone(), l.as_ref().clone()))
    } else {
        None
    }
}
