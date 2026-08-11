use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        executor::expression,
        parser::ast::{self, Expression},
        schema::{self, Table},
        types::Value,
    },
};

use super::Node;

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn build_statement<T: Transaction>(
        &self,
        stmt: ast::Statement,
        _txn: &mut T,
    ) -> Result<Node> {
        Ok(match stmt {
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Self::build_schema(name, columns)?,
            },

            ast::Statement::Insert {
                table_name,
                columns,
                values,
            } => Node::Insert {
                table_name,
                columns: columns.unwrap_or_default(),
                values,
            },

            ast::Statement::Update {
                table_name,
                set,
                r#where,
            } => Node::Update {
                table_name: table_name.clone(),
                source: Box::new(Self::build_filtered_scan(table_name, r#where)),
                set,
            },

            ast::Statement::Delete {
                table_name,
                r#where,
            } => Node::Delete {
                table_name: table_name.clone(),
                source: Box::new(Self::build_filtered_scan(table_name, r#where)),
            },

            ast::Statement::Select {
                select,
                from,
                r#where,
                group_by,
                order_by,
                limit,
                offset,
            } => {
                let mut node = Self::build_from_item(from);

                if let Some(predicate) = r#where {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate,
                    };
                }

                // select 列表中包含聚合函数或存在 GROUP BY 时构建聚合节点，
                // 聚合节点直接产出最终列，不再需要投影
                let has_aggregate = select.iter().any(|(e, _)| e.has_aggregate());
                let is_aggregate = has_aggregate || !group_by.is_empty();
                if is_aggregate {
                    node = Node::Aggregate {
                        source: Box::new(node),
                        select: select.clone(),
                        group_by,
                    };
                }

                if !order_by.is_empty() {
                    node = Node::Order {
                        source: Box::new(node),
                        order_by,
                    };
                }

                if let Some(expr) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: Self::eval_constant_usize(&expr, "offset")?,
                    };
                }
                if let Some(expr) = limit {
                    node = Node::Limit {
                        source: Box::new(node),
                        limit: Self::eval_constant_usize(&expr, "limit")?,
                    };
                }

                // SELECT * 不需要投影
                let select_all = select.len() == 1 && select[0].0 == Expression::All;
                if !is_aggregate && !select_all {
                    node = Node::Projection {
                        source: Box::new(node),
                        select,
                    };
                }
                node
            }

            ast::Statement::Explain(_) => {
                return Err(Error::Internal("explain handled by Plan::build".into()))
            }
        })
    }

    // FROM 子句转换成 Scan / Join 树
    fn build_from_item(item: ast::FromItem) -> Node {
        match item {
            ast::FromItem::Table { name } => Node::Scan {
                table_name: name,
                filter: None,
            },
            ast::FromItem::Join {
                left,
                right,
                join_type: _,
                predicate,
            } => Node::NestedLoopJoin {
                left: Box::new(Self::build_from_item(*left)),
                right: Box::new(Self::build_from_item(*right)),
                predicate,
            },
        }
    }

    fn build_filtered_scan(table_name: String, filter: Option<Expression>) -> Node {
        match filter {
            Some(predicate) => Node::Filter {
                source: Box::new(Node::Scan {
                    table_name,
                    filter: None,
                }),
                predicate,
            },
            None => Node::Scan {
                table_name,
                filter: None,
            },
        }
    }

    // 建表：校验主键约束，未显式指定主键时第一列作为主键
    fn build_schema(name: String, columns: Vec<ast::Column>) -> Result<Table> {
        let pk_count = columns.iter().filter(|c| c.primary_key).count();
        if pk_count > 1 {
            return Err(Error::Internal(format!(
                "table {} has multiple primary keys",
                name
            )));
        }
        let implicit_pk = pk_count == 0;

        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                let primary_key = c.primary_key || (implicit_pk && i == 0);
                // 主键不允许为空
                let nullable = if primary_key {
                    false
                } else {
                    c.nullable.unwrap_or(true)
                };
                let default = match c.default {
                    Some(expr) => Some(expression::evaluate(&expr, &[], &vec![])?),
                    None if nullable => Some(Value::Null),
                    None => None,
                };

                Ok(schema::Column {
                    name: c.name,
                    datatype: c.datatype,
                    nullable,
                    default,
                    primary_key,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Table { name, columns })
    }

    // LIMIT/OFFSET 必须是非负整数常量
    fn eval_constant_usize(expr: &Expression, what: &str) -> Result<usize> {
        match expression::evaluate(expr, &[], &vec![])? {
            Value::Integer(i) if i >= 0 => Ok(i as usize),
            v => Err(Error::Internal(format!("invalid {} value {}", what, v))),
        }
    }
}
