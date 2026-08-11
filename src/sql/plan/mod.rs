use std::fmt::Display;

use planner::Planner;

use crate::error::Result;

use super::{
    engine::Transaction,
    executor::{self, ResultSet},
    parser::ast::{self, Expression, OrderDirection},
    schema::Table,
    types::Value,
};

pub mod optimizer;
mod planner;

// 执行节点：组成执行计划树
#[derive(Debug, PartialEq)]
pub enum Node {
    // 创建表
    CreateTable {
        schema: Table,
    },

    // 插入数据
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },

    // 更新数据，source 是要更新的行的来源
    Update {
        table_name: String,
        source: Box<Node>,
        set: Vec<(String, Expression)>,
    },

    // 删除数据
    Delete {
        table_name: String,
        source: Box<Node>,
    },

    // 全表扫描，filter 是下推的过滤条件
    Scan {
        table_name: String,
        filter: Option<Expression>,
    },

    // 主键点查（优化器由 Scan + 主键等值条件改写而来）
    PointLookup {
        table_name: String,
        value: Value,
        filter: Option<Expression>,
    },

    // 嵌套循环连接
    NestedLoopJoin {
        left: Box<Node>,
        right: Box<Node>,
        predicate: Option<Expression>,
    },

    // 哈希连接（优化器由等值连接条件改写而来）
    HashJoin {
        left: Box<Node>,
        left_field: Expression,
        right: Box<Node>,
        right_field: Expression,
    },

    // 过滤
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },

    // 聚合：select 列表中包含聚合函数或存在 GROUP BY 时生成
    Aggregate {
        source: Box<Node>,
        select: Vec<(Expression, Option<String>)>,
        group_by: Vec<Expression>,
    },

    // 排序
    Order {
        source: Box<Node>,
        order_by: Vec<(Expression, OrderDirection)>,
    },

    // 跳过前 n 行
    Offset {
        source: Box<Node>,
        offset: usize,
    },

    // 只取前 n 行
    Limit {
        source: Box<Node>,
        limit: usize,
    },

    // 投影
    Projection {
        source: Box<Node>,
        select: Vec<(Expression, Option<String>)>,
    },

    // 输出执行计划而不执行
    Explain {
        source: Box<Node>,
    },
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_exprs(exprs: &[(Expression, Option<String>)]) -> String {
            exprs
                .iter()
                .map(|(e, alias)| match alias {
                    Some(a) => format!("{} AS {}", e, a),
                    None => format!("{}", e),
                })
                .collect::<Vec<_>>()
                .join(", ")
        }

        // 递归打印计划树
        fn fmt_node(
            node: &Node,
            f: &mut std::fmt::Formatter<'_>,
            prefix: &str,
        ) -> std::fmt::Result {
            let (desc, children): (String, Vec<&Node>) = match node {
                Node::CreateTable { schema } => (format!("CreateTable: {}", schema.name), vec![]),
                Node::Insert { table_name, .. } => (format!("Insert: {}", table_name), vec![]),
                Node::Update {
                    table_name, source, ..
                } => (format!("Update: {}", table_name), vec![source]),
                Node::Delete { table_name, source } => {
                    (format!("Delete: {}", table_name), vec![source])
                }
                Node::Scan { table_name, filter } => (
                    match filter {
                        Some(expr) => format!("Scan: {} ({})", table_name, expr),
                        None => format!("Scan: {}", table_name),
                    },
                    vec![],
                ),
                Node::PointLookup {
                    table_name,
                    value,
                    filter,
                } => (
                    match filter {
                        Some(expr) => {
                            format!("PointLookup: {} (pk = {}, {})", table_name, value, expr)
                        }
                        None => format!("PointLookup: {} (pk = {})", table_name, value),
                    },
                    vec![],
                ),
                Node::NestedLoopJoin {
                    left,
                    right,
                    predicate,
                } => (
                    match predicate {
                        Some(expr) => format!("NestedLoopJoin: {}", expr),
                        None => "NestedLoopJoin".to_string(),
                    },
                    vec![left, right],
                ),
                Node::HashJoin {
                    left,
                    left_field,
                    right,
                    right_field,
                } => (
                    format!("HashJoin: {} = {}", left_field, right_field),
                    vec![left, right],
                ),
                Node::Filter { source, predicate } => {
                    (format!("Filter: {}", predicate), vec![source])
                }
                Node::Aggregate {
                    source,
                    select,
                    group_by,
                } => (
                    if group_by.is_empty() {
                        format!("Aggregate: {}", fmt_exprs(select))
                    } else {
                        format!(
                            "Aggregate: {} GROUP BY {}",
                            fmt_exprs(select),
                            group_by
                                .iter()
                                .map(|e| e.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    },
                    vec![source],
                ),
                Node::Order { source, order_by } => (
                    format!(
                        "Order: {}",
                        order_by
                            .iter()
                            .map(|(e, d)| format!(
                                "{} {}",
                                e,
                                match d {
                                    OrderDirection::Asc => "asc",
                                    OrderDirection::Desc => "desc",
                                }
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    vec![source],
                ),
                Node::Offset { source, offset } => (format!("Offset: {}", offset), vec![source]),
                Node::Limit { source, limit } => (format!("Limit: {}", limit), vec![source]),
                Node::Projection { source, select } => {
                    (format!("Projection: {}", fmt_exprs(select)), vec![source])
                }
                Node::Explain { source } => ("Explain".to_string(), vec![source]),
            };

            writeln!(f, "{}{}", prefix, desc)?;
            let child_prefix = if prefix.is_empty() {
                "└─ ".to_string()
            } else {
                format!("   {}", prefix)
            };
            for child in children {
                fmt_node(child, f, &child_prefix)?;
            }
            Ok(())
        }

        fmt_node(self, f, "")
    }
}

#[derive(Debug, PartialEq)]
// 执行计划定义，底层是执行节点组成的树
pub struct Plan(pub Node);

impl Plan {
    // 构建并优化执行计划；构建和优化需要访问表结构信息
    pub fn build<T: Transaction>(stmt: ast::Statement, txn: &mut T) -> Result<Self> {
        match stmt {
            ast::Statement::Explain(inner) => {
                let plan = Self::build(*inner, txn)?;
                Ok(Plan(Node::Explain {
                    source: Box::new(plan.0),
                }))
            }
            stmt => {
                let node = Planner::new().build_statement(stmt, txn)?;
                let node = optimizer::optimize(node, txn)?;
                Ok(Plan(node))
            }
        }
    }

    pub fn execute<T: Transaction>(self, txn: &mut T) -> Result<ResultSet> {
        executor::execute(self.0, txn)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        sql::{
            engine::{kv::KVEngine, Engine, Transaction},
            parser::Parser,
            plan::{Node, Plan},
        },
        storage::memory::MemoryEngine,
    };

    // 建好表结构后构建语句的执行计划
    fn build_plan(sqls: &[&str], target: &str) -> Result<Plan> {
        let engine = KVEngine::new(MemoryEngine::new());
        let mut session = engine.session()?;
        for sql in sqls {
            session.execute(sql)?;
        }
        let stmt = Parser::new(target).parse()?;
        let mut txn = engine.begin()?;
        let plan = Plan::build(stmt, &mut txn)?;
        txn.rollback()?;
        Ok(plan)
    }

    #[test]
    fn test_plan_select_scan() -> Result<()> {
        let plan = build_plan(
            &["create table t (a int primary key, b text);"],
            "select * from t;",
        )?;
        assert_eq!(
            plan.0,
            Node::Scan {
                table_name: "t".to_string(),
                filter: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_plan_filter_pushdown() -> Result<()> {
        // WHERE 条件应该被下推进 Scan 节点
        let plan = build_plan(
            &["create table t (a int primary key, b int);"],
            "select * from t where b > 10;",
        )?;
        assert!(matches!(
            plan.0,
            Node::Scan {
                filter: Some(_),
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn test_plan_point_lookup() -> Result<()> {
        // 主键等值条件应该被改写成点查
        let plan = build_plan(
            &["create table t (a int primary key, b int);"],
            "select * from t where a = 3;",
        )?;
        assert!(matches!(plan.0, Node::PointLookup { .. }));
        Ok(())
    }

    #[test]
    fn test_plan_hash_join() -> Result<()> {
        // 等值连接应该选择 hash join
        let plan = build_plan(
            &[
                "create table t1 (a int primary key, b int);",
                "create table t2 (c int primary key, d int);",
            ],
            "select * from t1 join t2 on t1.a = t2.c;",
        )?;
        assert!(matches!(plan.0, Node::HashJoin { .. }));
        Ok(())
    }
}
