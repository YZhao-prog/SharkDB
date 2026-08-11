use std::fmt::Display;

use crate::sql::types::DataType;

// Abstract Syntax Tree 抽象语法树定义
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Select {
        // 选择列表：表达式 + 可选别名，SELECT * 对应 [(Expression::All, None)]
        select: Vec<(Expression, Option<String>)>,
        from: FromItem,
        r#where: Option<Expression>,
        group_by: Vec<Expression>,
        order_by: Vec<(Expression, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    Update {
        table_name: String,
        set: Vec<(String, Expression)>,
        r#where: Option<Expression>,
    },
    Delete {
        table_name: String,
        r#where: Option<Expression>,
    },
    // 输出执行计划而不执行
    Explain(Box<Statement>),
}

// FROM 子句：单表或 JOIN 树
#[derive(Debug, Clone, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        predicate: Option<Expression>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

// 列定义
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
}

// 表达式定义
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    // 常量
    Consts(Consts),
    // 列引用，可带表名限定：a 或 t.a
    Field(Option<String>, String),
    // SELECT * 以及 count(*) 中的 *
    All,
    // 函数调用（聚合函数）：count(*)、sum(a) 等
    Function(String, Box<Expression>),
    // 运算
    Operation(Operation),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Equal(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    Add(Box<Expression>, Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
}

impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

impl Expression {
    // 遍历表达式中引用到的所有列
    pub fn walk_fields(&self, visit: &mut impl FnMut(&Option<String>, &String)) {
        match self {
            Expression::Field(table, name) => visit(table, name),
            Expression::Function(_, arg) => arg.walk_fields(visit),
            Expression::Operation(op) => match op {
                Operation::Not(e) | Operation::Negate(e) => e.walk_fields(visit),
                Operation::And(l, r)
                | Operation::Or(l, r)
                | Operation::Equal(l, r)
                | Operation::NotEqual(l, r)
                | Operation::GreaterThan(l, r)
                | Operation::GreaterThanOrEqual(l, r)
                | Operation::LessThan(l, r)
                | Operation::LessThanOrEqual(l, r)
                | Operation::Add(l, r)
                | Operation::Subtract(l, r)
                | Operation::Multiply(l, r)
                | Operation::Divide(l, r) => {
                    l.walk_fields(visit);
                    r.walk_fields(visit);
                }
            },
            Expression::Consts(_) | Expression::All => {}
        }
    }

    // 是否包含聚合函数调用
    pub fn has_aggregate(&self) -> bool {
        match self {
            Expression::Function(_, _) => true,
            Expression::Operation(op) => match op {
                Operation::Not(e) | Operation::Negate(e) => e.has_aggregate(),
                Operation::And(l, r)
                | Operation::Or(l, r)
                | Operation::Equal(l, r)
                | Operation::NotEqual(l, r)
                | Operation::GreaterThan(l, r)
                | Operation::GreaterThanOrEqual(l, r)
                | Operation::LessThan(l, r)
                | Operation::LessThanOrEqual(l, r)
                | Operation::Add(l, r)
                | Operation::Subtract(l, r)
                | Operation::Multiply(l, r)
                | Operation::Divide(l, r) => l.has_aggregate() || r.has_aggregate(),
            },
            _ => false,
        }
    }

    // 把 AND 连接的条件拆成子条件列表（谓词下推时按子条件分配）
    pub fn split_conjuncts(self) -> Vec<Expression> {
        match self {
            Expression::Operation(Operation::And(l, r)) => {
                let mut out = l.split_conjuncts();
                out.extend(r.split_conjuncts());
                out
            }
            expr => vec![expr],
        }
    }

    // 把子条件列表重新用 AND 连接
    pub fn join_conjuncts(mut conjuncts: Vec<Expression>) -> Option<Expression> {
        let first = match conjuncts.is_empty() {
            true => return None,
            false => conjuncts.remove(0),
        };
        Some(conjuncts.into_iter().fold(first, |acc, e| {
            Expression::Operation(Operation::And(Box::new(acc), Box::new(e)))
        }))
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Consts(c) => match c {
                Consts::Null => write!(f, "NULL"),
                Consts::Boolean(b) => write!(f, "{}", b),
                Consts::Integer(i) => write!(f, "{}", i),
                Consts::Float(v) => write!(f, "{}", v),
                Consts::String(s) => write!(f, "'{}'", s),
            },
            Expression::Field(Some(table), name) => write!(f, "{}.{}", table, name),
            Expression::Field(None, name) => write!(f, "{}", name),
            Expression::All => write!(f, "*"),
            Expression::Function(name, arg) => write!(f, "{}({})", name, arg),
            Expression::Operation(op) => match op {
                Operation::And(l, r) => write!(f, "{} AND {}", l, r),
                Operation::Or(l, r) => write!(f, "{} OR {}", l, r),
                Operation::Not(e) => write!(f, "NOT {}", e),
                Operation::Equal(l, r) => write!(f, "{} = {}", l, r),
                Operation::NotEqual(l, r) => write!(f, "{} != {}", l, r),
                Operation::GreaterThan(l, r) => write!(f, "{} > {}", l, r),
                Operation::GreaterThanOrEqual(l, r) => write!(f, "{} >= {}", l, r),
                Operation::LessThan(l, r) => write!(f, "{} < {}", l, r),
                Operation::LessThanOrEqual(l, r) => write!(f, "{} <= {}", l, r),
                Operation::Add(l, r) => write!(f, "{} + {}", l, r),
                Operation::Subtract(l, r) => write!(f, "{} - {}", l, r),
                Operation::Multiply(l, r) => write!(f, "{} * {}", l, r),
                Operation::Divide(l, r) => write!(f, "{} / {}", l, r),
                Operation::Negate(e) => write!(f, "-{}", e),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}
