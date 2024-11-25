use super::parser::ast::{Consts, Expression};

#[derive(Debug, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

impl Value {
    pub fn from_expression(expr: Expression) -> Self {
        match expr {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Integer(x)) => Self::Integer(x),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::String(s)) => Self::String(s),
        }
    }
}