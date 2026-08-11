use std::{cmp::Ordering, fmt::Display, hash::Hash};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::parser::ast::{Consts, Expression};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

// GROUP BY 和 hash join 需要把 Value 作为哈希表的 key。
// f64 没有实现 Eq/Hash，这里手动实现：浮点数按 bit 位哈希
// （NaN 不满足自反性，但 SQL 数据里正常不会出现 NaN）
impl Eq for Value {}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(b) => b.hash(state),
            Value::Integer(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "{}", s),
        }
    }
}

impl Value {
    pub fn from_expression(expr: Expression) -> Self {
        match expr {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::String(s)) => Self::String(s),
            _ => Self::Null,
        }
    }

    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }

    // SQL 比较语义：与 NULL 比较返回 None（上层视为 NULL），
    // 整数和浮点数可以跨类型比较
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            _ => None,
        }
    }

    // ORDER BY 使用的全序：NULL < Boolean < 数值 < String，
    // 类型内部按 compare 的语义
    pub fn sort_cmp(&self, other: &Value) -> Ordering {
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Null => 0,
                Value::Boolean(_) => 1,
                Value::Integer(_) | Value::Float(_) => 2,
                Value::String(_) => 3,
            }
        }
        match rank(self).cmp(&rank(other)) {
            Ordering::Equal => self.compare(other).unwrap_or(Ordering::Equal),
            ord => ord,
        }
    }

    // 算术运算，整数与浮点数混合运算时提升为浮点数
    pub fn checked_add(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
            (a, b) => Err(Error::Internal(format!("cannot add {} and {}", a, b))),
        }
    }

    pub fn checked_sub(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - *b as f64)),
            (a, b) => Err(Error::Internal(format!("cannot subtract {} and {}", a, b))),
        }
    }

    pub fn checked_mul(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a * *b as f64)),
            (a, b) => Err(Error::Internal(format!("cannot multiply {} and {}", a, b))),
        }
    }

    pub fn checked_div(&self, other: &Value) -> Result<Value> {
        // 除以 0 直接报错
        match other {
            Value::Integer(0) => return Err(Error::Internal("division by zero".into())),
            Value::Float(f) if *f == 0.0 => {
                return Err(Error::Internal("division by zero".into()))
            }
            _ => {}
        }
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a / b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 / b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a / *b as f64)),
            (a, b) => Err(Error::Internal(format!("cannot divide {} and {}", a, b))),
        }
    }
}

pub type Row = Vec<Value>;
