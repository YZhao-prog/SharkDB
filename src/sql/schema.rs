use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::types::{DataType, Row, Value};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    // 主键列的下标（建表时已校验必然存在唯一主键）
    pub fn primary_key_index(&self) -> usize {
        self.columns
            .iter()
            .position(|c| c.primary_key)
            .unwrap_or(0)
    }

    // 取一行数据的主键值
    pub fn primary_key_of<'a>(&self, row: &'a Row) -> &'a Value {
        &row[self.primary_key_index()]
    }

    // 校验一行数据的列数、空值约束和类型
    pub fn check_row(&self, row: &Row) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(Error::Internal(format!(
                "row has {} values, table {} has {} columns",
                row.len(),
                self.name,
                self.columns.len()
            )));
        }
        for (i, col) in self.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {}
                None => {
                    return Err(Error::Internal(format!(
                        "column {} cannot be null",
                        col.name
                    )))
                }
                Some(dt) if dt != col.datatype => {
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )))
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
}
