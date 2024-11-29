use crate::error::Result;

use super::Executor;

pub struct Scan {
    table_name: String,
}

impl Scan {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(Self {table_name})
    }
}

impl Executor for Scan {
    fn executor(&self) -> Result<super::ResultSet> {
        todo!()
    }
}