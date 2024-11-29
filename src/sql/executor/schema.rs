use crate::{error::Result, sql::schema::Table};

use super::Executor;

pub struct CreateTable {
    schema: Table,
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self {schema})
    }
}

impl Executor for CreateTable {
    fn executor(&self) -> Result<super::ResultSet> {
        todo!()
    }
}