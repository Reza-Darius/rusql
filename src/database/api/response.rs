use crate::{database::tables::Record, interpreter::Statement};

#[derive(Debug, Default)]
pub struct DBResponse {
    query_result: Option<QueryResponse>,
}

#[derive(Debug, Default)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Record>,
}

impl QueryResponse {
    fn render(&self) {}
}

impl DBResponse {
    pub fn new(stmt: &Statement, records: Option<&[Record]>) -> Self {
        todo!()
    }
    fn rows(self) -> String {
        todo!()
    }
}
