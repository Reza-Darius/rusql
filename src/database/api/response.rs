use crate::database::tables::Record;

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
    fn rows(self) -> String {
        todo!()
    }
}
