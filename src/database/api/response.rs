use crate::database::tables::Record;

pub struct DBResponse {
    query_result: Option<QueryResponse>,
}

impl Default for DBResponse {
    fn default() -> Self {
        Self {
            query_result: Default::default(),
        }
    }
}

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
