use std::sync::Arc;

use crate::database::errors::Result;
use crate::database::transactions::kvdb::KVDB;
use crate::interpreter::*;

struct QueryResult {
    result: Option<String>,
}

impl QueryResult {
    fn rows(self) -> String {
        todo!()
    }
}

trait SQLInterface {
    fn execute(&self, statement: Statement) -> Result<QueryResult>;
}

struct Database {
    db: Arc<KVDB>,
}

impl Database {
    fn new(path: &'static str) -> Self {
        Database {
            db: Arc::new(KVDB::new(path)),
        }
    }
}

impl SQLInterface for Database {
    fn execute(&self, statement: Statement) -> Result<QueryResult> {
        match statement {
            Statement::Select(select_statement) => todo!(),
            Statement::Insert(insert_statement) => todo!(),
            Statement::Update(update_statement) => todo!(),
            Statement::Delete(delete_statement) => todo!(),
            Statement::Create(create_statement) => todo!(),
        }
    }
}

fn exec_select(db: &Database, stmt: SelectStatement) -> Result<()> {
    todo!()
}
