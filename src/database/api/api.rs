use std::sync::Arc;

use crate::database::api::response::DBResponse;
use crate::database::errors::{ExecError, Result};
use crate::database::pager::transaction::{CommitStatus, Transaction};
use crate::database::transactions::kvdb::*;
use crate::database::transactions::tx::*;
use crate::interpreter::*;

struct Database {
    db: Arc<KVDB>,
}

impl Database {
    fn new(path: &'static str) -> Self {
        Database {
            db: Arc::new(KVDB::new(path)),
        }
    }

    fn new_tx(&self, kind: TXKind) -> TX {
        self.db.begin(&self.db, kind)
    }

    fn commit_tx(&self, tx: TX) -> Result<CommitStatus> {
        self.db.commit(tx)
    }
}

impl Database {
    fn execute(&self, statement: Statement) -> Result<DBResponse> {
        match statement {
            Statement::Select(select_statement) => todo!(),
            Statement::Insert(insert_statement) => todo!(),
            Statement::Update(update_statement) => todo!(),
            Statement::Delete(delete_statement) => todo!(),
            Statement::Create(create_statement) => todo!(),
        }
    }
}

// SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = "hello" LIMIT -5 + 7;
// INSERT INTO table (col1, col2) VALUES (2*2), "Hello";

fn exec_select(tx: &mut TX, stmt: SelectStatement) -> Result<DBResponse> {
    let table = tx
        .get_table(&stmt.table)
        .ok_or_else(|| ExecError::ExecutionError("table not found"))?;

    // validate columns
    if let StatementColumns::Cols(ref columns) = stmt.columns {
        if !columns.iter().all(|e| table.col_exists(e).is_some()) {
            return Err(ExecError::ExecutionError("couldnt find column in table schema").into());
        };
    }

    Ok(DBResponse::default())
}

fn exec_insert(db: &Database, stmt: SelectStatement) -> Result<()> {
    todo!()
}
