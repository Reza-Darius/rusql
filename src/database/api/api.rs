use std::sync::Arc;

use tracing::error;

use crate::database::api::response::DBResponse;
use crate::database::errors::{ExecError, Result};
use crate::database::pager::transaction::{CommitStatus, Transaction};
use crate::database::tables::Record;
use crate::database::tables::tables::{Index, Table};
use crate::database::transactions::kvdb::*;
use crate::database::transactions::tx::*;
use crate::interpreter::*;

struct Database {
    db: Arc<StorageEngine>,
}

impl Database {
    fn new(path: &'static str) -> Self {
        Database {
            db: Arc::new(StorageEngine::new(path)),
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
        // TODO: set up worker
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
    let table = tx.get_table(&stmt.table_name).ok_or_else(|| {
        error!(table = stmt.table_name, "table not found");
        ExecError::ExecutionError("table not found")
    })?;

    // no WHERE clause with wildcard
    if stmt.columns == StatementColumns::Wildcard && stmt.index.is_none() {
        // TODO: transform into response
        select_full_scan(tx, &table, stmt.get_limit())?;
    };

    // if indices.is_empty() {
    //     // no index found, fall back to full table scan
    //     todo!()
    // } else {
    //     // query by index
    //     // use where clause
    //     todo!()
    // }

    Ok(DBResponse::default())
}

fn select_columns(tx: &mut TX, table: &Table, stmt: &SelectStatement) -> Result<Vec<Record>> {
    // validate columns and get indices
    let mut index = None;
    if let StatementColumns::Cols(ref columns) = stmt.columns {
        for col in columns.iter() {
            if let Some(_) = table.col_exists(col) {
                let str_slice: Vec<&str> = columns.iter().map(|str| str.as_str()).collect();
                index = table.get_index(&str_slice[..]);
            } else {
                error!(col, "couldnt find column in table schema");
                return Err(
                    ExecError::ExecutionError("couldnt find column in table schema").into(),
                );
            }
        }
    }

    if index.is_none() {
        return select_full_scan(tx, table, stmt.get_limit());
    }
    todo!()
}

fn select_full_scan(tx: &mut TX, table: &Table, limit: Option<u32>) -> Result<Vec<Record>> {
    let mut iter = tx.full_table_scan(table)?;

    if let Some(limit) = limit {
        let mut res = vec![];
        for i in 0..limit {
            while let Some(rec) = iter.next() {
                res.push(Record::from_kv(rec))
            }
        }
        Ok(res)
    } else {
        Ok(iter.collect_records())
    }
}

fn validate_select(stmt: &SelectStatement) -> Result<()> {
    Ok(())
}

fn exec_insert(db: &Database, stmt: SelectStatement) -> Result<()> {
    todo!()
}
