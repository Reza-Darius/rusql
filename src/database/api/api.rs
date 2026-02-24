use std::sync::Arc;

use tracing::error;

use crate::database::api::response::DBResponse;
use crate::database::btree::ScanMode;
use crate::database::errors::{ExecError, Result};
use crate::database::pager::transaction::{CommitStatus, Transaction};
use crate::database::tables::tables::Table;
use crate::database::tables::{Query, Record};
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

    let res = if stmt.index.is_some() {
        select_where(tx, &table, &stmt)?
    } else {
        select_columns(tx, &table, &stmt)?
    };

    Ok(DBResponse::default())
}

fn select_where(tx: &mut TX, table: &Table, stmt: &SelectStatement) -> Result<Vec<Record>> {
    let indices = stmt
        .index
        .as_ref()
        .expect("this function only gets called with where clauses");
    // check columns and data types
    let mut search_indices: Vec<_> = vec![];
    for index in indices.iter() {
        if !table.valid_col(&index.column, &index.expr) {
            error!(?index, "invaild column for index");
            return Err(ExecError::ExecutionError(
                "invalid index, check column name and provided data type",
            )
            .into());
        }
        // do we have an index
        if let Some(search_index) = table.get_index(std::slice::from_ref(&index.column)) {
            search_indices.push(search_index)
        };
    }
    // if search_indices.is_empty() {} else {
    //     let key = ScanMode::open(key, cmp)
    // }
    todo!()
}

fn record_matches_index(record: &Record, index: &StatementIndex, col_indices: &[usize]) -> bool {
    true
}

fn select_columns(tx: &mut TX, table: &Table, stmt: &SelectStatement) -> Result<Vec<Record>> {
    match &stmt.columns {
        StatementColumns::Wildcard => return select_full_scan(tx, table, stmt.get_limit()),
        StatementColumns::Cols(columns) => {
            // do the provided columns exist?
            for col in columns {
                if table.col_exists(col).is_none() {
                    error!(col, "couldnt find column in table schema");
                    return Err(
                        ExecError::ExecutionError("couldnt find column in table schema").into(),
                    );
                }
            }
            // do we have a matching index?
            if let Some(index) = table.get_index(columns.as_slice()) {
                let key = Query::by_tid_prefix(table, index.prefix);
                Ok(ScanMode::prefix(key, &tx.tree)?.collect_records())
            // if not, default to full table scan
            } else {
                select_full_scan(tx, table, stmt.get_limit())
            }
        }
    }
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

fn exec_insert(db: &Database, stmt: SelectStatement) -> Result<()> {
    todo!()
}
