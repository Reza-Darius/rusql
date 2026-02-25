use std::sync::Arc;

use tracing::error;

use crate::database::api::response::DBResponse;
use crate::database::btree::ScanMode;
use crate::database::errors::{ExecError, Result};
use crate::database::pager::transaction::{CommitStatus, Transaction};
use crate::database::tables::tables::{Index, Table};
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

/// resolving select statement with where clause
fn select_where(tx: &mut TX, table: &Table, stmt: &SelectStatement) -> Result<Vec<Record>> {
    let indices = stmt
        .index
        .as_ref()
        .expect("this function only gets called with where clauses");

    // check columns and data types
    let mut cols: Vec<_> = vec![];
    for index in indices.iter() {
        if !table.valid_col(&index.column, &index.expr) {
            error!(?index, "invaild column for index");
            return Err(ExecError::ExecutionError(
                "invalid index, check column name and provided data type",
            )
            .into());
        }
        cols.push(index.column.as_str())
    }

    // do we have an index?
    // we try to find the index covering the most columns
    let mut search_index = None;
    let mut covered: u16 = cols.len() as u16; // in case not all columns are covered, we need to know which ones to filter later
    for i in (0..cols.len()).rev() {
        if let Some(index) = table.get_index(&cols[..i]) {
            search_index = Some(index);
            break;
        };
        covered += 1
    }

    if let Some(index) = search_index {
        // construct key
        // scan iter
        // check each record against uncovered columns
        todo!()
    } else {
        // we fall back to columns
        let res = select_columns(tx, table, stmt)?;
        // check each record to filter
    }
    todo!()
}

fn record_matches_index(record: &Record, stmt_index: &StatementIndex, index: &Index) -> bool {
    true
}

/// resolving select statement with provided columns
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

// INSERT INTO table (col1, col2) VALUES (2*2), "Hello";
fn exec_insert(db: &Database, stmt: SelectStatement) -> Result<()> {
    todo!()
}
