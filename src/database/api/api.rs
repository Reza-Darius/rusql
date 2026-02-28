use std::collections::HashMap;
use std::slice;
use std::sync::Arc;

use tracing::{debug, error, info};

use crate::database::api::response::DBResponse;
use crate::database::api::types::IteratorDB;
use crate::database::btree::ScanMode;
use crate::database::errors::{ExecError, Result};
use crate::database::pager::transaction::{CommitStatus, Transaction};
use crate::database::tables::tables::{Table, TableIndex};
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
    pub fn execute(&self, statement: Statement) -> Result<DBResponse> {
        // TODO: set up worker
        let mut tx = self.new_tx(TXKind::Read);
        let res = match statement {
            Statement::Select(select_statement) => exec_select(&mut tx, select_statement),
            Statement::Insert(insert_statement) => todo!(),
            Statement::Update(update_statement) => todo!(),
            Statement::Delete(delete_statement) => todo!(),
            Statement::Create(create_statement) => todo!(),
        };
        let com_res = self.commit_tx(tx);
        res
    }
}

// SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = "hello" LIMIT -5 + 7;

fn exec_select(tx: &mut TX, stmt: SelectStatement) -> Result<DBResponse> {
    info!(?stmt, "executing select statemetn");

    let table = tx.get_table(&stmt.table_name).ok_or_else(|| {
        error!(table = stmt.table_name, "table not found");
        ExecError::ExecutionError("table not found")
    })?;

    let res = if stmt.index.is_some() {
        select_where(tx, &table, &stmt)?
    } else {
        select_columns(tx, &table, &stmt)?
    };
    // TODO: filter
    Ok(DBResponse::from_records(&table, res.as_slice()))
}

/// resolving select statement with where clause
fn select_where(tx: &mut TX, table: &Table, stmt: &SelectStatement) -> Result<Vec<Record>> {
    let indices = stmt.index.as_ref().ok_or_else(|| {
        error!("select_where() called without WHERE clause");
        ExecError::ExecutionError("select_where() called without WHERE clause")
    })?;

    // mapping column indices to WHERE clauses
    let col_map = validate_where_clause(table, &indices[..])?;

    if let Some((table_index, stmt_index)) = find_index(table, &col_map) {
        let key = Query::by_index(table, table_index)
            .add(stmt_index.expr.clone())
            .encode()?;
        let scan = ScanMode::open(key, stmt_index.operator.into())?;

        // filter results against non-indexed WHERE clauses
        let res: Vec<_> = scan
            .into_iter(&tx.tree)
            .ok_or_else(|| {
                error!("failed to create iterator");
                ExecError::ExecutionError("failed to create iterator")
            })?
            .filter_map(|(k, v)| Record::decode_with_index(k, v, table_index, table).ok()) // reorder into primary row layout
            .filter(|rec| filter_record(rec, &col_map))
            .limit(stmt)
            .collect();

        return Ok(res);
    }
    // no index found: we fall back to SELECT columns
    let scan = select_columns(tx, table, stmt)?;

    // filter results against WHERE clauses
    let res: Vec<Record> = scan
        .into_iter()
        .filter(|rec| filter_record(rec, &col_map))
        .limit(stmt)
        .collect();

    Ok(res)
}

// check columns and data types
/// mapping index in column array to statment index for later filtering
fn validate_where_clause<'a>(
    table: &Table,
    statements: &'a [StatementIndex],
) -> Result<HashMap<usize, &'a StatementIndex>> {
    let mut col_map = HashMap::new();

    for stmt in statements {
        if !table.validate_col_data(&stmt.column, &stmt.expr) {
            error!(?stmt, "invaild column for index");
            return Err(ExecError::ExecutionError(
                "invalid index, check column name and provided data type",
            )
            .into());
        }
        let col_idx = table
            .get_col_idx(&stmt.column)
            .expect("we just validated it");
        col_map.insert(col_idx, stmt);
    }
    Ok(col_map)
}

// TODO: support multi key indices
/// do we have an index we can query by?
fn find_index<'a, 'b>(
    table: &'a Table,
    col_map: &'b HashMap<usize, &'b StatementIndex>,
) -> Option<(&'a TableIndex, &'b StatementIndex)> {
    let mut search_index = None;
    for (k, v) in col_map.iter() {
        if let Some(table_index) = table.get_index(slice::from_ref(&table.cols[*k].title)) {
            assert_eq!(
                table_index.columns.len(),
                1,
                "as of now, we are only supporting single key indices"
            );
            search_index = Some((table_index, *v));
            break;
        };
    }
    search_index
}

/// a record needs to be in the primary row layout
fn filter_record<'a>(record: &'a Record, col_map: &HashMap<usize, &StatementIndex>) -> bool {
    for (col, index) in col_map {
        let data = record[*col].as_ref(); // converting to comparable types without reallocting
        if !match index.operator {
            Operator::Assign => data == (&index.expr).into(),
            Operator::Equal => data == (&index.expr).into(),
            Operator::Lt => data < (&index.expr).into(),
            Operator::Le => data <= (&index.expr).into(),
            Operator::Gt => data > (&index.expr).into(),
            Operator::Ge => data >= (&index.expr).into(),
            _ => unreachable!("invalid operator are already filtered out"),
        } {
            return false;
        };
    }
    true
}

// fn trim_record(record: &Record, cols: StatementColumns) -> Option<

/// resolving select statement with designated columns
fn select_columns(tx: &mut TX, table: &Table, stmt: &SelectStatement) -> Result<Vec<Record>> {
    match &stmt.columns {
        StatementColumns::Cols(columns) => {
            // do the provided columns exist?
            for col in columns {
                if !table.col_exists(col) {
                    error!(col, "couldnt find column in table schema");
                    return Err(
                        ExecError::ExecutionError("couldnt find column in table schema").into(),
                    );
                }
            }
            // do we have a matching index?
            if let Some(index) = table.get_index(columns.as_slice()) {
                debug!(columns = ?columns, index = ?index, "index found for SELECT columns");
                let key = Query::by_tid_prefix(table, index.prefix);
                let res: Vec<Record> = ScanMode::prefix(key, &tx.tree)?
                    // reorder into primary row layout
                    .filter_map(|(k, v)| Record::decode_with_index(k, v, index, table).ok())
                    .limit(stmt)
                    .collect();
                return Ok(res);
            }
            // fall back to full table scan
            ()
        }
        StatementColumns::Wildcard => (),
    }

    let res = tx
        .full_table_scan(table)?
        .map(Record::from_kv)
        .limit(stmt)
        .collect();

    Ok(res)
}

// INSERT INTO table (col1, col2) VALUES (2*2), "Hello";
fn exec_insert(db: &Database, stmt: SelectStatement) -> Result<()> {
    todo!()
}

#[cfg(test)]
mod execute_test {
    use crate::database::{
        btree::SetFlag,
        helper::cleanup_file,
        tables::{TypeCol, tables::TableBuilder},
    };

    use super::*;
    use test_log::test;

    #[test]
    fn select_exec_positive1() -> Result<()> {
        let path = "test-files/exec_select_stmt1.rdb";
        cleanup_file(path);
        let db = Database::new(path);
        let mut tx = db.db.begin(&db.db, TXKind::Write);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .add_col("id", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        let mut entries = vec![];
        entries.push(Record::new().add("Alice").add(20).add(1));
        entries.push(Record::new().add("Bob").add(15).add(2));
        entries.push(Record::new().add("Charlie").add(25).add(3));

        for entry in entries {
            tx.insert_rec(entry, &table, SetFlag::UPSERT)?;
        }
        db.db.commit(tx)?;

        let query = "SELECT * FROM mytable LIMIT 2;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());

        let rows = res.as_ref().unwrap().get_rows().unwrap();
        assert_eq!(rows.len(), 2);

        assert_eq!(&rows[0][0], "Alice");
        assert_eq!(&rows[0][1], "20");
        assert_eq!(&rows[0][2], "1");

        assert_eq!(&rows[1][0], "Bob");
        assert_eq!(&rows[1][1], "15");
        assert_eq!(&rows[1][2], "2");

        println!("{}", res.unwrap());

        let query = "SELECT * FROM mytable WHERE age >= 20;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());

        let rows = res.as_ref().unwrap().get_rows().unwrap();
        assert_eq!(rows.len(), 2);

        assert_eq!(&rows[0][0], "Alice");
        assert_eq!(&rows[0][1], "20");
        assert_eq!(&rows[0][2], "1");

        assert_eq!(&rows[1][0], "Charlie");
        assert_eq!(&rows[1][1], "25");
        assert_eq!(&rows[1][2], "3");

        println!("{}", res.unwrap());

        let query = "SELECT age FROM mytable WHERE age = 20, id = 1;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());

        let rows = res.as_ref().unwrap().get_rows().unwrap();
        assert_eq!(rows.len(), 1);

        assert_eq!(&rows[0][0], "Alice");
        assert_eq!(&rows[0][1], "20");
        assert_eq!(&rows[0][2], "1");

        println!("{}", res.unwrap());

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn select_exec_negative1() -> Result<()> {
        let path = "test-files/exec_select_stmt2.rdb";
        cleanup_file(path);
        let db = Database::new(path);
        let mut tx = db.db.begin(&db.db, TXKind::Write);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .add_col("id", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        let mut entries = vec![];
        entries.push(Record::new().add("Alice").add(20).add(1));
        entries.push(Record::new().add("Bob").add(15).add(2));
        entries.push(Record::new().add("Charlie").add(25).add(3));

        for entry in entries {
            tx.insert_rec(entry, &table, SetFlag::UPSERT)?;
        }
        db.db.commit(tx)?;

        // negative cases
        let query = "SELECT age FROM mytable WHERE id = 9999;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        assert_eq!(res.unwrap().len(), 0);

        let query = "SELECT asdfgsd FROM mytable WHERE id = 3;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_err());

        let query = "SELECT col FROM non_table;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_err());

        cleanup_file(path);
        Ok(())
    }
}
