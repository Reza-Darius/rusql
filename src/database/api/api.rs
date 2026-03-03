use std::collections::HashMap;
use std::slice;
use std::sync::Arc;

use tracing::{debug, error, info};

use crate::database::api::response::{DBResponse, FilteredRecord};
use crate::database::btree::{Compare, ScanMode};
use crate::database::errors::{ExecError, Result};
use crate::database::pager::transaction::{CommitStatus, Transaction};
use crate::database::tables::tables::{Table, TableIndex};
use crate::database::tables::{Query, Record};
use crate::database::transactions::kvdb::*;
use crate::database::transactions::tx::*;
use crate::database::types::IteratorDB;
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

pub fn exec_select(tx: &mut TX, stmt: SelectStatement) -> Result<DBResponse> {
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

    Ok(DBResponse::from_records(
        res.as_slice(),
        &stmt.columns,
        &table,
    ))
}

/// evaluates select statement with WHERE clause
fn select_where(tx: &mut TX, table: &Table, stmt: &SelectStatement) -> Result<Vec<FilteredRecord>> {
    let indices = stmt.index.as_ref().ok_or_else(|| {
        error!("select_where() called without WHERE clause");
        ExecError::ExecutionError("select_where() called without WHERE clause")
    })?;

    // mapping column indices to WHERE clauses
    let where_col_map = validate_where_clause(table, &indices[..])?;

    let select_col_indices = match &stmt.columns {
        StatementColumns::Wildcard => None,
        StatementColumns::Cols(columns) => {
            Some(validate_select_columns(columns.as_slice(), table)?)
        }
    };

    // do we have an index for the WHERE columns?
    if let Some((table_idx, stmt_idx)) = find_index(table, &where_col_map) {
        debug!(?table_idx, ?stmt_idx, "index for WHERE clause");

        // query the database
        let scan = get_scan(table, table_idx, stmt_idx)?;

        // filter results against non-indexed WHERE clauses
        let iter = scan
            .into_iter(&tx.tree)
            .filter_map(|(k, v)| Record::decode_with_index(k, v, table_idx, table).ok()) // reorder into primary row layout
            .filter(|rec| filter_record(rec, &where_col_map));

        // filter against possible select columns
        let res: Vec<FilteredRecord> = match select_col_indices {
            Some(col_indices) => iter
                .map(|rec| filter_columns(rec, col_indices.as_slice()))
                .limit(stmt)
                .collect(),
            None => iter.map(|rec| rec.into()).limit(stmt).collect(),
        };

        return Ok(res);
    }
    // query the database without index
    let scan = tx.full_table_scan(table)?;

    // filter results against WHERE clauses
    let iter = scan
        .into_iter()
        .map(Record::from_kv)
        .filter(|rec| filter_record(rec, &where_col_map));

    // filter against possible select columns
    let res: Vec<FilteredRecord> = match select_col_indices {
        Some(col_indices) => iter
            .map(|rec| filter_columns(rec, col_indices.as_slice()))
            .limit(stmt)
            .collect(),
        None => iter.map(|rec| rec.into()).limit(stmt).collect(),
    };

    Ok(res)
}

// check columns and data types
//
/// validates WHERE clauses for appropiate data types
///
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

/// ensures the provided columns exist and returns their corresponding indices
fn validate_select_columns<T: AsRef<str> + std::fmt::Debug>(
    columns: &[T],
    table: &Table,
) -> Result<Vec<usize>> {
    // do the provided columns exist?
    let col_indices: Vec<usize> = columns
        .iter()
        .filter_map(|col| table.get_col_idx(col.as_ref()))
        .collect();
    if col_indices.len() != columns.len() {
        error!(?columns, "couldnt find all columns in table schema");
        return Err(ExecError::ExecutionError("couldnt find all columns in table schema").into());
    }
    Ok(col_indices)
}

fn get_scan(table: &Table, table_idx: &TableIndex, stmt_idx: &StatementIndex) -> Result<ScanMode> {
    let key = Query::by_index(table, table_idx)
        .add(stmt_idx.expr.clone())
        .encode()?;

    debug!(key=%key, "scanning with key");

    let scan = match stmt_idx.operator {
        Operator::Assign | Operator::Equal => {
            ScanMode::range((key.clone(), Compare::Ge), (key.clone(), Compare::Gt))?
        }
        Operator::Lt => ScanMode::range((key.clone(), Compare::Lt), (key.clone(), Compare::Ge))?,
        Operator::Le => ScanMode::range((key.clone(), Compare::Le), (key.clone(), Compare::Gt))?,

        Operator::Gt => ScanMode::range((key.clone(), Compare::Gt), (key.clone(), Compare::Le))?,
        Operator::Ge => ScanMode::range((key.clone(), Compare::Ge), (key.clone(), Compare::Lt))?,
        // Operator::Assign | Operator::Equal => ScanMode::open(key, Compare::Ge)?,
        // Operator::Lt => ScanMode::open(key, Compare::Lt)?,
        // Operator::Le => ScanMode::open(key, Compare::Le)?,
        // Operator::Gt => ScanMode::open(key, Compare::Gt)?,
        // Operator::Ge => ScanMode::open(key, Compare::Ge)?,
        _ => unreachable!("invalid operator were already filtered out"),
    };

    Ok(scan)
}

/// finds an index for the provided column map, used in WHERE clauses
///
/// returns the first matching index found, does not support multi key indices as of yet
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

/// filters records based on WHERE clause predicates
///
/// a record needs to be in the primary row layout
fn filter_record<'a>(record: &'a Record, col_map: &HashMap<usize, &StatementIndex>) -> bool {
    for (col, index) in col_map {
        // converting to comparable types without reallocting
        let data = record[*col].as_ref();
        let idx_expr = (&index.expr).into();

        if !match index.operator {
            Operator::Assign => data == idx_expr,
            Operator::Equal => data == idx_expr,
            Operator::Lt => data < idx_expr,
            Operator::Le => data <= idx_expr,
            Operator::Gt => data > idx_expr,
            Operator::Ge => data >= idx_expr,
            _ => unreachable!("invalid operator are already filtered out"),
        } {
            return false;
        };
    }
    true
}

// fn trim_record(record: &Record, cols: StatementColumns) -> Option<

/// resolving select statement without where clause
fn select_columns(
    tx: &mut TX,
    table: &Table,
    stmt: &SelectStatement,
) -> Result<Vec<FilteredRecord>> {
    match &stmt.columns {
        StatementColumns::Cols(columns) => {
            // do the provided columns exist?
            let col_indices: Vec<usize> = validate_select_columns(columns.as_slice(), table)?;

            // do we have an index?
            if let Some(index) = table.get_index(columns.as_slice()) {
                debug!(columns = ?columns, index = ?index, "index found for SELECT columns");

                let key = Query::by_tid_prefix(table, index.prefix);
                let res: Vec<FilteredRecord> = ScanMode::prefix(key, &tx.tree, Compare::Eq)?
                    .filter_map(|(k, v)| Record::decode_with_index(k, v, index, table).ok()) // reorder into primary row layout
                    .map(|record| filter_columns(record, col_indices.as_slice()))
                    .limit(stmt)
                    .collect();

                debug!(?res, "filtered records");
                return Ok(res);
            }

            // fall back to full table scan
            debug!(?columns, ?col_indices, "full table scan");
            let res = tx
                .full_table_scan(table)?
                .map(Record::from_kv)
                .map(|record| filter_columns(record, col_indices.as_slice()))
                .limit(stmt)
                .collect();

            Ok(res)
        }
        StatementColumns::Wildcard => {
            debug!("full table scan wildcard");
            let res = tx
                .full_table_scan(table)?
                .map(Record::from_kv)
                .map(|record| record.into())
                .limit(stmt)
                .collect();

            Ok(res)
        }
    }
}

/// creates a new record by whitelisting the columns provided in the slice, the caller has to ensure the proper order
///
/// if an empty slice is provided, it does a one to one conversion without altering the record
fn filter_columns<'a>(record: Record, whitelist: &[usize]) -> FilteredRecord {
    if whitelist.is_empty() {
        return FilteredRecord::from(record);
    };
    debug!("filtering {:?} for whitelist {:?}", record, whitelist);

    let mut filtered_rec = vec![];
    let mut rec = record.into_vec();
    let mut removed = 0; // offset when indexing into the rec after removing elements

    for idx in whitelist {
        let cell = rec.remove(*idx - removed);
        filtered_rec.push(cell);
        removed += 1;
    }

    FilteredRecord::from(filtered_rec)
}

// INSERT INTO table (col1, col2) VALUES (2*2), "Hello";
fn exec_insert(tx: &mut TX, stmt: InsertStatement) -> Result<()> {
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

    fn test_data_single_index1(path: &'static str) -> Result<Database> {
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
        Ok(db)
    }

    fn test_data_multiple_index1(path: &'static str) -> Result<Database> {
        cleanup_file(path);
        let db = Database::new(path);
        let mut tx = db.db.begin(&db.db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .add_col("job", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        let mut entries = vec![];
        entries.push(Record::new().add(1).add("Alice").add(20).add("clerk"));
        entries.push(Record::new().add(2).add("Bob").add(20).add("student"));
        entries.push(
            Record::new()
                .add(3)
                .add("Charlie")
                .add(20)
                .add("firefighter"),
        );
        entries.push(Record::new().add(4).add("Rob").add(18).add("programmer"));
        entries.push(Record::new().add(5).add("Jane").add(25).add("artist"));

        let num_entries = entries.len();
        for entry in entries {
            tx.insert_rec(entry, &table, SetFlag::UPSERT)?;
        }

        let modified = tx.create_index("job", "job", &mut table)?;
        assert_eq!(modified as usize, num_entries);

        let modified = tx.create_index("age", "age", &mut table)?;
        assert_eq!(modified as usize, num_entries);

        db.db.commit(tx)?;
        Ok(db)
    }

    #[test]
    fn select_exec_positive1() -> Result<()> {
        let path = "test-files/exec_select_stmt1.rdb";
        let db = test_data_single_index1(path)?;

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

        println!("{query}\n{}", res.unwrap());

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

        println!("{query}\n{}", res.unwrap());

        let query = "SELECT age FROM mytable WHERE age = 20, id = 1;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());

        let rows = res.as_ref().unwrap().get_rows().unwrap();
        assert_eq!(rows.len(), 1);

        assert_eq!(&rows[0][0], "20");

        println!("{query}\n{}", res.unwrap());

        let query = "SELECT name, age FROM mytable;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());

        let rows = res.as_ref().unwrap().get_rows().unwrap();
        assert_eq!(rows.len(), 3);

        assert_eq!(&rows[0][0], "Alice");
        assert_eq!(&rows[0][1], "20");

        assert_eq!(&rows[1][0], "Bob");
        assert_eq!(&rows[1][1], "15");

        assert_eq!(&rows[2][0], "Charlie");
        assert_eq!(&rows[2][1], "25");

        println!("{query}\n{}", res.unwrap());

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn select_exec_positive2() -> Result<()> {
        let path = "test-files/exec_select_stmt2.rdb";
        let db = test_data_multiple_index1(path)?;

        let query = r#"SELECT * FROM mytable WHERE job = "clerk";"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        let rows = res.as_ref().unwrap().get_rows().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0], "1");
        assert_eq!(&rows[0][1], "Alice");
        assert_eq!(&rows[0][2], "20");
        assert_eq!(&rows[0][3], "clerk");

        println!("{query}\n{}", res.unwrap());

        let query = r#"SELECT * FROM mytable WHERE age >= 20, job = "clerk";"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        let rows = res.as_ref().unwrap().get_rows().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0], "1");
        assert_eq!(&rows[0][1], "Alice");
        assert_eq!(&rows[0][2], "20");
        assert_eq!(&rows[0][3], "clerk");

        println!("{query}\n{}", res.unwrap());

        let query = r#"SELECT * FROM mytable WHERE age > 15;"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        let rows = res.as_ref().unwrap().get_rows().unwrap();

        assert_eq!(rows.len(), 5);

        let query = r#"SELECT * FROM mytable WHERE age < 20;"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        let rows = res.as_ref().unwrap().get_rows().unwrap();

        assert_eq!(rows.len(), 1);

        let query = r#"SELECT * FROM mytable WHERE age > 20;"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        let rows = res.as_ref().unwrap().get_rows().unwrap();

        assert_eq!(rows.len(), 1);

        let query = r#"SELECT * FROM mytable WHERE age >= 20;"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        let rows = res.as_ref().unwrap().get_rows().unwrap();

        assert_eq!(rows.len(), 4);

        let query = r#"SELECT * FROM mytable WHERE age <= 20;"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        let rows = res.as_ref().unwrap().get_rows().unwrap();

        assert_eq!(rows.len(), 4);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn select_exec_negative1() -> Result<()> {
        let path = "test-files/exec_select_stmt2.rdb";
        let db = test_data_single_index1(path)?;

        let query = "SELECT age FROM mytable WHERE id = 9999;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_ok());
        assert_eq!(res.unwrap().len(), 0);

        let query = "SELECT asdfgsd FROM mytable WHERE id = 3;";
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0));
        assert!(res.is_err());

        let query = "SELECT * FROM mytable WHERE doesnt_exist = 3;";
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
