use super::super::response::*;
use crate::database::api::statements::helper::filter_where;
use crate::database::btree::Scanner;
use crate::database::errors::*;
use crate::database::tables::tables::Table;
use crate::database::tables::{Query, Record};
use crate::database::transactions::tx::*;
use crate::database::types::IteratorDB;
use crate::interpreter::*;

use tracing::{debug, error, info, instrument};

// SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = "hello" LIMIT -5 + 7;
#[instrument(skip_all)]
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

    Ok(SelectResponse::from_records(res.as_slice(), &stmt.columns, &table).into())
}

/// evaluates select statement with WHERE clause
pub(crate) fn select_where(
    tx: &mut TX,
    table: &Table,
    stmt: &SelectStatement,
) -> Result<Vec<FilteredRecord>> {
    let indices = stmt.index.as_ref().ok_or_else(|| {
        error!("select_where() called without WHERE clause");
        ExecError::ExecutionError("select_where() called without WHERE clause")
    })?;

    let records = filter_where(tx, table, indices)?;

    let res = match &stmt.columns {
        StatementColumns::Wildcard => records.map(|rec| rec.into()).limit(stmt).collect(),
        StatementColumns::Cols(columns) => {
            let col_indices = validate_select_columns(columns.as_slice(), table)?;
            records
                .map(|rec| filter_columns(rec, col_indices.as_slice()))
                .limit(stmt)
                .collect()
        }
    };

    Ok(res)
}

/// ensures the provided columns exist and returns their corresponding indices
pub(crate) fn validate_select_columns<T: AsRef<str> + std::fmt::Debug>(
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

/// resolving select statement without where clause
pub(crate) fn select_columns(
    tx: &mut TX,
    table: &Table,
    stmt: &SelectStatement,
) -> Result<Vec<FilteredRecord>> {
    match &stmt.columns {
        StatementColumns::Cols(columns) => {
            // do the provided columns exist?
            let col_indices: Vec<usize> = validate_select_columns(columns.as_slice(), table)?;

            // do we have an index?
            if let Some(index) = table.get_index_for_columns(columns.as_slice()) {
                debug!(columns = ?columns, index = ?index, "index found for SELECT columns");

                let key = Query::by_tid_prefix(table, index.prefix);
                let res: Vec<FilteredRecord> = Scanner::prefix(key, &tx.tree)
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
pub(crate) fn filter_columns<'a>(record: Record, whitelist: &[usize]) -> FilteredRecord {
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
pub(crate) fn exec_insert(tx: &mut TX, stmt: InsertStatement) -> Result<()> {
    todo!()
}

#[cfg(test)]
pub(crate) mod execute_select {
    use crate::database::{
        api::api::Database,
        btree::SetFlag,
        helper::cleanup_file,
        pager::transaction::Transaction,
        tables::{TypeCol, tables::TableBuilder},
    };

    use super::*;
    use test_log::test;

    fn test_data_single_index1(path: &'static str) -> Result<Database> {
        cleanup_file(path);
        let db = Database::open(path);
        let mut tx = db.db.begin(&db.db, TXKind::Write);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::Bytes)
            .add_col("age", TypeCol::Integer)
            .add_col("id", TypeCol::Integer)
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
        let db = Database::open(path);
        let mut tx = db.db.begin(&db.db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("id", TypeCol::Integer)
            .add_col("name", TypeCol::Bytes)
            .add_col("age", TypeCol::Integer)
            .add_col("job", TypeCol::Bytes)
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
        assert_eq!(modified as usize, num_entries + 1);

        let modified = tx.create_index("age", "age", &mut table)?;
        assert_eq!(modified as usize, num_entries + 1);

        db.db.commit(tx)?;
        Ok(db)
    }

    #[test]
    fn select_exec_positive1() -> Result<()> {
        let path = "test-files/exec_select_stmt1.rdb";
        let db = test_data_single_index1(path)?;

        let query = "SELECT * FROM mytable LIMIT 2;";
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();
        assert_eq!(rows.len(), 2);

        assert_eq!(&rows[0][0], "Alice");
        assert_eq!(&rows[0][1], 20);
        assert_eq!(&rows[0][2], 1);

        assert_eq!(&rows[1][0], "Bob");
        assert_eq!(&rows[1][1], 15);
        assert_eq!(&rows[1][2], 2);

        println!("{query}\n{}", res);

        let query = "SELECT * FROM mytable WHERE age >= 20;";
        let res = db.execute(query.into())?;

        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 2);

        assert_eq!(&rows[0][0], "Alice");
        assert_eq!(&rows[0][1], 20);
        assert_eq!(&rows[0][2], 1);

        assert_eq!(&rows[1][0], "Charlie");
        assert_eq!(&rows[1][1], 25);
        assert_eq!(&rows[1][2], 3);

        println!("{query}\n{}", res);

        let query = "SELECT age FROM mytable WHERE age = 20, id = 1;";
        let res = db.execute(query.into())?;

        let rows = res.get_rows().unwrap();
        assert_eq!(rows.len(), 1);

        assert_eq!(&rows[0][0], 20);

        println!("{query}\n{}", res);

        let query = "SELECT name, age FROM mytable;";
        let res = db.execute(query.into())?;

        let rows = res.get_rows().unwrap();
        assert_eq!(rows.len(), 3);

        assert_eq!(&rows[0][0], "Alice");
        assert_eq!(&rows[0][1], 20);

        assert_eq!(&rows[1][0], "Bob");
        assert_eq!(&rows[1][1], 15);

        assert_eq!(&rows[2][0], "Charlie");
        assert_eq!(&rows[2][1], 25);

        println!("{query}\n{}", res);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn select_exec_positive2() -> Result<()> {
        let path = "test-files/exec_select_stmt2.rdb";
        let db = test_data_multiple_index1(path)?;

        let query = r#"SELECT * FROM mytable WHERE job = "clerk";"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0], 1);
        assert_eq!(&rows[0][1], "Alice");
        assert_eq!(&rows[0][2], 20);
        assert_eq!(&rows[0][3], "clerk");

        println!("{query}\n{}", res);

        let query = r#"SELECT * FROM mytable WHERE job >= "clerk";"#;
        let res = db.execute(query.into())?;

        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 4);

        println!("{query}\n{}", res);

        let query = r#"SELECT * FROM mytable WHERE job < "clerk";"#;
        let res = db.execute(query.into())?;

        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0], 5);
        assert_eq!(&rows[0][1], "Jane");
        assert_eq!(&rows[0][2], 25);
        assert_eq!(&rows[0][3], "artist");

        println!("{query}\n{}", res);

        let query = r#"SELECT * FROM mytable WHERE age >= 20, job = "clerk";"#;
        let res = db.execute(query.into())?;

        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0], 1);
        assert_eq!(&rows[0][1], "Alice");
        assert_eq!(&rows[0][2], 20);
        assert_eq!(&rows[0][3], "clerk");

        println!("{query}\n{}", res);

        let query = r#"SELECT * FROM mytable WHERE age > 15;"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 5);
        println!("{query}\n{}", res);

        let query = r#"SELECT * FROM mytable WHERE age < 20;"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 1);
        println!("{query}\n{}", res);

        let query = r#"SELECT * FROM mytable WHERE age > 20;"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 1);
        println!("{query}\n{}", res);

        let query = r#"SELECT * FROM mytable WHERE age >= 20;"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 4);
        println!("{query}\n{}", res);

        let query = r#"SELECT * FROM mytable WHERE age <= 20;"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 4);
        println!("{query}\n{}", res);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn select_exec_negative1() -> Result<()> {
        let path = "test-files/exec_select_stmt2.rdb";
        let db = test_data_single_index1(path)?;

        let query = "SELECT age FROM mytable WHERE id = 9999;";
        let res = db.execute(query.into())?;
        assert_eq!(res.get_rows().unwrap().len(), 0);

        let query = "SELECT asdfgsd FROM mytable WHERE id = 3;";
        let res = db.execute(query.into());
        assert!(res.is_err());

        let query = "SELECT * FROM mytable WHERE doesnt_exist = 3;";
        let res = db.execute(query.into());
        assert!(res.is_err());

        let query = "SELECT col FROM non_table;";
        let res = db.execute(query.into());
        assert!(res.is_err());

        cleanup_file(path);
        Ok(())
    }
}
