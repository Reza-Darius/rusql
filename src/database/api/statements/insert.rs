use crate::{
    database::{
        api::response::DBResponse, btree::SetFlag, errors::*, tables::Record, transactions::tx::TX,
    },
    interpreter::{InsertStatement, StatementColumns},
};

use tracing::{error, info, instrument};

// INSERT INTO table (col1, col2) VALUES (2*2), "Hello"
#[instrument(skip_all)]
pub fn exec_insert(tx: &mut TX, stmt: InsertStatement) -> Result<DBResponse> {
    info!(?stmt, "executing insert statement");

    let table = tx.get_table(&stmt.table_name).ok_or_else(|| {
        error!(table = stmt.table_name, "table not found");
        ExecError::ExecutionError("table not found")
    })?;

    // validate columns and data
    if let StatementColumns::Cols(columns) = &stmt.columns {
        if columns.len() != table.cols.len() {
            error!("not enough columns provided for table");
            return Err(ExecError::ExecutionError("not enough columns provided for table").into());
        }

        if columns.len() != stmt.values.len() {
            error!("not enough values provided");
            return Err(ExecError::ExecutionError("not enough values or columns provided").into());
        }
    }

    let mut rec = Record::new();
    for value in stmt.values {
        rec = rec.add(value);
    }

    let modified = tx.insert_rec(rec, &table, SetFlag::INSERT)?;

    Ok(DBResponse {
        select_result: None,
        modified,
        ..Default::default()
    })
}

#[cfg(test)]
mod execute_insert {
    use crate::{
        database::{
            api::api::Database,
            helper::cleanup_file,
            pager::transaction::Transaction,
            tables::{TypeCol, tables::TableBuilder},
            transactions::tx::TXKind,
        },
        interpreter::Parser,
    };

    use super::*;
    use test_log::test;

    fn test_db_single_index(path: &'static str) -> Result<Database> {
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

        db.db.commit(tx)?;
        Ok(db)
    }

    fn test_db_multi_index(path: &'static str) -> Result<Database> {
        cleanup_file(path);
        let db = Database::open(path);
        let mut tx = db.db.begin(&db.db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::Bytes)
            .add_col("age", TypeCol::Integer)
            .add_col("id", TypeCol::Integer)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;
        tx.create_index("age", "age", &mut table)?;

        db.db.commit(tx)?;
        Ok(db)
    }

    #[test]
    fn insert_exec_positive1() -> Result<()> {
        let path = "test-files/insert_exec_positive1.rdb";
        let db = test_db_single_index(path)?;

        let query = r#"
            INSERT INTO mytable (name, age, id) VALUES "Alice", 10 + 10, 1;
            INSERT INTO mytable (name, age, id) VALUES "Bob", 15, 2;
            INSERT INTO mytable (name, age, id) VALUES "Char" + "lie", 25, 7 - 4;
            SELECT * FROM mytable;
        "#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;

        assert_eq!(res.len(), 4);

        assert_eq!(res[0].modified, 1);
        assert_eq!(res[1].modified, 1);
        assert_eq!(res[2].modified, 1);

        let rows = res[3].get_rows().unwrap();

        assert_eq!(&rows[0][0], "Alice");
        assert_eq!(&rows[0][1], "20");
        assert_eq!(&rows[0][2], "1");

        assert_eq!(&rows[1][0], "Bob");
        assert_eq!(&rows[1][1], "15");
        assert_eq!(&rows[1][2], "2");

        assert_eq!(&rows[2][0], "Charlie");
        assert_eq!(&rows[2][1], "25");
        assert_eq!(&rows[2][2], "3");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn insert_exec_negative1() -> Result<()> {
        let path = "test-files/insert_exec_negative1.rdb";
        let db = test_db_single_index(path)?;

        // invalid table
        let query = r#"INSERT INTO table_doesnt_exist (name, age, id) VALUES "Alice", 10 + 10, 1;"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt);
        assert!(res.is_err());

        // wrong data type
        let query = r#"INSERT INTO mytable (name, age, id) VALUES "Alice", "20", 1;"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt);
        assert!(res.is_err());

        // duplicate column names
        let query = r#"INSERT INTO mytable (name, name, id) VALUES "Alice", 20, 1;"#;
        let stmt = Parser::parse(query);
        assert!(stmt.is_err());

        // missing columns
        let query = r#"INSERT INTO mytable (name, id) VALUES "Alice", 20, 1;"#;
        let stmt = Parser::parse(query);
        assert!(stmt.is_err());

        // missing values
        let query = r#"INSERT INTO mytable (name, name, id) VALUES 20, 1;"#;
        let stmt = Parser::parse(query);
        assert!(stmt.is_err());

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn insert_exec_positive2() -> Result<()> {
        let path = "test-files/insert_exec_positive2.rdb";
        let db = test_db_multi_index(path)?;

        let query = r#"INSERT INTO mytable (name, age, id) VALUES "Alice", 10 + 10, 1;"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 2);

        let query = r#"INSERT INTO mytable (name, age, id) VALUES "Bob", 15, 2;"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 2);

        let query = r#"INSERT INTO mytable (name, age, id) VALUES "Char" + "lie", 25, 7 - 4;"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 2);

        let query = r#"SELECT * FROM mytable;"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        let rows = res[0].get_rows().unwrap();
        assert_eq!(rows.len(), 3);

        let query = r#"SELECT age FROM mytable;"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        let rows = res[0].get_rows().unwrap();
        assert_eq!(rows.len(), 3);

        cleanup_file(path);
        Ok(())
    }
}
