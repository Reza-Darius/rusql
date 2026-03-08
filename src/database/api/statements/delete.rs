use crate::{
    database::{
        api::{response::DBResponse, statements::helper::filter_where},
        errors::*,
        tables::{Record, tables::Table},
        transactions::tx::TX,
    },
    interpreter::DeleteStatement,
};

use tracing::{debug, info, instrument};

#[instrument(skip_all, err)]
pub fn exec_delete(tx: &mut TX, stmt: DeleteStatement) -> Result<DBResponse> {
    info!(?stmt, "executing delete statement");

    let table = tx
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecError::ExecutionError("table not found"))?;

    // processing statement
    let mut res = DBResponse::default();

    // do we have a WHERE clause?
    if stmt.index.is_some() {
        res.modified = delete_where(tx, &table, &stmt)?;
    } else {
        res.modified = delete_all(tx, &table, &stmt)?;
    };

    Ok(res)
}

fn delete_where(tx: &mut TX, table: &Table, stmt: &DeleteStatement) -> Result<u32> {
    let indices = stmt
        .index
        .as_ref()
        .ok_or_else(|| ExecError::ExecutionError("deleter_where() called without WHERE clause"))?;

    let records: Vec<_> = filter_where(tx, table, indices)?.collect();

    return delete_records(tx, table, records);
}

fn delete_all(tx: &mut TX, table: &Table, stmt: &DeleteStatement) -> Result<u32> {
    debug!("deleting all records from table");
    let records = tx.full_table_scan(table)?.into_iter().collect_records();

    delete_records(tx, table, records)
}

fn delete_records(tx: &mut TX, table: &Table, records: Vec<Record>) -> Result<u32> {
    let mut modified = 0;

    for rec in records {
        debug!(%rec, "deleting record:");
        modified += tx.delete_rec(rec, table)?;
    }

    debug!("{modified} rows modified");
    Ok(modified)
}

#[cfg(test)]
mod execute_delete {
    use crate::{
        database::{
            api::api::Database,
            btree::SetFlag,
            helper::cleanup_file,
            pager::transaction::Transaction,
            tables::{TypeCol, tables::TableBuilder},
            transactions::tx::TXKind,
        },
        interpreter::Parser,
    };

    use super::*;
    use test_log::test;

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
    fn delete_exec_positive() -> Result<()> {
        let path = "test-files/delete_exec_pos.rdb";
        let db = test_data_multiple_index1(path)?;

        let query = r#"DELETE FROM mytable WHERE name = "Alice";"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 3);

        let query = r#"SELECT * FROM mytable WHERE name = "Alice";"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt)?;
        let rows = res[0].select_result.as_ref().unwrap().get_rows();

        assert_eq!(rows.len(), 0);

        let query = r#"SELECT * FROM mytable;"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt)?;
        let rows = res[0].select_result.as_ref().unwrap().get_rows();

        assert_eq!(rows.len(), 4);

        // delete all rows
        let query = r#"DELETE FROM mytable;"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 12);

        let query = r#"SELECT * FROM mytable;"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt)?;
        let rows = res[0].select_result.as_ref().unwrap().get_rows();

        assert_eq!(rows.len(), 0);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn delete_exec_negative() -> Result<()> {
        let path = "test-files/delete_exec_neg.rdb";
        let db = test_data_multiple_index1(path)?;

        // non existant table
        let query = r#"DELETE FROM non_existant_table WHERE name = "Alice";"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt);
        assert!(res.is_err());

        // non existant column
        let query = r#"DELETE FROM mytable WHERE doesnt_exist = "Alice";"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt);
        assert!(res.is_err());

        // wrong data type for WHERE clause
        let query = r#"DELETE FROM mytable WHERE name = 9999;"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt);
        assert!(res.is_err());

        cleanup_file(path);
        Ok(())
    }
}
