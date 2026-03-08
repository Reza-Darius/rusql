use std::sync::Arc;

use tracing::{debug, info, instrument};

use crate::{
    database::{
        api::response::DBResponse,
        errors::{ExecError, Result},
        tables::tables::{IdxKind, TableBuilder},
        transactions::tx::TX,
    },
    interpreter::{
        CreateIndexStatement, CreateStatement, CreateTableStatement, DropIndexStatement,
        DropStatement, DropTableStatement,
    },
};

#[instrument(skip_all, err)]
pub fn exec_drop(tx: &mut TX, stmt: DropStatement) -> Result<DBResponse> {
    let mut res = DBResponse::default();

    match stmt {
        DropStatement::Table(stmt) => res.modified = drop_table(tx, stmt)?,
        DropStatement::Index(stmt) => res.modified = drop_index(tx, stmt)?,
    };

    Ok(res)
}

fn drop_table(tx: &mut TX, stmt: DropTableStatement) -> Result<u32> {
    tx.drop_table(&stmt.table_name)
}

fn drop_index(tx: &mut TX, stmt: DropIndexStatement) -> Result<u32> {
    let table = tx
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecError::ExecutionError("table not found"))?;

    let mut table = Arc::unwrap_or_clone(table);
    let modified = tx.delete_index(&stmt.idx_name, &mut table)?;

    assert!(table.idx_exists(&stmt.idx_name).is_none());

    Ok(modified)
}

#[cfg(test)]
mod execute_drop {
    use crate::{
        database::{
            api::api::Database,
            btree::SetFlag,
            helper::cleanup_file,
            pager::transaction::Transaction,
            tables::{Record, TypeCol, tables::TableBuilder},
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
    fn drop_table_exec_pos() -> Result<()> {
        let path = "test-files/drop_table_pos.rdb";
        cleanup_file(path);
        let db = Database::open(path);

        let query = r#"CREATE TABLE new_table (
            col1 = INT,
            col2 = STR,
        );"#;

        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 1);

        let query = r#"
            INSERT INTO new_table (col1, col2) VALUES 1, "Alice";
            INSERT INTO new_table (col1, col2) VALUES 2, "Bob";
            INSERT INTO new_table (col1, col2) VALUES 3, "Charlie";
        "#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 1);
        assert_eq!(res[1].modified, 1);
        assert_eq!(res[2].modified, 1);

        let query = r#"SELECT * FROM new_table;"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt)?;
        let rows = res[0].select_result.as_ref().unwrap().get_rows();

        assert_eq!(rows.len(), 3);

        let query = r#"SELECT * FROM new_table WHERE col2 = "Alice";"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt)?;
        let rows = res[0].select_result.as_ref().unwrap().get_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0], "1");
        assert_eq!(&rows[0][1], "Alice");

        let query = r#"DROP TABLE new_table;"#;

        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 4);

        let query = r#"SELECT * FROM new_table;"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt);
        assert!(res.is_err());

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn drop_table_exec_neg() -> Result<()> {
        let path = "test-files/drop_table_exec_neg.rdb";
        cleanup_file(path);
        let db = Database::open(path);

        // no columns
        let query = r#"DROP TABLE doesnt_exist;"#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt);
        assert!(res.is_err());

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn drop_index_exec_pos() -> Result<()> {
        let path = "test-files/create_index_exec_pos.rdb";
        let db = test_data_multiple_index1(path)?;

        let query = r#"
                    CREATE INDEX my_index ON mytable FOR name;
                "#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 6);

        let query = r#"SELECT * FROM mytable WHERE name >= "Alice";"#;
        let stmt = Parser::parse(query)?;

        let res = db.execute(stmt)?;
        let rows = res[0].select_result.as_ref().unwrap().get_rows();

        assert_eq!(rows.len(), 5);

        let query = r#"
                    DROP INDEX my_index FROM mytable;
                "#;

        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 6);

        let query = r#"
                    DROP INDEX age FROM mytable;
                "#;

        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 6);

        let query = r#"
                    DROP INDEX job FROM mytable;
                "#;

        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt)?;
        assert_eq!(res[0].modified, 6);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn drop_index_exec_neg() -> Result<()> {
        let path = "test-files/create_index_exec_neg.rdb";
        let db = test_data_multiple_index1(path)?;

        let query = r#"
                    DROP INDEX doesnt_exist FROM mytable;
                "#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt);
        assert!(res.is_err());

        let query = r#"
                    DROP INDEX job FROM doesnt_exist;
                "#;
        let stmt = Parser::parse(query)?;
        let res = db.execute(stmt);
        assert!(res.is_err());

        cleanup_file(path);
        Ok(())
    }
}
