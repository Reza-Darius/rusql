use std::sync::Arc;

use tracing::{info, instrument};

use crate::{
    database::{
        api::response::DBResponse,
        errors::{ExecError, Result},
        tables::tables::{IdxKind, TableBuilder},
        transactions::tx::TX,
        types::MAX_COLUMNS,
    },
    interpreter::{CreateIndexStatement, CreateStatement, CreateTableStatement},
};

#[instrument(skip_all, err)]
pub fn exec_create(tx: &mut TX, stmt: CreateStatement) -> Result<DBResponse> {
    let mut res = DBResponse::default();

    match stmt {
        CreateStatement::Table(create_table_statement) => {
            res.modified = create_table(tx, create_table_statement)?
        }
        CreateStatement::Index(create_index_statement) => {
            res.modified = create_index(tx, create_index_statement)?
        }
    };
    Ok(res)
}

fn create_table(tx: &mut TX, stmt: CreateTableStatement) -> Result<u32> {
    info!("executing create table statment");

    if tx.get_table(&stmt.table_name).is_some() {
        return Err(ExecError::ExecutionError("table already exists").into());
    }

    if stmt.columns.len() > MAX_COLUMNS as usize {
        return Err(ExecError::ExecutionError("maximum column size exceede").into());
    }

    // TODO: support multiple primary keys
    let mut table = TableBuilder::new().pkey(1).name(&stmt.table_name);

    for column in stmt.columns.iter() {
        table = table.add_col(&column.col_name, column.data_type.into());
    }

    let built_table = table.build(tx)?;

    assert_eq!(built_table.cols.len(), stmt.columns.len());
    assert_eq!(built_table.indices.len(), 1);
    assert_eq!(built_table.indices[0].kind, IdxKind::Primary);

    tx.insert_table(&built_table)?;

    Ok(1) // we only add a single entry to the tdef table
}

fn create_index(tx: &mut TX, stmt: CreateIndexStatement) -> Result<u32> {
    info!("executing create index statment");

    let table = tx
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecError::ExecutionError("table not found"))?;

    if table.idx_exists(&stmt.idx_name).is_some() {
        return Err(ExecError::ExecutionError("index already exists").into());
    }

    // is the requested column already indexed?
    if table
        .get_index_for_columns(std::slice::from_ref(&stmt.col_name))
        .is_some()
    {
        return Err(ExecError::ExecutionError("index already exists for column").into());
    }

    let mut table = Arc::unwrap_or_clone(table);
    let modified = tx.create_index(&stmt.idx_name, &stmt.col_name, &mut table)?;

    assert!(table.idx_exists(&stmt.idx_name).is_some());
    assert!(
        table
            .get_index_for_columns(std::slice::from_ref(&stmt.col_name))
            .is_some()
    );

    Ok(modified)
}

#[cfg(test)]
mod execute_create {
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
    fn create_table_exec_pos() -> Result<()> {
        let path = "test-files/create_table_pos.rdb";
        cleanup_file(path);
        let db = Database::open(path);

        let query = r#"CREATE TABLE new_table (
            col1 = INT,
            col2 = STR,
        );"#;

        let res = db.execute(query.into())?;
        assert_eq!(res.modified(), 1);

        let query = r#"
            INSERT INTO new_table (col1, col2) VALUES 1, "Alice";
            INSERT INTO new_table (col1, col2) VALUES 2, "Bob";
            INSERT INTO new_table (col1, col2) VALUES 3, "Charlie";
        "#;
        let res = db.execute(query.into())?;
        assert_eq!(res.modified(), 3);

        let query = r#"SELECT * FROM new_table;"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 3);

        let query = r#"SELECT * FROM new_table WHERE col2 = "Alice";"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0], 1);
        assert_eq!(&rows[0][1], "Alice");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn create_table_exec_neg() -> Result<()> {
        let path = "test-files/create_table_exec_neg.rdb";
        cleanup_file(path);
        let db = Database::open(path);

        // no columns
        let query = r#"CREATE TABLE new_table ();"#;
        let res = Parser::parse(query);
        assert!(res.is_err());

        // duplicate columns
        let query = r#"CREATE TABLE new_table (col1 = INT, col1 = STR);"#;
        let res = Parser::parse(query);
        assert!(res.is_err());

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn create_index_exec_pos() -> Result<()> {
        let path = "test-files/create_index_exec_pos.rdb";
        let db = test_data_multiple_index1(path)?;

        let query = r#"
                    CREATE INDEX my_index ON mytable FOR name;
                "#;
        let res = db.execute(query.into())?;
        assert_eq!(res.modified(), 6);

        let query = r#"SELECT * FROM mytable WHERE name >= "Alice";"#;
        let res = db.execute(query.into())?;
        let rows = res.get_rows().unwrap();

        assert_eq!(rows.len(), 5);
        cleanup_file(path);
        Ok(())
    }
}
