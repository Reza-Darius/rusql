use std::collections::HashMap;

use crate::{
    database::{
        api::{response::DBResponse, select::*},
        btree::SetFlag,
        errors::*,
        tables::{
            Record,
            tables::{IdxKind, Table},
        },
        transactions::tx::TX,
    },
    interpreter::{StatementSet, UpdateStatement},
};

use tracing::{debug, error, info, instrument};

#[instrument(skip_all, err)]
pub fn exec_update(tx: &mut TX, stmt: UpdateStatement) -> Result<DBResponse> {
    info!(?stmt, "executing update statement");

    let table = tx
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecError::ExecutionError("table not found"))?;

    let col_map = validate_update_stmt(&table, &stmt)?;

    // processing statement
    let mut res = DBResponse::default();

    // do we have a WHERE clause?
    if stmt.index.is_some() {
        res.modified = update_where(tx, &table, &stmt, &col_map)?;
    } else {
        res.modified = update_all(tx, &table, &col_map)?;
    };

    Ok(res)
}

// validates columns and maps the provided statements to column indices
fn validate_update_stmt<'a>(
    table: &Table,
    stmt: &'a UpdateStatement,
) -> Result<HashMap<usize, &'a StatementSet>> {
    if stmt.set.len() > table.cols.len() {
        return Err(ExecError::ExecutionError("column count doesn't match table schema").into());
    }

    let col_map: HashMap<usize, &StatementSet> = stmt
        .set
        .iter()
        .filter(|set| table.validate_col_data(&set.column, &set.expr))
        .filter_map(|set| table.get_col_idx(&set.column).map(|idx| (idx, set)))
        .collect();

    if col_map.len() != stmt.set.len() {
        return Err(ExecError::ExecutionError("invalid data provided").into());
    }

    Ok(col_map)
}

fn update_where(
    tx: &mut TX,
    table: &Table,
    stmt: &UpdateStatement,
    stmt_col_map: &HashMap<usize, &StatementSet>,
) -> Result<u32> {
    let indices = stmt
        .index
        .as_ref()
        .ok_or_else(|| ExecError::ExecutionError("select_where() called without WHERE clause"))?;

    // mapping column indices to WHERE clauses
    let where_col_map = validate_where_clause(table, &indices[..])?;

    // do we have an index for the WHERE columns?
    if let Some((table_idx, stmt_idx)) = find_index(table, &where_col_map) {
        debug!(?table_idx, ?stmt_idx, "index for WHERE clause");

        let scan = scan_db(table, table_idx, stmt_idx, &tx.tree)?;

        // filter results against non-indexed WHERE clauses
        let records: Vec<_> = scan
            .filter_map(|(k, v)| Record::decode_with_index(k, v, table_idx, table).ok()) // reorder into primary row layout
            .filter(|rec| filter_record(rec, &where_col_map))
            .collect();

        return write_records(tx, table, records, stmt_col_map);
    }
    // query the database without index
    let scan = tx.full_table_scan(table)?;

    // filter results against WHERE clauses
    let records: Vec<_> = scan
        .map(Record::from_kv)
        .filter(|rec| filter_record(rec, &where_col_map))
        .collect();

    return write_records(tx, table, records, stmt_col_map);
}

fn update_all(tx: &mut TX, table: &Table, col_map: &HashMap<usize, &StatementSet>) -> Result<u32> {
    let records = tx.full_table_scan(table)?.collect_records();

    write_records(tx, table, records, col_map)
}

fn write_records(
    tx: &mut TX,
    table: &Table,
    records: Vec<Record>,
    col_map: &HashMap<usize, &StatementSet>,
) -> Result<u32> {
    if !check_unique(table, records.as_slice(), col_map) {
        return Err(
            ExecError::ExecutionError("failed to update: PKEY uniqueness violation").into(),
        );
    }

    let mut modified = 0;

    for mut rec in records {
        debug!(%rec, "updating record:");

        // update with new values
        for (idx, set) in col_map {
            rec.insert(*idx, set.expr.clone());
        }

        debug!(%rec, "inserting record:");
        modified += tx.insert_rec(rec, table, SetFlag::UPDATE)?;
    }

    debug!("{modified} rows modified");
    Ok(modified)
}

fn check_unique(
    table: &Table,
    records: &[Record],
    stmt_col_map: &HashMap<usize, &StatementSet>,
) -> bool {
    assert!(!records.is_empty());

    // changing a single record is fine
    if records.len() == 1 {
        return true;
    }

    // get indicies of columns used by primary key
    let mut pkey_cols = table
        .indices
        .iter()
        .filter(|index| index.kind == IdxKind::Primary)
        .map(|p_idx| &p_idx.columns)
        .flatten();

    // check against indices from provided statement
    for (idx, _) in stmt_col_map.iter() {
        if pkey_cols.any(|col| *col == *idx) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod execute_update {
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
        assert_eq!(modified as usize, num_entries);

        let modified = tx.create_index("age", "age", &mut table)?;
        assert_eq!(modified as usize, num_entries);

        db.db.commit(tx)?;
        Ok(db)
    }

    #[test]
    fn update_exec_positive() -> Result<()> {
        let path = "test-files/upadate_exec_pos.rdb";
        let db = test_data_multiple_index1(path)?;

        let query = r#"UPDATE mytable SET job = "manager" WHERE name = "Alice";"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0))?;
        assert_eq!(res.modified, 3);

        let query = r#"SELECT * FROM mytable WHERE name = "Alice";"#;
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0))?;
        let rows = res.select_result.as_ref().unwrap().get_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0], "1");
        assert_eq!(&rows[0][1], "Alice");
        assert_eq!(&rows[0][2], "20");
        assert_eq!(&rows[0][3], "manager");

        // update all columns
        let query = r#"UPDATE mytable SET job = "manager";"#;
        let mut stmt = Parser::parse(query)?;
        let res = db.execute(stmt.remove(0))?;
        assert_eq!(res.modified, 12);

        let query = r#"SELECT * FROM mytable;"#;
        let mut stmt = Parser::parse(query)?;

        let res = db.execute(stmt.remove(0))?;
        let rows = res.select_result.as_ref().unwrap().get_rows();

        assert_eq!(rows.len(), 5);
        assert_eq!(&rows[0][3], "manager");
        assert_eq!(&rows[1][3], "manager");
        assert_eq!(&rows[2][3], "manager");
        assert_eq!(&rows[3][3], "manager");
        assert_eq!(&rows[4][3], "manager");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn update_exec_negative() -> Result<()> {
        let path = "test-files/upadate_exec_neg.rdb";
        let db = test_data_multiple_index1(path);

        cleanup_file(path);
        Ok(())
    }
}
