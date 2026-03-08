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
