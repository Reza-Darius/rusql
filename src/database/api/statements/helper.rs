use std::collections::HashMap;
use std::slice;
use tracing::debug;
use tracing::error;

use crate::database::btree::{BTree, Compare, ScanIter, Scanner};
use crate::database::codec::Bound;
use crate::database::errors::*;
use crate::database::pager::Pager;
use crate::database::tables::tables::Table;
use crate::database::tables::tables::TableIndex;
use crate::database::tables::{Query, Record};
use crate::database::transactions::tx::*;
use crate::interpreter::*;

pub(crate) fn filter_where(
    tx: &mut TX,
    table: &Table,
    indices: &[StatementIndex],
) -> Result<impl Iterator<Item = Record>> {
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

        return Ok(records.into_iter());
    }
    // query the database without index
    let scan = tx.full_table_scan(table)?;

    // filter results against WHERE clauses
    let records: Vec<_> = scan
        .map(Record::from_kv)
        .filter(|rec| filter_record(rec, &where_col_map))
        .collect();

    Ok(records.into_iter())
}

/// finds an index for the provided column map, used in WHERE clauses
///
/// returns the first matching index found, does not support multi key indices as of yet
fn find_index<'a, 'b>(
    table: &'a Table,
    col_map: &HashMap<usize, &'b StatementIndex>,
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
fn filter_record(record: &Record, col_map: &HashMap<usize, &StatementIndex>) -> bool {
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

/// scans the database for a given index, should only be called for secondary indices
fn scan_db<'a, P: Pager>(
    table: &Table,
    table_idx: &TableIndex,
    stmt_idx: &StatementIndex,
    tree: &'a BTree<P>,
) -> Result<ScanIter<'a, P>> {
    let key = Query::by_index(table, table_idx)
        .add(stmt_idx.expr.clone())
        .encode()?;

    // key to stop at the last key inside an index
    let idx_upper_bound_key =
        Query::by_tid_prefix(table, table_idx.prefix).with_bound(Bound::Positive);

    debug!(key=%key, "scanning with key");

    let scan = match stmt_idx.operator {
        Operator::Assign | Operator::Equal => Scanner::prefix(key, tree),
        Operator::Lt => Scanner::range(
            (key.clone(), Compare::Lt),
            (key.with_bound(Bound::Negative), Compare::Ge),
            tree,
        )?,
        Operator::Le => Scanner::range(
            (key.clone(), Compare::Le),
            (key.with_bound(Bound::Positive), Compare::Gt),
            tree,
        )?,

        Operator::Gt => Scanner::range(
            (key.with_bound(Bound::Positive), Compare::Gt),
            (idx_upper_bound_key, Compare::Gt),
            tree,
        )?,
        Operator::Ge => Scanner::range(
            (key.with_bound(Bound::Negative), Compare::Ge),
            (idx_upper_bound_key, Compare::Gt),
            tree,
        )?,

        _ => unreachable!("invalid operator were already filtered out"),
    };

    Ok(scan)
}
