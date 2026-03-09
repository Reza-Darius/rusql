use std::{fmt, ops::Index};

use tracing::{debug, instrument};

use crate::{
    database::{
        tables::{Record, tables::Table},
        types::DataCell,
    },
    interpreter::StatementColumns,
};

#[derive(Debug, Default)]
pub struct DBResponse {
    pub(crate) select_result: Option<SelectResponse>,
    pub(crate) modified: u32,
}

impl DBResponse {
    /// retrieves the fetched rows
    pub fn get_rows(&self) -> Option<&[Row]> {
        self.select_result.as_ref().map(|r| r.get_rows())
    }

    /// retrieves the amount of modified rows
    pub fn modified(&self) -> u32 {
        self.modified
    }
}

impl fmt::Display for DBResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(select_result) = &self.select_result {
            write!(f, "{select_result}")
        } else {
            write!(f, "rows modified: {}", self.modified)
        }
    }
}

impl From<SelectResponse> for DBResponse {
    fn from(value: SelectResponse) -> Self {
        Self {
            select_result: Some(value),
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub struct Row(Vec<DataCell>);

impl Row {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> std::slice::Iter<'_, DataCell> {
        self.0.iter()
    }
}

impl IntoIterator for Row {
    type Item = DataCell;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Index<usize> for Row {
    type Output = DataCell;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl FromIterator<DataCell> for Row {
    fn from_iter<T: IntoIterator<Item = DataCell>>(iter: T) -> Self {
        Row(iter.into_iter().collect())
    }
}
#[derive(Debug, Default)]
pub struct SelectResponse {
    columns: Vec<String>,
    rows: Vec<Row>,
}

impl SelectResponse {
    pub fn get_rows(&self) -> &[Row] {
        self.rows.as_slice()
    }

    pub fn from_records(
        records: &[FilteredRecord],
        columns: &StatementColumns,
        table: &Table,
    ) -> Self {
        let columns = match columns {
            StatementColumns::Wildcard => table.cols.iter().map(|col| col.title.clone()).collect(),
            StatementColumns::Cols(cols) => cols.iter().map(|col| col.clone()).collect(),
        };
        let rows = records
            .iter()
            .map(|rec| rec.iter().cloned().collect())
            .collect();

        SelectResponse { columns, rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn order_by(&mut self, col_idx: usize) {
        if col_idx >= self.columns.len() {
            return;
        }
        let len = self.rows.len();
        qs(self.rows.as_mut_slice(), col_idx);
    }
}

fn qs(rows: &mut [Row], col_idx: usize) {
    if rows.len() < 2 {
        return;
    }
    let pivot_idx = partition(rows, col_idx);

    qs(&mut rows[..pivot_idx], col_idx);
    qs(&mut rows[pivot_idx + 1..], col_idx);
}

fn partition(rows: &mut [Row], col_idx: usize) -> usize {
    let hi = rows.len() - 1;
    let mut idx = 0;
    for i in 0..hi {
        if rows[i][col_idx] <= rows[hi][col_idx] {
            rows.swap(idx as usize, i);
            idx += 1;
        }
    }
    rows.swap(idx, hi);
    idx
}

// i let ChatGPT create this for me
impl fmt::Display for SelectResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.columns.is_empty() {
            return writeln!(f, "(no columns)");
        }

        let col_count = self.columns.len();

        // Validate row widths
        for row in &self.rows {
            if row.len() != col_count {
                return writeln!(f, "(invalid row width)");
            }
        }

        // Compute column widths
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();

        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.char_len());
            }
        }

        // Helper to draw a separator line
        let draw_separator = |f: &mut fmt::Formatter<'_>| -> fmt::Result {
            write!(f, "+")?;
            for width in &widths {
                write!(f, "-{}-+", "-".repeat(*width))?;
            }
            writeln!(f)
        };

        // Top border
        draw_separator(f)?;

        // Header row
        write!(f, "|")?;
        for (col, width) in self.columns.iter().zip(&widths) {
            write!(f, " {:width$} |", col, width = *width)?;
        }
        writeln!(f)?;

        // Header separator
        draw_separator(f)?;

        // Data rows
        for row in &self.rows {
            write!(f, "|")?;
            for (cell, width) in row.iter().zip(&widths) {
                write!(f, " {:width$} |", cell, width = *width)?;
            }
            writeln!(f)?;
        }

        // Bottom border
        draw_separator(f)
    }
}

impl DBResponse {}

#[derive(Debug)]
pub struct FilteredRecord(Vec<DataCell>);

impl FilteredRecord {
    fn iter(&self) -> impl Iterator<Item = &DataCell> {
        self.0.iter()
    }
}

impl From<Vec<DataCell>> for FilteredRecord {
    fn from(value: Vec<DataCell>) -> Self {
        FilteredRecord(value)
    }
}

impl From<Record> for FilteredRecord {
    fn from(value: Record) -> Self {
        FilteredRecord(value.into_vec())
    }
}

#[cfg(test)]
mod response_test {
    use super::*;
    use test_log::test;

    #[test]
    fn sort_test() {
        let rows = vec![
            Row(vec![5.into(), 6.into(), 10.into(), "Charlie".into()]),
            Row(vec![1.into(), 5.into(), 1.into(), "Bob".into()]),
            Row(vec![2.into(), 8.into(), 3.into(), "Alice".into()]),
            Row(vec![3.into(), 6.into(), 3.into(), "Zac".into()]),
            Row(vec![4.into(), 9.into(), 3.into(), "Ethan".into()]),
        ];

        let mut resp = SelectResponse {
            columns: vec![
                "row 1".to_string(),
                "row 2".to_string(),
                "row 3".to_string(),
                "row 4".to_string(),
            ],
            rows,
        };

        resp.order_by(0);

        assert_eq!(resp.rows[0][0], 1);
        assert_eq!(resp.rows[1][0], 2);
        assert_eq!(resp.rows[2][0], 3);
        assert_eq!(resp.rows[3][0], 4);
        assert_eq!(resp.rows[4][0], 5);

        resp.order_by(1);

        assert_eq!(resp.rows[0][1], 5);
        assert_eq!(resp.rows[1][1], 6);
        assert_eq!(resp.rows[2][1], 6);
        assert_eq!(resp.rows[3][1], 8);
        assert_eq!(resp.rows[4][1], 9);

        resp.order_by(2);

        assert_eq!(resp.rows[0][2], 1);
        assert_eq!(resp.rows[1][2], 3);
        assert_eq!(resp.rows[2][2], 3);
        assert_eq!(resp.rows[3][2], 3);
        assert_eq!(resp.rows[4][2], 10);

        resp.order_by(3);

        assert_eq!(resp.rows[0][3], "Alice");
        assert_eq!(resp.rows[1][3], "Bob");
        assert_eq!(resp.rows[2][3], "Charlie");
        assert_eq!(resp.rows[3][3], "Ethan");
        assert_eq!(resp.rows[4][3], "Zac");
    }
}
