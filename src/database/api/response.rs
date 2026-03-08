use std::fmt;

use crate::{
    database::{
        tables::{Record, tables::Table},
        types::DataCell,
    },
    interpreter::StatementColumns,
};

#[derive(Debug, Default)]
pub struct DBResponse {
    pub(crate) query: String,
    pub(crate) select_result: Option<SelectResponse>,
    pub(crate) modified: u32,
}

impl DBResponse {
    pub fn get_rows(&self) -> Option<&[Vec<String>]> {
        self.select_result.as_ref().map(|r| r.get_rows())
    }
}

impl fmt::Display for DBResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.select_result {
            Some(res) => write!(f, "{res}"),
            None => write!(f, "No Result"),
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

#[derive(Debug, Default)]
pub struct SelectResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl SelectResponse {
    pub fn get_rows(&self) -> &[Vec<String>] {
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
            .map(|rec| rec.iter().map(|cell| cell.to_string()).collect())
            .collect();

        SelectResponse { columns, rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn order_by(&mut self, col_idx: usize) {
        if col_idx > self.columns.len() {
            return;
        }
        let len = self.rows.len();
        qs(self.rows.as_mut_slice(), col_idx, 0, len - 1);
        todo!()
    }
}

fn qs(arr: &mut [Vec<String>], col_idx: usize, lo: usize, hi: usize) {
    if lo < hi {
        let pivot_idx = partition(arr, col_idx, lo, hi);
        qs(arr, col_idx, lo, pivot_idx - 1);
        qs(arr, col_idx, pivot_idx + 1, hi);
    }
}

fn partition(arr: &mut [Vec<String>], col_idx: usize, lo: usize, hi: usize) -> usize {
    let mut idx: i32 = lo as i32 - 1;
    for i in lo..hi - 1 {
        if arr[i][col_idx] < arr[hi][col_idx] {
            idx += 1;
            arr.swap(idx as usize, i);
        }
    }
    let idx = idx as usize + 1;
    arr.swap(idx, hi);
    idx
}

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
                widths[i] = widths[i].max(cell.len());
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
