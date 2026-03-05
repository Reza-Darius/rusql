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
    pub(crate) query_result: Option<SelectResponse>,
    pub(crate) modified: u32,
}

impl fmt::Display for DBResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.query_result {
            Some(res) => write!(f, "{res}"),
            None => write!(f, "No Result"),
        }
    }
}

impl From<SelectResponse> for DBResponse {
    fn from(value: SelectResponse) -> Self {
        Self {
            query_result: Some(value),
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
