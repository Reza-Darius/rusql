use std::fmt;

use crate::{
    database::tables::{Record, tables::Table},
    interpreter::Statement,
};

#[derive(Debug, Default)]
pub struct DBResponse {
    query_result: Option<QueryResponse>,
}

impl DBResponse {
    pub fn get_rows(&self) -> Option<&[Vec<String>]> {
        if let Some(q) = &self.query_result {
            Some(q.rows.as_slice())
        } else {
            None
        }
    }
}

impl fmt::Display for DBResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.query_result {
            Some(res) => write!(f, "{res}"),
            None => write!(f, "No Result"),
        }
    }
}

#[derive(Debug, Default)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl fmt::Display for QueryResponse {
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

impl QueryResponse {
    fn render(&self) {}
}

impl DBResponse {
    pub fn new(stmt: &Statement, records: Option<&[Record]>) -> Self {
        todo!()
    }

    pub fn from_records(table: &Table, records: &[Record]) -> Self {
        let query_result = Some(QueryResponse {
            columns: table.cols.iter().map(|col| col.title.clone()).collect(),
            rows: records
                .iter()
                .map(|rec| rec.iter().map(|cell| cell.to_string()).collect())
                .collect(),
        });
        DBResponse { query_result }
    }

    pub fn len(&self) -> usize {
        match &self.query_result {
            Some(q) => q.rows.len(),
            None => 0,
        }
    }

    fn rows(self) -> String {
        todo!()
    }
}
