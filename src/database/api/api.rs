use std::sync::Arc;

use crate::database::api::response::DBResponse;
use crate::database::api::statements::*;
use crate::database::errors::{ExecError, Result};
use crate::database::pager::transaction::{CommitStatus, Transaction};
use crate::database::transactions::kvdb::*;
use crate::database::transactions::tx::*;
use crate::database::types::{DataCell, InputData};
use crate::interpreter::*;

pub struct Database {
    pub(crate) db: Arc<StorageEngine>,
}

impl Database {
    pub fn open(path: &str) -> Self {
        Database {
            db: Arc::new(StorageEngine::new(path)),
        }
    }

    fn new_tx(&self, kind: TXKind) -> TX {
        self.db.begin(&self.db, kind)
    }

    fn commit_tx(&self, tx: TX) -> Result<CommitStatus> {
        self.db.commit(tx)
    }

    fn abort_tx(&self, tx: TX) -> Result<CommitStatus> {
        self.db.abort(tx)
    }
}

impl Database {
    pub fn execute(&self, query: Query) -> Result<DBResponse> {
        let statements = query.parse()?;
        let mut response = DBResponse::default();
        for statement in statements {
            let mut tx = if let ParsedStatement::Select(_) = &statement {
                self.new_tx(TXKind::Read)
            } else {
                self.new_tx(TXKind::Write)
            };

            let res = match statement {
                ParsedStatement::Select(select_statement) => exec_select(&mut tx, select_statement),
                ParsedStatement::Insert(insert_statement) => exec_insert(&mut tx, insert_statement),
                ParsedStatement::Update(update_statement) => exec_update(&mut tx, update_statement),
                ParsedStatement::Delete(delete_statement) => exec_delete(&mut tx, delete_statement),
                ParsedStatement::Create(create_statement) => exec_create(&mut tx, create_statement),
                ParsedStatement::Drop(drop_statement) => exec_drop(&mut tx, drop_statement),
            };

            match res {
                Ok(r) => {
                    let com_res = self.commit_tx(tx)?;

                    response.select_result = r.select_result;
                    response.modified += r.modified;
                }
                Err(e) => {
                    self.abort_tx(tx)?;
                    return Err(e.into());
                }
            }
        }

        Ok(response)
    }
}

pub struct Statement;

impl Statement {
    pub fn new(query: &str) -> Query {
        Query {
            statement: query.to_string(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Default)]
pub struct Query {
    statement: String,
    data: Vec<DataCell>,
}

impl Query {
    pub fn bind(&mut self, data: impl InputData) -> &mut Self {
        self.data.push(data.into_cell());
        self
    }

    fn parse(self) -> Result<impl Iterator<Item = ParsedStatement>> {
        if self.data.is_empty() {
            let res = Parser::parse(&self.statement)?;
            return Ok(res);
        }

        // replace ? for datacells
        let mut parsed_statement = self.statement;
        for cell in self.data {
            let data = match cell {
                DataCell::Str(s) => format!("\"{s}\""),
                DataCell::Int(i) => format!("{i}"),
            };
            parsed_statement = parsed_statement.replacen('?', data.as_str(), 1);
        }

        let res = Parser::parse(&parsed_statement)?;
        Ok(res)
    }
}

impl<T: ToString> From<T> for Query {
    fn from(value: T) -> Self {
        Query {
            statement: value.to_string(),
            ..Default::default()
        }
    }
}
