use std::sync::Arc;

use crate::database::api::response::DBResponse;
use crate::database::api::statements::*;
use crate::database::errors::{ExecError, Result};
use crate::database::pager::transaction::{CommitStatus, Transaction};
use crate::database::transactions::kvdb::*;
use crate::database::transactions::tx::*;
use crate::interpreter::*;

pub struct Database {
    pub(crate) db: Arc<StorageEngine>,
}

impl Database {
    pub fn open(path: &'static str) -> Self {
        Database {
            db: Arc::new(StorageEngine::new(path)),
        }
    }

    pub fn new_tx(&self, kind: TXKind) -> TX {
        self.db.begin(&self.db, kind)
    }

    pub fn commit_tx(&self, tx: TX) -> Result<CommitStatus> {
        self.db.commit(tx)
    }

    pub fn abort_tx(&self, tx: TX) -> Result<CommitStatus> {
        self.db.abort(tx)
    }
}

impl Database {
    pub fn execute(&self, statements: impl Iterator<Item = Statement>) -> Result<Vec<DBResponse>> {
        let mut results = vec![];

        for statement in statements {
            // TODO: set up worker
            let mut tx = if let Statement::Select(_) = &statement {
                self.new_tx(TXKind::Read)
            } else {
                self.new_tx(TXKind::Write)
            };

            let res = match statement {
                Statement::Select(select_statement) => exec_select(&mut tx, select_statement),
                Statement::Insert(insert_statement) => exec_insert(&mut tx, insert_statement),
                Statement::Update(update_statement) => exec_update(&mut tx, update_statement),
                Statement::Delete(delete_statement) => todo!(),
                Statement::Create(create_statement) => todo!(),
                Statement::Drop(drop_statement) => todo!(),
            };

            match res {
                Ok(r) => {
                    let com_res = self.commit_tx(tx)?;
                    results.push(r);
                }
                Err(e) => {
                    self.abort_tx(tx)?;
                    return Err(e.into());
                }
            }
        }

        if results.is_empty() {
            return Err(ExecError::ExecutionError("no statements provided").into());
        }

        Ok(results)
    }
}
