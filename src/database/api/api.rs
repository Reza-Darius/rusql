use std::sync::Arc;

use crate::database::btree::SetFlag;
use crate::database::errors::Result;
use crate::database::transactions::kvdb::KVDB;
use crate::interpreter::{StatementColumns, ValueObject};

// outward API
trait DatabaseAPI {
    fn create_table(&self, name: &str) -> Result<()>;
    fn drop_table(&self, name: &str) -> Result<()>;

    fn create_idx(&self, table_name: &str, cols: StatementColumns) -> Result<()>;
    fn drop_idx(&self, table_name: &str, cols: StatementColumns) -> Result<()>;

    fn insert(
        &self,
        table: &str,
        cols: StatementColumns,
        values: &[ValueObject],
        flag: SetFlag,
    ) -> Result<()>;

    fn search(&self, table: &str, cols: StatementColumns, limit: u32) -> Result<()>;
    // fn update(&self, table: &str, cols: StatementColumns, values: &[ValueObject]) -> Result<()>;
    fn delete(&self, table: &str, cols: StatementColumns, values: &[ValueObject]) -> Result<()>;
}

struct Database {
    db: Arc<KVDB>,
}

impl Database {
    fn new(path: &'static str) -> Self {
        Database {
            db: Arc::new(KVDB::new(path)),
        }
    }

    fn select() {
        // TX begin
        // IDX strategy
        // TX commit
    }
}

impl DatabaseAPI for Database {
    fn create_table(&self, name: &str) -> Result<()> {
        todo!()
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        todo!()
    }

    fn create_idx(&self, table_name: &str, cols: StatementColumns) -> Result<()> {
        todo!()
    }

    fn drop_idx(&self, table_name: &str, cols: StatementColumns) -> Result<()> {
        todo!()
    }

    fn insert(
        &self,
        table: &str,
        cols: StatementColumns,
        values: &[ValueObject],
        flag: SetFlag,
    ) -> Result<()> {
        todo!()
    }

    fn search(&self, table: &str, cols: StatementColumns, limit: u32) -> Result<()> {
        todo!()
    }

    fn delete(&self, table: &str, cols: StatementColumns, values: &[ValueObject]) -> Result<()> {
        todo!()
    }
}
