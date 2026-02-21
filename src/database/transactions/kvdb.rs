use std::sync::Arc;
use tracing::instrument;

use crate::database::pager::DiskPager;
use crate::database::pager::transaction::CommitStatus;
use crate::database::pager::transaction::Transaction;
use crate::database::transactions::tx::{TX, TXKind};
use crate::database::{errors::Result, tables::tables::*};
/*
 * |--------------KEY---------------|----Value-----|
 * |                  [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PREFIX][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
*/

// central shared struct
pub(crate) struct KVDB {
    pub pager: Arc<DiskPager>,
    pub t_def: TDefTable,
    pub t_meta: MetaTable,
}

// pass through functions
impl Transaction for KVDB {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX {
        self.pager.begin(db, kind)
    }

    fn abort(&self, tx: TX) -> Result<CommitStatus> {
        self.pager.abort(tx)
    }

    #[instrument(skip_all)]
    fn commit(&self, tx: TX) -> Result<CommitStatus> {
        self.pager.commit(tx)
    }
}

impl KVDB {
    pub fn new(path: &'static str) -> Self {
        KVDB {
            t_def: TDefTable::new(),
            t_meta: MetaTable::new(),
            // t_buf: Mutex::new(TableBuffer::new()),
            pager: DiskPager::open(path).expect("DB initialize panic"),
        }
    }
}
