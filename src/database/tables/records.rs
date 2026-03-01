use std::collections::HashMap;
use std::fmt::Write;
use std::ops::Index;

use tracing::{debug, error};
use tracing_subscriber::registry::Data;

use crate::database::codec::*;
use crate::database::tables::keyvalues::DataCellRef;
use crate::database::tables::tables::{IdxKind, TableIndex};
use crate::database::tables::{Key, Value};
use crate::database::types::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE, DataCell, InputData};
use crate::database::{
    errors::{Result, TableError},
    tables::tables::{Table, TypeCol},
};
use crate::debug_if_env;

/// Record object used to insert data
#[derive(Debug)]
pub(crate) struct Record {
    data: Vec<DataCell>,
}

impl Record {
    pub fn new() -> Self {
        Record { data: vec![] }
    }

    /// add a datacell to the record
    ///
    /// sensitive to order in which input is added
    pub fn add<T: InputData>(mut self, data: T) -> Self {
        self.data.push(data.into_cell());
        self
    }

    /// encodes a record into the necessary key value pairs to fulfill all indices of a given table
    pub fn encode(self, table: &Table) -> Result<impl Iterator<Item = (Key, Value)>> {
        debug!(data=?self.data, "encoding");
        if table.cols.len() != self.data.len() {
            error!(?table, "input doesnt match column count");
            return Err(
                TableError::RecordError("input doesnt match column count".to_string()).into(),
            );
        }

        self.validate_data_types(table)?;

        // primary key
        let mut pkey_cells: Vec<&DataCell> = vec![];
        // secondary key
        let mut skey_cells: Vec<&DataCell>;
        // everything else inside the value field
        let mut val_cells: Vec<&DataCell>;
        let mut res = vec![];

        for (i, idx) in table.indices.iter().enumerate() {
            let n_cols = idx.columns.len(); // number of columns for an index

            match idx.kind {
                IdxKind::Primary => {
                    if i != 0 {
                        // first index has to be primary key
                        return Err(TableError::RecordError(format!(
                            "expected index 0 found {i} for primary keys"
                        ))
                        .into());
                    }

                    // constructing Key
                    pkey_cells = idx.columns.iter().map(|i| &self.data[*i]).collect();

                    // constructing Value
                    val_cells = (n_cols..self.data.len()).map(|i| &self.data[i]).collect();

                    debug_if_env!("RUSQL_LOG_RECORDS", {
                        debug!(?pkey_cells, ?val_cells);
                    });

                    // chaining together
                    let data_iter = pkey_cells
                        .iter()
                        .map(|c| *c)
                        .chain(val_cells.iter().map(|c| *c));

                    assert_eq!(pkey_cells.len(), table.pkeys as usize);
                    let kv = encode_to_kv(table.id, idx.prefix, data_iter, Some(n_cols))?;
                    assert!(!kv.0.as_slice().len() > TID_LEN + PREFIX_LEN);

                    res.push(kv);
                    val_cells.clear();
                }
                IdxKind::Secondary => {
                    // constructing Key
                    skey_cells = idx.columns.iter().map(|i| &self.data[*i]).collect();

                    // constructing Value
                    val_cells = (pkey_cells.len()..self.data.len())
                        .filter_map(|i| {
                            if !idx.columns.contains(&i) {
                                Some(&self.data[i])
                            } else {
                                None
                            }
                        })
                        .collect();

                    debug_if_env!("RUSQL_LOG_RECORDS", {
                        debug!(?skey_cells, ?val_cells);
                    });

                    // chaining together
                    let data_iter = skey_cells.iter().map(|c| *c).chain(
                        pkey_cells
                            .iter()
                            .map(|c| *c)
                            .chain(val_cells.iter().map(|c| *c)),
                    );

                    let kv = encode_to_kv(
                        table.id,
                        idx.prefix,
                        data_iter,
                        Some(pkey_cells.len() + n_cols),
                    )?;
                    assert!(!kv.0.as_slice().len() > TID_LEN + PREFIX_LEN);

                    res.push(kv);
                    skey_cells.clear();
                    val_cells.clear();
                }
            };
        }
        assert_eq!(res.len(), table.indices.len());
        Ok(res.into_iter())
    }

    pub fn from_kv(kv: (Key, Value)) -> Record {
        let mut data = Vec::new();
        data.extend(kv.0.into_iter());
        data.extend(kv.1.into_iter());
        Record { data }
    }

    /// takes a key value pair acquired through the given index and transforms it back into the primary key record layout
    ///
    /// this function errors if the index isnt found in the table schema or if there is a mismatch between data types
    pub fn decode_with_index(
        key: Key,
        value: Value,
        index: &TableIndex,
        table: &Table,
    ) -> Result<Record> {
        // does the index exist?
        if !table.indices.contains(index) {
            error!("index doesnt exist for provided table");
            return Err(TableError::RecordDecodeError(
                "index doesnt exist for provided table".to_string(),
            )
            .into());
        }

        // if they records is the primary index we dont need to rearrange it
        if index.kind == IdxKind::Primary {
            return Ok(Record::from_kv((key, value)));
        }

        debug!("decoding {key}, {value}");

        let mut rec = Record { data: vec![] };
        let mut key_iter = key.into_iter();
        let mut value_iter = value.into_iter();

        // isolating secondary keys
        let mut sec_key = HashMap::new();
        for i in 0..index.columns.len() {
            sec_key.insert(
                index.columns[i],
                key_iter.next().ok_or_else(|| {
                    error!("couldnt isolate secondary key");
                    TableError::RecordDecodeError("couldnt isolate secondary key".to_string())
                })?,
            );
        }

        // adding primary keys
        for i in 0..table.pkeys {
            rec.data.push(key_iter.next().ok_or_else(|| {
                error!("couldnt add primary key from key iter");
                TableError::RecordDecodeError("couldnt add primary key from key iter".to_string())
            })?);
        }

        assert!(key_iter.next().is_none());

        // reconstructing record
        for i in table.pkeys as usize..table.cols.len() {
            // inserting secondary key cells into the right position
            if let Some(cell) = sec_key.remove(&i) {
                rec.data.push(cell);
            } else {
                rec.data.push(value_iter.next().ok_or_else(|| {
                    TableError::RecordDecodeError(
                        "couldnt add value from secondary index".to_string(),
                    )
                })?)
            }
        }
        rec.validate_data_types(table)?;

        debug!(?rec, "reordered record");
        Ok(rec)
    }

    fn validate_data_types(&self, table: &Table) -> Result<()> {
        for (i, cell) in self.data.iter().enumerate() {
            let cell_type = match cell {
                DataCell::Str(s) => TypeCol::BYTES,
                DataCell::Int(_) => TypeCol::INTEGER,
            };
            if table.cols[i].data_type != cell_type {
                error!(expected=?table.cols[i].data_type, got=?cell_type, "Record doesnt match column");
                return Err(
                    TableError::RecordError("Record doesnt match column".to_string()).into(),
                );
            }
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &DataCell> {
        self.data.iter()
    }

    pub fn into_iter(self) -> impl Iterator<Item = DataCell> {
        self.data.into_iter()
    }

    pub fn into_vec(self) -> Vec<DataCell> {
        self.data
    }
}

impl Index<usize> for Record {
    type Output = DataCell;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        for cell in self.data.iter() {
            match cell {
                DataCell::Str(str) => write!(s, "{} ", str)?,
                DataCell::Int(i) => write!(s, "{} ", i)?,
            };
        }
        write!(f, "{}", s.trim())?;
        Ok(())
    }
}

pub struct RecordRef<'a> {
    data: Vec<DataCellRef<'a>>,
}

impl<'a> RecordRef<'a> {
    pub fn from_kv(k: &'a Key, v: &'a Value) -> Self {
        let mut data: Vec<_> = vec![];
        data.extend(k.iter());
        data.extend(v.iter());

        RecordRef { data }
    }
}

/// Query object used to construct a key
#[derive(Debug, Clone, Copy)]
pub(crate) struct Query;

impl Query {
    /// constructs the key for direct row lookup, add keys with `.add()` then call `.encode()`
    pub fn by_col(schema: &Table) -> QueryCol<'_> {
        QueryCol {
            data: HashMap::new(),
            schema,
        }
    }

    /// constructs a key based on a index
    pub fn by_index<'a>(schema: &'a Table, index: &'a TableIndex) -> QueryIndex<'a> {
        QueryIndex {
            data: vec![],
            schema,
            index,
        }
    }

    /// constructs a key with only the TID
    pub fn by_tid(schema: &Table) -> Key {
        let mut buf = [0u8; TID_LEN];
        buf.write_u32(schema.id);
        Key::from_encoded_slice(&buf)
    }

    /// constructs a key with TID + Prefix
    pub fn by_tid_prefix(schema: &Table, prefix: u16) -> Key {
        let mut buf = [0u8; TID_LEN + PREFIX_LEN];
        buf.write_u32(schema.id).write_u16(prefix);
        Key::from_encoded_slice(&buf)
    }
}

pub(crate) struct QueryCol<'a> {
    data: HashMap<String, DataCell>,
    schema: &'a Table,
}

impl<'a> QueryCol<'a> {
    /// add the column and key which you want to query, can only be used on QueryKey
    ///
    /// not sensitive to order, but all keys for an index have to be provided and need to match the designated data type
    pub fn add(mut self, col: &str, value: impl InputData) -> Self {
        self.data.insert(col.to_string(), value.into_cell());
        self
    }

    // Version 2
    /// attempts to encode a Query into a key
    ///
    /// will error if keys are missing or the data types don't match!
    pub fn encode(self) -> Result<Key> {
        let schema = self.schema;
        let index = find_index(&self.data, self.schema)
            .ok_or_else(|| TableError::QueryError("Index couldnt be found".to_string()))?;

        // maintaining order
        let mut cells = vec![];
        for col_idx in index.columns.iter() {
            let col_name = &schema.cols[*col_idx].title;
            let cell_ref = self
                .data
                .get(col_name)
                .ok_or_else(|| TableError::QueryError("error when ordering columns".to_string()))?;

            cells.push(cell_ref);
        }

        let (k, v) = encode_to_kv(schema.id, index.prefix, cells.iter().map(|e| *e), None)?;

        assert_eq!(v.len(), 0);
        assert!(k.len() > 0);

        Ok(k)
    }
}

pub(crate) struct QueryIndex<'a> {
    data: Vec<DataCell>,
    schema: &'a Table,
    index: &'a TableIndex,
}

impl<'a> QueryIndex<'a> {
    /// add data to the query, sensitive to order
    pub fn add(mut self, value: impl InputData) -> Self {
        self.data.push(value.into_cell());
        self
    }

    /// validates data type and constructs a key
    pub fn encode(self) -> Result<Key> {
        let schema = self.schema;
        let index = self.index;
        let cells = self.data;

        // validating if index cols match the data type
        for (i, cell) in cells.iter().enumerate() {
            let col_idx = index.columns[i];
            if !schema.validate_col_data(&schema.cols[col_idx].title, cell) {
                return Err(
                    TableError::QueryError("cell doesnt match data type".to_string()).into(),
                );
            }
        }

        let (k, v) = encode_to_kv(schema.id, index.prefix, cells.iter(), None)?;

        assert_eq!(v.len(), 0);
        assert!(k.len() > 0);

        Ok(k)
    }
}

/// finds the matching index for the provided col/value pairs
///
/// validates data types
fn find_index<'b>(data: &HashMap<String, DataCell>, schema: &'b Table) -> Option<&'b TableIndex> {
    let len = data.len();
    if len == 0 {
        return None;
    }

    // mapping the data to column indices
    let col_idx: Vec<usize> = data
        .iter()
        .filter_map(|e| schema.get_col_idx(e.0))
        .collect();

    // columns dont match schema
    if col_idx.len() != len {
        error!(
            data=?data,
            schema_cols=?schema.cols,
            ?col_idx,
            "columns dont match provided query"
        );
        return None;
    }

    let mut idx = None;
    let mut count = 0;

    // finding matching index by col amount
    for e in schema.indices.iter() {
        if e.columns.len() == len && e.columns.iter().all(|e| col_idx.contains(e)) {
            idx = Some(e);
            count += 1;
        }
    }

    match count {
        n if n == 0 => {
            error!(data=?data, "no matching index found");
            return None;
        }
        n if n > 1 => {
            error!(data=?data, matches=count, "multiple matching indices found");
            return None;
        }
        _ => (),
    }

    // validating if index cols match the data type
    if !data.iter().all(|e| schema.validate_col_data(e.0, e.1)) {
        error!(data=?data, "data types dont match!");
        return None;
    };

    assert!(idx.is_some());
    idx
}

/// encodes datacells into key value pairs
///
/// `delim` marks the idx where keys and values get seperated, none puts everything into `Key` leaving `Value` empty
fn encode_to_kv<'a, I>(tid: u32, prefix: u16, data: I, delim: Option<usize>) -> Result<(Key, Value)>
where
    I: IntoIterator<Item = &'a DataCell>,
{
    let mut iter = data.into_iter().peekable();
    if iter.peek().is_none() {
        return Err(TableError::KeyEncodeError("no data provided".to_string()).into());
    }

    let mut buf = Vec::<u8>::new();
    let mut idx: usize = 0;
    let mut key_delim: usize = 0;
    let mut count = 0;

    // table id and prefix
    buf.extend_from_slice(&tid.to_le_bytes());
    buf.extend_from_slice(&prefix.to_le_bytes());
    idx += TID_LEN + PREFIX_LEN;

    // composing byte array by iterating through all data cells
    for (i, cell) in iter.enumerate() {
        if let Some(n) = delim {
            if n == 0 {
                return Err(
                    TableError::RecordError("delimiter cant be Some(0)".to_string()).into(),
                );
            } else if n == i {
                // mark the cutoff point between keys and values
                key_delim = idx;
            }
        }

        match cell {
            DataCell::Str(str) => {
                let str = str.encode();
                buf.extend_from_slice(&str);
                idx += str.len();
            }
            DataCell::Int(num) => {
                let num = num.encode();
                buf.extend_from_slice(&num);
                idx += num.len();
            }
        }
        count += 1;
    }

    if let Some(n) = delim {
        if n == count {
            // we only have primary keys
            key_delim = idx;
        }
    };

    if delim.is_none() {
        // empty value
        key_delim = idx;
    };

    let key_slice = &buf[..key_delim];
    let val_slice = &buf[key_delim..];

    if key_slice.len() > BTREE_MAX_KEY_SIZE {
        return Err(TableError::RecordError("maximum key size exceeded".to_string()).into());
    }
    if val_slice.len() > BTREE_MAX_VAL_SIZE {
        return Err(TableError::RecordError("maximum value size exceeded".to_string()).into());
    }

    assert!(!key_slice.is_empty());

    Ok((
        Key::from_encoded_slice(key_slice),
        Value::from_encoded_slice(val_slice),
    ))
}

#[cfg(test)]
mod test {
    use crate::database::pager::transaction::Transaction;
    use crate::database::transactions::{kvdb::StorageEngine, tx::TXKind};
    use std::sync::Arc;

    use super::super::tables::TableBuilder;
    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;

    #[test]
    fn record1() -> Result<()> {
        let path = "test-files/record1.rdb";
        cleanup_file(path);
        let db = Arc::new(StorageEngine::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(2)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build(&mut tx)?;

        let s1 = "hello";
        let i1 = 10;
        let s2 = "world";

        let rec = Record::new().add(s1).add(i1).add(s2);

        let kv = rec.encode(&table)?.next().unwrap();
        assert_eq!(kv.0.to_string(), "2 0 hello 10");
        assert_eq!(kv.1.to_string(), "world");
        let _ = db.commit(tx);
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn queryindex1() -> Result<()> {
        let path = "test-files/record1.rdb";
        cleanup_file(path);
        let db = Arc::new(StorageEngine::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(2)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build(&mut tx)?;

        let q = Query::by_index(&table, &table.indices[0])
            .add(DataCell::Str("Alice".to_string()))
            .add(DataCell::Int(1))
            .encode()?;

        assert_eq!(q.to_string(), "2 0 Alice 1");

        let _ = db.commit(tx);
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn records_test_str() -> Result<()> {
        let key = Key::from_unencoded_type("hello".to_string());
        assert_eq!(key.to_string(), "1 0 hello");

        let key: Key = "hello".into();
        assert_eq!(key.to_string(), "1 0 hello");

        let key = Key::from_unencoded_type("owned hello".to_string());
        assert_eq!(key.to_string(), "1 0 owned hello");

        let val: Value = "world".into();
        assert_eq!(val.to_string(), "world");
        Ok(())
    }

    #[test]
    fn key_cmp2() -> Result<()> {
        let k2: Key = "9".into();
        let k3: Key = "10".into();
        let k1: Key = "1".into();
        let k4: Key = "1".into();
        assert!(k3 < k2);
        assert!(k1 < k2);
        assert!(k1 < k3);
        assert!(k1 == k4);
        Ok(())
    }

    #[test]
    fn records_secondary_indicies1() -> Result<()> {
        let path = "test-files/records_secondary_indicies1.rdb";
        cleanup_file(path);
        let db = Arc::new(StorageEngine::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(1)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build(&mut tx)?;

        table.create_index("number")?;
        table.add_col_to_index("number", "number")?;
        assert_eq!(table.indices.len(), 2);

        let s1 = "hello";
        let i1 = 10;
        let s2 = "world";

        let mut rec = Record::new().add(s1).add(i1).add(s2).encode(&table)?;
        let mut kv = rec.next().unwrap();

        assert_eq!(kv.0.to_string(), "2 0 hello");
        assert_eq!(kv.1.to_string(), "10 world");

        kv = rec.next().unwrap();
        assert_eq!(kv.0.to_string(), "2 1 10 hello");
        assert_eq!(kv.1.to_string(), "world");

        let _ = db.commit(tx);
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn records_secondary_decode1() -> Result<()> {
        let path = "test-files/records_secondary_indicies2.rdb";
        cleanup_file(path);
        let db = Arc::new(StorageEngine::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(1)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build(&mut tx)?;

        table.create_index("number")?;
        table.add_col_to_index("number", "number")?;
        assert_eq!(table.indices.len(), 2);

        let s1 = "hello";
        let i1 = 10;
        let s2 = "world";

        let mut rec = Record::new().add(s1).add(i1).add(s2).encode(&table)?;
        let mut kv = rec.next().unwrap();

        assert_eq!(kv.0.to_string(), "2 0 hello");
        assert_eq!(kv.1.to_string(), "10 world");

        kv = rec.next().unwrap();
        assert_eq!(kv.0.to_string(), "2 1 10 hello");
        assert_eq!(kv.1.to_string(), "world");

        let decoded_rec = Record::decode_with_index(kv.0, kv.1, &table.indices[1], &table);

        assert_eq!(decoded_rec.unwrap().to_string(), "hello 10 world");

        let _ = db.commit(tx);
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn records_secondary_indicies2() -> Result<()> {
        let path = "test-files/records_secondary_indicies2.rdb";
        cleanup_file(path);
        let db = Arc::new(StorageEngine::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("mytable")
            .id(5)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("city", TypeCol::BYTES)
            .add_col("job", TypeCol::BYTES)
            .build(&mut tx)?;

        table.create_index("city")?;
        table.add_col_to_index("city", "city")?;
        assert_eq!(table.indices.len(), 2);

        let mut rec = Record::new()
            .add(1)
            .add("Alfred")
            .add("Berlin")
            .add("Firefighter")
            .encode(&table)?;
        let mut kv = rec.next().unwrap();

        assert_eq!(kv.0.to_string(), "5 0 1");
        assert_eq!(kv.1.to_string(), "Alfred Berlin Firefighter");

        kv = rec.next().unwrap();
        assert_eq!(kv.0.to_string(), "5 1 Berlin 1");
        assert_eq!(kv.1.to_string(), "Alfred Firefighter");

        let decoded_rec = Record::decode_with_index(kv.0, kv.1, &table.indices[1], &table);

        assert_eq!(
            decoded_rec.unwrap().to_string(),
            "1 Alfred Berlin Firefighter"
        );

        let _ = db.commit(tx);
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn query_secondary_indicies1() -> Result<()> {
        let path = "test-files/query_secondary_indicies1.rdb";
        cleanup_file(path);
        let db = Arc::new(StorageEngine::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("mytable")
            .id(5)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("city", TypeCol::BYTES)
            .add_col("job", TypeCol::BYTES)
            .build(&mut tx)?;

        table.create_index("city")?;
        table.add_col_to_index("city", "city")?;
        assert_eq!(table.indices.len(), 2);

        // primary index
        let q = Query::by_col(&table).add("id", 1).encode();
        assert!(q.is_ok());
        assert_eq!(q.unwrap().to_string(), "5 0 1");

        // secondary index
        let q = Query::by_col(&table).add("city", "New York").encode();
        assert!(q.is_ok());
        assert_eq!(q.unwrap().to_string(), "5 1 New York");

        // non existant index
        let q = Query::by_col(&table).add("name", "nonexistant").encode();
        assert!(q.is_err());

        // Query by Index
        let q = Query::by_index(&table, &table.indices[1])
            .add("New York")
            .encode();
        assert!(q.is_ok());
        assert_eq!(q.unwrap().to_string(), "5 1 New York");

        let _ = db.commit(tx);
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn query_multiple_secondary_indicies1() -> Result<()> {
        let path = "test-files/query_multiple_secondary_indicies1.rdb";
        cleanup_file(path);
        let db = Arc::new(StorageEngine::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("mytable")
            .id(5)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("city", TypeCol::BYTES)
            .add_col("job", TypeCol::BYTES)
            .build(&mut tx)?;

        table.create_index("sec")?;
        table.add_col_to_index("sec", "city")?;
        table.add_col_to_index("sec", "name")?;

        assert_eq!(table.indices.len(), 2);

        // primary index
        let q = Query::by_col(&table).add("id", 1).encode();
        assert!(q.is_ok());
        assert_eq!(q.unwrap().to_string(), "5 0 1");

        // secondary index
        let q1 = Query::by_col(&table)
            .add("city", "New York")
            .add("name", "Alice")
            .encode();
        assert!(q1.is_ok());
        assert_eq!(q1.unwrap().to_string(), "5 1 New York Alice");

        // mixed order
        let q2 = Query::by_col(&table)
            .add("name", "Alice")
            .add("city", "New York")
            .encode();
        assert!(q2.is_ok());
        assert_eq!(q2.unwrap().to_string(), "5 1 New York Alice");

        // non existant index
        let q = Query::by_col(&table).add("job", "nonexistant").encode();
        assert!(q.is_err());

        // wrong data types
        let q = Query::by_col(&table).add("name", 1).add("city", 2).encode();
        assert!(q.is_err());

        let q = Query::by_col(&table)
            .add("name", "Alice")
            .add("city", 2)
            .encode();
        assert!(q.is_err());

        let q = Query::by_col(&table)
            .add("city", 2)
            .add("name", "Alice")
            .encode();
        assert!(q.is_err());

        // not all cols supplied
        let q = Query::by_col(&table).add("name", "Alice").encode();
        assert!(q.is_err());
        let q = Query::by_col(&table).add("city", "New York").encode();
        assert!(q.is_err());

        let _ = db.commit(tx);
        cleanup_file(path);
        Ok(())
    }
}
