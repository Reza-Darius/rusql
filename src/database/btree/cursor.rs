use std::sync::Arc;

use super::bs::*;
use tracing::{debug, error, instrument};

use crate::{
    database::{
        BTree,
        btree::{Tree, TreeNode, node::NodeType},
        errors::{Result, ScanError},
        pager::diskpager::Pager,
        tables::{Key, Record, Value, keyvalues::KeyRef},
        types::Node,
    },
    debug_if_env,
    interpreter::Operator,
};

#[derive(Debug, Clone)]
pub(crate) enum ScanMode {
    // scanning the entire table starting from key
    Open(Key, Compare, CursorDir),
    // scans table within range
    Range {
        lo: (Key, Compare),
        hi: (Key, Compare),
    },
}

pub(crate) struct PrefixScanIter<'a, P: Pager> {
    cursor: Cursor<'a, P>,
    pred: Compare,
    key: Key,
    tid: u32,
    finished: bool,
}

impl<'a, P: Pager> PrefixScanIter<'a, P> {
    pub fn collect_records(self) -> Vec<Record> {
        self.into_iter().map(|kv| Record::from_kv(kv)).collect()
    }
}

impl<'a, P: Pager> Iterator for PrefixScanIter<'a, P> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let (k, v) = self.cursor.next()?;

        debug_if_env!("RUSQL_LOG_CURSOR", {
            debug!(key=%k, pred=?self.pred, cmp=%self.key, "comparing prefix");
        });

        if !prefix_cmp(&k, self.pred, &self.key) {
            debug_if_env!("RUSQL_LOG_CURSOR", {
                debug!("false, returning none");
            });
            self.finished = true;
            return None;
        }

        debug_if_env!("RUSQL_LOG_CURSOR", {
            debug!("true, returning {k}");
        });

        Some((k, v))
    }
}

/// is key a a sub key of b?
///```ignore
/// let key_a = "0 1 Alice";
/// let key_b = "0 1 Alice Firefighter"
/// assert!(is_subkey(&key_a, &key_b))
///```
fn is_subkey(a: &Key, b: &Key) -> bool {
    let len = a.len();
    debug_if_env!("RUSQL_LOG_CURSOR", {
        debug!(%a, %b, "comparing subkeys");
    });
    if a.as_slice() == &b.as_slice()[..len] {
        true
    } else {
        false
    }
}

// equalized the length of both keys to the shortest key then compares
pub fn prefix_cmp<'a, K1: Into<KeyRef<'a>>, K2: Into<KeyRef<'a>>>(
    k1: K1,
    pred: Compare,
    k2: K2,
) -> bool {
    let k1 = k1.into();
    let k2 = k2.into();

    let trunc_len = usize::min(k1.len(), k2.len());

    let k1 = KeyRef::from_slice(&k1.as_slice()[..trunc_len]);
    let k2 = KeyRef::from_slice(&k2.as_slice()[..trunc_len]);

    match pred {
        Compare::Lt => k1 < k2,
        Compare::Le => k1 <= k2,
        Compare::Gt => k1 > k2,
        Compare::Ge => k1 >= k2,
        Compare::Eq => k1 == k2,
    }
}

impl ScanMode {
    /// returns key matching the predicate as a prefix, meaning keys get truncated to length
    ///
    /// seek uses non-prefix GE
    ///
    ///```ignore
    /// let key = "1 0 Alice"
    /// let prefixscan = ScanMode::prefix(key, &tree, Compare::Eq)?;
    /// assert_eq!(prefixscan.next(), "1 0 Alice Clerk");
    /// assert_eq!(prefixscan.next(), "1 0 Alice Firefighter");
    /// assert_eq!(prefixscan.next(), "1 0 Alice Policewoman");
    ///```
    /// This Scan is eagerly evaluated!
    pub fn prefix<P: Pager>(
        key: Key,
        tree: &BTree<P>,
        pred: Compare,
    ) -> Result<PrefixScanIter<'_, P>> {
        if key.len() == 0 || key.is_sentinal_empty() {
            return Err(ScanError::ScanCreateError(
                "couldnt create prefix iter: key is empty".to_string(),
            )
            .into());
        }

        if tree.is_empty() {
            return Err(ScanError::ScanCreateError(
                "couldnt create prefix iter: tree is empty".to_string(),
            )
            .into());
        }
        let tid = key.get_tid();

        if let Some(cursor) = seek(tree, &key, SeekConfig::Prefix(pred)) {
            Ok(PrefixScanIter {
                cursor,
                pred,
                key,
                tid,
                finished: false,
            })
        } else {
            // we return an empty iterator
            Ok(PrefixScanIter {
                cursor: Cursor::new(tree),
                pred,
                key,
                tid,
                finished: true,
            })
        }
    }

    /// single scan, basically tree_get() over the cursor API
    pub fn single<P: Pager>(key: Key, tree: &BTree<P>) -> Option<(Key, Value)> {
        let cursor = seek(tree, &key, SeekConfig::Pred(Compare::Eq))?;
        Some(cursor.deref())
    }

    /// Open ScanMode returns records that match the predicate starting from the first key matching the predicate
    ///
    /// if the key is 10 and predicate is "GT" it will match and return keys: 11,12,13 etc
    ///
    /// ScanMode is lazy, and wont yield anything until [`ScanMode::into_iter`] is called, which then performs read operations
    pub fn open(key: Key, pred: Compare, dir: CursorDir) -> Result<Self> {
        Ok(ScanMode::Open(key, pred, dir))
    }

    /// scans a range between two keys, start position is the the key that matches lo on the predicate.
    ///
    /// hi represents the end condition, so the iterator will return values until a key matching hi is found. See `tests::scan_range`.
    ///
    /// ScanMode is lazy, and wont yield anything until [`ScanMode::into_iter`] is called, which then performs read operations
    pub fn range(lo: (Key, Compare), hi: (Key, Compare)) -> Result<Self> {
        let tid = lo.0.get_tid();
        if tid != hi.0.get_tid() {
            error!("invalid input: keys from different tables provided");
            return Err(ScanError::ScanCreateError(
                "invalid input: keys from different tables provided".to_string(),
            )
            .into());
        }
        if lo.0 > hi.0 {
            error!("invalid input: low point exceeds high point");
            return Err(ScanError::ScanCreateError(
                "invalid input: low point exceeds high point".to_string(),
            )
            .into());
        }
        Ok(ScanMode::Range { lo, hi })
    }

    /// turns scanmode into iterator by performing tree read operations
    pub fn into_iter<'a, P: Pager>(self, tree: &'a BTree<P>) -> ScanIter<'a, P> {
        match self {
            ScanMode::Open(key, pred, dir) => {
                // let dir = match pred {
                //     Compare::Lt | Compare::Le => CursorDir::Prev,
                //     Compare::Gt | Compare::Ge | Compare::Eq => CursorDir::Next,
                // };
                let tid = key.get_tid();
                if let Some(cursor) = seek(tree, &key, SeekConfig::Pred(pred)) {
                    ScanIter {
                        cursor,
                        tid,
                        dir,
                        mode: ScanIterMode::Open(key, pred),
                        finished: false,
                    }
                } else {
                    ScanIter {
                        cursor: Cursor::new(tree),
                        tid,
                        dir,
                        mode: ScanIterMode::Open(key, pred),
                        finished: true,
                    }
                }
            }
            ScanMode::Range { lo, hi } => {
                let tid = lo.0.get_tid();
                if let Some(cursor) = seek(tree, &lo.0, SeekConfig::Pred(lo.1)) {
                    ScanIter {
                        cursor,
                        tid,
                        dir: CursorDir::Next,
                        mode: ScanIterMode::Range(hi.0, hi.1),
                        finished: false,
                    }
                } else {
                    ScanIter {
                        cursor: Cursor::new(tree),
                        tid,
                        dir: CursorDir::Next,
                        mode: ScanIterMode::Range(hi.0, hi.1),
                        finished: true,
                    }
                }
            }
        }
    }
}

pub(super) fn scan_single<P: Pager>(tree: &BTree<P>, key: &Key) -> Option<Vec<(Key, Value)>> {
    let mut res: Vec<(Key, Value)> = vec![];
    let cursor = seek(tree, &key, SeekConfig::Pred(Compare::Eq))?;
    res.push(cursor.deref());
    Some(res)
}

pub(crate) struct ScanIter<'a, P: Pager> {
    cursor: Cursor<'a, P>,
    tid: u32,
    dir: CursorDir,
    mode: ScanIterMode,
    finished: bool,
}

enum ScanIterMode {
    Open(Key, Compare),
    Range(Key, Compare),
}

impl<P: Pager> ScanIter<'_, P> {
    /// decodes key value pairs into a record type, note: the TID and prefix gets lost in the conversion
    pub fn collect_records(self) -> Vec<Record> {
        self.into_iter().map(|kv| Record::from_kv(kv)).collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CursorDir {
    Next,
    Prev,
}

impl<'a, P: Pager> Iterator for ScanIter<'a, P> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match &self.mode {
            // WIP
            // range scan
            ScanIterMode::Range(hi_key, hi_cmp) => {
                let (k, v) = self.cursor.next()?;

                debug_if_env!("RUSQL_LOG_CURSOR", {
                    debug!(key=%k, pred=?hi_cmp, hi=%hi_key, "comparing");
                });

                assert!(hi_key.len() <= k.len());
                let k_trunc = KeyRef::from_slice(&k.as_slice()[..hi_key.len()]); // truncating both keys to the same length

                // we return as soon as the key matches the high key predicate
                if key_cmp(k_trunc, hi_key, *hi_cmp) {
                    debug_if_env!("RUSQL_LOG_CURSOR", {
                        debug!("true, finished");
                    });
                    self.finished = true;
                    return None;
                }

                // debug_if_env!("RUSQL_LOG_CURSOR", {
                //     debug!(hi=%hi_key, pred=?hi_cmp, key=%k, "comparing");
                // });

                // // we return as soon as the key matches the high key predicate
                // if key_cmp(&hi_key, &k, *hi_cmp) {
                //     debug_if_env!("RUSQL_LOG_CURSOR", {
                //         debug!("true, finished");
                //     });
                //     self.finished = true;
                //     return None;
                // }

                debug_if_env!("RUSQL_LOG_CURSOR", {
                    debug!("false, returning {k} {v}");
                });

                Some((k, v))
            }
            // open scan
            ScanIterMode::Open(cmp_key, pred) => {
                let (k, v) = match self.dir {
                    CursorDir::Next => self.cursor.next()?,
                    CursorDir::Prev => self.cursor.prev()?,
                };

                debug_if_env!("RUSQL_LOG_CURSOR", {
                    debug!(key=%k, pred=?pred, hi=%cmp_key, "comparing");
                });

                if k.get_tid() != self.tid {
                    // return as soon as we see a key belonging to a different table or index
                    self.finished = true;
                    return None;
                };

                // we return as soon as the key doesnt match the predicate anymore
                if !key_cmp(&k, cmp_key, *pred) {
                    debug_if_env!("RUSQL_LOG_CURSOR", {
                        debug!("false, finished");
                    });

                    self.finished = true;
                    return None;
                }

                debug_if_env!("RUSQL_LOG_CURSOR", {
                    debug!("true, returning");
                });

                Some((k, v))
            }
        }
    }
}

/*
path = [R, N2, L2] Nodes to be read
pos  = [1, 1 , 2]   indices correspond to an idx to a given key

in this case, the key would be located at:
idx 1 in root, index 1 in Node 2 and index 2 in Leaf 2
*/

#[derive(Debug)]
pub(crate) struct Cursor<'a, P: Pager> {
    tree: &'a BTree<P>,
    path: Vec<Arc<Node>>, // from root to leaf
    pos: Vec<u16>,        // indices
    empty: bool,
}

impl<'a, P: Pager> Cursor<'a, P> {
    pub fn new(tree: &'a BTree<P>) -> Self {
        Cursor {
            tree: tree,
            path: vec![],
            pos: vec![],
            empty: false,
        }
    }

    // retrieves the key value pair at current position
    pub fn deref(&self) -> (Key, Value) {
        let node = &self.path[self.path.len() - 1];
        let idx = self.pos[self.path.len() - 1];

        let key = node.unwrap_tn().get_key(idx).unwrap();
        let val = node.unwrap_tn().get_val(idx).unwrap();
        (key, val)
    }

    // moves the path one idx forward
    pub fn next(&mut self) -> Option<(Key, Value)> {
        if self.empty {
            return None;
        }
        let res = self.deref();
        self.iter_next(self.path.len() - 1);
        Some(res)
    }

    fn iter_next(&mut self, level: usize) {
        if self.pos[level] + 1 < self.path[level].unwrap_tn().get_nkeys() {
            // move within node
            self.pos[level] += 1;
        } else if level > 0 {
            // we reached the last key of the node, so we go up one level to access the sibling
            self.iter_next(level - 1);
        } else {
            // past last key
            self.empty = true;
            return;
        }
        if level + 1 < self.pos.len() {
            // we are in a non leaf node and need to retrieve the next sibling
            let node = &self.path[level];
            let kid = self.tree.decode(node.unwrap_tn().get_ptr(self.pos[level]));

            self.path[level + 1] = kid;
            self.pos[level + 1] = 0;
        }
    }

    // moves the path one idx backwards
    pub fn prev(&mut self) -> Option<(Key, Value)> {
        if self.empty {
            return None;
        }
        let res = self.deref();
        if res.0.is_sentinal_empty() {
            // empty key edge case!
            self.empty = true;
            return None;
        }
        self.iter_prev(self.path.len() - 1);
        Some(res)
    }

    fn iter_prev(&mut self, level: usize) {
        if self.pos[level] > 0 {
            // move within node
            self.pos[level] -= 1;
        } else if level > 0 {
            // we reached the last key of the node, so we go up one level to access the sibling
            self.iter_prev(level - 1);
        } else {
            // past last key
            self.empty = true;
            return;
        }
        if level + 1 < self.pos.len() {
            // we are in a non leaf node and need to retrieve the next sibling
            let node = &self.path[level];
            let kid = self.tree.decode(node.unwrap_tn().get_ptr(self.pos[level]));

            self.pos[level + 1] = kid.unwrap_tn().get_nkeys() - 1;
            self.path[level + 1] = kid;
        }
    }
}

// creates a new cursor
#[instrument(skip_all)]
fn seek<'a, P: Pager>(tree: &'a BTree<P>, key: &Key, flag: SeekConfig) -> Option<Cursor<'a, P>> {
    let mut cursor = Cursor::new(tree);
    let mut ptr = tree.get_root();
    debug_if_env!("RUSQL_LOG_CURSOR", {
        debug!("seeking for key: {key}, with {flag:?}");
    });
    while let Some(p) = ptr {
        let node = tree.decode(p);

        ptr = match node.unwrap_tn().get_type() {
            NodeType::Node => {
                let idx = node_lookup(node.unwrap_tn(), &key, &SeekConfig::Pred(Compare::Le))?; // navigating nodes
                debug!(idx, "internal node lookup:");
                let ptr = node.unwrap_tn().get_ptr(idx);

                cursor.path.push(node);
                cursor.pos.push(idx);

                Some(ptr)
            }
            NodeType::Leaf => {
                let idx = node_lookup(node.unwrap_tn(), &key, &flag)?;
                debug_if_env!("RUSQL_LOG_CURSOR", {
                    debug!(idx, "seek idx after lookup");
                });

                cursor.path.push(node);
                cursor.pos.push(idx);

                None
            }
        }
    }
    debug!("creating cursor, pos: {:?}", cursor.pos);
    if cursor.pos.is_empty() || cursor.path.is_empty() {
        return None;
    }
    // accounting for empty key edge case
    if cursor.deref().0.is_sentinal_empty() {
        return None;
    }
    assert_eq!(cursor.pos.len(), cursor.path.len());

    Some(cursor)
}

fn key_cmp<'a, K1: Into<KeyRef<'a>>, K2: Into<KeyRef<'a>>>(k1: K1, k2: K2, pred: Compare) -> bool {
    let k1 = k1.into();
    let k2 = k2.into();
    match pred {
        Compare::Lt => k1 < k2,
        Compare::Le => k1 <= k2,
        Compare::Gt => k1 > k2,
        Compare::Ge => k1 >= k2,
        Compare::Eq => k1 == k2,
    }
}

fn node_lookup(node: &TreeNode, key: &Key, flag: &SeekConfig) -> Option<u16> {
    if node.get_nkeys() == 0 {
        return None;
    }
    match flag {
        SeekConfig::Pred(p) => {
            debug!("pred compare: {p:?}");
            match p {
                Compare::Lt => lookup_lt(node, key),
                Compare::Le => lookup_le(node, key),
                Compare::Gt => lookup_gt(node, key),
                Compare::Ge => lookup_ge(node, key),
                Compare::Eq => lookup_eq(node, key),
            }
        }
        SeekConfig::Prefix(p) => {
            // let idx = cmp_ge(node, key)?;
            // let cmp_key = node.get_key(idx).ok()?; // we just compared against it
            // let len = key.len();

            // if is_subkey(&key, &cmp_key) {
            //     Some(idx)
            // } else {
            //     None
            // }

            debug!("prefix compare: {p:?}");
            match p {
                Compare::Lt => lookup_lt(node, key),
                Compare::Le => lookup_le(node, key),
                Compare::Gt => lookup_gt(node, key),
                Compare::Ge => lookup_ge(node, key),
                Compare::Eq => lookup_ge(node, key), // we cannot seek on EQ for prefix
            }
        }
    }
}

#[derive(Debug)]
enum SeekConfig {
    Pred(Compare),
    Prefix(Compare),
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub(crate) enum Compare {
    Lt, // <
    Le, // <=
    Gt, // >
    Ge, // >=
    Eq, // ==
}

impl From<Operator> for Compare {
    fn from(value: Operator) -> Self {
        match value {
            Operator::Equal => Compare::Eq,
            Operator::Lt => Compare::Lt,
            Operator::Le => Compare::Le,
            Operator::Gt => Compare::Gt,
            Operator::Ge => Compare::Ge,
            _ => unreachable!("invalid compare operator for conversion"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_log::test;

    use crate::database::{
        btree::{
            SetFlag, Tree,
            cursor::{Compare, ScanMode},
        },
        pager::{KVEngine, mempage_tree},
        tables::Record,
    };

    #[test]
    fn scan_single1() -> Result<()> {
        let tree = mempage_tree();

        for i in 1u16..=100u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }

        for i in 1u16..=100u16 {
            let key = format!("{}", i).into();
            let tree_ref = tree.pager.tree.borrow();
            let res = scan_single(&tree_ref, &key);

            assert!(res.is_some());
            let res: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

            assert_eq!(res[0].to_string(), format!("{i} value"));
        }

        Ok(())
    }

    #[test]
    fn scan_open() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap()
        }

        let key = 5i64.into();
        let q = ScanMode::Open(key, Compare::Gt, CursorDir::Next);
        let tree_ref = tree.pager.tree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());

        let mut recs = res.unwrap().map(Record::from_kv).into_iter();

        assert_eq!(recs.next().unwrap().to_string(), "6 value");
        assert_eq!(recs.next().unwrap().to_string(), "7 value");
        assert_eq!(recs.next().unwrap().to_string(), "8 value");
        assert_eq!(recs.next().unwrap().to_string(), "9 value");
        assert_eq!(recs.next().unwrap().to_string(), "10 value");
        Ok(())
    }

    #[test]
    fn cursor_next_navigation() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 1i64.into();
        let btree = tree.pager.tree.borrow();
        let mut cursor = seek(&btree, &key, SeekConfig::Pred(Compare::Eq)).unwrap();

        // Navigate through all elements using next()
        for i in 1i64..=10i64 {
            let (k, v) = cursor.next().unwrap();
            assert_eq!(k.to_string(), format!("1 0 {}", i));
            assert_eq!(v.to_string(), format!("val{}", i));
        }

        // Should return None after last element
        assert!(cursor.next().is_none());

        Ok(())
    }

    #[test]
    fn cursor_prev_navigation() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 10i64.into();
        let btree = tree.pager.tree.borrow();
        let mut cursor = seek(&btree, &key, SeekConfig::Pred(Compare::Eq)).unwrap();

        // Navigate backwards using prev()
        for i in (1i64..=10i64).rev() {
            let (k, v) = cursor.prev().unwrap();
            assert_eq!(k.to_string(), format!("1 0 {}", i));
            assert_eq!(v.to_string(), format!("val{}", i));
        }

        // Should return None before first element
        assert!(cursor.prev().is_none());

        Ok(())
    }

    #[test]
    fn scan_single_existing_keys() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=50i64 {
            tree.set(
                format!("{}", i).into(),
                format!("value{}", i).into(),
                SetFlag::UPSERT,
            )
            .unwrap();
        }

        // Test random keys
        for i in &[1i64, 10, 25, 40, 50] {
            let key = format!("{}", i).into();
            let tree_ref = tree.pager.tree.borrow();
            let res = scan_single(&tree_ref, &key);

            assert!(res.is_some());
            let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].to_string(), format!("{} value{}", i, i));
        }

        Ok(())
    }

    #[test]
    fn scan_single_nonexistent_key() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap();
        }

        let key = 999i64.into();
        let tree_ref = tree.pager.tree.borrow();
        let res = scan_single(&tree_ref, &key);

        assert!(res.is_none());
        Ok(())
    }

    // Test scan_open with GT (greater than)
    #[test]
    fn scan_open_gt() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 5i64.into();
        let q = ScanMode::Open(key, Compare::Gt, CursorDir::Next);
        let tree_ref = tree.pager.tree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

        assert_eq!(records.len(), 5);
        assert_eq!(records[0].to_string(), "6 val6");
        assert_eq!(records[4].to_string(), "10 val10");

        Ok(())
    }

    #[test]
    fn scan_open_ge() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 5i64.into();
        let q = ScanMode::Open(key, Compare::Ge, CursorDir::Next);
        let tree_ref = tree.pager.tree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

        assert_eq!(records.len(), 6);
        assert_eq!(records[0].to_string(), "5 val5");
        assert_eq!(records[5].to_string(), "10 val10");

        Ok(())
    }
    #[test]
    fn scan_open_lt() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 6i64.into();
        let q = ScanMode::Open(key, Compare::Lt, CursorDir::Prev);
        let tree_ref = tree.pager.tree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

        assert_eq!(records.len(), 5);
        assert_eq!(records[0].to_string(), "5 val5");
        assert_eq!(records[4].to_string(), "1 val1");

        Ok(())
    }

    #[test]
    fn scan_open_le() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 6i64.into();
        let q = ScanMode::Open(key, Compare::Le, CursorDir::Prev);
        let tree_ref = tree.pager.tree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

        assert_eq!(records.len(), 6);
        assert_eq!(records[0].to_string(), "6 val6");
        assert_eq!(records[5].to_string(), "1 val1");

        Ok(())
    }

    #[test]
    fn scan_open_from_first_element() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap();
        }

        let key = 1i64.into();
        let q = ScanMode::Open(key, Compare::Ge, CursorDir::Next);
        let tree_ref = tree.pager.tree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        assert_eq!(res.unwrap().count(), 10);

        Ok(())
    }

    #[test]
    fn scan_open_from_last_element() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap();
        }

        let key = 10i64.into();
        let q = ScanMode::Open(key, Compare::Le, CursorDir::Prev);
        let tree_ref = tree.pager.tree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        assert_eq!(res.unwrap().count(), 10);

        Ok(())
    }

    #[test]
    fn scan_open_beyond_range() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap();
        }

        let tree_ref = tree.pager.tree.borrow();

        // GT from last element
        let key = 10i64.into();
        let q = ScanMode::Open(key, Compare::Gt, CursorDir::Next);
        let mut res = tree_ref.scan(q)?;

        assert!(res.next().is_none());

        // LT from first element
        let key = 1i64.into();
        let q = ScanMode::Open(key, Compare::Lt, CursorDir::Prev);
        let mut res = tree_ref.scan(q)?;

        assert!(res.next().is_none());

        Ok(())
    }
    // Test with large dataset
    #[test]
    fn scan_large_dataset() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=1000i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 500i64.into();
        let q = ScanMode::Open(key, Compare::Gt, CursorDir::Next);
        let tree_ref = tree.pager.tree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());

        let records: Vec<Record> = res.unwrap().map(Record::from_kv).collect();

        assert_eq!(records.len(), 500);
        assert_eq!(records[0].to_string(), "501 val501");
        assert_eq!(records[499].to_string(), "1000 val1000");

        Ok(())
    }

    // Test seek with different Compare flags
    #[test]
    fn seek_with_different_compares() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let btree = tree.pager.tree.borrow();

        // Test EQ - deref should return the exact match
        let key = 5i64.into();
        let cursor = seek(&btree, &key, SeekConfig::Pred(Compare::Eq)).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test GE - deref should return the exact match or next greater
        let cursor = seek(&btree, &key, SeekConfig::Pred(Compare::Ge)).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test GT - deref should return the next value after key
        let cursor = seek(&btree, &key, SeekConfig::Pred(Compare::Gt)).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 6");

        // Test LE - deref should return the exact match or next smaller
        let cursor = seek(&btree, &key, SeekConfig::Pred(Compare::Le)).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test LT - deref should return the value before key
        let cursor = seek(&btree, &key, SeekConfig::Pred(Compare::Lt)).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 4");

        Ok(())
    }

    #[test]
    fn empty_tree_scan() -> Result<()> {
        let tree = mempage_tree();

        let key = 1i64.into();
        let tree_ref = tree.pager.tree.borrow();
        let res = scan_single(&tree_ref, &key);

        assert!(res.is_none());

        let q = ScanMode::Open(1i64.into(), Compare::Gt, CursorDir::Next);
        let res = tree_ref.scan(q);

        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn single_element_tree() -> Result<()> {
        let tree = mempage_tree();
        tree.set(1i64.into(), "value".into(), SetFlag::UPSERT)
            .unwrap();
        let tree_ref = tree.pager.tree.borrow();

        // Scan single
        let res = scan_single(&tree_ref, &1i64.into());
        assert!(res.is_some());
        assert_eq!(res.unwrap().len(), 1);

        // Scan GT (should return none)
        let q = ScanMode::Open(1i64.into(), Compare::Gt, CursorDir::Next);
        let mut res = tree_ref.scan(q)?;
        assert!(res.next().is_none());

        // Scan GE (should return the element)
        let q = ScanMode::Open(1i64.into(), Compare::Ge, CursorDir::Next);
        let res = tree_ref.scan(q)?;
        assert_eq!(res.count(), 1);

        Ok(())
    }

    #[test]
    fn scan_range() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=5i64 {
            tree.set(
                format!("table 1 {}", i).into(),
                "value".into(),
                SetFlag::UPSERT,
            )
            .unwrap()
        }

        for i in 1i64..=5i64 {
            tree.set(
                format!("table 2 {}", i).into(),
                "value".into(),
                SetFlag::UPSERT,
            )
            .unwrap()
        }

        let k_lo = "table 1 1".into();
        let k_hi = "table 1 5".into();
        let tree_ref = tree.pager.tree.borrow();
        let res = ScanMode::range((k_lo, Compare::Ge), (k_hi, Compare::Gt))?.into_iter(&*tree_ref);

        let mut recs = res.collect_records().into_iter();

        assert_eq!(recs.next().unwrap().to_string(), "table 1 1 value");
        assert_eq!(recs.next().unwrap().to_string(), "table 1 2 value");
        assert_eq!(recs.next().unwrap().to_string(), "table 1 3 value");
        assert_eq!(recs.next().unwrap().to_string(), "table 1 4 value");
        assert_eq!(recs.next().unwrap().to_string(), "table 1 5 value");
        assert!(recs.next().is_none());

        Ok(())
    }
}
