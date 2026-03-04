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
pub(crate) struct Scanner {
    lo: (Key, Compare),
    hi: (Key, Compare),
}

impl Scanner {
    pub fn prefix<'a, P: Pager>(key: Key, tree: &'a BTree<P>) -> ScanIter<'a, P> {
        let hi = key.clone();
        let tid = key.get_tid();
        if let Some(cursor) = seek(tree, &key, Compare::Ge) {
            ScanIter {
                cursor,
                tid,
                finished: false,
                hi,
                pred: Compare::Eq,
            }
        } else {
            ScanIter {
                cursor: Cursor::new(tree),
                tid,
                finished: true,
                hi,
                pred: Compare::Eq,
            }
        }
    }

    /// convenience function for single key lookups, should not be used for composite keys like secondary indices
    pub fn open<'a, P: Pager>(key: Key, pred: Compare, tree: &'a BTree<P>) -> ScanIter<'a, P> {
        let hi = key.clone();
        let tid = key.get_tid();
        if let Some(cursor) = seek(tree, &key, pred) {
            // matching intended pred to appropiate stop condition
            let pred = match pred {
                Compare::Lt => Compare::Ge,
                Compare::Le => Compare::Gt,
                Compare::Gt => Compare::Le,
                Compare::Ge => Compare::Lt,
                Compare::Eq => Compare::Eq,
            };
            ScanIter {
                cursor,
                tid,
                finished: false,
                hi,
                pred,
            }
        } else {
            ScanIter {
                cursor: Cursor::new(tree),
                tid,
                finished: true,
                hi,
                pred: Compare::Eq,
            }
        }
    }

    /// scans a range between two keys, start position is the the key that matches lo on the predicate.
    ///
    /// hi represents the end condition, so the iterator will return values until a key matching hi is found. See `tests::scan_range`.
    ///
    /// setting the hi predicate to Eq will act as a prefix scan
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

        Ok(Scanner { lo, hi })
    }

    /// turns scanmode into iterator by performing tree read operations
    pub fn into_iter<'a, P: Pager>(self, tree: &'a BTree<P>) -> ScanIter<'a, P> {
        let lo = self.lo;
        let hi = self.hi;
        let tid = lo.0.get_tid();
        if let Some(cursor) = seek(tree, &lo.0, lo.1) {
            ScanIter {
                cursor,
                tid,
                finished: false,
                hi: hi.0,
                pred: hi.1,
            }
        } else {
            ScanIter {
                cursor: Cursor::new(tree),
                tid,
                finished: true,
                hi: hi.0,
                pred: hi.1,
            }
        }
    }
}

// pub(super) fn scan_single<P: Pager>(tree: &BTree<P>, key: &Key) -> Option<Vec<(Key, Value)>> {
//     let mut res: Vec<(Key, Value)> = vec![];
//     let cursor = seek(tree, &key, SeekConfig::Pred(Compare::Eq))?;
//     res.push(cursor.deref());
//     Some(res)
// }

pub(crate) struct ScanIter<'a, P: Pager> {
    cursor: Cursor<'a, P>,
    tid: u32,
    finished: bool,
    hi: Key,
    pred: Compare,
}

impl<P: Pager> ScanIter<'_, P> {
    /// decodes key value pairs into a record type, note: the TID and prefix gets lost in the conversion
    pub fn collect_records(self) -> Vec<Record> {
        self.into_iter().map(|kv| Record::from_kv(kv)).collect()
    }
}

impl<'a, P: Pager> Iterator for ScanIter<'a, P> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let hi_key = &self.hi;
        let hi_cmp = self.pred;

        let (k, v) = self.cursor.next()?;

        // we never cross table boundaries
        if k.get_tid() != self.tid {
            return None;
        }

        match hi_cmp {
            Compare::Eq => {
                debug_if_env!("RUSQL_LOG_CURSOR", {
                    debug!(key=%k, pred=?hi_cmp, hi=%hi_key, "comparing subkeys");
                });

                // we return as soon as the key isnt a as subkey and therefore not equal
                if !is_subkey(hi_key, &k) {
                    debug_if_env!("RUSQL_LOG_CURSOR", {
                        debug!("false, finished");
                    });
                    self.finished = true;
                    return None;
                }
            }
            pred => {
                debug_if_env!("RUSQL_LOG_CURSOR", {
                    debug!(key=%k, pred=?pred, hi=%hi_key, "comparing predicate");
                });

                // we return as soon as the key matches the high key predicate
                if key_cmp(&k, hi_key, pred) {
                    debug_if_env!("RUSQL_LOG_CURSOR", {
                        debug!("true, finished");
                    });
                    self.finished = true;
                    return None;
                }
            }
        }

        debug_if_env!("RUSQL_LOG_CURSOR", {
            debug!("false, returning {k} {v}");
        });

        Some((k, v))
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
fn seek<'a, P: Pager>(tree: &'a BTree<P>, key: &Key, pred: Compare) -> Option<Cursor<'a, P>> {
    let mut cursor = Cursor::new(tree);
    let mut ptr = tree.get_root();
    debug_if_env!("RUSQL_LOG_CURSOR", {
        debug!("seeking for key: {key}, with {pred:?}");
    });
    while let Some(p) = ptr {
        let node = tree.decode(p);

        ptr = match node.unwrap_tn().get_type() {
            NodeType::Node => {
                let idx = node_lookup(node.unwrap_tn(), &key, Compare::Le)?; // navigating nodes
                debug!(idx, "internal node lookup:");
                let ptr = node.unwrap_tn().get_ptr(idx);

                cursor.path.push(node);
                cursor.pos.push(idx);

                Some(ptr)
            }
            NodeType::Leaf => {
                let idx = node_lookup(node.unwrap_tn(), &key, pred)?;
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

/// check if the smaller key is a subkey of the larger key
///```ignore
/// let key_a = "0 1 Alice";
/// let key_b = "0 1 Alice Firefighter"
/// assert!(is_subkey(&key_a, &key_b))
///```
fn is_subkey(a: &Key, b: &Key) -> bool {
    let len = usize::min(a.len(), b.len());
    debug_if_env!("RUSQL_LOG_CURSOR", {
        debug!(%a, %b, "comparing subkeys");
    });
    &a.as_slice()[..len] == &b.as_slice()[..len]
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

fn node_lookup(node: &TreeNode, key: &Key, pred: Compare) -> Option<u16> {
    if node.get_nkeys() == 0 {
        return None;
    }
    match pred {
        Compare::Lt => lookup_lt(node, key),
        Compare::Le => lookup_le(node, key),
        Compare::Gt => lookup_gt(node, key),
        Compare::Ge => lookup_ge(node, key),
        Compare::Eq => lookup_eq(node, key),
    }
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
            cursor::{Compare, Scanner},
        },
        pager::{KVEngine, mempage_tree},
        tables::Record,
    };

    #[test]
    fn cursor_next_navigation() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 1i64.into();
        let btree = tree.pager.tree.borrow();
        let mut cursor = seek(&btree, &key, Compare::Eq).unwrap();

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
        let mut cursor = seek(&btree, &key, Compare::Eq).unwrap();

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
        let cursor = seek(&btree, &key, Compare::Eq).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test GE - deref should return the exact match or next greater
        let cursor = seek(&btree, &key, Compare::Ge).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test GT - deref should return the next value after key
        let cursor = seek(&btree, &key, Compare::Gt).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 6");

        // Test LE - deref should return the exact match or next smaller
        let cursor = seek(&btree, &key, Compare::Le).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test LT - deref should return the value before key
        let cursor = seek(&btree, &key, Compare::Lt).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 4");

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
        let res = Scanner::range((k_lo, Compare::Ge), (k_hi, Compare::Gt))?.into_iter(&*tree_ref);

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
