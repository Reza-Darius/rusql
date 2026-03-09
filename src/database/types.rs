/*
 * higher level types, wrappers and constants used throughout the database
 */

use std::{
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
};

use crate::{
    database::{
        btree::TreeNode,
        pager::{diskpager::NodeFlag, freelist::FLNode},
    },
    interpreter::ValueObject,
};

// Table Config
pub const MAX_COLUMNS: u16 = 100;

// Tree Config
pub const BTREE_MAX_KEY_SIZE: usize = 1000;
pub const BTREE_MAX_VAL_SIZE: usize = 3000;
// determines when nodes should be merged, higher number = less merges
pub const MERGE_FACTOR: usize = PAGE_SIZE / 4;
pub const RESERVED_PAGES: u64 = 2;

// size of one page on disk
pub const PAGE_SIZE: usize = 4096; // 4096 bytes
pub const TRUNC_THRESHOLD: usize = 1000; // amount of free list entries which trigger a truncation call
pub const LOAD_FACTOR_THRESHOLD: f64 = 0.5; // percentage value

// maximum size for nodes inside memory
pub const NODE_SIZE: usize = PAGE_SIZE * 2;
pub const PTR_SIZE: usize = 8;
pub const VER_SIZE: usize = std::mem::size_of::<u64>();
pub const FREE_PAGE: u64 = 0; // sentinel value for pages that are free to be reused, unreachable from any committed root

// buffer sizes
pub const LRU_BUFFER_SIZE: usize = 10;
pub const HISTORY_BUFFER_SIZE: usize = 10;
pub const TBUFFER_CAP: u16 = 10;

pub const U16_SIZE: usize = 2;

/// implements deref to get to the underlying array
pub enum Node {
    Tree(TreeNode),
    Freelist(FLNode),
}

impl Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tree(arg0) => f.debug_tuple("tree node").field(arg0).finish(),
            Self::Freelist(arg0) => f.debug_tuple("freelist node").field(arg0).finish(),
        }
    }
}

impl Node {
    /// deconstructs node to tree node, will panic if used on a FL node!
    pub fn unwrap_tn(&self) -> &TreeNode {
        let Node::Tree(n) = self else {
            panic!("Tree node deconstructor used on FL node!")
        };
        n
    }

    /// deconstructs node to FL node, will panic if used on a tree node!
    pub fn unwrap_fl(&self) -> &FLNode {
        let Node::Freelist(n) = self else {
            panic!("FL node deconstructor used on tree node!")
        };
        n
    }

    /// deconstructs node to FL node, will panic if used on a tree node!
    pub fn unwrap_fl_mut(&mut self) -> &mut FLNode {
        let Node::Freelist(n) = self else {
            panic!("FL node deconstructor used on tree node!")
        };
        n
    }
    pub fn fits_page(&self) -> bool {
        if let Node::Tree(n) = self {
            n.fits_page()
        } else {
            // FL nodes always fits
            true
        }
    }
    pub fn get_type(&self) -> NodeFlag {
        match self {
            Node::Tree(_) => NodeFlag::Tree,
            Node::Freelist(_) => NodeFlag::Freelist,
        }
    }
}

impl Deref for Node {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Node::Tree(tree_node) => tree_node,
            Node::Freelist(flnode) => flnode,
        }
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Node::Tree(tree_node) => tree_node,
            Node::Freelist(flnode) => flnode,
        }
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        match self {
            Self::Tree(arg0) => Self::Tree(arg0.clone()),
            Self::Freelist(arg0) => Self::Freelist(arg0.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash, Ord)]
pub(crate) struct Pointer(pub u64);

impl Pointer {
    pub fn as_slice(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
    pub fn get(self) -> u64 {
        self.0
    }
    pub fn set(&mut self, val: u64) {
        self.0 = val
    }
    pub fn from(val: u64) -> Self {
        Pointer(val)
    }
}

impl From<u64> for Pointer {
    fn from(value: u64) -> Self {
        Pointer(value)
    }
}
impl From<usize> for Pointer {
    fn from(value: usize) -> Self {
        Pointer(value as u64)
    }
}

impl Display for Pointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "page: {}", self.0)
    }
}

/// Cell data used for binding to query statements
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum DataCell {
    Str(String),
    Int(i64),
}

impl From<i64> for DataCell {
    fn from(value: i64) -> Self {
        DataCell::Int(value)
    }
}

impl From<&str> for DataCell {
    fn from(value: &str) -> Self {
        DataCell::Str(value.to_string())
    }
}

impl PartialEq<&str> for DataCell {
    fn eq(&self, other: &&str) -> bool {
        match self {
            DataCell::Str(s) => s.as_str() == *other,
            DataCell::Int(_) => false,
        }
    }
}

impl PartialEq<str> for DataCell {
    fn eq(&self, other: &str) -> bool {
        match self {
            DataCell::Str(s) => s.as_str() == other,
            DataCell::Int(_) => false,
        }
    }
}

impl PartialEq<i64> for DataCell {
    fn eq(&self, other: &i64) -> bool {
        match self {
            DataCell::Str(_) => false,
            DataCell::Int(i) => *i == *other,
        }
    }
}

impl PartialEq<i64> for &DataCell {
    fn eq(&self, other: &i64) -> bool {
        match self {
            DataCell::Str(_) => false,
            DataCell::Int(i) => *i == *other,
        }
    }
}

impl DataCell {
    pub(crate) fn as_ref(&self) -> DataCellRef<'_> {
        match self {
            DataCell::Str(s) => DataCellRef::Str(s.as_str()),
            DataCell::Int(i) => DataCellRef::Int(*i),
        }
    }

    pub(crate) fn char_len(&self) -> usize {
        match self {
            DataCell::Str(s) => s.len(),
            DataCell::Int(i) => {
                let mut n = *i;
                let mut digits = 0;
                while n > 0 {
                    n /= 10;
                    digits += 1;
                }
                digits
            }
        }
    }
}

impl std::fmt::Display for DataCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataCell::Str(s) => write!(f, "{s}"),
            DataCell::Int(i) => write!(f, "{i}"),
        }
    }
}

/// Trait for data types supported by RUSQL
pub trait InputData {
    fn into_cell(self) -> DataCell;
}

impl InputData for ValueObject {
    fn into_cell(self) -> DataCell {
        match self {
            ValueObject::Str(s) => DataCell::Str(String::from(&*s)),
            ValueObject::Int(i) => DataCell::Int(i),
        }
    }
}

impl InputData for DataCell {
    fn into_cell(self) -> DataCell {
        self
    }
}
impl InputData for String {
    fn into_cell(self) -> DataCell {
        DataCell::Str(self)
    }
}

impl InputData for &str {
    fn into_cell(self) -> DataCell {
        DataCell::Str(self.to_string())
    }
}

impl InputData for i64 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self)
    }
}

impl InputData for i32 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for i16 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for i8 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for u32 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for u16 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for u8 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

#[derive(PartialEq, PartialOrd)]
pub enum DataCellRef<'a> {
    Int(i64),
    Str(&'a str),
}

impl<'a> From<&'a DataCell> for DataCellRef<'a> {
    fn from(value: &'a DataCell) -> Self {
        match value {
            DataCell::Str(s) => DataCellRef::Str(s),
            DataCell::Int(i) => DataCellRef::Int(*i),
        }
    }
}

impl<'a> From<&'a ValueObject> for DataCellRef<'a> {
    fn from(value: &'a ValueObject) -> Self {
        match value {
            ValueObject::Str(s) => DataCellRef::Str(s),
            ValueObject::Int(i) => DataCellRef::Int(*i),
        }
    }
}
use crate::interpreter::StatementInterface;

pub struct LimitIter<I> {
    iter: I,
    limit: Option<usize>,
    count: usize,
}

impl<I: Iterator> Iterator for LimitIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(limit) = self.limit {
            if self.count < limit {
                self.count += 1;
                self.iter.next()
            } else {
                None
            }
        } else {
            self.iter.next()
        }
    }
}

pub trait IteratorDB: Iterator + Sized {
    /// limits the amount of iterations based on a LIMIT clause
    ///
    /// has no effect if there is no LIMIT clause is present
    fn limit(self, stmt: &impl StatementInterface) -> LimitIter<Self> {
        LimitIter {
            iter: self,
            limit: stmt.get_limit(),
            count: 0,
        }
    }
}

impl<T: Iterator> IteratorDB for T {}

#[cfg(test)]
mod types_test {
    use super::*;
    use test_log::test;

    #[test]
    fn cell_len() {
        let cell = DataCell::Int(1);
        assert_eq!(cell.char_len(), 1);

        let cell = DataCell::Int(1234);
        assert_eq!(cell.char_len(), 4);

        let cell = DataCell::Int(111);
        assert_eq!(cell.char_len(), 3);

        let cell = DataCell::Int(2314052);
        assert_eq!(cell.char_len(), 7);
    }
}
