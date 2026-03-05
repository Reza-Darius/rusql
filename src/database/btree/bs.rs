use crate::database::{btree::TreeNode, tables::Key};
use crate::debug_if_env;
use tracing::debug;

pub fn lookup_lt(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let key = key.as_ref();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug_if_env!("RUSQL_LOG_CMP", {
        debug!(
            "lookup_lt, key: {} in {:?} nkeys {}",
            key,
            node.get_type(),
            nkeys
        );
    });

    while hi > lo {
        let m = (hi + lo) / 2;
        let v = node.get_key(m).ok()?;
        // if v == key {
        //     return None; // key already exists
        // };
        if v >= key {
            // changed to larger equal
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == 0 { None } else { Some(lo - 1) }
}

pub fn lookup_le(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let key = key.as_ref();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug_if_env!("RUSQL_LOG_CMP", {
        debug!(
            "lookup_le, key: {} in {:?} nkeys {}",
            key,
            node.get_type(),
            nkeys
        );
    });

    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v == key {
            return Some(m as u16);
        };
        if v > key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == 0 { Some(0) } else { Some(lo - 1) }
}

pub fn lookup_gt(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let key = key.as_ref();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug_if_env!("RUSQL_LOG_CMP", {
        debug!(
            "lookup_gt, key: {} in {:?} nkeys {}",
            key,
            node.get_type(),
            nkeys
        );
    });

    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v > key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == nkeys { None } else { Some(lo) }
}

pub fn lookup_ge(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let key = key.as_ref();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug_if_env!("RUSQL_LOG_CMP", {
        debug!(
            "lookup_ge, key: {} in {:?} nkeys {}",
            key,
            node.get_type(),
            nkeys
        );
    });

    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m
        if v == key {
            return Some(m as u16);
        };
        if v > key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == nkeys { None } else { Some(lo) }
}

pub fn lookup_eq(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let key = key.as_ref();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug_if_env!("RUSQL_LOG_CMP", {
        debug!(
            "lookup_eq, key: {} in {:?} nkeys {}",
            key,
            node.get_type(),
            nkeys
        );
    });

    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v == key {
            return Some(m as u16);
        };
        if v > key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    None
}
