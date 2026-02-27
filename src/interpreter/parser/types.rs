use std::rc::Rc;
use std::{collections::HashSet, ops::Deref};

use crate::{
    database::{
        errors::{ParseError, Result},
        types::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE},
    },
    interpreter::tokens::Operator,
};

use tracing::error;

#[derive(Debug)]
pub struct StatementIdentifier(pub Rc<str>);

impl StatementIdentifier {
    fn is_valid(&self) -> Result<()> {
        if self.0.is_empty() || self.0.len() > BTREE_MAX_KEY_SIZE {
            error!(
                ident = self.0.deref(),
                "validation error: invalid identifier"
            );
            Err(ParseError::ValidationError("identifier is empty or exceeds size").into())
        } else {
            Ok(())
        }
    }
}

pub fn is_valid_col(column: &str) -> Result<()> {
    if column.is_empty() || column.len() > BTREE_MAX_KEY_SIZE {
        error!(column, "validation error: invalid column");
        Err(ParseError::ValidationError("column is empty or exceeds size").into())
    } else {
        Ok(())
    }
}

pub fn is_valid_identifier(ident: &str) -> Result<()> {
    if ident.is_empty() || ident.len() > BTREE_MAX_KEY_SIZE {
        error!(ident, "validation error: invalid identifier");
        Err(ParseError::ValidationError("identifier is empty or exceeds size").into())
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub struct StatementIndex {
    pub column: String,
    pub operator: Operator,
    pub expr: ValueObject,
}

impl StatementIndex {
    pub fn is_valid(&self, columns: Option<&StatementColumns>) -> Result<()> {
        is_valid_col(&self.column)?;

        // // if the index column doesn't matches the provided columns from a SELECT statement for example
        // if let Some(stmt) = columns
        //     && let StatementColumns::Cols(cols) = stmt
        // {
        //     for col in cols.iter() {
        //         is_valid_col(col)?;
        //     }
        // }

        self.expr.is_valid()?;
        self.operator.is_valid_cmp()?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum StatementColumns {
    Wildcard,
    Cols(Vec<String>),
}

impl StatementColumns {
    pub fn len(&self) -> usize {
        match self {
            StatementColumns::Wildcard => 0,
            StatementColumns::Cols(items) => items.len(),
        }
    }

    pub fn is_valid(&self) -> Result<()> {
        if let StatementColumns::Cols(cols) = self {
            let mut set = HashSet::with_capacity(cols.len());

            for col in cols.iter() {
                if is_valid_col(col).is_err() {
                    return Err(ParseError::ValidationError("invalid columns").into());
                }
                if set.contains(col) {
                    return Err(ParseError::ValidationError("cant list duplicate columns").into());
                }
                set.insert(col);
            }
        };
        Ok(())
    }
}

#[derive(Debug)]
pub struct StatementLimit(pub i64);

impl StatementLimit {
    pub fn is_valid(&self) -> Result<()> {
        if self.0 > 0 {
            Ok(())
        } else {
            error!(limit = self.0, "validation error: limit cant be negative");
            Err(ParseError::ValidationError("Limit clause cant be negative").into())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum Precedence {
    Lowest,
    Equals,
    LessGreater,
    Sum,
    Product,
    Prefix,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ValueObject {
    Str(Rc<str>),
    Int(i64),
}

impl ValueObject {
    pub fn is_valid(&self) -> Result<()> {
        match self {
            ValueObject::Str(s) => {
                if s.is_empty() || s.len() > BTREE_MAX_VAL_SIZE {
                    error!(
                        string = s.deref(),
                        "validation error: value string is empty or exceeds max size"
                    );
                    Err(
                        ParseError::ValidationError("value string is empty or exceeds max size")
                            .into(),
                    )
                } else {
                    Ok(())
                }
            }
            ValueObject::Int(_) => Ok(()),
        }
    }
}
