use crate::interpreter::{parser::eval::Expression, tokens::Operator};

#[derive(Debug)]
pub struct StatementIndex {
    pub column: String,
    pub operator: Operator,
    pub expr: Box<dyn Expression>,
}

#[derive(Debug)]
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

#[derive(Debug, PartialEq)]
pub enum ValueObject {
    Str(String),
    Int(i64),
}
