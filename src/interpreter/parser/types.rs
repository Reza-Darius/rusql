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
