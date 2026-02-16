use crate::interpreter::{parser::eval::Expression, tokens::Operator};

#[derive(Debug)]
pub struct Index {
    pub column: String,
    pub operator: Operator,
    pub expr: Box<dyn Expression>,
}

#[derive(Debug)]
pub enum Columns {
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
