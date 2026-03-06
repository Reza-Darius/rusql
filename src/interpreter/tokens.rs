use std::{collections::HashMap, rc::Rc};

use crate::database::errors::{ParseError, Result};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Default)]
pub enum Token {
    Illegal,
    #[default]
    Eof,

    Keyword(Keyword),
    Operator(Operator),
    Seperator(Seperator),

    Ident(String), // columns and table names
    Value(Value),
}

impl Token {
    pub fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Value {
    Int(Rc<str>),
    Str(Rc<str>),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Keyword {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,

    Order,
    Set,
    Values,
    All,
    And,
    Or,
    On,
    For,
    Not,
    From,
    Into,
    Where,
    Limit,
    Table,
    Index,

    Int,
    Str,
}

pub const SELECT: &str = "select";
pub const INSERT: &str = "insert";
pub const UPDATE: &str = "update";
pub const DELETE: &str = "delete";
pub const CREATE: &str = "create";
pub const DROP: &str = "drop";

pub const VALUES: &str = "values";
pub const ORDER: &str = "order";
pub const SET: &str = "set";
pub const ALL: &str = "all";
pub const AND: &str = "and";
pub const OR: &str = "or";
pub const ON: &str = "on";
pub const FOR: &str = "for";
pub const NOT: &str = "not";
pub const FROM: &str = "from";
pub const INTO: &str = "into";
pub const WHERE: &str = "where";
pub const LIMIT: &str = "limit";
pub const TABLE: &str = "table";
pub const INDEX: &str = "index";

pub const INT: &str = "int";
pub const STRING: &str = "str";

thread_local! {
    pub static KEYWORDS: HashMap<&'static str, Keyword> =  {
        let mut map = HashMap::new();

        map.insert(SELECT, Keyword::Select);
        map.insert(INSERT, Keyword::Insert);
        map.insert(UPDATE, Keyword::Update);
        map.insert(DELETE, Keyword::Delete);
        map.insert(CREATE, Keyword::Create);
        map.insert(DROP, Keyword::Drop);

        map.insert(VALUES, Keyword::Values);
 map.insert(ORDER, Keyword::Order);
        map.insert(SET, Keyword::Set);
        map.insert(ALL, Keyword::All);
        map.insert(AND, Keyword::And);
        map.insert(OR, Keyword::Or);
        map.insert(FOR, Keyword::For);
        map.insert(ON, Keyword::On);
        map.insert(NOT, Keyword::Not);

        map.insert(FROM, Keyword::From);
        map.insert(INTO, Keyword::Into);
        map.insert(WHERE, Keyword::Where);
        map.insert(LIMIT, Keyword::Limit);
        map.insert(TABLE, Keyword::Table);
        map.insert(INDEX, Keyword::Index);

        map.insert(INT, Keyword::Int);
        map.insert(STRING, Keyword::Str);
        map
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Operator {
    Assign,

    Plus,
    Minus,
    Multi,
    Divide,
    Modulo,

    Equal,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Operator {
    // is the provided operator appropiate for string arithmetic
    pub fn is_valid_for_str(&self) -> Result<()> {
        match self {
            Operator::Plus => Ok(()),
            _ => Err(ParseError::ValidationError("strings only support the + operator").into()),
        }
    }

    // is the provided operator appropiate for comparisons like in WHERE clauses
    pub fn is_valid_cmp(&self) -> Result<()> {
        match self {
            Operator::Assign => Ok(()),
            Operator::Equal => Ok(()),
            Operator::Lt => Ok(()),
            Operator::Le => Ok(()),
            Operator::Gt => Ok(()),
            Operator::Ge => Ok(()),

            _ => Err(ParseError::ValidationError("invalid comparison operator").into()),
        }
    }
}

pub const ASSIGN: char = '=';

pub const PLUS: char = '+';
pub const MINUS: char = '-';
pub const MULTI: char = '*';
pub const DIVIDE: char = '/';
pub const MODULO: char = '%';

pub const LT: char = '<';
pub const GT: char = '>';

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Seperator {
    LParen,
    RParen,
    Comma,
    Semicolon,
}

pub const LPAREN: char = '(';
pub const RPAREN: char = ')';
pub const COMMA: char = ',';
pub const SEMICOLON: char = ';';
