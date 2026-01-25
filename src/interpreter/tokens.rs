use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum Token {
    Illegal,
    EOF,

    Keyword(Keyword),
    Operand(Operator),
    Seperator(Seperator),

    Ident(String),
    Value(Value),
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Int(i64),
    Str(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Keyword {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE,

    VALUES,
    ALL,
    AND,
    FROM,
    INTO,
    WHERE,
    LIMIT,
}

pub const SELECT: &'static str = "select";
pub const INSERT: &'static str = "insert";
pub const UPDATE: &'static str = "update";
pub const DELETE: &'static str = "delete";
pub const CREATE: &'static str = "create";
pub const VALUES: &'static str = "values";

pub const ALL: &'static str = "all";
pub const AND: &'static str = "and";
pub const FROM: &'static str = "from";
pub const INTO: &'static str = "into";
pub const WHERE: &'static str = "where";
pub const LIMIT: &'static str = "limit";

thread_local! {
    pub static KEYWORDS: HashMap<&'static str, Keyword> =  {
        let mut map = HashMap::new();
        map.insert(SELECT, Keyword::SELECT);
        map.insert(INSERT, Keyword::INSERT);
        map.insert(UPDATE, Keyword::UPDATE);
        map.insert(DELETE, Keyword::DELETE);
        map.insert(CREATE, Keyword::CREATE);
        map.insert(VALUES, Keyword::VALUES);

        map.insert(ALL, Keyword::ALL);
        map.insert(AND, Keyword::AND);
        map.insert(FROM, Keyword::FROM);
        map.insert(INTO, Keyword::INTO);
        map.insert(WHERE, Keyword::WHERE);
        map.insert(LIMIT, Keyword::LIMIT);
        map
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operator {
    ASSIGN,
    PLUS,
    MINUS,
    MULTI,
    DIVIDE,
    MODULO,
    LT,
    LE,
    GT,
    GE,
}

pub const ASSIGN: char = '=';
pub const PLUS: char = '+';
pub const MINUS: char = '-';
pub const MULTI: char = '*';
pub const DIVIDE: char = '/';
pub const MODULO: char = '%';
pub const LT: char = '<';
pub const GT: char = '>';

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Seperator {
    LParen,
    RParen,
    Comma,
}

pub const LPAREN: char = '(';
pub const RPAREN: char = ')';
pub const COMMA: char = ',';
