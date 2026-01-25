use std::iter::Peekable;
use std::os::linux::raw::stat;

use crate::database::errors::{Error, ParseError, Result};
use crate::interpreter::{lexer::*, tokens::*};

enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
}

pub fn parse(input: &str) -> Result<Statement> {
    let mut tokens = Lexer::new(input).peekable();

    if let Some(t) = tokens.next() {
        match t {
            Token::Keyword(Keyword::SELECT) => parse_select(&mut tokens),
            // Token::Keyword(Keyword::INSERT) => parse_insert(&mut tokens),
            _ => todo!(),
        }
    } else {
        // error
        Err(ParseError::ParseError("invalid input".to_string()).into())
    }
}

struct SelectStatement {
    columns: Vec<String>,
    table: String,
    index: Option<Vec<Index>>,
    limit: Option<Limit>,
}

fn parse_select(tokens: &mut Peekable<Lexer>) -> Result<Statement> {
    let mut statement = SelectStatement {
        columns: vec![],
        table: String::new(),
        index: None,
        limit: None,
    };

    statement.columns = parse_columns(tokens)?;

    // parse table
    if let Some(t) = tokens.next() {
        match t {
            Token::Ident(i) => {
                statement.table.push_str(&i);
            }
            _ => return Err(ParseError::ParseError("invalid column token".to_string()).into()),
        }
    }

    if tokens.peek().is_none() {
        return Ok(Statement::Select(statement));
    }

    // optional index or limit
    todo!()
}

fn parse_columns(tokens: &mut Peekable<Lexer>) -> Result<Vec<String>> {
    let mut cols: Vec<String> = vec![];
    // single column
    if let Some(t) = tokens.next() {
        match t {
            Token::Ident(i) => {
                cols.push(i.clone());
                return Ok(cols);
            }
            Token::Seperator(Seperator::LParen) => (),
            _ => return Err(ParseError::ParseError("invalid column token".to_string()).into()),
        }
    }

    // multiple columns: (col1, col2...)
    if let Some(t) = tokens.next() {
        match t {
            Token::Ident(i) => {
                cols.push(i.clone());
            }
            _ => return Err(ParseError::ParseError("invalid column token".to_string()).into()),
        }
    } else {
        return Err(ParseError::ParseError("invalid column token".to_string()).into());
    }

    while let Some(t) = tokens.next() {
        match t {
            Token::Ident(i) => {
                cols.push(i.clone());
            }
            Token::Seperator(Seperator::RParen) => return Ok(cols),
            Token::Seperator(Seperator::Comma) => continue,
            _ => return Err(ParseError::ParseError("invalid column token".to_string()).into()),
        }
    }

    Err(ParseError::ParseError("invalid column token".to_string()).into())
}

trait Node {}

struct InsertStatement;
struct UpdateStatement;
struct DeleteStatement;
struct CreateStatement;

struct Index {
    lhs: Expression,
    rhs: Expression,
    operator: Operator,
}

struct Limit {
    value: Expression,
}

struct Expression {
    lhs: Value,
    rhs: Value,
    operator: Operator,
}
