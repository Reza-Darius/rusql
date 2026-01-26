use std::iter::Peekable;
use std::os::linux::raw::stat;

use crate::database::errors::{Error, ParseError, Result};
use crate::interpreter::{lexer::*, tokens::*};

pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
}

pub fn parse_input(input: &str) -> Result<Statement> {
    let mut tokens = Lexer::new(input).peekable();

    if let Some(t) = tokens.next() {
        match t {
            Token::Keyword(Keyword::SELECT) => parse_select(&mut tokens),
            // Token::Keyword(Keyword::INSERT) => parse_insert(&mut tokens),
            _ => Err(ParseError::ParseError("invalid input".to_string()).into()),
        }
    } else {
        // error
        Err(ParseError::ParseError("invalid input".to_string()).into())
    }
}

pub struct SelectStatement {
    columns: Vec<String>,
    table: String,
    index: Option<Vec<Index>>,
    limit: Option<Value>,
}

fn parse_select(tokens: &mut Peekable<Lexer>) -> Result<Statement> {
    let mut statement = SelectStatement {
        columns: vec![],
        table: String::new(),
        index: None,
        limit: None,
    };

    statement.columns = parse_columns(tokens)?;
    statement.table = parse_table(tokens)?;

    if let Some(t) = tokens.peek() {
        if *t == Token::EOF {
            return Ok(Statement::Select(statement));
        }
    } else {
        return Ok(Statement::Select(statement));
    }

    // optional index or limit clause
    match tokens.next().expect("we just peeked") {
        Token::Keyword(Keyword::WHERE) => {
            statement.index = Some(parse_index(tokens)?);
            ()
        }
        Token::Keyword(Keyword::LIMIT) => {
            statement.limit = Some(parse_limit(tokens)?);
            return Ok(Statement::Select(statement));
        }
        t => {
            return Err(ParseError::InvalidToken {
                expected: "expected WHERE or LIMIT clause".to_string(),
                got: t,
            }
            .into());
        }
    };
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
            t => {
                return Err(ParseError::InvalidToken {
                    expected: "expected column name".to_string(),
                    got: t,
                }
                .into());
            }
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
        return Err(ParseError::ParseError("missing token".to_string()).into());
    }

    while let Some(t) = tokens.next() {
        match t {
            Token::Ident(i) => {
                cols.push(i.clone());
            }
            Token::Seperator(Seperator::RParen) => return Ok(cols),
            Token::Seperator(Seperator::Comma) => continue,
            t => {
                return Err(ParseError::InvalidToken {
                    expected: "expected comma or closing parantheses".to_string(),
                    got: t,
                }
                .into());
            }
        }
    }

    Err(ParseError::ParseError("missing token".to_string()).into())
}

fn parse_table(tokens: &mut Peekable<Lexer>) -> Result<String> {
    if let Some(t) = tokens.next() {
        if t != Token::Keyword(Keyword::FROM) {
            return Err(ParseError::InvalidToken {
                expected: "expected FROM keyword".to_string(),
                got: t,
            }
            .into());
        }
    } else {
        return Err(ParseError::ParseError("invalid token".to_string()).into());
    }

    if let Some(t) = tokens.next() {
        match t {
            Token::Ident(i) => Ok(i),
            t => Err(ParseError::InvalidToken {
                expected: "expected table identifier".to_string(),
                got: t,
            }
            .into()),
        }
    } else {
        Err(ParseError::ParseError("missing token".to_string()).into())
    }
}

fn parse_index(tokens: &mut Peekable<Lexer>) -> Result<Vec<Index>> {
    todo!()
}

fn parse_limit(tokens: &mut Peekable<Lexer>) -> Result<Value> {
    todo!()
}

trait Node {
    fn token_literal(&self) -> String;
}

pub struct InsertStatement;
pub struct UpdateStatement;
pub struct DeleteStatement;
pub struct CreateStatement;

struct Index {
    lhs: Value,
    rhs: Value,
    operator: Operator,
}

struct Expression {
    lhs: Value,
    rhs: Value,
    operator: Operator,
}
