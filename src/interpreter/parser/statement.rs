use tracing::{debug, info};

use crate::database::errors::{ParseError, Result};
use crate::interpreter::parser::eval::*;
use crate::interpreter::parser::parser::*;
use crate::interpreter::parser::types::*;
use crate::interpreter::tokens::*;

#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
}

impl Statement {
    fn execute(self) {}
}

#[derive(Debug)]
pub struct SelectStatement {
    columns: StatementColumns,
    table: String,
    index: Option<Vec<StatementIndex>>,
    limit: Option<Box<dyn Expression>>,
}

pub fn parse_select(parser: &mut Parser) -> Result<Statement> {
    info!("parsing SELECT statement!");

    let mut statement = SelectStatement {
        columns: StatementColumns::Wildcard,
        table: String::new(),
        index: None,
        limit: None,
    };

    parser.next();
    statement.columns = parse_columns(parser)?;
    parse_token(parser, Token::Keyword(Keyword::From))?;
    statement.table = parse_identifier(parser)?;

    if parser.lexer.current == Token::Seperator(Seperator::Semicolon) {
        return Ok(Statement::Select(statement));
    }

    // optional index or limit clause
    match &parser.lexer.current {
        Token::Keyword(Keyword::Where) => {
            debug!("parsing WHERE clause");
            statement.index = Some(parse_index(parser)?);
        }
        Token::Keyword(Keyword::Limit) => {
            debug!("parsing LIMIT clause");
            statement.limit = Some(parse_limit(parser)?);
            return Ok(Statement::Select(statement));
        }
        t => {
            return Err(ParseError::InvalidToken {
                expected: "expected WHERE or LIMIT clause".to_string(),
                got: t.to_string(),
            }
            .into());
        }
    };

    match &parser.lexer.current {
        Token::Seperator(Seperator::Semicolon) => (),
        Token::Keyword(Keyword::Limit) => {
            debug!("parsing LIMIT clause");
            statement.limit = Some(parse_limit(parser)?);
        }
        t => {
            return Err(ParseError::InvalidToken {
                expected: "expected LIMIT clause".to_string(),
                got: t.to_string(),
            }
            .into());
        }
    };

    if let Some(t) = parser.next()
        && let Token::Seperator(Seperator::Semicolon) = t
    {
        Ok(Statement::Select(statement))
    } else {
        Err(ParseError::ParseError("Select statement wasnt closed properly").into())
    }
}

#[derive(Debug)]
pub struct InsertStatement {
    table: String,
    columns: StatementColumns,
    values: Vec<Box<dyn Expression>>,
}

pub fn parse_insert(parser: &mut Parser) -> Result<Statement> {
    info!("parsing insert statement");

    parser.next();
    parse_token(parser, Token::Keyword(Keyword::Into))?;
    let table = parse_identifier(parser)?;

    let columns = parse_columns(parser)?;
    if matches!(columns, StatementColumns::Wildcard) {
        return Err(ParseError::ParseError("wildcard cant be used here").into());
    }

    // parsing values
    parse_token(parser, Token::Keyword(Keyword::Values))?;

    let mut values = vec![];

    while let Some(t) = parser.current() {
        debug!(?t, "parsing token");
        match t {
            Token::Seperator(Seperator::Comma) => {
                parser.next();
                let expr = parse_expression_statement(parser)
                    .ok_or_else(|| ParseError::ParseError("expected expression"))?;

                values.push(expr);
                parser.next();
                continue;
            }
            Token::Seperator(Seperator::RParen) => {
                parser.next();
                continue;
            }
            Token::Seperator(Seperator::Semicolon) => break,

            _ => {
                return Err(ParseError::InvalidToken {
                    expected: "expected expression or comma".to_string(),
                    got: t.to_string(),
                }
                .into());
            }
        }
    }

    if columns.len() != values.len() {
        return Err(ParseError::ParseError("given values dont match provided columns").into());
    }

    Ok(Statement::Insert(InsertStatement {
        table,
        columns,
        values,
    }))
}

#[derive(Debug)]
pub struct UpdateStatement;
#[derive(Debug)]
pub struct DeleteStatement;
#[derive(Debug)]
pub struct CreateStatement;

#[cfg(test)]
mod parser_test {
    use super::*;
    use test_log::test;

    #[test]
    fn select_statement1() {
        let input = "SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = \"hello\" LIMIT -5 + 7;";
        let res = Parser::parse(input);
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[test]
    fn insert_statement() {
        let input = "INSERT INTO table (col1, col2) VALUES (2*2), \"Hello\";";
        let res = Parser::parse(input);
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[test]
    fn multiple_statements() {
        let input = r#"
           SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = "hello" LIMIT -5 + 7;
           INSERT INTO table (col1, col2) VALUES (2*2), "Hello";
           "#;
        let res = Parser::parse(input);
        assert_eq!(res.unwrap().len(), 2);
    }
}
