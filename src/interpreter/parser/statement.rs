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

#[derive(Debug)]
pub struct SelectStatement {
    columns: Columns,
    table: String,
    index: Option<Vec<Index>>,
    limit: Option<Box<dyn Expression>>,
}

// SELECT col1, col2 FROM table WHERE col1 = (10 + (1 * 2)) AND col2 > 5 LIMIT -5 + 7;
pub fn parse_select(parser: &mut Parser) -> Result<Statement> {
    info!("parsing SELECT statement!");

    let mut statement = SelectStatement {
        columns: Columns::Wildcard,
        table: String::new(),
        index: None,
        limit: None,
    };

    statement.columns = parse_columns(parser)?;

    // parsing FROM
    if let Some(t) = parser.next() {
        debug!("parsing {t:?}");
        if *t != Token::Keyword(Keyword::FROM) {
            return Err(ParseError::InvalidToken {
                expected: "expected FROM keyword".to_string(),
                got: t.to_string(),
            }
            .into());
        }
    } else {
        return Err(ParseError::ParseError("invalid token".to_string()).into());
    }

    // parsing table ident
    statement.table = parse_identifier(parser)?;

    parser.next();
    if parser.lexer.current == Token::Seperator(Seperator::Semicolon) {
        return Ok(Statement::Select(statement));
    }

    // optional index or limit clause
    match parser.lexer.current {
        Token::Keyword(Keyword::WHERE) => {
            debug!("parsing WHERE clause");
            statement.index = Some(parse_index(parser)?);
            ()
        }
        Token::Keyword(Keyword::LIMIT) => {
            debug!("parsing LIMIT clause");
            statement.limit = Some(parse_limit(parser)?);
            return Ok(Statement::Select(statement));
        }
        ref t => {
            return Err(ParseError::InvalidToken {
                expected: "expected WHERE or LIMIT clause".to_string(),
                got: t.to_string(),
            }
            .into());
        }
    };

    match parser.lexer.current {
        Token::Keyword(Keyword::LIMIT) => {
            debug!("parsing LIMIT clause");
            statement.limit = Some(parse_limit(parser)?);
        }
        ref t => {
            return Err(ParseError::InvalidToken {
                expected: "expected WHERE clause".to_string(),
                got: t.to_string(),
            }
            .into());
        }
    };

    Ok(Statement::Select(statement))
}

#[derive(Debug)]
pub struct InsertStatement;
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
    fn parser_test1() {
        let input = "SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2) LIMIT -5 + 7;";
        let mut parser = Parser::new(input);
        let res = parser.parse_input().unwrap();
        println!("{:?}", res);
    }
}
