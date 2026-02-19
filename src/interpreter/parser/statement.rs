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
    parse_keyword(parser, Token::Keyword(Keyword::FROM))?;
    statement.table = parse_identifier(parser)?;

    if parser.lexer.current == Token::Seperator(Seperator::Semicolon) {
        return Ok(Statement::Select(statement));
    }

    // optional index or limit clause
    match &parser.lexer.current {
        Token::Keyword(Keyword::WHERE) => {
            debug!("parsing WHERE clause");
            statement.index = Some(parse_index(parser)?);
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

    match &parser.lexer.current {
        Token::Seperator(Seperator::Semicolon) => (),
        Token::Keyword(Keyword::LIMIT) => {
            debug!("parsing LIMIT clause");
            statement.limit = Some(parse_limit(parser)?);
        }
        ref t => {
            return Err(ParseError::InvalidToken {
                expected: "expected LIMIT clause".to_string(),
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
    fn select_statement1() {
        let input = "SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = \"hello\" LIMIT -5 + 7;";
        let mut parser = Parser::new(input);
        parser.next();
        let res = parse_select(&mut parser);
        println!("{:?}", res);
        assert!(res.is_ok());
    }
}
