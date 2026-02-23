use tracing::{debug, info};

use crate::database::errors::{ParseError, Result};
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
    pub columns: StatementColumns,
    pub table_name: String,
    pub index: Option<Vec<StatementIndex>>,
    pub limit: Option<StatementLimit>,
}

impl SelectStatement {
    fn validate(&self) -> Result<()> {
        is_valid_identifier(&self.table_name)?;
        self.columns.is_valid()?;

        if let Some(indices) = &self.index {
            for index in indices.iter() {
                index.is_valid(Some(&self.columns))?;
            }
        }

        if let Some(limit) = &self.limit {
            limit.is_valid()?;
        }
        Ok(())
    }

    pub fn get_limit(&self) -> Option<u32> {
        self.limit.as_ref().map(|limit| limit.0 as u32)
    }
}

pub fn parse_select(parser: &mut Parser) -> Result<Statement> {
    info!("parsing SELECT statement!");

    let mut statement = SelectStatement {
        columns: StatementColumns::Wildcard,
        table_name: String::new(),
        index: None,
        limit: None,
    };

    parser.next();
    statement.columns = parse_columns(parser)?;
    parse_token(parser, Token::Keyword(Keyword::From))?;
    statement.table_name = parse_identifier(parser)?;

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
        && (*t != Token::Seperator(Seperator::Semicolon))
    {
        return Err(ParseError::InvalidToken {
            expected: "expected semicolon".to_string(),
            got: t.to_string(),
        }
        .into());
    }

    statement.validate()?;

    Ok(Statement::Select(statement))
}

#[derive(Debug)]
pub struct InsertStatement {
    table_name: String,
    columns: StatementColumns,
    values: Vec<ValueObject>,
}

impl InsertStatement {
    fn validate(&self) -> Result<()> {
        is_valid_identifier(&self.table_name)?;
        self.columns.is_valid()?;
        for value in self.values.iter() {
            value.is_valid()?;
        }
        Ok(())
    }
}

pub fn parse_insert(parser: &mut Parser) -> Result<Statement> {
    info!("parsing insert statement");

    parser.next();
    parse_token(parser, Token::Keyword(Keyword::Into))?;

    let table_name = parse_identifier(parser)?;
    let columns = parse_columns(parser)?;

    if matches!(columns, StatementColumns::Wildcard) {
        return Err(ParseError::ParseError("wildcard cant be used in insert statements").into());
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

                values.push(expr.evaluate()?);
                parser.next();
                continue;
            }
            Token::Seperator(Seperator::LParen) => {
                let expr = parse_expression_statement(parser)
                    .ok_or_else(|| ParseError::ParseError("expected expression"))?;

                values.push(expr.evaluate()?);
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

    if let Some(t) = parser.next()
        && (*t != Token::Seperator(Seperator::Semicolon))
    {
        return Err(ParseError::InvalidToken {
            expected: "expected semicolon".to_string(),
            got: t.to_string(),
        }
        .into());
    }

    let statement = InsertStatement {
        table_name,
        columns,
        values,
    };
    statement.validate()?;

    Ok(Statement::Insert(statement))
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
        assert_eq!(res.as_ref().unwrap().len(), 2);
        println!("{:?}", res.as_ref().unwrap()[0]);
    }
}
