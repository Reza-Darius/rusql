use tracing::{debug, error, info, instrument};

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

pub trait StatementInterface {
    fn get_limit(&self) -> Option<usize>;
}

#[derive(Debug)]
pub struct SelectStatement {
    pub columns: StatementColumns,
    pub table_name: String,
    pub index: Option<Vec<StatementIndex>>,
    pub limit: Option<StatementLimit>,
    pub order: Option<StatementOrder>,
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

        if let Some(order) = &self.order {
            order.is_valid()?;
        }
        Ok(())
    }
}

impl StatementInterface for SelectStatement {
    fn get_limit(&self) -> Option<usize> {
        self.limit.as_ref().map(|limit| limit.0 as usize)
    }
}

#[instrument(skip_all)]
pub fn parse_select(parser: &mut Parser) -> Result<Statement> {
    info!("parsing SELECT statement!");

    parser.next();
    let columns = parse_columns(parser)?;

    parse_token(parser, Token::Keyword(Keyword::From))?;

    let table_name = parse_identifier(parser)?;

    let mut statement = SelectStatement {
        columns,
        table_name,
        index: None,
        limit: None,
        order: None,
    };

    // optional index or limit clause
    loop {
        debug!("parsing token {:?}", &parser.lexer.current);
        match &parser.lexer.current {
            Token::Seperator(Seperator::Semicolon) => {
                statement.validate()?;
                return Ok(Statement::Select(statement));
            }
            Token::Keyword(Keyword::Where) => {
                debug!("parsing WHERE clause");

                if statement.index.is_some() {
                    error!("cant provide multiple WHERE clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple WHERE clauses").into(),
                    );
                }
                statement.index = Some(parse_index(parser)?);
            }
            Token::Keyword(Keyword::Limit) => {
                debug!("parsing LIMIT clause");

                if statement.limit.is_some() {
                    error!("cant provide multiple LIMIT clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple LIMIT clauses").into(),
                    );
                }
                statement.limit = Some(parse_limit(parser)?);
            }
            Token::Keyword(Keyword::Order) => {
                debug!("parsing ORDER clause");

                if statement.order.is_some() {
                    error!("cant provide multiple ORDER clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple ORDER clauses").into(),
                    );
                }
                statement.order = Some(parse_order(parser)?);
            }
            t => {
                let err = ParseError::InvalidToken {
                    expected: "expected WHERE or LIMIT clause".to_string(),
                    got: t.to_string(),
                };
                error!("{err}");
                return Err(err.into());
            }
        };
    }
}

#[derive(Debug)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: StatementColumns,
    pub values: Vec<ValueObject>,
}

impl InsertStatement {
    fn validate(&self) -> Result<()> {
        is_valid_identifier(&self.table_name)?;

        if self.columns == StatementColumns::Wildcard {
            error!("wildcard is not permitted for insert statements");
            return Err(ParseError::ValidationError(
                "wildcard is not permitted for insert statements",
            )
            .into());
        }

        self.columns.is_valid()?;

        if self.values.is_empty() {
            error!("no values provided");
            return Err(ParseError::ParseError("no values provided").into());
        }

        for value in self.values.iter() {
            value.is_valid()?;
        }

        if self.columns.len() != self.values.len() {
            error!("values amount doesnt match column amount");
            return Err(ParseError::ParseError("given values dont match provided columns").into());
        }

        Ok(())
    }
}

#[instrument(skip_all)]
pub fn parse_insert(parser: &mut Parser) -> Result<Statement> {
    info!("parsing insert statement");

    parser.next();
    parse_token(parser, Token::Keyword(Keyword::Into))?;

    let table_name = parse_identifier(parser)?;
    let columns = parse_columns(parser)?;

    parse_token(parser, Token::Keyword(Keyword::Values))?;

    let mut statement = InsertStatement {
        table_name,
        columns,
        values: vec![],
    };

    // parsing VALUES
    loop {
        debug!("parsing token {:?}", &parser.lexer.current);

        match &parser.lexer.current {
            Token::Seperator(Seperator::Comma) | Token::Seperator(Seperator::RParen) => {
                parser.next();
                continue;
            }
            // expression
            Token::Seperator(Seperator::LParen)
            | Token::Value(Value::Int(_))
            | Token::Value(Value::Str(_)) => {
                let expr = parse_expression_statement(parser)
                    .ok_or_else(|| ParseError::ParseError("expected expression"))?;

                statement.values.push(expr.evaluate()?);
                parser.next();
                continue;
            }
            Token::Seperator(Seperator::Semicolon) => {
                statement.validate()?;
                return Ok(Statement::Insert(statement));
            }
            t => {
                return Err(ParseError::InvalidToken {
                    expected: "expected expression or comma".to_string(),
                    got: t.to_string(),
                }
                .into());
            }
        }
    }
}

#[derive(Debug)]
pub struct UpdateStatement {
    table_name: String,
    set: Vec<StatementSet>,
    index: Option<Vec<StatementIndex>>,
    order: Option<StatementOrder>,
    limit: Option<StatementLimit>,
}

impl UpdateStatement {
    fn validate(&self) -> Result<()> {
        is_valid_identifier(&self.table_name)?;

        for set in self.set.iter() {
            set.is_valid()?;
        }

        if let Some(indices) = &self.index {
            for index in indices.iter() {
                index.is_valid(None)?;
            }
        }

        if let Some(limit) = &self.limit {
            limit.is_valid()?;
        }

        if let Some(order) = &self.order {
            order.is_valid()?;
        }

        Ok(())
    }
}

// UPDATE table_name SET col1 = expr, col2 = expr WHERE col1 > expr LIMIT 5
#[instrument(skip_all)]
pub fn parse_update(parser: &mut Parser) -> Result<Statement> {
    info!("parsing update statement");

    parser.next();

    let table_name = parse_identifier(parser)?;
    parse_token(parser, Token::Keyword(Keyword::Set))?;

    let mut statement = UpdateStatement {
        table_name,
        set: vec![],
        index: None,
        order: None,
        limit: None,
    };

    // parsing SET
    loop {
        debug!("parsing token {:?}", &parser.lexer.current);
        match &parser.lexer.current {
            Token::Seperator(Seperator::Comma) => {
                parser.next();
                continue;
            }
            Token::Ident(identifier) => {
                let column = identifier.clone();
                parser.next();

                parse_token(parser, Token::Operator(Operator::Assign))?;

                let expr = parse_expression_statement(parser)
                    .ok_or_else(|| ParseError::ParseError("expected expression"))?
                    .evaluate()?;

                statement.set.push(StatementSet { column, expr });
                parser.next();
            }
            Token::Seperator(Seperator::Semicolon) | Token::Keyword(_) => break,

            t => {
                return Err(ParseError::InvalidToken {
                    expected: "expected expression or comma".to_string(),
                    got: t.to_string(),
                }
                .into());
            }
        }
    }

    if statement.set.is_empty() {
        error!("no statements provided");
        return Err(ParseError::ParseError("no statements provided").into());
    }

    // optional index or limit clause
    loop {
        debug!("parsing token {:?}", &parser.lexer.current);
        match &parser.lexer.current {
            Token::Seperator(Seperator::Semicolon) => {
                statement.validate()?;
                return Ok(Statement::Update(statement));
            }
            Token::Keyword(Keyword::Where) => {
                debug!("parsing WHERE clause");

                if statement.index.is_some() {
                    error!("cant provide multiple WHERE clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple WHERE clauses").into(),
                    );
                }
                statement.index = Some(parse_index(parser)?);
            }
            Token::Keyword(Keyword::Limit) => {
                debug!("parsing LIMIT clause");

                if statement.limit.is_some() {
                    error!("cant provide multiple LIMIT clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple LIMIT clauses").into(),
                    );
                }
                statement.limit = Some(parse_limit(parser)?);
            }
            Token::Keyword(Keyword::Order) => {
                debug!("parsing ORDER clause");

                if statement.order.is_some() {
                    error!("cant provide multiple ORDER clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple ORDER clauses").into(),
                    );
                }
                statement.order = Some(parse_order(parser)?);
            }
            t => {
                let err = ParseError::InvalidToken {
                    expected: "expected WHERE or LIMIT clause".to_string(),
                    got: t.to_string(),
                };
                error!("{err}");
                return Err(err.into());
            }
        };
    }
}

#[derive(Debug)]
pub struct DeleteStatement {
    table_name: String,
    index: Option<Vec<StatementIndex>>,
    order: Option<StatementOrder>,
    limit: Option<StatementLimit>,
}

impl DeleteStatement {
    fn validate(&self) -> Result<()> {
        is_valid_identifier(&self.table_name)?;

        if let Some(indices) = &self.index {
            for index in indices.iter() {
                index.is_valid(None)?;
            }
        }

        if let Some(limit) = &self.limit {
            limit.is_valid()?;
        }

        if let Some(order) = &self.order {
            order.is_valid()?;
        }
        Ok(())
    }
}

pub fn parse_delete(parser: &mut Parser) -> Result<Statement> {
    info!("parsing delete statement");

    parser.next();
    parse_token(parser, Token::Keyword(Keyword::From))?;
    let table_name = parse_identifier(parser)?;

    let mut statement = DeleteStatement {
        table_name,
        index: None,
        order: None,
        limit: None,
    };

    // optional index or limit clause
    loop {
        debug!("parsing token {:?}", &parser.lexer.current);

        match &parser.lexer.current {
            Token::Seperator(Seperator::Semicolon) => {
                statement.validate()?;
                return Ok(Statement::Delete(statement));
            }
            Token::Keyword(Keyword::Where) => {
                debug!("parsing WHERE clause");

                if statement.index.is_some() {
                    error!("cant provide multiple WHERE clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple WHERE clauses").into(),
                    );
                }
                statement.index = Some(parse_index(parser)?);
            }
            Token::Keyword(Keyword::Limit) => {
                debug!("parsing LIMIT clause");

                if statement.limit.is_some() {
                    error!("cant provide multiple LIMIT clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple LIMIT clauses").into(),
                    );
                }
                statement.limit = Some(parse_limit(parser)?);
            }
            Token::Keyword(Keyword::Order) => {
                debug!("parsing ORDER clause");

                if statement.order.is_some() {
                    error!("cant provide multiple ORDER clauses");
                    return Err(
                        ParseError::ParseError("cant provide multiple ORDER clauses").into(),
                    );
                }
                statement.order = Some(parse_order(parser)?);
            }
            t => {
                let err = ParseError::InvalidToken {
                    expected: "expected WHERE or LIMIT clause".to_string(),
                    got: t.to_string(),
                };
                error!("{err}");
                return Err(err.into());
            }
        };
    }
}

#[derive(Debug)]
pub struct CreateStatement;

#[cfg(test)]
mod parser_test {
    use super::*;
    use test_log::test;

    #[test]
    fn select_parse1() {
        let input = r#"
            SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = "hello" LIMIT -5 + 7;
            SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = "hello" LIMIT -5 + 7 ORDER col2;
            SELECT * FROM table ORDER col1;
        "#;
        let res = Parser::parse(input);
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[test]
    fn insert_parse1() {
        let input = "INSERT INTO table (col1, col2) VALUES (2*2), \"Hello\";";
        let res = Parser::parse(input);
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[test]
    fn update_parse1() {
        let input = r#"
            UPDATE table SET col1 = "hello", col2 = 10 WHERE col2 > 10 LIMIT 5 ORDER col1;
            UPDATE table SET col = "hello" WHERE col <= "hi" ORDER col LIMIT 2;
        "#;
        let res = Parser::parse(input);
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[test]
    fn delete_parse1() {
        let input = r#"
            DELETE FROM table WHERE col1 = 1, col2 > 10, col3 <= "hello" LIMIT 10 - 2 ORDER col2;
            DELETE FROM table;
        "#;
        let res = Parser::parse(input);
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[test]
    fn composite_parse1() {
        let input = r#"
           INSERT INTO table (col1, col2) VALUES (2*2), "Hello";
           UPDATE table SET col1 = "hello", col2 = 10 WHERE col2 > 10 LIMIT 5;
           SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2), col2 = "hello" LIMIT -5 + 7;
           DELETE FROM table WHERE col1 = 1, col2 > 10, col3 <= "hello" LIMIT 10 - 2 ORDER col2;
           "#;
        let res = Parser::parse(input);
        assert_eq!(res.as_ref().unwrap().len(), 4);
        for stmt in res.unwrap() {
            println!("{stmt:?}");
        }
    }
}
